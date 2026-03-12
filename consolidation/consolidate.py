"""
consolidate.py — Consolidación SCD con DuckDB

Toma la tabla histórica SCD Type 2 y la consolida en una tabla resumen
con una fila por persona.

Columnas requeridas por el enunciado:
  - person_id         : ID de la persona
  - distinct_cities   : Cuántas ciudades distintas (no null) ha tenido
  - first_city        : Primera ciudad (por valid_from ASC), puede ser null
  - last_city         : Última ciudad (por valid_from DESC), puede ser null
  - last_non_null_city: Última ciudad no nula (por valid_from DESC)

Decisiones de diseño:
  - Usamos DuckDB en memoria: sin servidor, sin configuración, ideal para pipelines
  - Una sola query SQL con CTEs para máxima legibilidad y mantenibilidad
  - La función es idempotente: puede ejecutarse N veces con el mismo resultado
"""

import logging
from typing import Optional
import duckdb

from consolidation.models import SCD_RECORDS

logger = logging.getLogger(__name__)


# ── Query principal ───────────────────────────────────────────────────────────
#
# Estrategia:
#   1. ranked      : numera los registros por persona ordenados por valid_from
#   2. agg         : agrega distinct_cities y first_city en un solo paso
#   3. last_city   : obtiene la última ciudad (rank DESC = 1), puede ser null
#   4. last_non_null: obtiene la última ciudad no nula (filtrando nulls primero)
#   5. JOIN final  : une todo en la tabla resultado
#
CONSOLIDATION_QUERY = """
WITH ranked AS (
    SELECT
        person_id,
        name,
        city,
        valid_from,
        ROW_NUMBER() OVER (
            PARTITION BY person_id
            ORDER BY valid_from ASC
        ) AS rn_asc,
        ROW_NUMBER() OVER (
            PARTITION BY person_id
            ORDER BY valid_from DESC
        ) AS rn_desc
    FROM scd_history
),

-- Ciudades distintas (excluye nulls) y primera ciudad por persona
agg AS (
    SELECT
        person_id,
        COUNT(DISTINCT city) FILTER (WHERE city IS NOT NULL) AS distinct_cities,
        MAX(city) FILTER (WHERE rn_asc = 1)                  AS first_city
    FROM ranked
    GROUP BY person_id
),

-- Última ciudad (puede ser null — tomamos el registro más reciente)
last_city AS (
    SELECT person_id, city AS last_city
    FROM ranked
    WHERE rn_desc = 1
),

-- Última ciudad NO nula (ignoramos registros donde city IS NULL)
last_non_null_city AS (
    SELECT DISTINCT ON (person_id)
        person_id,
        city AS last_non_null_city
    FROM ranked
    WHERE city IS NOT NULL
    ORDER BY person_id, valid_from DESC
)

SELECT
    agg.person_id,
    agg.distinct_cities,
    agg.first_city,
    lc.last_city,
    lnnc.last_non_null_city
FROM agg
LEFT JOIN last_city         lc   ON agg.person_id = lc.person_id
LEFT JOIN last_non_null_city lnnc ON agg.person_id = lnnc.person_id
ORDER BY agg.person_id
"""



def load_from_jsonl(jsonl_path: str) -> list[dict]:
    """
    Carga registros SCD desde el archivo JSONL generado por el spider.

    Conecta la Parte 1 (scraping) con la Parte 2 (consolidación):
    en vez de usar datos hardcodeados, lee los obituarios reales scrapeados
    y los convierte al formato SCD esperado.

    En producción, esta función leería desde S3 o GCS en lugar del filesystem.
    """
    import json
    from pathlib import Path

    path = Path(jsonl_path)
    if not path.exists():
        raise FileNotFoundError(f"JSONL no encontrado: {jsonl_path}")

    records = []
    with open(path, encoding="utf-8") as f:
        for i, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
                # Mapear campos del obituario al esquema SCD
                records.append({
                    "person_id"  : i,
                    "name"       : item.get("full_name", "Unknown"),
                    "state"      : None,   # Echovita no expone estado directamente
                    "city"       : None,   # campo geográfico — se enriquece en pipeline real
                    "valid_from" : item.get("date_of_death") or item.get("scraped_at", "")[:10],
                    "valid_to"   : None,   # registro activo
                })
            except json.JSONDecodeError as e:
                logger.warning(f"Línea {i} inválida en JSONL: {e}")

    logger.info(f"Cargados {len(records)} registros desde {jsonl_path}")
    return records

def run_consolidation(records: Optional[list[dict]] = None) -> list[dict]:
    """
    Ejecuta la consolidación SCD y retorna los resultados como lista de dicts.

    Args:
        records: Lista de registros SCD. Si es None, usa SCD_RECORDS de models.py.
                 Esto permite inyectar datos en tests o desde Airflow.

    Returns:
        Lista de dicts con columnas: person_id, distinct_cities,
        first_city, last_city, last_non_null_city
    """
    if records is None:
        records = SCD_RECORDS

    logger.info(f"Iniciando consolidación con {len(records)} registros SCD")

    # DuckDB en memoria — no necesita servidor ni archivos
    con = duckdb.connect(database=":memory:")

    try:
        # Crear tabla SCD desde los records Python
        con.execute("""
            CREATE TABLE scd_history (
                person_id  INTEGER,
                name       VARCHAR,
                state      VARCHAR,
                city       VARCHAR,
                valid_from DATE,
                valid_to   DATE
            )
        """)

        # Insertar registros — DuckDB acepta list of dicts directamente
        con.executemany(
            "INSERT INTO scd_history VALUES (?, ?, ?, ?, ?, ?)",
            [
                (
                    r["person_id"],
                    r["name"],
                    r["state"],
                    r["city"],
                    r["valid_from"],
                    r["valid_to"],
                )
                for r in records
            ],
        )

        logger.info("Tabla scd_history creada e insertada correctamente")

        # Ejecutar consolidación
        result = con.execute(CONSOLIDATION_QUERY).fetchdf()

        logger.info(f"Consolidación completada: {len(result)} personas procesadas")

        # Convertir DataFrame a lista de dicts — float nan → None para JSON null
        raw = result.to_dict(orient="records")
        return [{k: (None if (v != v) else v) for k, v in row.items()} for row in raw]

    finally:
        con.close()


def print_results(results: list[dict]) -> None:
    """Imprime los resultados en formato tabla para debugging."""
    if not results:
        print("Sin resultados.")
        return

    headers = list(results[0].keys())
    col_width = 20

    # Header
    print("\n" + "─" * (col_width * len(headers)))
    print("".join(str(h).ljust(col_width) for h in headers))
    print("─" * (col_width * len(headers)))

    # Rows
    for row in results:
        print("".join(str(v).ljust(col_width) for v in row.values()))

    print("─" * (col_width * len(headers)))
    print(f"Total: {len(results)} personas\n")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    results = run_consolidation()
    print_results(results)
