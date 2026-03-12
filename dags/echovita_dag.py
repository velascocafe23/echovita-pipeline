"""
echovita_dag.py — DAG de orquestación del pipeline Echovita

Orquesta el pipeline completo en 4 tareas secuenciales:
  1. scrape        : Ejecuta el spider de Scrapy
  2. validate_s3   : Verifica que los uploads a S3 se realizaron
  3. validate_jsonl: Verifica que el archivo JSONL fue generado
  4. consolidate   : Ejecuta la consolidación SCD con DuckDB

Principios de diseño aplicados:
  - Idempotente: cada tarea puede re-ejecutarse sin efectos secundarios
  - Retries: 3 intentos con espacio de 5 minutos entre cada uno
  - Logging: cada tarea loguea su resultado explícitamente
  - Modular: cada tarea es independiente y testeable por separado
  - Schedule: diario a las 8:00 AM UTC
"""

import json
import logging
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# ── Configuración de paths ────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).parent.parent
SCRAPER_DIR  = PROJECT_ROOT / "scraper"
JSONL_PATH   = SCRAPER_DIR / "obituaries.jsonl"

# ── Argumentos por defecto del DAG ────────────────────────────────────────────
default_args = {
    "owner"           : "data-engineering",
    "depends_on_past" : False,
    "email_on_failure": False,
    "email_on_retry"  : False,
    "retries"         : 3,
    "retry_delay"     : timedelta(minutes=5),
}

# ── Definición del DAG ────────────────────────────────────────────────────────
with DAG(
    dag_id="echovita_pipeline",
    default_args=default_args,
    description="Pipeline diario de scraping y consolidación de obituarios Echovita",
    schedule_interval="0 8 * * *",   # 8:00 AM UTC todos los días
    start_date=datetime(2026, 1, 1),
    catchup=False,                   # No ejecutar runs históricos al activar el DAG
    tags=["echovita", "scraping", "data-engineering"],
) as dag:

    # ── Tarea 1: Web Scraping ─────────────────────────────────────────────────
    def run_scraper(**context) -> dict:
        """
        Ejecuta el spider de Scrapy como subproceso.

        Usamos subprocess en lugar de importar Scrapy directamente porque:
        1. Scrapy usa Twisted (event loop propio) — incompatible con Airflow
        2. subprocess aísla el proceso y captura stdout/stderr por separado
        3. Permite pasar settings dinámicos sin modificar el código del spider

        Idempotencia: el JSONL se abre en modo 'w' (write), cada ejecución
        sobreescribe el archivo anterior. No hay duplicados.
        """
        logger = logging.getLogger(__name__)
        logger.info(f"Iniciando scraper desde: {SCRAPER_DIR}")

        # Añadir el directorio del proyecto al PYTHONPATH del subproceso
        env = os.environ.copy()
        env["PYTHONPATH"] = str(PROJECT_ROOT)

        result = subprocess.run(
            [
                sys.executable, "-m", "scrapy", "crawl", "echovita",
                "--logfile", str(PROJECT_ROOT / "logs" / f"scrapy_{context['ds']}.log"),
                "--loglevel", "INFO",
            ],
            cwd=str(SCRAPER_DIR),
            capture_output=True,
            text=True,
            env=env,
        )

        if result.returncode != 0:
            logger.error(f"Scrapy stderr: {result.stderr[-2000:]}")
            raise RuntimeError(f"Scrapy falló con código {result.returncode}")

        logger.info("Scraper completado exitosamente")
        logger.info(result.stdout[-1000:])  # Últimas líneas del log

        return {"status": "ok", "execution_date": context["ds"]}

    # ── Tarea 2: Validación S3 ────────────────────────────────────────────────
    def validate_s3_uploads(**context) -> dict:
        """
        Verifica que la pipeline S3 haya registrado uploads.

        En producción real, aquí haríamos:
            s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        y verificaríamos que los objetos existen.

        Con el mock, verificamos que el JSONL tiene registros
        (proxy de que el pipeline corrió completo).

        Idempotencia: solo lee, no escribe. Siempre puede re-ejecutarse.
        """
        logger = logging.getLogger(__name__)

        if not JSONL_PATH.exists():
            raise FileNotFoundError(f"JSONL no encontrado en {JSONL_PATH}")

        with open(JSONL_PATH, encoding="utf-8") as f:
            lines = [l.strip() for l in f if l.strip()]

        if not lines:
            raise ValueError("El archivo JSONL está vacío — el scraper no generó resultados")

        logger.info(f"[S3 MOCK] Validación exitosa: {len(lines)} objetos habrían sido subidos a S3")

        # Push a XCom para que tareas posteriores puedan usar el conteo
        return {"s3_objects_count": len(lines), "status": "ok"}

    # ── Tarea 3: Validación JSONL ─────────────────────────────────────────────
    def validate_jsonl_export(**context) -> dict:
        """
        Verifica que el archivo JSONL:
        1. Existe en la ruta esperada
        2. No está vacío
        3. Cada línea es JSON válido
        4. Contiene los campos requeridos por el enunciado

        Idempotencia: solo lee, no modifica.
        """
        logger = logging.getLogger(__name__)
        required_fields = {"full_name", "date_of_birth", "date_of_death", "obituary_text"}

        if not JSONL_PATH.exists():
            raise FileNotFoundError(f"JSONL no encontrado: {JSONL_PATH}")

        valid_records   = 0
        invalid_records = 0
        missing_fields  = set()

        with open(JSONL_PATH, encoding="utf-8") as f:
            for i, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    present = set(record.keys())
                    missing = required_fields - present
                    if missing:
                        missing_fields.update(missing)
                        invalid_records += 1
                    else:
                        valid_records += 1
                except json.JSONDecodeError as e:
                    logger.warning(f"Línea {i} no es JSON válido: {e}")
                    invalid_records += 1

        if valid_records == 0:
            raise ValueError("No se encontraron registros válidos en el JSONL")

        if missing_fields:
            logger.warning(f"Campos faltantes detectados: {missing_fields}")

        logger.info(
            f"[JSONL] Validación: {valid_records} válidos, "
            f"{invalid_records} inválidos | path: {JSONL_PATH}"
        )

        return {
            "valid_records"  : valid_records,
            "invalid_records": invalid_records,
            "status"         : "ok",
        }

    # ── Tarea 4: Consolidación SCD ────────────────────────────────────────────
    def run_consolidation_task(**context) -> dict:
        """
        Ejecuta la consolidación SCD con DuckDB.

        Añade el PROJECT_ROOT al sys.path para importar el módulo
        de consolidación sin necesitar instalación como paquete.

        Idempotencia: DuckDB opera en memoria, cada ejecución crea
        una base de datos nueva desde cero. No hay estado persistente.
        """
        logger = logging.getLogger(__name__)

        if str(PROJECT_ROOT) not in sys.path:
            sys.path.insert(0, str(PROJECT_ROOT))

        from consolidation.consolidate import run_consolidation, print_results

        logger.info("Iniciando consolidación SCD con DuckDB")
        results = run_consolidation()

        logger.info(f"Consolidación completada: {len(results)} personas procesadas")
        for row in results:
            logger.info(f"  person_id={row['person_id']} | "
                       f"cities={row['distinct_cities']} | "
                       f"first={row['first_city']} | "
                       f"last={row['last_city']} | "
                       f"last_non_null={row['last_non_null_city']}")

        return {"persons_consolidated": len(results), "status": "ok"}

    # ── Declaración de tareas ─────────────────────────────────────────────────
    task_scrape = PythonOperator(
        task_id="scrape_echovita",
        python_callable=run_scraper,
    )

    task_validate_s3 = PythonOperator(
        task_id="validate_s3_uploads",
        python_callable=validate_s3_uploads,
    )

    task_validate_jsonl = PythonOperator(
        task_id="validate_jsonl_export",
        python_callable=validate_jsonl_export,
    )

    task_consolidate = PythonOperator(
        task_id="consolidate_scd",
        python_callable=run_consolidation_task,
    )

    # ── Dependencias ──────────────────────────────────────────────────────────
    #
    # Flujo:
    #   scrape → validate_s3 → validate_jsonl → consolidate
    #
    # validate_s3 y validate_jsonl podrían correr en paralelo,
    # pero las dejamos secuenciales para facilitar debugging:
    # si falla S3, no tiene sentido validar JSONL.
    #
    task_scrape >> task_validate_s3 >> task_validate_jsonl >> task_consolidate
