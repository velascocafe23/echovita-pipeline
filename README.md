# Echovita Pipeline

Pipeline de integración de datos de obituarios desde [Echovita](https://www.echovita.com), construido para enriquecer el índice FOD dentro del proyecto Veritas.

La idea era simple: extraer obituarios de forma confiable, almacenarlos en múltiples destinos y consolidar el historial de ubicaciones por persona. El resultado es un pipeline en tres capas — scraping, storage y consolidación SQL — orquestado con Airflow y desplegable con un solo comando Docker.

---

## Estructura

```
echovita_pipeline/
│
├── scraper/                        # Proyecto Scrapy
│   ├── scrapy.cfg
│   └── echovita/
│       ├── items.py                # Modelo de datos (ObituaryItem + ObituaryRecord)
│       ├── settings.py             # Configuración central
│       ├── middlewares.py          # UA rotation + logging
│       ├── pipelines/
│       │   ├── base.py             # StoragePipeline abstracta (Strategy Pattern)
│       │   ├── validation_pipeline.py
│       │   ├── s3_pipeline.py      # Mock AWS S3
│       │   ├── gcs_pipeline.py     # Mock Google Cloud Storage
│       │   └── jsonl_pipeline.py   # Export local
│       └── spiders/
│           └── echovita_spider.py
│
├── consolidation/
│   ├── models.py                   # Datos SCD de ejemplo
│   └── consolidate.py              # Query de consolidación DuckDB
│
├── dags/
│   └── echovita_dag.py
│
├── dashboard.py                    # Streamlit — visualización del pipeline
├── Dockerfile
├── docker-compose.yml
├── tests/
│   └── test_pipeline.py            # 24 tests unitarios
└── requirements.txt
```

---

## Inicio rápido con Docker

La forma más rápida de levantar todo:

```bash
git clone https://github.com/velascocafe23/echovita-pipeline.git
cd echovita-pipeline
docker compose up --build
```

El dashboard queda disponible en **http://localhost:8501**

---

## Instalación local

```bash
python -m venv venv

# Windows
venv\Scripts\activate
# Linux/Mac
source venv/bin/activate

pip install -r requirements.txt
```

---

## Parte 1 — Web Scraping

```bash
cd scraper

# Prueba rápida
scrapy crawl echovita -s CLOSESPIDER_ITEMCOUNT=10

# Crawl completo
scrapy crawl echovita
```

Cada ejecución genera `scraper/obituaries.jsonl` y simula uploads a S3 y GCS en consola.

### Campos extraídos

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `full_name` | string | Nombre completo |
| `date_of_birth` | string \| null | Fecha de nacimiento (ISO 8601) |
| `date_of_death` | string \| null | Fecha de fallecimiento (ISO 8601) |
| `obituary_text` | string \| null | Texto completo del obituario |
| `source_url` | string | URL de origen |
| `scraped_at` | string | Timestamp UTC de extracción |

### Ejemplo

```json
{
  "full_name": "Bonnie L. Williams",
  "date_of_birth": "1938-02-19",
  "date_of_death": "2026-02-18",
  "obituary_text": "She is survived by her children...",
  "source_url": "https://www.echovita.com/us/obituaries/tn/rutledge/bonnie-williams",
  "scraped_at": "2026-03-11T20:38:17Z"
}
```

---

## Parte 2 — Consolidación SCD

```bash
# Desde la raíz del proyecto
python -m consolidation.consolidate
```

Toma la tabla SCD Type 2 y genera una fila resumen por persona:

```
person_id   distinct_cities   first_city   last_city   last_non_null_city
──────────────────────────────────────────────────────────────────────────
1           2                 Houston      None        Dallas
2           1                 San          San         San
3           0                 None         None        None
```

Cuatro métricas por persona: ciudades distintas, primera ciudad, última ciudad (puede ser null) y última ciudad no-null. Los casos edge con nulls fueron los más interesantes de modelar en SQL.

---

## Parte 3 — Airflow DAG

| Parámetro | Valor |
|-----------|-------|
| `dag_id` | `echovita_pipeline` |
| `schedule` | `0 8 * * *` |
| `retries` | 3 |
| `retry_delay` | 5 min |
| `catchup` | False |

```
scrape_echovita → validate_s3_uploads → validate_jsonl_export → consolidate_scd
```

```bash
export AIRFLOW_HOME=~/airflow
airflow db init
airflow dags test echovita_pipeline 2026-03-11
```

---

## Tests

```bash
pytest tests/ -v
```

24 tests cubriendo extracción de campos, normalización de fechas, casos edge del SCD (nulls, persona sin ciudad, idempotencia) y validación de pipelines.

---

## Decisiones de diseño

**Strategy Pattern en storage** — `S3Pipeline` y `GCSPipeline` heredan de `StoragePipeline`. Agregar Azure Blob o SFTP es crear una clase nueva que implemente `_upload()`, sin tocar el código existente.

**Separación de capas** — El spider no sabe nada de storage. Los pipelines no saben nada de HTML. La consolidación opera sobre datos limpios. Cada capa tiene una sola responsabilidad.

**Idempotencia** — JSONL en modo `write`, DuckDB en memoria, Airflow con `catchup=False`. Cada ejecución parte de un estado limpio.

**Mocks fieles a producción** — Los mocks replican la interfaz exacta. Para producción real solo se reemplaza `_upload()`:
- S3: `boto3.client('s3').put_object(...)`
- GCS: `storage.Client().bucket(...).blob(...).upload_from_string(...)`

**AutoThrottle** — En vez de un delay fijo, el spider mide la latencia real del servidor y ajusta la frecuencia dinámicamente. Más respetuoso con el sitio y más eficiente en entornos variables.
