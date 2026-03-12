# Echovita Pipeline

Pipeline de integración de datos de obituarios desde [Echovita](https://www.echovita.com),
desarrollado como parte del proyecto Veritas para enriquecer el índice FOD.

---

## Estructura del proyecto

```
echovita_pipeline/
│
├── scraper/                        # Proyecto Scrapy
│   ├── scrapy.cfg
│   └── echovita/
│       ├── items.py                # Modelo de datos (ObituaryItem + ObituaryRecord)
│       ├── settings.py             # Configuración central
│       ├── pipelines/
│       │   ├── base.py             # Clase abstracta StoragePipeline (Strategy Pattern)
│       │   ├── validation_pipeline.py  # Filtrado y limpieza
│       │   ├── s3_pipeline.py      # Mock de AWS S3
│       │   ├── gcs_pipeline.py     # Mock de Google Cloud Storage
│       │   └── jsonl_pipeline.py   # Export local JSONL
│       └── spiders/
│           └── echovita_spider.py  # Spider principal con paginación
│
├── consolidation/                  # Parte 2: SQL con DuckDB
│   ├── models.py                   # Datos SCD de ejemplo
│   └── consolidate.py              # Query de consolidación
│
├── dags/                           # Parte 3: Airflow
│   └── echovita_dag.py             # DAG orquestador
│
├── requirements.txt
└── README.md
```

---

## Instalación

```bash
# 1. Clonar el repositorio
git clone <repo-url>
cd echovita_pipeline

# 2. Crear entorno virtual
python -m venv venv

# Windows
venv\Scripts\activate

# Linux/Mac
source venv/bin/activate

# 3. Instalar dependencias
pip install -r requirements.txt
```

---

## Parte 1 — Web Scraping

### Ejecutar el spider completo
```bash
cd scraper
scrapy crawl echovita
```

### Ejecutar con límite de items (prueba rápida)
```bash
scrapy crawl echovita -s CLOSESPIDER_ITEMCOUNT=10
```

### Output
- **JSONL local**: `scraper/obituaries.jsonl` — un obituario por línea
- **S3 mock**: logs de upload simulado en consola
- **GCS mock**: logs de upload simulado en consola

### Campos extraídos
| Campo | Tipo | Descripción |
|-------|------|-------------|
| `full_name` | string | Nombre completo del fallecido |
| `date_of_birth` | string \| null | Fecha de nacimiento (ISO 8601) |
| `date_of_death` | string \| null | Fecha de fallecimiento (ISO 8601) |
| `obituary_text` | string \| null | Texto completo del obituario |
| `source_url` | string | URL de origen |
| `scraped_at` | string | Timestamp de extracción (UTC) |

### Ejemplo de output JSONL
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

Consolida una tabla histórica SCD Type 2 en una vista resumen por persona.

### Ejecutar
```bash
cd echovita_pipeline
python -m consolidation.consolidate
```

### Output esperado
```
person_id   distinct_cities   first_city   last_city   last_non_null_city
──────────────────────────────────────────────────────────────────────────
1           2                 Houston      None        Dallas
2           1                 San          San         San
3           0                 None         None        None
```

### Lógica SQL
- `distinct_cities`: `COUNT DISTINCT` de ciudades no nulas
- `first_city`: ciudad del registro con `valid_from` más antiguo
- `last_city`: ciudad del registro con `valid_from` más reciente (puede ser null)
- `last_non_null_city`: ciudad más reciente ignorando nulls

---

## Parte 3 — Airflow DAG

### Configuración
| Parámetro | Valor |
|-----------|-------|
| `dag_id` | `echovita_pipeline` |
| `schedule` | `0 8 * * *` (8:00 AM UTC diario) |
| `retries` | 3 |
| `retry_delay` | 5 minutos |
| `catchup` | False |

### Flujo de tareas
```
scrape_echovita
      │
      ▼
validate_s3_uploads
      │
      ▼
validate_jsonl_export
      │
      ▼
consolidate_scd
```

### Iniciar Airflow localmente
```bash
export AIRFLOW_HOME=~/airflow
airflow db init
airflow dags list
airflow dags test echovita_pipeline 2026-03-11
```

---

## Decisiones de diseño

### Strategy Pattern para Storage
`S3Pipeline` y `GCSPipeline` heredan de `StoragePipeline` (abstracta).
Para agregar un nuevo destino (Azure Blob, SFTP), basta con crear una nueva
clase que implemente `_upload()`. No se modifica ningún código existente.

### Separación de capas
- **Spider**: solo extrae HTML, no sabe nada de storage
- **Pipelines**: transforman y almacenan, no saben nada de HTML
- **Consolidation**: opera sobre datos limpios, no sabe nada de scraping

### Idempotencia
- JSONL abre en modo `write` — cada ejecución sobreescribe
- DuckDB opera en memoria — cada ejecución parte de cero
- Airflow con `catchup=False` — no acumula runs históricos

### Mock de S3 y GCS
Los mocks replican la interfaz exacta de producción. Para ir a producción
real, solo se reemplaza el método `_upload()` en cada pipeline:
- S3: `boto3.client('s3').put_object(...)`
- GCS: `storage.Client().bucket(...).blob(...).upload_from_string(...)`
