"""
settings.py — Configuración central de Scrapy

"""

BOT_NAME = "echovita"
SPIDER_MODULES = ["echovita.spiders"]
NEWSPIDER_MODULE = "echovita.spiders"

# ── Comportamiento responsable hacia el servidor ──────────────────────────────
DOWNLOAD_DELAY = 1.5
RANDOMIZE_DOWNLOAD_DELAY = True
CONCURRENT_REQUESTS = 4
CONCURRENT_REQUESTS_PER_DOMAIN = 2
ROBOTSTXT_OBEY = True

# ── AutoThrottle ──────────────────────────────────────────────────────────────
# Scrapy mide la latencia real del servidor y ajusta el delay automáticamente.
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 1.5
AUTOTHROTTLE_MAX_DELAY = 10.0
AUTOTHROTTLE_TARGET_CONCURRENCY = 2.0
AUTOTHROTTLE_DEBUG = False

# ── Middlewares personalizados ────────────────────────────────────────────────
DOWNLOADER_MIDDLEWARES = {
    "scrapy.downloadermiddlewares.useragent.UserAgentMiddleware": None,
    "echovita.middlewares.RotateUserAgentMiddleware": 400,
    "echovita.middlewares.LoggingMiddleware": 500,
}

USER_AGENTS_POOL = []  # vacío = usar pool por defecto de middlewares.py

# ── Headers base ──────────────────────────────────────────────────────────────
DEFAULT_REQUEST_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
}

# ── Pipelines activas y su orden de ejecución ────────────────────────────────
ITEM_PIPELINES = {
    "echovita.pipelines.validation_pipeline.ValidationPipeline": 100,
    "echovita.pipelines.s3_pipeline.S3Pipeline": 200,
    "echovita.pipelines.gcs_pipeline.GCSPipeline": 300,
    "echovita.pipelines.jsonl_pipeline.JsonlPipeline": 400,
}

# ── Configuración de storage (mocked) ────────────────────────────────────────
S3_BUCKET_NAME = "echovita-obituaries-mock"
S3_PREFIX = "raw/echovita/"

GCS_BUCKET_NAME = "echovita-obituaries-mock"
GCS_PREFIX = "raw/echovita/"

# ── Output local ─────────────────────────────────────────────────────────────
JSONL_OUTPUT_PATH = "obituaries.jsonl"   # raíz del proyecto

# ── Logging ──────────────────────────────────────────────────────────────────
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s [%(name)s] %(levelname)s: %(message)s"

# ── Paginación ───────────────────────────────────────────────────────────────
MAX_PAGES = 5   # máximo de páginas hacia atrás desde la principal

# ── Desactivar telemetría de Scrapy ──────────────────────────────────────────
TELNETCONSOLE_ENABLED = False
REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"
