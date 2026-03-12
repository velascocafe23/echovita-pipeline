"""
validation_pipeline.py — Limpieza y validación de items

Primera pipeline en ejecutarse (prioridad 100 en settings.py).
Responsabilidades:
1. Filtrar items que no son obituarios reales (páginas de ciudad/estado)
2. Garantizar que full_name nunca sea None
3. Normalizar campos opcionales a None explícito
4. Asegurar scraped_at siempre presente

Si un item no pasa la validación, lanza DropItem para descartarlo.
Las pipelines siguientes (S3, GCS, JSONL) nunca ven items inválidos.
"""

import logging
from datetime import datetime, timezone

import scrapy
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem

from echovita.items import ObituaryItem

logger = logging.getLogger(__name__)


class ValidationPipeline:

    def process_item(self, item: ObituaryItem, spider) -> ObituaryItem:
        adapter = ItemAdapter(item)

        # ── Filtrar páginas que no son obituarios individuales ────────────────
        # Echovita devuelve links a páginas de estado/ciudad además de obituarios.
        # Las identificamos porque su URL no contiene un ID numérico al final
        # y su full_name termina en "Obituaries" (plural)
        name = adapter.get("full_name", "") or ""
        url = adapter.get("source_url", "") or ""

        if name.endswith("Obituaries") and not any(
            char.isdigit() for char in url.split("/")[-1]
        ):
            raise DropItem(f"Página de listado descartada: {name} | {url}")

        # ── Garantizar full_name ──────────────────────────────────────────────
        if not name.strip():
            raise DropItem(f"Item sin nombre descartado: {url}")

        # ── Normalizar campos opcionales a None explícito ─────────────────────
        for field in ["date_of_birth", "date_of_death", "obituary_text"]:
            if field not in adapter or adapter.get(field) == "":
                adapter[field] = None

        # ── Garantizar scraped_at ─────────────────────────────────────────────
        if not adapter.get("scraped_at"):
            adapter["scraped_at"] = datetime.now(timezone.utc).isoformat()

        return item
