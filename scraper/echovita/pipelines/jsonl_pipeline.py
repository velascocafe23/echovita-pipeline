"""
jsonl_pipeline.py — Export local en formato JSONL

JSONL (JSON Lines): un objeto JSON por línea.
Ventajas sobre JSON array:
- Streameable: se puede leer línea por línea sin cargar todo en memoria
- Appendable: se puede agregar sin parsear el archivo completo
- Compatible con Spark, DuckDB, BigQuery sin transformación

El archivo se escribe en la raíz del proyecto (configurado en settings.py).
Al cerrar el spider, se loguea el total de registros escritos.
"""

import json
import logging
import os
from datetime import datetime, timezone
from echovita.items import ObituaryItem

logger = logging.getLogger(__name__)


class JsonlPipeline:

    def open_spider(self, spider) -> None:
        self.output_path = spider.settings.get("JSONL_OUTPUT_PATH", "obituaries.jsonl")
        self.item_count = 0

        # Abrimos en modo write — cada ejecución genera un archivo limpio (idempotente)
        self.file = open(self.output_path, "w", encoding="utf-8")
        logger.info(f"[JSONL] Archivo abierto: {os.path.abspath(self.output_path)}")

    def close_spider(self, spider) -> None:
        self.file.close()
        logger.info(
            f"[JSONL] Archivo cerrado: {os.path.abspath(self.output_path)} "
            f"| {self.item_count} registros escritos"
        )

    def process_item(self, item: ObituaryItem, spider) -> ObituaryItem:
        """
        Escribe el item como una línea JSON.
        ensure_ascii=False preserva caracteres especiales (nombres con acentos, etc.)
        """
        line = json.dumps(dict(item), ensure_ascii=False)
        self.file.write(line + "\n")
        self.item_count += 1
        return item
