"""
base.py — Clase abstracta para pipelines de storage

Patrón Strategy: define el contrato que deben cumplir S3, GCS, y cualquier
otro destino futuro. Para agregar Azure Blob Storage, solo creas una nueva
clase que herede de StoragePipeline e implementes _upload().

Ventajas:
- Scrapy no necesita saber qué backend de storage se usa
- Los mocks y las implementaciones reales son intercambiables
- Tests unitarios pueden usar un MockStorage sin tocar infraestructura real
"""

import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any

from echovita.items import ObituaryItem

logger = logging.getLogger(__name__)


class StoragePipeline(ABC):
    """
    Contrato base para todas las pipelines de almacenamiento en la nube.
    Subclases deben implementar únicamente `_upload()`.
    """

    def open_spider(self, spider) -> None:
        """Inicialización al abrir el spider. Sobreescribir si se necesita conexión."""
        logger.info(f"{self.__class__.__name__} iniciada")

    def close_spider(self, spider) -> None:
        """Limpieza al cerrar el spider. Sobreescribir si se necesita flush/close."""
        logger.info(f"{self.__class__.__name__} cerrada")

    def process_item(self, item: ObituaryItem, spider) -> ObituaryItem:
        """
        Punto de entrada de Scrapy. Serializa el item y delega a _upload().
        No sobreescribir en subclases — sobreescribir _upload().
        """
        try:
            payload = self._serialize(item)
            key = self._build_key(item)
            self._upload(key=key, payload=payload)
            logger.info(f"{self.__class__.__name__} | uploaded: {item.get('full_name')} → {key}")
        except Exception as e:
            logger.error(f"{self.__class__.__name__} | error uploading {item.get('full_name')}: {e}")
        return item  # siempre retorna el item para que continúe el pipeline

    @abstractmethod
    def _upload(self, key: str, payload: str) -> None:
        """
        Implementa la lógica de upload al destino específico.
        - key: ruta/nombre del archivo en el bucket
        - payload: contenido JSON serializado del item
        """
        ...

    def _serialize(self, item: ObituaryItem) -> str:
        """Serializa el item a JSON con indentación para legibilidad."""
        return json.dumps(dict(item), ensure_ascii=False, indent=2)

    def _build_key(self, item: ObituaryItem) -> str:
        """
        Construye la clave del objeto en el bucket.
        Formato: raw/echovita/YYYY/MM/DD/<nombre_normalizado>.json
        Esto facilita particionado y queries en Athena/BigQuery.
        """
        scraped_at = item.get("scraped_at", datetime.now(timezone.utc).isoformat())

        # Parsear la fecha para construir el path particionado
        try:
            dt = datetime.fromisoformat(scraped_at.replace("Z", "+00:00"))
            date_path = dt.strftime("%Y/%m/%d")
        except (ValueError, AttributeError):
            date_path = datetime.now(timezone.utc).strftime("%Y/%m/%d")

        # Normalizar el nombre para usarlo como filename
        name = item.get("full_name", "unknown")
        safe_name = name.lower().replace(" ", "_").replace("/", "-")[:50]

        # Añadir timestamp para garantizar unicidad
        ts = datetime.now(timezone.utc).strftime("%H%M%S%f")

        return f"raw/echovita/{date_path}/{safe_name}_{ts}.json"
