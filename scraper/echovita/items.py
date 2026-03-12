"""
items.py — Modelo de datos del scraper

Define la estructura de un obituario extraído de Echovita.
Usamos scrapy.Item en lugar de dataclasses o dicts simples por tres razones:
  1. Scrapy valida automáticamente que no se asignen campos no declarados
  2. Las pipelines reciben un tipo conocido, no un dict genérico
  3. Facilita serialización/deserialización nativa de Scrapy

Si en el futuro se agrega una nueva fuente (ej: legacy.com), basta con
crear un nuevo Item que herede de ObituaryItem o implemente la misma interfaz.
"""

import scrapy
from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime, timezone


class ObituaryItem(scrapy.Item):
    """
    Representa un obituario scrapeado.
    Todos los campos son opcionales en runtime — si no se pueden extraer,
    la pipeline asignará None (mapeado a null en JSON).
    """

    # Identificación de la persona
    full_name: str = scrapy.Field()       # Nombre completo del fallecido
    date_of_birth: Optional[str] = scrapy.Field()   # ISO 8601: YYYY-MM-DD o null
    date_of_death: Optional[str] = scrapy.Field()   # ISO 8601: YYYY-MM-DD o null
    obituary_text: Optional[str] = scrapy.Field()   # Texto completo del obituario

    # Metadatos de trazabilidad — buena práctica para pipelines de producción
    source_url: str = scrapy.Field()      # URL de origen del obituario
    scraped_at: str = scrapy.Field()      # Timestamp de extracción (UTC ISO 8601)


@dataclass
class ObituaryRecord:
    """
    Representación Python pura del obituario, desacoplada de Scrapy.
    Se usa en la capa de consolidación (DuckDB) y en tests unitarios
    sin necesidad de instanciar un Item de Scrapy.

    Separar Item (capa scraping) de Record (capa de datos) es clave
    para la extensibilidad: si mañana reemplazamos Scrapy por Playwright,
    la capa de consolidación no cambia.
    """
    full_name: str
    date_of_birth: Optional[str] = None
    date_of_death: Optional[str] = None
    obituary_text: Optional[str] = None
    source_url: str = ""
    scraped_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    @classmethod
    def from_scrapy_item(cls, item: ObituaryItem) -> "ObituaryRecord":
        """Convierte un ObituaryItem de Scrapy a un ObituaryRecord tipado."""
        return cls(
            full_name=item.get("full_name", ""),
            date_of_birth=item.get("date_of_birth"),
            date_of_death=item.get("date_of_death"),
            obituary_text=item.get("obituary_text"),
            source_url=item.get("source_url", ""),
            scraped_at=item.get("scraped_at", datetime.now(timezone.utc).isoformat()),
        )

    def to_dict(self) -> dict:
        """Serialización explícita para JSON/JSONL export."""
        return {
            "full_name": self.full_name,
            "date_of_birth": self.date_of_birth,
            "date_of_death": self.date_of_death,
            "obituary_text": self.obituary_text,
            "source_url": self.source_url,
            "scraped_at": self.scraped_at,
        }
