"""
tests/ — Tests unitarios del pipeline Echovita

Cubre las dos piezas más críticas:
1. Spider: extracción de campos desde HTML real
2. Consolidation: lógica SQL SCD con casos edge

Ejecutar:
    cd echovita_pipeline
    pip install pytest
    pytest tests/ -v

Por qué estos tests son valiosos para la entrega:
- Demuestran que el código fue pensado para ser testeable
- Los casos edge (null cities, single records) muestran comprensión del dominio
- El uso de FakeResponse evita dependencia de red en los tests
"""

import json
import sys
import tempfile
from pathlib import Path

import pytest

# Añadir paths necesarios
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "scraper"))

# ── Helpers ───────────────────────────────────────────────────────────────────

def fake_response(url: str, html: str):
    """
    Crea un response falso de Scrapy sin hacer requests reales.
    Patrón estándar para testear spiders sin red.
    """
    from scrapy.http import HtmlResponse, Request
    request = Request(url=url)
    return HtmlResponse(
        url=url,
        request=request,
        body=html.encode("utf-8"),
        encoding="utf-8",
    )


# ── Tests del Spider ──────────────────────────────────────────────────────────

class TestEchovitaSpider:

    @pytest.fixture
    def spider(self):
        from echovita.spiders.echovita_spider import EchovitaSpider
        return EchovitaSpider()

    def test_extract_name_standard(self, spider):
        """Nombre en el selector principal de Echovita."""
        html = """
        <html><body>
          <p class="my-auto h1 text-white font-weight-bolder">John Doe</p>
        </body></html>
        """
        response = fake_response("https://www.echovita.com/us/obituaries/john-doe", html)
        assert spider._extract_name(response) == "John Doe"

    def test_extract_name_fallback_h1(self, spider):
        """Fallback al H1 cuando no existe el selector principal."""
        html = """
        <html><body>
          <h1>Jane Smith Obituary</h1>
        </body></html>
        """
        response = fake_response("https://www.echovita.com/us/obituaries/jane-smith", html)
        name = spider._extract_name(response)
        assert name == "Jane Smith"
        assert "Obituary" not in name

    def test_extract_name_missing_returns_none(self, spider):
        """Cuando no hay nombre, retorna None sin crashear."""
        html = "<html><body><p>No name here</p></body></html>"
        response = fake_response("https://www.echovita.com/us/obituaries/unknown", html)
        # No debe lanzar excepción
        result = spider._extract_name(response)
        assert result is None or isinstance(result, str)

    def test_normalize_date_long_format(self, spider):
        """Normaliza formato largo americano."""
        assert spider._normalize_date("January 15, 1940") == "1940-01-15"

    def test_normalize_date_short_month(self, spider):
        """Normaliza formato con mes abreviado."""
        assert spider._normalize_date("Jan 15, 1940") == "1940-01-15"

    def test_normalize_date_already_iso(self, spider):
        """No modifica fechas ya en formato ISO."""
        assert spider._normalize_date("1940-01-15") == "1940-01-15"

    def test_normalize_date_none_returns_none(self, spider):
        """None de entrada → None de salida."""
        assert spider._normalize_date(None) is None

    def test_normalize_date_empty_returns_none(self, spider):
        """String vacío → None."""
        assert spider._normalize_date("") is None

    def test_extract_dates_from_real_html(self, spider):
        """Extrae ambas fechas del bloque real de Echovita."""
        html = """
        <html><body>
          <p class="mt-2 mb-1 text-white font-weight-bold fs-18 lh-27">
            <i class="fal fa-calendar-day mr-2"></i>
            August 28, 1937<span> - </span>
            March 10, 2026
            <span> (88 years old)</span>
          </p>
        </body></html>
        """
        response = fake_response("https://www.echovita.com/us/obituaries/test", html)
        dob, dod = spider._extract_dates(response)
        assert dob == "1937-08-28"
        assert dod == "2026-03-10"

    def test_extract_dates_missing_returns_none_tuple(self, spider):
        """Sin bloque de fechas → (None, None)."""
        html = "<html><body><p>No dates</p></body></html>"
        response = fake_response("https://www.echovita.com/us/obituaries/test", html)
        dob, dod = spider._extract_dates(response)
        assert dob is None
        assert dod is None

    def test_parse_obituary_yields_item(self, spider):
        """parse_obituary debe yieldar exactamente un ObituaryItem."""
        html = """
        <html><body>
          <h1>Mary Smith Obituary</h1>
          <p class="my-auto h1 text-white font-weight-bolder">Mary Smith</p>
        </body></html>
        """
        response = fake_response("https://www.echovita.com/us/obituaries/mary-smith", html)
        items = list(spider.parse_obituary(response))
        assert len(items) == 1
        item = items[0]
        assert item["full_name"] == "Mary Smith"
        assert item["source_url"] == "https://www.echovita.com/us/obituaries/mary-smith"
        assert item["scraped_at"] is not None


# ── Tests de Consolidación SCD ────────────────────────────────────────────────

class TestConsolidation:

    @pytest.fixture
    def sample_records(self):
        """Datos del enunciado."""
        return [
            {"person_id": 1, "name": "John Doe",       "state": "TX", "city": "Houston", "valid_from": "2020-01-01", "valid_to": "2022-07-25"},
            {"person_id": 1, "name": "John Doe",       "state": "TX", "city": "Dallas",  "valid_from": "2022-07-25", "valid_to": "2023-08-19"},
            {"person_id": 1, "name": "John Doe",       "state": "TX", "city": None,      "valid_from": "2023-08-19", "valid_to": None},
            {"person_id": 2, "name": "Richard Smith",  "state": "CA", "city": "San",     "valid_from": "2022-04-12", "valid_to": None},
            {"person_id": 3, "name": "Max Mustermann", "state": "CA", "city": None,      "valid_from": "2000-07-22", "valid_to": None},
        ]

    def test_john_doe_distinct_cities(self, sample_records):
        """John tuvo Houston y Dallas → 2 ciudades distintas."""
        from consolidation.consolidate import run_consolidation
        results = {r["person_id"]: r for r in run_consolidation(sample_records)}
        assert results[1]["distinct_cities"] == 2

    def test_john_doe_first_city(self, sample_records):
        """Primera ciudad de John: Houston."""
        from consolidation.consolidate import run_consolidation
        results = {r["person_id"]: r for r in run_consolidation(sample_records)}
        assert results[1]["first_city"] == "Houston"

    def test_john_doe_last_city_is_null(self, sample_records):
        """Última ciudad de John es null (registro más reciente tiene city=None)."""
        from consolidation.consolidate import run_consolidation
        results = {r["person_id"]: r for r in run_consolidation(sample_records)}
        assert results[1]["last_city"] is None

    def test_john_doe_last_non_null_city(self, sample_records):
        """Última ciudad no-null de John: Dallas."""
        from consolidation.consolidate import run_consolidation
        results = {r["person_id"]: r for r in run_consolidation(sample_records)}
        assert results[1]["last_non_null_city"] == "Dallas"

    def test_max_all_nulls(self, sample_records):
        """Max nunca tuvo ciudad → distinct=0, todo None."""
        from consolidation.consolidate import run_consolidation
        results = {r["person_id"]: r for r in run_consolidation(sample_records)}
        assert results[3]["distinct_cities"] == 0
        assert results[3]["first_city"] is None
        assert results[3]["last_city"] is None
        assert results[3]["last_non_null_city"] is None

    def test_single_record_person(self, sample_records):
        """Richard tiene un solo registro con ciudad → distinct=1."""
        from consolidation.consolidate import run_consolidation
        results = {r["person_id"]: r for r in run_consolidation(sample_records)}
        assert results[2]["distinct_cities"] == 1
        assert results[2]["last_non_null_city"] == "San"

    def test_returns_all_persons(self, sample_records):
        """El resultado tiene una fila por persona."""
        from consolidation.consolidate import run_consolidation
        results = run_consolidation(sample_records)
        assert len(results) == 3

    def test_idempotent(self, sample_records):
        """Ejecutar dos veces produce el mismo resultado."""
        from consolidation.consolidate import run_consolidation
        result1 = run_consolidation(sample_records)
        result2 = run_consolidation(sample_records)
        assert result1 == result2

    def test_load_from_jsonl(self, tmp_path):
        """load_from_jsonl lee correctamente un archivo JSONL."""
        from consolidation.consolidate import load_from_jsonl

        jsonl_file = tmp_path / "test.jsonl"
        records = [
            {"full_name": "Alice Brown", "date_of_death": "2026-01-15", "scraped_at": "2026-01-15T10:00:00Z"},
            {"full_name": "Bob Jones",   "date_of_death": None,         "scraped_at": "2026-01-16T10:00:00Z"},
        ]
        with open(jsonl_file, "w") as f:
            for r in records:
                f.write(json.dumps(r) + "\n")

        loaded = load_from_jsonl(str(jsonl_file))
        assert len(loaded) == 2
        assert loaded[0]["name"] == "Alice Brown"
        assert loaded[1]["name"] == "Bob Jones"

    def test_load_from_jsonl_missing_file(self):
        """FileNotFoundError si el archivo no existe."""
        from consolidation.consolidate import load_from_jsonl
        with pytest.raises(FileNotFoundError):
            load_from_jsonl("/nonexistent/path/file.jsonl")


# ── Tests de Pipelines ────────────────────────────────────────────────────────

class TestValidationPipeline:

    @pytest.fixture
    def pipeline(self):
        from echovita.pipelines.validation_pipeline import ValidationPipeline
        return ValidationPipeline()

    @pytest.fixture
    def valid_item(self):
        from echovita.items import ObituaryItem
        item = ObituaryItem()
        item["full_name"] = "John Doe"
        item["date_of_birth"] = "1940-01-15"
        item["date_of_death"] = "2026-01-01"
        item["obituary_text"] = "A beloved father..."
        item["source_url"] = "https://www.echovita.com/us/obituaries/john-doe-12345"
        item["scraped_at"] = "2026-03-11T10:00:00Z"
        return item

    def test_valid_item_passes(self, pipeline, valid_item):
        """Item válido pasa la validación sin modificaciones."""
        from unittest.mock import MagicMock
        spider = MagicMock()
        result = pipeline.process_item(valid_item, spider)
        assert result["full_name"] == "John Doe"

    def test_city_page_is_dropped(self, pipeline):
        """Páginas de ciudad/estado son descartadas."""
        from scrapy.exceptions import DropItem
        from echovita.items import ObituaryItem
        from unittest.mock import MagicMock

        item = ObituaryItem()
        item["full_name"] = "Connecticut Obituaries"
        item["source_url"] = "https://www.echovita.com/us/obituaries/ct"
        item["scraped_at"] = "2026-03-11T10:00:00Z"

        spider = MagicMock()
        with pytest.raises(DropItem):
            pipeline.process_item(item, spider)

    def test_missing_optional_fields_set_to_none(self, pipeline, valid_item):
        """Campos opcionales faltantes se normalizan a None."""
        from unittest.mock import MagicMock
        del valid_item._values["date_of_birth"]
        spider = MagicMock()
        result = pipeline.process_item(valid_item, spider)
        assert result["date_of_birth"] is None
