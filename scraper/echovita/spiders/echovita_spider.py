"""
echovita_spider.py — Spider principal de Echovita

Responsabilidad ÚNICA: navegar el sitio y extraer datos crudos.
NO sabe nada de almacenamiento — eso es trabajo de las pipelines.

Decisiones de diseño:
- Hereda de scrapy.Spider (no CrawlSpider) para control explícito de paginación
- MAX_PAGES viene de settings.py, no hardcodeado aquí
- Cada campo se extrae con método propio → fácil de mantener y testear
- Si un campo falla, retorna None sin romper el spider (nunca un crash por dato faltante)
"""

import scrapy
from datetime import datetime, timezone
from typing import Optional, Generator

from echovita.items import ObituaryItem


class EchovitaSpider(scrapy.Spider):
    name = "echovita"
    allowed_domains = ["www.echovita.com"]
    start_urls = ["https://www.echovita.com/us/obituaries"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.current_page = 1
        self.max_pages: int = 5  # sobreescrito por settings.MAX_PAGES en from_crawler

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """
        Inyectamos settings al spider en construcción.
        Patrón estándar de Scrapy para acceder a settings sin acoplamiento.
        """
        spider = super().from_crawler(crawler, *args, **kwargs)
        spider.max_pages = crawler.settings.getint("MAX_PAGES", 5)
        return spider

    # ── Parsing principal ─────────────────────────────────────────────────────

    def parse(self, response) -> Generator:
        """
        Punto de entrada. Recibe la página de listado de obituarios.
        1. Extrae los links a obituarios individuales → parse_obituary
        2. Si no alcanzamos MAX_PAGES, sigue a la página siguiente
        """
        self.logger.info(f"Scraping página {self.current_page} de {self.max_pages}: {response.url}")

        # Extraer links a obituarios individuales
        obituary_links = response.css("a.obituary-item__name::attr(href)").getall()

        # Fallback: algunos layouts usan otra estructura
        if not obituary_links:
            obituary_links = response.css(
                "div.obituary-item a::attr(href), "
                "h2.obituary__name a::attr(href), "
                "a[href*='/obituaries/']::attr(href)"
            ).getall()

        self.logger.info(f"Encontrados {len(obituary_links)} obituarios en página {self.current_page}")

        for link in obituary_links:
            full_url = response.urljoin(link)
            yield scrapy.Request(
                url=full_url,
                callback=self.parse_obituary,
                errback=self.handle_error,
            )

        # ── Paginación ────────────────────────────────────────────────────────
        if self.current_page < self.max_pages:
            next_page = self._get_next_page(response)
            if next_page:
                self.current_page += 1
                yield scrapy.Request(
                    url=next_page,
                    callback=self.parse,
                    errback=self.handle_error,
                )
            else:
                self.logger.info("No se encontró página siguiente. Fin de paginación.")
        else:
            self.logger.info(f"Alcanzado límite de {self.max_pages} páginas.")

    def parse_obituary(self, response) -> Generator:
        """
        Parsea una página individual de obituario.
        Cada campo se extrae con su propio método para facilitar mantenimiento.
        Si cualquier extracción falla, el campo queda en None (null en JSON).
        """
        item = ObituaryItem()

        item["full_name"] = self._extract_name(response)
        item["date_of_birth"] = self._extract_date_of_birth(response)
        item["date_of_death"] = self._extract_date_of_death(response)
        item["obituary_text"] = self._extract_obituary_text(response)
        item["source_url"] = response.url
        item["scraped_at"] = datetime.now(timezone.utc).isoformat()

        self.logger.debug(f"Extraído: {item.get('full_name')} | {response.url}")

        yield item

    # ── Extractores individuales ──────────────────────────────────────────────
    # Cada método intenta múltiples selectores CSS/XPath por robustez.
    # Los sitios cambian su HTML — tener fallbacks evita que el spider rompa.

    def _extract_name(self, response) -> Optional[str]:
        """
        Extrae el nombre completo del fallecido.
        Echovita usa: <p class="my-auto h1 text-white font-weight-bolder">Name</p>
        El H1 de la página incluye 'Obituary' al final — NO lo usamos.
        """
        selectors = [
            "p.my-auto.h1.text-white.font-weight-bolder::text",
            "p[class*='my-auto'][class*='h1']::text",
            "div[class*='obit-main-info'] p[class*='font-weight-bolder']::text",
        ]
        name = self._try_selectors(response, selectors)

        # Fallback: H1 limpiando el sufijo "Obituary"
        if not name:
            raw = response.css("h1::text").get()
            if raw:
                name = raw.replace(" Obituary", "").strip()

        return name

    def _extract_dates(self, response) -> tuple:
        """
        Extrae ambas fechas juntas ya que en Echovita están en el mismo bloque.
        Estructura real:
          <p class="mt-2 mb-1 text-white ...">
            <i class="fal fa-calendar-day mr-2"></i>
            August 28, 1937<span> - </span>
            March 10, 2026
            <span> (88 years old)</span>
          </p>
        Retorna (date_of_birth, date_of_death)
        """
        date_block = response.css("p[class*='mt-2'][class*='mb-1'][class*='text-white']")

        if not date_block:
            return None, None

        # Extraemos todos los textos del bloque, filtrando iconos y separadores
        texts = [
            t.strip() for t in date_block.css("::text").getall()
            if t.strip() and t.strip() not in ["-", "–", " - "]
            and "years old" not in t
        ]

        dob = self._normalize_date(texts[0]) if len(texts) > 0 else None
        dod = self._normalize_date(texts[1]) if len(texts) > 1 else None

        return dob, dod

    def _extract_date_of_birth(self, response) -> Optional[str]:
        dob, _ = self._extract_dates(response)
        return dob

    def _extract_date_of_death(self, response) -> Optional[str]:
        _, dod = self._extract_dates(response)
        return dod

    def _extract_obituary_text(self, response) -> Optional[str]:
        """
        Extrae el texto completo del obituario.
        Echovita usa divs con clase 'obit-' para el contenido principal.
        """
        selectors = [
            "div[class*='obit-text']",
            "div[class*='obituary-text']",
            "div[itemprop='description']",
            "div[class*='obit-content']",
            "section[class*='obit']",
        ]

        for selector in selectors:
            node = response.css(selector)
            if node:
                texts = node.css("*::text").getall()
                full_text = " ".join(t.strip() for t in texts if t.strip())
                if full_text:
                    return full_text

        # Fallback: buscar el párrafo más largo de la página (suele ser el obituario)
        all_paragraphs = response.css("p::text").getall()
        if all_paragraphs:
            longest = max(all_paragraphs, key=len)
            if len(longest) > 100:
                return longest.strip()

        return None

    # ── Utilidades ────────────────────────────────────────────────────────────

    def _get_next_page(self, response) -> Optional[str]:
        """
        Encuentra la URL de la página siguiente.
        Intenta múltiples patrones de paginación.
        """
        selectors = [
            "a[rel='next']::attr(href)",
            "a.pagination__next::attr(href)",
            "li.next a::attr(href)",
            "a[class*='next']::attr(href)",
        ]
        for selector in selectors:
            href = response.css(selector).get()
            if href:
                return response.urljoin(href)
        return None

    def _try_selectors(self, response, selectors: list) -> Optional[str]:
        """
        Intenta una lista de selectores CSS en orden.
        Retorna el primer resultado no vacío, o None.
        """
        for selector in selectors:
            value = response.css(selector).get()
            if value:
                return value.strip()
        return None

    def _normalize_date(self, raw: Optional[str]) -> Optional[str]:
        """
        Intenta normalizar una fecha a formato YYYY-MM-DD.
        Si no puede parsearla, retorna el string original limpio.
        Si no hay nada, retorna None.
        """
        if not raw:
            return None

        raw = raw.strip()

        # Formatos comunes en Echovita
        formats = [
            "%B %d, %Y",    # January 15, 1940
            "%b %d, %Y",    # Jan 15, 1940
            "%m/%d/%Y",     # 01/15/1940
            "%Y-%m-%d",     # 1940-01-15 (ya normalizado)
            "%d %B %Y",     # 15 January 1940
        ]

        for fmt in formats:
            try:
                parsed = datetime.strptime(raw, fmt)
                return parsed.strftime("%Y-%m-%d")
            except ValueError:
                continue

        # Si no pudo parsear, devuelve el string tal cual (mejor que null)
        self.logger.debug(f"No se pudo normalizar la fecha: '{raw}'")
        return raw

    def handle_error(self, failure) -> None:
        """Manejo centralizado de errores de request."""
        self.logger.error(
            f"Error scrapeando {failure.request.url}: {failure.value}"
        )
