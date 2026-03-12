"""
middlewares.py — Middlewares del spider
"""

import logging
import random
import time
from typing import Optional

from scrapy import signals
from scrapy.http import Request, Response

logger = logging.getLogger(__name__)

# En producción: cargar desde un archivo externo o servicio de UA rotation
USER_AGENTS = [
    # Chrome Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    # Chrome Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    # Firefox Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    # Firefox Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0",
    # Safari Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    # Edge Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
]


class RotateUserAgentMiddleware:
    """
    Rota el User-Agent en cada request usando un pool de navegadores reales.

  
    Activación en settings.py:
        DOWNLOADER_MIDDLEWARES = {
            'echovita.middlewares.RotateUserAgentMiddleware': 400,
        }
    """

    def __init__(self, user_agents: list[str]):
        self.user_agents = user_agents
        self._last_ua: Optional[str] = None

    @classmethod
    def from_crawler(cls, crawler):
        # Permite sobreescribir el pool de UAs desde settings.py
        configured = crawler.settings.getlist("USER_AGENTS_POOL", [])
        user_agents = configured if configured else USER_AGENTS
        middleware = cls(user_agents=user_agents)
        crawler.signals.connect(middleware.spider_opened, signal=signals.spider_opened)
        return middleware

    def spider_opened(self, spider) -> None:
        logger.info(
            f"[RotateUserAgent] Iniciado con {len(self.user_agents)} User-Agents disponibles"
        )

    def process_request(self, request: Request, spider) -> None:
        """
        Asigna un UA aleatorio a cada request.
        Evita repetir el mismo UA consecutivamente para mayor naturalidad.
        """
        pool = self.user_agents if self.user_agents else USER_AGENTS
        available = [ua for ua in pool if ua != self._last_ua]
        ua = random.choice(available if available else pool)
        request.headers["User-Agent"] = ua
        self._last_ua = ua
        logger.debug(f"[RotateUserAgent] UA asignado: {ua[:50]}...")


class LoggingMiddleware:
    """
    Loguea métricas de cada request/response para observabilidad.

   
    Activación en settings.py:
        DOWNLOADER_MIDDLEWARES = {
            'echovita.middlewares.LoggingMiddleware': 500,
        }
    """

    def process_request(self, request: Request, spider) -> None:
        """Marca el timestamp de inicio para calcular latencia."""
        request.meta["_request_start_time"] = time.time()

    def process_response(self, request: Request, response: Response, spider) -> Response:
        """Loguea métricas de la respuesta."""
        start_time = request.meta.get("_request_start_time", time.time())
        elapsed_ms = int((time.time() - start_time) * 1000)
        size_kb = len(response.body) / 1024

        log_fn = logger.warning if response.status >= 400 else logger.debug
        log_fn(
            f"[{response.status}] {elapsed_ms}ms | {size_kb:.1f}KB | {response.url[:80]}"
        )
        return response

    def process_exception(self, request: Request, exception: Exception, spider) -> None:
        """Loguea excepciones con contexto suficiente para debugging."""
        logger.error(
            f"[Exception] {type(exception).__name__}: {exception} | URL: {request.url}"
        )
