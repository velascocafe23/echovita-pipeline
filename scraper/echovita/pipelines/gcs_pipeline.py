"""
gcs_pipeline.py — Pipeline de upload a Google Cloud Storage (implementación mock)

Idéntica en estructura a S3Pipeline pero simulando la API de GCS.

Para pasar a producción, reemplaza _upload() con:
    blob = self.bucket.blob(key)
    blob.upload_from_string(payload, content_type="application/json")
"""

import logging
from echovita.pipelines.base import StoragePipeline

logger = logging.getLogger(__name__)


class GCSPipeline(StoragePipeline):
    """
    Mock de Google Cloud Storage.
    En producción: reemplazar _upload() con llamada a google-cloud-storage.
    """

    def open_spider(self, spider) -> None:
        self.bucket_name = spider.settings.get("GCS_BUCKET_NAME", "echovita-mock")
        self._uploaded_blobs: list[dict] = []
        logger.info(f"[GCS MOCK] Conectado al bucket: gs://{self.bucket_name}")

    def close_spider(self, spider) -> None:
        count = len(self._uploaded_blobs)
        logger.info(f"[GCS MOCK] Spider cerrado. Total blobs subidos: {count}")

    def _upload(self, key: str, payload: str) -> None:
        """Mock de blob.upload_from_string()."""
        record = {
            "bucket": self.bucket_name,
            "blob": key,
            "size_bytes": len(payload.encode("utf-8")),
            "content_type": "application/json",
        }
        self._uploaded_blobs.append(record)
        logger.info(f"[GCS MOCK] UPLOAD gs://{self.bucket_name}/{key} ({record['size_bytes']} bytes)")

    def get_uploaded_blobs(self) -> list[dict]:
        """Expone los uploads para verificación en tests y Airflow."""
        return self._uploaded_blobs.copy()
