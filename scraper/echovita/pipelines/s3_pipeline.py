"""
s3_pipeline.py — Pipeline de upload a AWS S3 (implementación mock)

El mock simula el comportamiento real de S3:
- Genera el key path exactamente igual que lo haría en producción
- Registra cada upload en un log interno (útil para tests y auditoría)
- La interfaz es idéntica a la implementación real con boto3

Para pasar a producción, solo reemplaza _upload() con:
    self.s3_client.put_object(
        Bucket=self.bucket_name,
        Key=key,
        Body=payload,
        ContentType="application/json"
    )
"""

import logging
from echovita.pipelines.base import StoragePipeline

logger = logging.getLogger(__name__)


class S3Pipeline(StoragePipeline):
    """
    Mock de AWS S3. Simula uploads sin credenciales ni conexión real.
    En producción: reemplazar _upload() con llamada a boto3.
    """

    def open_spider(self, spider) -> None:
        self.bucket_name = spider.settings.get("S3_BUCKET_NAME", "echovita-mock")
        self._uploaded_objects: list[dict] = []
        logger.info(f"[S3 MOCK] Conectado al bucket: s3://{self.bucket_name}")

    def close_spider(self, spider) -> None:
        count = len(self._uploaded_objects)
        logger.info(f"[S3 MOCK] Spider cerrado. Total objetos subidos: {count}")

    def _upload(self, key: str, payload: str) -> None:
        """Mock de s3_client.put_object()."""
        record = {
            "bucket": self.bucket_name,
            "key": key,
            "size_bytes": len(payload.encode("utf-8")),
        }
        self._uploaded_objects.append(record)
        logger.info(f"[S3 MOCK] PUT s3://{self.bucket_name}/{key} ({record['size_bytes']} bytes)")

    def get_uploaded_objects(self) -> list[dict]:
        """Expone los uploads para verificación en tests y Airflow."""
        return self._uploaded_objects.copy()
