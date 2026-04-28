from __future__ import annotations

import io
import json
import logging
from typing import Any

from app.config import format_datetime
from app.models.frame import CapturedFrame, StoredFrame
from app.storage.local_storage import build_frame_metadata, build_frame_object_key

logger = logging.getLogger(__name__)


class MinioFrameStorage:
    def __init__(
        self,
        *,
        endpoint: str,
        access_key: str,
        secret_key: str,
        bucket: str,
        secure: bool = False,
        client: Any | None = None,
        backend_name: str = "minio",
        ensure_bucket: bool = True,
    ) -> None:
        if client is None:
            try:
                from minio import Minio
                from minio.error import S3Error
            except ImportError as exc:
                raise RuntimeError("MinIO/S3 storage requires the 'minio' package") from exc

            self._s3_error = S3Error
            client = Minio(
                endpoint,
                access_key=access_key,
                secret_key=secret_key,
                secure=secure,
            )
        else:
            self._s3_error = None
        self.client = client
        self.bucket = bucket
        self.backend_name = backend_name
        self.endpoint = endpoint
        if ensure_bucket:
            self._ensure_bucket()

    def save(self, frame: CapturedFrame) -> StoredFrame:
        object_key = build_frame_object_key(frame)
        metadata_key = object_key.rsplit(".", 1)[0] + ".json"

        frame_ref = f"s3://{self.bucket}/{object_key}"
        metadata = build_frame_metadata(
            frame,
            frame_ref=frame_ref,
            object_key=object_key,
            storage_backend=self.backend_name,
            bucket=self.bucket,
        )
        metadata_bytes = json.dumps(metadata, indent=2, sort_keys=True).encode("utf-8")
        try:
            self.client.put_object(
                self.bucket,
                object_key,
                io.BytesIO(frame.image_bytes),
                length=len(frame.image_bytes),
                content_type=frame.content_type,
                metadata={
                    "camera-id": frame.camera_id,
                    "captured-at": format_datetime(frame.captured_at),
                    "object-key": object_key,
                    "sample-index": str(frame.sample_index),
                    "source-frame-index": str(frame.source_frame_index),
                    "source-type": frame.source_type,
                    "storage-backend": self.backend_name,
                },
            )
            self.client.put_object(
                self.bucket,
                metadata_key,
                io.BytesIO(metadata_bytes),
                length=len(metadata_bytes),
                content_type="application/json",
                metadata={
                    "camera-id": frame.camera_id,
                    "object-key": metadata_key,
                    "storage-backend": self.backend_name,
                },
            )
        except Exception as exc:
            logger.exception(
                "frame_upload_failed storage_backend=%s endpoint=%s bucket=%s object_key=%s error_type=%s",
                self.backend_name,
                self.endpoint,
                self.bucket,
                object_key,
                type(exc).__name__,
            )
            raise

        logger.info(
            "frame_uploaded storage_backend=%s bucket=%s object_key=%s size_bytes=%s",
            self.backend_name,
            self.bucket,
            object_key,
            len(frame.image_bytes),
        )
        return StoredFrame(
            frame_ref=frame_ref,
            frame_uri=frame_ref,
            object_key=object_key,
            metadata_ref=f"s3://{self.bucket}/{metadata_key}",
            storage_backend=self.backend_name,
            content_type=frame.content_type,
            size_bytes=len(frame.image_bytes),
        )

    def _ensure_bucket(self) -> None:
        try:
            if not self.client.bucket_exists(self.bucket):
                self.client.make_bucket(self.bucket)
                logger.info(
                    "frame_storage_bucket_created storage_backend=%s endpoint=%s bucket=%s",
                    self.backend_name,
                    self.endpoint,
                    self.bucket,
                )
        except Exception as exc:
            logger.exception(
                "frame_storage_bucket_check_failed storage_backend=%s endpoint=%s bucket=%s error_type=%s",
                self.backend_name,
                self.endpoint,
                self.bucket,
                type(exc).__name__,
            )
            raise
