from __future__ import annotations

from app.storage.minio_storage import MinioFrameStorage


class S3FrameStorage(MinioFrameStorage):
    """S3-compatible frame storage using the same object model as MinIO."""

    def __init__(self, **kwargs) -> None:
        kwargs.setdefault("backend_name", "s3")
        super().__init__(**kwargs)
