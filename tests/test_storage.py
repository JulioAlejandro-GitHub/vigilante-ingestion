from __future__ import annotations

import json
from datetime import datetime, timezone

from app.models.frame import CapturedFrame
from app.storage.local_storage import LocalFrameStorage
from app.storage.minio_storage import MinioFrameStorage
from app.storage.s3_storage import S3FrameStorage


CAMERA_ID = "11111111-1111-1111-1111-111111111111"


def test_local_storage_persists_frame_and_metadata(tmp_path) -> None:
    frame = _frame()
    storage = LocalFrameStorage(tmp_path / "storage")

    stored = storage.save(frame)

    assert stored.storage_backend == "local"
    assert stored.frame_ref.endswith(".jpg")
    assert stored.object_key == (
        f"frames/{CAMERA_ID}/2026/01/01/00-00-00-000_s000000_f000000.jpg"
    )
    metadata = json.loads((tmp_path / "storage" / stored.object_key).with_suffix(".json").read_text())
    assert metadata["camera_id"] == CAMERA_ID
    assert metadata["width"] == 160


def test_minio_storage_uploads_frame_metadata_and_returns_s3_uri() -> None:
    client = _FakeMinioClient()
    storage = MinioFrameStorage(
        endpoint="localhost:9000",
        access_key="minio",
        secret_key="minio123",
        bucket="vigilante-frames",
        client=client,
    )

    stored = storage.save(_frame())

    expected_key = f"frames/{CAMERA_ID}/2026/01/01/00-00-00-000_s000000_f000000.jpg"
    assert stored.storage_backend == "minio"
    assert stored.object_key == expected_key
    assert stored.frame_ref == f"s3://vigilante-frames/{expected_key}"
    assert stored.frame_uri == stored.frame_ref
    assert stored.metadata_ref == f"s3://vigilante-frames/{expected_key[:-4]}.json"
    assert client.bucket_exists_calls == ["vigilante-frames"]
    assert client.created_buckets == ["vigilante-frames"]
    image_upload = client.objects[("vigilante-frames", expected_key)]
    metadata_upload = client.objects[("vigilante-frames", expected_key[:-4] + ".json")]
    assert image_upload["body"] == b"\xff\xd8test-jpeg\xff\xd9"
    assert image_upload["content_type"] == "image/jpeg"
    assert image_upload["metadata"]["camera-id"] == CAMERA_ID
    metadata = json.loads(metadata_upload["body"].decode("utf-8"))
    assert metadata["bucket"] == "vigilante-frames"
    assert metadata["storage_backend"] == "minio"
    assert metadata["object_key"] == expected_key


def test_s3_storage_uses_same_deterministic_object_model() -> None:
    client = _FakeMinioClient(bucket_exists=True)
    storage = S3FrameStorage(
        endpoint="s3-compatible.local:9000",
        access_key="access",
        secret_key="secret",
        bucket="evidence",
        client=client,
    )

    stored = storage.save(_frame())

    assert stored.storage_backend == "s3"
    assert stored.frame_uri.startswith("s3://evidence/frames/")
    assert client.created_buckets == []


def _frame() -> CapturedFrame:
    return CapturedFrame(
        image_bytes=b"\xff\xd8test-jpeg\xff\xd9",
        camera_id=CAMERA_ID,
        captured_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        width=160,
        height=90,
        content_type="image/jpeg",
        source_type="video_file",
        source_uri="samples/cam01.mp4",
        source_timestamp_seconds=0,
        source_frame_index=0,
        sample_index=0,
        capture_fps=1,
        source_fps=10,
    )


class _FakeMinioClient:
    def __init__(self, *, bucket_exists: bool = False) -> None:
        self._bucket_exists = bucket_exists
        self.bucket_exists_calls: list[str] = []
        self.created_buckets: list[str] = []
        self.objects: dict[tuple[str, str], dict] = {}

    def bucket_exists(self, bucket: str) -> bool:
        self.bucket_exists_calls.append(bucket)
        return self._bucket_exists or bucket in self.created_buckets

    def make_bucket(self, bucket: str) -> None:
        self.created_buckets.append(bucket)

    def put_object(
        self,
        bucket: str,
        object_key: str,
        data,
        *,
        length: int,
        content_type: str,
        metadata: dict | None = None,
    ) -> None:
        body = data.read()
        assert len(body) == length
        self.objects[(bucket, object_key)] = {
            "body": body,
            "content_type": content_type,
            "metadata": metadata or {},
        }
