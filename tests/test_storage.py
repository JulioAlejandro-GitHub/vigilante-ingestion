from __future__ import annotations

import json
from datetime import datetime, timezone

from app.models.frame import CapturedFrame
from app.storage.local_storage import LocalFrameStorage


CAMERA_ID = "11111111-1111-1111-1111-111111111111"


def test_local_storage_persists_frame_and_metadata(tmp_path) -> None:
    frame = CapturedFrame(
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

