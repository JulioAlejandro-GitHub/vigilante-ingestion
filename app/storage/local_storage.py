from __future__ import annotations

import json
from datetime import timezone
from pathlib import Path

from app.config import format_datetime
from app.models.frame import CapturedFrame, StoredFrame


class LocalFrameStorage:
    def __init__(self, root_dir: Path | str = "storage") -> None:
        self.root_dir = Path(root_dir)

    def save(self, frame: CapturedFrame) -> StoredFrame:
        object_key = build_frame_object_key(frame)
        image_path = self.root_dir / object_key
        metadata_path = image_path.with_suffix(".json")
        image_path.parent.mkdir(parents=True, exist_ok=True)
        image_path.write_bytes(frame.image_bytes)

        frame_ref = str(image_path.resolve())
        metadata = build_frame_metadata(frame, frame_ref=frame_ref, object_key=object_key)
        metadata_path.write_text(
            json.dumps(metadata, indent=2, sort_keys=True),
            encoding="utf-8",
        )

        return StoredFrame(
            frame_ref=frame_ref,
            frame_uri=frame_ref,
            object_key=object_key,
            metadata_ref=str(metadata_path.resolve()),
            storage_backend="local",
            content_type=frame.content_type,
            size_bytes=len(frame.image_bytes),
        )


def build_frame_object_key(frame: CapturedFrame) -> str:
    captured_at = frame.captured_at.astimezone(timezone.utc)
    timestamp = captured_at.strftime("%H-%M-%S-%f")[:-3]
    date_path = captured_at.strftime("%Y/%m/%d")
    return (
        f"frames/{frame.camera_id}/{date_path}/"
        f"{timestamp}_s{frame.sample_index:06d}_f{frame.source_frame_index:06d}.jpg"
    )


def build_frame_metadata(
    frame: CapturedFrame,
    *,
    frame_ref: str,
    object_key: str,
    storage_backend: str | None = None,
    bucket: str | None = None,
) -> dict:
    metadata = {
        "camera_id": frame.camera_id,
        "captured_at": format_datetime(frame.captured_at),
        "content_type": frame.content_type,
        "frame_ref": frame_ref,
        "height": frame.height,
        "object_key": object_key,
        "sample_index": frame.sample_index,
        "size_bytes": len(frame.image_bytes),
        "source_frame_index": frame.source_frame_index,
        "source_fps": frame.source_fps,
        "source_timestamp_seconds": frame.source_timestamp_seconds,
        "source_type": frame.source_type,
        "source_uri": frame.source_uri,
        "width": frame.width,
    }
    if storage_backend:
        metadata["storage_backend"] = storage_backend
    if bucket:
        metadata["bucket"] = bucket
    return metadata
