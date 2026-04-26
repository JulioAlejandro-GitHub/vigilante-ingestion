from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass(frozen=True)
class VideoMetadata:
    source_file: Path
    width: int
    height: int
    fps: float
    duration_seconds: float | None
    frame_count: int | None


@dataclass(frozen=True)
class CapturedFrame:
    image_bytes: bytes
    camera_id: str
    captured_at: datetime
    width: int
    height: int
    content_type: str
    source_type: str
    source_uri: str
    source_timestamp_seconds: float
    source_frame_index: int
    sample_index: int
    capture_fps: float
    source_fps: float


@dataclass(frozen=True)
class StoredFrame:
    frame_ref: str
    frame_uri: str
    object_key: str
    metadata_ref: str
    storage_backend: str
    content_type: str
    size_bytes: int

