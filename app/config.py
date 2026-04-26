from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID


DEFAULT_REPLAY_START_AT = "2026-01-01T00:00:00Z"


def load_dotenv(path: Path | str = ".env") -> None:
    env_path = Path(path)
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def parse_bool(value: str | bool | None, *, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(f"Invalid boolean value: {value!r}")


def parse_datetime(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        normalized = value.strip()
        if normalized.endswith("Z"):
            normalized = f"{normalized[:-1]}+00:00"
        parsed = datetime.fromisoformat(normalized)

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def format_datetime(value: datetime) -> str:
    utc_value = value.astimezone(timezone.utc)
    return utc_value.isoformat(timespec="milliseconds").replace("+00:00", "Z")


@dataclass(frozen=True)
class IngestionConfig:
    source_file: Path
    camera_id: str
    capture_fps: float = 1.0
    max_frames: int | None = None
    storage_backend: str = "local"
    local_storage_dir: Path = Path("storage")
    outbox_path: Path = Path("outbox/frame_ingested.jsonl")
    outbox_reset: bool = True
    replay: bool = True
    replay_start_at: datetime = parse_datetime(DEFAULT_REPLAY_START_AT)
    external_camera_key: str | None = None
    organization_id: str | None = None
    site_id: str | None = None
    source_type: str = "video_file"
    source_name: str | None = None
    event_version: str = "1.0"
    component_version: str = "0.1.0"
    instance_id: str = "local-replay"
    ffmpeg_path: str = "ffmpeg"
    ffprobe_path: str = "ffprobe"
    minio_endpoint: str = "localhost:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_bucket: str = "vigilante-frames"
    minio_secure: bool = False

    def __post_init__(self) -> None:
        if self.capture_fps <= 0:
            raise ValueError("capture_fps must be greater than zero")
        if self.max_frames is not None and self.max_frames <= 0:
            raise ValueError("max_frames must be greater than zero when provided")
        if self.storage_backend not in {"local", "minio"}:
            raise ValueError("storage_backend must be 'local' or 'minio'")
        UUID(self.camera_id)


def config_from_env() -> IngestionConfig:
    load_dotenv()
    max_frames = os.getenv("INGESTION_MAX_FRAMES")
    return IngestionConfig(
        source_file=Path(os.getenv("INGESTION_SOURCE_FILE", "samples/cam01.mp4")),
        camera_id=os.getenv("INGESTION_CAMERA_ID", "11111111-1111-1111-1111-111111111111"),
        capture_fps=float(os.getenv("INGESTION_FPS", "1")),
        max_frames=int(max_frames) if max_frames else None,
        storage_backend=os.getenv("INGESTION_STORAGE_BACKEND", "local"),
        local_storage_dir=Path(os.getenv("INGESTION_LOCAL_STORAGE_DIR", "storage")),
        outbox_path=Path(os.getenv("INGESTION_OUTBOX_PATH", "outbox/frame_ingested.jsonl")),
        outbox_reset=parse_bool(os.getenv("INGESTION_OUTBOX_RESET"), default=True),
        replay=parse_bool(os.getenv("INGESTION_REPLAY"), default=True),
        replay_start_at=parse_datetime(os.getenv("INGESTION_REPLAY_START_AT", DEFAULT_REPLAY_START_AT)),
        external_camera_key=os.getenv("INGESTION_EXTERNAL_CAMERA_KEY"),
        organization_id=os.getenv("INGESTION_ORGANIZATION_ID"),
        site_id=os.getenv("INGESTION_SITE_ID"),
        source_name=os.getenv("INGESTION_SOURCE_NAME"),
        instance_id=os.getenv("INGESTION_INSTANCE_ID", "local-replay"),
        ffmpeg_path=os.getenv("INGESTION_FFMPEG_PATH", "ffmpeg"),
        ffprobe_path=os.getenv("INGESTION_FFPROBE_PATH", "ffprobe"),
        minio_endpoint=os.getenv("INGESTION_MINIO_ENDPOINT", "localhost:9000"),
        minio_access_key=os.getenv("INGESTION_MINIO_ACCESS_KEY", "minioadmin"),
        minio_secret_key=os.getenv("INGESTION_MINIO_SECRET_KEY", "minioadmin"),
        minio_bucket=os.getenv("INGESTION_MINIO_BUCKET", "vigilante-frames"),
        minio_secure=parse_bool(os.getenv("INGESTION_MINIO_SECURE"), default=False),
    )

