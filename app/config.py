from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse
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
    camera_id: str | None = None
    capture_fps: float = 1.0
    max_frames: int | None = None
    storage_backend: str = "local"
    local_storage_dir: Path = Path("storage")
    publish_mode: str = "jsonl"
    outbox_path: Path = Path("outbox/frame_ingested.jsonl")
    outbox_reset: bool = True
    replay: bool = True
    replay_start_at: datetime = parse_datetime(DEFAULT_REPLAY_START_AT)
    external_camera_key: str | None = None
    organization_id: str | None = None
    site_id: str | None = None
    zone_id: str | None = None
    camera_runtime_config: dict[str, object] = field(default_factory=dict)
    source_type: str = "file_replay"
    source_name: str | None = None
    rtsp_url: str | None = None
    rtsp_transport: str = "tcp"
    camera_config_db_url: str | None = None
    camera_config_db_schema: str = "api"
    camera_secret_fernet_key: str | None = None
    active_camera_source: str = "db"
    active_camera_concurrency: int | None = None
    active_camera_refresh_seconds: float = 30.0
    active_camera_status_interval_seconds: float = 30.0
    active_camera_stop_timeout_seconds: float = 10.0
    active_camera_enable_health_server: bool = False
    active_camera_health_host: str = "127.0.0.1"
    active_camera_health_port: int = 8088
    rtsp_read_timeout_seconds: float = 10.0
    rtsp_reconnect_initial_delay_seconds: float = 1.0
    rtsp_reconnect_max_delay_seconds: float = 30.0
    rtsp_reconnect_backoff_multiplier: float = 2.0
    rtsp_max_reconnect_attempts: int | None = None
    event_version: str = "1.0"
    component_version: str = "0.1.0"
    instance_id: str = "local-replay"
    ffmpeg_path: str = "ffmpeg"
    ffprobe_path: str = "ffprobe"
    minio_endpoint: str = "localhost:9000"
    minio_access_key: str = "minio"
    minio_secret_key: str = "minio123"
    minio_bucket: str = "vigilante-frames"
    minio_secure: bool = False
    rabbitmq_host: str = "localhost"
    rabbitmq_port: int = 5672
    rabbitmq_user: str = "guest"
    rabbitmq_password: str = "guest"
    rabbitmq_vhost: str = "/"
    rabbitmq_frame_exchange: str = "vigilante.frames"
    rabbitmq_frame_routing_key: str = "frame.ingested"
    rabbitmq_recognition_queue: str = "vigilante.recognition.frame_ingested"
    rabbitmq_frame_dlx: str = "vigilante.frames.dlx"
    rabbitmq_frame_dlq: str = "vigilante.recognition.frame_ingested.dlq"
    rabbitmq_frame_dlq_routing_key: str = "frame.ingested.dlq"
    log_level: str = "INFO"

    def __post_init__(self) -> None:
        if self.capture_fps <= 0:
            raise ValueError("capture_fps must be greater than zero")
        if self.max_frames is not None and self.max_frames <= 0:
            raise ValueError("max_frames must be greater than zero when provided")
        if self.storage_backend not in {"local", "minio", "s3"}:
            raise ValueError("storage_backend must be 'local', 'minio' or 's3'")
        if self.publish_mode not in {"jsonl", "rabbitmq", "both"}:
            raise ValueError("publish_mode must be 'jsonl', 'rabbitmq' or 'both'")
        if self.source_type not in {"file_replay", "video_file", "rtsp", "active_cameras"}:
            raise ValueError("source_type must be 'file_replay', 'video_file', 'rtsp' or 'active_cameras'")
        if self.source_type != "active_cameras" and not self.camera_id:
            raise ValueError("camera_id is required unless source_type is 'active_cameras'")
        if self.source_type == "rtsp":
            if not self.camera_id:
                raise ValueError("camera_id is required when source_type is 'rtsp'")
            if not self.rtsp_url and not self.camera_config_db_url:
                raise ValueError("rtsp_url or camera_config_db_url is required when source_type is 'rtsp'")
            if self.rtsp_url:
                parsed = urlparse(self.rtsp_url)
                if parsed.scheme not in {"rtsp", "rtsps"} or not parsed.netloc:
                    raise ValueError("rtsp_url must be a valid rtsp:// or rtsps:// URL")
        if self.source_type in {"rtsp", "active_cameras"}:
            if self.rtsp_transport not in {"tcp", "udp"}:
                raise ValueError("rtsp_transport must be 'tcp' or 'udp'")
            if self.rtsp_read_timeout_seconds <= 0:
                raise ValueError("rtsp_read_timeout_seconds must be greater than zero")
            if self.rtsp_reconnect_initial_delay_seconds < 0:
                raise ValueError("rtsp_reconnect_initial_delay_seconds must not be negative")
            if self.rtsp_reconnect_max_delay_seconds < self.rtsp_reconnect_initial_delay_seconds:
                raise ValueError("rtsp_reconnect_max_delay_seconds must be greater than or equal to the initial delay")
            if self.rtsp_reconnect_backoff_multiplier < 1:
                raise ValueError("rtsp_reconnect_backoff_multiplier must be greater than or equal to one")
            if self.rtsp_max_reconnect_attempts is not None and self.rtsp_max_reconnect_attempts < 0:
                raise ValueError("rtsp_max_reconnect_attempts must not be negative")
        if self.source_type == "active_cameras":
            if self.active_camera_source != "db":
                raise ValueError("active_camera_source must be 'db'")
            if not self.camera_config_db_url:
                raise ValueError("camera_config_db_url is required when source_type is 'active_cameras'")
            if self.active_camera_concurrency is not None and self.active_camera_concurrency <= 0:
                raise ValueError("active_camera_concurrency must be greater than zero when provided")
            if self.active_camera_refresh_seconds <= 0:
                raise ValueError("active_camera_refresh_seconds must be greater than zero")
            if self.active_camera_status_interval_seconds <= 0:
                raise ValueError("active_camera_status_interval_seconds must be greater than zero")
            if self.active_camera_stop_timeout_seconds <= 0:
                raise ValueError("active_camera_stop_timeout_seconds must be greater than zero")
            if self.active_camera_health_port < 0 or self.active_camera_health_port > 65535:
                raise ValueError("active_camera_health_port must be between 0 and 65535")
        if self.camera_id:
            UUID(self.camera_id)


def config_from_env() -> IngestionConfig:
    load_dotenv()
    max_frames = os.getenv("INGESTION_MAX_FRAMES")
    storage_backend = os.getenv("INGESTION_STORAGE_BACKEND", "local")
    source_type = os.getenv("INGESTION_SOURCE_TYPE", "file_replay")
    replay_default = False if source_type in {"rtsp", "active_cameras"} else True
    rtsp_max_reconnect_attempts = os.getenv("INGESTION_RTSP_MAX_RECONNECT_ATTEMPTS")
    active_camera_concurrency = os.getenv("INGESTION_ACTIVE_CAMERA_CONCURRENCY")
    remote_prefixes = ("INGESTION_S3", "INGESTION_MINIO") if storage_backend == "s3" else ("INGESTION_MINIO", "INGESTION_S3")
    camera_id_default = None if source_type == "active_cameras" else "11111111-1111-1111-1111-111111111111"

    def remote_env(name: str, default: str) -> str:
        primary, fallback = remote_prefixes
        return os.getenv(f"{primary}_{name}", os.getenv(f"{fallback}_{name}", default))

    return IngestionConfig(
        source_file=Path(os.getenv("INGESTION_SOURCE_FILE", "samples/cam01.mp4")),
        camera_id=os.getenv("INGESTION_CAMERA_ID", camera_id_default),
        capture_fps=float(os.getenv("INGESTION_FPS", "1")),
        max_frames=int(max_frames) if max_frames else None,
        storage_backend=storage_backend,
        local_storage_dir=Path(os.getenv("INGESTION_LOCAL_STORAGE_DIR", "storage")),
        publish_mode=os.getenv("INGESTION_PUBLISH_MODE", "jsonl"),
        outbox_path=Path(os.getenv("INGESTION_OUTBOX_PATH", "outbox/frame_ingested.jsonl")),
        outbox_reset=parse_bool(os.getenv("INGESTION_OUTBOX_RESET"), default=True),
        replay=parse_bool(os.getenv("INGESTION_REPLAY"), default=replay_default),
        replay_start_at=parse_datetime(os.getenv("INGESTION_REPLAY_START_AT", DEFAULT_REPLAY_START_AT)),
        external_camera_key=os.getenv("INGESTION_EXTERNAL_CAMERA_KEY"),
        organization_id=os.getenv("INGESTION_ORGANIZATION_ID"),
        site_id=os.getenv("INGESTION_SITE_ID"),
        zone_id=os.getenv("INGESTION_ZONE_ID"),
        source_type=source_type,
        source_name=os.getenv("INGESTION_SOURCE_NAME"),
        rtsp_url=os.getenv("INGESTION_RTSP_URL"),
        rtsp_transport=os.getenv("INGESTION_RTSP_TRANSPORT", "tcp"),
        camera_config_db_url=_camera_config_db_url_from_env(),
        camera_config_db_schema=os.getenv("INGESTION_CAMERA_DB_SCHEMA", os.getenv("DB_SCHEMA_API", "api")),
        camera_secret_fernet_key=os.getenv("CAMERA_SECRET_FERNET_KEY"),
        active_camera_source=os.getenv("INGESTION_ACTIVE_CAMERA_SOURCE", "db"),
        active_camera_concurrency=int(active_camera_concurrency) if active_camera_concurrency else None,
        active_camera_refresh_seconds=float(os.getenv("INGESTION_ACTIVE_CAMERA_REFRESH_SECONDS", "30")),
        active_camera_status_interval_seconds=float(os.getenv("INGESTION_ACTIVE_CAMERA_STATUS_INTERVAL_SECONDS", "30")),
        active_camera_stop_timeout_seconds=float(os.getenv("INGESTION_ACTIVE_CAMERA_STOP_TIMEOUT_SECONDS", "10")),
        active_camera_enable_health_server=parse_bool(os.getenv("INGESTION_ACTIVE_CAMERA_ENABLE_HEALTH_SERVER"), default=False),
        active_camera_health_host=os.getenv("INGESTION_ACTIVE_CAMERA_HEALTH_HOST", "127.0.0.1"),
        active_camera_health_port=int(os.getenv("INGESTION_ACTIVE_CAMERA_HEALTH_PORT", "8088")),
        rtsp_read_timeout_seconds=float(os.getenv("INGESTION_RTSP_READ_TIMEOUT_SECONDS", "10")),
        rtsp_reconnect_initial_delay_seconds=float(os.getenv("INGESTION_RTSP_RECONNECT_INITIAL_DELAY_SECONDS", "1")),
        rtsp_reconnect_max_delay_seconds=float(os.getenv("INGESTION_RTSP_RECONNECT_MAX_DELAY_SECONDS", "30")),
        rtsp_reconnect_backoff_multiplier=float(os.getenv("INGESTION_RTSP_RECONNECT_BACKOFF_MULTIPLIER", "2")),
        rtsp_max_reconnect_attempts=int(rtsp_max_reconnect_attempts) if rtsp_max_reconnect_attempts else None,
        instance_id=os.getenv("INGESTION_INSTANCE_ID", "local-replay"),
        ffmpeg_path=os.getenv("INGESTION_FFMPEG_PATH", "ffmpeg"),
        ffprobe_path=os.getenv("INGESTION_FFPROBE_PATH", "ffprobe"),
        minio_endpoint=remote_env("ENDPOINT", "localhost:9000"),
        minio_access_key=remote_env("ACCESS_KEY", "minio"),
        minio_secret_key=remote_env("SECRET_KEY", "minio123"),
        minio_bucket=remote_env("BUCKET", "vigilante-frames"),
        minio_secure=parse_bool(remote_env("SECURE", "false"), default=False),
        rabbitmq_host=os.getenv("RABBITMQ_HOST", os.getenv("INGESTION_RABBITMQ_HOST", "localhost")),
        rabbitmq_port=int(os.getenv("RABBITMQ_PORT", os.getenv("INGESTION_RABBITMQ_PORT", "5672"))),
        rabbitmq_user=os.getenv("RABBITMQ_USER", os.getenv("INGESTION_RABBITMQ_USER", "guest")),
        rabbitmq_password=os.getenv("RABBITMQ_PASSWORD", os.getenv("INGESTION_RABBITMQ_PASSWORD", "guest")),
        rabbitmq_vhost=os.getenv("RABBITMQ_VHOST", os.getenv("INGESTION_RABBITMQ_VHOST", "/")),
        rabbitmq_frame_exchange=os.getenv("RABBITMQ_FRAME_EXCHANGE", "vigilante.frames"),
        rabbitmq_frame_routing_key=os.getenv("RABBITMQ_FRAME_ROUTING_KEY", "frame.ingested"),
        rabbitmq_recognition_queue=os.getenv("RABBITMQ_RECOGNITION_QUEUE", "vigilante.recognition.frame_ingested"),
        rabbitmq_frame_dlx=os.getenv("RABBITMQ_FRAME_DLX", "vigilante.frames.dlx"),
        rabbitmq_frame_dlq=os.getenv("RABBITMQ_FRAME_DLQ", "vigilante.recognition.frame_ingested.dlq"),
        rabbitmq_frame_dlq_routing_key=os.getenv("RABBITMQ_FRAME_DLQ_ROUTING_KEY", "frame.ingested.dlq"),
        log_level=os.getenv("INGESTION_LOG_LEVEL", "INFO"),
    )


def _camera_config_db_url_from_env() -> str | None:
    explicit = os.getenv("INGESTION_CAMERA_DB_URL")
    if explicit:
        return explicit
    db_url = os.getenv("DB_URL")
    if db_url:
        return db_url
    host = os.getenv("INGESTION_CAMERA_DB_HOST")
    if not host:
        return None
    port = os.getenv("INGESTION_CAMERA_DB_PORT", "5432")
    name = os.getenv("INGESTION_CAMERA_DB_NAME", "vigilante_api")
    user = os.getenv("INGESTION_CAMERA_DB_USER", "julio")
    password = os.getenv("INGESTION_CAMERA_DB_PASSWORD", "")
    return f"postgresql://{user}:{password}@{host}:{port}/{name}"
