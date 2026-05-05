from __future__ import annotations

import json
from dataclasses import replace
from typing import Any, Callable

from app.config import IngestionConfig
from app.services.camera_runtime_config_mapper import build_camera_runtime_config
from app.services.rtsp_url_builder import RtspCameraConfig, build_rtsp_url_from_camera_config


CameraRowFetcher = Callable[[IngestionConfig], dict[str, Any] | None]


class CameraConfigLookupError(RuntimeError):
    pass


def apply_camera_database_config(config: IngestionConfig, *, row_fetcher: CameraRowFetcher | None = None) -> IngestionConfig:
    if config.source_type != "rtsp" or not config.camera_config_db_url:
        return config

    fetcher = row_fetcher or fetch_camera_row
    row = fetcher(config)
    if row is None:
        raise CameraConfigLookupError(f"api.camera row not found for camera_id={config.camera_id}")

    rtsp = build_rtsp_url_from_camera_config(row_to_rtsp_camera_config(row), camera_secret_fernet_key=config.camera_secret_fernet_key)
    camera_runtime_config = build_camera_runtime_config(
        camera_id=config.camera_id,
        camera_metadata=_metadata_to_dict(row.get("metadata")),
    )
    return replace(
        config,
        rtsp_url=rtsp.url,
        rtsp_transport=rtsp.rtsp_transport,
        source_type=rtsp.source_type,
        external_camera_key=config.external_camera_key or _optional_text(row.get("external_camera_key")),
        site_id=config.site_id or _optional_text(row.get("site_id")),
        zone_id=config.zone_id or _optional_text(row.get("zone_id")),
        source_name=config.source_name or _optional_text(row.get("name")),
        camera_runtime_config=camera_runtime_config,
    )


def fetch_camera_row(config: IngestionConfig) -> dict[str, Any] | None:
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:
        raise CameraConfigLookupError("psycopg is required to load RTSP camera config from api.camera") from exc

    dsn = _normalize_psycopg_dsn(config.camera_config_db_url or "")
    table_name = f"{_quote_identifier(config.camera_config_db_schema)}.camera" if config.camera_config_db_schema else "camera"
    query = f"""
        SELECT
            camera_id,
            external_camera_key,
            site_id,
            zone_id,
            name,
            is_active,
            source_type,
            camera_hostname,
            camera_port,
            camera_path,
            rtsp_transport,
            channel,
            subtype,
            camera_user,
            camera_secret,
            metadata
        FROM {table_name}
        WHERE camera_id = %s
    """
    with psycopg.connect(dsn, row_factory=dict_row) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, (config.camera_id,))
            return cursor.fetchone()


def row_to_rtsp_camera_config(row: dict[str, Any]) -> RtspCameraConfig:
    return RtspCameraConfig(
        source_type=_optional_text(row.get("source_type")),
        camera_hostname=_optional_text(row.get("camera_hostname")),
        camera_port=row.get("camera_port"),
        camera_path=_optional_text(row.get("camera_path")),
        rtsp_transport=_optional_text(row.get("rtsp_transport")),
        channel=row.get("channel"),
        subtype=row.get("subtype"),
        camera_user=_optional_text(row.get("camera_user")),
        camera_secret=_optional_text(row.get("camera_secret")),
        metadata=_metadata_to_dict(row.get("metadata")),
    )


def _metadata_to_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if value in (None, ""):
        return {}
    if isinstance(value, str):
        return json.loads(value)
    return dict(value)


def _optional_text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _normalize_psycopg_dsn(value: str) -> str:
    return value.replace("postgresql+psycopg://", "postgresql://", 1).replace("postgresql+psycopg2://", "postgresql://", 1)


def _quote_identifier(value: str) -> str:
    if not value.replace("_", "").isalnum():
        raise CameraConfigLookupError("INGESTION_CAMERA_DB_SCHEMA contains invalid characters")
    return value
