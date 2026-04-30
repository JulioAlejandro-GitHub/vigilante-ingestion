from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable

from app.config import IngestionConfig
from app.services.camera_config_service import (
    _metadata_to_dict,
    _normalize_psycopg_dsn,
    _optional_text,
    _quote_identifier,
    row_to_rtsp_camera_config,
)
from app.services.rtsp_url_builder import build_rtsp_url_from_camera_config

logger = logging.getLogger(__name__)

ActiveCameraRowFetcher = Callable[[IngestionConfig], list[dict[str, Any]]]


class ActiveCameraLoaderError(RuntimeError):
    pass


@dataclass(frozen=True)
class ActiveCamera:
    camera_id: str
    external_camera_key: str | None
    site_id: str | None
    zone_id: str | None
    name: str | None
    rtsp_url: str
    safe_rtsp_url: str
    rtsp_transport: str
    source_type: str = "rtsp"


def load_active_rtsp_cameras(
    config: IngestionConfig,
    *,
    row_fetcher: ActiveCameraRowFetcher | None = None,
) -> list[ActiveCamera]:
    if config.source_type != "active_cameras":
        raise ActiveCameraLoaderError("source_type must be 'active_cameras' to load active cameras")
    if config.active_camera_source != "db":
        raise ActiveCameraLoaderError("Only active_camera_source='db' is supported")

    fetcher = row_fetcher or fetch_active_camera_rows
    rows = fetcher(config)
    cameras: list[ActiveCamera] = []
    invalid_rows = 0

    for row in rows:
        if not _row_matches_filters(row, config):
            continue
        camera_id = _required_text(row.get("camera_id"), "camera_id")
        try:
            rtsp = build_rtsp_url_from_camera_config(
                row_to_rtsp_camera_config(row),
                camera_secret_fernet_key=config.camera_secret_fernet_key,
            )
        except Exception as exc:
            invalid_rows += 1
            logger.error(
                "active_camera_config_invalid camera_id=%s external_camera_key=%s error_type=%s error=%s",
                camera_id,
                _optional_text(row.get("external_camera_key")),
                type(exc).__name__,
                exc,
            )
            continue

        cameras.append(
            ActiveCamera(
                camera_id=camera_id,
                external_camera_key=_optional_text(row.get("external_camera_key")),
                site_id=_optional_text(row.get("site_id")),
                zone_id=_optional_text(row.get("zone_id")),
                name=_optional_text(row.get("name")),
                rtsp_url=rtsp.url,
                safe_rtsp_url=rtsp.safe_url,
                rtsp_transport=rtsp.rtsp_transport,
                source_type=rtsp.source_type,
            )
        )

    logger.info(
        "active_camera_loader_summary rows_loaded=%s cameras_loaded=%s invalid_rows=%s "
        "filter_camera_id=%s filter_external_camera_key=%s filter_site_id=%s filter_zone_id=%s",
        len(rows),
        len(cameras),
        invalid_rows,
        config.camera_id or "",
        config.external_camera_key or "",
        config.site_id or "",
        config.zone_id or "",
    )
    return cameras


def fetch_active_camera_rows(config: IngestionConfig) -> list[dict[str, Any]]:
    if not config.camera_config_db_url:
        raise ActiveCameraLoaderError("camera_config_db_url is required to load active cameras")

    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:
        raise ActiveCameraLoaderError("psycopg is required to load active cameras from api.camera") from exc

    dsn = _normalize_psycopg_dsn(config.camera_config_db_url)
    table_name = f"{_quote_identifier(config.camera_config_db_schema)}.camera" if config.camera_config_db_schema else "camera"
    where_clauses = ["is_active IS TRUE", "lower(source_type) = 'rtsp'"]
    params: list[Any] = []
    if config.camera_id:
        where_clauses.append("camera_id = %s")
        params.append(config.camera_id)
    if config.external_camera_key:
        where_clauses.append("external_camera_key = %s")
        params.append(config.external_camera_key)
    if config.site_id:
        where_clauses.append("site_id = %s")
        params.append(config.site_id)
    if config.zone_id:
        where_clauses.append("zone_id = %s")
        params.append(config.zone_id)

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
        WHERE {" AND ".join(where_clauses)}
        ORDER BY camera_id ASC
    """
    with psycopg.connect(dsn, row_factory=dict_row) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            return [_normalize_row(row) for row in cursor.fetchall()]


def _row_matches_filters(row: dict[str, Any], config: IngestionConfig) -> bool:
    if _is_active_value(row.get("is_active")) is not True:
        return False
    if (_optional_text(row.get("source_type")) or "").strip().lower() != "rtsp":
        return False
    if config.camera_id and _optional_text(row.get("camera_id")) != config.camera_id:
        return False
    if config.external_camera_key and _optional_text(row.get("external_camera_key")) != config.external_camera_key:
        return False
    if config.site_id and _optional_text(row.get("site_id")) != config.site_id:
        return False
    if config.zone_id and _optional_text(row.get("zone_id")) != config.zone_id:
        return False
    return True


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(row)
    normalized["camera_id"] = _optional_text(normalized.get("camera_id"))
    normalized["site_id"] = _optional_text(normalized.get("site_id"))
    normalized["zone_id"] = _optional_text(normalized.get("zone_id"))
    normalized["metadata"] = _metadata_to_dict(normalized.get("metadata"))
    return normalized


def _required_text(value: Any, name: str) -> str:
    text = _optional_text(value)
    if not text:
        raise ActiveCameraLoaderError(f"api.camera {name} is required")
    return text


def _is_active_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value in (1, "1"):
        return True
    if isinstance(value, str):
        return value.strip().lower() in {"true", "t", "yes", "y", "on"}
    return False
