from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qs, quote, urlencode, urlparse, urlunparse

from app.capture.rtsp_source import mask_rtsp_credentials
from app.services.camera_secret_service import decrypt_camera_secret


ALLOWED_RTSP_TRANSPORTS = {"tcp", "udp"}


class CameraConfigError(ValueError):
    pass


@dataclass(frozen=True)
class RtspCameraConfig:
    source_type: str | None = None
    camera_hostname: str | None = None
    camera_port: int | str | None = None
    camera_path: str | None = None
    rtsp_transport: str | None = None
    channel: int | str | None = None
    subtype: int | str | None = None
    camera_user: str | None = None
    camera_secret: str | None = None
    metadata: dict[str, Any] | None = None


@dataclass(frozen=True)
class RtspUrlBuildResult:
    url: str
    safe_url: str
    rtsp_transport: str
    source_type: str


def build_rtsp_url_from_camera_config(config: RtspCameraConfig, *, camera_secret_fernet_key: str | None) -> RtspUrlBuildResult:
    metadata = dict(config.metadata or {})
    legacy_stream_url = _metadata_string(metadata, "stream_url")
    parsed_legacy_url = urlparse(legacy_stream_url) if legacy_stream_url else None

    source_type = _first_text(config.source_type, _metadata_string(metadata, "source_type"))
    source_type = source_type.lower() if source_type else None
    if not source_type and parsed_legacy_url and parsed_legacy_url.scheme in {"rtsp", "rtsps"}:
        source_type = "rtsp"
    if source_type != "rtsp":
        raise CameraConfigError("api.camera source_type must be 'rtsp' for RTSP ingestion")

    hostname = _first_text(config.camera_hostname, _metadata_string(metadata, "camera_hostname"), _parsed_hostname(parsed_legacy_url))
    path = _normalize_path(_first_text(config.camera_path, _metadata_string(metadata, "camera_path"), _parsed_path(parsed_legacy_url)))
    port = _parse_port(_first_value(config.camera_port, metadata.get("camera_port"), _parsed_port(parsed_legacy_url), 554))
    transport = _normalize_transport(_first_text(config.rtsp_transport, _metadata_string(metadata, "rtsp_transport"), "tcp"))

    legacy_query = parse_qs(parsed_legacy_url.query) if parsed_legacy_url else {}
    channel = _parse_optional_query_value(_first_value(config.channel, metadata.get("channel"), _first_query_value(legacy_query, "channel")), "channel")
    subtype = _parse_optional_query_value(_first_value(config.subtype, metadata.get("subtype"), _first_query_value(legacy_query, "subtype")), "subtype")
    username = _first_text(config.camera_user, _metadata_string(metadata, "camera_user"), _parsed_username(parsed_legacy_url))
    secret = _resolve_secret(config.camera_secret, metadata, parsed_legacy_url, camera_secret_fernet_key=camera_secret_fernet_key)

    if not hostname:
        raise CameraConfigError("api.camera camera_hostname is required for RTSP ingestion")
    if not path:
        raise CameraConfigError("api.camera camera_path is required for RTSP ingestion")
    if bool(username) != bool(secret):
        raise CameraConfigError("RTSP credentials must include both camera_user and camera_secret")

    query: dict[str, str] = {}
    if channel is not None:
        query["channel"] = str(channel)
    if subtype is not None:
        query["subtype"] = str(subtype)

    netloc_host = hostname
    if ":" in netloc_host and not netloc_host.startswith("["):
        netloc_host = f"[{netloc_host}]"
    netloc = f"{netloc_host}:{port}"
    if username and secret:
        netloc = f"{quote(username, safe='')}:{quote(secret, safe='')}@{netloc}"

    url = urlunparse(("rtsp", netloc, path, "", urlencode(query), ""))
    return RtspUrlBuildResult(
        url=url,
        safe_url=mask_rtsp_credentials(url),
        rtsp_transport=transport,
        source_type=source_type,
    )


def _resolve_secret(
    camera_secret: str | None,
    metadata: dict[str, Any],
    parsed_legacy_url,
    *,
    camera_secret_fernet_key: str | None,
) -> str | None:
    if camera_secret:
        return decrypt_camera_secret(camera_secret, key=camera_secret_fernet_key, allow_legacy_plaintext=True)
    for key in ("camera_pass", "camera_secret", "rtsp_password", "password"):
        value = metadata.get(key)
        if value:
            return str(value)
    if parsed_legacy_url and parsed_legacy_url.password:
        return parsed_legacy_url.password
    return None


def _first_value(*values):
    for value in values:
        if value not in (None, ""):
            return value
    return None


def _first_text(*values: str | None) -> str | None:
    value = _first_value(*values)
    if value in (None, ""):
        return None
    return str(value).strip() or None


def _metadata_string(metadata: dict[str, Any], key: str) -> str | None:
    value = metadata.get(key)
    if value in (None, ""):
        return None
    return str(value)


def _parsed_hostname(parsed_url) -> str | None:
    return parsed_url.hostname if parsed_url else None


def _parsed_port(parsed_url) -> int | None:
    return parsed_url.port if parsed_url else None


def _parsed_path(parsed_url) -> str | None:
    return parsed_url.path if parsed_url else None


def _parsed_username(parsed_url) -> str | None:
    return parsed_url.username if parsed_url else None


def _first_query_value(query: dict[str, list[str]], key: str) -> str | None:
    values = query.get(key)
    return values[0] if values else None


def _normalize_path(path: str | None) -> str | None:
    if not path:
        return None
    parsed = urlparse(path)
    if parsed.scheme in {"rtsp", "rtsps"}:
        path = parsed.path
    path = path.strip()
    if not path:
        return None
    return path if path.startswith("/") else f"/{path}"


def _parse_port(value) -> int:
    try:
        port = int(value)
    except (TypeError, ValueError) as exc:
        raise CameraConfigError("api.camera camera_port must be an integer") from exc
    if port <= 0 or port > 65535:
        raise CameraConfigError("api.camera camera_port must be between 1 and 65535")
    return port


def _normalize_transport(value: str | None) -> str:
    transport = (value or "tcp").strip().lower()
    if transport not in ALLOWED_RTSP_TRANSPORTS:
        raise CameraConfigError("api.camera rtsp_transport must be 'tcp' or 'udp'")
    return transport


def _parse_optional_query_value(value, name: str) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise CameraConfigError(f"api.camera {name} must be serializable as an integer query value") from exc
