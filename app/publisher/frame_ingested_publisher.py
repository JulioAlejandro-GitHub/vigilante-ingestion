from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid5

from app.capture.rtsp_source import mask_rtsp_credentials
from app.config import IngestionConfig, format_datetime
from app.models.frame import CapturedFrame, StoredFrame
from app.services.camera_runtime_config_mapper import RUNTIME_CONFIG_METADATA_KEY


class OutboxFilePublisher:
    def __init__(self, path: Path | str, *, reset: bool = True) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if reset:
            self.path.write_text("", encoding="utf-8")

    def publish(self, event: dict) -> None:
        with self.path.open("a", encoding="utf-8") as outbox_file:
            outbox_file.write(json.dumps(event, sort_keys=True, separators=(",", ":")))
            outbox_file.write("\n")


def build_frame_ingested_event(
    *,
    frame: CapturedFrame,
    stored_frame: StoredFrame,
    config: IngestionConfig,
) -> dict:
    UUID(frame.camera_id)
    captured_at = format_datetime(frame.captured_at)
    event_id = _deterministic_event_id(frame)
    safe_source_uri = mask_rtsp_credentials(frame.source_uri)
    idempotency_key = (
        f"frame:{frame.camera_id}:"
        f"{frame.source_type}:{Path(safe_source_uri).name}:"
        f"{frame.capture_fps:g}:{frame.sample_index}:{frame.source_frame_index}"
    )

    context = {
        "camera_id": frame.camera_id,
        "correlation_id": event_id.replace("evt_", "corr_", 1),
        "idempotency_key": idempotency_key,
    }
    if config.organization_id:
        context["organization_id"] = config.organization_id
    if config.site_id:
        context["site_id"] = config.site_id
    if config.zone_id:
        context["zone_id"] = config.zone_id

    payload = {
        "camera_id": frame.camera_id,
        "captured_at": captured_at,
        "content_type": frame.content_type,
        "frame_ref": stored_frame.frame_ref,
        "frame_uri": stored_frame.frame_uri,
        "height": frame.height,
        "metadata": {
            "capture_fps": frame.capture_fps,
            "frame_object_key": stored_frame.object_key,
            "metadata_ref": stored_frame.metadata_ref,
            "sample_index": frame.sample_index,
            "size_bytes": stored_frame.size_bytes,
            "source_frame_index": frame.source_frame_index,
            "source_fps": frame.source_fps,
            "source_timestamp_seconds": frame.source_timestamp_seconds,
            "source_uri": safe_source_uri,
            "storage_backend": stored_frame.storage_backend,
        },
        "quality_metadata": {
            "capture_fps": frame.capture_fps,
            "source_fps": frame.source_fps,
            "source_timestamp_seconds": frame.source_timestamp_seconds,
        },
        "source_type": frame.source_type,
        "width": frame.width,
    }
    if config.external_camera_key:
        payload["external_camera_key"] = config.external_camera_key
    if config.camera_runtime_config:
        payload["metadata"][RUNTIME_CONFIG_METADATA_KEY] = dict(config.camera_runtime_config)

    emitted_at = captured_at if config.replay else format_datetime(datetime.now(timezone.utc))
    return {
        "event_id": event_id,
        "event_type": "frame.ingested",
        "event_version": config.event_version,
        "occurred_at": captured_at,
        "emitted_at": emitted_at,
        "source": {
            "component": "vigilante-ingestion",
            "instance": config.instance_id,
            "version": config.component_version,
        },
        "payload": payload,
        "context": context,
    }


def _deterministic_event_id(frame: CapturedFrame) -> str:
    safe_source_uri = mask_rtsp_credentials(frame.source_uri)
    seed = (
        f"{frame.camera_id}|{frame.source_type}|{Path(safe_source_uri).name}|"
        f"{frame.capture_fps:g}|{frame.sample_index}|{frame.source_frame_index}|"
        f"{frame.source_timestamp_seconds:.6f}"
    )
    return f"evt_{uuid5(NAMESPACE_URL, seed)}"
