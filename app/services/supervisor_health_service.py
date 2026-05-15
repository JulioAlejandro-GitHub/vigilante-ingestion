from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from app.config import format_datetime
from app.services.camera_worker_state import CameraWorkerSnapshot


@dataclass(frozen=True)
class SupervisorRuntimeHealth:
    cameras_loaded: int
    desired_active_cameras: int
    workers_running: int
    last_refresh_at: datetime | None
    last_refresh_change_count: int
    last_refresh_error: str | None = None


def build_camera_health(snapshots: list[CameraWorkerSnapshot] | tuple[CameraWorkerSnapshot, ...]) -> list[dict[str, Any]]:
    return [_camera_snapshot_to_dict(snapshot) for snapshot in sorted(snapshots, key=lambda item: item.camera_id)]


def build_health_summary(
    snapshots: list[CameraWorkerSnapshot] | tuple[CameraWorkerSnapshot, ...],
    runtime: SupervisorRuntimeHealth,
) -> dict[str, Any]:
    snapshot_list = list(snapshots)
    return {
        "status": _overall_status(snapshot_list, runtime.last_refresh_error),
        "cameras_loaded": runtime.cameras_loaded,
        "desired_active_cameras": runtime.desired_active_cameras,
        "workers_running": runtime.workers_running,
        "workers_connected": sum(snapshot.worker_state in {"connected", "running"} for snapshot in snapshot_list),
        "workers_retrying": sum(snapshot.worker_state == "retrying" for snapshot in snapshot_list),
        "workers_failed": sum(snapshot.worker_state == "failed" for snapshot in snapshot_list),
        "workers_stopped": sum(snapshot.worker_state in {"stopped", "disabled"} for snapshot in snapshot_list),
        "workers_stopping": sum(snapshot.worker_state == "stopping" for snapshot in snapshot_list),
        "frames_captured_total": sum(snapshot.frames_captured for snapshot in snapshot_list),
        "events_published_total": sum(snapshot.events_published for snapshot in snapshot_list),
        "last_refresh_at": _format_optional_datetime(runtime.last_refresh_at),
        "last_refresh_change_count": runtime.last_refresh_change_count,
        "last_refresh_error": runtime.last_refresh_error,
        "generated_at": format_datetime(datetime.now(timezone.utc)),
    }


def build_health_document(
    snapshots: list[CameraWorkerSnapshot] | tuple[CameraWorkerSnapshot, ...],
    runtime: SupervisorRuntimeHealth,
) -> dict[str, Any]:
    return {
        "summary": build_health_summary(snapshots, runtime),
        "cameras": build_camera_health(snapshots),
    }


def _camera_snapshot_to_dict(snapshot: CameraWorkerSnapshot) -> dict[str, Any]:
    return {
        "camera_id": snapshot.camera_id,
        "external_camera_key": snapshot.external_camera_key,
        "site_id": snapshot.site_id,
        "zone_id": snapshot.zone_id,
        "name": snapshot.name,
        "is_desired_active": snapshot.is_desired_active,
        "worker_state": snapshot.worker_state,
        "last_started_at": _format_optional_datetime(snapshot.last_started_at),
        "last_connected_at": _format_optional_datetime(snapshot.last_connected_at),
        "last_frame_at": _format_optional_datetime(snapshot.last_frame_at),
        "last_publish_at": _format_optional_datetime(snapshot.last_publish_at),
        "frames_captured": snapshot.frames_captured,
        "frames_stored": snapshot.frames_stored,
        "events_published": snapshot.events_published,
        "stream_opens": snapshot.stream_opens,
        "reconnect_attempts": snapshot.reconnect_attempts,
        "restart_attempts": snapshot.restart_attempts,
        "read_failures": snapshot.read_failures,
        "store_failures": snapshot.store_failures,
        "publish_failures": snapshot.publish_failures,
        "last_error": snapshot.last_error,
        "last_error_at": _format_optional_datetime(snapshot.last_error_at),
        "config_version_hash": snapshot.config_version_hash,
        "updated_at": format_datetime(snapshot.updated_at),
    }


def _overall_status(snapshots: list[CameraWorkerSnapshot], last_refresh_error: str | None) -> str:
    if last_refresh_error:
        return "degraded"
    if any(snapshot.worker_state == "failed" for snapshot in snapshots):
        return "degraded"
    if any(snapshot.worker_state in {"connected", "running", "retrying", "starting", "connecting"} for snapshot in snapshots):
        return "ok"
    return "idle"


def _format_optional_datetime(value: datetime | None) -> str | None:
    return format_datetime(value) if value else None
