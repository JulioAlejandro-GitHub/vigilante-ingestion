from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from threading import Lock
from typing import Any

from app.services.active_camera_loader import ActiveCamera


@dataclass(frozen=True)
class CameraWorkerSnapshot:
    camera_id: str
    external_camera_key: str | None
    site_id: str | None
    zone_id: str | None
    name: str | None
    is_desired_active: bool
    worker_state: str
    status: str
    last_started_at: datetime | None
    last_connected_at: datetime | None
    last_frame_at: datetime | None
    last_publish_at: datetime | None
    stream_opens: int
    reconnect_attempts: int
    restart_attempts: int
    frames_captured: int
    frames_stored: int
    events_published: int
    read_failures: int
    store_failures: int
    publish_failures: int
    last_error: str | None
    last_error_at: datetime | None
    config_version_hash: str
    updated_at: datetime


@dataclass
class CameraWorkerState:
    camera: ActiveCamera
    is_desired_active: bool = True
    status: str = "pending"
    last_started_at: datetime | None = None
    last_connected_at: datetime | None = None
    last_frame_at: datetime | None = None
    last_publish_at: datetime | None = None
    stream_opens: int = 0
    reconnect_attempts: int = 0
    restart_attempts: int = 0
    frames_captured: int = 0
    frames_stored: int = 0
    events_published: int = 0
    read_failures: int = 0
    store_failures: int = 0
    publish_failures: int = 0
    last_error: str | None = None
    last_error_at: datetime | None = None
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    _lock: Lock = field(default_factory=Lock, repr=False)

    def mark(self, status: str, *, error: str | None = None, desired_active: bool | None = None) -> None:
        with self._lock:
            now = datetime.now(timezone.utc)
            self.status = status
            if desired_active is not None:
                self.is_desired_active = desired_active
            self.last_error = error
            if error:
                self.last_error_at = now
            if status == "starting":
                self.last_started_at = now
            self.updated_at = now

    def update_camera(self, camera: ActiveCamera) -> None:
        with self._lock:
            self.camera = camera
            self.updated_at = datetime.now(timezone.utc)

    def mark_desired(self, value: bool) -> None:
        with self._lock:
            self.is_desired_active = value
            self.updated_at = datetime.now(timezone.utc)

    def increment_restart(self) -> None:
        with self._lock:
            self.restart_attempts += 1
            self.updated_at = datetime.now(timezone.utc)

    def apply_runner_result(self, result: Any) -> None:
        with self._lock:
            self.frames_captured = int(getattr(result, "frames_captured", self.frames_captured))
            self.events_published = int(getattr(result, "events_published", self.events_published))
            self.stream_opens = int(getattr(result, "stream_opens", self.stream_opens))
            self.reconnect_attempts = int(getattr(result, "reconnect_attempts", self.reconnect_attempts))
            self.frames_stored = int(getattr(result, "frames_stored", self.frames_stored))
            self.read_failures = int(getattr(result, "read_failures", self.read_failures))
            self.store_failures = int(getattr(result, "store_failures", self.store_failures))
            self.publish_failures = int(getattr(result, "publish_failures", self.publish_failures))
            self.updated_at = datetime.now(timezone.utc)

    def apply_runner_event(self, event_name: str, payload: dict[str, Any]) -> None:
        with self._lock:
            now = datetime.now(timezone.utc)
            counters = payload.get("counters")
            if isinstance(counters, dict):
                self.frames_captured = int(counters.get("frames_captured", self.frames_captured))
                self.events_published = int(counters.get("events_published", self.events_published))
                self.stream_opens = int(counters.get("stream_opens", self.stream_opens))
                self.reconnect_attempts = int(counters.get("reconnect_attempts", self.reconnect_attempts))
                self.frames_stored = int(counters.get("frames_stored", self.frames_stored))
                self.read_failures = int(counters.get("read_failures", self.read_failures))
                self.store_failures = int(counters.get("store_failures", self.store_failures))
                self.publish_failures = int(counters.get("publish_failures", self.publish_failures))

            if event_name == "camera_stream_starting":
                self.status = "connecting"
                self.last_error = None
            elif event_name == "camera_stream_connected":
                self.status = "connected"
                self.last_connected_at = now
                self.last_error = None
            elif event_name == "camera_stream_disconnected":
                self.status = "disconnected"
                self.last_error = _optional_error(payload)
                self.last_error_at = now if self.last_error else self.last_error_at
            elif event_name == "camera_stream_reconnect_scheduled":
                self.status = "retrying"
                self.last_error = _optional_error(payload)
                self.last_error_at = now if self.last_error else self.last_error_at
            elif event_name == "camera_frame_ingested":
                self.status = "running"
                self.last_frame_at = now
                self.last_publish_at = now
                self.last_error = None
            elif event_name in {"camera_frame_store_failed", "camera_frame_publish_failed"}:
                self.last_error = _optional_error(payload)
                self.last_error_at = now if self.last_error else self.last_error_at
            elif event_name == "camera_worker_stopped":
                self.status = "stopped"

            self.updated_at = now

    def snapshot(self) -> CameraWorkerSnapshot:
        with self._lock:
            return CameraWorkerSnapshot(
                camera_id=self.camera.camera_id,
                external_camera_key=self.camera.external_camera_key,
                site_id=self.camera.site_id,
                zone_id=self.camera.zone_id,
                name=self.camera.name,
                is_desired_active=self.is_desired_active,
                worker_state=self.status,
                status=self.status,
                last_started_at=self.last_started_at,
                last_connected_at=self.last_connected_at,
                last_frame_at=self.last_frame_at,
                last_publish_at=self.last_publish_at,
                stream_opens=self.stream_opens,
                reconnect_attempts=self.reconnect_attempts,
                restart_attempts=self.restart_attempts,
                frames_captured=self.frames_captured,
                frames_stored=self.frames_stored,
                events_published=self.events_published,
                read_failures=self.read_failures,
                store_failures=self.store_failures,
                publish_failures=self.publish_failures,
                last_error=self.last_error,
                last_error_at=self.last_error_at,
                config_version_hash=self.camera.config_version_hash,
                updated_at=self.updated_at,
            )


def _optional_error(payload: dict[str, Any]) -> str | None:
    error = payload.get("error")
    return str(error) if error else None
