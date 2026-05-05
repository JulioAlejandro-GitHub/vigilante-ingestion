from __future__ import annotations

import logging
import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from threading import Event, Lock, Thread
from typing import Any, Callable

from app.capture.reconnect_policy import ReconnectPolicy
from app.config import IngestionConfig
from app.publisher.publish_mode import FrameIngestedPublisher, PublishMode
from app.routes.health import SupervisorHealthHttpServer
from app.runner.rtsp_runner import RtspLifecycleCallback, RtspRunner, ShouldStop
from app.services.active_camera_diff import diff_active_cameras
from app.services.active_camera_loader import ActiveCamera, load_active_rtsp_cameras
from app.services.camera_worker_state import CameraWorkerSnapshot, CameraWorkerState
from app.services.supervisor_health_service import (
    SupervisorRuntimeHealth,
    build_health_document,
    build_health_summary,
)
from app.storage.frame_storage import FrameStorage

logger = logging.getLogger(__name__)

CameraLoader = Callable[[IngestionConfig], list[ActiveCamera]]
StorageFactory = Callable[[IngestionConfig], FrameStorage]
PublisherFactory = Callable[[IngestionConfig], FrameIngestedPublisher]
RunnerFactory = Callable[
    [IngestionConfig, FrameStorage, FrameIngestedPublisher, RtspLifecycleCallback, ShouldStop],
    Any,
]
Sleeper = Callable[[float], None]


@dataclass(frozen=True)
class ActiveCameraSupervisorResult:
    frames_captured: int
    events_published: int
    outbox_path: str
    publish_mode: str
    destinations: tuple[str, ...]
    cameras_loaded: int
    workers_started: int
    workers_stopped: int
    workers_failed: int
    cameras_connected: int
    cameras_failing: int
    last_refresh_change_count: int
    states: tuple[CameraWorkerSnapshot, ...]


@dataclass
class CameraWorkerHandle:
    camera: ActiveCamera
    state: CameraWorkerState
    stop_event: Event
    thread: Thread
    started_at: datetime

    def is_alive(self) -> bool:
        return self.thread.is_alive()


class ActiveCameraSupervisor:
    def __init__(
        self,
        *,
        config: IngestionConfig,
        storage_factory: StorageFactory,
        publisher_factory: PublisherFactory,
        camera_loader: CameraLoader = load_active_rtsp_cameras,
        runner_factory: RunnerFactory | None = None,
        sleep: Sleeper = time.sleep,
        max_refresh_cycles: int | None = None,
    ) -> None:
        self.config = config
        self.storage_factory = storage_factory
        self.publisher_factory = publisher_factory
        self.camera_loader = camera_loader
        self.runner_factory = runner_factory or self._default_runner_factory
        self.sleep = sleep
        self.max_refresh_cycles = max_refresh_cycles
        self.stop_event = Event()
        self._lock = Lock()
        self.states: dict[str, CameraWorkerState] = {}
        self.handles: dict[str, CameraWorkerHandle] = {}
        self.desired_cameras: dict[str, ActiveCamera] = {}
        self.workers_started_total = 0
        self.last_cameras_loaded = 0
        self.last_refresh_at: datetime | None = None
        self.last_refresh_change_count = 0
        self.last_refresh_error: str | None = None
        self._health_server: SupervisorHealthHttpServer | None = None

    def run(self) -> ActiveCameraSupervisorResult:
        self._prepare_jsonl_outbox()
        self._start_health_server()
        refresh_cycles = 0
        next_refresh_at = time.monotonic()
        next_status_at = time.monotonic()

        try:
            while not self.stop_event.is_set():
                now = time.monotonic()
                if now >= next_refresh_at:
                    self.refresh_once()
                    refresh_cycles += 1
                    next_refresh_at = now + self.config.active_camera_refresh_seconds
                    if self.max_refresh_cycles is not None and refresh_cycles >= self.max_refresh_cycles:
                        break

                self._reap_stopped_workers()

                if now >= next_status_at:
                    self._log_status()
                    next_status_at = now + self.config.active_camera_status_interval_seconds

                if self.config.max_frames is not None and self._all_desired_workers_terminal():
                    break

                wait_seconds = min(
                    0.25,
                    max(0.01, next_refresh_at - time.monotonic()),
                    max(0.01, next_status_at - time.monotonic()),
                )
                self.stop_event.wait(wait_seconds)
        except KeyboardInterrupt:
            self.stop()
            raise
        finally:
            self._stop_health_server()
            self._stop_all_workers(reason="supervisor_stopping")

        result = self._build_result()
        logger.info(
            "active_camera_supervisor_stopped cameras_loaded=%s workers_started=%s workers_stopped=%s "
            "workers_failed=%s frames_captured=%s events_published=%s last_refresh_change_count=%s",
            result.cameras_loaded,
            result.workers_started,
            result.workers_stopped,
            result.workers_failed,
            result.frames_captured,
            result.events_published,
            result.last_refresh_change_count,
        )
        return result

    def stop(self) -> None:
        self.stop_event.set()

    def refresh_once(self) -> int:
        try:
            loaded_cameras = self.camera_loader(self.config)
        except Exception as exc:
            self.last_refresh_error = f"{type(exc).__name__}: {exc}"
            logger.exception("active_camera_refresh_failed error_type=%s", type(exc).__name__)
            return 0

        desired = {camera.camera_id: camera for camera in loaded_cameras}
        with self._lock:
            current = dict(self.desired_cameras)

        diff = diff_active_cameras(current, desired)
        logger.info(
            "active_camera_refresh_diff cameras_loaded=%s start=%s stop=%s restart=%s unchanged=%s",
            len(loaded_cameras),
            len(diff.start),
            len(diff.stop),
            len(diff.restart),
            len(diff.unchanged),
        )

        for camera in diff.stop:
            self._stop_worker(camera.camera_id, final_status="disabled", desired_active=False, reason="camera_disabled")
        for camera in diff.restart:
            self._stop_worker(camera.camera_id, final_status="stopped", desired_active=True, reason="camera_config_changed")
            self._start_worker(camera)
        for camera in diff.start:
            self._start_worker(camera)
        for camera in diff.unchanged:
            state = self.states.get(camera.camera_id)
            if state is not None:
                state.update_camera(camera)
                state.mark_desired(True)
            if not self._worker_is_alive(camera.camera_id) and (state is None or state.snapshot().worker_state != "failed"):
                self._start_worker(camera)

        with self._lock:
            self.desired_cameras = desired
            self.last_cameras_loaded = len(loaded_cameras)
            self.last_refresh_at = datetime.now(timezone.utc)
            self.last_refresh_change_count = diff.change_count
            self.last_refresh_error = None

        logger.info(
            "active_camera_refresh_applied cameras_loaded=%s change_count=%s desired_active=%s",
            len(loaded_cameras),
            diff.change_count,
            len(desired),
        )
        return diff.change_count

    def health_document(self) -> dict[str, Any]:
        snapshots = self._snapshots()
        return build_health_document(snapshots, self._runtime_health(snapshots))

    def health_summary(self) -> dict[str, Any]:
        snapshots = self._snapshots()
        return build_health_summary(snapshots, self._runtime_health(snapshots))

    def _start_worker(self, camera: ActiveCamera) -> bool:
        with self._lock:
            existing = self.handles.get(camera.camera_id)
            if existing is not None and existing.is_alive():
                return False
            if not self._can_start_more_workers_locked(camera.camera_id):
                state = self.states.get(camera.camera_id) or CameraWorkerState(camera=camera, is_desired_active=True)
                state.update_camera(camera)
                state.mark("stopped", desired_active=True)
                self.states[camera.camera_id] = state
                logger.warning(
                    "active_camera_not_started_due_to_concurrency_limit camera_id=%s external_camera_key=%s limit=%s",
                    camera.camera_id,
                    camera.external_camera_key or "",
                    self.config.active_camera_concurrency,
                )
                return False

            state = self.states.get(camera.camera_id) or CameraWorkerState(camera=camera, is_desired_active=True)
            state.update_camera(camera)
            state.mark("starting", desired_active=True)
            stop_event = Event()
            thread = Thread(
                target=self._run_camera_worker,
                name=f"camera-worker-{camera.camera_id}",
                args=(camera, state, stop_event),
                daemon=True,
            )
            self.handles[camera.camera_id] = CameraWorkerHandle(
                camera=camera,
                state=state,
                stop_event=stop_event,
                thread=thread,
                started_at=datetime.now(timezone.utc),
            )
            self.states[camera.camera_id] = state
            self.workers_started_total += 1

        thread.start()
        logger.info(
            "camera_worker_thread_started camera_id=%s external_camera_key=%s config_version_hash=%s",
            camera.camera_id,
            camera.external_camera_key or "",
            camera.config_version_hash,
        )
        return True

    def _stop_worker(self, camera_id: str, *, final_status: str, desired_active: bool, reason: str) -> None:
        with self._lock:
            handle = self.handles.get(camera_id)
            state = self.states.get(camera_id)
            if state is not None and handle is not None and handle.thread.is_alive():
                state.mark("stopping", desired_active=desired_active)
            if handle is not None:
                handle.stop_event.set()

        if handle is not None and handle.thread.is_alive():
            logger.info(
                "camera_worker_stop_requested camera_id=%s reason=%s final_status=%s",
                camera_id,
                reason,
                final_status,
            )
            handle.thread.join(timeout=self.config.active_camera_stop_timeout_seconds)

        with self._lock:
            still_alive = handle is not None and handle.thread.is_alive()
            if not still_alive:
                self.handles.pop(camera_id, None)
            state = self.states.get(camera_id)
            if state is not None:
                current_status = state.snapshot().worker_state
                if not still_alive and current_status == "failed" and final_status == "stopped":
                    state.mark_desired(desired_active)
                else:
                    state.mark("stopping" if still_alive else final_status, desired_active=desired_active)
            if still_alive:
                logger.warning("camera_worker_stop_timeout camera_id=%s reason=%s", camera_id, reason)
            else:
                logger.info("camera_worker_stopped_by_supervisor camera_id=%s reason=%s status=%s", camera_id, reason, final_status)

    def _run_camera_worker(self, camera: ActiveCamera, state: CameraWorkerState, worker_stop_event: Event) -> None:
        worker_config = self._camera_config(camera)
        restart_policy = ReconnectPolicy(
            initial_delay_seconds=worker_config.rtsp_reconnect_initial_delay_seconds,
            max_delay_seconds=worker_config.rtsp_reconnect_max_delay_seconds,
            backoff_multiplier=worker_config.rtsp_reconnect_backoff_multiplier,
            max_reconnect_attempts=worker_config.rtsp_max_reconnect_attempts,
        )

        def should_stop() -> bool:
            return self.stop_event.is_set() or worker_stop_event.is_set()

        while not should_stop():
            state.mark("starting", desired_active=True)
            logger.info(
                "camera_worker_starting camera_id=%s external_camera_key=%s site_id=%s zone_id=%s url=%s config_version_hash=%s",
                camera.camera_id,
                camera.external_camera_key or "",
                camera.site_id or "",
                camera.zone_id or "",
                camera.safe_rtsp_url,
                camera.config_version_hash,
            )
            try:
                storage = self.storage_factory(worker_config)
                publisher = self.publisher_factory(worker_config)
                runner = self.runner_factory(
                    worker_config,
                    storage,
                    publisher,
                    lambda event_name, payload: state.apply_runner_event(event_name, payload),
                    should_stop,
                )
                result = runner.run()
                state.apply_runner_result(result)
                state.mark("stopped", desired_active=not worker_stop_event.is_set())
                logger.info(
                    "camera_worker_stopped camera_id=%s external_camera_key=%s frames_captured=%s events_published=%s",
                    camera.camera_id,
                    camera.external_camera_key or "",
                    state.snapshot().frames_captured,
                    state.snapshot().events_published,
                )
                return
            except Exception as exc:
                if should_stop():
                    state.mark("stopped", error=str(exc), desired_active=False)
                    return
                logger.exception(
                    "camera_worker_failed camera_id=%s external_camera_key=%s error_type=%s",
                    camera.camera_id,
                    camera.external_camera_key or "",
                    type(exc).__name__,
                )
                if not restart_policy.can_retry():
                    state.mark("failed", error=str(exc), desired_active=True)
                    logger.error(
                        "camera_worker_stopped camera_id=%s external_camera_key=%s status=failed restart_attempts=%s",
                        camera.camera_id,
                        camera.external_camera_key or "",
                        restart_policy.attempts,
                    )
                    return
                delay = restart_policy.next_delay()
                state.increment_restart()
                state.mark("retrying", error=str(exc), desired_active=True)
                logger.info(
                    "camera_stream_reconnect_scheduled camera_id=%s external_camera_key=%s site_id=%s zone_id=%s "
                    "attempt=%s delay_seconds=%s reason=worker_error",
                    camera.camera_id,
                    camera.external_camera_key or "",
                    camera.site_id or "",
                    camera.zone_id or "",
                    state.snapshot().restart_attempts,
                    delay,
                )
                self._sleep_or_stop(delay, worker_stop_event)

    def _camera_config(self, camera: ActiveCamera) -> IngestionConfig:
        return replace(
            self.config,
            source_type="rtsp",
            source_name=camera.name,
            camera_id=camera.camera_id,
            external_camera_key=camera.external_camera_key,
            site_id=camera.site_id,
            zone_id=camera.zone_id,
            camera_runtime_config=dict(camera.camera_runtime_config or {}),
            rtsp_url=camera.rtsp_url,
            rtsp_transport=camera.rtsp_transport,
            replay=False,
            outbox_reset=False,
        )

    def _sleep_or_stop(self, delay: float, worker_stop_event: Event) -> None:
        if delay <= 0:
            self.sleep(0)
            return
        deadline = time.monotonic() + delay
        while delay > 0 and not self.stop_event.is_set() and not worker_stop_event.is_set():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            self.sleep(min(remaining, 0.25))

    def _reap_stopped_workers(self) -> None:
        with self._lock:
            stopped = [camera_id for camera_id, handle in self.handles.items() if not handle.thread.is_alive()]
            for camera_id in stopped:
                self.handles.pop(camera_id, None)
                state = self.states.get(camera_id)
                if state is not None and state.snapshot().worker_state == "stopping":
                    desired_active = camera_id in self.desired_cameras
                    state.mark("stopped" if desired_active else "disabled", desired_active=desired_active)

    def _worker_is_alive(self, camera_id: str) -> bool:
        with self._lock:
            handle = self.handles.get(camera_id)
            return bool(handle and handle.thread.is_alive())

    def _all_desired_workers_terminal(self) -> bool:
        with self._lock:
            if not self.desired_cameras:
                return False
            for camera_id in self.desired_cameras:
                handle = self.handles.get(camera_id)
                if handle is not None and handle.thread.is_alive():
                    return False
            return True

    def _log_status(self) -> None:
        summary = self.health_summary()
        logger.info(
            "active_camera_supervisor_status cameras_loaded=%s desired_active_cameras=%s workers_running=%s "
            "workers_connected=%s workers_retrying=%s workers_failed=%s frames_captured_total=%s events_published_total=%s "
            "last_refresh_change_count=%s",
            summary["cameras_loaded"],
            summary["desired_active_cameras"],
            summary["workers_running"],
            summary["workers_connected"],
            summary["workers_retrying"],
            summary["workers_failed"],
            summary["frames_captured_total"],
            summary["events_published_total"],
            summary["last_refresh_change_count"],
        )
        for snapshot in self._snapshots():
            logger.info(
                "camera_worker_status camera_id=%s external_camera_key=%s site_id=%s zone_id=%s status=%s "
                "desired_active=%s frames_captured=%s events_published=%s reconnect_attempts=%s restart_attempts=%s",
                snapshot.camera_id,
                snapshot.external_camera_key or "",
                snapshot.site_id or "",
                snapshot.zone_id or "",
                snapshot.worker_state,
                snapshot.is_desired_active,
                snapshot.frames_captured,
                snapshot.events_published,
                snapshot.reconnect_attempts,
                snapshot.restart_attempts,
            )

    def _build_result(self) -> ActiveCameraSupervisorResult:
        snapshots = tuple(self._snapshots())
        return ActiveCameraSupervisorResult(
            frames_captured=sum(snapshot.frames_captured for snapshot in snapshots),
            events_published=sum(snapshot.events_published for snapshot in snapshots),
            outbox_path=str(self.config.outbox_path),
            publish_mode=self.config.publish_mode,
            destinations=_destinations(self.config.publish_mode),
            cameras_loaded=self.last_cameras_loaded,
            workers_started=self.workers_started_total,
            workers_stopped=sum(snapshot.worker_state in {"stopped", "disabled"} for snapshot in snapshots),
            workers_failed=sum(snapshot.worker_state == "failed" for snapshot in snapshots),
            cameras_connected=sum(snapshot.worker_state in {"connected", "running"} for snapshot in snapshots),
            cameras_failing=sum(snapshot.worker_state in {"disconnected", "retrying", "failed"} for snapshot in snapshots),
            last_refresh_change_count=self.last_refresh_change_count,
            states=snapshots,
        )

    def _snapshots(self) -> list[CameraWorkerSnapshot]:
        with self._lock:
            states = list(self.states.values())
        return [state.snapshot() for state in states]

    def _runtime_health(self, snapshots: list[CameraWorkerSnapshot]) -> SupervisorRuntimeHealth:
        with self._lock:
            workers_running = sum(1 for handle in self.handles.values() if handle.thread.is_alive())
            return SupervisorRuntimeHealth(
                cameras_loaded=self.last_cameras_loaded,
                desired_active_cameras=len(self.desired_cameras),
                workers_running=workers_running,
                last_refresh_at=self.last_refresh_at,
                last_refresh_change_count=self.last_refresh_change_count,
                last_refresh_error=self.last_refresh_error,
            )

    def _can_start_more_workers_locked(self, camera_id: str) -> bool:
        limit = self.config.active_camera_concurrency
        if limit is None:
            return True
        running = sum(1 for existing_id, handle in self.handles.items() if existing_id != camera_id and handle.thread.is_alive())
        return running < limit

    def _stop_all_workers(self, *, reason: str) -> None:
        with self._lock:
            camera_ids = list(self.handles)
        for camera_id in camera_ids:
            self._stop_worker(camera_id, final_status="stopped", desired_active=False, reason=reason)

    def _prepare_jsonl_outbox(self) -> None:
        mode = PublishMode.parse(self.config.publish_mode)
        if self.config.outbox_reset and mode in {PublishMode.JSONL, PublishMode.BOTH}:
            path = Path(self.config.outbox_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("", encoding="utf-8")

    def _start_health_server(self) -> None:
        if not self.config.active_camera_enable_health_server:
            return
        self._health_server = SupervisorHealthHttpServer(
            host=self.config.active_camera_health_host,
            port=self.config.active_camera_health_port,
            health_provider=self.health_document,
        )
        self._health_server.start()

    def _stop_health_server(self) -> None:
        if self._health_server is None:
            return
        self._health_server.stop()
        self._health_server = None

    def _default_runner_factory(
        self,
        config: IngestionConfig,
        storage: FrameStorage,
        publisher: FrameIngestedPublisher,
        lifecycle_callback: RtspLifecycleCallback,
        should_stop: ShouldStop,
    ) -> RtspRunner:
        return RtspRunner(
            config=config,
            storage=storage,
            publisher=publisher,
            lifecycle_callback=lifecycle_callback,
            should_stop=should_stop,
        )


def _destinations(publish_mode: str) -> tuple[str, ...]:
    mode = PublishMode.parse(publish_mode)
    if mode == PublishMode.JSONL:
        return ("jsonl",)
    if mode == PublishMode.RABBITMQ:
        return ("rabbitmq",)
    return ("jsonl", "rabbitmq")
