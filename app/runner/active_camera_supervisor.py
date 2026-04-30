from __future__ import annotations

import logging
import time
from dataclasses import dataclass, replace
from pathlib import Path
from threading import Event, Thread
from typing import Any, Callable

from app.capture.reconnect_policy import ReconnectPolicy
from app.config import IngestionConfig
from app.publisher.publish_mode import FrameIngestedPublisher, PublishMode
from app.runner.rtsp_runner import RtspLifecycleCallback, RtspRunner
from app.services.active_camera_loader import ActiveCamera, load_active_rtsp_cameras
from app.services.camera_worker_state import CameraWorkerSnapshot, CameraWorkerState
from app.storage.frame_storage import FrameStorage

logger = logging.getLogger(__name__)

CameraLoader = Callable[[IngestionConfig], list[ActiveCamera]]
StorageFactory = Callable[[IngestionConfig], FrameStorage]
PublisherFactory = Callable[[IngestionConfig], FrameIngestedPublisher]
RunnerFactory = Callable[
    [IngestionConfig, FrameStorage, FrameIngestedPublisher, RtspLifecycleCallback],
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
    states: tuple[CameraWorkerSnapshot, ...]


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
    ) -> None:
        self.config = config
        self.storage_factory = storage_factory
        self.publisher_factory = publisher_factory
        self.camera_loader = camera_loader
        self.runner_factory = runner_factory or self._default_runner_factory
        self.sleep = sleep
        self.stop_event = Event()
        self.states: dict[str, CameraWorkerState] = {}
        self.threads: list[Thread] = []

    def run(self) -> ActiveCameraSupervisorResult:
        cameras = self.camera_loader(self.config)
        cameras_to_start = self._apply_concurrency_limit(cameras)
        self._prepare_jsonl_outbox()

        self.states = {camera.camera_id: CameraWorkerState(camera=camera) for camera in cameras_to_start}
        logger.info(
            "active_camera_supervisor_loaded cameras_loaded=%s workers_to_start=%s concurrency_limit=%s",
            len(cameras),
            len(cameras_to_start),
            self.config.active_camera_concurrency or "unbounded",
        )

        for camera in cameras_to_start:
            thread = Thread(
                target=self._run_camera_worker,
                name=f"camera-worker-{camera.camera_id}",
                args=(camera, self.states[camera.camera_id]),
                daemon=True,
            )
            self.threads.append(thread)
            thread.start()

        try:
            self._wait_for_workers()
        except KeyboardInterrupt:
            self.stop()
            raise

        result = self._build_result(cameras_loaded=len(cameras))
        logger.info(
            "active_camera_supervisor_stopped cameras_loaded=%s workers_started=%s workers_stopped=%s "
            "workers_failed=%s frames_captured=%s events_published=%s",
            result.cameras_loaded,
            result.workers_started,
            result.workers_stopped,
            result.workers_failed,
            result.frames_captured,
            result.events_published,
        )
        return result

    def stop(self) -> None:
        self.stop_event.set()

    def _run_camera_worker(self, camera: ActiveCamera, state: CameraWorkerState) -> None:
        worker_config = self._camera_config(camera)
        restart_policy = ReconnectPolicy(
            initial_delay_seconds=worker_config.rtsp_reconnect_initial_delay_seconds,
            max_delay_seconds=worker_config.rtsp_reconnect_max_delay_seconds,
            backoff_multiplier=worker_config.rtsp_reconnect_backoff_multiplier,
            max_reconnect_attempts=worker_config.rtsp_max_reconnect_attempts,
        )

        while not self.stop_event.is_set():
            state.mark("starting")
            logger.info(
                "camera_worker_starting camera_id=%s external_camera_key=%s site_id=%s zone_id=%s url=%s",
                camera.camera_id,
                camera.external_camera_key or "",
                camera.site_id or "",
                camera.zone_id or "",
                camera.safe_rtsp_url,
            )
            try:
                storage = self.storage_factory(worker_config)
                publisher = self.publisher_factory(worker_config)
                runner = self.runner_factory(
                    worker_config,
                    storage,
                    publisher,
                    lambda event_name, payload: state.apply_runner_event(event_name, payload),
                )
                result = runner.run()
                state.apply_runner_result(result)
                state.mark("stopped")
                logger.info(
                    "camera_worker_stopped camera_id=%s external_camera_key=%s frames_captured=%s events_published=%s",
                    camera.camera_id,
                    camera.external_camera_key or "",
                    state.snapshot().frames_captured,
                    state.snapshot().events_published,
                )
                return
            except Exception as exc:
                if self.stop_event.is_set():
                    state.mark("stopped", error=str(exc))
                    return
                logger.exception(
                    "camera_worker_failed camera_id=%s external_camera_key=%s error_type=%s",
                    camera.camera_id,
                    camera.external_camera_key or "",
                    type(exc).__name__,
                )
                if not restart_policy.can_retry():
                    state.mark("failed", error=str(exc))
                    logger.error(
                        "camera_worker_stopped camera_id=%s external_camera_key=%s status=failed restart_attempts=%s",
                        camera.camera_id,
                        camera.external_camera_key or "",
                        restart_policy.attempts,
                    )
                    return
                delay = restart_policy.next_delay()
                state.increment_restart()
                state.mark("retrying", error=str(exc))
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
                self.sleep(delay)

    def _camera_config(self, camera: ActiveCamera) -> IngestionConfig:
        return replace(
            self.config,
            source_type="rtsp",
            source_name=camera.name,
            camera_id=camera.camera_id,
            external_camera_key=camera.external_camera_key,
            site_id=camera.site_id,
            zone_id=camera.zone_id,
            rtsp_url=camera.rtsp_url,
            rtsp_transport=camera.rtsp_transport,
            replay=False,
            outbox_reset=False,
        )

    def _wait_for_workers(self) -> None:
        next_status_at = time.monotonic()
        while any(thread.is_alive() for thread in self.threads):
            for thread in self.threads:
                thread.join(timeout=min(self.config.active_camera_status_interval_seconds, 1.0))
            now = time.monotonic()
            if now >= next_status_at:
                self._log_status()
                next_status_at = now + self.config.active_camera_status_interval_seconds

    def _log_status(self) -> None:
        snapshots = [state.snapshot() for state in self.states.values()]
        connected = sum(snapshot.status == "connected" for snapshot in snapshots)
        failing = sum(snapshot.status in {"disconnected", "retrying", "failed"} for snapshot in snapshots)
        logger.info(
            "active_camera_supervisor_status cameras_active=%s cameras_connected=%s cameras_failing=%s "
            "frames_captured=%s events_published=%s",
            len(snapshots),
            connected,
            failing,
            sum(snapshot.frames_captured for snapshot in snapshots),
            sum(snapshot.events_published for snapshot in snapshots),
        )
        for snapshot in snapshots:
            logger.info(
                "camera_worker_status camera_id=%s external_camera_key=%s site_id=%s zone_id=%s status=%s "
                "frames_captured=%s events_published=%s reconnect_attempts=%s restart_attempts=%s",
                snapshot.camera_id,
                snapshot.external_camera_key or "",
                snapshot.site_id or "",
                snapshot.zone_id or "",
                snapshot.status,
                snapshot.frames_captured,
                snapshot.events_published,
                snapshot.reconnect_attempts,
                snapshot.restart_attempts,
            )

    def _build_result(self, *, cameras_loaded: int) -> ActiveCameraSupervisorResult:
        snapshots = tuple(state.snapshot() for state in self.states.values())
        return ActiveCameraSupervisorResult(
            frames_captured=sum(snapshot.frames_captured for snapshot in snapshots),
            events_published=sum(snapshot.events_published for snapshot in snapshots),
            outbox_path=str(self.config.outbox_path),
            publish_mode=self.config.publish_mode,
            destinations=_destinations(self.config.publish_mode),
            cameras_loaded=cameras_loaded,
            workers_started=len(self.threads),
            workers_stopped=sum(snapshot.status == "stopped" for snapshot in snapshots),
            workers_failed=sum(snapshot.status == "failed" for snapshot in snapshots),
            cameras_connected=sum(snapshot.status == "connected" for snapshot in snapshots),
            cameras_failing=sum(snapshot.status in {"disconnected", "retrying", "failed"} for snapshot in snapshots),
            states=snapshots,
        )

    def _apply_concurrency_limit(self, cameras: list[ActiveCamera]) -> list[ActiveCamera]:
        limit = self.config.active_camera_concurrency
        if limit is None or len(cameras) <= limit:
            return cameras
        skipped = cameras[limit:]
        for camera in skipped:
            logger.warning(
                "active_camera_not_started_due_to_concurrency_limit camera_id=%s external_camera_key=%s limit=%s",
                camera.camera_id,
                camera.external_camera_key or "",
                limit,
            )
        return cameras[:limit]

    def _prepare_jsonl_outbox(self) -> None:
        mode = PublishMode.parse(self.config.publish_mode)
        if self.config.outbox_reset and mode in {PublishMode.JSONL, PublishMode.BOTH}:
            path = Path(self.config.outbox_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("", encoding="utf-8")

    def _default_runner_factory(
        self,
        config: IngestionConfig,
        storage: FrameStorage,
        publisher: FrameIngestedPublisher,
        lifecycle_callback: RtspLifecycleCallback,
    ) -> RtspRunner:
        return RtspRunner(
            config=config,
            storage=storage,
            publisher=publisher,
            lifecycle_callback=lifecycle_callback,
        )


def _destinations(publish_mode: str) -> tuple[str, ...]:
    mode = PublishMode.parse(publish_mode)
    if mode == PublishMode.JSONL:
        return ("jsonl",)
    if mode == PublishMode.RABBITMQ:
        return ("rabbitmq",)
    return ("jsonl", "rabbitmq")
