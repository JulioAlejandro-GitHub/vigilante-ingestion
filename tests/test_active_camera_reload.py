from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from threading import Event

from app.config import IngestionConfig
from app.runner.active_camera_supervisor import ActiveCameraSupervisor
from app.services.active_camera_loader import ActiveCamera


CAMERA_A = "11111111-1111-1111-1111-111111111111"
CAMERA_B = "22222222-2222-2222-2222-222222222222"


def test_refresh_starts_new_camera_worker(tmp_path) -> None:
    active = [_camera(CAMERA_A)]
    starts: list[str] = []
    supervisor = _supervisor(tmp_path, active, starts=starts)

    try:
        assert supervisor.refresh_once() == 1
        assert _wait_for(lambda: CAMERA_A in starts)

        active.append(_camera(CAMERA_B))
        assert supervisor.refresh_once() == 1

        assert _wait_for(lambda: CAMERA_B in starts)
        assert sorted(supervisor.handles) == [CAMERA_A, CAMERA_B]
    finally:
        supervisor.stop()
        supervisor._stop_all_workers(reason="test_cleanup")


def test_refresh_stops_disabled_camera_worker(tmp_path) -> None:
    active = [_camera(CAMERA_A)]
    starts: list[str] = []
    supervisor = _supervisor(tmp_path, active, starts=starts)

    try:
        supervisor.refresh_once()
        assert _wait_for(lambda: CAMERA_A in starts)

        active.clear()
        assert supervisor.refresh_once() == 1

        assert CAMERA_A not in supervisor.handles
        assert supervisor.states[CAMERA_A].snapshot().worker_state == "disabled"
        assert supervisor.states[CAMERA_A].snapshot().is_desired_active is False
    finally:
        supervisor.stop()
        supervisor._stop_all_workers(reason="test_cleanup")


def test_refresh_restarts_camera_when_config_hash_changes(tmp_path) -> None:
    active = [_camera(CAMERA_A, config_hash="hash-v1")]
    starts: list[str] = []
    supervisor = _supervisor(tmp_path, active, starts=starts)

    try:
        supervisor.refresh_once()
        assert _wait_for(lambda: starts.count(CAMERA_A) == 1)

        active[0] = _camera(CAMERA_A, config_hash="hash-v2", path="/cam/changed")
        assert supervisor.refresh_once() == 1

        assert _wait_for(lambda: starts.count(CAMERA_A) == 2)
        assert supervisor.states[CAMERA_A].snapshot().config_version_hash == "hash-v2"
    finally:
        supervisor.stop()
        supervisor._stop_all_workers(reason="test_cleanup")


def test_refresh_failure_keeps_existing_workers_running(tmp_path) -> None:
    starts: list[str] = []
    calls = 0

    def loader(_config):
        nonlocal calls
        calls += 1
        if calls == 1:
            return [_camera(CAMERA_A)]
        raise RuntimeError("db unavailable")

    supervisor = ActiveCameraSupervisor(
        config=_config(tmp_path),
        camera_loader=loader,
        storage_factory=lambda _config: _FakeStorage(),
        publisher_factory=lambda _config: _FakePublisher(),
        runner_factory=lambda config, storage, publisher, callback, should_stop: _BlockingRunner(
            camera_id=config.camera_id,
            callback=callback,
            should_stop=should_stop,
            starts=starts,
        ),
        sleep=lambda _delay: None,
    )

    try:
        assert supervisor.refresh_once() == 1
        assert _wait_for(lambda: CAMERA_A in starts)

        assert supervisor.refresh_once() == 0

        assert CAMERA_A in supervisor.handles
        assert supervisor.handles[CAMERA_A].thread.is_alive()
        assert "RuntimeError" in (supervisor.last_refresh_error or "")
    finally:
        supervisor.stop()
        supervisor._stop_all_workers(reason="test_cleanup")


def test_run_performs_periodic_refresh(tmp_path) -> None:
    calls = 0

    def loader(_config):
        nonlocal calls
        calls += 1
        return [_camera(CAMERA_A)]

    supervisor = ActiveCameraSupervisor(
        config=_config(tmp_path, active_camera_refresh_seconds=0.01, active_camera_status_interval_seconds=1),
        camera_loader=loader,
        storage_factory=lambda _config: _FakeStorage(),
        publisher_factory=lambda _config: _FakePublisher(),
        runner_factory=lambda config, storage, publisher, callback, should_stop: _BlockingRunner(
            camera_id=config.camera_id,
            callback=callback,
            should_stop=should_stop,
            starts=[],
        ),
        sleep=lambda _delay: None,
        max_refresh_cycles=2,
    )

    result = supervisor.run()

    assert calls == 2
    assert result.cameras_loaded == 1
    assert result.workers_started == 1


def _supervisor(tmp_path: Path, active: list[ActiveCamera], *, starts: list[str]) -> ActiveCameraSupervisor:
    return ActiveCameraSupervisor(
        config=_config(tmp_path),
        camera_loader=lambda _config: list(active),
        storage_factory=lambda _config: _FakeStorage(),
        publisher_factory=lambda _config: _FakePublisher(),
        runner_factory=lambda config, storage, publisher, callback, should_stop: _BlockingRunner(
            camera_id=config.camera_id,
            callback=callback,
            should_stop=should_stop,
            starts=starts,
        ),
        sleep=lambda _delay: None,
    )


def _config(tmp_path: Path, **overrides) -> IngestionConfig:
    values = {
        "source_file": Path("samples/cam01.mp4"),
        "source_type": "active_cameras",
        "camera_config_db_url": "postgresql://localhost/vigilante_api",
        "capture_fps": 1,
        "outbox_path": tmp_path / "outbox" / "frame_ingested.jsonl",
        "active_camera_refresh_seconds": 30,
        "active_camera_status_interval_seconds": 30,
        "active_camera_stop_timeout_seconds": 1,
    }
    values.update(overrides)
    return IngestionConfig(**values)


def _camera(camera_id: str, *, config_hash: str | None = None, path: str = "/cam/realmonitor") -> ActiveCamera:
    return ActiveCamera(
        camera_id=camera_id,
        external_camera_key=f"key-{camera_id[:4]}",
        site_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        zone_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        name=f"Camera {camera_id[:4]}",
        rtsp_url=f"rtsp://admin:secret@camera-{camera_id[:4]}.local:554{path}",
        safe_rtsp_url=f"rtsp://admin:***@camera-{camera_id[:4]}.local:554{path}",
        rtsp_transport="tcp",
        config_version_hash=config_hash or f"hash-{camera_id[:4]}",
    )


def _wait_for(predicate, *, timeout_seconds: float = 1.0) -> bool:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.001)
    return predicate()


@dataclass(frozen=True)
class _Result:
    frames_captured: int = 0
    events_published: int = 0
    outbox_path: str = "outbox/frame_ingested.jsonl"
    publish_mode: str = "jsonl"
    destinations: tuple[str, ...] = ("jsonl",)
    stream_opens: int = 1
    reconnect_attempts: int = 0
    frames_stored: int = 0
    read_failures: int = 0
    store_failures: int = 0
    publish_failures: int = 0


class _BlockingRunner:
    def __init__(self, *, camera_id: str, callback, should_stop, starts: list[str]) -> None:
        self.camera_id = camera_id
        self.callback = callback
        self.should_stop = should_stop
        self.starts = starts
        self.started = Event()

    def run(self) -> _Result:
        self.starts.append(self.camera_id)
        self.callback("camera_stream_connected", {"counters": {"stream_opens": 1}})
        while not self.should_stop():
            time.sleep(0.001)
        return _Result()


class _FakeStorage:
    pass


class _FakePublisher:
    pass
