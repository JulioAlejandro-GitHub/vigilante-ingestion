from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from app.config import IngestionConfig
from app.main import _build_runner, _merge_cli, build_parser
from app.runner.active_camera_supervisor import ActiveCameraSupervisor
from app.runner.replay_runner import ReplayRunner
from app.runner.rtsp_runner import RtspRunner
from app.services.active_camera_loader import ActiveCamera


CAMERA_A = "11111111-1111-1111-1111-111111111111"
CAMERA_B = "22222222-2222-2222-2222-222222222222"


def test_supervisor_builds_one_worker_per_active_camera_and_uses_pipeline_factories(tmp_path) -> None:
    storage_configs: list[str] = []
    publisher_configs: list[str] = []
    runner_configs: list[str] = []

    supervisor = ActiveCameraSupervisor(
        config=_config(tmp_path),
        camera_loader=lambda _config: [_camera(CAMERA_A), _camera(CAMERA_B)],
        storage_factory=lambda config: storage_configs.append(config.camera_id) or _FakeStorage(),
        publisher_factory=lambda config: publisher_configs.append(config.camera_id) or _FakePublisher(),
        runner_factory=lambda config, storage, publisher, callback: runner_configs.append(config.camera_id)
        or _FakeRunner(callback=callback, result=_result(frames=1)),
        sleep=lambda _delay: None,
    )

    result = supervisor.run()

    assert result.cameras_loaded == 2
    assert result.workers_started == 2
    assert result.workers_stopped == 2
    assert result.frames_captured == 2
    assert result.events_published == 2
    assert sorted(storage_configs) == [CAMERA_A, CAMERA_B]
    assert sorted(publisher_configs) == [CAMERA_A, CAMERA_B]
    assert sorted(runner_configs) == [CAMERA_A, CAMERA_B]


def test_supervisor_isolates_failed_camera_from_other_workers(tmp_path) -> None:
    def runner_factory(config, storage, publisher, callback):
        if config.camera_id == CAMERA_A:
            return _FailingRunner(callback=callback, error=RuntimeError("camera unavailable"))
        return _FakeRunner(callback=callback, result=_result(frames=2))

    supervisor = ActiveCameraSupervisor(
        config=_config(tmp_path, rtsp_max_reconnect_attempts=0),
        camera_loader=lambda _config: [_camera(CAMERA_A), _camera(CAMERA_B)],
        storage_factory=lambda _config: _FakeStorage(),
        publisher_factory=lambda _config: _FakePublisher(),
        runner_factory=runner_factory,
        sleep=lambda _delay: None,
    )

    result = supervisor.run()

    assert result.workers_started == 2
    assert result.workers_failed == 1
    assert result.workers_stopped == 1
    assert result.frames_captured == 2
    assert {state.camera_id: state.status for state in result.states} == {
        CAMERA_A: "failed",
        CAMERA_B: "stopped",
    }


def test_supervisor_restarts_failed_camera_with_backoff(tmp_path) -> None:
    attempts: dict[str, int] = {CAMERA_A: 0}
    sleeps: list[float] = []

    def runner_factory(config, storage, publisher, callback):
        attempts[config.camera_id] += 1
        if attempts[config.camera_id] == 1:
            return _FailingRunner(callback=callback, error=RuntimeError("temporary publish failure"))
        return _FakeRunner(callback=callback, result=_result(frames=1))

    supervisor = ActiveCameraSupervisor(
        config=_config(
            tmp_path,
            rtsp_reconnect_initial_delay_seconds=0,
            rtsp_reconnect_max_delay_seconds=0,
            rtsp_max_reconnect_attempts=1,
        ),
        camera_loader=lambda _config: [_camera(CAMERA_A)],
        storage_factory=lambda _config: _FakeStorage(),
        publisher_factory=lambda _config: _FakePublisher(),
        runner_factory=runner_factory,
        sleep=sleeps.append,
    )

    result = supervisor.run()

    assert attempts[CAMERA_A] == 2
    assert sleeps == [0]
    assert result.workers_failed == 0
    assert result.workers_stopped == 1
    assert result.frames_captured == 1
    assert result.states[0].restart_attempts == 1


def test_build_runner_keeps_supervisor_separate_from_existing_modes(tmp_path) -> None:
    active_runner = _build_runner(config=_config(tmp_path))
    rtsp_runner = _build_runner(
        config=IngestionConfig(
            source_file=Path("samples/cam01.mp4"),
            camera_id=CAMERA_A,
            source_type="rtsp",
            rtsp_url="rtsp://127.0.0.1:8554/cam01",
        ),
        storage=_FakeStorage(),
        publisher=_FakePublisher(),
    )
    replay_runner = _build_runner(
        config=IngestionConfig(source_file=Path("samples/cam01.mp4"), camera_id=CAMERA_A),
        storage=_FakeStorage(),
        publisher=_FakePublisher(),
    )

    assert isinstance(active_runner, ActiveCameraSupervisor)
    assert isinstance(rtsp_runner, RtspRunner)
    assert isinstance(replay_runner, ReplayRunner)


def test_cli_active_cameras_loads_all_by_default_even_with_env_camera_id(tmp_path) -> None:
    base_config = IngestionConfig(
        source_file=Path("samples/cam01.mp4"),
        camera_id=CAMERA_A,
        external_camera_key="cam01",
        site_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        zone_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
    )
    args = build_parser().parse_args(
        [
            "--source-type",
            "active_cameras",
            "--camera-db-url",
            "postgresql://localhost/vigilante_api",
        ]
    )

    merged = _merge_cli(base_config, args)

    assert merged.source_type == "active_cameras"
    assert merged.camera_id is None
    assert merged.external_camera_key is None
    assert merged.site_id is None
    assert merged.zone_id is None


def _config(tmp_path: Path, **overrides) -> IngestionConfig:
    values = {
        "source_file": Path("samples/cam01.mp4"),
        "source_type": "active_cameras",
        "camera_config_db_url": "postgresql://localhost/vigilante_api",
        "capture_fps": 1,
        "max_frames": 1,
        "outbox_path": tmp_path / "outbox" / "frame_ingested.jsonl",
        "active_camera_status_interval_seconds": 0.01,
    }
    values.update(overrides)
    return IngestionConfig(**values)


def _camera(camera_id: str) -> ActiveCamera:
    return ActiveCamera(
        camera_id=camera_id,
        external_camera_key=f"key-{camera_id[:4]}",
        site_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        zone_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        name=f"Camera {camera_id[:4]}",
        rtsp_url=f"rtsp://admin:secret@camera-{camera_id[:4]}.local:554/live",
        safe_rtsp_url=f"rtsp://admin:***@camera-{camera_id[:4]}.local:554/live",
        rtsp_transport="tcp",
    )


@dataclass(frozen=True)
class _Result:
    frames_captured: int
    events_published: int
    outbox_path: str
    publish_mode: str
    destinations: tuple[str, ...]
    stream_opens: int
    reconnect_attempts: int
    frames_stored: int
    read_failures: int
    store_failures: int
    publish_failures: int


def _result(*, frames: int) -> _Result:
    return _Result(
        frames_captured=frames,
        events_published=frames,
        outbox_path="outbox/frame_ingested.jsonl",
        publish_mode="jsonl",
        destinations=("jsonl",),
        stream_opens=1,
        reconnect_attempts=0,
        frames_stored=frames,
        read_failures=0,
        store_failures=0,
        publish_failures=0,
    )


class _FakeRunner:
    def __init__(self, *, callback, result: _Result) -> None:
        self.callback = callback
        self.result = result

    def run(self) -> _Result:
        self.callback("camera_stream_connected", {"counters": {"stream_opens": 1}})
        self.callback(
            "camera_frame_ingested",
            {
                "counters": {
                    "frames_captured": self.result.frames_captured,
                    "frames_stored": self.result.frames_stored,
                    "events_published": self.result.events_published,
                    "stream_opens": self.result.stream_opens,
                    "reconnect_attempts": self.result.reconnect_attempts,
                }
            },
        )
        return self.result


class _FailingRunner:
    def __init__(self, *, callback, error: Exception) -> None:
        self.callback = callback
        self.error = error

    def run(self):
        self.callback(
            "camera_stream_disconnected",
            {"error": str(self.error), "counters": {"read_failures": 1, "stream_opens": 1}},
        )
        raise self.error


class _FakeStorage:
    pass


class _FakePublisher:
    pass
