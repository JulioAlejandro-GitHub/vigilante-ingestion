from __future__ import annotations

import json
import urllib.request
from datetime import datetime, timezone

from app.routes.health import SupervisorHealthHttpServer
from app.services.active_camera_loader import ActiveCamera
from app.services.camera_worker_state import CameraWorkerState
from app.services.supervisor_health_service import (
    SupervisorRuntimeHealth,
    build_camera_health,
    build_health_document,
    build_health_summary,
)


CAMERA_A = "11111111-1111-1111-1111-111111111111"
CAMERA_B = "22222222-2222-2222-2222-222222222222"


def test_camera_health_reflects_state_transitions() -> None:
    state = CameraWorkerState(camera=_camera(CAMERA_A))

    state.mark("starting")
    state.apply_runner_event("camera_stream_connected", {"counters": {"stream_opens": 1}})
    state.apply_runner_event(
        "camera_frame_ingested",
        {"counters": {"frames_captured": 1, "frames_stored": 1, "events_published": 1}},
    )

    health = build_camera_health([state.snapshot()])[0]

    assert health["worker_state"] == "running"
    assert health["is_desired_active"] is True
    assert health["last_started_at"] is not None
    assert health["last_connected_at"] is not None
    assert health["last_frame_at"] is not None
    assert health["last_publish_at"] is not None
    assert health["frames_captured"] == 1
    assert health["events_published"] == 1
    assert health["config_version_hash"] == "hash-1111"


def test_global_health_summary_aggregates_worker_counts() -> None:
    running = CameraWorkerState(camera=_camera(CAMERA_A))
    retrying = CameraWorkerState(camera=_camera(CAMERA_B))
    running.apply_runner_event("camera_frame_ingested", {"counters": {"frames_captured": 2, "events_published": 2}})
    retrying.apply_runner_event(
        "camera_stream_reconnect_scheduled",
        {"error": "timeout", "counters": {"reconnect_attempts": 3}},
    )

    summary = build_health_summary(
        [running.snapshot(), retrying.snapshot()],
        SupervisorRuntimeHealth(
            cameras_loaded=2,
            desired_active_cameras=2,
            workers_running=2,
            last_refresh_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            last_refresh_change_count=1,
        ),
    )

    assert summary["status"] == "ok"
    assert summary["cameras_loaded"] == 2
    assert summary["desired_active_cameras"] == 2
    assert summary["workers_running"] == 2
    assert summary["workers_connected"] == 1
    assert summary["workers_retrying"] == 1
    assert summary["frames_captured_total"] == 2
    assert summary["events_published_total"] == 2
    assert summary["last_refresh_change_count"] == 1


def test_health_http_server_exposes_summary_and_cameras() -> None:
    document = build_health_document(
        [CameraWorkerState(camera=_camera(CAMERA_A)).snapshot()],
        SupervisorRuntimeHealth(
            cameras_loaded=1,
            desired_active_cameras=1,
            workers_running=0,
            last_refresh_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            last_refresh_change_count=1,
        ),
    )
    server = SupervisorHealthHttpServer(host="127.0.0.1", port=0, health_provider=lambda: document)
    server.start()
    try:
        base_url = f"http://127.0.0.1:{server.bound_port}"
        summary = _get_json(f"{base_url}/health/summary")
        cameras = _get_json(f"{base_url}/health/cameras")
        full = _get_json(f"{base_url}/health")
    finally:
        server.stop()

    assert summary["cameras_loaded"] == 1
    assert cameras[0]["camera_id"] == CAMERA_A
    assert full["summary"]["desired_active_cameras"] == 1


def _get_json(url: str):
    with urllib.request.urlopen(url, timeout=2) as response:
        return json.loads(response.read().decode("utf-8"))


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
        config_version_hash=f"hash-{camera_id[:4]}",
    )
