from __future__ import annotations

from app.services.camera_runtime_config_mapper import build_camera_runtime_config


CAMERA_ID = "11111111-1111-1111-1111-111111111111"


def test_build_camera_runtime_config_maps_recognition_metadata() -> None:
    runtime_config = build_camera_runtime_config(
        camera_id=CAMERA_ID,
        camera_metadata={
            "stream_url": "rtsp://admin:secret@camera.local/live",
            "recognition": {
                "version": "ops-v3",
                "enabled": True,
                "face_tuning": {
                    "det_size": [320, 320],
                    "detection_threshold": 0.65,
                    "max_faces": 2,
                    "face_quality_threshold": 0.6,
                    "min_face_bbox_size": 42,
                    "min_face_area_ratio": 0.01,
                },
                "vlm_policy": {
                    "enabled": True,
                    "backend": "auto",
                    "preferred_backend": "qwen",
                    "secondary_backend": "smolvlm",
                    "eligible_events": ["human_presence_no_face"],
                    "latency_budget_seconds": 12,
                    "memory_budget_mb": 4096,
                    "max_concurrency": 1,
                    "degradation_policy": "preferred_then_secondary_then_simple",
                    "api_key": "must-not-leak",
                },
            },
        },
    )

    assert runtime_config["schema_version"] == "camera_runtime_config_v1"
    assert runtime_config["config_source"] == "api.camera.metadata"
    assert runtime_config["camera_id"] == CAMERA_ID
    assert runtime_config["camera_config_version"] == "ops-v3"
    recognition = runtime_config["recognition"]
    assert recognition["face_tuning"]["det_size"] == [320, 320]
    assert recognition["vlm_policy"]["enable_for_event_types"] == ["human_presence_no_face"]
    assert recognition["vlm_policy"]["max_latency_seconds"] == 12
    assert recognition["vlm_policy"]["max_rss_mb"] == 4096
    assert recognition["vlm_policy"]["max_concurrent_inferences"] == 1
    assert "stream_url" not in str(runtime_config)
    assert "secret" not in str(runtime_config)
    assert "api_key" not in str(runtime_config)


def test_build_camera_runtime_config_returns_empty_without_recognition_namespace() -> None:
    assert build_camera_runtime_config(camera_id=CAMERA_ID, camera_metadata={"notes": "plain camera"}) == {}
