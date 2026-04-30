from __future__ import annotations

from pathlib import Path

from app.config import IngestionConfig
from app.services.active_camera_loader import load_active_rtsp_cameras


CAMERA_A = "11111111-1111-1111-1111-111111111111"
CAMERA_B = "22222222-2222-2222-2222-222222222222"
CAMERA_C = "33333333-3333-3333-3333-333333333333"
SITE_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
ZONE_ID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"


def test_load_active_rtsp_cameras_filters_active_rtsp_rows() -> None:
    config = _config()

    cameras = load_active_rtsp_cameras(config, row_fetcher=lambda _config: _rows())

    assert [camera.camera_id for camera in cameras] == [CAMERA_A]
    assert cameras[0].external_camera_key == "cam-a"
    assert cameras[0].site_id == SITE_ID
    assert cameras[0].zone_id == ZONE_ID
    assert cameras[0].rtsp_url == "rtsp://admin:secret@192.168.100.143:554/cam/realmonitor?channel=1&subtype=0"
    assert cameras[0].safe_rtsp_url == "rtsp://admin:***@192.168.100.143:554/cam/realmonitor?channel=1&subtype=0"


def test_load_active_rtsp_cameras_applies_optional_filters() -> None:
    config = _config(external_camera_key="cam-b")
    rows = [
        _row(camera_id=CAMERA_A, external_camera_key="cam-a", site_id=SITE_ID, zone_id=ZONE_ID),
        _row(camera_id=CAMERA_B, external_camera_key="cam-b", site_id=SITE_ID, zone_id=ZONE_ID),
    ]

    cameras = load_active_rtsp_cameras(config, row_fetcher=lambda _config: rows)

    assert [camera.camera_id for camera in cameras] == [CAMERA_B]


def test_load_active_rtsp_cameras_supports_legacy_metadata_fallback() -> None:
    row = {
        "camera_id": CAMERA_A,
        "external_camera_key": "legacy-cam",
        "site_id": SITE_ID,
        "zone_id": ZONE_ID,
        "name": "Legacy camera",
        "is_active": True,
        "source_type": "rtsp",
        "camera_hostname": None,
        "camera_port": None,
        "camera_path": None,
        "rtsp_transport": None,
        "channel": None,
        "subtype": None,
        "camera_user": None,
        "camera_secret": None,
        "metadata": {
            "stream_url": "rtsp://legacy:legacy-secret@legacy.local:8554/live?channel=2&subtype=1",
        },
    }

    cameras = load_active_rtsp_cameras(_config(), row_fetcher=lambda _config: [row])

    assert len(cameras) == 1
    assert cameras[0].rtsp_url == "rtsp://legacy:legacy-secret@legacy.local:8554/live?channel=2&subtype=1"
    assert cameras[0].safe_rtsp_url == "rtsp://legacy:***@legacy.local:8554/live?channel=2&subtype=1"


def _config(**overrides) -> IngestionConfig:
    values = {
        "source_file": Path("samples/cam01.mp4"),
        "source_type": "active_cameras",
        "camera_config_db_url": "postgresql://localhost/vigilante_api",
        "capture_fps": 1,
    }
    values.update(overrides)
    return IngestionConfig(**values)


def _rows() -> list[dict]:
    return [
        _row(camera_id=CAMERA_A, external_camera_key="cam-a", site_id=SITE_ID, zone_id=ZONE_ID),
        _row(camera_id=CAMERA_B, external_camera_key="cam-b", is_active=False, site_id=SITE_ID, zone_id=ZONE_ID),
        _row(camera_id=CAMERA_C, external_camera_key="cam-c", source_type="video_file", site_id=SITE_ID, zone_id=ZONE_ID),
    ]


def _row(
    *,
    camera_id: str,
    external_camera_key: str,
    site_id: str | None = None,
    zone_id: str | None = None,
    is_active: bool = True,
    source_type: str = "rtsp",
) -> dict:
    return {
        "camera_id": camera_id,
        "external_camera_key": external_camera_key,
        "site_id": site_id,
        "zone_id": zone_id,
        "name": external_camera_key,
        "is_active": is_active,
        "source_type": source_type,
        "camera_hostname": "192.168.100.143",
        "camera_port": 554,
        "camera_path": "/cam/realmonitor",
        "rtsp_transport": "tcp",
        "channel": 1,
        "subtype": 0,
        "camera_user": "admin",
        "camera_secret": "secret",
        "metadata": {},
    }
