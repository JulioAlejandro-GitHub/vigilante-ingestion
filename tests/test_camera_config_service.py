from __future__ import annotations

from pathlib import Path

from app.config import IngestionConfig
from app.services.camera_config_service import apply_camera_database_config
from app.services.camera_secret_service import encrypt_camera_secret


FERNET_KEY = "MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA="
CAMERA_ID = "11111111-1111-1111-1111-111111111111"


def test_apply_camera_database_config_resolves_rtsp_url_from_api_camera_row() -> None:
    encrypted_secret = encrypt_camera_secret("admin123", key=FERNET_KEY)
    config = IngestionConfig(
        source_file=Path("samples/cam01.mp4"),
        camera_id=CAMERA_ID,
        source_type="rtsp",
        camera_config_db_url="postgresql://localhost/vigilante_api",
        camera_secret_fernet_key=FERNET_KEY,
    )

    resolved = apply_camera_database_config(
        config,
        row_fetcher=lambda _config: {
            "external_camera_key": "cam-rtsp",
            "site_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbb1",
            "source_type": "rtsp",
            "camera_hostname": "192.168.100.143",
            "camera_port": 554,
            "camera_path": "/cam/realmonitor",
            "rtsp_transport": "tcp",
            "channel": 1,
            "subtype": 0,
            "camera_user": "admin",
            "camera_secret": encrypted_secret,
            "metadata": {"stream_url": "rtsp://wrong:wrong@legacy.local:8554/live"},
        },
    )

    assert resolved.rtsp_url == "rtsp://admin:admin123@192.168.100.143:554/cam/realmonitor?channel=1&subtype=0"
    assert resolved.rtsp_transport == "tcp"
    assert resolved.external_camera_key == "cam-rtsp"
    assert resolved.site_id == "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbb1"
