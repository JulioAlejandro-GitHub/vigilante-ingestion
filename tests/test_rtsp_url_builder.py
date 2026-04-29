from __future__ import annotations

import pytest

from app.services.camera_secret_service import CameraSecretError, encrypt_camera_secret
from app.services.rtsp_url_builder import CameraConfigError, RtspCameraConfig, build_rtsp_url_from_camera_config


FERNET_KEY = "MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA="


def test_builds_rtsp_url_from_structured_camera_columns() -> None:
    encrypted_secret = encrypt_camera_secret("admin123", key=FERNET_KEY)

    result = build_rtsp_url_from_camera_config(
        RtspCameraConfig(
            source_type="rtsp",
            camera_hostname="192.168.100.143",
            camera_port=554,
            camera_path="/cam/realmonitor",
            rtsp_transport="tcp",
            channel=1,
            subtype=0,
            camera_user="admin",
            camera_secret=encrypted_secret,
            metadata={
                "stream_url": "rtsp://wrong:wrong@10.0.0.1:8554/legacy",
                "camera_pass": "wrong",
            },
        ),
        camera_secret_fernet_key=FERNET_KEY,
    )

    assert result.url == "rtsp://admin:admin123@192.168.100.143:554/cam/realmonitor?channel=1&subtype=0"
    assert result.safe_url == "rtsp://admin:***@192.168.100.143:554/cam/realmonitor?channel=1&subtype=0"
    assert result.rtsp_transport == "tcp"
    assert "admin123" not in result.safe_url


def test_falls_back_to_metadata_when_structured_columns_are_missing() -> None:
    result = build_rtsp_url_from_camera_config(
        RtspCameraConfig(
            metadata={
                "source_type": "rtsp",
                "camera_hostname": "camera.local",
                "camera_port": "8554",
                "camera_path": "live",
                "camera_user": "operator",
                "camera_pass": "legacy-secret",
                "rtsp_transport": "udp",
                "channel": "2",
                "subtype": "1",
            }
        ),
        camera_secret_fernet_key=None,
    )

    assert result.url == "rtsp://operator:legacy-secret@camera.local:8554/live?channel=2&subtype=1"
    assert result.safe_url == "rtsp://operator:***@camera.local:8554/live?channel=2&subtype=1"
    assert result.rtsp_transport == "udp"


def test_derives_missing_legacy_fields_from_metadata_stream_url_last() -> None:
    result = build_rtsp_url_from_camera_config(
        RtspCameraConfig(
            source_type="rtsp",
            metadata={
                "stream_url": "rtsp://user:legacy-secret@legacy.local:554/cam/realmonitor?channel=1&subtype=0",
            },
        ),
        camera_secret_fernet_key=None,
    )

    assert result.url == "rtsp://user:legacy-secret@legacy.local:554/cam/realmonitor?channel=1&subtype=0"
    assert result.safe_url == "rtsp://user:***@legacy.local:554/cam/realmonitor?channel=1&subtype=0"


def test_reports_invalid_encrypted_secret_without_leaking_ciphertext() -> None:
    with pytest.raises(CameraSecretError) as exc:
        build_rtsp_url_from_camera_config(
            RtspCameraConfig(
                source_type="rtsp",
                camera_hostname="camera.local",
                camera_path="/live",
                camera_user="admin",
                camera_secret="fernet:v1:not-a-valid-token",
            ),
            camera_secret_fernet_key=FERNET_KEY,
        )

    assert str(exc.value) == "camera_secret could not be decrypted"
    assert "not-a-valid-token" not in str(exc.value)


def test_requires_complete_rtsp_credentials_when_one_side_is_present() -> None:
    with pytest.raises(CameraConfigError, match="both camera_user and camera_secret"):
        build_rtsp_url_from_camera_config(
            RtspCameraConfig(
                source_type="rtsp",
                camera_hostname="camera.local",
                camera_path="/live",
                camera_user="admin",
            ),
            camera_secret_fernet_key=None,
        )


def test_validates_rtsp_transport_and_port() -> None:
    with pytest.raises(CameraConfigError, match="camera_port"):
        build_rtsp_url_from_camera_config(
            RtspCameraConfig(source_type="rtsp", camera_hostname="camera.local", camera_path="/live", camera_port="bad"),
            camera_secret_fernet_key=None,
        )

    with pytest.raises(CameraConfigError, match="rtsp_transport"):
        build_rtsp_url_from_camera_config(
            RtspCameraConfig(source_type="rtsp", camera_hostname="camera.local", camera_path="/live", rtsp_transport="http"),
            camera_secret_fernet_key=None,
        )
