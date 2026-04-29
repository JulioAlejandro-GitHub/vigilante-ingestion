from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from app.config import IngestionConfig
from app.capture.rtsp_source import InvalidRtspUrl, RtspSource, mask_rtsp_credentials, read_jpeg_dimensions
from app.capture.stream_reader import StreamReadTimeout, mask_rtsp_credentials_in_text


CAMERA_ID = "11111111-1111-1111-1111-111111111111"


def test_rtsp_source_reads_sampled_frames_from_reader() -> None:
    clock = _Clock(datetime(2026, 1, 1, tzinfo=timezone.utc), step_seconds=0.5)
    source = RtspSource(
        rtsp_url="rtsp://127.0.0.1:8554/cam01",
        camera_id=CAMERA_ID,
        capture_fps=2,
        reader_factory=lambda: _FakeReader([_jpeg(width=160, height=90), _jpeg(width=320, height=180)]),
        clock=clock,
    )

    frames = list(source.iter_frames(max_frames=2, start_sample_index=5))

    assert [frame.sample_index for frame in frames] == [5, 6]
    assert [frame.source_frame_index for frame in frames] == [5, 6]
    assert frames[0].source_type == "rtsp"
    assert frames[0].source_uri == "rtsp://127.0.0.1:8554/cam01"
    assert frames[0].width == 160
    assert frames[0].height == 90
    assert frames[1].width == 320
    assert frames[1].height == 180
    assert frames[0].captured_at.isoformat() == "2026-01-01T00:00:00.500000+00:00"


def test_rtsp_source_masks_credentials_in_frame_metadata() -> None:
    source = RtspSource(
        rtsp_url="rtsp://user:secret@camera.local:554/stream",
        camera_id=CAMERA_ID,
        capture_fps=1,
        reader_factory=lambda: _FakeReader([_jpeg(width=160, height=90)]),
        clock=_Clock(datetime(2026, 1, 1, tzinfo=timezone.utc)),
    )

    frame = next(source.iter_frames(max_frames=1))

    assert frame.source_uri == "rtsp://user:***@camera.local:554/stream"
    assert mask_rtsp_credentials("rtsp://user:secret@camera.local/stream") == "rtsp://user:***@camera.local/stream"


def test_stream_error_text_masks_rtsp_credentials() -> None:
    text = "Input error for rtsp://user:secret@camera.local:554/stream"

    masked = mask_rtsp_credentials_in_text(text)

    assert masked == "Input error for rtsp://user:***@camera.local:554/stream"
    assert "secret" not in masked


def test_rtsp_source_rejects_invalid_url() -> None:
    with pytest.raises(InvalidRtspUrl):
        RtspSource(rtsp_url="http://127.0.0.1/live", camera_id=CAMERA_ID, capture_fps=1)


def test_ingestion_config_accepts_rtsp_source() -> None:
    config = IngestionConfig(
        source_file="samples/cam01.mp4",
        source_type="rtsp",
        rtsp_url="rtsp://127.0.0.1:8554/cam01",
        camera_id=CAMERA_ID,
        capture_fps=5,
    )

    assert config.source_type == "rtsp"
    assert config.rtsp_url == "rtsp://127.0.0.1:8554/cam01"
    assert config.capture_fps == 5


def test_ingestion_config_rejects_rtsp_without_url() -> None:
    with pytest.raises(ValueError, match="rtsp_url or camera_config_db_url is required"):
        IngestionConfig(
            source_file="samples/cam01.mp4",
            source_type="rtsp",
            rtsp_url=None,
            camera_id=CAMERA_ID,
        )


def test_ingestion_config_accepts_rtsp_with_camera_database_config() -> None:
    config = IngestionConfig(
        source_file="samples/cam01.mp4",
        source_type="rtsp",
        rtsp_url=None,
        camera_config_db_url="postgresql://localhost/vigilante_api",
        camera_id=CAMERA_ID,
    )

    assert config.camera_config_db_url == "postgresql://localhost/vigilante_api"


def test_rtsp_source_propagates_read_timeout_for_runner_reconnect() -> None:
    source = RtspSource(
        rtsp_url="rtsp://127.0.0.1:8554/cam01",
        camera_id=CAMERA_ID,
        capture_fps=1,
        reader_factory=lambda: _FakeReader([]),
        clock=_Clock(datetime(2026, 1, 1, tzinfo=timezone.utc)),
    )

    with pytest.raises(StreamReadTimeout):
        list(source.iter_frames(max_frames=1))


def test_read_jpeg_dimensions_parses_sof_segment() -> None:
    assert read_jpeg_dimensions(_jpeg(width=640, height=360)) == (640, 360)


class _FakeReader:
    def __init__(self, frames: list[bytes]) -> None:
        self.frames = list(frames)
        self.opened = False
        self.closed = False

    def open(self) -> None:
        self.opened = True

    def read_frame(self) -> bytes:
        if not self.frames:
            raise StreamReadTimeout("no frame")
        return self.frames.pop(0)

    def close(self) -> None:
        self.closed = True


class _Clock:
    def __init__(self, start: datetime, *, step_seconds: float = 1.0) -> None:
        self.current = start
        self.step = timedelta(seconds=step_seconds)
        self.calls = 0

    def __call__(self) -> datetime:
        if self.calls == 0:
            self.calls += 1
            return self.current
        self.current += self.step
        self.calls += 1
        return self.current


def _jpeg(*, width: int, height: int) -> bytes:
    return (
        b"\xff\xd8"
        b"\xff\xc0"
        b"\x00\x11"
        b"\x08"
        + height.to_bytes(2, "big")
        + width.to_bytes(2, "big")
        + b"\x03\x01\x11\x00\x02\x11\x00\x03\x11\x00"
        b"\xff\xd9"
    )
