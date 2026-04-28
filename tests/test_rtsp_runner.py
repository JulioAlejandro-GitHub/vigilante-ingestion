from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from app.config import IngestionConfig
from app.models.frame import CapturedFrame, StoredFrame
from app.publisher.frame_ingested_publisher import OutboxFilePublisher
from app.runner.rtsp_runner import RtspRunner
from app.storage.local_storage import LocalFrameStorage
from app.capture.stream_reader import StreamOpenError, StreamReadTimeout


CAMERA_ID = "11111111-1111-1111-1111-111111111111"


def test_rtsp_runner_reconnects_after_read_failure_and_continues() -> None:
    sources = [
        _FakeRtspSource(frame_count=1, fail_after=StreamReadTimeout("temporary timeout")),
        _FakeRtspSource(frame_count=2),
    ]
    publisher = _FakePublisher()
    sleeps: list[float] = []
    runner = RtspRunner(
        config=_config(max_frames=3),
        storage=_FakeStorage(),
        publisher=publisher,
        source_factory=lambda: sources.pop(0),
        sleep=sleeps.append,
    )

    result = runner.run()

    assert result.frames_captured == 3
    assert result.frames_stored == 3
    assert result.events_published == 3
    assert result.stream_opens == 2
    assert result.reconnect_attempts == 1
    assert result.read_failures == 1
    assert sleeps == [0]
    assert publisher.closed is True
    assert [event["payload"]["metadata"]["sample_index"] for event in publisher.events] == [0, 1, 2]
    assert {event["payload"]["source_type"] for event in publisher.events} == {"rtsp"}


def test_rtsp_runner_raises_when_reconnect_attempts_are_exhausted() -> None:
    attempts = 0
    sleeps: list[float] = []
    publisher = _FakePublisher()

    def source_factory():
        nonlocal attempts
        attempts += 1
        return _UnavailableRtspSource()

    runner = RtspRunner(
        config=_config(max_frames=1, max_reconnect_attempts=2),
        storage=_FakeStorage(),
        publisher=publisher,
        source_factory=source_factory,
        sleep=sleeps.append,
    )

    with pytest.raises(StreamOpenError):
        runner.run()

    assert attempts == 3
    assert sleeps == [0, 0]
    assert publisher.closed is True


def test_rtsp_runner_uses_existing_local_storage_and_jsonl_contract(tmp_path) -> None:
    config = _config(
        max_frames=1,
        local_storage_dir=tmp_path / "storage",
        outbox_path=tmp_path / "outbox" / "frame_ingested.jsonl",
    )
    runner = RtspRunner(
        config=config,
        storage=LocalFrameStorage(config.local_storage_dir),
        publisher=OutboxFilePublisher(config.outbox_path, reset=True),
        source_factory=lambda: _FakeRtspSource(frame_count=1),
        sleep=lambda _: None,
    )

    result = runner.run()
    event = json.loads(config.outbox_path.read_text(encoding="utf-8").splitlines()[0])

    assert result.frames_captured == 1
    assert result.events_published == 1
    assert Path(event["payload"]["frame_ref"]).exists()
    assert event["payload"]["frame_uri"] == event["payload"]["frame_ref"]
    assert event["payload"]["camera_id"] == CAMERA_ID
    assert event["payload"]["source_type"] == "rtsp"
    assert event["payload"]["content_type"] == "image/jpeg"
    assert event["payload"]["metadata"]["source_uri"] == "rtsp://127.0.0.1:8554/cam01"


def _config(
    *,
    max_frames: int,
    local_storage_dir: Path | None = None,
    outbox_path: Path | None = None,
    max_reconnect_attempts: int | None = None,
) -> IngestionConfig:
    return IngestionConfig(
        source_file=Path("samples/cam01.mp4"),
        source_type="rtsp",
        rtsp_url="rtsp://127.0.0.1:8554/cam01",
        camera_id=CAMERA_ID,
        capture_fps=1,
        max_frames=max_frames,
        local_storage_dir=local_storage_dir or Path("storage"),
        outbox_path=outbox_path or Path("outbox/frame_ingested.jsonl"),
        replay=False,
        rtsp_reconnect_initial_delay_seconds=0,
        rtsp_reconnect_max_delay_seconds=0,
        rtsp_max_reconnect_attempts=max_reconnect_attempts,
    )


class _FakeRtspSource:
    def __init__(self, *, frame_count: int, fail_after: Exception | None = None) -> None:
        self.frame_count = frame_count
        self.fail_after = fail_after

    def iter_frames(self, *, max_frames: int | None, start_sample_index: int = 0):
        frame_count = self.frame_count if max_frames is None else min(self.frame_count, max_frames)
        for offset in range(frame_count):
            sample_index = start_sample_index + offset
            yield _frame(sample_index=sample_index)
        if self.fail_after is not None:
            raise self.fail_after


class _UnavailableRtspSource:
    def iter_frames(self, *, max_frames: int | None, start_sample_index: int = 0):
        raise StreamOpenError("stream unavailable")
        yield


class _FakeStorage:
    def save(self, frame: CapturedFrame) -> StoredFrame:
        return StoredFrame(
            frame_ref=f"s3://vigilante-frames/frames/{frame.camera_id}/{frame.sample_index}.jpg",
            frame_uri=f"s3://vigilante-frames/frames/{frame.camera_id}/{frame.sample_index}.jpg",
            object_key=f"frames/{frame.camera_id}/{frame.sample_index}.jpg",
            metadata_ref=f"s3://vigilante-frames/frames/{frame.camera_id}/{frame.sample_index}.json",
            storage_backend="minio",
            content_type=frame.content_type,
            size_bytes=len(frame.image_bytes),
        )


class _FakePublisher:
    def __init__(self) -> None:
        self.events: list[dict] = []
        self.closed = False

    def publish(self, event: dict) -> None:
        self.events.append(event)

    def close(self) -> None:
        self.closed = True


def _frame(*, sample_index: int) -> CapturedFrame:
    captured_at = datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=sample_index)
    return CapturedFrame(
        image_bytes=b"\xff\xd8test-jpeg\xff\xd9",
        camera_id=CAMERA_ID,
        captured_at=captured_at,
        width=160,
        height=90,
        content_type="image/jpeg",
        source_type="rtsp",
        source_uri="rtsp://127.0.0.1:8554/cam01",
        source_timestamp_seconds=float(sample_index),
        source_frame_index=sample_index,
        sample_index=sample_index,
        capture_fps=1,
        source_fps=1,
    )
