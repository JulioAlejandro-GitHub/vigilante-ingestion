from __future__ import annotations

from datetime import datetime, timezone

import pytest

from app.capture.video_file_source import SourceNotFoundError, VideoFileSource, VideoProbeError
from tests.conftest import make_sample_video


CAMERA_ID = "11111111-1111-1111-1111-111111111111"


def test_video_file_source_reads_metadata_and_extracts_frames(
    tmp_path,
    ffmpeg_available,
    ffprobe_available,
) -> None:
    source_file = make_sample_video(tmp_path / "cam01.mp4", ffmpeg=ffmpeg_available)
    source = VideoFileSource(
        source_file,
        camera_id=CAMERA_ID,
        ffmpeg_path=ffmpeg_available,
        ffprobe_path=ffprobe_available,
    )

    metadata = source.inspect()
    frames = list(
        source.iter_frames(
            capture_fps=1,
            max_frames=2,
            replay_start_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            replay=True,
        )
    )

    assert metadata.width == 160
    assert metadata.height == 90
    assert metadata.fps == pytest.approx(10)
    assert len(frames) == 2
    assert frames[0].image_bytes.startswith(b"\xff\xd8")
    assert frames[0].source_timestamp_seconds == 0
    assert frames[1].source_timestamp_seconds == 1
    assert frames[1].captured_at.isoformat() == "2026-01-01T00:00:01+00:00"


def test_video_file_source_rejects_missing_source(tmp_path) -> None:
    source = VideoFileSource(tmp_path / "missing.mp4", camera_id=CAMERA_ID)

    with pytest.raises(SourceNotFoundError):
        source.inspect()


def test_video_file_source_rejects_invalid_video(tmp_path, ffprobe_available) -> None:
    invalid_video = tmp_path / "invalid.mp4"
    invalid_video.write_text("not a video", encoding="utf-8")
    source = VideoFileSource(invalid_video, camera_id=CAMERA_ID, ffprobe_path=ffprobe_available)

    with pytest.raises(VideoProbeError):
        source.inspect()

