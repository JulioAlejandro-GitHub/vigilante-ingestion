from __future__ import annotations

import json
from pathlib import Path

from app.config import IngestionConfig
from app.publisher.frame_ingested_publisher import OutboxFilePublisher
from app.runner.replay_runner import ReplayRunner
from app.storage.local_storage import LocalFrameStorage
from tests.conftest import make_sample_video


CAMERA_ID = "11111111-1111-1111-1111-111111111111"


def test_replay_runner_is_deterministic(tmp_path, ffmpeg_available, ffprobe_available) -> None:
    source_file = make_sample_video(tmp_path / "samples" / "cam01.mp4", ffmpeg=ffmpeg_available)
    config = IngestionConfig(
        source_file=source_file,
        camera_id=CAMERA_ID,
        capture_fps=1,
        max_frames=3,
        local_storage_dir=tmp_path / "storage",
        outbox_path=tmp_path / "outbox" / "frame_ingested.jsonl",
        ffmpeg_path=ffmpeg_available,
        ffprobe_path=ffprobe_available,
    )

    first_result = _run(config)
    first_events = config.outbox_path.read_text(encoding="utf-8")
    second_result = _run(config)
    second_events = config.outbox_path.read_text(encoding="utf-8")

    assert first_result.frames_captured == 3
    assert second_result.events_published == 3
    assert first_events == second_events
    parsed_events = [json.loads(line) for line in second_events.splitlines()]
    assert [event["payload"]["metadata"]["sample_index"] for event in parsed_events] == [0, 1, 2]
    for event in parsed_events:
        assert Path(event["payload"]["frame_ref"]).exists()


def _run(config: IngestionConfig):
    return ReplayRunner(
        config=config,
        storage=LocalFrameStorage(config.local_storage_dir),
        publisher=OutboxFilePublisher(config.outbox_path, reset=True),
    ).run()

