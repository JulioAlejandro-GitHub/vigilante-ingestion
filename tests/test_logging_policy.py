from __future__ import annotations

import logging

from app.logging_config import apply_runtime_log_level_file, current_log_level_name, set_log_level
from app.publisher.frame_ingested_publisher import OutboxFilePublisher


def _event() -> dict:
    return {
        "event_id": "evt_frame_001",
        "event_type": "frame.ingested",
        "context": {"camera_id": "cam-1", "run_id": "run-1"},
        "payload": {
            "camera_id": "cam-1",
            "frame_ref": "s3://vigilante-frames/frames/cam-1/frame-001.jpg",
            "metadata": {
                "camera_runtime_config": {"recognition": {"vlm_policy": {"backend_chain": ["qwen", "smolvlm", "simple"]}}},
                "very_large_payload": "x" * 2000,
            },
        },
    }


def test_outbox_publish_info_is_compact_and_debug_keeps_payload(caplog, tmp_path) -> None:
    publisher = OutboxFilePublisher(tmp_path / "frame_ingested.jsonl")

    with caplog.at_level(logging.INFO):
        publisher.publish(_event())

    info_text = "\n".join(record.getMessage() for record in caplog.records if record.levelno == logging.INFO)
    assert "frame_ingested_published_jsonl event_id=evt_frame_001" in info_text
    assert "camera_id=cam-1" in info_text
    assert "very_large_payload" not in info_text
    assert "backend_chain" not in info_text

    caplog.clear()
    publisher = OutboxFilePublisher(tmp_path / "frame_ingested_debug.jsonl")
    with caplog.at_level(logging.DEBUG):
        publisher.publish(_event())

    debug_text = "\n".join(record.getMessage() for record in caplog.records if record.levelno == logging.DEBUG)
    assert "very_large_payload" in debug_text
    assert "backend_chain" in debug_text


def test_runtime_log_level_file_changes_level_without_restart(tmp_path) -> None:
    previous_level = current_log_level_name()
    level_path = tmp_path / "log-level"
    try:
        level_path.write_text("DEBUG\n", encoding="utf-8")
        assert apply_runtime_log_level_file(level_path) == "DEBUG"
        assert current_log_level_name() == "DEBUG"

        level_path.write_text("INFO\n", encoding="utf-8")
        assert apply_runtime_log_level_file(level_path) == "INFO"
        assert current_log_level_name() == "INFO"
    finally:
        set_log_level(previous_level, source="test_restore", announce=False)
