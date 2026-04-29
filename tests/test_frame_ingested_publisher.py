from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from app.config import IngestionConfig
from app.main import _build_publisher
from app.messaging.topology import FrameIngestedTopology
from app.models.frame import CapturedFrame, StoredFrame
from app.publisher.frame_ingested_publisher import OutboxFilePublisher, build_frame_ingested_event
from app.publisher.publish_mode import CompositePublisher, PublishMode
from app.publisher.rabbitmq_publisher import RabbitMQFrameIngestedPublisher


CAMERA_ID = "11111111-1111-1111-1111-111111111111"


def test_build_frame_ingested_event_matches_recognition_contract(tmp_path) -> None:
    frame = _frame()
    stored = _stored_frame(tmp_path)
    config = IngestionConfig(
        source_file=Path("samples/cam01.mp4"),
        camera_id=CAMERA_ID,
        capture_fps=1,
        max_frames=1,
        external_camera_key="cam01",
        organization_id="org_demo",
        site_id="site_demo",
    )

    event = build_frame_ingested_event(frame=frame, stored_frame=stored, config=config)

    assert event["event_type"] == "frame.ingested"
    assert event["event_version"] == "1.0"
    assert event["occurred_at"] == "2026-01-01T00:00:00.000Z"
    assert event["source"]["component"] == "vigilante-ingestion"
    assert event["payload"]["camera_id"] == CAMERA_ID
    assert event["payload"]["frame_ref"] == stored.frame_ref
    assert event["payload"]["frame_uri"] == stored.frame_uri
    assert event["payload"]["captured_at"] == "2026-01-01T00:00:00.000Z"
    assert event["payload"]["width"] == 160
    assert event["payload"]["height"] == 90
    assert event["payload"]["content_type"] == "image/jpeg"
    assert event["payload"]["source_type"] == "video_file"
    assert event["payload"]["external_camera_key"] == "cam01"
    assert event["context"]["idempotency_key"].startswith(f"frame:{CAMERA_ID}:video_file")
    assert event["context"]["organization_id"] == "org_demo"


def test_build_frame_ingested_event_preserves_remote_s3_frame_uri() -> None:
    frame = _frame()
    stored = StoredFrame(
        frame_ref="s3://vigilante-frames/frames/cam01/frame.jpg",
        frame_uri="s3://vigilante-frames/frames/cam01/frame.jpg",
        object_key="frames/cam01/frame.jpg",
        metadata_ref="s3://vigilante-frames/frames/cam01/frame.json",
        storage_backend="minio",
        content_type="image/jpeg",
        size_bytes=12,
    )
    config = IngestionConfig(
        source_file=Path("samples/cam01.mp4"),
        camera_id=CAMERA_ID,
        storage_backend="minio",
    )

    event = build_frame_ingested_event(frame=frame, stored_frame=stored, config=config)

    assert event["payload"]["frame_ref"] == "s3://vigilante-frames/frames/cam01/frame.jpg"
    assert event["payload"]["frame_uri"] == event["payload"]["frame_ref"]
    assert event["payload"]["metadata"]["frame_object_key"] == "frames/cam01/frame.jpg"
    assert event["payload"]["metadata"]["metadata_ref"] == "s3://vigilante-frames/frames/cam01/frame.json"
    assert event["payload"]["metadata"]["storage_backend"] == "minio"


def test_build_frame_ingested_event_masks_rtsp_secret_if_frame_uri_is_raw() -> None:
    frame = _frame(
        camera_id=CAMERA_ID,
        source_type="rtsp",
        source_uri="rtsp://admin:admin123@camera.local:554/live",
    )
    stored = StoredFrame(
        frame_ref="s3://vigilante-frames/frames/cam01/frame.jpg",
        frame_uri="s3://vigilante-frames/frames/cam01/frame.jpg",
        object_key="frames/cam01/frame.jpg",
        metadata_ref="s3://vigilante-frames/frames/cam01/frame.json",
        storage_backend="minio",
        content_type="image/jpeg",
        size_bytes=len(frame.image_bytes),
    )
    config = IngestionConfig(source_file=Path("samples/cam01.mp4"), camera_id=CAMERA_ID, source_type="rtsp", rtsp_url=frame.source_uri)

    event = build_frame_ingested_event(frame=frame, stored_frame=stored, config=config)

    assert event["payload"]["metadata"]["source_uri"] == "rtsp://admin:***@camera.local:554/live"
    assert "admin123" not in str(event)


def test_outbox_file_publisher_writes_jsonl(tmp_path) -> None:
    outbox = tmp_path / "outbox" / "frame_ingested.jsonl"
    publisher = OutboxFilePublisher(outbox, reset=True)

    publisher.publish({"event_id": "evt_1", "event_type": "frame.ingested"})

    lines = outbox.read_text(encoding="utf-8").splitlines()
    assert json.loads(lines[0]) == {"event_id": "evt_1", "event_type": "frame.ingested"}


def test_rabbitmq_publisher_declares_topology_and_publishes_to_frame_exchange() -> None:
    channel = _FakeChannel()
    publisher = RabbitMQFrameIngestedPublisher(
        host="localhost",
        port=5672,
        username="guest",
        password="guest",
        virtual_host="/",
        topology=FrameIngestedTopology(),
        connection_factory=lambda: _FakeConnection(channel),
    )

    publisher.publish({"event_id": "evt_1", "event_type": "frame.ingested", "payload": {"camera_id": CAMERA_ID}})

    assert ("vigilante.frames", "topic", True) in channel.exchange_declarations
    assert (
        "vigilante.recognition.frame_ingested",
        True,
        {
            "x-dead-letter-exchange": "vigilante.frames.dlx",
            "x-dead-letter-routing-key": "frame.ingested.dlq",
        },
    ) in channel.queue_declarations
    assert channel.published[0]["exchange"] == "vigilante.frames"
    assert channel.published[0]["routing_key"] == "frame.ingested"
    assert json.loads(channel.published[0]["body"])["event_id"] == "evt_1"


def test_publish_mode_parser_and_publisher_selection(tmp_path) -> None:
    assert PublishMode.parse("rabbitmq") == PublishMode.RABBITMQ

    jsonl_config = IngestionConfig(
        source_file=Path("samples/cam01.mp4"),
        camera_id=CAMERA_ID,
        publish_mode="jsonl",
        outbox_path=tmp_path / "outbox" / "frame_ingested.jsonl",
    )
    both_config = IngestionConfig(
        source_file=Path("samples/cam01.mp4"),
        camera_id=CAMERA_ID,
        publish_mode="both",
        outbox_path=tmp_path / "outbox" / "frame_ingested.jsonl",
    )

    assert isinstance(_build_publisher(jsonl_config), OutboxFilePublisher)
    assert isinstance(_build_publisher(both_config), CompositePublisher)


def _frame(
    *,
    camera_id: str = CAMERA_ID,
    source_type: str = "video_file",
    source_uri: str = "samples/cam01.mp4",
) -> CapturedFrame:
    return CapturedFrame(
        image_bytes=b"\xff\xd8test-jpeg\xff\xd9",
        camera_id=camera_id,
        captured_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        width=160,
        height=90,
        content_type="image/jpeg",
        source_type=source_type,
        source_uri=source_uri,
        source_timestamp_seconds=0,
        source_frame_index=0,
        sample_index=0,
        capture_fps=1,
        source_fps=10,
    )


def _stored_frame(tmp_path) -> StoredFrame:
    frame_ref = str(tmp_path / "storage" / "frame.jpg")
    return StoredFrame(
        frame_ref=frame_ref,
        frame_uri=frame_ref,
        object_key="frames/cam/frame.jpg",
        metadata_ref=str(tmp_path / "storage" / "frame.json"),
        storage_backend="local",
        content_type="image/jpeg",
        size_bytes=12,
    )


class _FakeConnection:
    is_closed = False

    def __init__(self, channel: "_FakeChannel") -> None:
        self._channel = channel

    def channel(self):
        return self._channel

    def close(self) -> None:
        self.is_closed = True


class _FakeChannel:
    is_open = True

    def __init__(self) -> None:
        self.exchange_declarations = []
        self.queue_declarations = []
        self.queue_bindings = []
        self.published = []
        self.confirmed = False

    def exchange_declare(self, *, exchange, exchange_type, durable):
        self.exchange_declarations.append((exchange, exchange_type, durable))

    def queue_declare(self, *, queue, durable, arguments=None):
        self.queue_declarations.append((queue, durable, arguments))

    def queue_bind(self, *, queue, exchange, routing_key):
        self.queue_bindings.append((queue, exchange, routing_key))

    def confirm_delivery(self):
        self.confirmed = True

    def basic_publish(self, *, exchange, routing_key, body, properties, mandatory):
        self.published.append(
            {
                "exchange": exchange,
                "routing_key": routing_key,
                "body": body,
                "properties": properties,
                "mandatory": mandatory,
            }
        )
