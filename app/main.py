from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import replace
from pathlib import Path

from app.config import config_from_env, parse_bool, parse_datetime
from app.messaging.topology import FrameIngestedTopology
from app.publisher.frame_ingested_publisher import OutboxFilePublisher
from app.publisher.publish_mode import CompositePublisher, PublishMode
from app.publisher.rabbitmq_publisher import RabbitMQFrameIngestedPublisher
from app.runner.replay_runner import ReplayRunner
from app.storage.local_storage import LocalFrameStorage
from app.storage.minio_storage import MinioFrameStorage


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    config = _merge_cli(config_from_env(), args)
    logging.basicConfig(level=getattr(logging, config.log_level.upper(), logging.INFO))
    storage = _build_storage(config)
    publisher = _build_publisher(config)
    runner = ReplayRunner(config=config, storage=storage, publisher=publisher)

    try:
        result = runner.run()
    except Exception as exc:
        print(f"ingestion failed: {exc}", file=sys.stderr)
        return 1

    print(
        "ingestion completed "
        f"frames_captured={result.frames_captured} "
        f"events_published={result.events_published} "
        f"publish_mode={result.publish_mode} "
        f"destinations={','.join(result.destinations)} "
        f"outbox={result.outbox_path}"
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Replay a local video file as a virtual camera.")
    parser.add_argument("--source-file", type=Path, help="Local MP4 file to replay.")
    parser.add_argument("--camera-id", help="Canonical UUID from api.camera.camera_id.")
    parser.add_argument("--fps", type=float, help="Frame capture frequency.")
    parser.add_argument("--max-frames", type=int, help="Limit captured frames for tests or demos.")
    parser.add_argument("--storage-backend", choices=["local", "minio"], help="Frame storage backend.")
    parser.add_argument("--output-dir", type=Path, help="Local storage root directory.")
    parser.add_argument("--publish-mode", choices=[mode.value for mode in PublishMode], help="Publication mode: jsonl, rabbitmq or both.")
    parser.add_argument("--outbox", type=Path, help="JSONL outbox path for frame.ingested events.")
    parser.add_argument("--outbox-reset", choices=["true", "false"], help="Reset the outbox before publishing.")
    parser.add_argument("--append-outbox", action="store_true", help="Append to the configured outbox.")
    parser.add_argument("--replay", choices=["true", "false"], help="Use deterministic replay timestamps.")
    parser.add_argument("--replay-start-at", help="UTC replay base timestamp, e.g. 2026-01-01T00:00:00Z.")
    parser.add_argument("--external-camera-key", help="Optional logical/external camera key.")
    parser.add_argument("--organization-id", help="Optional organization context.")
    parser.add_argument("--site-id", help="Optional site context.")
    parser.add_argument("--minio-endpoint", help="MinIO endpoint, e.g. localhost:9000.")
    parser.add_argument("--minio-access-key", help="MinIO access key.")
    parser.add_argument("--minio-secret-key", help="MinIO secret key.")
    parser.add_argument("--minio-bucket", help="MinIO bucket.")
    parser.add_argument("--minio-secure", choices=["true", "false"], help="Use TLS for MinIO.")
    parser.add_argument("--rabbitmq-host", help="RabbitMQ host.")
    parser.add_argument("--rabbitmq-port", type=int, help="RabbitMQ AMQP port.")
    parser.add_argument("--rabbitmq-user", help="RabbitMQ username.")
    parser.add_argument("--rabbitmq-password", help="RabbitMQ password.")
    parser.add_argument("--rabbitmq-vhost", help="RabbitMQ virtual host.")
    parser.add_argument("--rabbitmq-frame-exchange", help="RabbitMQ exchange for frame.ingested.")
    parser.add_argument("--rabbitmq-frame-routing-key", help="RabbitMQ routing key for frame.ingested.")
    parser.add_argument("--log-level", help="Python log level.")
    return parser


def _merge_cli(config, args):
    updates = {}
    mapping = {
        "source_file": args.source_file,
        "camera_id": args.camera_id,
        "capture_fps": args.fps,
        "max_frames": args.max_frames,
        "storage_backend": args.storage_backend,
        "local_storage_dir": args.output_dir,
        "publish_mode": args.publish_mode,
        "outbox_path": args.outbox,
        "external_camera_key": args.external_camera_key,
        "organization_id": args.organization_id,
        "site_id": args.site_id,
        "minio_endpoint": args.minio_endpoint,
        "minio_access_key": args.minio_access_key,
        "minio_secret_key": args.minio_secret_key,
        "minio_bucket": args.minio_bucket,
        "rabbitmq_host": args.rabbitmq_host,
        "rabbitmq_port": args.rabbitmq_port,
        "rabbitmq_user": args.rabbitmq_user,
        "rabbitmq_password": args.rabbitmq_password,
        "rabbitmq_vhost": args.rabbitmq_vhost,
        "rabbitmq_frame_exchange": args.rabbitmq_frame_exchange,
        "rabbitmq_frame_routing_key": args.rabbitmq_frame_routing_key,
        "log_level": args.log_level,
    }
    updates.update({key: value for key, value in mapping.items() if value is not None})
    if args.outbox_reset is not None:
        updates["outbox_reset"] = parse_bool(args.outbox_reset)
    if args.append_outbox:
        updates["outbox_reset"] = False
    if args.replay is not None:
        updates["replay"] = parse_bool(args.replay)
    if args.replay_start_at is not None:
        updates["replay_start_at"] = parse_datetime(args.replay_start_at)
    if args.minio_secure is not None:
        updates["minio_secure"] = parse_bool(args.minio_secure)
    return replace(config, **updates)


def _build_storage(config):
    if config.storage_backend == "local":
        return LocalFrameStorage(config.local_storage_dir)
    return MinioFrameStorage(
        endpoint=config.minio_endpoint,
        access_key=config.minio_access_key,
        secret_key=config.minio_secret_key,
        bucket=config.minio_bucket,
        secure=config.minio_secure,
    )


def _build_publisher(config):
    mode = PublishMode.parse(config.publish_mode)
    publishers = []
    if mode in {PublishMode.JSONL, PublishMode.BOTH}:
        publishers.append(OutboxFilePublisher(config.outbox_path, reset=config.outbox_reset))
    if mode in {PublishMode.RABBITMQ, PublishMode.BOTH}:
        publishers.append(
            RabbitMQFrameIngestedPublisher(
                host=config.rabbitmq_host,
                port=config.rabbitmq_port,
                username=config.rabbitmq_user,
                password=config.rabbitmq_password,
                virtual_host=config.rabbitmq_vhost,
                topology=FrameIngestedTopology(
                    exchange=config.rabbitmq_frame_exchange,
                    routing_key=config.rabbitmq_frame_routing_key,
                    recognition_queue=config.rabbitmq_recognition_queue,
                    dead_letter_exchange=config.rabbitmq_frame_dlx,
                    dead_letter_queue=config.rabbitmq_frame_dlq,
                    dead_letter_routing_key=config.rabbitmq_frame_dlq_routing_key,
                ),
            )
        )
    if len(publishers) == 1:
        return publishers[0]
    return CompositePublisher(publishers)


if __name__ == "__main__":
    raise SystemExit(main())
