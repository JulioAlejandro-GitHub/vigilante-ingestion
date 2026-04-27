from __future__ import annotations

import json
import logging
from typing import Any, Callable

from app.messaging.topology import FrameIngestedTopology, declare_frame_ingested_topology

logger = logging.getLogger(__name__)


class RabbitMQFrameIngestedPublisher:
    def __init__(
        self,
        *,
        host: str,
        port: int,
        username: str,
        password: str,
        virtual_host: str,
        topology: FrameIngestedTopology,
        connection_factory: Callable[[], Any] | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.virtual_host = virtual_host
        self.topology = topology
        self._connection_factory = connection_factory
        self._connection = None
        self._channel = None
        self.published_count = 0

    def publish(self, event: dict) -> None:
        channel = self._ensure_channel()
        body = json.dumps(event, sort_keys=True, separators=(",", ":")).encode("utf-8")
        channel.basic_publish(
            exchange=self.topology.exchange,
            routing_key=self.topology.routing_key,
            body=body,
            properties=self._properties(event),
            mandatory=True,
        )
        self.published_count += 1
        logger.info(
            "frame_ingested_published_rabbitmq event_id=%s exchange=%s routing_key=%s count=%s",
            event.get("event_id"),
            self.topology.exchange,
            self.topology.routing_key,
            self.published_count,
        )

    def close(self) -> None:
        if self._connection is not None and getattr(self._connection, "is_closed", False) is False:
            self._connection.close()
        self._connection = None
        self._channel = None

    def _ensure_channel(self):
        if self._channel is not None and getattr(self._channel, "is_open", True):
            return self._channel

        self._connection = self._build_connection()
        self._channel = self._connection.channel()
        declare_frame_ingested_topology(self._channel, self.topology)
        confirm_delivery = getattr(self._channel, "confirm_delivery", None)
        if callable(confirm_delivery):
            confirm_delivery()
        return self._channel

    def _build_connection(self):
        if self._connection_factory is not None:
            return self._connection_factory()

        try:
            import pika
        except ImportError as exc:  # pragma: no cover - exercised only without optional dependency
            raise RuntimeError("RabbitMQ publish mode requires the 'pika' package. Install requirements.txt.") from exc

        credentials = pika.PlainCredentials(self.username, self.password)
        parameters = pika.ConnectionParameters(
            host=self.host,
            port=self.port,
            virtual_host=self.virtual_host,
            credentials=credentials,
            heartbeat=30,
            blocked_connection_timeout=30,
        )
        return pika.BlockingConnection(parameters)

    def _properties(self, event: dict):
        try:
            import pika
        except ImportError:
            return None

        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        return pika.BasicProperties(
            app_id="vigilante-ingestion",
            content_type="application/json",
            delivery_mode=2,
            message_id=str(event.get("event_id", "")),
            timestamp=None,
            type=str(event.get("event_type", "frame.ingested")),
            headers={
                "event_type": event.get("event_type"),
                "event_version": event.get("event_version"),
                "camera_id": payload.get("camera_id"),
            },
        )
