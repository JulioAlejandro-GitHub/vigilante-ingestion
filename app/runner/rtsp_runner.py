from __future__ import annotations

import logging
import time
from dataclasses import asdict, dataclass
from typing import Any, Callable

from app.capture.reconnect_policy import ReconnectPolicy
from app.capture.rtsp_source import JpegMetadataError, RtspSource, mask_rtsp_credentials
from app.capture.stream_reader import StreamOpenError, StreamReadError
from app.config import IngestionConfig
from app.publisher.frame_ingested_publisher import build_frame_ingested_event
from app.publisher.publish_mode import FrameIngestedPublisher, PublishMode
from app.storage.frame_storage import FrameStorage

logger = logging.getLogger(__name__)

RtspSourceFactory = Callable[[], object]
Sleeper = Callable[[float], None]
RtspLifecycleCallback = Callable[[str, dict[str, Any]], None]
RTSP_RECOVERABLE_ERRORS = (StreamOpenError, StreamReadError, JpegMetadataError)


@dataclass(frozen=True)
class RtspResult:
    frames_captured: int
    events_published: int
    outbox_path: str
    publish_mode: str
    destinations: tuple[str, ...]
    stream_opens: int
    reconnect_attempts: int
    frames_stored: int
    read_failures: int
    store_failures: int
    publish_failures: int


@dataclass
class RtspCounters:
    frames_captured: int = 0
    events_published: int = 0
    stream_opens: int = 0
    reconnect_attempts: int = 0
    frames_stored: int = 0
    read_failures: int = 0
    store_failures: int = 0
    publish_failures: int = 0


class RtspRunner:
    def __init__(
        self,
        *,
        config: IngestionConfig,
        storage: FrameStorage,
        publisher: FrameIngestedPublisher,
        source_factory: RtspSourceFactory | None = None,
        sleep: Sleeper = time.sleep,
        lifecycle_callback: RtspLifecycleCallback | None = None,
    ) -> None:
        self.config = config
        self.storage = storage
        self.publisher = publisher
        self.source_factory = source_factory
        self.sleep = sleep
        self.lifecycle_callback = lifecycle_callback
        self.safe_rtsp_url = mask_rtsp_credentials(config.rtsp_url or "")

    def run(self) -> RtspResult:
        counters = RtspCounters()
        reconnect_policy = ReconnectPolicy(
            initial_delay_seconds=self.config.rtsp_reconnect_initial_delay_seconds,
            max_delay_seconds=self.config.rtsp_reconnect_max_delay_seconds,
            backoff_multiplier=self.config.rtsp_reconnect_backoff_multiplier,
            max_reconnect_attempts=self.config.rtsp_max_reconnect_attempts,
        )

        try:
            while self._should_continue(counters):
                remaining = self._remaining_frames(counters)
                source = self._build_source()
                counters.stream_opens += 1
                stream_frames = 0
                logger.info(
                    "rtsp_stream_connecting url=%s attempt=%s max_frames_remaining=%s",
                    self.safe_rtsp_url,
                    counters.stream_opens,
                    remaining if remaining is not None else "unbounded",
                )
                self._emit_lifecycle(
                    "camera_stream_starting",
                    counters,
                    attempt=counters.stream_opens,
                    max_frames_remaining=remaining,
                )
                try:
                    for frame in source.iter_frames(
                        max_frames=remaining,
                        start_sample_index=counters.frames_captured,
                    ):
                        if stream_frames == 0:
                            reconnect_policy.reset()
                            logger.info(
                                "rtsp_stream_connected url=%s stream_open_count=%s",
                                self.safe_rtsp_url,
                                counters.stream_opens,
                            )
                            self._emit_lifecycle(
                                "camera_stream_connected",
                                counters,
                                stream_open_count=counters.stream_opens,
                            )

                        stored_frame = self._save_frame(frame, counters)
                        event = build_frame_ingested_event(
                            frame=frame,
                            stored_frame=stored_frame,
                            config=self.config,
                        )
                        self._publish_event(event, counters)
                        counters.frames_captured += 1
                        stream_frames += 1
                        logger.info(
                            "rtsp_frame_ingested camera_id=%s sample_index=%s frame_ref=%s",
                            frame.camera_id,
                            frame.sample_index,
                            stored_frame.frame_ref,
                        )
                        self._emit_lifecycle(
                            "camera_frame_ingested",
                            counters,
                            sample_index=frame.sample_index,
                            frame_ref=stored_frame.frame_ref,
                        )

                    if self._should_continue(counters):
                        raise StreamReadError("RTSP stream ended before max_frames was reached")
                except RTSP_RECOVERABLE_ERRORS as exc:
                    counters.read_failures += 1
                    logger.warning(
                        "rtsp_stream_disconnected url=%s error_type=%s error=%s frames_in_stream=%s",
                        self.safe_rtsp_url,
                        type(exc).__name__,
                        exc,
                        stream_frames,
                    )
                    self._emit_lifecycle(
                        "camera_stream_disconnected",
                        counters,
                        error_type=type(exc).__name__,
                        error=str(exc),
                        frames_in_stream=stream_frames,
                    )
                    if not reconnect_policy.can_retry():
                        logger.error(
                            "rtsp_reconnect_exhausted url=%s attempts=%s",
                            self.safe_rtsp_url,
                            reconnect_policy.attempts,
                        )
                        raise
                    delay = reconnect_policy.next_delay()
                    counters.reconnect_attempts += 1
                    logger.info(
                        "rtsp_reconnect_scheduled url=%s attempt=%s delay_seconds=%s",
                        self.safe_rtsp_url,
                        counters.reconnect_attempts,
                        delay,
                    )
                    self._emit_lifecycle(
                        "camera_stream_reconnect_scheduled",
                        counters,
                        attempt=counters.reconnect_attempts,
                        delay_seconds=delay,
                    )
                    self.sleep(delay)
        finally:
            close = getattr(self.publisher, "close", None)
            if callable(close):
                close()
            self._emit_lifecycle("camera_worker_stopped", counters)

        logger.info(
            "rtsp_ingestion_summary url=%s frames_captured=%s events_published=%s stream_opens=%s "
            "reconnect_attempts=%s read_failures=%s store_failures=%s publish_failures=%s",
            self.safe_rtsp_url,
            counters.frames_captured,
            counters.events_published,
            counters.stream_opens,
            counters.reconnect_attempts,
            counters.read_failures,
            counters.store_failures,
            counters.publish_failures,
        )
        return RtspResult(
            frames_captured=counters.frames_captured,
            events_published=counters.events_published,
            outbox_path=str(self.config.outbox_path),
            publish_mode=self.config.publish_mode,
            destinations=_destinations(self.config.publish_mode),
            stream_opens=counters.stream_opens,
            reconnect_attempts=counters.reconnect_attempts,
            frames_stored=counters.frames_stored,
            read_failures=counters.read_failures,
            store_failures=counters.store_failures,
            publish_failures=counters.publish_failures,
        )

    def _should_continue(self, counters: RtspCounters) -> bool:
        return self.config.max_frames is None or counters.frames_captured < self.config.max_frames

    def _remaining_frames(self, counters: RtspCounters) -> int | None:
        if self.config.max_frames is None:
            return None
        return self.config.max_frames - counters.frames_captured

    def _build_source(self):
        if self.source_factory is not None:
            return self.source_factory()
        return RtspSource(
            rtsp_url=self.config.rtsp_url or "",
            camera_id=self.config.camera_id,
            capture_fps=self.config.capture_fps,
            ffmpeg_path=self.config.ffmpeg_path,
            rtsp_transport=self.config.rtsp_transport,
            read_timeout_seconds=self.config.rtsp_read_timeout_seconds,
        )

    def _save_frame(self, frame, counters: RtspCounters):
        try:
            stored_frame = self.storage.save(frame)
        except Exception as exc:
            counters.store_failures += 1
            logger.exception(
                "rtsp_frame_store_failed camera_id=%s sample_index=%s error_type=%s",
                frame.camera_id,
                frame.sample_index,
                type(exc).__name__,
            )
            self._emit_lifecycle(
                "camera_frame_store_failed",
                counters,
                sample_index=frame.sample_index,
                error_type=type(exc).__name__,
                error=str(exc),
            )
            raise
        counters.frames_stored += 1
        return stored_frame

    def _publish_event(self, event: dict, counters: RtspCounters) -> None:
        try:
            self.publisher.publish(event)
        except Exception as exc:
            counters.publish_failures += 1
            logger.exception(
                "rtsp_frame_publish_failed event_id=%s error_type=%s",
                event.get("event_id"),
                type(exc).__name__,
            )
            self._emit_lifecycle(
                "camera_frame_publish_failed",
                counters,
                event_id=event.get("event_id"),
                error_type=type(exc).__name__,
                error=str(exc),
            )
            raise
        counters.events_published += 1
        logger.info("rtsp_frame_published event_id=%s", event.get("event_id"))

    def _emit_lifecycle(self, event_name: str, counters: RtspCounters, **payload: Any) -> None:
        event_payload: dict[str, Any] = {
            "camera_id": self.config.camera_id,
            "external_camera_key": self.config.external_camera_key,
            "site_id": self.config.site_id,
            "zone_id": self.config.zone_id,
            "safe_rtsp_url": self.safe_rtsp_url,
            "counters": asdict(counters),
            **payload,
        }
        if event_name in {
            "camera_stream_starting",
            "camera_stream_connected",
            "camera_stream_disconnected",
            "camera_stream_reconnect_scheduled",
            "camera_frame_ingested",
            "camera_worker_stopped",
        }:
            logger.info(
                "%s camera_id=%s external_camera_key=%s site_id=%s zone_id=%s stream_opens=%s "
                "reconnect_attempts=%s frames_captured=%s events_published=%s",
                event_name,
                self.config.camera_id,
                self.config.external_camera_key or "",
                self.config.site_id or "",
                self.config.zone_id or "",
                counters.stream_opens,
                counters.reconnect_attempts,
                counters.frames_captured,
                counters.events_published,
            )
        if self.lifecycle_callback is not None:
            try:
                self.lifecycle_callback(event_name, event_payload)
            except Exception:
                logger.exception(
                    "rtsp_lifecycle_callback_failed event_name=%s camera_id=%s",
                    event_name,
                    self.config.camera_id,
                )


def _destinations(publish_mode: str) -> tuple[str, ...]:
    mode = PublishMode.parse(publish_mode)
    if mode == PublishMode.JSONL:
        return ("jsonl",)
    if mode == PublishMode.RABBITMQ:
        return ("rabbitmq",)
    return ("jsonl", "rabbitmq")
