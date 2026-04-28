from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Callable, Iterator
from urllib.parse import urlparse, urlunparse

from app.capture.stream_reader import FfmpegMjpegStreamReader, FrameStreamReader
from app.models.frame import CapturedFrame

logger = logging.getLogger(__name__)

SOF_MARKERS = {
    0xC0,
    0xC1,
    0xC2,
    0xC3,
    0xC5,
    0xC6,
    0xC7,
    0xC9,
    0xCA,
    0xCB,
    0xCD,
    0xCE,
    0xCF,
}
NO_LENGTH_MARKERS = {0x01, 0xD8, 0xD9, *range(0xD0, 0xD8)}


class InvalidRtspUrl(ValueError):
    pass


class JpegMetadataError(ValueError):
    pass


ReaderFactory = Callable[[], FrameStreamReader]
Clock = Callable[[], datetime]


class RtspSource:
    def __init__(
        self,
        *,
        rtsp_url: str,
        camera_id: str,
        capture_fps: float,
        ffmpeg_path: str = "ffmpeg",
        rtsp_transport: str = "tcp",
        read_timeout_seconds: float = 10.0,
        reader_factory: ReaderFactory | None = None,
        clock: Clock | None = None,
    ) -> None:
        validate_rtsp_url(rtsp_url)
        if capture_fps <= 0:
            raise ValueError("capture_fps must be greater than zero")
        self.rtsp_url = rtsp_url
        self.safe_rtsp_url = mask_rtsp_credentials(rtsp_url)
        self.camera_id = camera_id
        self.capture_fps = capture_fps
        self.ffmpeg_path = ffmpeg_path
        self.rtsp_transport = rtsp_transport
        self.read_timeout_seconds = read_timeout_seconds
        self._reader_factory = reader_factory
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def iter_frames(
        self,
        *,
        max_frames: int | None,
        start_sample_index: int = 0,
    ) -> Iterator[CapturedFrame]:
        if max_frames is not None and max_frames <= 0:
            return

        reader = self._build_reader()
        session_started_at = self._clock()
        session_frame_index = 0
        reader.open()
        logger.info("rtsp_stream_opened url=%s capture_fps=%s", self.safe_rtsp_url, self.capture_fps)
        try:
            while max_frames is None or session_frame_index < max_frames:
                image_bytes = reader.read_frame()
                captured_at = self._clock()
                width, height = read_jpeg_dimensions(image_bytes)
                sample_index = start_sample_index + session_frame_index
                source_timestamp_seconds = round(
                    max(0.0, (captured_at - session_started_at).total_seconds()),
                    6,
                )
                yield CapturedFrame(
                    image_bytes=image_bytes,
                    camera_id=self.camera_id,
                    captured_at=captured_at,
                    width=width,
                    height=height,
                    content_type="image/jpeg",
                    source_type="rtsp",
                    source_uri=self.safe_rtsp_url,
                    source_timestamp_seconds=source_timestamp_seconds,
                    source_frame_index=sample_index,
                    sample_index=sample_index,
                    capture_fps=self.capture_fps,
                    source_fps=self.capture_fps,
                )
                session_frame_index += 1
        finally:
            reader.close()
            logger.info("rtsp_stream_closed url=%s frames_read=%s", self.safe_rtsp_url, session_frame_index)

    def _build_reader(self) -> FrameStreamReader:
        if self._reader_factory is not None:
            return self._reader_factory()
        return FfmpegMjpegStreamReader(
            rtsp_url=self.rtsp_url,
            capture_fps=self.capture_fps,
            ffmpeg_path=self.ffmpeg_path,
            rtsp_transport=self.rtsp_transport,
            read_timeout_seconds=self.read_timeout_seconds,
        )


def validate_rtsp_url(rtsp_url: str) -> None:
    parsed = urlparse(rtsp_url)
    if parsed.scheme not in {"rtsp", "rtsps"} or not parsed.netloc:
        raise InvalidRtspUrl("RTSP URL must use rtsp:// or rtsps:// and include a host")


def mask_rtsp_credentials(rtsp_url: str) -> str:
    parsed = urlparse(rtsp_url)
    if parsed.password is None:
        return rtsp_url

    host = parsed.hostname or ""
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    if parsed.port is not None:
        host = f"{host}:{parsed.port}"

    username = parsed.username or ""
    netloc = f"{username}:***@{host}" if username else host
    return urlunparse(parsed._replace(netloc=netloc))


def read_jpeg_dimensions(image_bytes: bytes) -> tuple[int, int]:
    if len(image_bytes) < 4 or not image_bytes.startswith(b"\xff\xd8"):
        raise JpegMetadataError("Frame is not a JPEG image")

    index = 2
    while index < len(image_bytes):
        if image_bytes[index] != 0xFF:
            index += 1
            continue

        while index < len(image_bytes) and image_bytes[index] == 0xFF:
            index += 1
        if index >= len(image_bytes):
            break

        marker = image_bytes[index]
        index += 1
        if marker in NO_LENGTH_MARKERS:
            continue
        if index + 2 > len(image_bytes):
            break

        segment_length = int.from_bytes(image_bytes[index : index + 2], "big")
        if segment_length < 2:
            raise JpegMetadataError("JPEG segment has invalid length")
        if index + segment_length > len(image_bytes):
            break

        if marker in SOF_MARKERS:
            if segment_length < 7:
                raise JpegMetadataError("JPEG SOF segment is too short")
            height = int.from_bytes(image_bytes[index + 3 : index + 5], "big")
            width = int.from_bytes(image_bytes[index + 5 : index + 7], "big")
            if width <= 0 or height <= 0:
                raise JpegMetadataError("JPEG dimensions must be greater than zero")
            return width, height

        index += segment_length

    raise JpegMetadataError("Could not find JPEG dimensions")
