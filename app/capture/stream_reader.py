from __future__ import annotations

import logging
import os
import select
import subprocess
import time
from typing import Protocol
from urllib.parse import urlparse, urlunparse

logger = logging.getLogger(__name__)

JPEG_SOI = b"\xff\xd8"
JPEG_EOI = b"\xff\xd9"


class StreamOpenError(RuntimeError):
    pass


class StreamReadError(RuntimeError):
    pass


class StreamReadTimeout(StreamReadError):
    pass


class FrameStreamReader(Protocol):
    def open(self) -> None:
        """Open the underlying stream."""

    def read_frame(self) -> bytes:
        """Read one encoded JPEG frame."""

    def close(self) -> None:
        """Release stream resources."""


class FfmpegMjpegStreamReader:
    """Read sampled RTSP frames from a persistent FFmpeg image2pipe process."""

    def __init__(
        self,
        *,
        rtsp_url: str,
        capture_fps: float,
        ffmpeg_path: str = "ffmpeg",
        rtsp_transport: str = "tcp",
        read_timeout_seconds: float = 10.0,
        max_buffer_bytes: int = 20 * 1024 * 1024,
    ) -> None:
        if capture_fps <= 0:
            raise ValueError("capture_fps must be greater than zero")
        if read_timeout_seconds <= 0:
            raise ValueError("read_timeout_seconds must be greater than zero")
        self.rtsp_url = rtsp_url
        self.capture_fps = capture_fps
        self.ffmpeg_path = ffmpeg_path
        self.rtsp_transport = rtsp_transport
        self.read_timeout_seconds = read_timeout_seconds
        self.max_buffer_bytes = max_buffer_bytes
        self._process: subprocess.Popen[bytes] | None = None
        self._buffer = bytearray()

    def open(self) -> None:
        if self._process is not None and self._process.poll() is None:
            return

        command = [
            self.ffmpeg_path,
            "-hide_banner",
            "-loglevel",
            "error",
            "-nostdin",
            "-rtsp_transport",
            self.rtsp_transport,
            "-i",
            self.rtsp_url,
            "-an",
            "-vf",
            f"fps={self.capture_fps:g}",
            "-f",
            "image2pipe",
            "-vcodec",
            "mjpeg",
            "-q:v",
            "2",
            "pipe:1",
        ]
        try:
            self._process = subprocess.Popen(
                command,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        except OSError as exc:
            raise StreamOpenError(f"Could not start FFmpeg: {exc}") from exc
        self._buffer.clear()

    def read_frame(self) -> bytes:
        if self._process is None or self._process.stdout is None:
            raise StreamReadError("Stream reader is not open")

        deadline = time.monotonic() + self.read_timeout_seconds
        while True:
            frame = self._pop_jpeg_frame()
            if frame is not None:
                return frame

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise StreamReadTimeout(f"No frame received within {self.read_timeout_seconds:g}s")

            process_return_code = self._process.poll()
            if process_return_code is not None:
                stderr = self._read_stderr()
                detail = f"FFmpeg exited with code {process_return_code}"
                if stderr:
                    detail = f"{detail}: {stderr}"
                raise StreamReadError(detail)

            ready, _, _ = select.select([self._process.stdout], [], [], remaining)
            if not ready:
                raise StreamReadTimeout(f"No frame received within {self.read_timeout_seconds:g}s")

            chunk = os.read(self._process.stdout.fileno(), 64 * 1024)
            if not chunk:
                process_return_code = self._process.poll()
                if process_return_code is not None:
                    stderr = self._read_stderr()
                    detail = f"FFmpeg exited with code {process_return_code}"
                    if stderr:
                        detail = f"{detail}: {stderr}"
                    raise StreamReadError(detail)
                continue

            self._buffer.extend(chunk)
            if len(self._buffer) > self.max_buffer_bytes:
                self._buffer.clear()
                raise StreamReadError("MJPEG stream buffer exceeded max_buffer_bytes")

    def close(self) -> None:
        process = self._process
        self._process = None
        self._buffer.clear()
        if process is None:
            return
        if process.poll() is not None:
            return
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            logger.warning("ffmpeg_rtsp_process_kill pid=%s", process.pid)
            process.kill()
            process.wait(timeout=5)

    def _pop_jpeg_frame(self) -> bytes | None:
        start = self._buffer.find(JPEG_SOI)
        if start < 0:
            if len(self._buffer) > 2:
                del self._buffer[:-2]
            return None
        if start > 0:
            del self._buffer[:start]

        end = self._buffer.find(JPEG_EOI, len(JPEG_SOI))
        if end < 0:
            return None

        frame_end = end + len(JPEG_EOI)
        frame = bytes(self._buffer[:frame_end])
        del self._buffer[:frame_end]
        return frame

    def _read_stderr(self) -> str:
        process = self._process
        if process is None or process.stderr is None:
            return ""
        try:
            return mask_rtsp_credentials_in_text(process.stderr.read().decode("utf-8", errors="replace").strip())
        except Exception:
            return ""


def mask_rtsp_credentials_in_text(text: str) -> str:
    words = text.split()
    if not words:
        return text
    return " ".join(_mask_rtsp_credentials(word) for word in words)


def _mask_rtsp_credentials(value: str) -> str:
    parsed = urlparse(value)
    if parsed.scheme not in {"rtsp", "rtsps"} or parsed.password is None:
        return value

    host = parsed.hostname or ""
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    if parsed.port is not None:
        host = f"{host}:{parsed.port}"
    username = parsed.username or ""
    netloc = f"{username}:***@{host}" if username else host
    return urlunparse(parsed._replace(netloc=netloc))
