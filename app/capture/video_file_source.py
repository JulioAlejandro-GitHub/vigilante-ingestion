from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Iterator

from app.capture.frame_sampler import FrameSampler
from app.models.frame import CapturedFrame, VideoMetadata


class SourceNotFoundError(FileNotFoundError):
    pass


class VideoProbeError(RuntimeError):
    pass


class FrameExtractionError(RuntimeError):
    pass


class VideoFileSource:
    def __init__(
        self,
        source_file: Path | str,
        *,
        camera_id: str,
        ffmpeg_path: str = "ffmpeg",
        ffprobe_path: str = "ffprobe",
    ) -> None:
        self.source_file = Path(source_file)
        self.camera_id = camera_id
        self.ffmpeg_path = ffmpeg_path
        self.ffprobe_path = ffprobe_path
        self._metadata: VideoMetadata | None = None

    def inspect(self) -> VideoMetadata:
        if self._metadata is not None:
            return self._metadata
        if not self.source_file.exists():
            raise SourceNotFoundError(f"Video source does not exist: {self.source_file}")

        command = [
            self.ffprobe_path,
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_entries",
            "stream=width,height,avg_frame_rate,r_frame_rate,duration,nb_frames:format=duration",
            "-of",
            "json",
            str(self.source_file),
        ]
        result = subprocess.run(command, capture_output=True, text=True, check=False)
        if result.returncode != 0:
            raise VideoProbeError(result.stderr.strip() or f"ffprobe failed for {self.source_file}")

        try:
            payload = json.loads(result.stdout)
            stream = payload["streams"][0]
            width = int(stream["width"])
            height = int(stream["height"])
            fps = _parse_rate(stream.get("avg_frame_rate")) or _parse_rate(stream.get("r_frame_rate"))
            duration_seconds = _parse_optional_float(stream.get("duration"))
            if duration_seconds is None:
                duration_seconds = _parse_optional_float(payload.get("format", {}).get("duration"))
            frame_count = _parse_optional_int(stream.get("nb_frames"))
        except (KeyError, IndexError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise VideoProbeError(f"Could not parse ffprobe metadata for {self.source_file}") from exc

        if not fps or fps <= 0:
            raise VideoProbeError(f"Video source has invalid FPS metadata: {self.source_file}")

        self._metadata = VideoMetadata(
            source_file=self.source_file,
            width=width,
            height=height,
            fps=fps,
            duration_seconds=duration_seconds,
            frame_count=frame_count,
        )
        return self._metadata

    def iter_frames(
        self,
        *,
        capture_fps: float,
        max_frames: int | None,
        replay_start_at: datetime,
        replay: bool = True,
    ) -> Iterator[CapturedFrame]:
        metadata = self.inspect()
        sampler = FrameSampler(capture_fps=capture_fps, source_fps=metadata.fps)
        now = datetime.now(timezone.utc)
        for sample in sampler.iter_samples(
            duration_seconds=metadata.duration_seconds,
            max_frames=max_frames,
        ):
            image_bytes = self.extract_frame_jpeg(sample.timestamp_seconds)
            captured_at = (
                replay_start_at + timedelta(seconds=sample.timestamp_seconds)
                if replay
                else now + timedelta(seconds=sample.timestamp_seconds)
            )
            yield CapturedFrame(
                image_bytes=image_bytes,
                camera_id=self.camera_id,
                captured_at=captured_at,
                width=metadata.width,
                height=metadata.height,
                content_type="image/jpeg",
                source_type="video_file",
                source_uri=str(self.source_file),
                source_timestamp_seconds=sample.timestamp_seconds,
                source_frame_index=sample.source_frame_index,
                sample_index=sample.sample_index,
                capture_fps=capture_fps,
                source_fps=metadata.fps,
            )

    def extract_frame_jpeg(self, timestamp_seconds: float) -> bytes:
        if timestamp_seconds < 0:
            raise FrameExtractionError("timestamp_seconds must not be negative")
        command = [
            self.ffmpeg_path,
            "-hide_banner",
            "-loglevel",
            "error",
            "-nostdin",
            "-ss",
            f"{timestamp_seconds:.6f}",
            "-i",
            str(self.source_file),
            "-frames:v",
            "1",
            "-f",
            "image2pipe",
            "-vcodec",
            "mjpeg",
            "-q:v",
            "2",
            "pipe:1",
        ]
        result = subprocess.run(command, capture_output=True, check=False)
        if result.returncode != 0:
            stderr = result.stderr.decode("utf-8", errors="replace").strip()
            raise FrameExtractionError(stderr or f"ffmpeg failed extracting frame at {timestamp_seconds}")
        if not result.stdout:
            raise FrameExtractionError(f"ffmpeg returned an empty frame at {timestamp_seconds}")
        return result.stdout


def _parse_rate(value: str | None) -> float | None:
    if not value or value == "0/0":
        return None
    if "/" not in value:
        return float(value)
    numerator, denominator = value.split("/", 1)
    denominator_value = float(denominator)
    if denominator_value == 0:
        return None
    return float(numerator) / denominator_value


def _parse_optional_float(value: str | int | float | None) -> float | None:
    if value in {None, "N/A"}:
        return None
    return float(value)


def _parse_optional_int(value: str | int | None) -> int | None:
    if value in {None, "N/A"}:
        return None
    return int(value)

