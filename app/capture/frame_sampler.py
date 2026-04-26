from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SamplePlan:
    sample_index: int
    timestamp_seconds: float
    source_frame_index: int


class FrameSampler:
    def __init__(self, *, capture_fps: float, source_fps: float) -> None:
        if capture_fps <= 0:
            raise ValueError("capture_fps must be greater than zero")
        if source_fps <= 0:
            raise ValueError("source_fps must be greater than zero")
        self.capture_fps = capture_fps
        self.source_fps = source_fps

    def iter_samples(
        self,
        *,
        duration_seconds: float | None,
        max_frames: int | None = None,
    ):
        if duration_seconds is None and max_frames is None:
            raise ValueError("duration_seconds or max_frames is required")
        if max_frames is not None and max_frames <= 0:
            raise ValueError("max_frames must be greater than zero")

        sample_index = 0
        while True:
            if max_frames is not None and sample_index >= max_frames:
                break

            timestamp_seconds = sample_index / self.capture_fps
            if duration_seconds is not None and timestamp_seconds >= duration_seconds:
                break

            yield SamplePlan(
                sample_index=sample_index,
                timestamp_seconds=round(timestamp_seconds, 6),
                source_frame_index=max(0, round(timestamp_seconds * self.source_fps)),
            )
            sample_index += 1

