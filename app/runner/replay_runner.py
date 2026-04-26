from __future__ import annotations

from dataclasses import dataclass

from app.capture.video_file_source import VideoFileSource
from app.config import IngestionConfig
from app.publisher.frame_ingested_publisher import OutboxFilePublisher, build_frame_ingested_event
from app.storage.frame_storage import FrameStorage


@dataclass(frozen=True)
class ReplayResult:
    frames_captured: int
    events_published: int
    outbox_path: str


class ReplayRunner:
    def __init__(
        self,
        *,
        config: IngestionConfig,
        storage: FrameStorage,
        publisher: OutboxFilePublisher,
    ) -> None:
        self.config = config
        self.storage = storage
        self.publisher = publisher

    def run(self) -> ReplayResult:
        source = VideoFileSource(
            self.config.source_file,
            camera_id=self.config.camera_id,
            ffmpeg_path=self.config.ffmpeg_path,
            ffprobe_path=self.config.ffprobe_path,
        )
        frames_captured = 0
        for frame in source.iter_frames(
            capture_fps=self.config.capture_fps,
            max_frames=self.config.max_frames,
            replay_start_at=self.config.replay_start_at,
            replay=self.config.replay,
        ):
            stored_frame = self.storage.save(frame)
            event = build_frame_ingested_event(
                frame=frame,
                stored_frame=stored_frame,
                config=self.config,
            )
            self.publisher.publish(event)
            frames_captured += 1

        return ReplayResult(
            frames_captured=frames_captured,
            events_published=frames_captured,
            outbox_path=str(self.config.outbox_path),
        )

