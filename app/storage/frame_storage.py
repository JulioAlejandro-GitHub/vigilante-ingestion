from __future__ import annotations

from typing import Protocol

from app.models.frame import CapturedFrame, StoredFrame


class FrameStorage(Protocol):
    def save(self, frame: CapturedFrame) -> StoredFrame:
        """Persist frame bytes plus metadata and return a reusable reference."""

