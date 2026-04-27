from __future__ import annotations

from enum import StrEnum
from typing import Protocol


class FrameIngestedPublisher(Protocol):
    def publish(self, event: dict) -> None:
        """Publish one frame.ingested event."""


class PublishMode(StrEnum):
    JSONL = "jsonl"
    RABBITMQ = "rabbitmq"
    BOTH = "both"

    @classmethod
    def parse(cls, value: str | "PublishMode") -> "PublishMode":
        if isinstance(value, cls):
            return value
        try:
            return cls(value.strip().lower())
        except ValueError as exc:
            allowed = ", ".join(mode.value for mode in cls)
            raise ValueError(f"Invalid publish mode {value!r}. Expected one of: {allowed}") from exc


class CompositePublisher:
    def __init__(self, publishers: list[FrameIngestedPublisher]) -> None:
        if not publishers:
            raise ValueError("At least one publisher is required")
        self.publishers = publishers

    def publish(self, event: dict) -> None:
        for publisher in self.publishers:
            publisher.publish(event)

    def close(self) -> None:
        for publisher in self.publishers:
            close = getattr(publisher, "close", None)
            if callable(close):
                close()
