from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ReconnectPolicy:
    initial_delay_seconds: float = 1.0
    max_delay_seconds: float = 30.0
    backoff_multiplier: float = 2.0
    max_reconnect_attempts: int | None = None

    def __post_init__(self) -> None:
        if self.initial_delay_seconds < 0:
            raise ValueError("initial_delay_seconds must not be negative")
        if self.max_delay_seconds < self.initial_delay_seconds:
            raise ValueError("max_delay_seconds must be greater than or equal to initial_delay_seconds")
        if self.backoff_multiplier < 1:
            raise ValueError("backoff_multiplier must be greater than or equal to one")
        if self.max_reconnect_attempts is not None and self.max_reconnect_attempts < 0:
            raise ValueError("max_reconnect_attempts must not be negative")
        self._next_delay = self.initial_delay_seconds
        self._attempts = 0

    @property
    def attempts(self) -> int:
        return self._attempts

    def reset(self) -> None:
        self._next_delay = self.initial_delay_seconds
        self._attempts = 0

    def can_retry(self) -> bool:
        return self.max_reconnect_attempts is None or self._attempts < self.max_reconnect_attempts

    def next_delay(self) -> float:
        if not self.can_retry():
            raise RuntimeError("Reconnect attempts exhausted")

        delay = self._next_delay
        self._attempts += 1
        self._next_delay = min(
            self.max_delay_seconds,
            self._next_delay * self.backoff_multiplier,
        )
        return delay
