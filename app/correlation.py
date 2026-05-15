from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class IngestionCorrelation:
    run_id: str | None = None
    source: str | None = None

    @property
    def is_active(self) -> bool:
        return bool(self.run_id)


def load_ingestion_correlation(
    *,
    run_id: str | None = None,
    source: str | None = None,
) -> IngestionCorrelation:
    if _clean(run_id):
        return IngestionCorrelation(run_id=_clean(run_id), source=_clean(source) or "ingestion_config")
    return IngestionCorrelation()


def apply_ingestion_correlation(event: dict[str, Any], correlation: IngestionCorrelation) -> dict[str, Any]:
    if not correlation.is_active:
        return event

    run_id = str(correlation.run_id)
    context = _dict(event.setdefault("context", {}))
    context["run_id"] = run_id
    event["context"] = context

    payload = _dict(event.setdefault("payload", {}))
    metadata = _dict(payload.setdefault("metadata", {}))

    pipeline = _dict(metadata.get("pipeline"))
    pipeline["run_id"] = run_id
    metadata["pipeline"] = pipeline

    correlation_payload = _dict(metadata.get("correlation"))
    correlation_payload["run_id"] = run_id
    if correlation.source:
        correlation_payload["source"] = correlation.source
    metadata["correlation"] = correlation_payload

    payload["metadata"] = metadata
    event["payload"] = payload
    return event


def extract_run_id(event: dict[str, Any]) -> str | None:
    context = _dict(event.get("context"))
    payload = _dict(event.get("payload"))
    metadata = _dict(payload.get("metadata"))

    candidates = [
        context.get("run_id"),
        metadata.get("run_id"),
        _dict(metadata.get("pipeline")).get("run_id"),
        _dict(metadata.get("correlation")).get("run_id"),
    ]
    for candidate in candidates:
        value = _clean(candidate)
        if value:
            return value
    return None

def _clean(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def _dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}
