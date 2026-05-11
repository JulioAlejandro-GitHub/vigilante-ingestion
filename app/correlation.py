from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class IngestionCorrelation:
    run_id: str | None = None
    source: str | None = None
    created_at: str | None = None

    @property
    def is_active(self) -> bool:
        return bool(self.run_id)


def load_ingestion_correlation(
    *,
    run_id: str | None = None,
    source: str | None = None,
    correlation_path: Path | str | None = None,
) -> IngestionCorrelation:
    if _clean(run_id):
        return IngestionCorrelation(run_id=_clean(run_id), source=_clean(source) or "ingestion_config")
    if correlation_path is None:
        return IngestionCorrelation()
    return _load_correlation_file(Path(correlation_path))


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

    if correlation.source == "vigilante_stack_smoke":
        smoke = _dict(metadata.get("smoke"))
        smoke["run_id"] = run_id
        if correlation.created_at:
            smoke["created_at"] = correlation.created_at
        metadata["smoke"] = smoke

    payload["metadata"] = metadata
    event["payload"] = payload
    return event


def extract_run_id(event: dict[str, Any]) -> str | None:
    context = _dict(event.get("context"))
    payload = _dict(event.get("payload"))
    metadata = _dict(payload.get("metadata"))

    candidates = [
        context.get("run_id"),
        context.get("smoke_run_id"),
        metadata.get("run_id"),
        _dict(metadata.get("pipeline")).get("run_id"),
        _dict(metadata.get("correlation")).get("run_id"),
        _dict(metadata.get("smoke")).get("run_id"),
    ]
    for candidate in candidates:
        value = _clean(candidate)
        if value:
            return value
    return None


def _load_correlation_file(path: Path) -> IngestionCorrelation:
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return IngestionCorrelation()
    except OSError as exc:
        logger.warning("ingestion_correlation_file_unreadable path=%s error=%s", path, type(exc).__name__)
        return IngestionCorrelation()

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        logger.warning("ingestion_correlation_file_invalid_json path=%s", path)
        return IngestionCorrelation()
    if not isinstance(payload, dict):
        return IngestionCorrelation()

    expires_at = payload.get("expires_at_epoch")
    if expires_at is not None:
        try:
            if float(expires_at) < time.time():
                return IngestionCorrelation()
        except (TypeError, ValueError):
            return IngestionCorrelation()

    run_id = _clean(payload.get("run_id"))
    if not run_id:
        return IngestionCorrelation()
    return IngestionCorrelation(
        run_id=run_id,
        source=_clean(payload.get("source")),
        created_at=_clean(payload.get("created_at")),
    )


def _clean(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def _dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}
