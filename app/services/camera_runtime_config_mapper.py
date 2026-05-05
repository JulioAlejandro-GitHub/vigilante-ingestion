from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any


RUNTIME_CONFIG_METADATA_KEY = "camera_runtime_config"
RUNTIME_CONFIG_SCHEMA_VERSION = "camera_runtime_config_v1"
RUNTIME_CONFIG_SOURCE = "api.camera.metadata"

_FACE_TUNING_ALIASES: dict[str, tuple[str, ...]] = {
    "det_size": ("det_size", "insightface_det_size"),
    "detection_threshold": ("detection_threshold", "det_thresh", "insightface_detection_threshold"),
    "max_faces": ("max_faces", "insightface_max_faces"),
    "face_quality_threshold": ("face_quality_threshold", "quality_threshold", "min_quality_score"),
    "min_face_bbox_size": ("min_face_bbox_size", "minimum_face_bbox_size", "min_bbox_size"),
    "min_face_area_ratio": ("min_face_area_ratio", "minimum_face_area_ratio", "min_area_ratio"),
}

_VLM_POLICY_ALIASES: dict[str, tuple[str, ...]] = {
    "enabled": ("enabled", "vlm_enabled", "semantic_vlm_enabled"),
    "force_simple": ("force_simple", "vlm_force_simple"),
    "backend": ("backend", "vlm_backend", "semantic_backend", "semantic_descriptor_backend"),
    "preferred_backend": ("preferred_backend", "vlm_preferred_backend"),
    "secondary_backend": ("secondary_backend", "vlm_secondary_backend", "fallback_backend"),
    "enable_for_event_types": (
        "enable_for_event_types",
        "enabled_event_types",
        "eligible_events",
        "vlm_enable_for_event_types",
    ),
    "disable_for_event_types": ("disable_for_event_types", "disabled_event_types", "vlm_disable_for_event_types"),
    "max_latency_seconds": (
        "max_latency_seconds",
        "max_allowed_latency_seconds",
        "latency_budget_seconds",
        "vlm_max_allowed_latency_seconds",
    ),
    "max_rss_mb": (
        "max_rss_mb",
        "max_allowed_rss_mb",
        "memory_budget_mb",
        "max_memory_mb",
        "vlm_max_allowed_rss_mb",
    ),
    "max_concurrent_inferences": (
        "max_concurrent_inferences",
        "max_concurrency",
        "max_camera_concurrency",
        "vlm_max_concurrent_inferences",
    ),
    "degradation_policy": ("degradation_policy", "vlm_degradation_policy"),
}


def build_camera_runtime_config(
    *,
    camera_id: str | None,
    camera_metadata: Mapping[str, Any] | None,
    config_version_hint: str | None = None,
) -> dict[str, Any]:
    metadata = dict(camera_metadata or {})
    recognition = _mapping(metadata.get("recognition"))
    if not recognition:
        return {}

    face_tuning = _sanitize_face_tuning(_face_tuning_source(recognition))
    vlm_policy = _sanitize_vlm_policy(_first_mapping(
        recognition.get("vlm_policy"),
        recognition.get("semantic_vlm_policy"),
        recognition.get("semantic_descriptor_policy"),
    ))
    enabled = _optional_bool(
        _first_present(
            recognition,
            "enabled",
            "recognition_enabled",
            "vlm_enabled",
        )
    )

    sanitized_recognition: dict[str, Any] = {}
    if enabled is not None:
        sanitized_recognition["enabled"] = enabled
    if face_tuning:
        sanitized_recognition["face_tuning"] = face_tuning
    if vlm_policy:
        vlm_policy = dict(vlm_policy)
        if enabled is not None and "enabled" not in vlm_policy:
            vlm_policy["enabled"] = enabled
        sanitized_recognition["vlm_policy"] = vlm_policy

    if not sanitized_recognition:
        return {}

    snapshot = {
        "camera_id": str(camera_id) if camera_id else None,
        "recognition": sanitized_recognition,
    }
    config_hash = _stable_hash(snapshot)
    camera_config_version = (
        _optional_text(recognition.get("config_version"))
        or _optional_text(recognition.get("version"))
        or config_version_hint
        or config_hash
    )
    runtime_config = {
        "schema_version": RUNTIME_CONFIG_SCHEMA_VERSION,
        "config_source": RUNTIME_CONFIG_SOURCE,
        "camera_id": str(camera_id) if camera_id else None,
        "camera_config_version": camera_config_version,
        "config_hash": config_hash,
        "recognition": sanitized_recognition,
    }
    runtime_config["effective_config_hash"] = _stable_hash(runtime_config)
    return runtime_config


def camera_runtime_config_hash(runtime_config: Mapping[str, Any] | None) -> str | None:
    if not runtime_config:
        return None
    value = runtime_config.get("effective_config_hash") if isinstance(runtime_config, Mapping) else None
    return _optional_text(value) or _stable_hash(runtime_config)


def _sanitize_face_tuning(raw: Mapping[str, Any] | None) -> dict[str, Any]:
    return _sanitize_by_aliases(raw, _FACE_TUNING_ALIASES)


def _sanitize_vlm_policy(raw: Mapping[str, Any] | None) -> dict[str, Any]:
    return _sanitize_by_aliases(raw, _VLM_POLICY_ALIASES)


def _face_tuning_source(recognition: Mapping[str, Any]) -> dict[str, Any] | None:
    raw_face_tuning = recognition.get("face_tuning")
    if isinstance(raw_face_tuning, Mapping):
        nested = raw_face_tuning.get("insightface")
        if isinstance(nested, Mapping) and not any(key in raw_face_tuning for aliases in _FACE_TUNING_ALIASES.values() for key in aliases):
            return dict(nested)
        return dict(raw_face_tuning)
    raw_insightface = recognition.get("insightface")
    if isinstance(raw_insightface, Mapping):
        return dict(raw_insightface)
    return None


def _sanitize_by_aliases(raw: Mapping[str, Any] | None, aliases: Mapping[str, tuple[str, ...]]) -> dict[str, Any]:
    if not raw:
        return {}
    sanitized: dict[str, Any] = {}
    for canonical, candidate_keys in aliases.items():
        for key in candidate_keys:
            if key in raw:
                value = _sanitize_value(raw[key])
                if value is not None:
                    sanitized[canonical] = value
                break
    return sanitized


def _sanitize_value(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, (list, tuple, set)):
        sanitized_items = [_sanitize_value(item) for item in value]
        return [item for item in sanitized_items if item is not None]
    if isinstance(value, Mapping):
        return {
            str(key): sanitized_value
            for key, raw_value in value.items()
            if (sanitized_value := _sanitize_value(raw_value)) is not None
        }
    return str(value)


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _first_mapping(*values: Any) -> dict[str, Any] | None:
    for value in values:
        if isinstance(value, Mapping):
            return dict(value)
    return None


def _first_present(source: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in source:
            return source[key]
    return None


def _optional_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "y", "on"}:
            return True
        if normalized in {"0", "false", "no", "n", "off"}:
            return False
    return None


def _optional_text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _stable_hash(value: Mapping[str, Any]) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()
