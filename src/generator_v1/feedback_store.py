from __future__ import annotations

import json
import uuid
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_FEEDBACK_EVENTS_PATH = Path("data/runtime/generator_v1_feedback_events.jsonl")
FEEDBACK_TYPES = {"liked", "disliked", "too_long", "explicit_avoid"}
SOURCES = {"streamlit", "cli", "test"}
EVENT_FIELDS = [
    "event_id",
    "created_at",
    "household_id",
    "member_profile_id",
    "dataset_profile",
    "recipe_id",
    "recipe_family_name",
    "display_name",
    "slot",
    "feedback_type",
    "source",
    "run_id",
    "plan_id",
    "notes",
]


def append_feedback_event(
    event: dict[str, Any],
    path: str | Path | None = None,
) -> dict[str, Any]:
    output_path = _resolved_path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = feedback_event_to_dict(**dict(event))
    with output_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")
    return payload


def load_feedback_events(path: str | Path | None = None) -> list[dict[str, Any]]:
    input_path = _resolved_path(path)
    if not input_path.exists():
        return []

    events: list[dict[str, Any]] = []
    with input_path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            text = line.strip()
            if not text:
                continue
            try:
                raw_event = json.loads(text)
            except json.JSONDecodeError as exc:
                warnings.warn(
                    (
                        "Linie feedback JSONL ignorata: "
                        f"path={input_path} line={line_number} error={exc}"
                    ),
                    RuntimeWarning,
                    stacklevel=2,
                )
                continue
            if not isinstance(raw_event, dict):
                warnings.warn(
                    (
                        "Linie feedback JSONL ignorata: "
                        f"path={input_path} line={line_number} not_object"
                    ),
                    RuntimeWarning,
                    stacklevel=2,
                )
                continue
            events.append(_normalise_loaded_event(raw_event))
    return events


def clear_feedback_events(path: str | Path | None = None) -> None:
    output_path = _resolved_path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("", encoding="utf-8")


def feedback_event_to_dict(
    event_id: str | None = None,
    created_at: str | None = None,
    household_id: str | None = None,
    member_profile_id: str | None = None,
    dataset_profile: str | None = None,
    recipe_id: str | None = None,
    recipe_family_name: str | None = None,
    display_name: str | None = None,
    slot: str | None = None,
    feedback_type: str | None = None,
    source: str | None = None,
    run_id: str | None = None,
    plan_id: str | None = None,
    notes: str | None = None,
    **extra: Any,
) -> dict[str, Any]:
    feedback_type_value = _clean_text(feedback_type)
    if feedback_type_value not in FEEDBACK_TYPES:
        raise ValueError(f"feedback_type necunoscut: {feedback_type!r}")

    source_value = _clean_text(source) or "test"
    if source_value not in SOURCES:
        source_value = "test"

    payload = {
        "event_id": _clean_text(event_id) or str(uuid.uuid4()),
        "created_at": _clean_text(created_at) or _utc_now_iso(),
        "household_id": _clean_text(household_id),
        "member_profile_id": _clean_text(member_profile_id),
        "dataset_profile": _clean_text(dataset_profile),
        "recipe_id": _clean_text(recipe_id),
        "recipe_family_name": _clean_text(recipe_family_name),
        "display_name": _clean_text(display_name),
        "slot": _clean_text(slot),
        "feedback_type": feedback_type_value,
        "source": source_value,
        "run_id": _clean_text(run_id),
        "plan_id": _clean_text(plan_id),
        "notes": _clean_text(notes),
    }
    return {field: payload.get(field, "") for field in EVENT_FIELDS}


def _normalise_loaded_event(event: dict[str, Any]) -> dict[str, Any]:
    normalised = {
        field: _clean_text(event.get(field))
        for field in EVENT_FIELDS
    }
    if normalised["feedback_type"] not in FEEDBACK_TYPES:
        normalised["feedback_type"] = ""
    if normalised["source"] not in SOURCES:
        normalised["source"] = "test"
    return normalised


def _resolved_path(path: str | Path | None) -> Path:
    if path is None:
        return DEFAULT_FEEDBACK_EVENTS_PATH
    return Path(path)


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"none", "nan", "nat"}:
        return ""
    return text


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
