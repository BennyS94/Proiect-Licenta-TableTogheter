from __future__ import annotations

from collections import Counter
from typing import Any


def build_household_preference_context(
    events: list[dict[str, Any]],
    household_id: str | None = None,
    member_profile_id: str | None = None,
    dataset_profile: str | None = None,
) -> dict[str, Any]:
    filtered_events = [
        event
        for event in events
        if _event_matches(
            event=event,
            household_id=household_id,
            member_profile_id=member_profile_id,
            dataset_profile=dataset_profile,
        )
    ]

    liked_counts: Counter[str] = Counter()
    disliked_counts: Counter[str] = Counter()
    too_long_counts: Counter[str] = Counter()
    banned_recipe_ids: set[str] = set()
    last_updated_at = ""

    for event in filtered_events:
        recipe_id = _clean_text(event.get("recipe_id"))
        feedback_type = _clean_text(event.get("feedback_type"))
        if not recipe_id:
            continue
        if feedback_type == "liked":
            liked_counts[recipe_id] += 1
        elif feedback_type == "disliked":
            disliked_counts[recipe_id] += 1
        elif feedback_type == "too_long":
            too_long_counts[recipe_id] += 1
        elif feedback_type == "explicit_avoid":
            banned_recipe_ids.add(recipe_id)
        created_at = _clean_text(event.get("created_at"))
        if created_at and created_at > last_updated_at:
            last_updated_at = created_at

    return {
        "household_id": _clean_text(household_id),
        "member_profile_id": _clean_text(member_profile_id),
        "dataset_profile": _clean_text(dataset_profile),
        "hard_filters": {
            "banned_recipe_ids": sorted(banned_recipe_ids),
            "banned_ingredient_names": [],
        },
        "score_preferences": {
            "liked_recipe_ids": dict(sorted(liked_counts.items())),
            "disliked_recipe_ids": dict(sorted(disliked_counts.items())),
        },
        "time_preferences": {
            "too_long_recipe_ids": dict(sorted(too_long_counts.items())),
            "household_time_sensitivity": "normal",
        },
        "meta": {
            "event_count": len(filtered_events),
            "last_updated_at": last_updated_at,
        },
    }


def _event_matches(
    event: dict[str, Any],
    household_id: str | None,
    member_profile_id: str | None,
    dataset_profile: str | None,
) -> bool:
    filters = {
        "household_id": _clean_text(household_id),
        "member_profile_id": _clean_text(member_profile_id),
        "dataset_profile": _clean_text(dataset_profile),
    }
    for key, expected in filters.items():
        if not expected:
            continue
        actual = _clean_text(event.get(key))
        if actual and actual != expected:
            return False
    return True


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"none", "nan", "nat"}:
        return ""
    return text
