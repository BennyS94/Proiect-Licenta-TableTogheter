from __future__ import annotations

from collections.abc import Mapping

from src.generator_v1.macro_fit import clamp


def compute_feedback_fit(
    candidate_row: Mapping[str, object],
    preference_context: Mapping[str, object] | None,
) -> dict[str, object]:
    recipe_id = _clean_text(candidate_row.get("recipe_id"))
    context = preference_context if isinstance(preference_context, Mapping) else {}
    hard_filters = _mapping(context.get("hard_filters"))
    score_preferences = _mapping(context.get("score_preferences"))

    reasons: list[str] = []
    if recipe_id and recipe_id in _string_set(hard_filters.get("banned_recipe_ids")):
        return {
            "feedback_fit": 0.0,
            "feedback_bonus_penalty": -0.50,
            "feedback_reasons": ["explicit_avoid_should_have_been_filtered"],
        }

    liked_counts = _count_mapping(score_preferences.get("liked_recipe_ids"))
    disliked_counts = _count_mapping(score_preferences.get("disliked_recipe_ids"))
    liked_count = liked_counts.get(recipe_id, 0)
    disliked_count = disliked_counts.get(recipe_id, 0)

    liked_delta = min(0.25, 0.12 * liked_count)
    disliked_delta = min(0.30, 0.15 * disliked_count)
    if liked_count:
        reasons.append(f"liked_count:{liked_count}")
    if disliked_count:
        reasons.append(f"disliked_count:{disliked_count}")
    if liked_count and disliked_count:
        reasons.append("mixed_feedback_net_score")

    bonus_penalty = liked_delta - disliked_delta
    feedback_fit = clamp(0.50 + bonus_penalty)
    if not reasons:
        reasons.append("feedback_neutral")

    return {
        "feedback_fit": round(feedback_fit, 4),
        "feedback_bonus_penalty": round(bonus_penalty, 4),
        "feedback_reasons": reasons,
    }


def _mapping(value: object) -> Mapping[str, object]:
    if isinstance(value, Mapping):
        return value
    return {}


def _count_mapping(value: object) -> dict[str, int]:
    if not isinstance(value, Mapping):
        return {}
    result: dict[str, int] = {}
    for key, item in value.items():
        recipe_id = _clean_text(key)
        if not recipe_id:
            continue
        try:
            count = int(item)
        except (TypeError, ValueError):
            count = 0
        if count > 0:
            result[recipe_id] = count
    return result


def _string_set(value: object) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, str):
        value = [value]
    try:
        return {str(item).strip() for item in value if str(item).strip()}
    except TypeError:
        return set()


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"none", "nan", "nat"}:
        return ""
    return text
