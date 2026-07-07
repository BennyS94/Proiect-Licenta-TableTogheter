from __future__ import annotations

import json
import math
import re
from typing import Any, Mapping, Sequence

import pandas as pd


DEFAULT_PAIRING_SHARED_SLOTS = {"lunch", "dinner"}
DEFAULT_MIN_PAIR_SCORE = 52.0


def annotate_mixed_vegetarian_pairing(
    candidates: pd.DataFrame,
    *,
    member_targets: Mapping[str, Any],
    config: Mapping[str, Any],
    shared_slots: Sequence[str],
) -> pd.DataFrame:
    if candidates.empty or "recipe_id" not in candidates.columns:
        return candidates.copy()

    members = _members(member_targets)
    vegetarian_member_ids = {
        member_id
        for member_id, member in members.items()
        if _is_vegetarian_member(member)
    }
    if not vegetarian_member_ids or len(members) <= len(vegetarian_member_ids):
        return _with_pairing_defaults(candidates)

    allowed_by_member = _allowed_recipe_ids_by_member(config)
    if not allowed_by_member:
        return _with_pairing_defaults(candidates)

    shared_slot_set = {
        str(slot)
        for slot in shared_slots
        if str(slot) in DEFAULT_PAIRING_SHARED_SLOTS
    }
    if not shared_slot_set:
        return _with_pairing_defaults(candidates)

    prepared = _with_pairing_defaults(candidates)
    row_records = [row.to_dict() for _, row in prepared.iterrows()]
    by_slot: dict[str, list[dict[str, Any]]] = {}
    for row in row_records:
        by_slot.setdefault(str(row.get("slot") or ""), []).append(row)

    updates: dict[int, dict[str, Any]] = {}
    for index, row in prepared.iterrows():
        row_dict = row.to_dict()
        slot = str(row_dict.get("slot") or "")
        if slot not in shared_slot_set:
            continue

        recipe_id = _clean_id(row_dict.get("recipe_id"))
        if not recipe_id:
            continue

        compatible = _compatible_member_ids(
            recipe_id,
            member_ids=members.keys(),
            allowed_by_member=allowed_by_member,
        )
        excluded_vegetarians = sorted(vegetarian_member_ids - compatible)
        if not excluded_vegetarians:
            updates[int(index)] = {
                "mixed_vegetarian_pairing_status": "shared_compatible",
                "mixed_vegetarian_compatible_member_ids_json": _json_list(sorted(compatible)),
            }
            continue

        normal_compatible = bool(compatible - vegetarian_member_ids)
        if not normal_compatible:
            continue

        companions = _rank_vegetarian_companions(
            source=row_dict,
            candidates=by_slot.get(slot, []),
            vegetarian_member_ids=excluded_vegetarians,
            allowed_by_member=allowed_by_member,
            min_pair_score=float(
                config.get("mixed_vegetarian_min_pair_score")
                or DEFAULT_MIN_PAIR_SCORE
            ),
        )
        if companions:
            primary = companions[0]
            updates[int(index)] = {
                "mixed_vegetarian_pairing_status": "paired",
                "mixed_vegetarian_compatible_member_ids_json": _json_list(sorted(compatible)),
                "household_vegetarian_companion_recipe_id": primary["recipe_id"],
                "household_vegetarian_companion_display_name": primary["display_name"],
                "household_vegetarian_companion_score": round(primary["score"], 1),
                "household_vegetarian_companion_template": primary["template"],
                "household_vegetarian_companion_recipe_ids_json": _json_list(
                    [item["recipe_id"] for item in companions]
                ),
            }
            prepared.at[index, "score_preview"] = _bounded_score(
                row_dict.get("score_preview"),
                delta=float(config.get("mixed_vegetarian_pair_bonus") or 0.015),
            )
        else:
            warnings = _append_warning(
                row_dict.get("household_fit_warnings"),
                "mixed_vegetarian_no_companion",
            )
            updates[int(index)] = {
                "mixed_vegetarian_pairing_status": "unpaired",
                "mixed_vegetarian_compatible_member_ids_json": _json_list(sorted(compatible)),
                "mixed_vegetarian_excluded_member_ids_json": _json_list(excluded_vegetarians),
                "household_fit_warnings": warnings,
            }
            prepared.at[index, "score_preview"] = _bounded_score(
                row_dict.get("score_preview"),
                delta=-float(config.get("mixed_vegetarian_unpaired_penalty") or 0.12),
            )
            prepared.at[index, "household_loss"] = round(
                (_to_float(row_dict.get("household_loss")) or 0.0)
                + float(config.get("mixed_vegetarian_unpaired_loss_penalty") or 0.08),
                6,
            )

    for index, values in updates.items():
        for key, value in values.items():
            prepared.at[index, key] = value
    return prepared


def preferred_companion_recipe_ids(meal: Mapping[str, Any]) -> list[str]:
    values = _parse_json_list(meal.get("household_vegetarian_companion_recipe_ids_json"))
    if values:
        return values
    recipe_id = _clean_id(meal.get("household_vegetarian_companion_recipe_id"))
    return [recipe_id] if recipe_id else []


def is_preferred_companion(recipe_id: str, preferred_recipe_ids: Sequence[str]) -> bool:
    return _clean_id(recipe_id) in {_clean_id(value) for value in preferred_recipe_ids}


def _with_pairing_defaults(candidates: pd.DataFrame) -> pd.DataFrame:
    result = candidates.copy()
    defaults = {
        "mixed_vegetarian_pairing_status": "",
        "mixed_vegetarian_compatible_member_ids_json": "[]",
        "mixed_vegetarian_excluded_member_ids_json": "[]",
        "household_vegetarian_companion_recipe_id": "",
        "household_vegetarian_companion_display_name": "",
        "household_vegetarian_companion_score": 0.0,
        "household_vegetarian_companion_template": "",
        "household_vegetarian_companion_recipe_ids_json": "[]",
    }
    for column, default in defaults.items():
        if column not in result.columns:
            result[column] = default
    return result


def _rank_vegetarian_companions(
    *,
    source: Mapping[str, Any],
    candidates: Sequence[Mapping[str, Any]],
    vegetarian_member_ids: Sequence[str],
    allowed_by_member: Mapping[str, set[str]],
    min_pair_score: float,
) -> list[dict[str, Any]]:
    source_recipe_id = _clean_id(source.get("recipe_id"))
    ranked: list[tuple[float, str, dict[str, Any]]] = []
    for candidate in candidates:
        recipe_id = _clean_id(candidate.get("recipe_id"))
        if not recipe_id or recipe_id == source_recipe_id:
            continue
        if not all(
            _recipe_allowed(recipe_id, member_id, allowed_by_member)
            for member_id in vegetarian_member_ids
        ):
            continue
        score = _pair_score(source, candidate)
        if score < min_pair_score:
            continue
        ranked.append(
            (
                -score,
                str(candidate.get("display_name") or ""),
                {
                    "recipe_id": recipe_id,
                    "display_name": str(candidate.get("display_name") or recipe_id),
                    "score": score,
                    "template": _recipe_template(candidate),
                },
            )
        )
    ranked.sort(key=lambda item: item[:2])
    return [item[2] for item in ranked[:5]]


def _pair_score(source: Mapping[str, Any], candidate: Mapping[str, Any]) -> float:
    score = 20.0
    source_template = _recipe_template(source)
    candidate_template = _recipe_template(candidate)
    if source_template == candidate_template:
        score += 28.0
    elif {source_template, candidate_template} <= {"bowl", "rice_bowl", "pasta"}:
        score += 16.0
    elif candidate_template != "other":
        score += 8.0

    score += _closeness_points(
        _to_float(source.get("kcal")),
        _to_float(candidate.get("kcal")),
        perfect_ratio=0.18,
        ok_ratio=0.35,
        points=18.0,
    )
    score += _closeness_points(
        _to_float(source.get("protein_g")),
        _to_float(candidate.get("protein_g")),
        perfect_ratio=0.30,
        ok_ratio=0.50,
        points=12.0,
    )
    score += _closeness_points(
        _time_value(source),
        _time_value(candidate),
        perfect_ratio=0.25,
        ok_ratio=0.45,
        points=8.0,
    )

    candidate_protein = _to_float(candidate.get("protein_g")) or 0.0
    if candidate_protein >= 30:
        score += 10.0
    elif candidate_protein >= 22:
        score += 6.0

    score_preview = _to_float(candidate.get("score_preview")) or 0.0
    score += min(max(score_preview, 0.0), 1.0) * 4.0
    return min(score, 100.0)


def _closeness_points(
    left: float | None,
    right: float | None,
    *,
    perfect_ratio: float,
    ok_ratio: float,
    points: float,
) -> float:
    if left is None or right is None or left <= 0 or right <= 0:
        return 0.0
    ratio = abs(left - right) / max(left, right)
    if ratio <= perfect_ratio:
        return points
    if ratio <= ok_ratio:
        return points * (1.0 - (ratio - perfect_ratio) / (ok_ratio - perfect_ratio))
    return 0.0


def _recipe_template(row: Mapping[str, Any]) -> str:
    text = _normalize_text(
        " ".join(
            str(row.get(key) or "")
            for key in ("display_name", "recipe", "title", "recipe_name")
        )
    )
    if any(term in text for term in ("pasta", "ziti", "lasagna", "spaghetti")):
        return "pasta"
    if any(term in text for term in ("fried rice", "rice bowl", "rice plate")):
        return "rice_bowl"
    if any(term in text for term in ("soup", "stew", "chili", "minestrone")):
        return "soup_stew_chili"
    if "salad" in text:
        return "salad"
    if any(term in text for term in ("casserole", "bake", "baked")):
        return "casserole_bake"
    if any(term in text for term in ("wrap", "sandwich", "toast", "burger", "tortilla")):
        return "wrap_sandwich_toast"
    if any(term in text for term in ("bowl", "plate")):
        return "bowl"
    return "other"


def _members(member_targets: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    result: dict[str, Mapping[str, Any]] = {}
    for member in member_targets.get("members", []) or []:
        if not isinstance(member, Mapping):
            continue
        member_id = _clean_id(member.get("member_id"))
        if member_id:
            result[member_id] = member
    return result


def _is_vegetarian_member(member: Mapping[str, Any]) -> bool:
    dietary = member.get("dietary_preferences")
    if isinstance(dietary, Mapping) and bool(dietary.get("vegetarian")):
        return True
    health = member.get("health_and_diet_preferences")
    if isinstance(health, Mapping):
        patterns = health.get("dietary_patterns")
        if isinstance(patterns, Mapping) and bool(patterns.get("vegetarian")):
            return True
    return False


def _compatible_member_ids(
    recipe_id: str,
    *,
    member_ids: Sequence[str],
    allowed_by_member: Mapping[str, set[str]],
) -> set[str]:
    return {
        member_id
        for member_id in member_ids
        if _recipe_allowed(recipe_id, member_id, allowed_by_member)
    }


def _recipe_allowed(
    recipe_id: str,
    member_id: str,
    allowed_by_member: Mapping[str, set[str]],
) -> bool:
    allowed_ids = allowed_by_member.get(str(member_id))
    if allowed_ids is None:
        return True
    return _clean_id(recipe_id) in allowed_ids


def _allowed_recipe_ids_by_member(config: Mapping[str, Any]) -> dict[str, set[str]]:
    raw = config.get("member_allowed_recipe_ids_by_member_id")
    if not isinstance(raw, Mapping):
        return {}
    result: dict[str, set[str]] = {}
    for member_id, values in raw.items():
        if values is None:
            continue
        if isinstance(values, str):
            iterable = [values]
        else:
            try:
                iterable = list(values)
            except TypeError:
                iterable = []
        result[str(member_id)] = {
            _clean_id(value)
            for value in iterable
            if _clean_id(value)
        }
    return result


def _append_warning(value: Any, warning: str) -> str:
    parts = [
        str(part).strip()
        for part in str(value or "").split(";")
        if str(part).strip()
    ]
    parts.append(warning)
    return ";".join(dict.fromkeys(parts))


def _bounded_score(value: Any, *, delta: float) -> float:
    base = _to_float(value) or 0.0
    return round(min(max(base + delta, 0.0), 1.0), 4)


def _time_value(row: Mapping[str, Any]) -> float | None:
    for key in (
        "effective_time_min_for_scoring",
        "total_time_min",
        "total_elapsed_time_min",
    ):
        value = _to_float(row.get(key))
        if value is not None and value > 0:
            return value
    return None


def _parse_json_list(value: Any) -> list[str]:
    if isinstance(value, list):
        source = value
    else:
        try:
            parsed = json.loads(str(value or "[]"))
        except json.JSONDecodeError:
            parsed = []
        source = parsed if isinstance(parsed, list) else []
    return [_clean_id(item) for item in source if _clean_id(item)]


def _json_list(values: Sequence[str]) -> str:
    return json.dumps([_clean_id(value) for value in values if _clean_id(value)])


def _clean_id(value: Any) -> str:
    return str(value or "").strip()


def _normalize_text(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value).lower()).strip()


def _to_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric) or math.isinf(numeric):
        return None
    return numeric
