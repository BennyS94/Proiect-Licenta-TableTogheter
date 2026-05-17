from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd


EGG_GRAMS_PER_UNIT = 50.0


def audit_household_ingredient_load(
    plan: Mapping[str, Any],
    allocations: Sequence[Mapping[str, Any]],
    grocery_items: Sequence[Mapping[str, Any]] | None = None,
    config: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    config_data = dict(config or {})
    egg_load = compute_egg_load(
        plan,
        allocations,
        recipe_ingredients_df=config_data.get("recipe_ingredients_df"),
        config=config_data,
    )
    ingredient_rows = _ingredient_load_rows(grocery_items or [], plan, egg_load)
    return {
        "egg_load": egg_load,
        "ingredient_load_rows": ingredient_rows,
    }


def compute_egg_load(
    plan: Mapping[str, Any],
    allocations: Sequence[Mapping[str, Any]],
    recipe_ingredients_df: Any = None,
    config: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    ingredients = _coerce_ingredients_df(recipe_ingredients_df)
    if ingredients.empty:
        return _empty_egg_load("recipe_ingredients_missing")

    egg_rows = _egg_ingredient_rows(ingredients)
    if egg_rows.empty:
        return _empty_egg_load("no_egg_ingredients")

    by_recipe = {
        str(recipe_id): frame.copy()
        for recipe_id, frame in egg_rows.groupby("recipe_id", dropna=False)
    }
    breakdown: list[dict[str, Any]] = []
    for allocation in allocations:
        recipe_id = str(allocation.get("recipe_id") or "").strip()
        recipe_rows = by_recipe.get(recipe_id)
        if recipe_rows is None or recipe_rows.empty:
            continue
        multiplier = (
            _to_float(allocation.get("portion_multiplier_member"))
            or _to_float(allocation.get("portion_multiplier"))
            or 1.0
        )
        recipe_name = str(allocation.get("display_name") or allocation.get("recipe") or "")
        slot = str(allocation.get("slot") or "")
        source_type = classify_egg_source(recipe_name, recipe_rows, slot=slot)
        notes = _egg_source_note(source_type)
        for _, ingredient in recipe_rows.iterrows():
            base_grams = _to_float(ingredient.get("quantity_grams_estimated")) or 0.0
            if base_grams <= 0:
                continue
            grams = base_grams * multiplier
            breakdown.append(
                {
                    "recipe_id": recipe_id,
                    "recipe_name": recipe_name,
                    "member_id": allocation.get("member_id"),
                    "member_name": allocation.get("member"),
                    "day": allocation.get("day_index"),
                    "slot": slot,
                    "portion_multiplier": round(multiplier, 3),
                    "ingredient_raw_text": ingredient.get("ingredient_raw_text"),
                    "ingredient_name_normalized": ingredient.get("ingredient_name_normalized"),
                    "mapped_food_id": ingredient.get("mapped_food_id"),
                    "base_egg_grams": round(base_grams, 3),
                    "egg_grams_contribution": round(grams, 3),
                    "estimated_egg_count_contribution": round(grams / EGG_GRAMS_PER_UNIT, 3),
                    "egg_source_type": source_type,
                    "notes": notes,
                }
            )

    member_count = _member_count(plan, allocations)
    day_count = _day_count(plan)
    total_grams = sum(_to_float(row.get("egg_grams_contribution")) or 0.0 for row in breakdown)
    direct_grams = sum(
        _to_float(row.get("egg_grams_contribution")) or 0.0
        for row in breakdown
        if row.get("egg_source_type") == "direct"
    )
    embedded_grams = sum(
        _to_float(row.get("egg_grams_contribution")) or 0.0
        for row in breakdown
        if row.get("egg_source_type") == "embedded"
    )
    uncertain_grams = sum(
        _to_float(row.get("egg_grams_contribution")) or 0.0
        for row in breakdown
        if row.get("egg_source_type") == "uncertain"
    )
    total_count = total_grams / EGG_GRAMS_PER_UNIT
    direct_count = direct_grams / EGG_GRAMS_PER_UNIT
    embedded_count = embedded_grams / EGG_GRAMS_PER_UNIT
    uncertain_count = uncertain_grams / EGG_GRAMS_PER_UNIT
    eggs_per_person_per_day = total_count / max(member_count * day_count, 1)
    direct_per_person_per_day = direct_count / max(member_count * day_count, 1)
    total_status = _egg_status(eggs_per_person_per_day, total=True)
    direct_status = _egg_status(direct_per_person_per_day, total=False)
    status = _worst_status([direct_status, total_status])
    reasons = _egg_load_reasons(
        status=status,
        total_status=total_status,
        direct_status=direct_status,
        eggs_per_person_per_day=eggs_per_person_per_day,
        direct_eggs_per_person_per_day=direct_per_person_per_day,
    )
    return {
        "total_egg_grams": round(total_grams, 3),
        "total_egg_count": round(total_count, 3),
        "direct_egg_grams": round(direct_grams, 3),
        "direct_egg_count": round(direct_count, 3),
        "embedded_egg_grams": round(embedded_grams, 3),
        "embedded_egg_count": round(embedded_count, 3),
        "uncertain_egg_grams": round(uncertain_grams, 3),
        "uncertain_egg_count": round(uncertain_count, 3),
        "eggs_per_person_per_day": round(eggs_per_person_per_day, 3),
        "direct_eggs_per_person_per_day": round(direct_per_person_per_day, 3),
        "egg_load_status": status,
        "egg_load_reasons": reasons,
        "egg_source_breakdown": breakdown,
        "member_count": member_count,
        "days": day_count,
    }


def classify_egg_source(
    recipe_name: str,
    ingredient_rows: Any,
    slot: str | None = None,
) -> str:
    text = _normalise(f"{recipe_name} {slot or ''} {_ingredient_text(ingredient_rows)}")
    direct_keywords = [
        "boiled egg",
        "hard boiled egg",
        "hard cooked egg",
        "egg toast",
        "creamed egg",
        "omelet",
        "omelette",
        "scrambled egg",
        "egg bite",
        "frittata",
        "quiche",
        "eggs on toast",
        "egg spinach plate",
        "ham cheese egg toast",
    ]
    embedded_keywords = [
        "waffle",
        "pancake",
        "baked oatmeal",
        "batter",
        "muffin",
        "meatball",
        "binder",
        "cake",
        "dough",
        "casserole",
    ]
    if any(keyword in text for keyword in direct_keywords):
        return "direct"
    if any(keyword in text for keyword in embedded_keywords):
        return "embedded"
    if "egg" in text:
        return "uncertain"
    return "uncertain"


def candidate_egg_load(
    candidate: Mapping[str, Any],
    *,
    recipe_ingredients_df: Any,
    portion_multiplier: float = 1.0,
    config: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    ingredients = _coerce_ingredients_df(recipe_ingredients_df)
    if ingredients.empty:
        return _empty_candidate_load()
    recipe_id = str(candidate.get("recipe_id") or "").strip()
    if not recipe_id:
        return _empty_candidate_load()
    recipe_rows = ingredients.loc[ingredients["recipe_id"].astype(str) == recipe_id]
    egg_rows = _egg_ingredient_rows(recipe_rows)
    if egg_rows.empty:
        return _empty_candidate_load()
    source_type = classify_egg_source(
        str(candidate.get("display_name") or candidate.get("recipe") or ""),
        egg_rows,
        slot=str(candidate.get("slot") or ""),
    )
    grams = 0.0
    for _, row in egg_rows.iterrows():
        grams += (_to_float(row.get("quantity_grams_estimated")) or 0.0) * portion_multiplier
    count = grams / EGG_GRAMS_PER_UNIT
    direct_count = count if source_type == "direct" else 0.0
    embedded_count = count if source_type == "embedded" else 0.0
    uncertain_count = count if source_type == "uncertain" else 0.0
    penalty = candidate_egg_penalty(
        direct_egg_count=direct_count,
        total_egg_count=count,
        source_type=source_type,
        config=config,
    )
    status = _candidate_status(direct_count, count, source_type)
    return {
        "egg_grams": round(grams, 3),
        "egg_count": round(count, 3),
        "direct_egg_count": round(direct_count, 3),
        "embedded_egg_count": round(embedded_count, 3),
        "uncertain_egg_count": round(uncertain_count, 3),
        "egg_source_type": source_type,
        "egg_load_status": status,
        "egg_load_penalty": round(penalty, 6),
        "egg_load_reasons": _candidate_reasons(status, direct_count, count, source_type),
    }


def candidate_egg_penalty(
    *,
    direct_egg_count: float,
    total_egg_count: float,
    source_type: str,
    config: Mapping[str, Any] | None = None,
) -> float:
    config_data = dict(config or {})
    warning_weight = _to_float(config_data.get("egg_guard_warning_penalty_per_egg")) or 0.25
    severe_weight = _to_float(config_data.get("egg_guard_severe_penalty_per_egg")) or 0.55
    total_weight = _to_float(config_data.get("egg_guard_total_penalty_per_egg")) or 0.08
    penalty = 0.0
    if source_type == "direct":
        if direct_egg_count > 3.0:
            penalty += (direct_egg_count - 2.0) * severe_weight
        elif direct_egg_count > 2.0:
            penalty += (direct_egg_count - 2.0) * warning_weight
    if source_type != "embedded" and total_egg_count > 4.0:
        penalty += (total_egg_count - 4.0) * total_weight
    return max(0.0, penalty)


def _ingredient_load_rows(
    grocery_items: Sequence[Mapping[str, Any]],
    plan: Mapping[str, Any],
    egg_load: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    member_count = max(1, int(egg_load.get("member_count") or _member_count(plan, [])))
    day_count = max(1, int(egg_load.get("days") or _day_count(plan)))
    total_grocery_grams = sum(_to_float(item.get("total_grams")) or 0.0 for item in grocery_items)
    for item in grocery_items:
        name = str(item.get("display_name_clean") or item.get("display_name") or "")
        total_grams = _to_float(item.get("total_grams")) or 0.0
        normalised = _normalise(name)
        status = "normal"
        reason = "normal reuse or realistic household quantity"
        if normalised == "pressed" or "unclear_grocery_item_name" in item.get("warnings", []):
            status = "unclear_review"
            reason = "unclear grocery item identity"
        elif normalised == "eggs":
            status = str(egg_load.get("egg_load_status") or "normal")
            reason = "; ".join(egg_load.get("egg_load_reasons") or [])
        elif total_grocery_grams and total_grams / total_grocery_grams > 0.35:
            status = "warning"
            reason = "one item dominates grocery grams; review whether repeated meals are realistic"
        rows.append(
            {
                "display_name": name,
                "category": item.get("grocery_category") or item.get("category"),
                "total_grams": round(total_grams, 3),
                "grams_per_person_per_day": round(total_grams / member_count / day_count, 3),
                "purchase_display": item.get("purchase_display"),
                "ingredient_load_status": status,
                "reason": reason,
            }
        )
    return rows


def _egg_ingredient_rows(ingredients: pd.DataFrame) -> pd.DataFrame:
    if ingredients.empty:
        return ingredients.copy()
    return ingredients[
        ingredients.apply(
            lambda row: _is_egg_ingredient(
                row.get("mapped_food_id"),
                row.get("mapped_food_canonical_name"),
                row.get("ingredient_name_normalized"),
                row.get("ingredient_raw_text"),
            ),
            axis=1,
        )
    ].copy()


def _coerce_ingredients_df(value: Any) -> pd.DataFrame:
    if isinstance(value, pd.DataFrame):
        return value
    if isinstance(value, (str, Path)) and str(value).strip():
        path = Path(value)
        if path.exists():
            return pd.read_csv(path)
    return pd.DataFrame()


def _ingredient_text(ingredient_rows: Any) -> str:
    if isinstance(ingredient_rows, pd.DataFrame):
        values: list[str] = []
        for column in ["ingredient_raw_text", "ingredient_name_normalized", "mapped_food_canonical_name"]:
            if column in ingredient_rows.columns:
                values.extend(str(value or "") for value in ingredient_rows[column].tolist())
        return " ".join(values)
    if isinstance(ingredient_rows, Sequence) and not isinstance(ingredient_rows, (str, bytes)):
        return " ".join(str(row) for row in ingredient_rows)
    return str(ingredient_rows or "")


def _is_egg_ingredient(*values: object) -> bool:
    text = _normalise(" ".join(str(value or "") for value in values))
    return "egg" in text and "eggplant" not in text


def _member_count(
    plan: Mapping[str, Any],
    allocations: Sequence[Mapping[str, Any]],
) -> int:
    ids = {
        str(row.get("member_id") or "").strip()
        for row in allocations
        if str(row.get("member_id") or "").strip()
    }
    if ids:
        return len(ids)
    summary = plan.get("household_summary") if isinstance(plan.get("household_summary"), dict) else {}
    return max(1, int(_to_float(summary.get("member_count")) or 1))


def _day_count(plan: Mapping[str, Any]) -> int:
    summary = plan.get("household_summary") if isinstance(plan.get("household_summary"), dict) else {}
    if summary.get("days_generated"):
        return max(1, int(_to_float(summary.get("days_generated")) or 1))
    days = plan.get("days") if isinstance(plan.get("days"), list) else []
    return max(1, len(days))


def _egg_status(value: float, *, total: bool) -> str:
    if total:
        if value > 4.0:
            return "severe_warning"
        if value > 3.0:
            return "warning"
        return "ok"
    if value > 3.0:
        return "severe_warning"
    if value > 2.0:
        return "warning"
    return "ok"


def _worst_status(statuses: Sequence[str]) -> str:
    priority = {"ok": 0, "warning": 1, "severe_warning": 2}
    return max(statuses or ["ok"], key=lambda status: priority.get(status, 0))


def _candidate_status(direct_count: float, total_count: float, source_type: str) -> str:
    if source_type != "direct":
        return "ok"
    if direct_count > 3.0 or total_count > 4.0:
        return "severe_warning"
    if direct_count > 2.0:
        return "warning"
    return "ok"


def _egg_load_reasons(
    *,
    status: str,
    total_status: str,
    direct_status: str,
    eggs_per_person_per_day: float,
    direct_eggs_per_person_per_day: float,
) -> list[str]:
    reasons: list[str] = []
    if direct_status == "severe_warning":
        reasons.append(
            f"direct eggs exceed 3/person/day ({direct_eggs_per_person_per_day:.2f})"
        )
    elif direct_status == "warning":
        reasons.append(
            f"direct eggs exceed 2/person/day ({direct_eggs_per_person_per_day:.2f})"
        )
    if total_status == "severe_warning":
        reasons.append(f"total eggs exceed 4/person/day ({eggs_per_person_per_day:.2f})")
    elif total_status == "warning":
        reasons.append(f"total eggs exceed 3/person/day ({eggs_per_person_per_day:.2f})")
    if not reasons and status == "ok":
        reasons.append("egg load within demo thresholds")
    return reasons


def _candidate_reasons(
    status: str,
    direct_count: float,
    total_count: float,
    source_type: str,
) -> list[str]:
    if status == "ok":
        return []
    if source_type == "direct":
        return [f"direct egg meal has about {direct_count:.1f} eggs"]
    return [f"egg-heavy candidate has about {total_count:.1f} eggs"]


def _egg_source_note(source_type: str) -> str:
    if source_type == "direct":
        return "visible egg meal/protein"
    if source_type == "embedded":
        return "egg likely embedded in batter/binder"
    return "egg ingredient present but role not certain"


def _empty_egg_load(reason: str) -> dict[str, Any]:
    return {
        "total_egg_grams": 0.0,
        "total_egg_count": 0.0,
        "direct_egg_grams": 0.0,
        "direct_egg_count": 0.0,
        "embedded_egg_grams": 0.0,
        "embedded_egg_count": 0.0,
        "uncertain_egg_grams": 0.0,
        "uncertain_egg_count": 0.0,
        "eggs_per_person_per_day": 0.0,
        "direct_eggs_per_person_per_day": 0.0,
        "egg_load_status": "ok",
        "egg_load_reasons": [reason],
        "egg_source_breakdown": [],
        "member_count": 0,
        "days": 0,
    }


def _empty_candidate_load() -> dict[str, Any]:
    return {
        "egg_grams": 0.0,
        "egg_count": 0.0,
        "direct_egg_count": 0.0,
        "embedded_egg_count": 0.0,
        "uncertain_egg_count": 0.0,
        "egg_source_type": "",
        "egg_load_status": "ok",
        "egg_load_penalty": 0.0,
        "egg_load_reasons": [],
    }


def _normalise(value: Any) -> str:
    text = str(value or "").lower().replace("_", " ").replace("-", " ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return " ".join(text.split())


def _to_float(value: Any) -> float | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
