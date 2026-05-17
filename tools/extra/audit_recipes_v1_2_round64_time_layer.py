from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.generator_v1.recipe_time_layer import normalize_recipe_time  # noqa: E402


RECIPES_PATH = ROOT / "data/recipesdb/draft/v1_2_demo_final/recipes.csv"
INGREDIENTS_PATH = ROOT / "data/recipesdb/draft/v1_2_demo_final/recipe_ingredients.csv"
NUTRITION_PATH = ROOT / "data/recipesdb/draft/v1_2_demo_final/recipe_nutrition_cache.csv"
AUDIT_DIR = ROOT / "data/recipesdb/audit"
SUMMARY_OUT = AUDIT_DIR / "recipes_v1_2_round64_time_inventory_summary.txt"
AUDIT_OUT = AUDIT_DIR / "recipes_v1_2_round64_time_audit.csv"
RISK_OUT = AUDIT_DIR / "recipes_v1_2_round64_time_risk_review.csv"


def main() -> None:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    recipes = pd.read_csv(RECIPES_PATH)
    ingredients = pd.read_csv(INGREDIENTS_PATH)
    nutrition = pd.read_csv(NUTRITION_PATH)
    audit_rows = [_audit_recipe(row) for _, row in recipes.iterrows()]
    audit = pd.DataFrame(audit_rows)
    risk = audit.loc[audit["time_status"].isin(["partial", "missing", "suspicious"])].copy()
    audit.to_csv(AUDIT_OUT, index=False)
    risk.to_csv(RISK_OUT, index=False)
    SUMMARY_OUT.write_text(
        _summary_text(
            audit=audit,
            recipes=recipes,
            ingredients=ingredients,
            nutrition=nutrition,
        ),
        encoding="utf-8",
    )
    print("Round64 time inventory audit written")
    print(f"summary={SUMMARY_OUT}")
    print(f"audit={AUDIT_OUT}")
    print(f"risk_review={RISK_OUT}")
    print(
        "rows="
        f"{len(audit)}; suspicious={int((audit['time_status'] == 'suspicious').sum())}; "
        f"needs_review={len(risk)}"
    )


def _audit_recipe(row: pd.Series) -> dict[str, Any]:
    normalized = normalize_recipe_time(row)
    risk_flags = _risk_flags(row, normalized)
    time_status = _time_status(row, risk_flags)
    return {
        "recipe_id": row.get("recipe_id"),
        "display_name": row.get("display_name"),
        "recipe_kind": row.get("recipe_kind"),
        "allowed_slots_json": row.get("allowed_slots_json"),
        "prep_time_min": row.get("prep_time_min"),
        "cook_time_min": row.get("cook_time_min"),
        "total_time_min": row.get("total_time_min"),
        "existing_active_time_estimated_min": row.get("active_time_estimated_min"),
        "existing_passive_time_estimated_min": row.get("passive_time_estimated_min"),
        "existing_effective_time_min_for_scoring": row.get("effective_time_min_for_scoring"),
        "normalized_active_time_estimated_min": normalized["active_time_estimated_min"],
        "normalized_passive_time_estimated_min": normalized["passive_time_estimated_min"],
        "normalized_total_elapsed_time_min": normalized["total_elapsed_time_min"],
        "normalized_effective_time_min_for_scoring": normalized[
            "effective_time_min_for_scoring"
        ],
        "has_long_passive_time": normalized["has_long_passive_time"],
        "time_confidence": normalized["time_confidence"],
        "time_estimation_method": normalized["time_estimation_method"],
        "directions_step_count": _directions_step_count(row),
        "directions_summary": _directions_summary(row),
        "time_status": time_status,
        "time_risk_flags": ";".join(risk_flags),
        "time_warnings": ";".join(normalized["time_warnings"]),
        "time_reasons": ";".join(normalized["time_reasons"]),
    }


def _risk_flags(row: pd.Series, normalized: dict[str, Any]) -> list[str]:
    flags: list[str] = []
    prep = _to_float(row.get("prep_time_min"))
    cook = _to_float(row.get("cook_time_min"))
    total = _to_float(row.get("total_time_min"))
    effective = _to_float(normalized.get("effective_time_min_for_scoring"))
    total_elapsed = _to_float(normalized.get("total_elapsed_time_min"))
    passive = _to_float(normalized.get("passive_time_estimated_min")) or 0.0
    allowed_slots = _allowed_slots(row.get("allowed_slots_json"))
    kind = str(row.get("recipe_kind") or "").lower()
    warnings = set(normalized.get("time_warnings") or [])

    if total is None:
        flags.append("missing_total_time")
    if prep is not None and cook is not None and total is not None and total + 1e-6 < prep + cook:
        flags.append("total_less_than_prep_plus_cook")
    if total is not None and total >= 240:
        flags.append("very_long_total_time")
    if cook is not None and cook >= 180:
        flags.append("very_long_cook_time")
    if passive >= 30 or "likely_passive_time" in warnings:
        flags.append("likely_passive_time")
    if normalized.get("active_time_estimated_min") is None:
        flags.append("no_active_time")
    if normalized.get("time_confidence") in {"", None, "unknown"}:
        flags.append("no_confidence")
    if ("snack" in kind or allowed_slots == ["snack"]) and (
        (effective is not None and effective > 20) or (total_elapsed is not None and total_elapsed > 30)
    ):
        flags.append("snack_too_long")
    if ("breakfast" in kind or allowed_slots == ["breakfast"]) and (
        (effective is not None and effective > 45) or (total_elapsed is not None and total_elapsed > 60)
    ):
        flags.append("breakfast_too_long")
    if {"lunch", "dinner"} & set(allowed_slots) and effective is not None and effective > 90:
        flags.append("main_too_long")
    return _unique(flags)


def _time_status(row: pd.Series, flags: list[str]) -> str:
    has_prep = _to_float(row.get("prep_time_min")) is not None
    has_cook = _to_float(row.get("cook_time_min")) is not None
    has_total = _to_float(row.get("total_time_min")) is not None
    if any(
        flag in flags
        for flag in (
            "total_less_than_prep_plus_cook",
            "very_long_total_time",
            "very_long_cook_time",
            "no_active_time",
            "no_confidence",
            "snack_too_long",
            "breakfast_too_long",
            "main_too_long",
        )
    ):
        return "suspicious"
    if not has_prep and not has_cook and not has_total:
        return "missing"
    if not (has_prep and has_cook and has_total):
        return "partial"
    return "complete"


def _summary_text(
    *,
    audit: pd.DataFrame,
    recipes: pd.DataFrame,
    ingredients: pd.DataFrame,
    nutrition: pd.DataFrame,
) -> str:
    status_counts = Counter(audit["time_status"])
    confidence_counts = Counter(audit["time_confidence"])
    warning_counts = Counter()
    risk_counts = Counter()
    for value in audit["time_warnings"].fillna(""):
        warning_counts.update(_split_codes(value))
    for value in audit["time_risk_flags"].fillna(""):
        risk_counts.update(_split_codes(value))
    with_prep = pd.to_numeric(recipes.get("prep_time_min"), errors="coerce").notna().sum()
    with_cook = pd.to_numeric(recipes.get("cook_time_min"), errors="coerce").notna().sum()
    with_total = pd.to_numeric(recipes.get("total_time_min"), errors="coerce").notna().sum()
    complete_source = int(
        (
            pd.to_numeric(recipes.get("prep_time_min"), errors="coerce").notna()
            & pd.to_numeric(recipes.get("cook_time_min"), errors="coerce").notna()
            & pd.to_numeric(recipes.get("total_time_min"), errors="coerce").notna()
        ).sum()
    )
    lines = [
        "Recipes_DB v1.2 Round64 time inventory summary",
        "",
        f"recipes_path={RECIPES_PATH}",
        f"ingredients_rows={len(ingredients)}",
        f"nutrition_rows={len(nutrition)}",
        f"total_recipes={len(recipes)}",
        f"recipes_with_prep_time={with_prep}",
        f"recipes_with_cook_time={with_cook}",
        f"recipes_with_total_time={with_total}",
        f"recipes_with_complete_prep_cook_total={complete_source}",
        f"missing_time_fields={int((audit['time_status'] == 'missing').sum())}",
        f"suspicious_time_fields={int((audit['time_status'] == 'suspicious').sum())}",
        f"likely_passive_time_count={risk_counts.get('likely_passive_time', 0)}",
        f"recipes_needing_review={int(audit['time_status'].isin(['partial', 'missing', 'suspicious']).sum())}",
        "",
        f"time_status_counts={_format_counts(status_counts)}",
        f"time_confidence_distribution={_format_counts(confidence_counts)}",
        f"time_risk_flag_counts={_format_counts(risk_counts)}",
        f"time_warning_counts={_format_counts(warning_counts)}",
        "",
        "Top review rows:",
    ]
    review = audit.loc[audit["time_status"].isin(["partial", "missing", "suspicious"])]
    for _, row in review.head(20).iterrows():
        lines.append(
            f"- {row['recipe_id']} | {row['display_name']} | "
            f"status={row['time_status']} | flags={row['time_risk_flags']}"
        )
    return "\n".join(lines) + "\n"


def _directions_step_count(row: pd.Series) -> int:
    value = row.get("directions_step_count")
    numeric = _to_float(value)
    if numeric is not None:
        return int(numeric)
    steps = _directions_steps(row.get("directions_json"))
    return len(steps)


def _directions_summary(row: pd.Series) -> str:
    steps = _directions_steps(row.get("directions_json"))
    text = " ".join(str(step) for step in steps[:2])
    text = " ".join(text.split())
    if len(text) > 360:
        return text[:357].rstrip() + "..."
    return text


def _directions_steps(value: object) -> list[str]:
    if value is None:
        return []
    text = str(value).strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return [text]
    if isinstance(parsed, list):
        return [str(item) for item in parsed]
    return [str(parsed)]


def _allowed_slots(value: object) -> list[str]:
    if value is None:
        return []
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        parsed = [str(value)]
    if not isinstance(parsed, list):
        return []
    return [str(item).strip().lower() for item in parsed if str(item).strip()]


def _to_float(value: object) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(numeric):
        return None
    return numeric


def _split_codes(value: str) -> list[str]:
    return [part.strip() for part in str(value or "").split(";") if part.strip()]


def _format_counts(counter: Counter | dict[str, int]) -> str:
    if not counter:
        return "none"
    return "; ".join(f"{key}={value}" for key, value in sorted(counter.items()))


def _unique(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            result.append(value)
    return result


if __name__ == "__main__":
    main()
