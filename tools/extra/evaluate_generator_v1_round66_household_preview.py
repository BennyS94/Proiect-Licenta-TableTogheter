from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from round65_household_common import AUDIT_DIR, generate_primary_member_plan  # noqa: E402
from src.generator_v1.household_preview import (  # noqa: E402
    DEFAULT_ALLOCATION_MODE,
    build_household_preview,
    household_preview_readable_lines,
    load_household_profile,
)


SUMMARY_OUT = AUDIT_DIR / "generator_v1_round66_household_preview_summary.txt"
ALLOCATIONS_OUT = AUDIT_DIR / "generator_v1_round66_household_preview_allocations.csv"
MEMBER_MACROS_OUT = AUDIT_DIR / "generator_v1_round66_household_preview_member_macros.csv"
GROCERY_SCALING_OUT = AUDIT_DIR / "generator_v1_round66_household_preview_grocery_scaling.csv"


def main() -> None:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    household_profile = load_household_profile()
    generation = generate_primary_member_plan(household_profile, days=3)
    preview = build_household_preview(
        generation["plan"],
        household_profile,
        allocation_mode=DEFAULT_ALLOCATION_MODE,
    )

    pd.DataFrame(preview.get("allocations", [])).to_csv(ALLOCATIONS_OUT, index=False)
    pd.DataFrame(preview.get("member_daily_rows", [])).to_csv(MEMBER_MACROS_OUT, index=False)
    pd.DataFrame(preview.get("grocery_scaling", [])).to_csv(GROCERY_SCALING_OUT, index=False)
    SUMMARY_OUT.write_text(
        _summary_text(generation=generation, preview=preview),
        encoding="utf-8",
    )

    print("Round66 household preview audit written")
    print(f"summary={SUMMARY_OUT}")
    print(f"allocations={ALLOCATIONS_OUT}")
    print(f"member_macros={MEMBER_MACROS_OUT}")
    print(f"grocery_scaling={GROCERY_SCALING_OUT}")
    print(
        "preview="
        f"members={preview.get('summary', {}).get('member_count', 0)}; "
        f"shared_meals={preview.get('summary', {}).get('shared_meal_count', 0)}; "
        f"max_grocery_factor={preview.get('summary', {}).get('max_grocery_scaling_factor')}"
    )


def _summary_text(*, generation: dict[str, Any], preview: dict[str, Any]) -> str:
    plan = generation.get("plan", {})
    plan_summary = plan.get("multi_day_summary", {}) if isinstance(plan, dict) else {}
    dataset = generation.get("dataset", {})
    summary = preview.get("summary", {})
    member_rows = preview.get("member_daily_rows", [])
    warning_counts = summary.get("warning_counts", {}) if isinstance(summary, dict) else {}
    mean_kcal_dev = _mean_abs(member_rows, "kcal_deviation_pct")
    max_kcal_dev = _max_abs(member_rows, "kcal_deviation_pct")
    mean_protein_dev = _mean_abs(member_rows, "protein_deviation_pct")
    max_protein_dev = _max_abs(member_rows, "protein_deviation_pct")
    usable = (
        int(plan_summary.get("accept_day_count", 0) or 0) == 3
        and float(max_kcal_dev or 0.0) <= 20.0
        and int(summary.get("shared_meal_count", 0) or 0) > 0
    )
    lines = [
        "Round66 Household preview audit",
        "",
        f"dataset_profile={dataset.get('dataset_profile') if isinstance(dataset, dict) else ''}",
        f"member_count={summary.get('member_count')}",
        f"days_generated={summary.get('days_generated')}",
        f"plan_valid_day_count={plan_summary.get('valid_day_count')}",
        f"plan_accept_day_count={plan_summary.get('accept_day_count')}",
        f"multi_day_loss={plan_summary.get('multi_day_loss')}",
        f"allocation_mode={summary.get('allocation_mode')}",
        f"shared_meal_count={summary.get('shared_meal_count')}",
        f"allocation_count={summary.get('allocation_count')}",
        f"min_portion_multiplier={summary.get('min_portion_multiplier')}",
        f"max_portion_multiplier={summary.get('max_portion_multiplier')}",
        f"review_meal_count={summary.get('review_meal_count')}",
        f"max_grocery_scaling_factor={summary.get('max_grocery_scaling_factor')}",
        "",
        "Per-member macro deviations:",
        f"mean_abs_kcal_deviation_pct={mean_kcal_dev}",
        f"max_abs_kcal_deviation_pct={max_kcal_dev}",
        f"mean_abs_protein_deviation_pct={mean_protein_dev}",
        f"max_abs_protein_deviation_pct={max_protein_dev}",
        "",
        f"warning_counts={_format_counts(warning_counts)}",
        f"preview_usable_for_demo={usable}",
        "",
        "Warnings / limitations:",
        *[f"- {warning}" for warning in preview.get("warnings", [])],
        "",
        "Copy-friendly sample:",
        *household_preview_readable_lines(preview)[:80],
    ]
    return "\n".join(lines) + "\n"


def _mean_abs(rows: list[dict[str, Any]], field: str) -> float:
    values = [abs(_to_float(row.get(field)) or 0.0) for row in rows]
    return round(sum(values) / len(values), 1) if values else 0.0


def _max_abs(rows: list[dict[str, Any]], field: str) -> float:
    values = [abs(_to_float(row.get(field)) or 0.0) for row in rows]
    return round(max(values), 1) if values else 0.0


def _to_float(value: object) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(numeric):
        return None
    return numeric


def _format_counts(value: object) -> str:
    if not isinstance(value, dict) or not value:
        return "none"
    return "; ".join(f"{key}={item}" for key, item in sorted(value.items()))


if __name__ == "__main__":
    main()
