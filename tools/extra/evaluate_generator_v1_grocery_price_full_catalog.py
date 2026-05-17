from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.generator_v1.candidate_filter import (  # noqa: E402
    build_household_preference_context,
    filter_recipe_candidates,
)
from src.generator_v1.data_loader import (  # noqa: E402
    V1_2_DEMO_FINAL_INGREDIENTS_PATH,
    V1_2_DEMO_FINAL_NUTRITION_PATH,
    V1_2_DEMO_FINAL_PROFILE,
    V1_2_DEMO_FINAL_RECIPES_PATH,
    load_fooddb_current,
    load_recipe_candidate_pool,
)
from src.generator_v1.grocery_list import (  # noqa: E402
    build_grocery_list,
    grocery_list_readable_lines,
    grocery_list_rows,
)
from src.generator_v1.grocery_pricing import load_grocery_product_catalog  # noqa: E402
from src.generator_v1.multi_day_selector import (  # noqa: E402
    MULTI_DAY_MODE_GLOBAL,
    generate_multi_day_plan,
)
from src.generator_v1.profile_loader import load_member_profile  # noqa: E402
from src.generator_v1.slot_candidates import build_slot_candidates  # noqa: E402
from src.generator_v1.target_builder import build_nutrition_target  # noqa: E402


PROFILE_PATH = ROOT / "profiles/member_profile_demo_v1.json"
CATALOG_PATH = ROOT / "data/grocery/draft/grocery_product_catalog_v1_full.csv"
BATCH_INPUT_PATH = ROOT / "data/grocery/draft/grocery_product_catalog_v1_price_research_filled_all.csv"
AUDIT_DIR = ROOT / "data/grocery/audit"
SUMMARY_OUT = AUDIT_DIR / "grocery_price_full_catalog_demo_summary.txt"
ITEMS_OUT = AUDIT_DIR / "grocery_price_full_catalog_demo_items.csv"
SAMPLE_OUT = AUDIT_DIR / "grocery_price_full_catalog_demo_sample.txt"


def main() -> None:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    batch = pd.read_csv(BATCH_INPUT_PATH).fillna("") if BATCH_INPUT_PATH.exists() else pd.DataFrame()
    catalog = load_grocery_product_catalog(CATALOG_PATH)
    grocery_list = build_demo_grocery_list()
    item_rows = grocery_list_rows(grocery_list, include_pantry_basics=True)
    sample_lines = grocery_list_readable_lines(
        grocery_list,
        include_pantry_basics=True,
        include_purchase_suggestions=True,
        include_price_estimates=True,
    )
    pd.DataFrame(item_rows).to_csv(ITEMS_OUT, index=False)
    SAMPLE_OUT.write_text("\n".join(sample_lines) + "\n", encoding="utf-8")
    SUMMARY_OUT.write_text(
        build_summary(
            batch=batch,
            catalog=catalog,
            grocery_list=grocery_list,
            item_rows=item_rows,
            sample_lines=sample_lines,
        ),
        encoding="utf-8",
    )
    summary = grocery_list.get("summary", {})
    pricing_summary = summary.get("pricing_summary", {}) if isinstance(summary, dict) else {}
    if not isinstance(pricing_summary, dict):
        pricing_summary = {}
    print("Generator v1 Grocery price full catalog audit written")
    print(f"summary={SUMMARY_OUT}")
    print(f"items={ITEMS_OUT}")
    print(f"sample={SAMPLE_OUT}")
    print(
        "pricing="
        f"priced={pricing_summary.get('priced_item_count', 0)}; "
        f"missing={pricing_summary.get('unpriced_item_count', 0)}; "
        f"total={pricing_summary.get('total_estimated_cost')} "
        f"{pricing_summary.get('currency', '')}"
    )


def build_demo_grocery_list() -> dict[str, Any]:
    profile = load_member_profile(PROFILE_PATH)
    target = build_nutrition_target(profile)
    pool = load_recipe_candidate_pool(
        recipes_path=ROOT / V1_2_DEMO_FINAL_RECIPES_PATH,
        ingredients_path=ROOT / V1_2_DEMO_FINAL_INGREDIENTS_PATH,
        nutrition_path=ROOT / V1_2_DEMO_FINAL_NUTRITION_PATH,
        dataset_profile=V1_2_DEMO_FINAL_PROFILE,
    )
    fooddb = load_fooddb_current()
    preference_context = build_household_preference_context(profile)
    filtered_candidates = filter_recipe_candidates(
        eligible_candidates=pool.eligible_candidates,
        ingredients=pool.ingredients,
        context=preference_context,
    )
    slot_candidates = build_slot_candidates(
        target=target,
        filtered_candidates=filtered_candidates,
        time_sensitivity=preference_context.time_sensitivity,
        ingredients=pool.ingredients,
        fooddb=fooddb,
        portion_policy_mode="target_aware",
    )
    plan = generate_multi_day_plan(
        profile=profile,
        target=target,
        slot_candidates=slot_candidates,
        days=3,
        config={
            "selection_mode": "balanced_day",
            "portion_policy": "target_aware",
            "meal_realism_mode": "practical",
            "quality_gate": "demo_safe",
            "alternative_count": 3,
            "return_alternatives": True,
            "multi_day_mode": MULTI_DAY_MODE_GLOBAL,
            "candidate_day_alternative_count": 10,
            "global_max_candidates_per_slot": 26,
            "day_candidate_pool_size_target": 75,
            "day_candidate_pool_max": 150,
            "include_slot_forced_variants": True,
            "no_repeat_policy": "hard",
            "multi_day_speed_mode": "fast",
            "day_candidate_builder": "direct_from_slots",
            "direct_slot_shortlist_size": 12,
        },
    )
    grocery_list = build_grocery_list(
        plan,
        pool.ingredients,
        fooddb_df=fooddb,
        config={
            "include_pantry_basics": True,
            "include_purchase_suggestions": True,
            "enable_cooked_to_raw_conversion": True,
            "include_price_estimates": True,
            "product_catalog_path": CATALOG_PATH,
            "exclude_water": True,
            "round_grams_for_display": True,
        },
    )
    grocery_list["full_catalog_plan_summary"] = plan.get("multi_day_summary", {})
    return grocery_list


def build_summary(
    *,
    batch: pd.DataFrame,
    catalog: list[dict[str, Any]],
    grocery_list: dict[str, Any],
    item_rows: list[dict[str, Any]],
    sample_lines: list[str],
) -> str:
    summary = grocery_list.get("summary", {})
    if not isinstance(summary, dict):
        summary = {}
    pricing_summary = summary.get("pricing_summary", {})
    if not isinstance(pricing_summary, dict):
        pricing_summary = {}
    plan_summary = grocery_list.get("full_catalog_plan_summary", {})
    if not isinstance(plan_summary, dict):
        plan_summary = {}
    catalog_status_counts = _counts(batch.get("decision_status", []))
    priced_rows = [row for row in item_rows if _to_float(row.get("estimated_cost")) is not None]
    missing_rows = [
        row
        for row in item_rows
        if _to_float(row.get("estimated_cost")) is None
        and _clean_text(row.get("price_warning"))
    ]
    lines = [
        "Generator v1 Grocery price full catalog demo audit",
        "",
        f"dataset_profile={V1_2_DEMO_FINAL_PROFILE}",
        f"selected_plan_days={plan_summary.get('actual_days_generated', 3)}",
        f"accept_day_count={plan_summary.get('accept_day_count', 0)}",
        "",
        f"total_catalog_rows={len(batch)}",
        f"safe_rows_applied={len(catalog)}",
        f"needs_review_count={catalog_status_counts.get('needs_review', 0)}",
        f"keep_deferred_count={catalog_status_counts.get('keep_deferred', 0)}",
        f"catalog_decision_status_counts={_format_counts(catalog_status_counts)}",
        "",
        f"grocery_items_total={len(item_rows)}",
        f"grocery_items_with_price={len(priced_rows)}",
        f"grocery_items_missing_price={len(missing_rows)}",
        f"estimated_total_cost={pricing_summary.get('total_estimated_cost')} {pricing_summary.get('currency', '')}",
        f"price_warning_counts={_format_counts(pricing_summary.get('price_warning_counts', {}))}",
        f"grocery_warning_counts={_format_counts(summary.get('warning_counts', {}))}",
        "",
        "Category subtotals:",
        *category_subtotals(item_rows),
        "",
        "Missing price items:",
        *missing_price_lines(missing_rows),
        "",
        "Sample copy-friendly grocery output:",
        *sample_lines[:110],
        "",
        "Scope notes:",
        "- price estimates are demo estimates only",
        "- no live price fetching",
        "- no store/brand optimization",
        "- needs_review and keep_deferred rows are not applied",
    ]
    return "\n".join(lines) + "\n"


def category_subtotals(item_rows: list[dict[str, Any]]) -> list[str]:
    subtotals: dict[str, float] = {}
    for row in item_rows:
        cost = _to_float(row.get("estimated_cost"))
        if cost is None:
            continue
        category = _clean_text(row.get("category_label")) or _clean_text(row.get("grocery_category")) or "Other"
        subtotals[category] = subtotals.get(category, 0.0) + cost
    if not subtotals:
        return ["none"]
    return [
        f"- {category}: {cost:.2f} RON"
        for category, cost in sorted(subtotals.items(), key=lambda item: (-item[1], item[0]))
    ]


def missing_price_lines(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["none"]
    return [
        f"- {row.get('display_name_clean')} | warning={row.get('price_warning')}"
        for row in rows
    ]


def _counts(values: Any) -> dict[str, int]:
    result: dict[str, int] = {}
    for value in values:
        key = _clean_text(value) or "blank"
        result[key] = result.get(key, 0) + 1
    return dict(sorted(result.items(), key=lambda item: (-item[1], item[0])))


def _format_counts(value: Any) -> str:
    if not isinstance(value, dict) or not value:
        return "none"
    return "; ".join(f"{key}={count}" for key, count in value.items())


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "none", "null"}:
        return ""
    return text


def _to_float(value: Any) -> float | None:
    text = _clean_text(value)
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


if __name__ == "__main__":
    main()
