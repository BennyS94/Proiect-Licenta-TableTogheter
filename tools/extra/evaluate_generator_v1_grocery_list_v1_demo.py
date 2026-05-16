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
    grocery_list_recipe_breakdown_rows,
    grocery_list_rows,
)
from src.generator_v1.multi_day_selector import (  # noqa: E402
    MULTI_DAY_MODE_GLOBAL,
    generate_multi_day_plan,
)
from src.generator_v1.profile_loader import load_member_profile  # noqa: E402
from src.generator_v1.slot_candidates import build_slot_candidates  # noqa: E402
from src.generator_v1.target_builder import build_nutrition_target  # noqa: E402


PROFILE_PATH = ROOT / "profiles/member_profile_demo_v1.json"
AUDIT_DIR = ROOT / "data/recipesdb/audit"
SUMMARY_OUT = AUDIT_DIR / "generator_v1_grocery_list_v1_demo_summary.txt"
ITEMS_OUT = AUDIT_DIR / "generator_v1_grocery_list_v1_demo_items.csv"
BREAKDOWN_OUT = AUDIT_DIR / "generator_v1_grocery_list_v1_demo_recipe_breakdown.csv"


def main() -> None:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
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
            "exclude_water": True,
        },
    )
    item_rows = grocery_list_rows(grocery_list, include_pantry_basics=True)
    breakdown_rows = grocery_list_recipe_breakdown_rows(
        grocery_list,
        include_pantry_basics=True,
    )
    pd.DataFrame(item_rows).to_csv(ITEMS_OUT, index=False)
    pd.DataFrame(breakdown_rows).to_csv(BREAKDOWN_OUT, index=False)
    SUMMARY_OUT.write_text(
        build_summary(plan, grocery_list),
        encoding="utf-8",
    )
    summary = grocery_list.get("summary", {})
    print("Generator v1 Grocery List v1 demo audit written")
    print(f"summary={SUMMARY_OUT}")
    print(f"items={ITEMS_OUT}")
    print(f"breakdown={BREAKDOWN_OUT}")
    print(
        "item_count="
        f"{summary.get('item_count', 0)}; "
        f"mapped={summary.get('mapped_item_count', 0)}; "
        f"fallback={summary.get('fallback_item_count', 0)}"
    )


def build_summary(plan: dict[str, Any], grocery_list: dict[str, Any]) -> str:
    plan_summary = plan.get("multi_day_summary", {})
    grocery_summary = grocery_list.get("summary", {})
    if not isinstance(plan_summary, dict):
        plan_summary = {}
    if not isinstance(grocery_summary, dict):
        grocery_summary = {}
    category_counts = grocery_summary.get("category_counts", {})
    top_categories = top_category_text(category_counts)
    warnings = grocery_list.get("warnings", [])
    usable = bool(grocery_summary.get("is_demo_usable", False))
    lines = [
        "Generator v1 Grocery List v1 demo audit",
        "",
        f"dataset_profile={V1_2_DEMO_FINAL_PROFILE}",
        f"selected_plan_days={plan_summary.get('actual_days_generated', len(plan.get('days', [])))}",
        f"requested_days={plan_summary.get('requested_days', 3)}",
        f"valid_day_count={plan_summary.get('valid_day_count', 0)}",
        f"accept_day_count={plan_summary.get('accept_day_count', 0)}",
        "",
        f"grocery_item_count={grocery_summary.get('item_count', 0)}",
        f"shopping_item_count={grocery_summary.get('shopping_item_count', 0)}",
        f"mapped_item_count={grocery_summary.get('mapped_item_count', 0)}",
        f"fallback_item_count={grocery_summary.get('fallback_item_count', 0)}",
        f"pantry_basic_count={grocery_summary.get('pantry_basic_count', 0)}",
        f"excluded_water_count={grocery_summary.get('excluded_water_count', 0)}",
        f"top_categories={top_categories}",
        f"warnings={'; '.join(str(item) for item in warnings) if warnings else 'none'}",
        f"demo_usable={'yes' if usable else 'no'}",
        "",
        "Scope notes:",
        "- deterministic grocery grouping from selected recipes and portion multipliers",
        "- no price estimation",
        "- no package-size optimization",
        "- no pantry inventory optimization",
    ]
    return "\n".join(lines) + "\n"


def top_category_text(category_counts: Any) -> str:
    if not isinstance(category_counts, dict) or not category_counts:
        return "none"
    pairs = sorted(
        ((str(key), int(value)) for key, value in category_counts.items()),
        key=lambda item: (-item[1], item[0]),
    )
    return "; ".join(f"{key}={value}" for key, value in pairs[:5])


if __name__ == "__main__":
    main()
