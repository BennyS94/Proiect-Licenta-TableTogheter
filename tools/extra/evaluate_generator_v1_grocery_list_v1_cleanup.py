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
from src.generator_v1.multi_day_selector import (  # noqa: E402
    MULTI_DAY_MODE_GLOBAL,
    generate_multi_day_plan,
)
from src.generator_v1.profile_loader import load_member_profile  # noqa: E402
from src.generator_v1.slot_candidates import build_slot_candidates  # noqa: E402
from src.generator_v1.target_builder import build_nutrition_target  # noqa: E402


PROFILE_PATH = ROOT / "profiles/member_profile_demo_v1.json"
AUDIT_DIR = ROOT / "data/recipesdb/audit"
SUMMARY_OUT = AUDIT_DIR / "generator_v1_grocery_list_v1_cleanup_summary.txt"
ITEMS_OUT = AUDIT_DIR / "generator_v1_grocery_list_v1_cleanup_items.csv"
SAMPLE_OUT = AUDIT_DIR / "generator_v1_grocery_list_v1_cleanup_sample.txt"


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
            "round_grams_for_display": True,
        },
    )
    item_rows = grocery_list_rows(grocery_list, include_pantry_basics=True)
    sample_lines = grocery_list_readable_lines(
        grocery_list,
        include_pantry_basics=True,
    )
    pd.DataFrame(item_rows).to_csv(ITEMS_OUT, index=False)
    SAMPLE_OUT.write_text("\n".join(sample_lines) + "\n", encoding="utf-8")
    SUMMARY_OUT.write_text(
        build_summary(plan, grocery_list, sample_lines),
        encoding="utf-8",
    )
    summary = grocery_list.get("summary", {})
    print("Generator v1 Grocery List v1 cleanup audit written")
    print(f"summary={SUMMARY_OUT}")
    print(f"items={ITEMS_OUT}")
    print(f"sample={SAMPLE_OUT}")
    print(
        "raw_item_count="
        f"{summary.get('raw_item_count', 0)}; "
        f"display_item_count={summary.get('display_item_count', 0)}; "
        f"alias_groups={summary.get('safe_alias_group_count', 0)}"
    )


def build_summary(
    plan: dict[str, Any],
    grocery_list: dict[str, Any],
    sample_lines: list[str],
) -> str:
    plan_summary = plan.get("multi_day_summary", {})
    grocery_summary = grocery_list.get("summary", {})
    if not isinstance(plan_summary, dict):
        plan_summary = {}
    if not isinstance(grocery_summary, dict):
        grocery_summary = {}
    warning_counts = grocery_summary.get("warning_counts", {})
    category_counts = grocery_summary.get("category_counts", {})
    alias_examples = alias_group_examples(grocery_list)
    lines = [
        "Generator v1 Grocery List v1 display cleanup audit",
        "",
        f"dataset_profile={V1_2_DEMO_FINAL_PROFILE}",
        f"selected_plan_days={plan_summary.get('actual_days_generated', len(plan.get('days', [])))}",
        f"requested_days={plan_summary.get('requested_days', 3)}",
        f"valid_day_count={plan_summary.get('valid_day_count', 0)}",
        f"accept_day_count={plan_summary.get('accept_day_count', 0)}",
        "",
        f"raw_item_count={grocery_summary.get('raw_item_count', 0)}",
        f"display_item_count={grocery_summary.get('display_item_count', 0)}",
        f"shopping_item_count={grocery_summary.get('shopping_item_count', 0)}",
        f"safe_alias_groups_applied={grocery_summary.get('safe_alias_group_count', 0)}",
        f"safe_alias_source_item_count={grocery_summary.get('safe_alias_source_item_count', 0)}",
        f"unclear_item_count={grocery_summary.get('unclear_item_count', 0)}",
        f"fallback_grouped_item_count={grocery_summary.get('fallback_item_count', 0)}",
        f"pantry_basic_count={grocery_summary.get('pantry_basic_count', 0)}",
        f"cooked_raw_ambiguity_count={grocery_summary.get('cooked_raw_ambiguity_count', 0)}",
        f"category_counts={format_counts(category_counts)}",
        f"warning_counts={format_counts(warning_counts)}",
        f"alias_examples={alias_examples or 'none'}",
        "",
        "Sample copy-friendly grocery output:",
        *sample_lines[:80],
        "",
        "Scope notes:",
        "- display cleanup only",
        "- exact grams remain in CSV/detail columns",
        "- no price estimation",
        "- no package-size optimization",
        "- no pantry inventory subtraction",
    ]
    return "\n".join(lines) + "\n"


def alias_group_examples(grocery_list: dict[str, Any]) -> str:
    examples: list[str] = []
    for item in grocery_list.get("display_items", []):
        if item.get("grouped_display_method") != "safe_display_alias":
            continue
        source_names = item.get("source_item_names", [])
        examples.append(
            f"{item.get('display_name_clean')} <= "
            + ", ".join(str(value) for value in source_names)
        )
    return "; ".join(examples[:8])


def format_counts(value: Any) -> str:
    if not isinstance(value, dict) or not value:
        return "none"
    pairs = sorted(
        ((str(key), int(item)) for key, item in value.items()),
        key=lambda item: (-item[1], item[0]),
    )
    return "; ".join(f"{key}={item}" for key, item in pairs)


if __name__ == "__main__":
    main()
