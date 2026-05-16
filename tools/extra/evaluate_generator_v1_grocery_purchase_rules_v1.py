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
SUMMARY_OUT = AUDIT_DIR / "generator_v1_grocery_purchase_rules_v1_summary.txt"
ITEMS_OUT = AUDIT_DIR / "generator_v1_grocery_purchase_rules_v1_items.csv"
SAMPLE_OUT = AUDIT_DIR / "generator_v1_grocery_purchase_rules_v1_sample.txt"


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
            "include_purchase_suggestions": True,
            "exclude_water": True,
            "round_grams_for_display": True,
        },
    )
    item_rows = grocery_list_rows(grocery_list, include_pantry_basics=True)
    sample_lines = grocery_list_readable_lines(
        grocery_list,
        include_pantry_basics=True,
        include_purchase_suggestions=True,
    )
    pd.DataFrame(item_rows).to_csv(ITEMS_OUT, index=False)
    SAMPLE_OUT.write_text("\n".join(sample_lines) + "\n", encoding="utf-8")
    SUMMARY_OUT.write_text(
        build_summary(plan, grocery_list, sample_lines),
        encoding="utf-8",
    )
    summary = grocery_list.get("summary", {})
    purchase_summary = summary.get("purchase_summary", {}) if isinstance(summary, dict) else {}
    if not isinstance(purchase_summary, dict):
        purchase_summary = {}
    print("Generator v1 Grocery Purchase Rules v1 audit written")
    print(f"summary={SUMMARY_OUT}")
    print(f"items={ITEMS_OUT}")
    print(f"sample={SAMPLE_OUT}")
    print(
        "grocery_items="
        f"{summary.get('display_item_count', 0) if isinstance(summary, dict) else 0}; "
        "purchase_suggestions="
        f"{purchase_summary.get('items_with_purchase_suggestions', 0)}; "
        f"fallback_grams={purchase_summary.get('fallback_grams_only_count', 0)}"
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
    purchase_summary = grocery_summary.get("purchase_summary", {})
    if not isinstance(purchase_summary, dict):
        purchase_summary = {}
    confidence_counts = purchase_summary.get("purchase_confidence_counts", {})
    warning_counts = purchase_summary.get("purchase_warning_counts", {})
    example_rows = purchase_examples(grocery_list)
    lines = [
        "Generator v1 Grocery Purchase Rules v1 audit",
        "",
        f"dataset_profile={V1_2_DEMO_FINAL_PROFILE}",
        f"selected_plan_days={plan_summary.get('actual_days_generated', len(plan.get('days', [])))}",
        f"requested_days={plan_summary.get('requested_days', 3)}",
        f"valid_day_count={plan_summary.get('valid_day_count', 0)}",
        f"accept_day_count={plan_summary.get('accept_day_count', 0)}",
        "",
        f"grocery_item_count={grocery_summary.get('display_item_count', 0)}",
        f"shopping_item_count={grocery_summary.get('shopping_item_count', 0)}",
        f"items_with_purchase_suggestions={purchase_summary.get('items_with_purchase_suggestions', 0)}",
        f"purchase_confidence_counts={format_counts(confidence_counts)}",
        f"pantry_basic_count={purchase_summary.get('pantry_basic_count', 0)}",
        f"package_rounded_items_count={purchase_summary.get('package_rounded_items_count', 0)}",
        f"piece_rounded_items_count={purchase_summary.get('piece_rounded_items_count', 0)}",
        f"fallback_grams_only_count={purchase_summary.get('fallback_grams_only_count', 0)}",
        f"cooked_raw_ambiguity_count={purchase_summary.get('cooked_raw_ambiguity_count', 0)}",
        f"purchase_warning_counts={format_counts(warning_counts)}",
        f"rules_loaded_count={purchase_summary.get('rules_loaded_count', 0)}",
        "",
        "Purchase examples:",
        *(example_rows or ["none"]),
        "",
        "Sample copy-friendly grocery output:",
        *sample_lines[:90],
        "",
        "Scope notes:",
        "- purchase suggestions only",
        "- no price estimation",
        "- no store or brand logic",
        "- no real pantry inventory subtraction",
        "- no advanced grocery optimization",
        "- cooked-to-raw conversion is not implemented",
    ]
    return "\n".join(lines) + "\n"


def purchase_examples(grocery_list: dict[str, Any]) -> list[str]:
    preferred = [
        "Eggs",
        "Onions",
        "Pasta (dry)",
        "Rice (raw)",
        "Greek yogurt",
        "Semi-skimmed milk",
        "Chicken breast",
        "Olive oil",
    ]
    by_name = {
        str(item.get("display_name_clean")): item
        for item in grocery_list.get("display_items", [])
    }
    rows: list[str] = []
    for name in preferred:
        item = by_name.get(name)
        if not item:
            continue
        purchase_display = str(item.get("purchase_display") or "")
        if purchase_display.startswith("check pantry") or purchase_display.startswith("review item"):
            rows.append(f"{name}: {purchase_display}")
            continue
        rows.append(f"{name}: need {item.get('needed_grams_display')}; buy {purchase_display}")
    return rows


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
