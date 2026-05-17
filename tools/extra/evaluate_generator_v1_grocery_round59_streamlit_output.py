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
    grocery_raw_item_rows,
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
SUMMARY_OUT = AUDIT_DIR / "generator_v1_grocery_round59_output_summary.txt"
ITEMS_OUT = AUDIT_DIR / "generator_v1_grocery_round59_items.csv"
SAMPLE_OUT = AUDIT_DIR / "generator_v1_grocery_round59_sample.txt"


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
            "enable_cooked_to_raw_conversion": True,
            "exclude_water": True,
            "round_grams_for_display": True,
        },
    )
    item_rows = grocery_list_rows(grocery_list, include_pantry_basics=True)
    raw_rows = grocery_raw_item_rows(grocery_list)
    sample_lines = grocery_list_readable_lines(
        grocery_list,
        include_pantry_basics=True,
        include_purchase_suggestions=True,
    )
    pd.DataFrame(item_rows).to_csv(ITEMS_OUT, index=False)
    SAMPLE_OUT.write_text("\n".join(sample_lines) + "\n", encoding="utf-8")
    SUMMARY_OUT.write_text(
        build_summary(plan, grocery_list, item_rows, raw_rows, sample_lines),
        encoding="utf-8",
    )
    checks = round59_checks(item_rows, raw_rows, sample_lines)
    egg_alias_fixture = round59_egg_alias_fixture_check()
    print("Generator v1 Grocery Round59 output audit written")
    print(f"summary={SUMMARY_OUT}")
    print(f"items={ITEMS_OUT}")
    print(f"sample={SAMPLE_OUT}")
    print(
        "checks="
        f"eggs_grouped={checks['eggs_grouped']}; "
        f"eggs_purchase_display={checks['eggs_purchase_display']}; "
        f"no_raw_egg_split_in_sample={checks['no_raw_egg_split_in_sample']}; "
        f"raw_debug_has_egg_source_rows={checks['raw_debug_has_egg_source_rows']}; "
        f"raw_and_hard_boiled_eggs_grouped="
        f"{egg_alias_fixture['raw_and_hard_boiled_eggs_grouped']}"
    )


def build_summary(
    plan: dict[str, Any],
    grocery_list: dict[str, Any],
    item_rows: list[dict[str, Any]],
    raw_rows: list[dict[str, Any]],
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
    checks = round59_checks(item_rows, raw_rows, sample_lines)
    egg_alias_fixture = round59_egg_alias_fixture_check()
    egg_rows = [
        row for row in item_rows
        if str(row.get("display_name_clean", "")).lower() == "eggs"
    ]
    lines = [
        "Generator v1 Grocery Round59 Streamlit output audit",
        "",
        f"dataset_profile={V1_2_DEMO_FINAL_PROFILE}",
        f"selected_plan_days={plan_summary.get('actual_days_generated', len(plan.get('days', [])))}",
        f"valid_day_count={plan_summary.get('valid_day_count', 0)}",
        f"accept_day_count={plan_summary.get('accept_day_count', 0)}",
        "",
        f"display_item_count={grocery_summary.get('display_item_count', 0)}",
        f"raw_item_count={grocery_summary.get('raw_item_count', 0)}",
        f"shopping_item_count={grocery_summary.get('shopping_item_count', 0)}",
        f"items_with_purchase_suggestions={purchase_summary.get('items_with_purchase_suggestions', 0)}",
        f"cooked_to_raw_converted_items={purchase_summary.get('cooked_to_raw_converted_item_count', 0)}",
        f"raw_debug_row_count={len(raw_rows)}",
        "",
        f"eggs_display_item_count={len(egg_rows)}",
        f"eggs_grouped={checks['eggs_grouped']}",
        f"eggs_purchase_display={checks['eggs_purchase_display']}",
        f"no_raw_egg_split_in_sample={checks['no_raw_egg_split_in_sample']}",
        f"raw_debug_has_egg_source_rows={checks['raw_debug_has_egg_source_rows']}",
        "raw_and_hard_boiled_egg_fixture_grouped="
        f"{egg_alias_fixture['raw_and_hard_boiled_eggs_grouped']}",
        "raw_and_hard_boiled_egg_fixture_purchase="
        f"{egg_alias_fixture['purchase_display']}",
        f"unclear_items_warning_backed={checks['unclear_items_warning_backed']}",
        "",
        "Egg display rows:",
        *(format_egg_rows(egg_rows) or ["none"]),
        "",
        "Egg raw + hard-boiled fixture rows:",
        *(egg_alias_fixture["formatted_rows"] or ["none"]),
        "",
        "Sample copy-friendly grocery output:",
        *sample_lines[:90],
    ]
    return "\n".join(lines) + "\n"


def round59_checks(
    item_rows: list[dict[str, Any]],
    raw_rows: list[dict[str, Any]],
    sample_lines: list[str],
) -> dict[str, bool]:
    egg_rows = [
        row for row in item_rows
        if str(row.get("display_name_clean", "")).lower() == "eggs"
    ]
    sample_text = "\n".join(sample_lines).lower()
    raw_egg_rows = [
        row for row in raw_rows
        if "egg" in _row_text(row)
    ]
    unclear_rows = [
        row for row in item_rows
        if str(row.get("display_name_clean", "")).lower() in {"pressed", "unknown item"}
    ]
    return {
        "eggs_grouped": len(egg_rows) == 1,
        "eggs_purchase_display": bool(egg_rows and egg_rows[0].get("purchase_display")),
        "no_raw_egg_split_in_sample": "egg, raw" not in sample_text
        and "egg, hard-boiled" not in sample_text
        and "hard-boiled eggs:" not in sample_text,
        "raw_debug_has_egg_source_rows": len(raw_egg_rows) >= 1,
        "unclear_items_warning_backed": all(
            "unclear_grocery_item_name" in str(row.get("warnings", ""))
            for row in unclear_rows
        ),
    }


def format_egg_rows(rows: list[dict[str, Any]]) -> list[str]:
    result = []
    for row in rows:
        result.append(
            (
                f"Eggs: total_grams={row.get('total_grams')}, "
                f"needed={row.get('needed_grams_display')}, "
                f"purchase={row.get('purchase_display')}, "
                f"source_items={row.get('source_item_names')}, "
                f"source_raw_item_ids={row.get('source_raw_item_ids')}"
            )
        )
    return result


def round59_egg_alias_fixture_check() -> dict[str, Any]:
    plan = {
        "selected_meals": [
            {
                "slot": "snack",
                "recipe_id": "round59_raw_egg_fixture",
                "display_name": "Raw egg fixture",
                "portion_multiplier": 1.0,
            },
            {
                "slot": "snack",
                "recipe_id": "round59_hard_boiled_egg_fixture",
                "display_name": "Hard-boiled egg fixture",
                "portion_multiplier": 1.0,
            },
        ]
    }
    ingredients = pd.DataFrame(
        [
            {
                "recipe_id": "round59_raw_egg_fixture",
                "quantity_grams_estimated": 210.0,
                "mapped_food_id": "food_egg_raw",
                "mapped_food_canonical_name": "egg_raw",
                "ingredient_name_normalized": "raw egg",
                "ingredient_raw_text": "raw egg",
            },
            {
                "recipe_id": "round59_hard_boiled_egg_fixture",
                "quantity_grams_estimated": 125.0,
                "mapped_food_id": "food_egg_hard_boiled",
                "mapped_food_canonical_name": "egg_hard_boiled",
                "ingredient_name_normalized": "hard boiled egg",
                "ingredient_raw_text": "hard-boiled egg",
            },
        ]
    )
    grocery_list = build_grocery_list(
        plan,
        ingredients,
        fooddb_df=None,
        config={
            "include_pantry_basics": True,
            "include_purchase_suggestions": True,
            "enable_cooked_to_raw_conversion": True,
            "exclude_water": True,
            "round_grams_for_display": True,
        },
    )
    item_rows = grocery_list_rows(grocery_list, include_pantry_basics=True)
    egg_rows = [
        row for row in item_rows
        if str(row.get("display_name_clean", "")).lower() == "eggs"
    ]
    purchase_display = str(egg_rows[0].get("purchase_display", "")) if egg_rows else ""
    return {
        "raw_and_hard_boiled_eggs_grouped": len(egg_rows) == 1
        and "raw" in str(egg_rows[0].get("source_item_names", "")).lower()
        and "hard" in str(egg_rows[0].get("source_item_names", "")).lower()
        and str(egg_rows[0].get("total_grams", "")) in {"335.0", "335"},
        "purchase_display": purchase_display,
        "formatted_rows": format_egg_rows(egg_rows),
    }


def _row_text(row: dict[str, Any]) -> str:
    return " ".join(str(value).lower() for value in row.values())


if __name__ == "__main__":
    main()
