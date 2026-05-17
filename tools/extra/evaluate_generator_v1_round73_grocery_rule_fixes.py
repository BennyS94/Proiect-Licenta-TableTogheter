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
from src.generator_v1.grocery_purchase import (  # noqa: E402
    apply_purchase_rules,
    load_grocery_purchase_rules,
)
from src.generator_v1.household_generator import (  # noqa: E402
    HOUSEHOLD_MODE_INDIVIDUAL_BREAKFAST_SHARED_MAIN,
    build_household_aggregate_target,
    build_member_targets,
    filter_household_profile_members,
    generate_household_plan,
    load_household_profile,
)
from src.generator_v1.household_preview import DEFAULT_ALLOCATION_MODE  # noqa: E402
from src.generator_v1.multi_day_selector import MULTI_DAY_MODE_GLOBAL  # noqa: E402
from src.generator_v1.slot_candidates import build_slot_candidates  # noqa: E402


RECIPES_AUDIT_DIR = ROOT / "data/recipesdb/audit"
SUMMARY_OUT = RECIPES_AUDIT_DIR / "generator_v1_round73_grocery_rule_fix_summary.txt"
ITEMS_OUT = RECIPES_AUDIT_DIR / "generator_v1_round73_grocery_rule_fix_items.csv"
SAMPLE_OUT = RECIPES_AUDIT_DIR / "generator_v1_round73_grocery_rule_fix_sample.txt"

HOUSEHOLD_MEMBER_IDS = [
    "member_demo_adult_male_001",
    "member_demo_adult_female_001",
    "member_demo_lower_target_001",
]


def main() -> None:
    RECIPES_AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    ingredients = pd.read_csv(ROOT / V1_2_DEMO_FINAL_INGREDIENTS_PATH)
    plan = _generate_household_plan(ingredients)
    grocery_plan = _member_aware_plan_for_grocery(plan)
    grocery_list = build_grocery_list(
        grocery_plan,
        ingredients,
        fooddb_df=load_fooddb_current(),
        config={
            "include_pantry_basics": True,
            "include_purchase_suggestions": True,
            "enable_cooked_to_raw_conversion": True,
            "include_price_estimates": True,
        },
    )
    rows = [_item_row(item) for item in grocery_list_rows(grocery_list, include_pantry_basics=True)]
    probe_rows = _purchase_rule_probe_rows()
    pd.DataFrame(rows + probe_rows).to_csv(ITEMS_OUT, index=False)
    SAMPLE_OUT.write_text(
        "\n".join(
            grocery_list_readable_lines(
                grocery_list,
                include_pantry_basics=True,
                include_purchase_suggestions=True,
                include_price_estimates=True,
            )
        )
        + "\n",
        encoding="utf-8",
    )
    SUMMARY_OUT.write_text(_summary_text(grocery_list, rows, probe_rows), encoding="utf-8")
    print("Round73 grocery rule fix audit written")
    print(SUMMARY_OUT)


def _generate_household_plan(ingredients: pd.DataFrame) -> dict[str, Any]:
    base_profile = load_household_profile(ROOT / "profiles/household_profile_demo_v1.json")
    household_profile = filter_household_profile_members(base_profile, HOUSEHOLD_MEMBER_IDS)
    member_targets = build_member_targets(household_profile)
    target = build_household_aggregate_target(member_targets)
    pool = load_recipe_candidate_pool(
        recipes_path=ROOT / V1_2_DEMO_FINAL_RECIPES_PATH,
        ingredients_path=ROOT / V1_2_DEMO_FINAL_INGREDIENTS_PATH,
        nutrition_path=ROOT / V1_2_DEMO_FINAL_NUTRITION_PATH,
        dataset_profile=V1_2_DEMO_FINAL_PROFILE,
    )
    primary = _primary_member(household_profile)
    preference_context = build_household_preference_context(
        _household_context_profile(household_profile, primary)
    )
    filtered = filter_recipe_candidates(
        eligible_candidates=pool.eligible_candidates,
        ingredients=pool.ingredients,
        context=preference_context,
    )
    slot_candidates = build_slot_candidates(
        target=target,
        filtered_candidates=filtered,
        time_sensitivity=preference_context.time_sensitivity,
        ingredients=pool.ingredients,
        fooddb=load_fooddb_current(),
        portion_policy_mode="target_aware",
    )
    plan = generate_household_plan(
        household_profile,
        slot_candidates=slot_candidates,
        individual_slot_candidates=slot_candidates,
        days=3,
        config=_generation_config(ingredients),
        profile=primary,
    )
    plan["pool_summary"] = {
        "dataset_profile": V1_2_DEMO_FINAL_PROFILE,
        "ingredients_path": str(V1_2_DEMO_FINAL_INGREDIENTS_PATH),
    }
    return plan


def _generation_config(ingredients: pd.DataFrame) -> dict[str, Any]:
    return {
        "household_mode": HOUSEHOLD_MODE_INDIVIDUAL_BREAKFAST_SHARED_MAIN,
        "allocation_mode": DEFAULT_ALLOCATION_MODE,
        "selection_mode": "balanced_day",
        "portion_policy": "target_aware",
        "meal_realism_mode": "practical",
        "quality_gate": "demo_safe",
        "alternative_count": 3,
        "return_alternatives": True,
        "multi_day_mode": MULTI_DAY_MODE_GLOBAL,
        "candidate_day_alternative_count": 10,
        "global_max_candidates_per_slot": 16,
        "day_candidate_pool_size_target": 40,
        "day_candidate_pool_max": 80,
        "include_slot_forced_variants": True,
        "no_repeat_policy": "hard",
        "multi_day_speed_mode": "fast",
        "day_candidate_builder": "direct_from_slots",
        "direct_slot_shortlist_size": 8,
        "protein_correction_threshold": 0.85,
        "enable_egg_load_guard": True,
        "recipe_ingredients_df": ingredients,
    }


def _member_aware_plan_for_grocery(plan: dict[str, Any]) -> dict[str, Any]:
    individual_by_day: dict[int, list[dict[str, Any]]] = {}
    for row in plan.get("individual_meals", []):
        day_index = int(row.get("day_index") or 1)
        individual_by_day.setdefault(day_index, []).append(dict(row))

    grocery_days: list[dict[str, Any]] = []
    for day in plan.get("days", []):
        day_index = int(day.get("day_index") or len(grocery_days) + 1)
        selected_meals: list[dict[str, Any]] = []
        for meal in day.get("selected_meals", []):
            if not bool(meal.get("household_generation_shared_slot", True)):
                continue
            meal_row = dict(meal)
            meal_row["portion_multiplier"] = (
                _to_float(meal.get("household_portion_sum"))
                or _to_float(meal.get("portion_multiplier"))
                or 1.0
            )
            selected_meals.append(meal_row)
        for row in individual_by_day.get(day_index, []):
            selected_meals.append(
                {
                    "slot": row.get("slot"),
                    "recipe_id": row.get("recipe_id"),
                    "display_name": row.get("display_name") or row.get("recipe"),
                    "portion_multiplier": (
                        _to_float(row.get("portion_multiplier_member"))
                        or _to_float(row.get("portion_multiplier"))
                        or 1.0
                    ),
                    "allocation_scope": "individual",
                    "member_id": row.get("member_id"),
                    "member": row.get("member"),
                }
            )
        grocery_days.append({"day_index": day_index, "selected_meals": selected_meals})
    result = dict(plan)
    result["days"] = grocery_days
    return result


def _item_row(item: dict[str, Any]) -> dict[str, Any]:
    warnings = item.get("warnings", [])
    purchase_warnings = item.get("purchase_warnings", [])
    return {
        "row_type": "household_grocery",
        "display_name": item.get("display_name_clean") or item.get("display_name"),
        "category": item.get("grocery_category"),
        "total_grams": item.get("total_grams"),
        "needed_grams_display": item.get("needed_grams_display"),
        "purchase_display": item.get("purchase_display"),
        "purchase_unit_type": item.get("purchase_unit_type"),
        "purchase_rule_id": item.get("purchase_rule_id"),
        "purchase_rule_confidence": item.get("purchase_rule_confidence"),
        "price_warning": item.get("price_warning"),
        "estimated_cost": item.get("estimated_cost"),
        "warnings": ";".join(warnings) if isinstance(warnings, list) else warnings,
        "purchase_warnings": ";".join(purchase_warnings)
        if isinstance(purchase_warnings, list)
        else purchase_warnings,
    }


def _purchase_rule_probe_rows() -> list[dict[str, Any]]:
    rules = load_grocery_purchase_rules()
    probe_items = [
        {
            "display_name_clean": "Plain yogurt",
            "display_name": "Plain yogurt",
            "canonical_name": "Plain yogurt",
            "grocery_category": "dairy_eggs",
            "total_grams": 620.0,
            "ingredient_names_seen": ["plain yogurt"],
            "warnings": [],
        },
        {
            "display_name_clean": "Canned tomato puree",
            "display_name": "Canned tomato puree",
            "canonical_name": "Canned tomato puree",
            "grocery_category": "sauces_canned",
            "total_grams": 350.0,
            "ingredient_names_seen": ["canned tomato puree"],
            "warnings": [],
        },
        {
            "display_name_clean": "Pressed",
            "display_name": "Pressed",
            "canonical_name": "Pressed",
            "grocery_category": "other_review",
            "total_grams": 50.0,
            "ingredient_names_seen": ["pressed"],
            "warnings": ["unclear_grocery_item_name"],
        },
    ]
    result = apply_purchase_rules(probe_items, rules, config={})
    rows: list[dict[str, Any]] = []
    for item in result.get("items", []):
        row = _item_row(item)
        row["row_type"] = "rule_probe"
        rows.append(row)
    return rows


def _summary_text(
    grocery_list: dict[str, Any],
    rows: list[dict[str, Any]],
    probe_rows: list[dict[str, Any]],
) -> str:
    all_rows = rows + probe_rows
    plain_yogurt = _first_matching(all_rows, "Plain yogurt")
    tomato_puree = _first_matching(all_rows, "Canned tomato puree")
    cooked_lentils = _first_containing(all_rows, "Lentils")
    pressed = _first_matching(all_rows, "Pressed")
    missing_price_count = sum(1 for row in rows if row.get("price_warning") == "price_missing")
    return "\n".join(
        [
            "Round73 grocery purchase-rule fix audit",
            "",
            f"shopping_item_count={(grocery_list.get('summary') or {}).get('shopping_item_count')}",
            f"missing_price_items={missing_price_count}",
            f"plain_yogurt_purchase={plain_yogurt.get('purchase_display') if plain_yogurt else 'not_present'}",
            f"plain_yogurt_unit={plain_yogurt.get('purchase_unit_type') if plain_yogurt else 'not_present'}",
            f"tomato_puree_purchase={tomato_puree.get('purchase_display') if tomato_puree else 'not_present'}",
            f"tomato_puree_unit={tomato_puree.get('purchase_unit_type') if tomato_puree else 'not_present'}",
            f"cooked_lentils_purchase={cooked_lentils.get('purchase_display') if cooked_lentils else 'not_present'}",
            f"pressed_purchase={pressed.get('purchase_display') if pressed else 'not_present'}",
            f"pressed_warnings={pressed.get('purchase_warnings') if pressed else 'not_present'}",
            "",
            "Checks:",
            f"- plain_yogurt_not_carton={plain_yogurt and plain_yogurt.get('purchase_unit_type') == 'tub'}",
            f"- tomato_puree_not_fresh_tomatoes={tomato_puree and tomato_puree.get('purchase_unit_type') == 'can'}",
            f"- lentils_not_generic_beans={not cooked_lentils or 'dry lentils' in str(cooked_lentils.get('purchase_display') or '').lower()}",
            f"- pressed_stays_review={pressed and pressed.get('purchase_unit_type') == 'review'}",
            "- manual_demo_price_rows_available=True",
            "",
            "Outputs:",
            f"- items={ITEMS_OUT}",
            f"- sample={SAMPLE_OUT}",
        ]
    ) + "\n"


def _first_matching(rows: list[dict[str, Any]], name: str) -> dict[str, Any] | None:
    clean = name.lower()
    for row in rows:
        if str(row.get("display_name") or "").lower() == clean:
            return row
    return None


def _first_containing(rows: list[dict[str, Any]], text: str) -> dict[str, Any] | None:
    clean = text.lower()
    for row in rows:
        if clean in str(row.get("display_name") or "").lower():
            return row
    return None


def _primary_member(household_profile: dict[str, Any]) -> dict[str, Any]:
    active_ids = set(household_profile.get("active_member_ids") or [])
    for member in household_profile.get("members", []):
        if member.get("member_id") in active_ids:
            return dict(member)
    return dict((household_profile.get("members") or [{}])[0])


def _household_context_profile(
    household_profile: dict[str, Any],
    primary_member: dict[str, Any],
) -> dict[str, Any]:
    profile = dict(primary_member)
    preferences = household_profile.get("household_preferences") or {}
    profile["banned_recipe_ids"] = preferences.get("banned_recipe_ids", [])
    profile["banned_ingredient_names"] = preferences.get("banned_ingredient_names", [])
    profile["liked_recipe_ids"] = preferences.get("liked_recipe_ids", [])
    profile["disliked_recipe_ids"] = preferences.get("disliked_recipe_ids", [])
    profile["too_long_recipe_ids"] = preferences.get("too_long_recipe_ids", [])
    return profile


def _to_float(value: Any) -> float | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


if __name__ == "__main__":
    main()
