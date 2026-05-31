from __future__ import annotations

import json
import re
import sys
from datetime import date
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
    grocery_list_rows,
    grocery_raw_item_rows,
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
GROCERY_AUDIT_DIR = ROOT / "data/grocery/audit"
GROCERY_DRAFT_DIR = ROOT / "data/grocery/draft"

SUMMARY_OUT = RECIPES_AUDIT_DIR / "generator_v1_round72_household_grocery_summary.txt"
MEALS_OUT = RECIPES_AUDIT_DIR / "generator_v1_round72_household_grocery_meals.csv"
ITEMS_OUT = RECIPES_AUDIT_DIR / "generator_v1_round72_household_grocery_items.csv"
LOAD_OUT = RECIPES_AUDIT_DIR / "generator_v1_round72_household_ingredient_load.csv"
EGG_OUT = RECIPES_AUDIT_DIR / "generator_v1_round72_household_egg_load_audit.csv"
PURCHASE_ISSUES_OUT = RECIPES_AUDIT_DIR / "generator_v1_round72_household_purchase_rule_issues.csv"
MISSING_PRICES_OUT = GROCERY_AUDIT_DIR / "grocery_price_round72_missing_prices.csv"
MISSING_TEMPLATE_OUT = GROCERY_DRAFT_DIR / "grocery_product_catalog_v1_price_research_round72_missing_template.csv"
CHAT_LIST_OUT = GROCERY_AUDIT_DIR / "grocery_price_round72_missing_prices_chat_list.txt"
RECOMMENDATIONS_OUT = RECIPES_AUDIT_DIR / "generator_v1_round72_recommendations.txt"

HOUSEHOLD_MEMBER_IDS = [
    "member_demo_adult_male_001",
    "member_demo_adult_female_001",
    "member_demo_lower_target_001",
]
EGG_GRAMS_PER_UNIT = 50.0


def main() -> None:
    RECIPES_AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    GROCERY_AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    GROCERY_DRAFT_DIR.mkdir(parents=True, exist_ok=True)

    recipes = pd.read_csv(ROOT / V1_2_DEMO_FINAL_RECIPES_PATH)
    ingredients = pd.read_csv(ROOT / V1_2_DEMO_FINAL_INGREDIENTS_PATH)
    fooddb = load_fooddb_current()
    plan = _generate_household_plan(HOUSEHOLD_MEMBER_IDS)
    grocery_plan = _member_aware_plan_for_grocery(plan)
    grocery_list = build_grocery_list(
        grocery_plan,
        ingredients,
        fooddb_df=fooddb,
        config={
            "include_pantry_basics": True,
            "include_purchase_suggestions": True,
            "enable_cooked_to_raw_conversion": True,
            "include_price_estimates": True,
            "exclude_water": True,
        },
    )

    meals = _meal_rows(plan)
    item_rows = _grocery_item_rows(grocery_list)
    egg_rows = _egg_load_rows(plan, ingredients)
    load_rows = _ingredient_load_rows(item_rows, plan, egg_rows)
    purchase_issue_rows = _purchase_rule_issue_rows(item_rows)
    missing_price_rows = _missing_price_rows(item_rows)
    template_rows = _missing_price_template_rows(missing_price_rows)

    pd.DataFrame(meals).to_csv(MEALS_OUT, index=False)
    pd.DataFrame(item_rows).to_csv(ITEMS_OUT, index=False)
    pd.DataFrame(load_rows).to_csv(LOAD_OUT, index=False)
    pd.DataFrame(egg_rows).to_csv(EGG_OUT, index=False)
    pd.DataFrame(purchase_issue_rows).to_csv(PURCHASE_ISSUES_OUT, index=False)
    pd.DataFrame(missing_price_rows).to_csv(MISSING_PRICES_OUT, index=False)
    pd.DataFrame(template_rows).to_csv(MISSING_TEMPLATE_OUT, index=False)
    CHAT_LIST_OUT.write_text(_chat_ready_missing_price_list(template_rows), encoding="utf-8")
    RECOMMENDATIONS_OUT.write_text(
        _recommendations_text(egg_rows, load_rows, purchase_issue_rows, missing_price_rows),
        encoding="utf-8",
    )
    SUMMARY_OUT.write_text(
        _summary_text(plan, grocery_list, egg_rows, load_rows, purchase_issue_rows, missing_price_rows),
        encoding="utf-8",
    )

    egg_summary = _egg_summary(plan, egg_rows)
    print("Round72 household grocery realism and missing price audit written")
    print(f"summary={SUMMARY_OUT}")
    print(f"items={ITEMS_OUT}")
    print(f"egg_audit={EGG_OUT}")
    print(f"missing_prices={MISSING_PRICES_OUT}")
    print(f"chat_list={CHAT_LIST_OUT}")
    print(
        "eggs: "
        f"total={egg_summary['total_egg_count']:.1f}; "
        f"direct={egg_summary['direct_egg_count']:.1f}; "
        f"embedded={egg_summary['embedded_egg_count']:.1f}; "
        f"direct_per_person_day={egg_summary['direct_eggs_per_person_per_day']:.2f}"
    )
    print(f"missing_price_items={len(missing_price_rows)}")


def _generate_household_plan(member_ids: list[str]) -> dict[str, Any]:
    base_profile = load_household_profile(ROOT / "profiles/household_profile_demo_v1.json")
    household_profile = filter_household_profile_members(base_profile, member_ids)
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
        config=_generation_config(),
        profile=primary,
    )
    plan["selected_household_member_ids"] = list(member_ids)
    plan["selected_household_member_names"] = [
        str(member.get("display_name") or member.get("member_id"))
        for member in household_profile.get("members", [])
    ]
    plan["pool_summary"] = {
        "dataset_profile": V1_2_DEMO_FINAL_PROFILE,
        "ingredients_path": str(V1_2_DEMO_FINAL_INGREDIENTS_PATH),
    }
    return plan


def _generation_config() -> dict[str, Any]:
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
    }


def _member_aware_plan_for_grocery(plan: dict[str, Any]) -> dict[str, Any]:
    individual_by_day: dict[int, list[dict[str, Any]]] = {}
    for row in plan.get("individual_meals", []):
        day_index = int(row.get("day_index") or 1)
        individual_by_day.setdefault(day_index, []).append(dict(row))

    grocery_days: list[dict[str, Any]] = []
    for day in plan.get("days", []):
        if not isinstance(day, dict):
            continue
        day_index = int(day.get("day_index") or len(grocery_days) + 1)
        selected_meals: list[dict[str, Any]] = []
        for meal in day.get("selected_meals", []):
            if not isinstance(meal, dict):
                continue
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


def _meal_rows(plan: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in plan.get("allocations", []):
        rows.append(
            {
                "day": row.get("day_index"),
                "slot": row.get("slot"),
                "member_id": row.get("member_id"),
                "member": row.get("member"),
                "recipe_id": row.get("recipe_id"),
                "recipe_name": row.get("display_name") or row.get("recipe"),
                "meal_scope": row.get("allocation_scope", "shared"),
                "portion_multiplier": row.get("portion_multiplier_member"),
                "grams_estimated": row.get("grams_estimated"),
                "kcal": row.get("kcal"),
                "protein_g": row.get("protein_g"),
                "carbs_g": row.get("carbs_g"),
                "fat_g": row.get("fat_g"),
                "warnings": row.get("household_portion_fit_warning"),
            }
        )
    return rows


def _grocery_item_rows(grocery_list: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in grocery_list_rows(grocery_list, include_pantry_basics=True):
        raw_display_name = _display_name(item)
        friendly_name = _friendly_display_name(raw_display_name)
        warnings = _join_values(item.get("warnings"))
        purchase_warnings = _join_values(item.get("purchase_warnings"))
        rows.append(
            {
                "display_name": friendly_name,
                "raw_display_name": raw_display_name,
                "mapped_food_id": item.get("mapped_food_id"),
                "canonical_name": item.get("canonical_name"),
                "category": item.get("grocery_category"),
                "total_grams": _to_float(item.get("total_grams")) or 0.0,
                "display_grams": item.get("display_grams"),
                "needed_amount_display": item.get("needed_grams_display"),
                "purchase_display": item.get("purchase_display"),
                "purchase_unit_type": item.get("purchase_unit_type"),
                "purchase_quantity": item.get("purchase_quantity"),
                "purchase_amount_grams": item.get("purchase_amount_grams"),
                "estimated_leftover_grams": item.get("estimated_leftover_grams"),
                "price_warning": item.get("price_warning"),
                "estimated_cost": item.get("estimated_cost"),
                "estimated_cost_display": item.get("estimated_cost_display"),
                "currency": item.get("currency"),
                "price_confidence": item.get("price_confidence"),
                "price_source_name": item.get("price_source_name"),
                "source_recipes": _join_values(item.get("source_recipes")),
                "source_meals": _join_values(item.get("source_meals")),
                "source_item_names": _join_values(item.get("source_item_names")),
                "ingredient_names_seen": _join_values(item.get("ingredient_names_seen")),
                "grouped_display_method": item.get("grouped_display_method"),
                "grouping_method": item.get("grouping_method"),
                "warnings": warnings,
                "purchase_warnings": purchase_warnings,
                "is_pantry_basic": item.get("is_pantry_basic"),
            }
        )
    return rows


def _egg_load_rows(plan: dict[str, Any], ingredients: pd.DataFrame) -> list[dict[str, Any]]:
    egg_rows = ingredients[
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
    if egg_rows.empty:
        return []
    rows: list[dict[str, Any]] = []
    by_recipe = {
        str(recipe_id): frame.copy()
        for recipe_id, frame in egg_rows.groupby("recipe_id", dropna=False)
    }
    for allocation in plan.get("allocations", []):
        recipe_id = str(allocation.get("recipe_id") or "")
        recipe_egg_rows = by_recipe.get(recipe_id)
        if recipe_egg_rows is None or recipe_egg_rows.empty:
            continue
        multiplier = _to_float(allocation.get("portion_multiplier_member")) or 1.0
        recipe_name = str(allocation.get("display_name") or allocation.get("recipe") or "")
        source_type, notes = _classify_egg_source(recipe_name, str(allocation.get("slot") or ""))
        for _, ingredient in recipe_egg_rows.iterrows():
            base_grams = _to_float(ingredient.get("quantity_grams_estimated")) or 0.0
            grams = base_grams * multiplier
            rows.append(
                {
                    "recipe_id": recipe_id,
                    "recipe_name": recipe_name,
                    "member_id": allocation.get("member_id"),
                    "member_name": allocation.get("member"),
                    "day": allocation.get("day_index"),
                    "slot": allocation.get("slot"),
                    "portion_multiplier": multiplier,
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
    return rows


def _ingredient_load_rows(
    item_rows: list[dict[str, Any]],
    plan: dict[str, Any],
    egg_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    member_count = max(1, int((plan.get("household_summary") or {}).get("member_count") or 1))
    day_count = max(1, int((plan.get("household_summary") or {}).get("days_generated") or 1))
    rows: list[dict[str, Any]] = []
    total_grocery_grams = sum(_to_float(row.get("total_grams")) or 0.0 for row in item_rows)
    egg_summary = _egg_summary(plan, egg_rows)
    for row in item_rows:
        name = str(row.get("display_name") or "")
        total_grams = _to_float(row.get("total_grams")) or 0.0
        per_person_day = total_grams / member_count / day_count
        status, reason = _ingredient_load_status(
            row,
            per_person_day=per_person_day,
            share_of_total=(total_grams / total_grocery_grams if total_grocery_grams else 0.0),
            egg_summary=egg_summary if _normalise(name) == "eggs" else None,
        )
        rows.append(
            {
                "display_name": name,
                "category": row.get("category"),
                "total_grams": round(total_grams, 3),
                "grams_per_person_per_day": round(per_person_day, 3),
                "purchase_display": row.get("purchase_display"),
                "source_recipes": row.get("source_recipes"),
                "source_members": _source_members_for_item(row, plan),
                "source_days": _source_days_for_item(row, plan),
                "selected_meal_count": _source_meal_count(row),
                "share_of_total_grocery_grams": round(
                    total_grams / total_grocery_grams if total_grocery_grams else 0.0,
                    4,
                ),
                "ingredient_load_status": status,
                "reason": reason,
            }
        )
    return sorted(rows, key=lambda item: (-_to_float(item.get("total_grams")) or 0.0, item["display_name"]))


def _purchase_rule_issue_rows(item_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in item_rows:
        name = str(item.get("display_name") or "")
        purchase = str(item.get("purchase_display") or "")
        text = _normalise(" ".join([name, purchase, str(item.get("ingredient_names_seen") or "")]))
        issue = None
        fix = "no_action"
        priority = "low"
        notes = ""
        if "pressed" == _normalise(name):
            issue = "unclear_item_identity"
            fix = "move_to_review_items"
            priority = "medium"
            notes = "Unclear grocery display name; do not guess identity."
        elif "tomato puree" in text and ("tomatoes" in purchase.lower() or "medium" in purchase.lower()):
            issue = "tomato_puree_purchase_display_too_fresh"
            fix = "add_specific_purchase_rule"
            priority = "high"
            notes = "Tomato puree/sauce should not display as fresh tomatoes."
        elif "yogurt" in _normalise(name) and ("carton" in purchase.lower() or "milk" in purchase.lower()):
            issue = "yogurt_mapped_like_milk"
            fix = "add_specific_purchase_rule"
            priority = "high"
            notes = "Yogurt should prefer tub/grams, not milk carton/liter wording."
        elif "lentil" in text and "generic dry beans" in purchase.lower():
            issue = "lentil_wording_too_generic"
            fix = "add_specific_purchase_rule"
            priority = "medium"
            notes = "Cooked-to-raw wording should mention lentils/legumes specifically."
        elif "cooked_raw_purchase_ambiguity" in str(item.get("warnings") or ""):
            issue = "cooked_raw_purchase_ambiguity"
            fix = "keep_warning_only"
            priority = "medium"
            notes = "No automatic conversion should be applied without explicit rule."
        if issue:
            rows.append(
                {
                    "grocery_item_name": name,
                    "current_purchase_display": purchase,
                    "issue_type": issue,
                    "recommended_fix_type": fix,
                    "priority": priority,
                    "notes": notes,
                }
            )
    return rows


def _missing_price_rows(item_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in item_rows:
        price_warning = str(item.get("price_warning") or "").strip()
        estimated_cost = item.get("estimated_cost")
        if price_warning != "price_missing" and _to_float(estimated_cost) is not None:
            continue
        priority, reason = _price_priority(item)
        rows.append(
            {
                "display_name": item.get("display_name"),
                "mapped_food_id": item.get("mapped_food_id"),
                "canonical_name": item.get("canonical_name"),
                "shopping_category": item.get("category"),
                "needed_amount_display": item.get("needed_amount_display") or item.get("display_grams"),
                "purchase_display": item.get("purchase_display"),
                "total_grams": item.get("total_grams"),
                "price_warning": price_warning or "missing_estimated_cost",
                "purchase_unit_type": item.get("purchase_unit_type"),
                "purchase_quantity": item.get("purchase_quantity"),
                "purchase_amount_grams": item.get("purchase_amount_grams"),
                "source_recipes": item.get("source_recipes"),
                "priority": priority,
                "reason_for_priority": reason,
            }
        )
    return sorted(rows, key=lambda row: (_priority_sort(row.get("priority")), str(row.get("display_name"))))


def _missing_price_template_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    template: list[dict[str, Any]] = []
    for index, row in enumerate(rows, start=1):
        package_type, sold_by, price_method, package_size_g, package_size_ml, unit_count = _price_research_shape(row)
        display_name = str(row.get("display_name") or "")
        template.append(
            {
                "catalog_item_id": f"grocery_round72_missing_{index:03d}",
                "match_key": f"display_alias:{_normalise(display_name).replace(' ', '_')}",
                "mapped_food_id": row.get("mapped_food_id"),
                "display_name": display_name,
                "display_name_ro": "",
                "display_name_en": display_name,
                "shopping_category": row.get("shopping_category"),
                "needed_amount_display": row.get("needed_amount_display"),
                "purchase_display": row.get("purchase_display"),
                "package_type": package_type,
                "package_size_g": package_size_g,
                "package_size_ml": package_size_ml,
                "unit_count": unit_count,
                "sold_by": sold_by,
                "price_method": price_method,
                "suggested_source_query": _suggested_source_query(display_name, price_method),
                "priority": row.get("priority"),
                "reason_for_priority": row.get("reason_for_priority"),
                "decision_status": "pending_research",
                "reference_product_name": "",
                "store_name": "",
                "reference_price": "",
                "price_per_kg": "",
                "price_per_liter": "",
                "currency": "RON",
                "source_url": "",
                "captured_at": "",
                "confidence": "",
                "qc_notes": "",
            }
        )
    return template


def _summary_text(
    plan: dict[str, Any],
    grocery_list: dict[str, Any],
    egg_rows: list[dict[str, Any]],
    load_rows: list[dict[str, Any]],
    purchase_issue_rows: list[dict[str, Any]],
    missing_price_rows: list[dict[str, Any]],
) -> str:
    summary = plan.get("household_summary") or {}
    grocery_summary = grocery_list.get("summary") or {}
    pricing_summary = grocery_summary.get("pricing_summary") or {}
    egg_summary = _egg_summary(plan, egg_rows)
    load_counts = _count_by(load_rows, "ingredient_load_status")
    issue_counts = _count_by(purchase_issue_rows, "priority")
    missing_counts = _count_by(missing_price_rows, "priority")
    lines = [
        "Round72 household grocery realism and missing price audit",
        "",
        f"dataset_profile={V1_2_DEMO_FINAL_PROFILE}",
        f"household_mode={HOUSEHOLD_MODE_INDIVIDUAL_BREAKFAST_SHARED_MAIN}",
        f"allocation_mode={DEFAULT_ALLOCATION_MODE}",
        f"days={summary.get('days_generated')}",
        f"member_count={summary.get('member_count')}",
        f"household_quality_status={summary.get('household_quality_status')}",
        "",
        "Grocery summary",
        f"- shopping_item_count={grocery_summary.get('shopping_item_count')}",
        f"- display_item_count={grocery_summary.get('display_item_count')}",
        f"- priced_item_count={pricing_summary.get('priced_item_count')}",
        f"- unpriced_item_count={pricing_summary.get('unpriced_item_count')}",
        f"- estimated_total_cost={pricing_summary.get('total_estimated_cost')} {pricing_summary.get('currency', '')}",
        "",
        "Egg load",
        f"- total_egg_grams={egg_summary['total_egg_grams']:.1f}",
        f"- total_egg_count={egg_summary['total_egg_count']:.1f}",
        f"- total_eggs_per_person_per_day={egg_summary['total_eggs_per_person_per_day']:.2f}",
        f"- total_egg_status={egg_summary['total_egg_status']}",
        f"- direct_egg_grams={egg_summary['direct_egg_grams']:.1f}",
        f"- direct_egg_count={egg_summary['direct_egg_count']:.1f}",
        f"- direct_eggs_per_person_per_day={egg_summary['direct_eggs_per_person_per_day']:.2f}",
        f"- direct_egg_status={egg_summary['direct_egg_status']}",
        f"- embedded_egg_grams={egg_summary['embedded_egg_grams']:.1f}",
        f"- embedded_egg_count={egg_summary['embedded_egg_count']:.1f}",
        f"- uncertain_egg_grams={egg_summary['uncertain_egg_grams']:.1f}",
        "",
        "Ingredient load",
        f"- load_status_counts={json.dumps(load_counts, sort_keys=True)}",
        "",
        "Purchase rule issues",
        f"- purchase_issue_count={len(purchase_issue_rows)}",
        f"- purchase_issue_priority_counts={json.dumps(issue_counts, sort_keys=True)}",
        "",
        "Missing prices",
        f"- missing_price_item_count={len(missing_price_rows)}",
        f"- missing_price_priority_counts={json.dumps(missing_counts, sort_keys=True)}",
        f"- chat_ready_list={CHAT_LIST_OUT}",
        "",
        "Outputs",
        f"- meals={MEALS_OUT}",
        f"- items={ITEMS_OUT}",
        f"- ingredient_load={LOAD_OUT}",
        f"- egg_load={EGG_OUT}",
        f"- purchase_rule_issues={PURCHASE_ISSUES_OUT}",
        f"- missing_prices={MISSING_PRICES_OUT}",
        f"- missing_template={MISSING_TEMPLATE_OUT}",
        f"- recommendations={RECOMMENDATIONS_OUT}",
    ]
    return "\n".join(lines) + "\n"


def _chat_ready_missing_price_list(rows: list[dict[str, Any]]) -> str:
    lines = [
        "Please complete prices for these grocery catalog items.",
        "Rules:",
        "- Do not invent prices.",
        "- Use Romanian/European supermarket or official product sources.",
        "- RON currency.",
        "- Include source URL and captured date.",
        "- If uncertain, mark needs_review.",
        "- If no good source, keep_deferred.",
        "",
        "Items:",
    ]
    for index, row in enumerate(rows, start=1):
        lines.extend(
            [
                f"{index}. {row.get('display_name')}",
                f"   - needed/purchase: {row.get('needed_amount_display')} | {row.get('purchase_display')}",
                f"   - price method needed: {row.get('price_method')}",
                f"   - suggested query: {row.get('suggested_source_query')}",
                f"   - priority: {row.get('priority')} ({row.get('reason_for_priority')})",
            ]
        )
    lines.extend(
        [
            "",
            "CSV columns expected:",
            "- catalog_item_id",
            "- display_name",
            "- reference_product_name",
            "- store_name",
            "- reference_price",
            "- price_per_kg",
            "- price_per_liter",
            "- currency",
            "- source_url",
            "- captured_at",
            "- confidence",
            "- decision_status",
            "- qc_notes",
        ]
    )
    return "\n".join(lines) + "\n"


def _recommendations_text(
    egg_rows: list[dict[str, Any]],
    load_rows: list[dict[str, Any]],
    purchase_issue_rows: list[dict[str, Any]],
    missing_price_rows: list[dict[str, Any]],
) -> str:
    egg_summary = _egg_summary_from_rows(egg_rows)
    severe_load = [row for row in load_rows if row.get("ingredient_load_status") == "severe_warning"]
    warning_load = [row for row in load_rows if row.get("ingredient_load_status") == "warning"]
    lines = [
        "Round72 recommendations",
        "",
        "1. Generator / household realism",
        "- Add an egg-load soft penalty or warning in a later round; do not ban eggs globally.",
        "- Distinguish direct eggs from embedded eggs. Waffles/pancakes/batter eggs should be treated differently from egg toast/creamed eggs/omelets.",
        "- Do not force artificial variety just for variety; focus on excessive direct ingredient loads.",
        f"- Current audit direct eggs estimate: {egg_summary.get('direct_egg_count', 0.0):.1f} eggs.",
        "",
        "2. Grocery rule fixes",
        "- Add or audit tomato puree/sauce purchase rule so canned puree does not look like fresh tomatoes.",
        "- Keep yogurt tub/carton distinction explicit.",
        "- Improve lentils-specific cooked-to-raw wording instead of generic dry beans wording.",
        "- Keep unclear items such as Pressed in a review section until identity is fixed.",
        f"- Purchase-rule issue rows found: {len(purchase_issue_rows)}.",
        "",
        "3. Price coverage",
        "- Complete the Round72 missing price template manually.",
        "- Apply the filled prices through a later validation batch.",
        "- Do not invent prices and do not use pending/needs_review rows automatically.",
        f"- Missing price item rows found: {len(missing_price_rows)}.",
        "",
        "High-load rows needing attention",
    ]
    for row in [*severe_load, *warning_load][:12]:
        lines.append(
            "- "
            f"{row.get('display_name')}: {row.get('ingredient_load_status')} "
            f"({row.get('reason')})"
        )
    if not severe_load and not warning_load:
        lines.append("- none")
    return "\n".join(lines) + "\n"


def _egg_summary(plan: dict[str, Any], rows: list[dict[str, Any]]) -> dict[str, float | str]:
    summary = plan.get("household_summary") or {}
    member_count = max(1, int(summary.get("member_count") or 1))
    day_count = max(1, int(summary.get("days_generated") or 1))
    result = _egg_summary_from_rows(rows)
    result["selected_member_count"] = float(member_count)
    result["days"] = float(day_count)
    result["total_eggs_per_person_per_day"] = result["total_egg_count"] / member_count / day_count
    result["direct_eggs_per_person_per_day"] = result["direct_egg_count"] / member_count / day_count
    result["embedded_eggs_per_person_per_day"] = result["embedded_egg_count"] / member_count / day_count
    result["total_egg_status"] = _egg_status(float(result["total_eggs_per_person_per_day"]), total=True)
    result["direct_egg_status"] = _egg_status(float(result["direct_eggs_per_person_per_day"]), total=False)
    return result


def _egg_summary_from_rows(rows: list[dict[str, Any]]) -> dict[str, float]:
    total_grams = sum(_to_float(row.get("egg_grams_contribution")) or 0.0 for row in rows)
    direct_grams = sum(
        _to_float(row.get("egg_grams_contribution")) or 0.0
        for row in rows
        if row.get("egg_source_type") == "direct"
    )
    embedded_grams = sum(
        _to_float(row.get("egg_grams_contribution")) or 0.0
        for row in rows
        if row.get("egg_source_type") == "embedded"
    )
    uncertain_grams = sum(
        _to_float(row.get("egg_grams_contribution")) or 0.0
        for row in rows
        if row.get("egg_source_type") == "uncertain"
    )
    return {
        "total_egg_grams": total_grams,
        "total_egg_count": total_grams / EGG_GRAMS_PER_UNIT,
        "direct_egg_grams": direct_grams,
        "direct_egg_count": direct_grams / EGG_GRAMS_PER_UNIT,
        "embedded_egg_grams": embedded_grams,
        "embedded_egg_count": embedded_grams / EGG_GRAMS_PER_UNIT,
        "uncertain_egg_grams": uncertain_grams,
        "uncertain_egg_count": uncertain_grams / EGG_GRAMS_PER_UNIT,
    }


def _ingredient_load_status(
    item: dict[str, Any],
    *,
    per_person_day: float,
    share_of_total: float,
    egg_summary: dict[str, Any] | None,
) -> tuple[str, str]:
    name = _normalise(item.get("display_name"))
    warnings = _normalise(str(item.get("warnings") or "") + " " + str(item.get("purchase_warnings") or ""))
    purchase = _normalise(item.get("purchase_display"))
    if "unclear" in warnings or name == "pressed":
        return "unclear_review", "unclear grocery item identity"
    if egg_summary:
        status = str(egg_summary.get("direct_egg_status"))
        if status == "severe_warning":
            return "severe_warning", "direct eggs per person/day exceeds threshold"
        if status == "warning":
            return "warning", "direct eggs per person/day is high"
    if share_of_total > 0.35 and per_person_day > 300:
        return "warning", "one item dominates grocery grams; review whether repeated meals are realistic"
    if "review item" in purchase:
        return "unclear_review", "purchase suggestion says review item"
    if any(token in name for token in ["salt", "black pepper", "thyme"]) and per_person_day < 10:
        return "normal", "tiny pantry basic; normal reuse"
    return "normal", "normal reuse or realistic household quantity"


def _classify_egg_source(recipe_name: str, slot: str) -> tuple[str, str]:
    text = _normalise(f"{recipe_name} {slot}")
    direct_keywords = [
        "boiled egg",
        "hard boiled egg",
        "egg toast",
        "creamed egg",
        "omelet",
        "omelette",
        "scrambled egg",
        "egg bite",
        "frittata",
        "quiche",
        "eggs on toast",
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
    ]
    if any(keyword in text for keyword in direct_keywords):
        return "direct", "visible egg meal/protein"
    if any(keyword in text for keyword in embedded_keywords):
        return "embedded", "egg likely embedded in batter/binder"
    if "egg" in text:
        return "uncertain", "egg visible in recipe name but source type not certain"
    return "uncertain", "egg ingredient present but role not certain"


def _is_egg_ingredient(*values: object) -> bool:
    text = _normalise(" ".join(str(value or "") for value in values))
    return "egg" in text and "eggplant" not in text


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


def _price_priority(item: dict[str, Any]) -> tuple[str, str]:
    category = str(item.get("shopping_category") or item.get("category") or "")
    total_grams = _to_float(item.get("total_grams")) or 0.0
    name = _normalise(item.get("display_name"))
    if name == "pressed":
        return "low", "unclear identity; fix item before price research"
    if category in {"meat_fish", "dairy_eggs", "legumes_beans"}:
        return "high", "protein/dairy/legume item can materially affect household cost"
    if total_grams >= 750:
        return "high", "high quantity item can materially affect household cost"
    if category in {"vegetables", "fruits", "carbs_grains", "oils_fats"}:
        return "medium", "common grocery category"
    if category in {"pantry_basics", "seasonings_spices"}:
        return "low", "tiny pantry basic or seasoning"
    return "medium", "missing price in household grocery list"


def _price_research_shape(row: dict[str, Any]) -> tuple[str, str, str, str, str, str]:
    name = _normalise(row.get("display_name"))
    category = str(row.get("shopping_category") or "")
    purchase = _normalise(row.get("purchase_display"))
    if "milk" in name:
        return "carton", "per_liter", "price_per_liter", "", "1000", ""
    if "yogurt" in name:
        return "tub", "per_package", "price_per_package", "500", "", ""
    if "salmon" in name or category == "meat_fish":
        return "loose_weight", "per_kg", "price_per_kg", "", "", ""
    if "pack" in purchase:
        return "pack", "per_package", "price_per_package", "", "", ""
    if category in {"vegetables", "fruits", "legumes_beans", "carbs_grains"}:
        return "loose_weight", "per_kg", "price_per_kg", "", "", ""
    if category in {"pantry_basics", "seasonings_spices"}:
        return "pantry_check", "unknown", "unknown", "", "", ""
    return "other", "unknown", "unknown", "", "", ""


def _suggested_source_query(display_name: str, price_method: str) -> str:
    suffix = {
        "price_per_kg": "pret kg Romania supermarket",
        "price_per_liter": "pret 1L Romania supermarket",
        "price_per_package": "pret pachet Romania supermarket",
    }.get(price_method, "pret Romania supermarket")
    return f"{display_name} {suffix}"


def _primary_member(household_profile: dict[str, Any]) -> dict[str, Any]:
    active_ids = set(household_profile.get("active_member_ids") or [])
    for member in household_profile.get("members", []):
        if str(member.get("member_id") or "") in active_ids:
            return dict(member)
    raise ValueError("Household profile has no active members.")


def _household_context_profile(
    household_profile: dict[str, Any],
    primary_member: dict[str, Any],
) -> dict[str, Any]:
    profile = dict(primary_member)
    preferences = household_profile.get("household_preferences") or {}
    profile["banned_recipe_ids"] = preferences.get("banned_recipe_ids", [])
    profile["banned_ingredient_names"] = preferences.get("banned_ingredient_names", [])
    return profile


def _display_name(item: dict[str, Any]) -> str:
    return str(
        item.get("display_name_clean")
        or item.get("display_name")
        or item.get("canonical_name")
        or "Unknown item"
    )


def _source_members_for_item(item: dict[str, Any], plan: dict[str, Any]) -> str:
    recipes = {
        _source_recipe_id(value)
        for value in str(item.get("source_recipes") or "").split(";")
        if _source_recipe_id(value)
    }
    members = {
        str(row.get("member") or "")
        for row in plan.get("allocations", [])
        if str(row.get("recipe_id") or "") in recipes
    }
    return "; ".join(sorted(member for member in members if member))


def _source_days_for_item(item: dict[str, Any], plan: dict[str, Any]) -> str:
    recipes = {
        _source_recipe_id(value)
        for value in str(item.get("source_recipes") or "").split(";")
        if _source_recipe_id(value)
    }
    days = {
        str(row.get("day_index") or "")
        for row in plan.get("allocations", [])
        if str(row.get("recipe_id") or "") in recipes
    }
    return "; ".join(sorted(day for day in days if day))


def _source_meal_count(item: dict[str, Any]) -> int:
    text = str(item.get("source_meals") or "")
    if not text:
        return 0
    return len([value for value in text.split(";") if value.strip()])


def _source_recipe_id(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return text.split(":", 1)[0].strip()


def _friendly_display_name(value: object) -> str:
    raw = str(value or "").strip()
    text = _normalise(raw)
    replacements = [
        ("apple pulp and peel raw", "Apples"),
        ("dried pasta cooked unsalted", "Cooked pasta"),
        ("lentil boiled cooked in water", "Cooked lentils"),
        ("milk fat content unknown uht sterilized", "UHT milk"),
        ("pork tenderloin lean raw", "Pork tenderloin"),
        ("tuna plain canned drained", "Canned tuna"),
        ("wheat bulgur cooked unsalted", "Cooked bulgur"),
        ("wheat flour white all purpose enriched unbleached", "White flour"),
        ("yogurt fermented milk or dairy specialty plain", "Plain yogurt"),
        ("courgette or zucchini pulp and peel raw", "Zucchini"),
        ("couscous precooked durum wheat semolina cooked unsalted", "Cooked couscous"),
        ("tomato puree canned", "Canned tomato puree"),
        ("semi skimmed milk", "Semi-skimmed milk"),
        ("dried pasta cooked unsalted", "Cooked pasta"),
        ("rice cooked", "Cooked rice"),
    ]
    for needle, replacement in replacements:
        if needle in text:
            return replacement
    return raw


def _join_values(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple, set)):
        return "; ".join(str(item) for item in value if str(item).strip())
    return str(value)


def _priority_sort(value: object) -> int:
    return {"high": 0, "medium": 1, "low": 2}.get(str(value), 9)


def _count_by(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = str(row.get(key) or "missing")
        counts[value] = counts.get(value, 0) + 1
    return counts


def _normalise(value: object) -> str:
    text = str(value or "").lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _to_float(value: Any) -> float | None:
    numeric = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric):
        return None
    return float(numeric)


if __name__ == "__main__":
    main()
