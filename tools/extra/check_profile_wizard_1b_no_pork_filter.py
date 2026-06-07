from __future__ import annotations

import json
import re
import sqlite3
import sys
from pathlib import Path
from typing import Any, Iterable, Mapping

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

SUMMARY_PATH = PROJECT_ROOT / "data/recipesdb/audit/profile_wizard_1b_no_pork_filter_summary.txt"
HOUSEHOLD_ID = "profile_wizard_1b_household"
DATASET_PROFILE = "v1_2_demo_final"
NO_PORK_SMOKE_DAYS = 3
SECONDARY_SMOKE_DAYS = 1

PORK_TERMS = [
    "pork",
    "bacon",
    "ham",
    "prosciutto",
    "pancetta",
    "chorizo",
    "pepperoni",
    "pork tenderloin",
    "pork loin",
    "pork chop",
]

AVOID_INGREDIENT = "mushroom"


def _cleanup_previous_test_rows(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "DELETE FROM member_profiles WHERE household_id = ?",
            (HOUSEHOLD_ID,),
        )


def _base_profile_request(display_name: str) -> dict[str, Any]:
    return {
        "household_id": HOUSEHOLD_ID,
        "display_name": display_name,
        "age": 35,
        "sex": "male",
        "weight_kg": 82.0,
        "height_cm": 180.0,
        "activity_level": "moderately_active",
        "goal": "maintain",
        "goal_speed": "normal",
        "training": {
            "sessions_per_week": 3,
            "type": "mixed",
        },
        "meal_config": {
            "meals_per_day": 3,
            "include_snacks": True,
            "day_structure": "3_meals_plus_snack",
        },
        "dietary_preferences": {
            "vegetarian": False,
            "vegan": False,
            "gluten_free": False,
            "no_beef": False,
            "no_pork": False,
            "no_chicken": False,
            "no_fish": False,
            "no_dairy": False,
        },
        "food_preferences": {
            "ratings": {},
            "avoid_ingredients": [],
            "cooking_time_preference": "balanced",
        },
        "bf_profile": "normal",
    }


def _legacy_profile_request(display_name: str) -> dict[str, Any]:
    request = _base_profile_request(display_name)
    request["dietary_preferences"] = {
        "no_beef": False,
        "no_chicken": False,
        "no_fish": False,
        "no_dairy": False,
        "vegetarian": False,
        "vegan": False,
        "gluten_free": False,
    }
    request.pop("food_preferences", None)
    return request


def _generation_request(
    member_profile_id: str,
    household_id: str,
    *,
    days: int,
) -> dict[str, Any]:
    return {
        "dataset_profile": DATASET_PROFILE,
        "days": days,
        "household_id": household_id,
        "member_profile_id": member_profile_id,
        "generation_options": {
            "selection_mode": "balanced_day",
            "portion_policy": "target_aware",
            "meal_realism_mode": "practical",
            "quality_gate": "demo_safe",
            "profile_guard": "demo",
            "multi_day_mode": "global_alternatives_3_day",
            "multi_day_no_repeat_policy": "hard",
            "day_candidate_builder": "direct_from_slots",
            "include_grocery_list": False,
            "include_purchase_suggestions": False,
            "include_price_estimates": False,
            "feedback_enabled": True,
        },
        "include_grocery_list": False,
        "include_purchase_suggestions": False,
        "include_price_estimates": False,
        "feedback_enabled": True,
    }


def _create_profile(client: Any, request: dict[str, Any], errors: list[str], label: str) -> dict[str, Any]:
    response = client.post("/profiles", json=request)
    payload = response.json()
    if response.status_code != 200:
        errors.append(f"{label}_profile_status={response.status_code}")
    if not payload.get("member_profile_id"):
        errors.append(f"{label}_profile_missing_id")
    return payload


def _generate_plan(
    client: Any,
    profile: Mapping[str, Any],
    errors: list[str],
    label: str,
    *,
    days: int,
) -> dict[str, Any]:
    response = client.post(
        "/plans/generate",
        json=_generation_request(
            member_profile_id=str(profile.get("member_profile_id") or ""),
            household_id=str(profile.get("household_id") or HOUSEHOLD_ID),
            days=days,
        ),
    )
    payload = response.json()
    if response.status_code != 200:
        errors.append(f"{label}_generation_status={response.status_code}")
    if payload.get("status") != "ok":
        errors.append(f"{label}_generation_payload_status={payload.get('status')}")
    recipe_ids = _selected_recipe_ids(payload)
    if not recipe_ids:
        errors.append(f"{label}_selected_recipe_ids_missing")
    return payload


def _selected_recipe_ids(payload: Mapping[str, Any]) -> list[str]:
    recipe_ids: list[str] = []
    daily_plan = payload.get("daily_plan")
    if not isinstance(daily_plan, list):
        return recipe_ids
    for day in daily_plan:
        if not isinstance(day, Mapping):
            continue
        for key in ("selected_meals", "meals"):
            meals = day.get(key)
            if not isinstance(meals, list):
                continue
            for meal in meals:
                if not isinstance(meal, Mapping):
                    continue
                recipe_id = _clean_text(meal.get("recipe_id"))
                if recipe_id:
                    recipe_ids.append(recipe_id)
    return sorted(set(recipe_ids))


def _load_ingredient_texts(recipe_ids: Iterable[str]) -> dict[str, str]:
    from src.generator_v1.data_loader import (
        V1_2_DEMO_FINAL_INGREDIENTS_PATH,
        V1_2_DEMO_FINAL_RECIPES_PATH,
    )

    recipe_id_set = {str(recipe_id).strip() for recipe_id in recipe_ids if str(recipe_id).strip()}
    ingredients = pd.read_csv(V1_2_DEMO_FINAL_INGREDIENTS_PATH)
    recipes = pd.read_csv(V1_2_DEMO_FINAL_RECIPES_PATH)
    text_columns = [
        column
        for column in (
            "ingredient_name_normalized",
            "ingredient_name_parsed",
            "mapped_food_canonical_name",
            "ingredient_raw_text",
        )
        if column in ingredients.columns
    ]
    recipe_columns = [
        column
        for column in (
            "display_name",
            "recipe_family_name",
            "recipe_category",
        )
        if column in recipes.columns
    ]

    result: dict[str, str] = {}
    ingredient_rows = ingredients.loc[ingredients["recipe_id"].astype(str).isin(recipe_id_set)]
    recipe_rows = recipes.loc[recipes["recipe_id"].astype(str).isin(recipe_id_set)]
    for recipe_id in recipe_id_set:
        parts: list[str] = []
        rows = ingredient_rows.loc[ingredient_rows["recipe_id"].astype(str) == recipe_id]
        if text_columns and not rows.empty:
            parts.extend(
                rows[text_columns]
                .fillna("")
                .astype(str)
                .agg(" ".join, axis=1)
                .tolist()
            )
        recipe_row = recipe_rows.loc[recipe_rows["recipe_id"].astype(str) == recipe_id]
        if recipe_columns and not recipe_row.empty:
            parts.extend(
                recipe_row[recipe_columns]
                .fillna("")
                .astype(str)
                .agg(" ".join, axis=1)
                .tolist()
            )
        result[recipe_id] = _normalize_text(" ".join(parts))
    return result


def _matching_terms(recipe_texts: Mapping[str, str], terms: Iterable[str]) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {}
    for recipe_id, text in recipe_texts.items():
        hits = [
            term
            for term in terms
            if re.search(_term_pattern(term), text, flags=re.IGNORECASE)
        ]
        if hits:
            result[recipe_id] = hits
    return result


def _synthetic_filter_checks() -> list[str]:
    from src.generator_v1.candidate_filter import (
        build_household_preference_context,
        filter_recipe_candidates,
    )

    errors: list[str] = []
    eligible_candidates = pd.DataFrame(
        [
            {"recipe_id": "synthetic_pork", "display_name": "Synthetic Pork"},
            {"recipe_id": "synthetic_mushroom", "display_name": "Synthetic Mushroom"},
            {"recipe_id": "synthetic_chicken", "display_name": "Synthetic Chicken"},
        ]
    )
    ingredients = pd.DataFrame(
        [
            {
                "recipe_id": "synthetic_pork",
                "ingredient_name_normalized": "pork tenderloin",
                "ingredient_name_parsed": "pork tenderloin",
                "mapped_food_canonical_name": "pork tenderloin",
                "ingredient_raw_text": "pork tenderloin",
            },
            {
                "recipe_id": "synthetic_mushroom",
                "ingredient_name_normalized": "mushroom",
                "ingredient_name_parsed": "mushroom",
                "mapped_food_canonical_name": "mushroom",
                "ingredient_raw_text": "mushroom",
            },
            {
                "recipe_id": "synthetic_chicken",
                "ingredient_name_normalized": "chicken breast",
                "ingredient_name_parsed": "chicken breast",
                "mapped_food_canonical_name": "chicken breast",
                "ingredient_raw_text": "chicken breast",
            },
        ]
    )

    no_pork_context = build_household_preference_context(
        {"dietary_preferences": {"no_pork": True}}
    )
    no_pork_filtered = filter_recipe_candidates(
        eligible_candidates=eligible_candidates,
        ingredients=ingredients,
        context=no_pork_context,
    )
    no_pork_ids = set(no_pork_filtered["recipe_id"].astype(str))
    if "synthetic_pork" in no_pork_ids:
        errors.append("synthetic_no_pork_failed")
    if "synthetic_chicken" not in no_pork_ids:
        errors.append("synthetic_no_pork_removed_chicken")

    pork_avoid_context = build_household_preference_context(
        {"food_preferences": {"ratings": {"pork": "avoid"}}}
    )
    pork_avoid_filtered = filter_recipe_candidates(
        eligible_candidates=eligible_candidates,
        ingredients=ingredients,
        context=pork_avoid_context,
    )
    pork_avoid_ids = set(pork_avoid_filtered["recipe_id"].astype(str))
    if "synthetic_pork" in pork_avoid_ids:
        errors.append("synthetic_pork_rating_avoid_failed")

    mushroom_context = build_household_preference_context(
        {"food_preferences": {"avoid_ingredients": [AVOID_INGREDIENT]}}
    )
    mushroom_filtered = filter_recipe_candidates(
        eligible_candidates=eligible_candidates,
        ingredients=ingredients,
        context=mushroom_context,
    )
    mushroom_ids = set(mushroom_filtered["recipe_id"].astype(str))
    if "synthetic_mushroom" in mushroom_ids:
        errors.append("synthetic_avoid_ingredient_failed")
    if "synthetic_chicken" not in mushroom_ids:
        errors.append("synthetic_avoid_ingredient_removed_chicken")

    return errors


def main() -> int:
    from fastapi.testclient import TestClient

    from backend.app.db.database import init_db
    from backend.app.main import app

    db_path = init_db()
    _cleanup_previous_test_rows(db_path)

    client = TestClient(app)
    errors: list[str] = []

    no_pork_request = _base_profile_request("Profile Wizard 1b No Pork")
    no_pork_request["dietary_preferences"]["no_pork"] = True
    no_pork_profile = _create_profile(client, no_pork_request, errors, "no_pork")
    no_pork_plan = _generate_plan(
        client,
        no_pork_profile,
        errors,
        "no_pork",
        days=NO_PORK_SMOKE_DAYS,
    )
    no_pork_recipe_ids = _selected_recipe_ids(no_pork_plan)
    no_pork_hits = _matching_terms(_load_ingredient_texts(no_pork_recipe_ids), PORK_TERMS)
    if no_pork_hits:
        errors.append("no_pork_selected_pork_terms")

    pork_avoid_request = _base_profile_request("Profile Wizard 1b Pork Avoid")
    pork_avoid_request["food_preferences"]["ratings"] = {"pork": "avoid"}
    pork_avoid_profile = _create_profile(client, pork_avoid_request, errors, "pork_avoid")
    pork_avoid_plan = _generate_plan(
        client,
        pork_avoid_profile,
        errors,
        "pork_avoid",
        days=NO_PORK_SMOKE_DAYS,
    )
    pork_avoid_recipe_ids = _selected_recipe_ids(pork_avoid_plan)
    pork_avoid_hits = _matching_terms(_load_ingredient_texts(pork_avoid_recipe_ids), PORK_TERMS)
    if pork_avoid_hits:
        errors.append("pork_avoid_selected_pork_terms")

    avoid_ingredient_request = _base_profile_request("Profile Wizard 1b Mushroom Avoid")
    avoid_ingredient_request["food_preferences"]["avoid_ingredients"] = [AVOID_INGREDIENT]
    avoid_profile = _create_profile(client, avoid_ingredient_request, errors, "avoid_ingredient")
    avoid_plan = _generate_plan(
        client,
        avoid_profile,
        errors,
        "avoid_ingredient",
        days=SECONDARY_SMOKE_DAYS,
    )
    avoid_recipe_ids = _selected_recipe_ids(avoid_plan)
    avoid_hits = _matching_terms(
        _load_ingredient_texts(avoid_recipe_ids),
        [AVOID_INGREDIENT, "mushrooms"],
    )
    if avoid_hits:
        errors.append("avoid_ingredient_selected_avoided_terms")

    legacy_profile = _create_profile(
        client,
        _legacy_profile_request("Profile Wizard 1b Legacy"),
        errors,
        "legacy",
    )
    legacy_plan = _generate_plan(
        client,
        legacy_profile,
        errors,
        "legacy",
        days=SECONDARY_SMOKE_DAYS,
    )
    legacy_defaults_ok = (
        legacy_profile.get("dietary_preferences", {}).get("no_pork") is False
        and legacy_profile.get("food_preferences")
        == {
            "ratings": {},
            "avoid_ingredients": [],
            "cooking_time_preference": "balanced",
        }
    )
    if not legacy_defaults_ok:
        errors.append("legacy_profile_defaults_failed")

    errors.extend(_synthetic_filter_checks())

    status_ok = not errors
    summary_lines = [
        "PROFILE-WIZARD-1b no_pork hard-filter summary",
        "status=ok" if status_ok else "status=failed",
        f"db_path={db_path}",
        f"dataset_profile={DATASET_PROFILE}",
        f"no_pork_smoke_days={NO_PORK_SMOKE_DAYS}",
        f"secondary_smoke_days={SECONDARY_SMOKE_DAYS}",
        "pork_terms_checked=" + ",".join(PORK_TERMS),
        "no_pork_plan_status=" + str(no_pork_plan.get("status")),
        "no_pork_selected_recipe_ids=" + ",".join(no_pork_recipe_ids),
        "no_pork_hits=" + _json_or_none(no_pork_hits),
        "pork_avoid_plan_status=" + str(pork_avoid_plan.get("status")),
        "pork_avoid_selected_recipe_ids=" + ",".join(pork_avoid_recipe_ids),
        "pork_avoid_hits=" + _json_or_none(pork_avoid_hits),
        f"avoid_ingredient={AVOID_INGREDIENT}",
        "avoid_ingredient_plan_status=" + str(avoid_plan.get("status")),
        "avoid_ingredient_selected_recipe_ids=" + ",".join(avoid_recipe_ids),
        "avoid_ingredient_hits=" + _json_or_none(avoid_hits),
        "legacy_plan_status=" + str(legacy_plan.get("status")),
        f"legacy_defaults_ok={str(legacy_defaults_ok).lower()}",
        "errors=" + (";".join(errors) if errors else "none"),
    ]

    SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)
    SUMMARY_PATH.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
    print("\n".join(summary_lines))
    return 0 if status_ok else 1


def _json_or_none(value: Mapping[str, Any]) -> str:
    if not value:
        return "none"
    return json.dumps(value, ensure_ascii=True, sort_keys=True)


def _normalize_text(value: Any) -> str:
    text = str(value).lower().replace("_", " ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _term_pattern(term: str) -> str:
    normalized = _normalize_text(term)
    return rf"(?<![a-z0-9]){re.escape(normalized)}(?![a-z0-9])"


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


if __name__ == "__main__":
    raise SystemExit(main())
