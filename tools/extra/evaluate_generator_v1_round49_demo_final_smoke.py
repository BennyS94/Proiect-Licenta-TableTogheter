from __future__ import annotations

import copy
import csv
import hashlib
import json
import sys
import time
from collections.abc import Mapping
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
    DEFAULT_FOODDB_PATH,
    V1_2_DEMO_FINAL_INGREDIENTS_PATH,
    V1_2_DEMO_FINAL_NUTRITION_PATH,
    V1_2_DEMO_FINAL_PROFILE,
    V1_2_DEMO_FINAL_RECIPES_PATH,
    load_fooddb_current,
    load_recipe_candidate_pool,
)
from src.generator_v1.day_selector_balanced import select_one_day_plan_balanced  # noqa: E402
from src.generator_v1.multi_day_selector import (  # noqa: E402
    MULTI_DAY_MODE_GLOBAL,
    generate_multi_day_plan,
)
from src.generator_v1.plan_quality_gate import evaluate_plan_quality  # noqa: E402
from src.generator_v1.plan_validator import validate_one_day_plan  # noqa: E402
from src.generator_v1.profile_guard import evaluate_profile_guard  # noqa: E402
from src.generator_v1.profile_loader import load_member_profile  # noqa: E402
from src.generator_v1.slot_candidates import build_slot_candidates  # noqa: E402
from src.generator_v1.target_builder import NutritionTarget, build_nutrition_target  # noqa: E402


PROFILE_PATH = ROOT / "profiles/member_profile_demo_v1.json"
AUDIT_DIR = ROOT / "data/recipesdb/audit"
SUMMARY_OUT = AUDIT_DIR / "generator_v1_round49_demo_final_smoke_summary.txt"
RUNS_OUT = AUDIT_DIR / "generator_v1_round49_demo_final_smoke_runs.csv"
GUARD_OUT = AUDIT_DIR / "generator_v1_round49_profile_guard_results.csv"


def main() -> None:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    before_hashes = current_data_hashes()
    base_profile = load_member_profile(PROFILE_PATH)
    edge_profile = edge_low_kcal_profile(base_profile)
    pool = load_recipe_candidate_pool(
        recipes_path=ROOT / V1_2_DEMO_FINAL_RECIPES_PATH,
        ingredients_path=ROOT / V1_2_DEMO_FINAL_INGREDIENTS_PATH,
        nutrition_path=ROOT / V1_2_DEMO_FINAL_NUTRITION_PATH,
        dataset_profile=V1_2_DEMO_FINAL_PROFILE,
    )
    fooddb = load_fooddb_current(ROOT / DEFAULT_FOODDB_PATH)

    run_rows: list[dict[str, Any]] = []
    guard_rows: list[dict[str, Any]] = []

    normal_inputs = build_inputs(base_profile, pool, fooddb)
    run_rows.append(
        run_one_day(
            scenario_id="demo_profile_1_day",
            profile=base_profile,
            inputs=normal_inputs,
            profile_guard_mode="demo",
            allow_unsupported_profile=False,
        )
    )
    run_rows.append(
        run_three_day(
            scenario_id="demo_profile_3_days",
            profile=base_profile,
            inputs=normal_inputs,
            profile_guard_mode="demo",
            allow_unsupported_profile=False,
        )
    )

    edge_target = build_nutrition_target(edge_profile)
    blocked_guard = evaluate_profile_guard(
        edge_profile,
        edge_target,
        meal_config=edge_profile.get("meal_config") or {},
        mode="demo",
    )
    guard_rows.append(
        guard_output_row(
            scenario_id="edge_low_kcal_profile_guard_demo",
            profile=edge_profile,
            target=edge_target,
            guard_result=blocked_guard,
            profile_guard_mode="demo",
            allow_unsupported_profile=False,
            generation_executed=False,
        )
    )
    run_rows.append(
        blocked_run_row(
            scenario_id="edge_low_kcal_profile_guard_demo",
            profile=edge_profile,
            target=edge_target,
            guard_result=blocked_guard,
            profile_guard_mode="demo",
        )
    )

    edge_inputs = build_inputs(edge_profile, pool, fooddb)
    permissive_row = run_one_day(
        scenario_id="edge_low_kcal_profile_permissive_1_day",
        profile=edge_profile,
        inputs=edge_inputs,
        profile_guard_mode="permissive",
        allow_unsupported_profile=False,
    )
    run_rows.append(permissive_row)
    guard_rows.append(
        guard_output_row(
            scenario_id="edge_low_kcal_profile_permissive_1_day",
            profile=edge_profile,
            target=edge_inputs["target"],
            guard_result=permissive_row["profile_guard_result"],
            profile_guard_mode="permissive",
            allow_unsupported_profile=False,
            generation_executed=True,
        )
    )

    for row in run_rows:
        guard_result = row.pop("profile_guard_result", None)
        if guard_result is not None and not any(
            item["scenario_id"] == row["scenario_id"] for item in guard_rows
        ):
            guard_rows.append(
                guard_output_row(
                    scenario_id=row["scenario_id"],
                    profile=base_profile
                    if row["scenario_id"].startswith("demo_profile")
                    else edge_profile,
                    target=normal_inputs["target"]
                    if row["scenario_id"].startswith("demo_profile")
                    else edge_inputs["target"],
                    guard_result=guard_result,
                    profile_guard_mode=row["profile_guard_mode"],
                    allow_unsupported_profile=bool(row["allow_unsupported_profile"]),
                    generation_executed=bool(row["generation_executed"]),
                )
            )

    after_hashes = current_data_hashes()
    current_changed = before_hashes != after_hashes
    write_csv(RUNS_OUT, run_rows, run_columns(run_rows))
    write_csv(GUARD_OUT, guard_rows, guard_columns())
    SUMMARY_OUT.write_text(
        build_summary(
            run_rows=run_rows,
            guard_rows=guard_rows,
            recipe_count=len(pool.recipes),
            active_recipe_count=active_recipe_count(pool.recipes),
            eligible_candidate_count=len(pool.eligible_candidates),
            current_changed=current_changed,
        ),
        encoding="utf-8",
    )
    print("Round49 demo-final smoke written")
    print(f"summary={SUMMARY_OUT}")
    print(f"current_data_changed={current_changed}")


def build_inputs(
    profile: dict[str, Any],
    pool: Any,
    fooddb: pd.DataFrame,
) -> dict[str, Any]:
    target = build_nutrition_target(profile)
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
    return {
        "target": target,
        "filtered_candidate_count": len(filtered_candidates),
        "slot_candidate_count": len(slot_candidates),
        "slot_candidates": slot_candidates,
    }


def run_one_day(
    scenario_id: str,
    profile: dict[str, Any],
    inputs: dict[str, Any],
    profile_guard_mode: str,
    allow_unsupported_profile: bool,
) -> dict[str, Any]:
    target = inputs["target"]
    guard_result = evaluate_profile_guard(
        profile,
        target,
        meal_config=profile.get("meal_config") or {},
        mode=profile_guard_mode,
    )
    if guard_result["should_block_generation"] and not allow_unsupported_profile:
        return blocked_run_row(
            scenario_id=scenario_id,
            profile=profile,
            target=target,
            guard_result=guard_result,
            profile_guard_mode=profile_guard_mode,
        )

    started = time.perf_counter()
    slot_order = slot_order_for_target(target)
    slot_candidates_by_slot = {
        slot: inputs["slot_candidates"].loc[
            inputs["slot_candidates"]["slot"].eq(slot)
        ].copy()
        for slot in slot_order
    }
    plan = select_one_day_plan_balanced(
        slot_candidates_by_slot=slot_candidates_by_slot,
        target=target,
        slot_order=slot_order,
        config={
            "meal_realism_mode": "practical",
            "return_alternatives": False,
            "alternative_count": 1,
        },
    )
    plan["target"] = target_to_dict(target)
    plan["validation"] = validate_one_day_plan(plan, target)
    plan["quality_gate"] = evaluate_plan_quality(
        plan,
        target,
        config={"quality_gate": "demo_safe"},
    )
    plan["quality_gate_status"] = plan["quality_gate"]["quality_gate_status"]
    runtime = round(time.perf_counter() - started, 3)
    validation = plan.get("validation", {})
    quality = plan.get("quality_gate", {})
    totals = plan.get("day_totals", {})
    diagnostics = plan.get("selector_diagnostics", {})
    return {
        "scenario_id": scenario_id,
        "dataset_profile": V1_2_DEMO_FINAL_PROFILE,
        "profile_name": clean_text(profile.get("profile_name")),
        "profile_guard_mode": profile_guard_mode,
        "profile_guard_status": guard_result["profile_guard_status"],
        "profile_guard_reasons": format_list(
            guard_result["profile_guard_reasons"]
        ),
        "allow_unsupported_profile": allow_unsupported_profile,
        "generation_executed": True,
        "blocked_by_guard": False,
        "days": 1,
        "target_kcal": target.kcal,
        "validation_status": value_from(validation, "validation_status"),
        "quality_gate_status": value_from(quality, "quality_gate_status"),
        "valid_day_count": 1
        if value_from(validation, "validation_status") == "valid"
        else 0,
        "accept_day_count": 1
        if value_from(quality, "quality_gate_status") == "accept"
        else 0,
        "review_day_count": 1
        if value_from(quality, "quality_gate_status") == "review"
        else 0,
        "reject_day_count": 1
        if value_from(quality, "quality_gate_status") == "reject"
        else 0,
        "repeated_recipe_count": 0,
        "unique_recipe_count": len(selected_recipe_ids(plan)),
        "multi_day_loss": "",
        "base_day_loss": value_from(diagnostics, "base_day_loss"),
        "adjusted_day_loss": value_from(diagnostics, "adjusted_day_loss"),
        "total_kcal": value_from(totals, "total_kcal"),
        "runtime_s": runtime,
        "strict_verdict": one_day_verdict(plan),
        "filtered_candidate_count": inputs["filtered_candidate_count"],
        "slot_candidate_count": inputs["slot_candidate_count"],
        "selected_recipes": " | ".join(selected_recipe_names(plan)),
        "profile_guard_result": guard_result,
    }


def run_three_day(
    scenario_id: str,
    profile: dict[str, Any],
    inputs: dict[str, Any],
    profile_guard_mode: str,
    allow_unsupported_profile: bool,
) -> dict[str, Any]:
    target = inputs["target"]
    guard_result = evaluate_profile_guard(
        profile,
        target,
        meal_config=profile.get("meal_config") or {},
        mode=profile_guard_mode,
    )
    if guard_result["should_block_generation"] and not allow_unsupported_profile:
        return blocked_run_row(
            scenario_id=scenario_id,
            profile=profile,
            target=target,
            guard_result=guard_result,
            profile_guard_mode=profile_guard_mode,
        )

    started = time.perf_counter()
    plan = generate_multi_day_plan(
        profile=profile,
        target=target,
        slot_candidates=inputs["slot_candidates"],
        days=3,
        config=multi_day_config(),
    )
    runtime = round(time.perf_counter() - started, 3)
    summary = plan.get("multi_day_summary", {})
    validation = plan.get("multi_day_validation", {})
    return {
        "scenario_id": scenario_id,
        "dataset_profile": V1_2_DEMO_FINAL_PROFILE,
        "profile_name": clean_text(profile.get("profile_name")),
        "profile_guard_mode": profile_guard_mode,
        "profile_guard_status": guard_result["profile_guard_status"],
        "profile_guard_reasons": format_list(
            guard_result["profile_guard_reasons"]
        ),
        "allow_unsupported_profile": allow_unsupported_profile,
        "generation_executed": True,
        "blocked_by_guard": False,
        "days": 3,
        "target_kcal": target.kcal,
        "validation_status": validation.get("validation_status"),
        "quality_gate_status": "",
        "valid_day_count": summary.get("valid_day_count"),
        "accept_day_count": summary.get("accept_day_count"),
        "review_day_count": summary.get("review_day_count"),
        "reject_day_count": summary.get("reject_day_count"),
        "repeated_recipe_count": summary.get("repeated_recipe_count"),
        "unique_recipe_count": summary.get("unique_recipe_count"),
        "multi_day_loss": plan.get("multi_day_loss"),
        "base_day_loss": "",
        "adjusted_day_loss": "",
        "total_kcal": "",
        "runtime_s": runtime,
        "strict_verdict": summary.get("multi_day_classification"),
        "filtered_candidate_count": inputs["filtered_candidate_count"],
        "slot_candidate_count": inputs["slot_candidate_count"],
        "selected_recipes": " | ".join(selected_recipe_names_from_days(plan)),
        "profile_guard_result": guard_result,
    }


def blocked_run_row(
    scenario_id: str,
    profile: dict[str, Any],
    target: NutritionTarget,
    guard_result: Mapping[str, Any],
    profile_guard_mode: str,
) -> dict[str, Any]:
    return {
        "scenario_id": scenario_id,
        "dataset_profile": V1_2_DEMO_FINAL_PROFILE,
        "profile_name": clean_text(profile.get("profile_name")),
        "profile_guard_mode": profile_guard_mode,
        "profile_guard_status": guard_result.get("profile_guard_status"),
        "profile_guard_reasons": format_list(
            guard_result.get("profile_guard_reasons")
        ),
        "allow_unsupported_profile": False,
        "generation_executed": False,
        "blocked_by_guard": True,
        "days": "",
        "target_kcal": target.kcal,
        "validation_status": "blocked_by_profile_guard",
        "quality_gate_status": "",
        "valid_day_count": 0,
        "accept_day_count": 0,
        "review_day_count": 0,
        "reject_day_count": 0,
        "repeated_recipe_count": "",
        "unique_recipe_count": "",
        "multi_day_loss": "",
        "base_day_loss": "",
        "adjusted_day_loss": "",
        "total_kcal": "",
        "runtime_s": 0,
        "strict_verdict": "blocked_by_profile_guard",
        "filtered_candidate_count": "",
        "slot_candidate_count": "",
        "selected_recipes": "",
        "profile_guard_result": dict(guard_result),
    }


def guard_output_row(
    scenario_id: str,
    profile: dict[str, Any],
    target: NutritionTarget,
    guard_result: Mapping[str, Any],
    profile_guard_mode: str,
    allow_unsupported_profile: bool,
    generation_executed: bool,
) -> dict[str, Any]:
    return {
        "scenario_id": scenario_id,
        "profile_name": clean_text(profile.get("profile_name")),
        "profile_guard_mode": profile_guard_mode,
        "target_kcal": target.kcal,
        "goal": clean_text(profile.get("goal")),
        "goal_speed": clean_text(profile.get("goal_speed")),
        "activity_level": clean_text(profile.get("activity_level")),
        "include_snacks": bool(
            (profile.get("meal_config") or {}).get("include_snacks", False)
        ),
        "profile_guard_status": guard_result.get("profile_guard_status"),
        "profile_guard_reasons": format_list(
            guard_result.get("profile_guard_reasons")
        ),
        "profile_guard_recommendations": format_list(
            guard_result.get("profile_guard_recommendations")
        ),
        "should_block_generation": guard_result.get("should_block_generation"),
        "allow_unsupported_profile": allow_unsupported_profile,
        "generation_executed": generation_executed,
        "suggested_adjustments": json.dumps(
            guard_result.get("suggested_adjustments") or {},
            sort_keys=True,
        ),
    }


def build_summary(
    run_rows: list[dict[str, Any]],
    guard_rows: list[dict[str, Any]],
    recipe_count: int,
    active_recipe_count: int,
    eligible_candidate_count: int,
    current_changed: bool,
) -> str:
    normal_one_day = find_row(run_rows, "demo_profile_1_day")
    normal_three_day = find_row(run_rows, "demo_profile_3_days")
    blocked_edge = find_row(run_rows, "edge_low_kcal_profile_guard_demo")
    permissive_edge = find_row(run_rows, "edge_low_kcal_profile_permissive_1_day")
    demo_ok = (
        normal_one_day.get("validation_status") == "valid"
        and normal_one_day.get("quality_gate_status") == "accept"
    )
    multi_ok = (
        to_int(normal_three_day.get("valid_day_count")) == 3
        and to_int(normal_three_day.get("accept_day_count")) == 3
        and to_int(normal_three_day.get("repeated_recipe_count")) == 0
    )
    edge_blocked = bool(blocked_edge.get("blocked_by_guard"))
    edge_warned = permissive_edge.get("profile_guard_status") == "unsupported_for_demo"
    lines = [
        "Generator v1 Round49 demo-final smoke summary",
        "",
        f"dataset_profile={V1_2_DEMO_FINAL_PROFILE}",
        "source_dataset_profile=v1_2_demo_candidate_round48_cleaned",
        f"recipe_count={recipe_count}",
        f"active_recipe_count={active_recipe_count}",
        f"eligible_candidate_count={eligible_candidate_count}",
        "",
        "Recommended demo config:",
        "- selection_mode=balanced_day",
        "- portion_policy=target_aware",
        "- meal_realism_mode=practical",
        "- quality_gate=demo_safe",
        "- multi_day_mode=global_alternatives_3_day",
        "- no_repeat_policy=hard",
        "- day_candidate_builder=direct_from_slots",
        "- profile_guard=demo",
        "",
        "Smoke results:",
        f"- demo_profile_1_day_valid_accept={demo_ok}",
        f"- demo_profile_3_days_valid_accept_no_repeat={multi_ok}",
        f"- demo_profile_3_days_multi_day_loss={normal_three_day.get('multi_day_loss')}",
        f"- edge_profile_blocked_in_demo={edge_blocked}",
        f"- edge_profile_warned_in_permissive={edge_warned}",
        f"- current_data_changed={current_changed}",
        "",
        "Guard statuses:",
    ]
    for row in guard_rows:
        lines.append(
            "- {scenario_id}: status={profile_guard_status}, block={should_block_generation}, "
            "executed={generation_executed}, reasons={profile_guard_reasons}".format(**row)
        )
    lines.extend(
        [
            "",
            "Strict conclusion:",
            strict_conclusion(
                demo_ok=demo_ok,
                multi_ok=multi_ok,
                edge_blocked=edge_blocked,
                edge_warned=edge_warned,
                current_changed=current_changed,
            ),
        ]
    )
    return "\n".join(lines) + "\n"


def strict_conclusion(
    demo_ok: bool,
    multi_ok: bool,
    edge_blocked: bool,
    edge_warned: bool,
    current_changed: bool,
) -> str:
    if demo_ok and multi_ok and edge_blocked and edge_warned and not current_changed:
        return (
            "Round49 trece: demo-final genereaza normal, profilul agresiv este blocat/warned, "
            "iar datele current nu au fost schimbate."
        )
    return "Round49 are risc: verifica CSV-urile pentru generare, guard sau modificari current."


def edge_low_kcal_profile(base_profile: dict[str, Any]) -> dict[str, Any]:
    profile = copy.deepcopy(base_profile)
    profile.update(
        {
            "member_profile_id": "sedentary_lose_fast_with_snack",
            "profile_name": "Sedentary Lose Fast With Snack",
            "age": 50,
            "sex": "female",
            "weight_kg": 88.0,
            "height_cm": 162.0,
            "activity_level": "sedentary",
            "goal": "lose",
            "goal_speed": "fast",
            "training": {"type": "none", "sessions_per_week": 0},
            "meal_config": {
                "meals_per_day": 3,
                "include_snacks": True,
                "day_structure": "3_meals_plus_snack",
            },
        }
    )
    return profile


def multi_day_config() -> dict[str, Any]:
    return {
        "selection_mode": "balanced_day",
        "portion_policy": "target_aware",
        "meal_realism_mode": "practical",
        "quality_gate": "demo_safe",
        "alternative_count": 3,
        "return_alternatives": True,
        "multi_day_mode": MULTI_DAY_MODE_GLOBAL,
        "no_repeat_policy": "hard",
        "multi_day_speed_mode": "fast",
        "day_candidate_builder": "direct_from_slots",
        "direct_slot_shortlist_size": 12,
        "day_candidate_pool_size_target": 50,
        "day_candidate_pool_max": 150,
        "include_slot_forced_variants": True,
    }


def slot_order_for_target(target: NutritionTarget) -> list[str]:
    preferred_order = ["breakfast", "lunch", "dinner", "snack"]
    known_slots = [slot for slot in preferred_order if slot in target.slot_targets]
    extra_slots = [slot for slot in target.slot_targets if slot not in preferred_order]
    return known_slots + extra_slots


def target_to_dict(target: NutritionTarget) -> dict[str, Any]:
    return {
        "kcal": target.kcal,
        "protein_g": target.protein_g,
        "carbs_g": target.carbs_g,
        "fat_g": target.fat_g,
        "slot_targets": target.slot_targets,
    }


def one_day_verdict(plan: Mapping[str, Any]) -> str:
    validation_status = value_from(plan.get("validation", {}), "validation_status")
    quality_status = value_from(plan.get("quality_gate", {}), "quality_gate_status")
    if validation_status == "valid" and quality_status == "accept":
        return "one_day_good"
    if validation_status == "valid":
        return "one_day_review"
    return "one_day_bad"


def selected_recipe_names(plan: Mapping[str, Any]) -> list[str]:
    names: list[str] = []
    for meal in plan.get("selected_meals", []):
        if isinstance(meal, Mapping) and meal.get("display_name"):
            names.append(clean_text(meal["display_name"]))
    return names


def selected_recipe_names_from_days(plan: Mapping[str, Any]) -> list[str]:
    names: list[str] = []
    for day in plan.get("days", []):
        if isinstance(day, Mapping):
            names.extend(selected_recipe_names(day))
    return names


def selected_recipe_ids(plan: Mapping[str, Any]) -> list[str]:
    ids: list[str] = []
    for meal in plan.get("selected_meals", []):
        if isinstance(meal, Mapping) and meal.get("recipe_id"):
            ids.append(clean_text(meal["recipe_id"]))
    return ids


def value_from(value: object, key: str) -> object:
    if isinstance(value, Mapping):
        return value.get(key, "")
    return ""


def active_recipe_count(recipes: pd.DataFrame) -> int:
    if "is_active" not in recipes.columns:
        return len(recipes)
    values = pd.to_numeric(recipes["is_active"], errors="coerce").fillna(0)
    return int(values.eq(1).sum())


def current_data_hashes() -> dict[str, str]:
    paths = [
        ROOT / "data/recipesdb/current/recipes.csv",
        ROOT / "data/recipesdb/current/recipe_ingredients.csv",
        ROOT / "data/recipesdb/current/recipe_nutrition_cache.csv",
        ROOT / "data/fooddb/current/fooddb_v1_core_master_draft.csv",
    ]
    return {str(path.relative_to(ROOT)): file_hash(path) for path in paths}


def file_hash(path: Path) -> str:
    if not path.exists():
        return "missing"
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def run_columns(rows: list[dict[str, Any]]) -> list[str]:
    preferred = [
        "scenario_id",
        "dataset_profile",
        "profile_name",
        "profile_guard_mode",
        "profile_guard_status",
        "profile_guard_reasons",
        "allow_unsupported_profile",
        "generation_executed",
        "blocked_by_guard",
        "days",
        "target_kcal",
        "validation_status",
        "quality_gate_status",
        "valid_day_count",
        "accept_day_count",
        "review_day_count",
        "reject_day_count",
        "repeated_recipe_count",
        "unique_recipe_count",
        "multi_day_loss",
        "base_day_loss",
        "adjusted_day_loss",
        "total_kcal",
        "runtime_s",
        "strict_verdict",
        "filtered_candidate_count",
        "slot_candidate_count",
        "selected_recipes",
    ]
    return [column for column in preferred if any(column in row for row in rows)]


def guard_columns() -> list[str]:
    return [
        "scenario_id",
        "profile_name",
        "profile_guard_mode",
        "target_kcal",
        "goal",
        "goal_speed",
        "activity_level",
        "include_snacks",
        "profile_guard_status",
        "profile_guard_reasons",
        "profile_guard_recommendations",
        "should_block_generation",
        "allow_unsupported_profile",
        "generation_executed",
        "suggested_adjustments",
    ]


def find_row(rows: list[dict[str, Any]], scenario_id: str) -> dict[str, Any]:
    for row in rows:
        if row.get("scenario_id") == scenario_id:
            return row
    return {}


def format_list(value: object) -> str:
    if isinstance(value, list):
        return "|".join(str(item) for item in value)
    if isinstance(value, tuple):
        return "|".join(str(item) for item in value)
    if value is None:
        return ""
    return str(value)


def clean_text(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() == "nan":
        return ""
    return text


def to_int(value: object) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


if __name__ == "__main__":
    main()
