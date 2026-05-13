from __future__ import annotations

from datetime import datetime
import sys
import time
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.generator_v1.candidate_diagnostics import build_candidate_diagnostics
from src.generator_v1.candidate_filter import (
    build_household_preference_context,
    filter_recipe_candidates,
)
from src.generator_v1.data_loader import (
    DEFAULT_INGREDIENTS_PATH,
    DEFAULT_NUTRITION_PATH,
    DEFAULT_RECIPES_PATH,
    PILOT_CURRENT_PROFILE,
    V1_1_GENERATOR_READY_INGREDIENTS_PATH,
    V1_1_GENERATOR_READY_NUTRITION_PATH,
    V1_1_GENERATOR_READY_PROFILE,
    V1_1_GENERATOR_READY_RECIPES_PATH,
    V1_1_GENERATOR_READY_SLOT_CHECKED_INGREDIENTS_PATH,
    V1_1_GENERATOR_READY_SLOT_CHECKED_NUTRITION_PATH,
    V1_1_GENERATOR_READY_SLOT_CHECKED_PROFILE,
    V1_1_GENERATOR_READY_SLOT_CHECKED_RECIPES_PATH,
    V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_INGREDIENTS_PATH,
    V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_NUTRITION_PATH,
    V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_PROFILE,
    V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_RECIPES_PATH,
    V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_INGREDIENTS_PATH,
    V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_NUTRITION_PATH,
    V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PLUS10_INGREDIENTS_PATH,
    V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PLUS10_NUTRITION_PATH,
    V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PLUS10_PROFILE,
    V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PLUS10_RECIPES_PATH,
    V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PROFILE,
    V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_RECIPES_PATH,
    V1_2_GENERATOR_READY_PLUS30_INGREDIENTS_PATH,
    V1_2_GENERATOR_READY_PLUS30_NUTRITION_PATH,
    V1_2_GENERATOR_READY_PLUS30_PLUS15_INGREDIENTS_PATH,
    V1_2_GENERATOR_READY_PLUS30_PLUS15_NUTRITION_PATH,
    V1_2_GENERATOR_READY_PLUS30_PLUS15_PROFILE,
    V1_2_GENERATOR_READY_PLUS30_PLUS15_RECIPES_PATH,
    V1_2_GENERATOR_READY_PLUS30_PROFILE,
    V1_2_GENERATOR_READY_PLUS30_RECIPES_PATH,
    load_fooddb_current,
    load_recipe_candidate_pool,
)
from src.generator_v1.day_selector import select_one_day_plan
from src.generator_v1.day_selector_balanced import select_one_day_plan_balanced
from src.generator_v1.ingredient_diagnostics import build_ingredient_diagnostics
from src.generator_v1.multi_day_audit import multi_day_readable_lines
from src.generator_v1.multi_day_selector import (
    MULTI_DAY_MODE_GLOBAL,
    MULTI_DAY_MODE_SIMPLE,
    generate_multi_day_plan,
)
from src.generator_v1.nutrition_cache_diagnostics import (
    build_nutrition_cache_diagnostics,
)
from src.generator_v1.pilot_servings_estimator import (
    build_pilot_servings_diagnostics,
)
from src.generator_v1.plan_validator import validate_one_day_plan
from src.generator_v1.plan_quality_gate import evaluate_plan_quality
from src.generator_v1.profile_loader import load_member_profile
from src.generator_v1.reroll_policy import select_quality_gated_reroll
from src.generator_v1.slot_candidates import build_slot_candidates
from src.generator_v1.target_builder import NutritionTarget, build_nutrition_target


PROFILE_PATH = Path("profiles/member_profile_demo_v1.json")
SESSION_SCHEMA_VERSION = 25
SESSION_SCHEMA_KEY = "generator_v1_dashboard_schema_version"
SESSION_MENUS_KEY = "generator_v1_generated_menus"
SESSION_LATEST_MENU_KEY = "generator_v1_latest_menu"
SESSION_MULTIDAY_PLANS_KEY = "generator_v1_multiday_plans"
SESSION_LATEST_MULTIDAY_KEY = "generator_v1_latest_multiday"
SESSION_FEEDBACK_KEY = "generator_v1_feedback_events"
SESSION_RECENT_RECIPE_IDS_KEY = "generator_v1_recent_recipe_ids"
SESSION_RUN_COUNTER_KEY = "generator_v1_run_counter"
SESSION_DATASET_PROFILE_KEY = "generator_v1_dataset_profile"
SESSION_SELECTION_MODE_KEY = "generator_v1_selection_mode"
SESSION_ALTERNATIVE_COUNT_KEY = "generator_v1_alternative_count"
SESSION_DIVERSITY_MODE_KEY = "generator_v1_diversity_mode"
SESSION_PORTION_POLICY_KEY = "generator_v1_portion_policy"
SESSION_MEAL_REALISM_MODE_KEY = "generator_v1_meal_realism_mode"
SESSION_QUALITY_GATE_KEY = "generator_v1_quality_gate"
SESSION_MULTIDAY_MODE_KEY = "generator_v1_multiday_mode"
SESSION_MULTIDAY_NO_REPEAT_POLICY_KEY = "generator_v1_multiday_no_repeat_policy"
SESSION_DAY_CANDIDATE_POOL_SIZE_KEY = "generator_v1_day_candidate_pool_size"
SESSION_MULTIDAY_SPEED_MODE_KEY = "generator_v1_multiday_speed_mode"
SESSION_DAY_CANDIDATE_BUILDER_KEY = "generator_v1_day_candidate_builder"
SESSION_DIRECT_SLOT_SHORTLIST_SIZE_KEY = "generator_v1_direct_slot_shortlist_size"
RECENT_MENUS_FOR_VARIATION = 2
V1_1_RECOMMENDED_SELECTION_MODE = "balanced_day"
V1_1_RECOMMENDED_ALTERNATIVE_COUNT = 3
V1_1_RECOMMENDED_DIVERSITY_MODE = "none"
V1_1_RECOMMENDED_PORTION_POLICY = "target_aware"
V1_1_RECOMMENDED_MEAL_REALISM_MODE = "practical"
V1_1_RECOMMENDED_QUALITY_GATE = "demo_safe"

SELECTION_MODE_OPTIONS = {
    "Greedy": "greedy",
    "Balanced day": "balanced_day",
}
ALTERNATIVE_COUNT_OPTIONS = [1, 2, 3]
DIVERSITY_MODE_OPTIONS = ["none", "soft", "avoid_recent"]
PORTION_POLICY_OPTIONS = ["standard", "expanded_safe", "target_aware"]
MEAL_REALISM_MODE_OPTIONS = ["off", "audit", "soft", "practical"]
QUALITY_GATE_OPTIONS = ["off", "demo_safe"]
MULTI_DAY_MODE_OPTIONS = [MULTI_DAY_MODE_SIMPLE, MULTI_DAY_MODE_GLOBAL]
MULTI_DAY_NO_REPEAT_POLICY_OPTIONS = ["prefer", "hard", "main_only", "none"]
DAY_CANDIDATE_POOL_SIZE_OPTIONS = [50, 75, 100, 150]
MULTI_DAY_SPEED_MODE_OPTIONS = ["fast", "quality"]
DAY_CANDIDATE_BUILDER_OPTIONS = ["direct_from_slots", "balanced_repeated"]
DIRECT_SLOT_SHORTLIST_SIZE_OPTIONS = [8, 10, 12, 15]

DATASET_OPTIONS = {
    "Pilot current": {
        "dataset_profile": PILOT_CURRENT_PROFILE,
        "recipes_path": DEFAULT_RECIPES_PATH,
        "ingredients_path": DEFAULT_INGREDIENTS_PATH,
        "nutrition_path": DEFAULT_NUTRITION_PATH,
        "is_draft": False,
    },
    "Recipes_DB v1.1 generator-ready draft": {
        "dataset_profile": V1_1_GENERATOR_READY_PROFILE,
        "recipes_path": V1_1_GENERATOR_READY_RECIPES_PATH,
        "ingredients_path": V1_1_GENERATOR_READY_INGREDIENTS_PATH,
        "nutrition_path": V1_1_GENERATOR_READY_NUTRITION_PATH,
        "is_draft": True,
    },
    "Recipes_DB v1.1 generator-ready slot-checked draft": {
        "dataset_profile": V1_1_GENERATOR_READY_SLOT_CHECKED_PROFILE,
        "recipes_path": V1_1_GENERATOR_READY_SLOT_CHECKED_RECIPES_PATH,
        "ingredients_path": V1_1_GENERATOR_READY_SLOT_CHECKED_INGREDIENTS_PATH,
        "nutrition_path": V1_1_GENERATOR_READY_SLOT_CHECKED_NUTRITION_PATH,
        "is_draft": True,
    },
    "Recipes_DB v1.1 generator-ready slot-checked time-enriched draft": {
        "dataset_profile": V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_PROFILE,
        "recipes_path": V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_RECIPES_PATH,
        "ingredients_path": V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_INGREDIENTS_PATH,
        "nutrition_path": V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_NUTRITION_PATH,
        "is_draft": True,
    },
    "Recipes_DB v1.1 generator-ready time-enriched snack-curated draft": {
        "dataset_profile": V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PROFILE,
        "recipes_path": V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_RECIPES_PATH,
        "ingredients_path": V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_INGREDIENTS_PATH,
        "nutrition_path": V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_NUTRITION_PATH,
        "is_draft": True,
    },
    "Recipes_DB v1.1 snack-curated + Round26 plus10 draft": {
        "dataset_profile": V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PLUS10_PROFILE,
        "recipes_path": V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PLUS10_RECIPES_PATH,
        "ingredients_path": V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PLUS10_INGREDIENTS_PATH,
        "nutrition_path": V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PLUS10_NUTRITION_PATH,
        "is_draft": True,
    },
    "Recipes_DB v1.2 Round28 plus30 draft": {
        "dataset_profile": V1_2_GENERATOR_READY_PLUS30_PROFILE,
        "recipes_path": V1_2_GENERATOR_READY_PLUS30_RECIPES_PATH,
        "ingredients_path": V1_2_GENERATOR_READY_PLUS30_INGREDIENTS_PATH,
        "nutrition_path": V1_2_GENERATOR_READY_PLUS30_NUTRITION_PATH,
        "is_draft": True,
    },
    "Recipes_DB v1.2 Round30 plus30+plus15 draft": {
        "dataset_profile": V1_2_GENERATOR_READY_PLUS30_PLUS15_PROFILE,
        "recipes_path": V1_2_GENERATOR_READY_PLUS30_PLUS15_RECIPES_PATH,
        "ingredients_path": V1_2_GENERATOR_READY_PLUS30_PLUS15_INGREDIENTS_PATH,
        "nutrition_path": V1_2_GENERATOR_READY_PLUS30_PLUS15_NUTRITION_PATH,
        "is_draft": True,
    },
}

PIPELINE_STEPS = [
    "profile_loader.py",
    "target_builder.py",
    "data_loader.py",
    "candidate_filter.py",
    "portion_policy.py",
    "slot_candidates.py",
    "recipe_time_adapter.py",
    "time_fit.py",
    "macro_fit.py",
    "slot_fit.py",
    "nutrition_quality.py",
    "meal_realism.py",
    "plan_quality_gate.py",
    "reroll_policy.py",
    "multi_day_selector.py",
    "multi_day_audit.py",
    "pilot_servings_estimator.py",
    "pilot_nutrition_overlay.py",
    "score_preview.py",
    "day_selector.py",
    "day_selector_balanced.py",
    "plan_validator.py",
    "plan_audit.py",
]


def main() -> None:
    st.set_page_config(
        page_title="Generator v1 Test Dashboard",
        layout="wide",
    )
    _render_styles()
    _ensure_session_state()

    pipeline_col, content_col = st.columns([0.85, 3.15], gap="large")
    with pipeline_col:
        _render_pipeline()

    with content_col:
        st.title("TableTogether / Generator v1 Test Dashboard")
        dataset_config = _render_dataset_selector()
        selection_mode = _render_selection_mode_selector()
        alternative_count, diversity_mode = _render_alternative_controls(selection_mode)
        portion_policy = _render_portion_policy_selector()
        meal_realism_mode = _render_meal_realism_selector()
        quality_gate = _render_quality_gate_selector()
        multi_day_mode = _render_multi_day_mode_selector()
        no_repeat_policy = _render_multi_day_no_repeat_policy_selector()
        day_candidate_pool_size = _render_day_candidate_pool_size_selector()
        multi_day_speed_mode = _render_multi_day_speed_mode_selector()
        day_candidate_builder = _render_day_candidate_builder_selector()
        direct_slot_shortlist_size = _render_direct_slot_shortlist_size_selector()
        active_config = _current_generation_config(dataset_config)
        _render_active_generation_config(active_config)
        button_cols = st.columns([1.0, 1.0, 1.0, 1.0, 1.8])
        button_cols[0].button(
            "Generate 1 day",
            type="primary",
            on_click=_generate_and_store_one_day_menu,
            use_container_width=True,
        )
        button_cols[1].button(
            "Generate 3 days",
            type="secondary",
            on_click=_generate_and_store_three_day_plan,
            use_container_width=True,
        )
        button_cols[2].button(
            "Generate best",
            type="secondary",
            on_click=_generate_and_store_best_menu,
            use_container_width=True,
        )
        button_cols[3].button(
            "Generate varied",
            type="secondary",
            on_click=_generate_and_store_varied_menu,
            use_container_width=True,
        )
        button_cols[4].button(
            "Clear generated menu history",
            type="secondary",
            on_click=_clear_generated_menu_history,
            use_container_width=True,
        )
        st.caption(f"Selected dataset profile: {dataset_config['dataset_profile']}")
        st.caption(f"Selection mode: {selection_mode}")
        st.caption(f"Portion policy: {portion_policy}")
        st.caption(f"Meal realism mode: {meal_realism_mode}")
        st.caption(f"Quality gate: {quality_gate}")
        st.caption(f"Alternatives: {alternative_count}; diversity: {diversity_mode}")
        st.caption(f"3-day mode: {multi_day_mode}")
        st.caption(
            "3-day no-repeat: "
            f"{no_repeat_policy}; candidate pool target: {day_candidate_pool_size}; "
            f"speed mode: {multi_day_speed_mode}; builder: {day_candidate_builder}; "
            f"shortlist: {direct_slot_shortlist_size}"
        )
        st.info(
            "Generate 1 day behaves like the current best one-day generation. "
            "Generate best uses the selected diversity mode. "
            "diversity_mode=none is deterministic and may repeat the same menu. "
            "diversity_mode=soft can use recent recipes with a small penalty when history exists. "
            "Generate varied uses quality-gated reroll when demo_safe is active. "
            "Generate 3 days uses the selected draft multi-day selector."
        )
        if multi_day_speed_mode == "quality":
            st.warning("Quality mode can take multiple minutes for 3-day generation.")
        if day_candidate_builder == "balanced_repeated":
            st.warning("Balanced repeated builder is the preserved quality fallback and may take minutes.")
        _render_recent_recipe_debug(
            st.session_state.get(SESSION_MENUS_KEY, [])
        )
        if dataset_config["is_draft"]:
            st.warning("Recipes_DB v1.1 generator-ready is draft/test data, not current production.")
        if (
            dataset_config["dataset_profile"]
            in {
                V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PROFILE,
                V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PLUS10_PROFILE,
                V1_2_GENERATOR_READY_PLUS30_PROFILE,
                V1_2_GENERATOR_READY_PLUS30_PLUS15_PROFILE,
            }
        ):
            st.info(
                "Recommended for generator-ready draft testing: Balanced day + target_aware "
                "+ practical realism + 3 alternatives."
            )

        generated_menus = st.session_state[SESSION_MENUS_KEY]
        latest_multi_day = st.session_state.get(SESSION_LATEST_MULTIDAY_KEY)
        if not generated_menus and not latest_multi_day:
            st.info("No generated menu yet.")
            return

        if latest_multi_day:
            _render_multi_day_plan("Latest 3-day draft plan", latest_multi_day)
            st.divider()

        if not generated_menus:
            st.info("No generated one-day menu yet.")
            return

        st.caption(f"Menus stored in this Streamlit session: {len(generated_menus)}")
        latest = st.session_state.get(SESSION_LATEST_MENU_KEY) or generated_menus[0]
        if _menu_config_differs(latest, active_config):
            st.warning("Latest menu was generated with a different config.")
        _render_plan("Latest menu", latest, enable_feedback=True)
        st.subheader("Copy latest menu")
        st.code(_menu_as_text(latest), language=None)
        _render_feedback_events()
        _render_session_menu_history(generated_menus)

        if len(generated_menus) > 1:
            st.divider()
            _render_plan("Previous menu", generated_menus[1], enable_feedback=False)


def _render_dataset_selector() -> dict[str, Any]:
    labels = list(DATASET_OPTIONS)
    current_profile = st.session_state.get(SESSION_DATASET_PROFILE_KEY, PILOT_CURRENT_PROFILE)
    current_label = _label_for_dataset_profile(current_profile)
    selected_label = st.selectbox(
        "Dataset",
        labels,
        index=labels.index(current_label),
        help="Alege sursa de retete folosita la urmatoarea generare.",
    )
    selected_config = DATASET_OPTIONS[selected_label]
    selected_profile = str(selected_config["dataset_profile"])
    if selected_profile != current_profile:
        st.session_state[SESSION_DATASET_PROFILE_KEY] = selected_profile
        _apply_dataset_recommendations(selected_profile)
    return selected_config


def _apply_dataset_recommendations(dataset_profile: str) -> None:
    if dataset_profile == PILOT_CURRENT_PROFILE:
        st.session_state[SESSION_SELECTION_MODE_KEY] = "greedy"
        st.session_state[SESSION_ALTERNATIVE_COUNT_KEY] = 1
        st.session_state[SESSION_DIVERSITY_MODE_KEY] = "none"
        st.session_state[SESSION_PORTION_POLICY_KEY] = "standard"
        st.session_state[SESSION_MEAL_REALISM_MODE_KEY] = "off"
        st.session_state[SESSION_QUALITY_GATE_KEY] = "off"
        st.session_state[SESSION_MULTIDAY_MODE_KEY] = MULTI_DAY_MODE_SIMPLE
        st.session_state[SESSION_MULTIDAY_NO_REPEAT_POLICY_KEY] = "prefer"
        st.session_state[SESSION_DAY_CANDIDATE_POOL_SIZE_KEY] = 75
        st.session_state[SESSION_MULTIDAY_SPEED_MODE_KEY] = "fast"
        st.session_state[SESSION_DAY_CANDIDATE_BUILDER_KEY] = "direct_from_slots"
        st.session_state[SESSION_DIRECT_SLOT_SHORTLIST_SIZE_KEY] = 12
        return
    if dataset_profile == V1_2_GENERATOR_READY_PLUS30_PLUS15_PROFILE:
        st.session_state[SESSION_MULTIDAY_NO_REPEAT_POLICY_KEY] = "hard"
        st.session_state[SESSION_DAY_CANDIDATE_POOL_SIZE_KEY] = 50
        st.session_state[SESSION_MULTIDAY_SPEED_MODE_KEY] = "fast"
        st.session_state[SESSION_DAY_CANDIDATE_BUILDER_KEY] = "direct_from_slots"
        st.session_state[SESSION_DIRECT_SLOT_SHORTLIST_SIZE_KEY] = 12
    if dataset_profile in {
        V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PROFILE,
        V1_1_GENERATOR_READY_SLOT_CHECKED_TIME_ENRICHED_SNACK_CURATED_PLUS10_PROFILE,
        V1_2_GENERATOR_READY_PLUS30_PROFILE,
        V1_2_GENERATOR_READY_PLUS30_PLUS15_PROFILE,
    }:
        st.session_state[SESSION_SELECTION_MODE_KEY] = V1_1_RECOMMENDED_SELECTION_MODE
        st.session_state[SESSION_ALTERNATIVE_COUNT_KEY] = V1_1_RECOMMENDED_ALTERNATIVE_COUNT
        st.session_state[SESSION_DIVERSITY_MODE_KEY] = V1_1_RECOMMENDED_DIVERSITY_MODE
        st.session_state[SESSION_PORTION_POLICY_KEY] = V1_1_RECOMMENDED_PORTION_POLICY
        st.session_state[SESSION_MEAL_REALISM_MODE_KEY] = V1_1_RECOMMENDED_MEAL_REALISM_MODE
        st.session_state[SESSION_QUALITY_GATE_KEY] = V1_1_RECOMMENDED_QUALITY_GATE
        st.session_state[SESSION_MULTIDAY_MODE_KEY] = MULTI_DAY_MODE_GLOBAL
        if dataset_profile != V1_2_GENERATOR_READY_PLUS30_PLUS15_PROFILE:
            st.session_state[SESSION_MULTIDAY_NO_REPEAT_POLICY_KEY] = "prefer"
            st.session_state[SESSION_DAY_CANDIDATE_POOL_SIZE_KEY] = 75
            st.session_state[SESSION_MULTIDAY_SPEED_MODE_KEY] = "fast"
            st.session_state[SESSION_DAY_CANDIDATE_BUILDER_KEY] = "direct_from_slots"
            st.session_state[SESSION_DIRECT_SLOT_SHORTLIST_SIZE_KEY] = 12


def _render_selection_mode_selector() -> str:
    labels = list(SELECTION_MODE_OPTIONS)
    current_mode = st.session_state.get(SESSION_SELECTION_MODE_KEY, "greedy")
    current_label = _label_for_selection_mode(current_mode)
    selected_label = st.selectbox(
        "Selection mode",
        labels,
        index=labels.index(current_label),
        help="Greedy pastreaza comportamentul curent; Balanced day optimizeaza macro-urile la nivel de zi.",
    )
    selected_mode = SELECTION_MODE_OPTIONS[selected_label]
    if selected_mode != current_mode:
        st.session_state[SESSION_SELECTION_MODE_KEY] = selected_mode
    return selected_mode


def _render_alternative_controls(selection_mode: str) -> tuple[int, str]:
    current_count = int(st.session_state.get(SESSION_ALTERNATIVE_COUNT_KEY, 1))
    if current_count not in ALTERNATIVE_COUNT_OPTIONS:
        current_count = 1
    selected_count = st.selectbox(
        "Alternative count",
        ALTERNATIVE_COUNT_OPTIONS,
        index=ALTERNATIVE_COUNT_OPTIONS.index(current_count),
        disabled=selection_mode != "balanced_day",
        help="Pentru Balanced day, genereaza pana la 3 variante deterministe.",
    )

    current_mode = str(st.session_state.get(SESSION_DIVERSITY_MODE_KEY, "none"))
    if current_mode not in DIVERSITY_MODE_OPTIONS:
        current_mode = "none"
    selected_mode = st.selectbox(
        "Diversity mode",
        DIVERSITY_MODE_OPTIONS,
        index=DIVERSITY_MODE_OPTIONS.index(current_mode),
        disabled=selection_mode != "balanced_day",
        help="avoid_recent aplica o penalizare mica pentru retetele din ultimele doua meniuri.",
    )
    if selection_mode != "balanced_day":
        selected_count = 1
        selected_mode = "none"
    if (
        selected_count != st.session_state.get(SESSION_ALTERNATIVE_COUNT_KEY)
        or selected_mode != st.session_state.get(SESSION_DIVERSITY_MODE_KEY)
    ):
        st.session_state[SESSION_ALTERNATIVE_COUNT_KEY] = selected_count
        st.session_state[SESSION_DIVERSITY_MODE_KEY] = selected_mode
    return int(selected_count), str(selected_mode)


def _render_portion_policy_selector() -> str:
    current_policy = str(st.session_state.get(SESSION_PORTION_POLICY_KEY, "standard"))
    if current_policy not in PORTION_POLICY_OPTIONS:
        current_policy = "standard"
    selected_policy = st.selectbox(
        "Portion policy",
        PORTION_POLICY_OPTIONS,
        index=PORTION_POLICY_OPTIONS.index(current_policy),
        help="standard pastreaza multiplicatorii curenti; target_aware este doar pentru testare v1.1.",
    )
    if selected_policy != current_policy:
        st.session_state[SESSION_PORTION_POLICY_KEY] = selected_policy
    return str(selected_policy)


def _render_meal_realism_selector() -> str:
    current_mode = _current_meal_realism_mode()
    selected_mode = st.selectbox(
        "Meal realism mode",
        MEAL_REALISM_MODE_OPTIONS,
        index=MEAL_REALISM_MODE_OPTIONS.index(current_mode),
        help=(
            "audit doar raporteaza; soft adauga o penalizare mica; "
            "practical filtreaza mesele clar nerealiste pe slot."
        ),
    )
    if selected_mode != current_mode:
        st.session_state[SESSION_MEAL_REALISM_MODE_KEY] = selected_mode
    return str(selected_mode)


def _render_quality_gate_selector() -> str:
    current_mode = _current_quality_gate()
    selected_mode = st.selectbox(
        "Quality gate",
        QUALITY_GATE_OPTIONS,
        index=QUALITY_GATE_OPTIONS.index(current_mode),
        help="demo_safe refuza reroll-urile care strica prea mult macro sau realismul.",
    )
    if selected_mode != current_mode:
        st.session_state[SESSION_QUALITY_GATE_KEY] = selected_mode
    return str(selected_mode)


def _render_multi_day_mode_selector() -> str:
    current_mode = _current_multi_day_mode()
    selected_mode = st.selectbox(
        "3-day mode",
        MULTI_DAY_MODE_OPTIONS,
        index=MULTI_DAY_MODE_OPTIONS.index(current_mode),
        help=(
            "global_alternatives_3_day incearca mai multe zile candidate si "
            "alege combinatia cu repetitii mai putine."
        ),
    )
    if selected_mode != current_mode:
        st.session_state[SESSION_MULTIDAY_MODE_KEY] = selected_mode
    return str(selected_mode)


def _render_multi_day_no_repeat_policy_selector() -> str:
    current_policy = _current_multi_day_no_repeat_policy()
    selected_policy = st.selectbox(
        "3-day no-repeat policy",
        MULTI_DAY_NO_REPEAT_POLICY_OPTIONS,
        index=MULTI_DAY_NO_REPEAT_POLICY_OPTIONS.index(current_policy),
        help="hard cere fara retete repetate daca exista combinatie valida; main_only aplica regula doar la lunch/dinner.",
    )
    if selected_policy != current_policy:
        st.session_state[SESSION_MULTIDAY_NO_REPEAT_POLICY_KEY] = selected_policy
    return str(selected_policy)


def _render_day_candidate_pool_size_selector() -> int:
    current_size = _current_day_candidate_pool_size()
    selected_size = st.selectbox(
        "3-day candidate pool target",
        DAY_CANDIDATE_POOL_SIZE_OPTIONS,
        index=DAY_CANDIDATE_POOL_SIZE_OPTIONS.index(current_size),
        help="Pool mai mare inseamna sanse mai bune la no-repeat, dar generatie mai lenta.",
    )
    if int(selected_size) != current_size:
        st.session_state[SESSION_DAY_CANDIDATE_POOL_SIZE_KEY] = int(selected_size)
    return int(selected_size)


def _render_multi_day_speed_mode_selector() -> str:
    current_mode = _current_multi_day_speed_mode()
    selected_mode = st.selectbox(
        "3-day speed mode",
        MULTI_DAY_SPEED_MODE_OPTIONS,
        index=MULTI_DAY_SPEED_MODE_OPTIONS.index(current_mode),
        help="fast reduce pool-ul intern; quality pastreaza calea Round32, dar poate dura minute.",
    )
    if selected_mode != current_mode:
        st.session_state[SESSION_MULTIDAY_SPEED_MODE_KEY] = selected_mode
    return str(selected_mode)


def _render_day_candidate_builder_selector() -> str:
    current_builder = _current_day_candidate_builder()
    selected_builder = st.selectbox(
        "3-day candidate builder",
        DAY_CANDIDATE_BUILDER_OPTIONS,
        index=DAY_CANDIDATE_BUILDER_OPTIONS.index(current_builder),
        help=(
            "direct_from_slots construieste zile din candidatii pe slot deja calculati; "
            "balanced_repeated pastreaza calea Round32, dar este lenta."
        ),
    )
    if selected_builder != current_builder:
        st.session_state[SESSION_DAY_CANDIDATE_BUILDER_KEY] = selected_builder
    return str(selected_builder)


def _render_direct_slot_shortlist_size_selector() -> int:
    current_size = _current_direct_slot_shortlist_size()
    selected_size = st.selectbox(
        "Direct slot shortlist",
        DIRECT_SLOT_SHORTLIST_SIZE_OPTIONS,
        index=DIRECT_SLOT_SHORTLIST_SIZE_OPTIONS.index(current_size),
        help="Numar de candidati pastrati pe fiecare slot in builder-ul direct.",
    )
    if int(selected_size) != current_size:
        st.session_state[SESSION_DIRECT_SLOT_SHORTLIST_SIZE_KEY] = int(selected_size)
    return int(selected_size)


def _current_dataset_config() -> dict[str, Any]:
    profile = st.session_state.get(SESSION_DATASET_PROFILE_KEY, PILOT_CURRENT_PROFILE)
    label = _label_for_dataset_profile(profile)
    return DATASET_OPTIONS[label]


def _current_selection_mode() -> str:
    return str(st.session_state.get(SESSION_SELECTION_MODE_KEY, "greedy"))


def _current_alternative_count() -> int:
    value = int(st.session_state.get(SESSION_ALTERNATIVE_COUNT_KEY, 1))
    return value if value in ALTERNATIVE_COUNT_OPTIONS else 1


def _current_diversity_mode() -> str:
    value = str(st.session_state.get(SESSION_DIVERSITY_MODE_KEY, "none"))
    return value if value in DIVERSITY_MODE_OPTIONS else "none"


def _current_portion_policy() -> str:
    value = str(st.session_state.get(SESSION_PORTION_POLICY_KEY, "standard"))
    return value if value in PORTION_POLICY_OPTIONS else "standard"


def _current_meal_realism_mode() -> str:
    value = str(st.session_state.get(SESSION_MEAL_REALISM_MODE_KEY, "off"))
    return value if value in MEAL_REALISM_MODE_OPTIONS else "off"


def _current_quality_gate() -> str:
    value = str(st.session_state.get(SESSION_QUALITY_GATE_KEY, "off"))
    return value if value in QUALITY_GATE_OPTIONS else "off"


def _current_multi_day_mode() -> str:
    value = str(st.session_state.get(SESSION_MULTIDAY_MODE_KEY, MULTI_DAY_MODE_GLOBAL))
    return value if value in MULTI_DAY_MODE_OPTIONS else MULTI_DAY_MODE_GLOBAL


def _current_multi_day_no_repeat_policy() -> str:
    value = str(st.session_state.get(SESSION_MULTIDAY_NO_REPEAT_POLICY_KEY, "prefer"))
    if value in MULTI_DAY_NO_REPEAT_POLICY_OPTIONS:
        return value
    return "prefer"


def _current_day_candidate_pool_size() -> int:
    try:
        value = int(st.session_state.get(SESSION_DAY_CANDIDATE_POOL_SIZE_KEY, 75))
    except (TypeError, ValueError):
        value = 75
    return value if value in DAY_CANDIDATE_POOL_SIZE_OPTIONS else 75


def _current_multi_day_speed_mode() -> str:
    value = str(st.session_state.get(SESSION_MULTIDAY_SPEED_MODE_KEY, "fast"))
    if value in MULTI_DAY_SPEED_MODE_OPTIONS:
        return value
    return "fast"


def _current_day_candidate_builder() -> str:
    value = str(
        st.session_state.get(
            SESSION_DAY_CANDIDATE_BUILDER_KEY,
            "direct_from_slots",
        )
    )
    if value in DAY_CANDIDATE_BUILDER_OPTIONS:
        return value
    return "direct_from_slots"


def _current_direct_slot_shortlist_size() -> int:
    try:
        value = int(st.session_state.get(SESSION_DIRECT_SLOT_SHORTLIST_SIZE_KEY, 12))
    except (TypeError, ValueError):
        value = 12
    return value if value in DIRECT_SLOT_SHORTLIST_SIZE_OPTIONS else 12


def _current_generation_config(
    dataset_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    config = dataset_config or _current_dataset_config()
    return {
        "dataset_profile": str(config["dataset_profile"]),
        "selection_mode": _current_selection_mode(),
        "portion_policy": _current_portion_policy(),
        "meal_realism_mode": _current_meal_realism_mode(),
        "quality_gate": _current_quality_gate(),
        "alternative_count": _current_alternative_count(),
        "diversity_mode": _current_diversity_mode(),
        "multi_day_mode": _current_multi_day_mode(),
        "multi_day_no_repeat_policy": _current_multi_day_no_repeat_policy(),
        "day_candidate_pool_size": _current_day_candidate_pool_size(),
        "multi_day_speed_mode": _current_multi_day_speed_mode(),
        "day_candidate_builder": _current_day_candidate_builder(),
        "direct_slot_shortlist_size": _current_direct_slot_shortlist_size(),
    }


def _render_active_generation_config(config: dict[str, Any]) -> None:
    st.markdown("#### Active generation config")
    st.dataframe(
        pd.DataFrame([config]),
        use_container_width=True,
        hide_index=True,
    )


def _menu_generation_config(menu: dict[str, Any]) -> dict[str, Any]:
    stored_config = menu.get("generation_config_used") or menu.get("generation_config")
    if isinstance(stored_config, dict) and stored_config:
        return _normalise_generation_config(stored_config)

    pool_summary = menu.get("pool_summary", {})
    if not isinstance(pool_summary, dict) or not pool_summary:
        return {}
    return _normalise_generation_config(
        {
            "dataset_profile": pool_summary.get("dataset_profile"),
            "selection_mode": pool_summary.get("selection_mode"),
            "portion_policy": pool_summary.get("portion_policy"),
            "meal_realism_mode": pool_summary.get("meal_realism_mode"),
            "quality_gate": pool_summary.get("quality_gate"),
            "alternative_count": pool_summary.get("alternative_count"),
            "diversity_mode": pool_summary.get("diversity_mode"),
            "multi_day_mode": pool_summary.get("multi_day_mode"),
            "multi_day_no_repeat_policy": pool_summary.get(
                "multi_day_no_repeat_policy"
            ),
            "day_candidate_pool_size": pool_summary.get("day_candidate_pool_size"),
            "multi_day_speed_mode": pool_summary.get("multi_day_speed_mode"),
            "day_candidate_builder": pool_summary.get("day_candidate_builder"),
            "direct_slot_shortlist_size": pool_summary.get(
                "direct_slot_shortlist_size"
            ),
        }
    )


def _normalise_generation_config(config: dict[str, Any]) -> dict[str, Any]:
    normalised = {
        "dataset_profile": str(config.get("dataset_profile", "")),
        "selection_mode": str(config.get("selection_mode", "")),
        "portion_policy": str(config.get("portion_policy", "")),
        "meal_realism_mode": str(config.get("meal_realism_mode", "")),
        "quality_gate": str(config.get("quality_gate", "")),
        "diversity_mode": str(config.get("diversity_mode", "")),
        "multi_day_mode": str(config.get("multi_day_mode", "")),
        "multi_day_no_repeat_policy": str(
            config.get("multi_day_no_repeat_policy", "")
        ),
        "multi_day_speed_mode": str(config.get("multi_day_speed_mode", "")),
        "day_candidate_builder": str(config.get("day_candidate_builder", "")),
    }
    try:
        normalised["alternative_count"] = int(config.get("alternative_count", 1))
    except (TypeError, ValueError):
        normalised["alternative_count"] = 1
    try:
        normalised["day_candidate_pool_size"] = int(
            config.get("day_candidate_pool_size", 75)
        )
    except (TypeError, ValueError):
        normalised["day_candidate_pool_size"] = 75
    try:
        normalised["direct_slot_shortlist_size"] = int(
            config.get("direct_slot_shortlist_size", 12)
        )
    except (TypeError, ValueError):
        normalised["direct_slot_shortlist_size"] = 12
    return normalised


def _menu_config_differs(
    menu: dict[str, Any],
    active_config: dict[str, Any],
) -> bool:
    stored_config = _menu_generation_config(menu)
    if not stored_config:
        return True
    return stored_config != _normalise_generation_config(active_config)


def _render_recent_recipe_debug(generated_menus: list[dict[str, Any]]) -> None:
    recent_rows = _recent_recipe_details(generated_menus)
    recent_ids = sorted({str(row["recipe_id"]) for row in recent_rows})
    st.caption(
        "Recent recipe ids available from last "
        f"{RECENT_MENUS_FOR_VARIATION} menus: {len(recent_ids)}"
    )
    if not recent_rows:
        st.caption("Generate varied has no recent menu history yet, so it behaves like a first generation.")
        return
    with st.expander("Recent recipes available for varied generation", expanded=False):
        st.dataframe(
            pd.DataFrame(recent_rows),
            use_container_width=True,
            hide_index=True,
        )


def _label_for_dataset_profile(profile: object) -> str:
    profile_text = str(profile or PILOT_CURRENT_PROFILE)
    for label, config in DATASET_OPTIONS.items():
        if config["dataset_profile"] == profile_text:
            return label
    return "Pilot current"


def _label_for_selection_mode(mode: object) -> str:
    mode_text = str(mode or "greedy")
    for label, value in SELECTION_MODE_OPTIONS.items():
        if value == mode_text:
            return label
    return "Greedy"


def _generate_and_store_best_menu() -> None:
    _generate_and_store_latest_menu(
        forced_diversity_mode=None,
        generation_trigger="best",
    )


def _generate_and_store_one_day_menu() -> None:
    _generate_and_store_latest_menu(
        forced_diversity_mode=None,
        generation_trigger="one_day",
    )


def _generate_and_store_varied_menu() -> None:
    _generate_and_store_latest_menu(
        forced_diversity_mode="avoid_recent",
        generation_trigger="varied",
    )


def _generate_and_store_three_day_plan() -> None:
    run_counter = int(st.session_state.get(SESSION_RUN_COUNTER_KEY, 0)) + 1
    st.session_state[SESSION_RUN_COUNTER_KEY] = run_counter
    generated_at = datetime.now().isoformat(timespec="seconds")
    latest_plan = _build_multi_day_result(
        run_id=f"streamlit-{run_counter}",
        generated_at=generated_at,
    )
    stored_plans = st.session_state.get(SESSION_MULTIDAY_PLANS_KEY, [])
    updated_plans = [latest_plan, *stored_plans][:10]
    st.session_state[SESSION_MULTIDAY_PLANS_KEY] = updated_plans
    st.session_state[SESSION_LATEST_MULTIDAY_KEY] = latest_plan


def _generate_and_store_latest_menu(
    forced_diversity_mode: str | None = None,
    generation_trigger: str = "best",
) -> None:
    stored_menus = st.session_state.get(SESSION_MENUS_KEY, [])
    available_recent_recipe_ids = _recent_recipe_ids(stored_menus)
    diversity_mode_used = forced_diversity_mode or _current_diversity_mode()
    recent_recipe_ids = (
        available_recent_recipe_ids
        if diversity_mode_used in {"soft", "avoid_recent"}
        else set()
    )
    recent_recipe_details = _recent_recipe_details(stored_menus)
    run_counter = int(st.session_state.get(SESSION_RUN_COUNTER_KEY, 0)) + 1
    st.session_state[SESSION_RUN_COUNTER_KEY] = run_counter
    generated_at = datetime.now().isoformat(timespec="seconds")
    latest_menu = _build_generator_result(
        recent_recipe_ids=recent_recipe_ids,
        recent_recipe_details=recent_recipe_details
        if recent_recipe_ids
        else [],
        diversity_mode_override=diversity_mode_used,
        generation_trigger=generation_trigger,
        run_id=f"streamlit-{run_counter}",
        generated_at=generated_at,
    )
    updated_menus = [latest_menu, *stored_menus][:20]
    st.session_state[SESSION_MENUS_KEY] = updated_menus
    st.session_state[SESSION_LATEST_MENU_KEY] = latest_menu
    st.session_state[SESSION_RECENT_RECIPE_IDS_KEY] = sorted(
        _recent_recipe_ids(updated_menus)
    )


def _clear_generated_menu_history() -> None:
    st.session_state[SESSION_MENUS_KEY] = []
    st.session_state[SESSION_LATEST_MENU_KEY] = None
    st.session_state[SESSION_MULTIDAY_PLANS_KEY] = []
    st.session_state[SESSION_LATEST_MULTIDAY_KEY] = None
    st.session_state[SESSION_RECENT_RECIPE_IDS_KEY] = []
    st.session_state[SESSION_RUN_COUNTER_KEY] = 0


def _build_multi_day_result(
    run_id: str | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    dataset_config = _current_dataset_config()
    multi_day_mode = _current_multi_day_mode()
    no_repeat_policy = _current_multi_day_no_repeat_policy()
    day_candidate_pool_size = _current_day_candidate_pool_size()
    multi_day_speed_mode = _current_multi_day_speed_mode()
    day_candidate_builder = _current_day_candidate_builder()
    direct_slot_shortlist_size = _current_direct_slot_shortlist_size()
    generated_at = generated_at or datetime.now().isoformat(timespec="seconds")
    run_id = run_id or f"streamlit-{generated_at}"
    generation_started = time.perf_counter()
    profile = load_member_profile(PROFILE_PATH)
    target = build_nutrition_target(profile)
    pool = load_recipe_candidate_pool(
        recipes_path=dataset_config["recipes_path"],
        ingredients_path=dataset_config["ingredients_path"],
        nutrition_path=dataset_config["nutrition_path"],
        dataset_profile=str(dataset_config["dataset_profile"]),
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
    candidate_diagnostics = build_candidate_diagnostics(
        slot_candidates=slot_candidates,
        slot_targets=target.slot_targets,
    )
    nutrition_cache_diagnostics = build_nutrition_cache_diagnostics(
        recipes=pool.recipes,
        nutrition=pool.nutrition,
        candidates=pool.candidates,
        eligible_candidates=pool.eligible_candidates,
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
            "multi_day_mode": multi_day_mode,
            "candidate_day_alternative_count": 10,
            "global_max_candidates_per_slot": 26,
            "day_candidate_pool_size_target": day_candidate_pool_size,
            "day_candidate_pool_max": max(150, day_candidate_pool_size),
            "include_slot_forced_variants": True,
            "no_repeat_policy": no_repeat_policy,
            "multi_day_speed_mode": multi_day_speed_mode,
            "day_candidate_builder": day_candidate_builder,
            "direct_slot_shortlist_size": direct_slot_shortlist_size,
        },
    )
    generation_seconds = round(time.perf_counter() - generation_started, 3)
    generation_config = {
        "dataset_profile": str(dataset_config["dataset_profile"]),
        "selection_mode": "balanced_day",
        "portion_policy": "target_aware",
        "meal_realism_mode": "practical",
        "quality_gate": "demo_safe",
        "alternative_count": 3,
        "diversity_mode": "quality_gated_multi_day",
        "days": 3,
        "multi_day_mode": multi_day_mode,
        "multi_day_no_repeat_policy": no_repeat_policy,
        "day_candidate_pool_size": day_candidate_pool_size,
        "multi_day_speed_mode": multi_day_speed_mode,
        "day_candidate_builder": day_candidate_builder,
        "direct_slot_shortlist_size": direct_slot_shortlist_size,
    }
    plan["run_id"] = run_id
    plan["generated_at"] = generated_at
    plan["generation_trigger"] = "three_day"
    plan["generation_config"] = generation_config
    plan["generation_config_used"] = generation_config
    plan["generation_runtime_seconds"] = generation_seconds
    plan["candidate_diagnostics"] = candidate_diagnostics
    plan["nutrition_cache_diagnostics"] = nutrition_cache_diagnostics
    plan["pool_summary"] = {
        "run_id": run_id,
        "generated_at": generated_at,
        "generation_trigger": "three_day",
        "dataset_profile": dataset_config["dataset_profile"],
        "recipes_path": str(dataset_config["recipes_path"]),
        "ingredients_path": str(dataset_config["ingredients_path"]),
        "nutrition_path": str(dataset_config["nutrition_path"]),
        "total_recipes_loaded": len(pool.candidates),
        "eligible_candidate_count": len(pool.eligible_candidates),
        "filtered_candidate_count": len(filtered_candidates),
        "slot_candidate_count": len(slot_candidates),
        "selection_mode": "balanced_day",
        "portion_policy": "target_aware",
        "meal_realism_mode": "practical",
        "quality_gate": "demo_safe",
        "alternative_count": 3,
        "days": 3,
        "multi_day_mode": multi_day_mode,
        "multi_day_no_repeat_policy": no_repeat_policy,
        "day_candidate_pool_size": day_candidate_pool_size,
        "multi_day_speed_mode": multi_day_speed_mode,
        "day_candidate_builder": day_candidate_builder,
        "direct_slot_shortlist_size": direct_slot_shortlist_size,
        "generation_runtime_seconds": generation_seconds,
        "loader_warnings": pool.loader_diagnostics.get("warnings", []),
    }
    return plan


def _build_generator_result(
    recent_recipe_ids: set[str] | None = None,
    recent_recipe_details: list[dict[str, Any]] | None = None,
    diversity_mode_override: str | None = None,
    generation_trigger: str = "best",
    run_id: str | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    dataset_config = _current_dataset_config()
    selection_mode = _current_selection_mode()
    alternative_count = _current_alternative_count()
    diversity_mode = diversity_mode_override or _current_diversity_mode()
    portion_policy = _current_portion_policy()
    meal_realism_mode = _current_meal_realism_mode()
    quality_gate = _current_quality_gate()
    generation_config = _current_generation_config(dataset_config)
    generation_config["diversity_mode"] = diversity_mode
    generated_at = generated_at or datetime.now().isoformat(timespec="seconds")
    run_id = run_id or f"streamlit-{generated_at}"
    profile = load_member_profile(PROFILE_PATH)
    target = build_nutrition_target(profile)
    pool = load_recipe_candidate_pool(
        recipes_path=dataset_config["recipes_path"],
        ingredients_path=dataset_config["ingredients_path"],
        nutrition_path=dataset_config["nutrition_path"],
        dataset_profile=str(dataset_config["dataset_profile"]),
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
        portion_policy_mode=portion_policy,
    )
    candidate_diagnostics = build_candidate_diagnostics(
        slot_candidates=slot_candidates,
        slot_targets=target.slot_targets,
    )
    nutrition_cache_diagnostics = build_nutrition_cache_diagnostics(
        recipes=pool.recipes,
        nutrition=pool.nutrition,
        candidates=pool.candidates,
        eligible_candidates=pool.eligible_candidates,
    )
    selection_candidates = (
        slot_candidates
        if selection_mode == "balanced_day"
        else _without_recipe_ids(slot_candidates, recent_recipe_ids or set())
    )
    slot_order = _slot_order(target)
    selector_config = _balanced_selector_config(
            alternative_count=alternative_count,
            diversity_mode=diversity_mode,
            recent_recipe_ids=recent_recipe_ids or set(),
            meal_realism_mode=meal_realism_mode,
        )
    if (
        generation_trigger == "varied"
        and quality_gate == "demo_safe"
        and selection_mode == "balanced_day"
    ):
        plan = select_quality_gated_reroll(
            slot_candidates_by_slot=_slot_candidates_by_slot(
                selection_candidates,
                slot_order,
            ),
            target=target,
            slot_order=slot_order,
            recent_recipe_ids=recent_recipe_ids or set(),
            base_config=selector_config,
        )
        diversity_mode = str(plan.get("quality_gate_selected_mode", diversity_mode))
        generation_config["diversity_mode"] = diversity_mode
    else:
        plan = _select_one_day_plan(
            selection_mode=selection_mode,
            slot_candidates=selection_candidates,
            target=target,
            slot_order=slot_order,
            selector_config=selector_config,
        )
    ingredient_diagnostics = build_ingredient_diagnostics(
        recipes_df=pool.recipes,
        ingredients_df=pool.ingredients,
        nutrition_df=pool.nutrition,
        selected_recipe_ids=_selected_recipe_ids(plan),
    )
    pilot_servings_diagnostics = build_pilot_servings_diagnostics(
        recipes_df=pool.recipes,
        ingredients_df=pool.ingredients,
        nutrition_df=pool.nutrition,
        eligible_candidates=pool.eligible_candidates,
        selected_recipe_ids=_selected_recipe_ids(plan),
    )
    plan["target"] = _target_to_dict(target)
    plan["candidate_diagnostics"] = candidate_diagnostics
    plan["nutrition_cache_diagnostics"] = nutrition_cache_diagnostics
    plan["ingredient_diagnostics"] = ingredient_diagnostics
    plan["pilot_servings_diagnostics"] = pilot_servings_diagnostics
    plan["validation"] = validate_one_day_plan(plan, target)
    if quality_gate == "demo_safe" and "quality_gate" not in plan:
        plan["quality_gate"] = evaluate_plan_quality(
            plan,
            target,
            config={"quality_gate": "demo_safe"},
        )
        plan["quality_gate_status"] = plan["quality_gate"]["quality_gate_status"]
        plan["quality_gate_reasons"] = plan["quality_gate"]["quality_gate_reasons"]
        plan["quality_gate_score"] = plan["quality_gate"]["quality_gate_score"]
        plan["quality_gate_fallback_used"] = False
        plan["quality_gate_selected_mode"] = diversity_mode
    plan["run_id"] = run_id
    plan["generated_at"] = generated_at
    plan["generation_trigger"] = generation_trigger
    plan["generation_config"] = generation_config
    plan["generation_config_used"] = generation_config
    plan["diversity_mode_used"] = diversity_mode
    plan["recent_recipe_ids_used"] = sorted(recent_recipe_ids or set())
    plan["recent_recipe_count_used"] = len(recent_recipe_ids or set())
    plan["recent_recipe_details_used"] = recent_recipe_details or []
    plan["pool_summary"] = {
        "run_id": run_id,
        "generated_at": generated_at,
        "generation_trigger": generation_trigger,
        "dataset_profile": dataset_config["dataset_profile"],
        "recipes_path": str(dataset_config["recipes_path"]),
        "ingredients_path": str(dataset_config["ingredients_path"]),
        "nutrition_path": str(dataset_config["nutrition_path"]),
        "total_recipes_loaded": len(pool.candidates),
        "eligible_candidate_count": len(pool.eligible_candidates),
        "filtered_candidate_count": len(filtered_candidates),
        "slot_candidate_count": len(slot_candidates),
        "selection_mode": selection_mode,
        "portion_policy": portion_policy,
        "meal_realism_mode": meal_realism_mode,
        "quality_gate": quality_gate,
        "quality_gate_status": plan.get("quality_gate_status"),
        "quality_gate_selected_mode": plan.get("quality_gate_selected_mode"),
        "quality_gate_fallback_used": plan.get("quality_gate_fallback_used", False),
        "alternative_count": alternative_count,
        "diversity_mode": diversity_mode,
        "recent_recipe_id_count": len(recent_recipe_ids or set()),
        "diversity_mode_used": diversity_mode,
        "recent_recipe_count_used": len(recent_recipe_ids or set()),
        "recent_recipe_ids_used": sorted(recent_recipe_ids or set()),
        "loader_warnings": pool.loader_diagnostics.get("warnings", []),
    }
    plan["dashboard_recent_recipe_ids"] = sorted(recent_recipe_ids or set())
    return plan


def _render_multi_day_plan(
    title: str,
    plan: dict[str, Any],
) -> None:
    summary = plan.get("multi_day_summary", {})
    validation = plan.get("multi_day_validation", {})
    days = plan.get("days", [])
    st.subheader(title)
    st.caption(
        f"Run: {plan.get('run_id', 'missing')}; "
        f"trigger={plan.get('generation_trigger', 'three_day')}; "
        f"generated_at={plan.get('generated_at', 'missing')}; "
        f"multi_day_mode={plan.get('multi_day_selector_mode', 'simple_3_day')}"
    )
    _render_pool_summary(plan.get("pool_summary", {}))
    runtime_value = plan.get("generation_runtime_seconds")
    if runtime_value is not None:
        st.caption(f"Generation runtime: {_format_number(runtime_value)} seconds")

    metric_cols = st.columns(8)
    metric_cols[0].metric("Valid days", summary.get("valid_day_count", 0))
    metric_cols[1].metric("Accept days", summary.get("accept_day_count", 0))
    metric_cols[2].metric("Review days", summary.get("review_day_count", 0))
    metric_cols[3].metric("Fallback days", summary.get("fallback_day_count", 0))
    metric_cols[4].metric("Unique recipes", summary.get("unique_recipe_count", 0))
    metric_cols[5].metric("Repeated recipes", summary.get("repeated_recipe_count", 0))
    metric_cols[6].metric("Avg day loss", _format_number(summary.get("average_day_loss")))
    metric_cols[7].metric("Multi-day loss", _format_number(summary.get("multi_day_loss")))

    if isinstance(validation, dict):
        status = validation.get("validation_status", "not_validated")
        if status == "valid":
            st.success(f"Multi-day validation: {status}")
        else:
            st.warning(f"Multi-day validation: {status}")
        classification = validation.get("multi_day_classification")
        if classification == "multi_day_good":
            st.success("Strict multi-day verdict: multi_day_good")
        elif classification:
            st.warning(f"Strict multi-day verdict: {classification}")

    warnings = list(plan.get("multi_day_warnings", []))
    warnings.extend(summary.get("quality_warnings", []))
    if isinstance(validation, dict):
        warnings.extend(validation.get("warnings", []))
    if warnings:
        with st.expander("Multi-day warnings", expanded=True):
            for warning in dict.fromkeys(str(item) for item in warnings):
                st.write(f"- {warning}")

    candidate_pool = summary.get("candidate_day_pool_summary", {})
    if isinstance(candidate_pool, dict) and candidate_pool:
        st.caption(
            "Candidate pool: "
            f"total={candidate_pool.get('candidate_day_count')}; "
            f"accept={candidate_pool.get('accept_candidate_count')}; "
            f"review={candidate_pool.get('review_candidate_count')}; "
            f"reject={candidate_pool.get('reject_candidate_count')}"
        )
    selector_diagnostics = plan.get("selector_diagnostics", {})
    if isinstance(selector_diagnostics, dict) and selector_diagnostics:
        st.caption(
            "No-repeat: "
            f"policy={selector_diagnostics.get('no_repeat_policy_used')}; "
            "feasible_exact="
            f"{selector_diagnostics.get('feasible_no_repeat_combinations')}; "
            "fallback="
            f"{selector_diagnostics.get('fallback_from_hard_no_repeat')}; "
            "builder="
            f"{selector_diagnostics.get('day_candidate_builder')}; "
            "shortlist="
            f"{selector_diagnostics.get('direct_slot_shortlist_size')}; "
            "direct_combos="
            f"{selector_diagnostics.get('direct_candidate_combinations_evaluated')}; "
            "pool_s="
            f"{selector_diagnostics.get('candidate_pool_build_seconds')}; "
            "combo_s="
            f"{selector_diagnostics.get('combination_selection_seconds')}"
        )

    if not isinstance(days, list) or not days:
        st.info("Nu exista zile generate.")
        return

    tabs = st.tabs([f"Day {day.get('day_index', index + 1)}" for index, day in enumerate(days)])
    for tab, day in zip(tabs, days):
        with tab:
            _render_multi_day_day(day)

    st.markdown("#### Copy 3-day draft plan")
    st.code("\n".join(multi_day_readable_lines(plan)), language=None)


def _render_multi_day_day(day: dict[str, Any]) -> None:
    validation_status = day.get("validation_status", "not_validated")
    quality_status = day.get("quality_gate_status", "missing")
    fallback_used = bool(day.get("fallback_used", False))
    diversity_mode = day.get("diversity_mode_used", "none")
    repeated = day.get("repeated_recipe_ids_vs_previous_days", [])
    totals = day.get("day_totals", {})
    diagnostics = day.get("selector_diagnostics", {})
    if not isinstance(diagnostics, dict):
        diagnostics = {}
    st.markdown(f"#### Day {day.get('day_index')}")
    st.write(
        (
            f"validation_status={validation_status}; "
            f"quality_gate_status={quality_status}; "
            f"fallback_used={fallback_used}; "
            f"diversity_mode_used={diversity_mode}; "
            f"multi_day_mode_used={day.get('multi_day_mode_used', 'simple_3_day')}; "
            f"recent_recipe_count_used={day.get('recent_recipe_count_used', 0)}; "
            f"base_day_loss={_format_number(diagnostics.get('base_day_loss'))}; "
            f"adjusted_day_loss={_format_number(diagnostics.get('adjusted_day_loss'))}"
        )
    )
    if repeated:
        st.warning(
            "Repeated recipes vs previous days: "
            + ", ".join(str(item) for item in repeated)
        )
    if fallback_used:
        st.warning("Fallback best plan was used for this day.")
    if quality_status in {"review", "reject"}:
        st.warning(
            "Quality gate reasons: "
            + _format_reasons(day.get("quality_gate_reasons"))
        )

    metric_cols = st.columns(5)
    metric_cols[0].metric("Kcal", _format_number(totals.get("total_kcal")))
    metric_cols[1].metric("Protein", _format_number(totals.get("total_protein_g"), "g"))
    metric_cols[2].metric("Carbs", _format_number(totals.get("total_carbs_g"), "g"))
    metric_cols[3].metric("Fat", _format_number(totals.get("total_fat_g"), "g"))
    metric_cols[4].metric(
        "Effective time",
        _format_number(totals.get("effective_time_min_sum"), " min"),
    )

    selected_meals = day.get("selected_meals", [])
    if selected_meals:
        st.dataframe(
            _selected_meals_frame(selected_meals),
            use_container_width=True,
            hide_index=True,
        )
        _render_long_passive_notes(selected_meals)
        _render_slot_suspicion_notes(selected_meals)
        _render_meal_realism_notes(selected_meals)
    day_warnings = day.get("warnings", [])
    if day_warnings:
        with st.expander("Day warnings", expanded=True):
            for warning in day_warnings:
                st.write(f"- {warning}")


def _render_plan(
    title: str,
    plan: dict[str, Any],
    enable_feedback: bool,
) -> None:
    validation = plan.get("validation", {})
    totals = plan.get("day_totals", {})
    warnings = list(plan.get("warnings", []))
    warnings.extend(validation.get("validation_warnings", []))

    st.subheader(title)
    _render_menu_generation_metadata(plan)
    status = str(validation.get("validation_status", "not_validated"))
    if validation.get("is_valid_for_checkpoint_1"):
        st.success(f"Validation status: {status}")
    else:
        st.warning(f"Validation status: {status}")
    _render_pool_summary(plan.get("pool_summary", {}))
    used_recent_ids = plan.get("dashboard_recent_recipe_ids") or []
    if used_recent_ids:
        st.caption(
            "Recent recipe ids used for this generation: "
            + ", ".join(str(item) for item in used_recent_ids)
        )
    _render_selector_diagnostics(plan.get("selector_diagnostics", {}))
    _render_plan_alternatives(plan.get("alternatives", []))

    metric_cols = st.columns(6)
    metric_cols[0].metric("Kcal", _format_number(totals.get("total_kcal")))
    metric_cols[1].metric("Protein", _format_number(totals.get("total_protein_g"), "g"))
    metric_cols[2].metric("Carbs", _format_number(totals.get("total_carbs_g"), "g"))
    metric_cols[3].metric("Fat", _format_number(totals.get("total_fat_g"), "g"))
    metric_cols[4].metric(
        "Total time",
        _format_number(totals.get("total_time_min_sum"), " min"),
    )
    metric_cols[5].metric(
        "Effective time",
        _format_number(totals.get("effective_time_min_sum"), " min"),
    )

    selected_meals = plan.get("selected_meals", [])
    if selected_meals:
        st.dataframe(
            _selected_meals_frame(selected_meals),
            use_container_width=True,
            hide_index=True,
        )
        _render_long_passive_notes(selected_meals)
        _render_slot_suspicion_notes(selected_meals)
        _render_meal_realism_notes(selected_meals)
        if enable_feedback:
            _render_feedback_controls(selected_meals)
    else:
        st.info("Nu exista mese selectate.")

    if warnings:
        with st.expander("Warnings", expanded=True):
            for warning in warnings:
                st.write(f"- {warning}")
    else:
        st.caption("Warnings: none")

    _render_nutrition_cache_diagnostics(plan.get("nutrition_cache_diagnostics", {}))
    _render_pilot_servings_diagnostics(plan.get("pilot_servings_diagnostics", {}))
    _render_pilot_nutrition_overlay_details(plan)
    _render_ingredient_diagnostics(plan.get("ingredient_diagnostics", {}))


def _render_pool_summary(pool_summary: dict[str, Any]) -> None:
    if not pool_summary:
        return
    cols = st.columns(5)
    cols[0].metric("Dataset", str(pool_summary.get("dataset_profile", "unknown")))
    cols[1].metric("Mode", str(pool_summary.get("selection_mode", "greedy")))
    cols[2].metric("Loaded recipes", pool_summary.get("total_recipes_loaded", 0))
    cols[3].metric("Eligible", pool_summary.get("eligible_candidate_count", 0))
    cols[4].metric("Filtered", pool_summary.get("filtered_candidate_count", 0))
    st.caption(
        "Portion policy: "
        f"{pool_summary.get('portion_policy', 'standard')}; "
        f"meal realism={pool_summary.get('meal_realism_mode', 'off')}; "
        "Alternatives: "
        f"{pool_summary.get('alternative_count', 1)}; "
        f"diversity={pool_summary.get('diversity_mode', 'none')}; "
        f"recent recipes={pool_summary.get('recent_recipe_id_count', 0)}; "
        f"multi-day speed={pool_summary.get('multi_day_speed_mode', 'n/a')}; "
        f"builder={pool_summary.get('day_candidate_builder', 'n/a')}; "
        f"direct shortlist={pool_summary.get('direct_slot_shortlist_size', 'n/a')}"
    )
    warnings = pool_summary.get("loader_warnings") or []
    if warnings:
        st.warning("Loader warnings: " + "; ".join(str(item) for item in warnings))


def _render_menu_generation_metadata(plan: dict[str, Any]) -> None:
    run_id = plan.get("run_id", "missing")
    generated_at = plan.get("generated_at", "missing")
    generation_trigger = plan.get("generation_trigger", "unknown")
    diversity_mode_used = plan.get("diversity_mode_used", "none")
    recent_count = int(plan.get("recent_recipe_count_used") or 0)
    stored_config = _menu_generation_config(plan)
    if stored_config:
        config_text = ", ".join(
            f"{key}={value}" for key, value in stored_config.items()
        )
    else:
        config_text = "missing generation_config"
    st.caption(
        f"Run: {run_id}; trigger={generation_trigger}; generated_at={generated_at}; "
        f"diversity_mode_used={diversity_mode_used}; "
        f"recent_recipe_count_used={recent_count}; {config_text}"
    )
    _render_quality_gate_summary(plan)
    recent_details = plan.get("recent_recipe_details_used") or []
    if recent_details:
        with st.expander("Recent recipes used by this generation", expanded=False):
            st.dataframe(
                pd.DataFrame(recent_details),
                use_container_width=True,
                hide_index=True,
            )


def _render_quality_gate_summary(plan: dict[str, Any]) -> None:
    quality_gate = plan.get("quality_gate")
    if not isinstance(quality_gate, dict):
        return
    status = quality_gate.get("quality_gate_status", "unknown")
    reasons = quality_gate.get("quality_gate_reasons", [])
    selected_mode = plan.get("quality_gate_selected_mode", plan.get("diversity_mode_used"))
    fallback_used = bool(plan.get("quality_gate_fallback_used", False))
    message = (
        f"quality_gate_status={status}; "
        f"selected_varied_mode={selected_mode}; "
        f"fallback_used={fallback_used}; "
        "reasons="
        f"{_format_reasons(reasons)}"
    )
    if fallback_used:
        st.warning(message)
    elif status == "reject":
        st.error(message)
    elif status == "review":
        st.warning(message)
    else:
        st.caption(message)


def _render_selector_diagnostics(diagnostics: object) -> None:
    if not isinstance(diagnostics, dict) or diagnostics.get("selector_mode") != "balanced_day":
        return
    with st.expander("Balanced day selector diagnostics", expanded=False):
        metric_cols = st.columns(6)
        metric_cols[0].metric("Day loss", _format_number(diagnostics.get("day_loss")))
        metric_cols[1].metric("Kcal loss", _format_number(diagnostics.get("kcal_loss")))
        metric_cols[2].metric("Protein loss", _format_number(diagnostics.get("protein_loss")))
        metric_cols[3].metric("Carbs loss", _format_number(diagnostics.get("carbs_loss")))
        metric_cols[4].metric("Fat loss", _format_number(diagnostics.get("fat_loss")))
        metric_cols[5].metric(
            "Evaluated",
            diagnostics.get("evaluated_combination_count", 0),
        )
        st.write(
            "Shortlist counts: "
            + _format_counts(
                diagnostics.get("candidate_count_per_slot_after_shortlist", {})
            )
        )
        st.write(
            "Realism filter before/after: "
            + _format_counts(
                diagnostics.get("candidate_count_per_slot_before_realism_filter", {})
            )
            + " -> "
            + _format_counts(
                diagnostics.get("candidate_count_per_slot_after_realism_filter", {})
            )
        )
        hard_rejected = diagnostics.get("hard_rejected_count_by_slot", {})
        if isinstance(hard_rejected, dict) and any(
            int(value or 0) > 0 for value in hard_rejected.values()
        ):
            st.write("Hard rejected by slot: " + _format_counts(hard_rejected))
        warnings = diagnostics.get("selector_warnings") or []
        if warnings:
            st.write("Warnings: " + "; ".join(str(item) for item in warnings))
        st.write(
            "Base/adjusted: "
            f"{_format_number(diagnostics.get('base_day_loss'))} / "
            f"{_format_number(diagnostics.get('adjusted_day_loss'))}"
        )
        st.write(
            "Meal realism: "
            f"mode={diagnostics.get('meal_realism_mode', 'off')}; "
            "total_penalty="
            f"{_format_number(diagnostics.get('meal_realism_total_penalty'))}; "
            "applied_penalty="
            f"{_format_number(diagnostics.get('meal_realism_applied_penalty'))}"
        )
        st.write(
            "Diversity penalties: "
            + _format_counts(diagnostics.get("diversity_penalties", {}))
        )
        realism_penalties = diagnostics.get("meal_realism_penalties", {})
        if isinstance(realism_penalties, dict):
            st.write(
                "Meal realism penalties: "
                + _format_counts(realism_penalties)
            )
        hard_reject_reasons = _hard_reject_reason_frame(
            diagnostics.get("hard_reject_reasons")
        )
        if not hard_reject_reasons.empty:
            st.dataframe(
                hard_reject_reasons,
                use_container_width=True,
                hide_index=True,
            )
        recent_ids = diagnostics.get("recent_recipe_ids_considered") or []
        if recent_ids:
            st.write("Recent recipe ids considered: " + ", ".join(str(item) for item in recent_ids))


def _render_plan_alternatives(alternatives: object) -> None:
    if not isinstance(alternatives, list) or len(alternatives) <= 1:
        return
    with st.expander("Balanced day alternatives", expanded=False):
        rows = []
        for alternative in alternatives:
            if not isinstance(alternative, dict):
                continue
            totals = alternative.get("day_totals", {})
            penalties = alternative.get("diversity_penalties", {})
            selected_meals = alternative.get("selected_meals", [])
            rows.append(
                {
                    "rank": alternative.get("alternative_rank"),
                    "base_day_loss": alternative.get("base_day_loss"),
                    "adjusted_day_loss": alternative.get("adjusted_day_loss"),
                    "kcal": totals.get("total_kcal"),
                    "protein_g": totals.get("total_protein_g"),
                    "carbs_g": totals.get("total_carbs_g"),
                    "fat_g": totals.get("total_fat_g"),
                    "diversity_penalty": (
                        penalties.get("total_diversity_penalty")
                        if isinstance(penalties, dict)
                        else 0
                    ),
                    "meal_realism_penalty": alternative.get(
                        "meal_realism_total_penalty"
                    ),
                    "meal_realism_applied": alternative.get(
                        "meal_realism_applied_penalty"
                    ),
                    "recent_repeats": (
                        penalties.get("recent_recipe_count")
                        if isinstance(penalties, dict)
                        else 0
                    ),
                    "recipes": " | ".join(
                        str(meal.get("display_name"))
                        for meal in selected_meals
                        if isinstance(meal, dict)
                    ),
                }
            )
        if rows:
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


def _selected_meals_frame(selected_meals: list[dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for meal in selected_meals:
        rows.append(
            {
                "slot": meal.get("slot"),
                "recipe": meal.get("display_name"),
                "portion": meal.get("portion_multiplier"),
                "portion_grams_estimated": _format_estimated_grams(
                    _meal_portion_grams_estimated(meal)
                ),
                "portion_grams_source": meal.get("portion_grams_source"),
                "portion_policy": meal.get("portion_policy_mode"),
                "portion_policy_warnings": _format_reasons(
                    meal.get("portion_policy_warnings")
                ),
                "meal_realism_score": meal.get("meal_realism_score"),
                "meal_realism_penalty": meal.get("meal_realism_penalty"),
                "meal_realism_flags": _format_reasons(
                    meal.get("meal_realism_flags")
                ),
                "realism_hard_reject": meal.get("realism_hard_reject"),
                "realism_reject_reason": _format_reasons(
                    meal.get("realism_reject_reason")
                ),
                "original_portion_g_estimated": _format_estimated_grams(
                    meal.get("original_portion_grams_estimated")
                ),
                "overlay_portion_g_estimated": _format_estimated_grams(
                    meal.get("overlay_portion_grams_estimated")
                ),
                "original_kcal_serving": meal.get("original_energy_kcal_per_serving"),
                "original_protein_serving": meal.get("original_protein_g_per_serving"),
                "overlay_kcal_serving": meal.get("overlay_energy_kcal_per_serving"),
                "overlay_protein_serving": meal.get("overlay_protein_g_per_serving"),
                "pilot_nutrition_overlay": meal.get("uses_pilot_nutrition_overlay"),
                "kcal": meal.get("kcal"),
                "protein_g": meal.get("protein_g"),
                "carbs_g": meal.get("carbs_g"),
                "fat_g": meal.get("fat_g"),
                "total_time_min": meal.get("total_time_min"),
                "effective_time_min": meal.get("effective_time_min_for_scoring"),
                "original_effective_time_min": meal.get(
                    "original_effective_time_min_for_scoring"
                ),
                "passive_time_min": meal.get("passive_time_estimated_min"),
                "long_passive": meal.get("has_long_passive_time"),
                "pilot_time_fallback": meal.get("uses_pilot_time_fallback"),
                "score_preview": meal.get("score_preview"),
                "suspicious": meal.get("is_nutrition_suspicious"),
                "slot_suspicious": meal.get("is_slot_suspicious"),
                "slot_suspicion_reasons": _format_reasons(
                    meal.get("slot_suspicion_reasons")
                ),
                "meal_realism_reasons": _format_reasons(
                    meal.get("meal_realism_reasons")
                ),
            }
        )
    return pd.DataFrame(rows)


def _menu_as_text(plan: dict[str, Any]) -> str:
    validation = plan.get("validation", {})
    totals = plan.get("day_totals", {})
    lines = [
        "TableTogether / Generator v1 latest menu",
        f"run_id={plan.get('run_id', 'missing')}",
        f"generation_trigger={plan.get('generation_trigger', 'unknown')}",
        f"diversity_mode_used={plan.get('diversity_mode_used', 'none')}",
        f"recent_recipe_count_used={plan.get('recent_recipe_count_used', 0)}",
        f"quality_gate_status={plan.get('quality_gate_status', 'missing')}",
        f"quality_gate_fallback_used={plan.get('quality_gate_fallback_used', False)}",
        f"quality_gate_selected_mode={plan.get('quality_gate_selected_mode', 'missing')}",
        "quality_gate_reasons="
        f"{_format_reasons(plan.get('quality_gate_reasons'))}",
        f"validation_status={validation.get('validation_status', 'not_validated')}",
        (
            f"totals: kcal={_format_number(totals.get('total_kcal'))}, "
            f"protein={_format_number(totals.get('total_protein_g'), 'g')}, "
            f"carbs={_format_number(totals.get('total_carbs_g'), 'g')}, "
            f"fat={_format_number(totals.get('total_fat_g'), 'g')}, "
            f"original_kcal={_format_number(totals.get('original_total_kcal'))}, "
            "pilot_overlay_count="
            f"{totals.get('uses_pilot_nutrition_overlay_count')}, "
            f"total_time_min={_format_number(totals.get('total_time_min_sum'))}, "
            "effective_time_min="
            f"{_format_number(totals.get('effective_time_min_sum'))}, "
            "passive_time_min="
            f"{_format_number(totals.get('passive_time_estimated_sum'))}"
        ),
        "",
        "meals:",
    ]
    for meal in plan.get("selected_meals", []):
        lines.append(
            (
                f"- {meal.get('slot')}: {meal.get('display_name')} "
                f"({meal.get('recipe_id')}), portion={meal.get('portion_multiplier')}, "
                f"portion_grams_estimated="
                f"{_format_estimated_grams(_meal_portion_grams_estimated(meal))}, "
                f"portion_grams_source={meal.get('portion_grams_source')}, "
                f"portion_policy={meal.get('portion_policy_mode')}, "
                "portion_policy_warnings="
                f"{_format_reasons(meal.get('portion_policy_warnings'))}, "
                "original_portion_grams_estimated="
                f"{_format_estimated_grams(meal.get('original_portion_grams_estimated'))}, "
                "overlay_portion_grams_estimated="
                f"{_format_estimated_grams(meal.get('overlay_portion_grams_estimated'))}, "
                f"kcal={_format_number(meal.get('kcal'))}, "
                f"protein={_format_number(meal.get('protein_g'), 'g')}, "
                f"carbs={_format_number(meal.get('carbs_g'), 'g')}, "
                f"fat={_format_number(meal.get('fat_g'), 'g')}, "
                "original_kcal_serving="
                f"{_format_number(meal.get('original_energy_kcal_per_serving'))}, "
                "overlay_kcal_serving="
                f"{_format_number(meal.get('overlay_energy_kcal_per_serving'))}, "
                "pilot_nutrition_overlay="
                f"{meal.get('uses_pilot_nutrition_overlay')}, "
                f"total_time_min={_format_number(meal.get('total_time_min'))}, "
                "effective_time_min="
                f"{_format_number(meal.get('effective_time_min_for_scoring'))}, "
                "original_effective_time_min="
                f"{_format_number(meal.get('original_effective_time_min_for_scoring'))}, "
                "passive_time_min="
                f"{_format_number(meal.get('passive_time_estimated_min'))}, "
                f"long_passive={meal.get('has_long_passive_time')}, "
                f"pilot_time_fallback={meal.get('uses_pilot_time_fallback')}, "
                f"slot_suspicious={meal.get('is_slot_suspicious')}, "
                "slot_suspicion_reasons="
                f"{_format_reasons(meal.get('slot_suspicion_reasons'))}, "
                "meal_realism_score="
                f"{_format_number(meal.get('meal_realism_score'))}, "
                "meal_realism_penalty="
                f"{_format_number(meal.get('meal_realism_penalty'))}, "
                "meal_realism_flags="
                f"{_format_reasons(meal.get('meal_realism_flags'))}, "
                "realism_hard_reject="
                f"{meal.get('realism_hard_reject')}, "
                "realism_reject_reason="
                f"{_format_reasons(meal.get('realism_reject_reason'))}, "
                f"score_preview={_format_number(meal.get('score_preview'))}"
            )
        )

    warnings = list(plan.get("warnings", []))
    warnings.extend(validation.get("validation_warnings", []))
    lines.extend(["", "warnings:"])
    lines.extend([f"- {warning}" for warning in warnings] or ["- none"])
    return "\n".join(lines)


def _render_feedback_controls(selected_meals: list[dict[str, Any]]) -> None:
    st.markdown("#### Demo feedback")
    st.caption(
        "Feedback is stored only in this Streamlit session and does not influence generation yet."
    )
    feedback_buttons = [
        ("Like", "liked"),
        ("Dislike", "disliked"),
        ("Too long", "too_long"),
        ("Avoid", "explicit_avoid"),
    ]
    for meal_index, meal in enumerate(selected_meals):
        st.markdown(f"**{meal.get('slot')}: {meal.get('display_name')}**")
        button_cols = st.columns(4)
        for button_index, (label, feedback_type) in enumerate(feedback_buttons):
            button_cols[button_index].button(
                label,
                key=(
                    f"feedback_{meal_index}_{meal.get('slot')}_"
                    f"{meal.get('recipe_id')}_{feedback_type}"
                ),
                on_click=_store_feedback_event,
                args=(meal, feedback_type),
                use_container_width=True,
            )


def _render_nutrition_cache_diagnostics(diagnostics: object) -> None:
    if not isinstance(diagnostics, dict) or not diagnostics:
        return

    with st.expander("Nutrition cache diagnostics", expanded=False):
        cache_counts = diagnostics.get("cache_status_counts", {})
        eligible = diagnostics.get("eligible_candidates", {})
        mapped_ratio = diagnostics.get("mapped_weight_ratio", {})
        total_weight = diagnostics.get("total_weight_grams_estimated", {})
        servings_basis = diagnostics.get("servings_basis", {})
        macros = diagnostics.get("per_serving_macros", {})

        st.write(f"Total nutrition rows: {diagnostics.get('total_nutrition_rows')}")
        st.write(f"Cache status counts: {_format_counts(cache_counts)}")
        if isinstance(servings_basis, dict):
            st.write(
                (
                    "Servings basis: "
                    f"missing={servings_basis.get('missing_count')}, "
                    f"zero_or_invalid={servings_basis.get('zero_or_invalid_count')}, "
                    f"value_counts={_format_counts(servings_basis.get('value_counts', {}))}"
                )
            )
        if isinstance(total_weight, dict):
            st.write(
                (
                    "Total weight grams estimated: "
                    f"missing={total_weight.get('missing_count')}, "
                    f"zero_or_invalid={total_weight.get('zero_or_invalid_count')}, "
                    f"median={_format_number(total_weight.get('median'))}"
                )
            )
        if isinstance(mapped_ratio, dict):
            st.write(
                (
                    "Mapped weight ratio: "
                    f"median={_format_number(mapped_ratio.get('median'))}, "
                    f"below_0.20={mapped_ratio.get('count_below_0.20')}, "
                    f"below_0.40={mapped_ratio.get('count_below_0.40')}, "
                    f"below_0.60={mapped_ratio.get('count_below_0.60')}"
                )
            )
        if isinstance(eligible, dict):
            st.write(
                (
                    "Eligible candidates: "
                    f"rows={eligible.get('eligible_rows_count')}, "
                    f"kcal_lt_150={eligible.get('kcal_per_serving_lt_150_count')}, "
                    f"protein_lt_10={eligible.get('protein_per_serving_lt_10_count')}, "
                    "kcal_ge_150_and_protein_ge_10="
                    f"{eligible.get('kcal_ge_150_and_protein_ge_10_count')}"
                )
            )
        if isinstance(macros, dict):
            macro_rows = [
                {"macro": macro_name, **macro_values}
                for macro_name, macro_values in macros.items()
                if isinstance(macro_values, dict)
            ]
            if macro_rows:
                st.dataframe(
                    pd.DataFrame(macro_rows),
                    use_container_width=True,
                    hide_index=True,
                )
        suspicious = diagnostics.get("top_suspicious_eligible_recipes", [])
        if isinstance(suspicious, list) and suspicious:
            st.caption("Top suspicious eligible recipes")
            st.dataframe(
                pd.DataFrame(suspicious),
                use_container_width=True,
                hide_index=True,
            )


def _render_pilot_servings_diagnostics(diagnostics: object) -> None:
    if not isinstance(diagnostics, dict) or not diagnostics:
        return

    with st.expander("Pilot servings diagnostics", expanded=False):
        st.write(f"Eligible recipes: {diagnostics.get('eligible_recipe_count')}")
        st.write(
            (
                "Estimated servings distribution: "
                f"{_format_counts(diagnostics.get('estimated_servings_basis_distribution', {}))}"
            )
        )
        st.write(
            (
                "Pilot servings fallback count: "
                f"{diagnostics.get('uses_pilot_servings_fallback_count')}"
            )
        )
        st.caption(
            "Pilot fallback only: these values do not overwrite nutrition_cache yet."
        )

        selected = diagnostics.get("selected_plan_servings", [])
        if isinstance(selected, list) and selected:
            st.dataframe(
                pd.DataFrame(selected),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.write("Selected plan servings: none")


def _render_pilot_nutrition_overlay_details(plan: dict[str, Any]) -> None:
    selected_meals = plan.get("selected_meals", [])
    if not selected_meals:
        return

    totals = plan.get("day_totals", {})
    with st.expander("Pilot nutrition overlay details", expanded=False):
        st.caption(
            "Pilot fallback / overlay only: original cache values are not overwritten."
        )
        st.write(
            (
                "Original day totals: "
                f"kcal={_format_number(totals.get('original_total_kcal'))}, "
                f"protein={_format_number(totals.get('original_total_protein_g'), 'g')}, "
                f"carbs={_format_number(totals.get('original_total_carbs_g'), 'g')}, "
                f"fat={_format_number(totals.get('original_total_fat_g'), 'g')}"
            )
        )
        st.write(
            (
                "Overlay-based day totals: "
                f"kcal={_format_number(totals.get('total_kcal'))}, "
                f"protein={_format_number(totals.get('total_protein_g'), 'g')}, "
                f"carbs={_format_number(totals.get('total_carbs_g'), 'g')}, "
                f"fat={_format_number(totals.get('total_fat_g'), 'g')}, "
                "uses_overlay_count="
                f"{totals.get('uses_pilot_nutrition_overlay_count')}"
            )
        )

        rows = []
        for meal in selected_meals:
            rows.append(
                {
                    "slot": meal.get("slot"),
                    "recipe_id": meal.get("recipe_id"),
                    "display_name": meal.get("display_name"),
                    "original_kcal_serving": meal.get(
                        "original_energy_kcal_per_serving"
                    ),
                    "original_protein_serving": meal.get(
                        "original_protein_g_per_serving"
                    ),
                    "overlay_kcal_serving": meal.get(
                        "overlay_energy_kcal_per_serving"
                    ),
                    "overlay_protein_serving": meal.get(
                        "overlay_protein_g_per_serving"
                    ),
                    "estimated_servings": meal.get("overlay_estimated_servings_basis"),
                    "original_portion_g_estimated": meal.get(
                        "original_portion_grams_estimated"
                    ),
                    "overlay_portion_g_estimated": meal.get(
                        "overlay_portion_grams_estimated"
                    ),
                    "portion_grams_source": meal.get("portion_grams_source"),
                    "alias_weight_g": meal.get("overlay_alias_weight_grams"),
                    "uses_overlay": meal.get("uses_pilot_nutrition_overlay"),
                    "aliases": _format_reasons(meal.get("overlay_aliases_used")),
                    "reasons": _format_reasons(
                        meal.get("pilot_nutrition_overlay_reasons")
                    ),
                }
            )
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


def _render_ingredient_diagnostics(diagnostics: object) -> None:
    if not isinstance(diagnostics, dict) or not diagnostics:
        return

    with st.expander("Ingredient diagnostics", expanded=False):
        global_summary = diagnostics.get("global_mapping_summary", {})
        if isinstance(global_summary, dict):
            st.write(
                (
                    "Global mapping: "
                    f"rows={global_summary.get('total_ingredient_rows')}, "
                    "status_counts="
                    f"{_format_counts(global_summary.get('mapping_status_counts', {}))}, "
                    "mapped_food_id_present="
                    f"{global_summary.get('mapped_food_id_present_count')}, "
                    "grams_gt_0="
                    f"{global_summary.get('quantity_grams_estimated_gt_0_count')}, "
                    "mapped_food_id_and_grams_gt_0="
                    f"{global_summary.get('mapped_food_id_and_grams_gt_0_count')}"
                )
            )
            st.write(
                (
                    "By status with grams: "
                    "accepted_auto="
                    f"{global_summary.get('accepted_auto_with_grams_gt_0_count')}, "
                    "accepted_auto_without_grams="
                    f"{global_summary.get('accepted_auto_without_grams_count')}, "
                    "review_needed="
                    f"{global_summary.get('review_needed_with_grams_gt_0_count')}, "
                    "unmapped="
                    f"{global_summary.get('unmapped_with_grams_gt_0_count')}"
                )
            )

        _render_records_table(
            "Top suspicious recipes",
            diagnostics.get("top_suspicious_recipes", []),
        )
        _render_records_table(
            "Common unmapped ingredients",
            diagnostics.get("common_unmapped_ingredients", []),
        )
        _render_records_table(
            "Common review-needed ingredients",
            diagnostics.get("common_review_needed_ingredients", []),
        )
        _render_records_table(
            "Selected plan ingredient breakdown",
            diagnostics.get("selected_plan_ingredient_breakdown", []),
        )


def _render_records_table(title: str, records: object) -> None:
    st.caption(title)
    if not isinstance(records, list) or not records:
        st.write("none")
        return
    st.dataframe(pd.DataFrame(records), use_container_width=True, hide_index=True)


def _render_long_passive_notes(selected_meals: list[dict[str, Any]]) -> None:
    long_passive_meals = [
        meal for meal in selected_meals if bool(meal.get("has_long_passive_time", False))
    ]
    if not long_passive_meals:
        return

    with st.expander("Long passive time notes", expanded=True):
        for meal in long_passive_meals:
            if meal.get("uses_pilot_time_fallback"):
                st.warning(
                    (
                        f"{meal.get('slot')}: {meal.get('display_name')} - "
                        "Pilot time fallback applied: passive time estimated from text keywords. "
                        "Reasons: "
                        f"{_format_reasons(meal.get('time_estimation_reasons'))}"
                    )
                )
                continue
            st.warning(
                (
                    f"{meal.get('slot')}: {meal.get('display_name')} has long passive time. "
                    "Reasons: "
                    f"{_format_reasons(meal.get('time_estimation_reasons'))}"
                )
            )


def _render_slot_suspicion_notes(selected_meals: list[dict[str, Any]]) -> None:
    suspicious_meals = [
        meal for meal in selected_meals if bool(meal.get("is_slot_suspicious", False))
    ]
    if not suspicious_meals:
        return

    with st.expander("Slot realism notes", expanded=True):
        for meal in suspicious_meals:
            st.warning(
                (
                    f"{meal.get('slot')}: {meal.get('display_name')} may not fit this slot. "
                    "Reasons: "
                    f"{_format_reasons(meal.get('slot_suspicion_reasons'))}"
                )
            )


def _render_meal_realism_notes(selected_meals: list[dict[str, Any]]) -> None:
    groups = {
        "Hard reject": [
            "breakfast_too_large",
            "main_too_large",
            "snack_too_large",
            "unrealistic_large_portion",
        ],
        "Large portions": [
            "borderline_large_portion",
            "unrealistic_large_portion",
            "breakfast_too_large",
            "main_too_large",
        ],
        "Mono-macro meals": [
            "mostly_carb_meal",
            "mostly_protein_meal",
            "low_carb_main",
            "low_protein_main",
        ],
        "Snack too large": ["snack_too_large", "snack_too_meal_like"],
        "Breakfast low protein": ["breakfast_low_protein"],
    }
    rows = []
    for meal in selected_meals:
        flags = set(_reason_items(meal.get("meal_realism_flags")))
        if meal.get("realism_hard_reject"):
            flags.add("realism_hard_reject")
        if not flags:
            continue
        for group_name, group_flags in groups.items():
            matched = [flag for flag in group_flags if flag in flags]
            if group_name == "Hard reject" and meal.get("realism_hard_reject"):
                matched.append("realism_hard_reject")
            if not matched:
                continue
            rows.append(
                {
                    "group": group_name,
                    "slot": meal.get("slot"),
                    "recipe": meal.get("display_name"),
                    "portion_g": meal.get("portion_grams_estimated"),
                    "kcal": meal.get("kcal"),
                    "protein_g": meal.get("protein_g"),
                    "carbs_g": meal.get("carbs_g"),
                    "fat_g": meal.get("fat_g"),
                    "flags": ", ".join(matched),
                    "hard_reject": meal.get("realism_hard_reject"),
                    "reject_reason": _format_reasons(
                        meal.get("realism_reject_reason")
                    ),
                    "reasons": _format_reasons(meal.get("meal_realism_reasons")),
                }
            )
    if not rows:
        return
    with st.expander("Meal realism QA warnings", expanded=True):
        st.warning("Some selected meals need realism review; validation status alone is not enough.")
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


def _store_feedback_event(meal: dict[str, Any], feedback_type: str) -> None:
    event = {
        "recipe_id": meal.get("recipe_id"),
        "display_name": meal.get("display_name"),
        "slot": meal.get("slot"),
        "feedback_type": feedback_type,
        "created_at": datetime.now().isoformat(timespec="seconds"),
    }
    st.session_state[SESSION_FEEDBACK_KEY].append(event)


def _render_feedback_events() -> None:
    st.markdown("#### Collected feedback")
    st.caption(
        "Feedback is stored only in this Streamlit session and does not influence generation yet."
    )
    events = st.session_state.get(SESSION_FEEDBACK_KEY, [])
    if not events:
        st.caption("No feedback collected yet.")
        return
    st.dataframe(pd.DataFrame(events), use_container_width=True, hide_index=True)


def _render_session_menu_history(generated_menus: list[dict[str, Any]]) -> None:
    st.markdown("#### Session menu history")
    rows = []
    for menu_index, menu in enumerate(generated_menus, start=1):
        totals = menu.get("day_totals", {})
        validation = menu.get("validation", {})
        pool_summary = menu.get("pool_summary", {})
        generation_config = _menu_generation_config(menu)
        rows.append(
            {
                "menu": menu_index,
                "run_id": menu.get("run_id"),
                "generated_at": menu.get("generated_at"),
                "trigger": menu.get("generation_trigger"),
                "status": validation.get("validation_status"),
                "dataset_profile": generation_config.get(
                    "dataset_profile",
                    pool_summary.get("dataset_profile"),
                ),
                "selection_mode": generation_config.get(
                    "selection_mode",
                    pool_summary.get("selection_mode"),
                ),
                "portion_policy": generation_config.get(
                    "portion_policy",
                    pool_summary.get("portion_policy"),
                ),
                "diversity": generation_config.get(
                    "diversity_mode",
                    pool_summary.get("diversity_mode"),
                ),
                "diversity_mode_used": menu.get(
                    "diversity_mode_used",
                    pool_summary.get("diversity_mode_used"),
                ),
                "recent_count_used": menu.get(
                    "recent_recipe_count_used",
                    pool_summary.get("recent_recipe_count_used"),
                ),
                "meal_realism_mode": generation_config.get(
                    "meal_realism_mode",
                    pool_summary.get("meal_realism_mode"),
                ),
                "quality_gate": generation_config.get(
                    "quality_gate",
                    pool_summary.get("quality_gate"),
                ),
                "quality_gate_status": menu.get(
                    "quality_gate_status",
                    pool_summary.get("quality_gate_status"),
                ),
                "quality_gate_selected_mode": menu.get(
                    "quality_gate_selected_mode",
                    pool_summary.get("quality_gate_selected_mode"),
                ),
                "quality_gate_fallback_used": menu.get(
                    "quality_gate_fallback_used",
                    pool_summary.get("quality_gate_fallback_used"),
                ),
                "alternative_count": generation_config.get(
                    "alternative_count",
                    pool_summary.get("alternative_count"),
                ),
                "kcal": totals.get("total_kcal"),
                "protein_g": totals.get("total_protein_g"),
                "carbs_g": totals.get("total_carbs_g"),
                "fat_g": totals.get("total_fat_g"),
                "total_time_min": totals.get("total_time_min_sum"),
                "effective_time_min": totals.get("effective_time_min_sum"),
                "recipes": _menu_recipe_summary(menu),
            }
        )
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


def _render_pipeline() -> None:
    st.markdown("#### Generator v1 pipeline")
    for index, step in enumerate(PIPELINE_STEPS):
        color = _pipeline_color(index)
        st.markdown(
            (
                f"<div class='pipeline-card' style='border-left-color:{color};'>"
                f"{step}</div>"
            ),
            unsafe_allow_html=True,
        )

    # TODO: adauga planificare saptamanala dupa stabilizarea multi-day v1.


def _render_styles() -> None:
    st.markdown(
        """
        <style>
        .pipeline-card {
            background: #f7f9fc;
            border: 1px solid #d8dee9;
            border-left: 6px solid #3b82f6;
            border-radius: 8px;
            color: #1f2937;
            font-size: 0.86rem;
            font-weight: 600;
            margin: 0 0 0.42rem 0;
            padding: 0.45rem 0.55rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _ensure_session_state() -> None:
    if st.session_state.get(SESSION_SCHEMA_KEY) != SESSION_SCHEMA_VERSION:
        st.session_state[SESSION_SCHEMA_KEY] = SESSION_SCHEMA_VERSION
        st.session_state[SESSION_MENUS_KEY] = []
        st.session_state[SESSION_LATEST_MENU_KEY] = None
        st.session_state[SESSION_MULTIDAY_PLANS_KEY] = []
        st.session_state[SESSION_LATEST_MULTIDAY_KEY] = None
        st.session_state[SESSION_FEEDBACK_KEY] = []
        st.session_state[SESSION_RECENT_RECIPE_IDS_KEY] = []
        st.session_state[SESSION_RUN_COUNTER_KEY] = 0
        st.session_state[SESSION_DATASET_PROFILE_KEY] = PILOT_CURRENT_PROFILE
        st.session_state[SESSION_SELECTION_MODE_KEY] = "greedy"
        st.session_state[SESSION_ALTERNATIVE_COUNT_KEY] = 1
        st.session_state[SESSION_DIVERSITY_MODE_KEY] = "none"
        st.session_state[SESSION_PORTION_POLICY_KEY] = "standard"
        st.session_state[SESSION_MEAL_REALISM_MODE_KEY] = "off"
        st.session_state[SESSION_QUALITY_GATE_KEY] = "off"
        st.session_state[SESSION_MULTIDAY_MODE_KEY] = MULTI_DAY_MODE_GLOBAL
        st.session_state[SESSION_MULTIDAY_NO_REPEAT_POLICY_KEY] = "prefer"
        st.session_state[SESSION_DAY_CANDIDATE_POOL_SIZE_KEY] = 75
        st.session_state[SESSION_MULTIDAY_SPEED_MODE_KEY] = "fast"
        st.session_state[SESSION_DAY_CANDIDATE_BUILDER_KEY] = "direct_from_slots"
        st.session_state[SESSION_DIRECT_SLOT_SHORTLIST_SIZE_KEY] = 12
        return
    if SESSION_MENUS_KEY not in st.session_state:
        st.session_state[SESSION_MENUS_KEY] = []
    if SESSION_LATEST_MENU_KEY not in st.session_state:
        menus = st.session_state.get(SESSION_MENUS_KEY, [])
        st.session_state[SESSION_LATEST_MENU_KEY] = menus[0] if menus else None
    if SESSION_MULTIDAY_PLANS_KEY not in st.session_state:
        st.session_state[SESSION_MULTIDAY_PLANS_KEY] = []
    if SESSION_LATEST_MULTIDAY_KEY not in st.session_state:
        plans = st.session_state.get(SESSION_MULTIDAY_PLANS_KEY, [])
        st.session_state[SESSION_LATEST_MULTIDAY_KEY] = plans[0] if plans else None
    if SESSION_FEEDBACK_KEY not in st.session_state:
        st.session_state[SESSION_FEEDBACK_KEY] = []
    if SESSION_RECENT_RECIPE_IDS_KEY not in st.session_state:
        st.session_state[SESSION_RECENT_RECIPE_IDS_KEY] = sorted(
            _recent_recipe_ids(st.session_state.get(SESSION_MENUS_KEY, []))
        )
    if SESSION_RUN_COUNTER_KEY not in st.session_state:
        st.session_state[SESSION_RUN_COUNTER_KEY] = len(
            st.session_state.get(SESSION_MENUS_KEY, [])
        )
    if SESSION_DATASET_PROFILE_KEY not in st.session_state:
        st.session_state[SESSION_DATASET_PROFILE_KEY] = PILOT_CURRENT_PROFILE
    if SESSION_SELECTION_MODE_KEY not in st.session_state:
        st.session_state[SESSION_SELECTION_MODE_KEY] = "greedy"
    if SESSION_ALTERNATIVE_COUNT_KEY not in st.session_state:
        st.session_state[SESSION_ALTERNATIVE_COUNT_KEY] = 1
    if SESSION_DIVERSITY_MODE_KEY not in st.session_state:
        st.session_state[SESSION_DIVERSITY_MODE_KEY] = "none"
    if SESSION_PORTION_POLICY_KEY not in st.session_state:
        st.session_state[SESSION_PORTION_POLICY_KEY] = "standard"
    if SESSION_MEAL_REALISM_MODE_KEY not in st.session_state:
        st.session_state[SESSION_MEAL_REALISM_MODE_KEY] = "off"
    if SESSION_QUALITY_GATE_KEY not in st.session_state:
        st.session_state[SESSION_QUALITY_GATE_KEY] = "off"
    if SESSION_MULTIDAY_MODE_KEY not in st.session_state:
        st.session_state[SESSION_MULTIDAY_MODE_KEY] = MULTI_DAY_MODE_GLOBAL
    if SESSION_MULTIDAY_NO_REPEAT_POLICY_KEY not in st.session_state:
        st.session_state[SESSION_MULTIDAY_NO_REPEAT_POLICY_KEY] = "prefer"
    if SESSION_DAY_CANDIDATE_POOL_SIZE_KEY not in st.session_state:
        st.session_state[SESSION_DAY_CANDIDATE_POOL_SIZE_KEY] = 75
    if SESSION_MULTIDAY_SPEED_MODE_KEY not in st.session_state:
        st.session_state[SESSION_MULTIDAY_SPEED_MODE_KEY] = "fast"
    if SESSION_DAY_CANDIDATE_BUILDER_KEY not in st.session_state:
        st.session_state[SESSION_DAY_CANDIDATE_BUILDER_KEY] = "direct_from_slots"
    if SESSION_DIRECT_SLOT_SHORTLIST_SIZE_KEY not in st.session_state:
        st.session_state[SESSION_DIRECT_SLOT_SHORTLIST_SIZE_KEY] = 12


def _slot_order(target: NutritionTarget) -> list[str]:
    preferred_order = ["breakfast", "lunch", "dinner", "snack"]
    known_slots = [slot for slot in preferred_order if slot in target.slot_targets]
    extra_slots = [slot for slot in target.slot_targets if slot not in preferred_order]
    return known_slots + extra_slots


def _slot_candidates_by_slot(
    slot_candidates: pd.DataFrame,
    slot_order: list[str],
) -> dict[str, pd.DataFrame]:
    return {
        slot: slot_candidates.loc[slot_candidates["slot"].eq(slot)].copy()
        for slot in slot_order
    }


def _select_one_day_plan(
    selection_mode: str,
    slot_candidates: pd.DataFrame,
    target: NutritionTarget,
    slot_order: list[str],
    selector_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    candidates_by_slot = _slot_candidates_by_slot(slot_candidates, slot_order)
    if selection_mode == "balanced_day":
        return select_one_day_plan_balanced(
            slot_candidates_by_slot=candidates_by_slot,
            target=target,
            slot_order=slot_order,
            config=selector_config,
        )
    plan = select_one_day_plan(
        slot_candidates_by_slot=candidates_by_slot,
        slot_order=slot_order,
    )
    plan["selector_mode"] = "greedy"
    plan["selector_diagnostics"] = {"selector_mode": "greedy"}
    return plan


def _balanced_selector_config(
    alternative_count: int,
    diversity_mode: str,
    recent_recipe_ids: set[str],
    meal_realism_mode: str,
) -> dict[str, Any]:
    count = max(1, min(3, int(alternative_count)))
    return {
        "return_alternatives": count > 1,
        "alternative_count": count,
        "diversity_mode": diversity_mode,
        "recent_recipe_ids": sorted(recent_recipe_ids),
        "meal_realism_mode": meal_realism_mode,
    }


def _without_recipe_ids(
    slot_candidates: pd.DataFrame,
    blocked_recipe_ids: set[str],
) -> pd.DataFrame:
    if not blocked_recipe_ids or slot_candidates.empty:
        return slot_candidates
    mask = ~slot_candidates["recipe_id"].astype(str).isin(blocked_recipe_ids)
    filtered = slot_candidates.loc[mask].copy()
    if filtered.empty:
        return slot_candidates
    return filtered


def _recent_recipe_ids(menus: list[dict[str, Any]]) -> set[str]:
    recipe_ids: set[str] = set()
    for row in _recent_recipe_details(menus):
        recipe_ids.add(str(row["recipe_id"]))
    return recipe_ids


def _recent_recipe_details(menus: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen_recipe_ids: set[str] = set()
    for menu_index, menu in enumerate(menus[:RECENT_MENUS_FOR_VARIATION], start=1):
        for meal in menu.get("selected_meals", []):
            recipe_id = str(meal.get("recipe_id", "")).strip()
            if not recipe_id or recipe_id in seen_recipe_ids:
                continue
            seen_recipe_ids.add(recipe_id)
            rows.append(
                {
                    "source_menu": menu_index,
                    "run_id": menu.get("run_id"),
                    "slot": meal.get("slot"),
                    "recipe_id": recipe_id,
                    "display_name": meal.get("display_name"),
                }
            )
    return rows


def _selected_recipe_ids(plan: dict[str, Any]) -> list[str]:
    return [
        str(meal.get("recipe_id"))
        for meal in plan.get("selected_meals", [])
        if meal.get("recipe_id")
    ]


def _menu_recipe_summary(menu: dict[str, Any]) -> str:
    parts = []
    for meal in menu.get("selected_meals", []):
        parts.append(f"{meal.get('slot')}: {meal.get('display_name')}")
    return " | ".join(parts)


def _target_to_dict(target: NutritionTarget) -> dict[str, Any]:
    return {
        "kcal": target.kcal,
        "protein_g": target.protein_g,
        "carbs_g": target.carbs_g,
        "fat_g": target.fat_g,
        "slot_targets": target.slot_targets,
    }


def _format_number(value: object, suffix: str = "") -> str:
    numeric_value = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric_value):
        return "missing"
    return f"{float(numeric_value):.1f}{suffix}"


def _format_estimated_grams(value: object) -> str:
    numeric_value = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric_value) or float(numeric_value) <= 0:
        return "unknown"
    return f"{float(numeric_value):.0f} g estimated"


def _meal_portion_grams_estimated(meal: dict[str, Any]) -> object:
    portion_grams = pd.to_numeric(meal.get("portion_grams_estimated"), errors="coerce")
    if not pd.isna(portion_grams) and float(portion_grams) > 0:
        return float(portion_grams)

    serving_weight = pd.to_numeric(
        meal.get("serving_weight_g_estimated"),
        errors="coerce",
    )
    portion_multiplier = pd.to_numeric(meal.get("portion_multiplier"), errors="coerce")
    if pd.isna(serving_weight) or pd.isna(portion_multiplier):
        return None
    if float(serving_weight) <= 0 or float(portion_multiplier) <= 0:
        return None
    return round(float(serving_weight) * float(portion_multiplier), 1)


def _format_reasons(value: object) -> str:
    if isinstance(value, list):
        return ", ".join(str(item) for item in value)
    if value is None:
        return "none"
    return str(value)


def _reason_items(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, tuple):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value or "").strip()
    if not text:
        return []
    if "|" in text:
        return [item.strip() for item in text.split("|") if item.strip()]
    if "," in text:
        return [item.strip() for item in text.split(",") if item.strip()]
    return [text]


def _hard_reject_reason_frame(value: object) -> pd.DataFrame:
    rows = []
    if not isinstance(value, dict):
        return pd.DataFrame(rows)
    for slot, items in value.items():
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            rows.append(
                {
                    "slot": slot,
                    "recipe_id": item.get("recipe_id"),
                    "recipe": item.get("display_name"),
                    "portion": item.get("portion_multiplier"),
                    "grams": item.get("portion_grams_estimated"),
                    "kcal": item.get("kcal"),
                    "reject_reason": _format_reasons(
                        item.get("realism_reject_reason")
                    ),
                    "flags": _format_reasons(item.get("meal_realism_flags")),
                }
            )
    return pd.DataFrame(rows)


def _format_counts(value: object) -> str:
    if not isinstance(value, dict) or not value:
        return "none"
    return ", ".join(f"{key}:{item}" for key, item in value.items())


def _pipeline_color(index: int) -> str:
    colors = ["#2563eb", "#059669", "#d97706", "#7c3aed", "#dc2626"]
    return colors[index % len(colors)]


if __name__ == "__main__":
    main()
