from __future__ import annotations

import sys
import time
from collections import Counter
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.generator_v1.multi_day_audit import summarize_multi_day_plan, validate_multi_day_plan
from src.generator_v1.multi_day_selector import (
    MULTI_DAY_MODE_GLOBAL,
    _build_candidate_day_pool,
    _resolve_slot_candidates_by_slot,
    _resolved_config,
    _select_global_day_combination,
)
from tools.extra.profile_generator_v1_round33_multiday_performance import (
    DAYS,
    MEAL_REALISM_MODE,
    PORTION_POLICY,
    QUALITY_GATE,
    SLOTS,
    V1_2_GENERATOR_READY_PLUS30_PLUS15_PROFILE,
    build_generation_context,
    plan_from_selected,
)


OUT_DIR = REPO_ROOT / "data" / "recipesdb" / "audit"
OUT_SUMMARY = OUT_DIR / "generator_v1_round34_direct_builder_performance_summary.txt"
OUT_RUNS = OUT_DIR / "generator_v1_round34_direct_builder_performance_runs.csv"
ROUND33_BASELINE = OUT_DIR / "generator_v1_round33_speed_quality_tradeoff.csv"

SHORTLIST_SIZES = [8, 10, 12, 15]
POOL_SIZE = 50


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    context = build_generation_context()
    rows: list[dict[str, Any]] = []
    baseline = load_round33_baseline()
    if baseline:
        rows.append(baseline)
    for shortlist_size in SHORTLIST_SIZES:
        rows.append(run_direct_builder_case(context, shortlist_size))

    output_started = time.perf_counter()
    pd.DataFrame(rows).to_csv(OUT_RUNS, index=False)
    OUT_SUMMARY.write_text(build_summary_text(rows), encoding="utf-8")
    output_seconds = round(time.perf_counter() - output_started, 3)
    for row in rows:
        row["output_writing_seconds"] = output_seconds
    pd.DataFrame(rows).to_csv(OUT_RUNS, index=False)

    recommended = recommended_direct_row(rows)
    print("Generator v1 Round34 direct day builder profile written")
    print(f"summary={OUT_SUMMARY}")
    print(f"runs={OUT_RUNS}")
    if recommended:
        print(
            "recommended_direct="
            f"shortlist:{recommended.get('direct_slot_shortlist_size')} "
            f"runtime:{recommended.get('total_runtime_seconds')} "
            f"loss:{recommended.get('multi_day_loss')} "
            f"repeated:{recommended.get('repeated_recipe_count')}"
        )


def load_round33_baseline() -> dict[str, Any]:
    if not ROUND33_BASELINE.exists():
        return {}
    rows = pd.read_csv(ROUND33_BASELINE)
    if rows.empty:
        return {}
    pool_rows = rows.loc[
        (rows.get("speed_mode") == "fast")
        & (pd.to_numeric(rows.get("pool_size_requested"), errors="coerce") == POOL_SIZE)
    ]
    if pool_rows.empty:
        pool_rows = rows.loc[rows.get("speed_mode") == "fast"].head(1)
    if pool_rows.empty:
        return {}
    row = pool_rows.iloc[0].to_dict()
    return {
        "case_name": "round33_old_fast_pool50_baseline",
        "dataset_profile": row.get("dataset_profile", V1_2_GENERATOR_READY_PLUS30_PLUS15_PROFILE),
        "day_candidate_builder": "balanced_repeated_round33_baseline",
        "direct_slot_shortlist_size": "",
        "pool_size_requested": POOL_SIZE,
        "total_runtime_seconds": _to_float(row.get("total_runtime_seconds")),
        "data_loading_seconds": _to_float(row.get("data_loading_seconds")),
        "slot_candidate_generation_seconds": _to_float(
            row.get("slot_candidate_generation_seconds")
        ),
        "day_candidate_pool_generation_seconds": _to_float(
            row.get("day_candidate_pool_generation_seconds")
        ),
        "combination_evaluation_seconds": _to_float(
            row.get("combination_evaluation_seconds")
        ),
        "quality_gate_evaluation_seconds": _to_float(
            row.get("quality_gate_evaluation_seconds")
        ),
        "validation_seconds": _to_float(row.get("validation_seconds")),
        "balanced_selector_seconds": _to_float(row.get("balanced_selector_seconds")),
        "balanced_selector_runs": int(_to_float(row.get("balanced_selector_runs"))),
        "direct_candidate_combinations_evaluated": "",
        "candidate_day_pool_count": int(_to_float(row.get("candidate_day_pool_count"))),
        "accept_candidate_day_count": "",
        "review_candidate_day_count": "",
        "reject_candidate_day_count": "",
        "combinations_evaluated": int(_to_float(row.get("combinations_evaluated"))),
        "feasible_no_repeat_combinations": int(
            _to_float(row.get("feasible_no_repeat_combinations"))
        ),
        "valid_day_count": int(_to_float(row.get("valid_day_count"))),
        "accept_day_count": int(_to_float(row.get("accept_day_count"))),
        "review_day_count": int(_to_float(row.get("review_day_count"))),
        "repeated_recipe_count": int(_to_float(row.get("repeated_recipe_count"))),
        "unique_recipe_count": int(_to_float(row.get("unique_recipe_count"))),
        "multi_day_loss": _to_float(row.get("multi_day_loss")),
        "strict_verdict": row.get("strict_verdict", ""),
        "no_repeat_policy_used": row.get("no_repeat_policy_used", ""),
        "fallback_from_hard_no_repeat": bool(row.get("fallback_from_hard_no_repeat")),
        "selected_plan": "see Round33 output",
    }


def run_direct_builder_case(
    context: Mapping[str, Any],
    shortlist_size: int,
) -> dict[str, Any]:
    profile_stats: dict[str, Any] = {}
    config = selector_config(shortlist_size, profile_stats)
    slot_candidates_by_slot = _resolve_slot_candidates_by_slot(
        slot_candidates=context["slot_candidates"],
        slot_candidates_by_slot=None,
        slot_order=SLOTS,
    )

    pool_started = time.perf_counter()
    candidate_days = _build_candidate_day_pool(
        slot_candidates_by_slot=slot_candidates_by_slot,
        target=context["target"],
        slot_order=SLOTS,
        config=config,
    )
    pool_seconds = time.perf_counter() - pool_started

    combination_started = time.perf_counter()
    selected, report = _select_global_day_combination(
        candidate_days=candidate_days,
        day_count=DAYS,
        config=config,
    )
    combination_seconds = time.perf_counter() - combination_started

    plan = plan_from_selected(selected, report, context["target"])
    summary = plan["multi_day_summary"]
    candidate_counts = Counter(
        str(candidate.get("quality_gate_status", "missing"))
        for candidate in candidate_days
    )
    total_runtime = (
        float(context["data_loading_seconds"])
        + float(context["slot_candidate_generation_seconds"])
        + pool_seconds
        + combination_seconds
    )
    return {
        "case_name": f"direct_from_slots_shortlist_{shortlist_size}",
        "dataset_profile": V1_2_GENERATOR_READY_PLUS30_PLUS15_PROFILE,
        "day_candidate_builder": "direct_from_slots",
        "direct_slot_shortlist_size": shortlist_size,
        "pool_size_requested": POOL_SIZE,
        "total_runtime_seconds": round(total_runtime, 3),
        "data_loading_seconds": round(float(context["data_loading_seconds"]), 3),
        "slot_candidate_generation_seconds": round(
            float(context["slot_candidate_generation_seconds"]),
            3,
        ),
        "day_candidate_pool_generation_seconds": round(pool_seconds, 3),
        "combination_evaluation_seconds": round(combination_seconds, 3),
        "quality_gate_evaluation_seconds": round(
            _to_float(profile_stats.get("quality_gate_seconds")),
            6,
        ),
        "validation_seconds": round(_to_float(profile_stats.get("validation_seconds")), 6),
        "balanced_selector_seconds": round(
            _to_float(profile_stats.get("balanced_selector_seconds")),
            6,
        ),
        "balanced_selector_runs": int(profile_stats.get("balanced_selector_runs", 0) or 0),
        "direct_candidate_combinations_evaluated": int(
            profile_stats.get("direct_candidate_combinations_evaluated", 0) or 0
        ),
        "candidate_day_pool_count": len(candidate_days),
        "accept_candidate_day_count": int(candidate_counts.get("accept", 0)),
        "review_candidate_day_count": int(candidate_counts.get("review", 0)),
        "reject_candidate_day_count": int(candidate_counts.get("reject", 0)),
        "combinations_evaluated": int(report.get("combinations_evaluated", 0) or 0),
        "feasible_no_repeat_combinations": int(
            report.get("feasible_no_repeat_combinations", 0) or 0
        ),
        "valid_day_count": int(summary.get("valid_day_count", 0) or 0),
        "accept_day_count": int(summary.get("accept_day_count", 0) or 0),
        "review_day_count": int(summary.get("review_day_count", 0) or 0),
        "repeated_recipe_count": int(summary.get("repeated_recipe_count", 0) or 0),
        "unique_recipe_count": int(summary.get("unique_recipe_count", 0) or 0),
        "multi_day_loss": report.get("multi_day_loss"),
        "strict_verdict": summary.get("multi_day_classification"),
        "no_repeat_policy_used": report.get("no_repeat_policy_used"),
        "fallback_from_hard_no_repeat": bool(
            report.get("fallback_from_hard_no_repeat", False)
        ),
        "selected_plan": selected_plan_label(plan["days"]),
    }


def selector_config(
    shortlist_size: int,
    profile_stats: dict[str, Any] | None = None,
) -> dict[str, Any]:
    raw_config: dict[str, Any] = {
        "selection_mode": "balanced_day",
        "portion_policy": PORTION_POLICY,
        "meal_realism_mode": MEAL_REALISM_MODE,
        "quality_gate": QUALITY_GATE,
        "alternative_count": 3,
        "return_alternatives": True,
        "multi_day_mode": MULTI_DAY_MODE_GLOBAL,
        "candidate_day_alternative_count": 10,
        "global_max_candidates_per_slot": 26,
        "day_candidate_pool_size_target": POOL_SIZE,
        "day_candidate_pool_max": 150,
        "include_slot_forced_variants": True,
        "no_repeat_policy": "hard",
        "multi_day_speed_mode": "fast",
        "day_candidate_builder": "direct_from_slots",
        "direct_slot_shortlist_size": shortlist_size,
    }
    if profile_stats is not None:
        raw_config["_profile_stats"] = profile_stats
    return _resolved_config(raw_config)


def selected_plan_label(days: Sequence[Mapping[str, Any]]) -> str:
    parts: list[str] = []
    for day in days:
        meals = day.get("selected_meals", [])
        names = [
            str(meal.get("display_name", "")).strip()
            for meal in meals
            if isinstance(meal, Mapping)
        ]
        parts.append(" / ".join(name for name in names if name))
    return " || ".join(parts)


def recommended_direct_row(rows: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    direct_rows = [
        row
        for row in rows
        if row.get("day_candidate_builder") == "direct_from_slots"
        and int(_to_float(row.get("accept_day_count"))) == 3
        and int(_to_float(row.get("review_day_count"))) == 0
        and int(_to_float(row.get("repeated_recipe_count"))) == 0
    ]
    if not direct_rows:
        return None
    direct_rows.sort(
        key=lambda row: (
            _to_float(row.get("total_runtime_seconds")),
            _to_float(row.get("multi_day_loss")),
            _to_float(row.get("direct_slot_shortlist_size")),
        )
    )
    return direct_rows[0]


def build_summary_text(rows: Sequence[Mapping[str, Any]]) -> str:
    recommended = recommended_direct_row(rows)
    lines = [
        "Round34 direct day-candidate builder performance",
        "",
        f"- dataset_profile: {V1_2_GENERATOR_READY_PLUS30_PLUS15_PROFILE}",
        f"- pool_size: {POOL_SIZE}",
        "- no_repeat_policy: hard",
        "",
        "Runs",
    ]
    for row in rows:
        lines.append(
            "- "
            f"{row.get('case_name')}: "
            f"builder={row.get('day_candidate_builder')}, "
            f"shortlist={row.get('direct_slot_shortlist_size')}, "
            f"runtime={row.get('total_runtime_seconds')}s, "
            f"pool_s={row.get('day_candidate_pool_generation_seconds')}s, "
            f"combo_s={row.get('combination_evaluation_seconds')}s, "
            f"candidates={row.get('candidate_day_pool_count')}, "
            f"accept/review/reject="
            f"{row.get('accept_candidate_day_count')}/"
            f"{row.get('review_candidate_day_count')}/"
            f"{row.get('reject_candidate_day_count')}, "
            f"no_repeat={row.get('feasible_no_repeat_combinations')}, "
            f"days accept/review={row.get('accept_day_count')}/"
            f"{row.get('review_day_count')}, "
            f"repeated={row.get('repeated_recipe_count')}, "
            f"loss={row.get('multi_day_loss')}"
        )
    lines.extend(["", "Assessment"])
    if recommended:
        lines.append(
            "- Recommended Streamlit/test default: "
            f"direct_from_slots shortlist={recommended.get('direct_slot_shortlist_size')}."
        )
        lines.append(
            "- The direct builder keeps a 3-day accept no-repeat plan and removes the repeated "
            "balanced selector bottleneck."
        )
        lines.append(f"- Selected plan: {recommended.get('selected_plan')}")
    else:
        lines.append(
            "- No direct run found a 3-day accept no-repeat plan; keep the balanced fallback for quality mode."
        )
    lines.append(
        "- Round33 baseline is read from the previous audit CSV instead of rerunning the slow path."
    )
    return "\n".join(lines) + "\n"


def _to_float(value: object) -> float:
    try:
        if pd.isna(value):
            return 0.0
    except (TypeError, ValueError):
        pass
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


if __name__ == "__main__":
    main()
