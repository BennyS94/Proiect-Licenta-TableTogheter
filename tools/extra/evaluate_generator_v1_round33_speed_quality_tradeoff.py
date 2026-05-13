from __future__ import annotations

import sys
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.extra.profile_generator_v1_round33_multiday_performance import (
    POOL_SIZES,
    V1_2_GENERATOR_READY_PLUS30_PLUS15_PROFILE,
    build_candidate_pool,
    build_generation_context,
    run_selector_case,
)


OUT_DIR = REPO_ROOT / "data" / "recipesdb" / "audit"
OUT_SUMMARY = OUT_DIR / "generator_v1_round33_speed_quality_tradeoff_summary.txt"
OUT_CSV = OUT_DIR / "generator_v1_round33_speed_quality_tradeoff.csv"

SPEED_MODES = ["fast", "quality"]


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    context = build_generation_context()
    rows: list[dict[str, Any]] = []
    shared_pools: dict[str, tuple[list[dict[str, Any]], dict[str, Any], float]] = {}

    for speed_mode in SPEED_MODES:
        started = time.perf_counter()
        candidate_days, pool_stats = build_candidate_pool(
            context=context,
            pool_size=max(POOL_SIZES),
            speed_mode=speed_mode,
        )
        shared_pools[speed_mode] = (
            candidate_days,
            pool_stats,
            time.perf_counter() - started,
        )

    for speed_mode in SPEED_MODES:
        candidate_days, pool_stats, pool_seconds = shared_pools[speed_mode]
        for pool_size in POOL_SIZES:
            row = run_selector_case(
                context=context,
                pool_size=pool_size,
                speed_mode=speed_mode,
                candidate_days=candidate_days,
                pool_stats=pool_stats,
                pool_generation_seconds=pool_seconds,
                reused_candidate_pool=pool_size != max(POOL_SIZES),
            )
            row["recommended_streamlit_default"] = (
                speed_mode == "fast" and pool_size == 50
            )
            rows.append(row)

    pd.DataFrame(rows).to_csv(OUT_CSV, index=False)
    OUT_SUMMARY.write_text(build_summary_text(rows), encoding="utf-8")

    recommended = next(row for row in rows if row["recommended_streamlit_default"])
    print("Generator v1 Round33 speed/quality tradeoff evaluation written")
    print(f"summary={OUT_SUMMARY}")
    print(f"csv={OUT_CSV}")
    print(
        "recommended="
        f"pool:{recommended['pool_size_requested']} "
        f"speed:{recommended['speed_mode']} "
        f"runtime:{recommended['total_runtime_seconds']} "
        f"repeated:{recommended['repeated_recipe_count']} "
        f"loss:{recommended['multi_day_loss']}"
    )


def build_summary_text(rows: Sequence[Mapping[str, Any]]) -> str:
    recommended = next(row for row in rows if row["recommended_streamlit_default"])
    quality = next(
        row
        for row in rows
        if row["speed_mode"] == "quality" and int(row["pool_size_requested"]) == 100
    )
    lines = [
        "Round33 speed / quality tradeoff",
        "",
        f"- dataset_profile: {V1_2_GENERATOR_READY_PLUS30_PLUS15_PROFILE}",
        "",
        "Rows",
    ]
    for row in rows:
        lines.append(
            "- "
            f"pool={row['pool_size_requested']}, speed={row['speed_mode']}, "
            f"runtime={row['total_runtime_seconds']}s, "
            f"candidates={row['candidate_day_pool_count']}, "
            f"accept={row['accept_day_count']}, review={row['review_day_count']}, "
            f"unique={row['unique_recipe_count']}, repeated={row['repeated_recipe_count']}, "
            f"no_repeat={row['feasible_no_repeat_combinations']}, "
            f"loss={row['multi_day_loss']}, verdict={row['strict_verdict']}"
        )
    lines.extend(
        [
            "",
            "Recommended Streamlit default",
            f"- pool={recommended['pool_size_requested']}",
            f"- speed_mode={recommended['speed_mode']}",
            f"- no_repeat_policy=hard",
            f"- expected_runtime_seconds={recommended['total_runtime_seconds']}",
            f"- repeated_recipe_count={recommended['repeated_recipe_count']}",
            f"- multi_day_loss={recommended['multi_day_loss']}",
            "",
            "Quality reference",
            f"- pool=100 quality runtime={quality['total_runtime_seconds']}s",
            f"- repeated_recipe_count={quality['repeated_recipe_count']}",
            f"- multi_day_loss={quality['multi_day_loss']}",
            "",
            "Assessment",
            "- Fast mode is materially faster than the old Round32 runtime, but still not interactive-fast.",
            "- No <=30s mode was found without risking quality; Streamlit should warn users.",
        ]
    )
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    main()
