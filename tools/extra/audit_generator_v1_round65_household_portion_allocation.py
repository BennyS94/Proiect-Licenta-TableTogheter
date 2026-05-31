from __future__ import annotations

import pandas as pd

from round65_household_common import (  # noqa: E402
    AUDIT_DIR,
    allocation_rows_for_plan,
    format_counts,
    generate_primary_member_plan,
    load_household_profile,
    member_target_rows,
)


SUMMARY_OUT = AUDIT_DIR / "generator_v1_round65_household_portion_allocation_summary.txt"
ALLOCATION_OUT = AUDIT_DIR / "generator_v1_round65_household_portion_allocation.csv"

ALLOCATION_METHODS = (
    "equal_portions",
    "proportional_to_daily_kcal",
    "proportional_to_slot_kcal",
    "macro_aware_simple",
)


def main() -> None:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    household_profile = load_household_profile()
    _, targets = member_target_rows(household_profile)
    generation = generate_primary_member_plan(household_profile, days=3)
    shared_slots = set(household_profile.get("planning_config", {}).get("shared_meals", []))
    rows = allocation_rows_for_plan(
        household_profile,
        generation["plan"],
        targets,
        methods=ALLOCATION_METHODS,
        shared_slots=shared_slots,
    )
    pd.DataFrame(rows).to_csv(ALLOCATION_OUT, index=False)
    SUMMARY_OUT.write_text(
        _summary_text(household_profile, generation, rows),
        encoding="utf-8",
    )
    print("Round65 household portion allocation audit written")
    print(f"summary={SUMMARY_OUT}")
    print(f"allocation={ALLOCATION_OUT}")
    print(f"rows={len(rows)}")


def _summary_text(
    household_profile: dict[str, object],
    generation: dict[str, object],
    rows: list[dict[str, object]],
) -> str:
    dataset = generation.get("dataset", {})
    plan = generation.get("plan", {})
    plan_summary = plan.get("multi_day_summary", {}) if isinstance(plan, dict) else {}
    frame = pd.DataFrame(rows)
    method_summaries = _method_summaries(frame)
    best_method = _best_method(method_summaries)
    warning_counts = (
        frame["household_portion_fit_warning"]
        .fillna("")
        .astype(str)
        .str.split(";")
        .explode()
        .loc[lambda series: series.astype(str).str.strip().ne("")]
        .value_counts()
        if not frame.empty
        else pd.Series(dtype=int)
    )
    lines = [
        "Round65 Household portion allocation audit",
        "",
        f"household_id={household_profile.get('household_id')}",
        f"dataset_profile={dataset.get('dataset_profile') if isinstance(dataset, dict) else ''}",
        f"generated_days={plan_summary.get('actual_days_generated')}",
        f"valid_day_count={plan_summary.get('valid_day_count')}",
        f"accept_day_count={plan_summary.get('accept_day_count')}",
        f"multi_day_loss={plan_summary.get('multi_day_loss')}",
        f"shared_slots={', '.join(household_profile.get('planning_config', {}).get('shared_meals', []))}",
        f"allocation_methods={', '.join(ALLOCATION_METHODS)}",
        f"row_count={len(rows)}",
        f"warning_counts={format_counts(warning_counts)}",
        "",
        "Method summaries:",
    ]
    for method, summary in method_summaries.items():
        lines.append(
            "- "
            f"{method}: "
            f"mean_abs_kcal_deviation_pct={summary['mean_abs_kcal_deviation_pct']}, "
            f"mean_abs_protein_deviation_pct={summary['mean_abs_protein_deviation_pct']}, "
            f"review_rows={summary['review_rows']}, "
            f"clamped_rows={summary['clamped_rows']}"
        )
    lines.extend(
        [
            "",
            f"best_prototype_method={best_method}",
            "",
            "Interpretation:",
            "- equal_portions este baza de comparatie, nu recomandarea principala.",
            "- proportional_to_slot_kcal este simplu si stabil pentru prima versiune.",
            "- macro_aware_simple reduce unele diferente, dar ramane euristic si poate cere review pe proteina/carbs.",
            "- Acest script nu schimba generatorul; doar masoara cum ar arata portii per-member peste retetele shared.",
        ]
    )
    return "\n".join(lines) + "\n"


def _method_summaries(frame: pd.DataFrame) -> dict[str, dict[str, object]]:
    summaries: dict[str, dict[str, object]] = {}
    if frame.empty:
        return summaries
    for method, group in frame.groupby("method"):
        warnings = group["household_portion_fit_warning"].fillna("").astype(str)
        summaries[str(method)] = {
            "mean_abs_kcal_deviation_pct": round(
                group["kcal_deviation_pct"].abs().mean(),
                1,
            ),
            "mean_abs_protein_deviation_pct": round(
                group["protein_deviation_pct"].abs().mean(),
                1,
            ),
            "mean_abs_carbs_deviation_pct": round(
                group["carbs_deviation_pct"].abs().mean(),
                1,
            ),
            "mean_abs_fat_deviation_pct": round(
                group["fat_deviation_pct"].abs().mean(),
                1,
            ),
            "review_rows": int(warnings.str.strip().ne("").sum()),
            "clamped_rows": int(group["portion_multiplier_clamped"].astype(bool).sum()),
        }
    return summaries


def _best_method(method_summaries: dict[str, dict[str, object]]) -> str:
    if not method_summaries:
        return "not_available"
    ranked = sorted(
        method_summaries.items(),
        key=lambda item: (
            float(item[1]["mean_abs_kcal_deviation_pct"]),
            float(item[1]["mean_abs_protein_deviation_pct"]),
            int(item[1]["review_rows"]),
        ),
    )
    return ranked[0][0]


if __name__ == "__main__":
    main()
