from __future__ import annotations

import pandas as pd

from round65_household_common import (  # noqa: E402
    AUDIT_DIR,
    active_members,
    format_counts,
    generate_primary_member_plan,
    load_household_profile,
    member_target_rows,
    recommended_household_mode,
    scenario_member_macro_rows,
    summarize_feasibility,
)


SUMMARY_OUT = AUDIT_DIR / "generator_v1_round65_household_shared_meal_feasibility_summary.txt"
DAYS_OUT = AUDIT_DIR / "generator_v1_round65_household_shared_meal_days.csv"
MEMBER_MACROS_OUT = AUDIT_DIR / "generator_v1_round65_household_shared_meal_member_macros.csv"
WARNINGS_OUT = AUDIT_DIR / "generator_v1_round65_household_shared_meal_warnings.csv"
GROCERY_SCALING_OUT = AUDIT_DIR / "generator_v1_round65_household_grocery_scaling.csv"

SCENARIOS = {
    "shared_dinner_only": {"dinner"},
    "shared_lunch_dinner": {"lunch", "dinner"},
    "shared_all_except_snack": {"breakfast", "lunch", "dinner"},
    "individual_breakfast_shared_main": {"lunch", "dinner"},
}


def main() -> None:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    household_profile = load_household_profile()
    _, targets = member_target_rows(household_profile)
    generation = generate_primary_member_plan(household_profile, days=3)
    member_macro_rows: list[dict[str, object]] = []
    warning_rows: list[dict[str, object]] = []
    grocery_rows: list[dict[str, object]] = []
    for scenario_name, shared_slots in SCENARIOS.items():
        scenario_rows, scenario_warnings, scenario_grocery = scenario_member_macro_rows(
            household_profile,
            generation["plan"],
            targets,
            scenario_name=scenario_name,
            shared_slots=shared_slots,
        )
        member_macro_rows.extend(scenario_rows)
        warning_rows.extend(scenario_warnings)
        grocery_rows.extend(scenario_grocery)

    days_rows = _day_summary_rows(member_macro_rows)
    pd.DataFrame(days_rows).to_csv(DAYS_OUT, index=False)
    pd.DataFrame(member_macro_rows).to_csv(MEMBER_MACROS_OUT, index=False)
    pd.DataFrame(warning_rows).to_csv(WARNINGS_OUT, index=False)
    pd.DataFrame(grocery_rows).to_csv(GROCERY_SCALING_OUT, index=False)
    SUMMARY_OUT.write_text(
        _summary_text(
            household_profile=household_profile,
            generation=generation,
            member_rows=member_macro_rows,
            warning_rows=warning_rows,
            grocery_rows=grocery_rows,
        ),
        encoding="utf-8",
    )
    print("Round65 household shared meal feasibility audit written")
    print(f"summary={SUMMARY_OUT}")
    print(f"days={DAYS_OUT}")
    print(f"member_macros={MEMBER_MACROS_OUT}")
    print(f"warnings={WARNINGS_OUT}")
    print(f"grocery_scaling={GROCERY_SCALING_OUT}")


def _summary_text(
    *,
    household_profile: dict[str, object],
    generation: dict[str, object],
    member_rows: list[dict[str, object]],
    warning_rows: list[dict[str, object]],
    grocery_rows: list[dict[str, object]],
) -> str:
    dataset = generation.get("dataset", {})
    plan = generation.get("plan", {})
    plan_summary = plan.get("multi_day_summary", {}) if isinstance(plan, dict) else {}
    feasibility = summarize_feasibility(member_rows)
    recommendation = recommended_household_mode(feasibility)
    warning_counts = (
        pd.Series([row.get("warning_code") for row in warning_rows]).value_counts()
        if warning_rows
        else pd.Series(dtype=int)
    )
    grocery_frame = pd.DataFrame(grocery_rows)
    max_factor = (
        round(pd.to_numeric(grocery_frame["household_quantity_factor"], errors="coerce").max(), 2)
        if not grocery_frame.empty
        else 0.0
    )
    lines = [
        "Round65 Household shared meal feasibility audit",
        "",
        f"household_id={household_profile.get('household_id')}",
        f"member_count={len(active_members(household_profile))}",
        f"dataset_profile={dataset.get('dataset_profile') if isinstance(dataset, dict) else ''}",
        f"generated_days={plan_summary.get('actual_days_generated')}",
        f"valid_day_count={plan_summary.get('valid_day_count')}",
        f"accept_day_count={plan_summary.get('accept_day_count')}",
        f"multi_day_loss={plan_summary.get('multi_day_loss')}",
        f"warning_counts={format_counts(warning_counts)}",
        f"max_household_quantity_factor={max_factor}",
        "",
        "Scenario feasibility:",
    ]
    for scenario, summary in feasibility.items():
        lines.append(
            "- "
            f"{scenario}: "
            f"feasible={summary['feasible']}, "
            f"mean_abs_kcal_deviation_pct={summary['mean_abs_kcal_deviation_pct']}, "
            f"max_abs_kcal_deviation_pct={summary['max_abs_kcal_deviation_pct']}, "
            f"mean_abs_protein_deviation_pct={summary['mean_abs_protein_deviation_pct']}"
        )
    lines.extend(
        [
            "",
            f"recommended_first_implementation_mode={recommendation}",
            "",
            "Household grocery implication:",
            "- Pentru o reteta shared, cantitatea household = ingredient grams * suma multiplicatorilor membrilor.",
            "- Grocery scaling este cantitativ doar; nu include pret, pantry, pachete sau optimizare.",
            f"- Factorul household observat in audit are maxim {max_factor}x fata de portia single-profile.",
            "",
            "Interpretation:",
            "- Shared dinner only este cel mai sigur mod daca devierile macro cresc pentru lunch+dinner.",
            "- Individual breakfast/snack pot functiona ca top-up pentru diferentele de kcal/protein/carbs.",
            "- Full household generator nu este implementat aici; acesta este doar audit de fezabilitate.",
        ]
    )
    return "\n".join(lines) + "\n"


def _day_summary_rows(member_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    frame = pd.DataFrame(member_rows)
    if frame.empty:
        return []
    rows: list[dict[str, object]] = []
    for (scenario, day_index), group in frame.groupby(["scenario", "day_index"]):
        rows.append(
            {
                "scenario": scenario,
                "day_index": day_index,
                "member_count": int(group["member_id"].nunique()),
                "mean_abs_kcal_deviation_pct": round(
                    group["kcal_deviation_pct"].abs().mean(),
                    1,
                ),
                "max_abs_kcal_deviation_pct": round(
                    group["kcal_deviation_pct"].abs().max(),
                    1,
                ),
                "mean_abs_protein_deviation_pct": round(
                    group["protein_deviation_pct"].abs().mean(),
                    1,
                ),
                "max_abs_protein_deviation_pct": round(
                    group["protein_deviation_pct"].abs().max(),
                    1,
                ),
            }
        )
    return rows


if __name__ == "__main__":
    main()
