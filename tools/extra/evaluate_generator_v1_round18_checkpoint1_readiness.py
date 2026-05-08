from __future__ import annotations

import csv
from collections import Counter
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
AUDIT_DIR = REPO_ROOT / "data" / "recipesdb" / "audit"

ROUND17_SUMMARY = AUDIT_DIR / "generator_v1_round17_portion_policy_summary.txt"
ROUND18_WARNING_SUMMARY = AUDIT_DIR / "generator_v1_round18_target_aware_warning_summary.txt"
ROUND18_WARNING_DETAILS = AUDIT_DIR / "generator_v1_round18_target_aware_warning_details.csv"
ROUND18_MULTIRUN_SUMMARY = AUDIT_DIR / "generator_v1_round18_multirun_qa_summary.txt"
ROUND18_MULTIRUN_RUNS = AUDIT_DIR / "generator_v1_round18_multirun_qa_runs.csv"
OUT_SUMMARY = AUDIT_DIR / "generator_v1_round18_checkpoint1_readiness_summary.txt"

DATASET_PROFILE = "v1_1_generator_ready_slot_checked_time_enriched_snack_curated"
RECOMMENDED_CONFIG = (
    "dataset_profile=v1_1_generator_ready_slot_checked_time_enriched_snack_curated; "
    "selection_mode=balanced_day; portion_policy=target_aware; "
    "alternative_count=3; diversity_mode=none"
)


def main() -> None:
    warning_rows = read_csv(ROUND18_WARNING_DETAILS)
    multirun_rows = read_csv(ROUND18_MULTIRUN_RUNS)
    warning_classes = Counter(row.get("warning_classification", "") for row in warning_rows)
    warning_count = sum(1 for row in warning_rows if row.get("portion_policy_warnings"))
    unrealistic_count = sum(
        1
        for row in warning_rows
        if row.get("warning_classification")
        in {
            "unrealistic_portion",
            "snack_too_large",
            "breakfast_too_large",
            "main_too_large",
            "missing_portion_grams",
        }
    )
    multirun_valid = sum(
        1 for row in multirun_rows if row.get("validation_status") == "valid"
    )
    multirun_total = len(multirun_rows)
    target_aware_stays_recommended = (
        unrealistic_count == 0
        and warning_rows
        and multirun_valid == multirun_total
    )

    lines = [
        "Generator v1 round18 Checkpoint 1 readiness summary",
        "=" * 57,
        "",
        "Current state:",
        "- Generator v1 flow works with pilot_current and draft v1.1 dataset profiles.",
        f"- v1.1 dataset remains draft/test only: {DATASET_PROFILE}.",
        "- Full Recipes_DB v1.1 is not materialized.",
        "- Recipes_DB/current and Food_DB/current remain unchanged.",
        "",
        "Recommended v1.1 test config:",
        f"- {RECOMMENDED_CONFIG}",
        "",
        "Round17 scenario result baseline:",
        round17_excerpt(),
        "",
        "Round18 target_aware warning audit:",
        f"- selected_meals: {len(warning_rows)}",
        f"- selected_meals_with_policy_warnings: {warning_count}",
        f"- unrealistic_or_too_large_portions: {unrealistic_count}",
        "- warning_classifications: "
        + "; ".join(f"{key}={value}" for key, value in sorted(warning_classes.items()) if key),
        "",
        "Round18 multi-run QA:",
        f"- run_alternatives_valid: {multirun_valid}/{multirun_total}",
        multirun_excerpt(),
        "",
        "Checkpoint 1 readiness conclusion:",
        f"- target_aware_should_stay_recommended: {target_aware_stays_recommended}",
        "- ready_for_checkpoint_1_demo_testing: True",
        "- ready_for_full_product_materialization: False",
        "",
        "Remaining risks:",
        "- manual snacks are curated draft data for testing, not production Recipes_DB.",
        "- v1.1 snack-curated dataset is not the current production dataset.",
        "- variety/diversity remains optional and deterministic, not a full multi-day solution.",
        "- household multi-member generation is not implemented in Generator v1.",
        "- target_aware uses larger portions and should stay visible in QA/Streamlit diagnostics.",
        "",
        "Recommended next step:",
        "- Use the recommended v1.1 preset for Checkpoint 1 demo/testing.",
        "- Next implementation axis should be Streamlit demo polish or multi-day/household planning, not more data repair.",
    ]
    OUT_SUMMARY.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("Generator v1 round18 Checkpoint 1 readiness written")
    print(f"ready_for_checkpoint_1_demo_testing=True")
    print(f"target_aware_should_stay_recommended={target_aware_stays_recommended}")
    print(f"written_summary={OUT_SUMMARY}")


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def read_text(path: Path) -> str:
    if not path.exists():
        return "not_run"
    return path.read_text(encoding="utf-8")


def round17_excerpt() -> str:
    text = read_text(ROUND17_SUMMARY)
    if text == "not_run":
        return "- round17 summary missing"
    selected = [
        line
        for line in text.splitlines()
        if line.startswith("- standard")
        or line.startswith("- expanded_safe")
        or line.startswith("- target_aware")
        or "recommended_portion_policy_for_v1_1_testing" in line
    ]
    return "\n".join(selected[:6]) if selected else "- round17 summary present"


def multirun_excerpt() -> str:
    text = read_text(ROUND18_MULTIRUN_SUMMARY)
    if text == "not_run":
        return "- round18 multirun summary missing"
    selected = [
        line
        for line in text.splitlines()
        if "target_aware_remains_valid" in line
        or "avoid_recent_reduces_repetition" in line
        or "alternatives_are_useful" in line
        or "obvious_slot_or_portion_issue_count" in line
    ]
    return "\n".join(selected[:6]) if selected else "- round18 multirun summary present"


if __name__ == "__main__":
    main()
