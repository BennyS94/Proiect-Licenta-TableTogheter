from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SUMMARY_PATH = PROJECT_ROOT / ".codex_runtime_logs/checks/recipesdb/mobile_knn_replacement_structure_summary.txt"

MOBILE_SRC = PROJECT_ROOT / "mobile/src"
API_CLIENT_PATH = MOBILE_SRC / "services/apiClient.ts"
API_TYPES_PATH = MOBILE_SRC / "types/api.ts"
PANEL_PATH = MOBILE_SRC / "components/RecipeAlternativesPanel.tsx"
MEAL_ROW_PATH = MOBILE_SRC / "components/MealRow.tsx"
HOUSEHOLD_MEAL_ROW_PATH = MOBILE_SRC / "components/HouseholdMealRow.tsx"
HOUSEHOLD_MEMBER_VIEW_PATH = MOBILE_SRC / "components/HouseholdMemberPlanView.tsx"
PLAN_DAY_CARD_PATH = MOBILE_SRC / "components/PlanDayCard.tsx"
HOME_SCREEN_PATH = MOBILE_SRC / "screens/HomeScreen.tsx"
README_PATH = PROJECT_ROOT / "mobile/README_mobile.md"

FORBIDDEN_MOBILE_MARKERS = [
    "data/recipesdb",
    "data\\recipesdb",
    "data/fooddb",
    "data\\fooddb",
    ".csv",
    "generator_v1",
    "src/legacy",
    "src\\legacy",
    "pandas",
    "readFileSync",
    "fs/promises",
]

FORBIDDEN_AUTO_REPLACEMENT_MARKERS = [
    "useEffect(() => { applyMealReplacement",
    "void applyMealReplacement(",
    "onPress={applyMealReplacement}",
]


def _read(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def _has_all(text: str, markers: list[str]) -> bool:
    return all(marker in text for marker in markers)


def _scan_mobile_forbidden(markers: list[str]) -> list[str]:
    hits: list[str] = []
    for path in sorted(MOBILE_SRC.rglob("*")):
        if not path.is_file() or path.suffix not in {".ts", ".tsx"}:
            continue
        text = path.read_text(encoding="utf-8")
        for marker in markers:
            if marker in text:
                hits.append(f"{path.relative_to(PROJECT_ROOT).as_posix()}:{marker}")
    return hits


def main() -> int:
    api_client = _read(API_CLIENT_PATH)
    api_types = _read(API_TYPES_PATH)
    panel = _read(PANEL_PATH)
    meal_row = _read(MEAL_ROW_PATH)
    household_meal_row = _read(HOUSEHOLD_MEAL_ROW_PATH)
    household_member_view = _read(HOUSEHOLD_MEMBER_VIEW_PATH)
    plan_day_card = _read(PLAN_DAY_CARD_PATH)
    home_screen = _read(HOME_SCREEN_PATH)
    readme = _read(README_PATH)

    checks = {
        "api_client_has_preview_and_apply": _has_all(
            api_client,
            [
                "previewMealReplacement",
                "applyMealReplacement",
                "/replace-meal?dry_run=true",
                "/replace-meal?dry_run=false",
            ],
        ),
        "api_types_have_replacement_contract": _has_all(
            api_types,
            [
                "MealReplacementRequest",
                "MealReplacementResponse",
                "MealReplacementImpact",
                "MealReplacementScope",
            ],
        ),
        "panel_has_preview_confirm_flow": _has_all(
            panel,
            [
                "Preview",
                "Replace meal",
                "Meal replaced. Plan and grocery list updated.",
                "This alternative cannot replace the meal yet.",
            ],
        ),
        "meal_row_passes_replacement_context": _has_all(
            meal_row,
            ["planId", "dayIndex", "onReplacementApplied", "replaceScope=\"individual_meal\""],
        ),
        "household_row_passes_replacement_context": _has_all(
            household_meal_row,
            [
                "generationType=\"household\"",
                "memberId={memberId}",
                "replacementScopeFromMeal",
                "household_shared_meal",
                "household_member_meal",
            ],
        ),
        "household_member_view_forwards_callback": _has_all(
            household_member_view,
            ["onReplacementApplied", "planId", "memberId={memberId}"],
        ),
        "plan_day_forwards_callback": _has_all(
            plan_day_card,
            ["onReplacementApplied", "planId", "dayIndex"],
        ),
        "home_screen_updates_plan_state": _has_all(
            home_screen,
            [
                "handleMealReplacementApplied",
                "setGeneratedPlan",
                "setGeneratedHouseholdPlan",
            ],
        ),
        "readme_mentions_knn4_replacement": _has_all(
            readme,
            ["KNN-4", "POST /plans/{plan_id}/replace-meal", "dry_run"],
        ),
    }

    forbidden_mobile_hits = _scan_mobile_forbidden(FORBIDDEN_MOBILE_MARKERS)
    forbidden_auto_hits = _scan_mobile_forbidden(FORBIDDEN_AUTO_REPLACEMENT_MARKERS)
    checks["no_forbidden_mobile_csv_or_generator_imports"] = not forbidden_mobile_hits
    checks["no_auto_apply_markers"] = not forbidden_auto_hits

    failed = [name for name, ok in checks.items() if not ok]
    status_ok = not failed

    summary_lines = [
        "Mobile KNN replacement structure summary",
        "status=ok" if status_ok else "status=failed",
        *[f"{name}={str(ok).lower()}" for name, ok in checks.items()],
        "forbidden_mobile_hits=" + (";".join(forbidden_mobile_hits) if forbidden_mobile_hits else "none"),
        "forbidden_auto_hits=" + (";".join(forbidden_auto_hits) if forbidden_auto_hits else "none"),
        "errors=" + (";".join(failed) if failed else "none"),
    ]
    SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)
    SUMMARY_PATH.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
    print("\n".join(summary_lines))
    return 0 if status_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
