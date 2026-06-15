from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
MOBILE_DIR = PROJECT_ROOT / "mobile"
MOBILE_SRC = MOBILE_DIR / "src"
SUMMARY_PATH = PROJECT_ROOT / "data/recipesdb/audit/mobile_ui_shell_structure_summary.txt"

REQUIRED_FILES = [
    "src/screens/HomePage.tsx",
    "src/screens/MealPlanPage.tsx",
    "src/screens/InsightsPage.tsx",
    "src/screens/HouseholdPage.tsx",
    "src/components/navigation/FloatingNav.tsx",
    "src/components/ui/AppScreen.tsx",
    "src/components/ui/AppCard.tsx",
    "src/components/ui/SectionHeader.tsx",
    "src/components/ui/EmptyState.tsx",
]

SHELL_MARKERS = [
    "HomePage",
    "MealPlanPage",
    "InsightsPage",
    "HouseholdPage",
    "FloatingNav",
    'home"',
    'mealPlan"',
    'insights"',
    'household"',
]

REAL_FLOW_MARKERS = [
    "getHealth",
    "getDemoHousehold",
    "getProfiles",
    "createProfile",
    "deleteProfile",
    "generateIndividualPlan",
    "generateHouseholdPlan",
    "GroceryListSection",
    "PlanDayCard",
    "HouseholdMemberPlanView",
    "handleMealReplacementApplied",
    "submitMealFeedback",
    "undoMealFeedback",
    "Refresh feedback context",
    "Clear feedback",
]

MEAL_ROW_MARKERS = [
    "Cook / Steps",
    "Alternatives",
    "RecipeAlternativesPanel",
    "onReplacementApplied",
]

MEAL_ROW_FORBIDDEN_MARKERS = [
    "Details",
]

GUARD_MARKERS = [
    "Set up your household",
    "No household members yet",
    "No profile data available",
    "No insights yet",
    "No meal plan yet",
]

README_MARKERS = [
    "UI-1",
    "4-page",
    "Home este hardcoded",
    "Meal Plan",
    "Insights",
    "Household",
    "telefon fizic",
]

FORBIDDEN_MOBILE_MARKERS = [
    "data/recipesdb/current",
    "data\\recipesdb\\current",
    "data/fooddb/current",
    "data\\fooddb\\current",
    ".csv",
    "generator_v1",
    "src/legacy",
    "src\\legacy",
    "readFileSync",
    "fs/promises",
    "pandas",
]


def _read(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8", errors="replace")


def _has_all(text: str, markers: list[str]) -> bool:
    return all(marker in text for marker in markers)


def _mobile_source_files() -> list[Path]:
    if not MOBILE_SRC.exists():
        return []
    return [
        path
        for path in MOBILE_SRC.rglob("*")
        if path.is_file() and path.suffix in {".ts", ".tsx", ".js", ".jsx"}
    ]


def _scan_mobile_forbidden() -> list[str]:
    hits: list[str] = []
    for path in _mobile_source_files():
        text = _read(path)
        for marker in FORBIDDEN_MOBILE_MARKERS:
            if marker in text:
                hits.append(f"{path.relative_to(PROJECT_ROOT).as_posix()}:{marker}")
    return hits


def main() -> int:
    shell = _read(MOBILE_SRC / "screens/HomeScreen.tsx")
    meal_plan_page = _read(MOBILE_SRC / "screens/MealPlanPage.tsx")
    home_page = _read(MOBILE_SRC / "screens/HomePage.tsx")
    insights_page = _read(MOBILE_SRC / "screens/InsightsPage.tsx")
    household_page = _read(MOBILE_SRC / "screens/HouseholdPage.tsx")
    meal_row = _read(MOBILE_SRC / "components/MealRow.tsx")
    household_meal_row = _read(MOBILE_SRC / "components/HouseholdMealRow.tsx")
    readme = _read(MOBILE_DIR / "README_mobile.md")

    missing_files = [
        relative_path
        for relative_path in REQUIRED_FILES
        if not (MOBILE_DIR / relative_path).exists()
    ]

    checks = {
        "required_files_exist": not missing_files,
        "app_shell_references_all_pages": _has_all(shell, SHELL_MARKERS),
        "meal_plan_page_has_tabs_and_guards": _has_all(
            meal_plan_page,
            ["Meal Plan", "Grocery List", "Generate meal plan", "No meal plan yet"],
        ),
        "real_flows_preserved_in_shell": _has_all(shell, REAL_FLOW_MARKERS),
        "meal_rows_have_cook_and_alternatives": _has_all(
            meal_row + household_meal_row,
            MEAL_ROW_MARKERS,
        ),
        "meal_rows_do_not_show_details_action": not any(
            marker in meal_row + household_meal_row for marker in MEAL_ROW_FORBIDDEN_MARKERS
        ),
        "guard_states_present": _has_all(
            shell + home_page + meal_plan_page + insights_page,
            GUARD_MARKERS,
        ),
        "household_page_has_account_sections": _has_all(
            household_page,
            [
                "Account Settings",
                "Household Management",
                "Default Viewer",
                "App Settings",
                "Change Email",
                "Change Password",
            ],
        ),
        "readme_mentions_ui1": _has_all(readme, README_MARKERS),
    }

    forbidden_hits = _scan_mobile_forbidden()
    checks["no_forbidden_mobile_csv_or_generator_imports"] = not forbidden_hits

    failed = [name for name, ok in checks.items() if not ok]
    status_ok = not failed
    summary_lines = [
        "Mobile UI shell structure summary",
        "status=ok" if status_ok else "status=failed",
        f"required_file_count={len(REQUIRED_FILES)}",
        "missing_files=" + (",".join(missing_files) if missing_files else "none"),
        *[f"{name}={str(ok).lower()}" for name, ok in checks.items()],
        "forbidden_mobile_hits=" + (";".join(forbidden_hits) if forbidden_hits else "none"),
        "errors=" + (";".join(failed) if failed else "none"),
    ]

    SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)
    SUMMARY_PATH.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
    print("\n".join(summary_lines))
    return 0 if status_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
