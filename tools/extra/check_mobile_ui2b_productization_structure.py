from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
MOBILE_DIR = PROJECT_ROOT / "mobile"
MOBILE_SRC = MOBILE_DIR / "src"
SUMMARY_PATH = PROJECT_ROOT / ".codex_runtime_logs/checks/recipesdb/mobile_ui2b_productization_structure_summary.txt"

FORBIDDEN_MAIN_FLOW_TEXT = [
    "Demo Family Household",
    "Load Demo",
    "Generated household plan",
    "Generated plan",
    "Status: ok",
    "Plan ID",
    "Selected members",
    "Selected source",
    "Accept/review/reject",
    "Generate individual",
    "Generate family",
    "Individual plan",
    "Household plan",
    "Alternatives are read-only",
    "Pool ",
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

VISIBLE_SOURCE_FILES = [
    MOBILE_SRC / "screens/HomeScreen.tsx",
    MOBILE_SRC / "screens/HomePage.tsx",
    MOBILE_SRC / "screens/MealPlanPage.tsx",
    MOBILE_SRC / "screens/HouseholdPage.tsx",
    MOBILE_SRC / "screens/InsightsPage.tsx",
    MOBILE_SRC / "components/RecipeAlternativesPanel.tsx",
    MOBILE_SRC / "components/HouseholdMemberPlanView.tsx",
    MOBILE_SRC / "components/PlanDayCard.tsx",
    MOBILE_SRC / "components/ProfileForm.tsx",
    MOBILE_SRC / "components/GroceryListSection.tsx",
    MOBILE_SRC / "components/GroceryItemRow.tsx",
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


def _scan_forbidden_text() -> list[str]:
    hits: list[str] = []
    for path in VISIBLE_SOURCE_FILES:
        text = _read(path)
        for marker in FORBIDDEN_MAIN_FLOW_TEXT:
            if marker in text:
                hits.append(f"{path.relative_to(PROJECT_ROOT).as_posix()}:{marker}")
    return hits


def _scan_mobile_forbidden_imports() -> list[str]:
    hits: list[str] = []
    for path in _mobile_source_files():
        text = _read(path)
        for marker in FORBIDDEN_MOBILE_MARKERS:
            if marker in text:
                hits.append(f"{path.relative_to(PROJECT_ROOT).as_posix()}:{marker}")
    return hits


def main() -> int:
    api_client = _read(MOBILE_SRC / "services/apiClient.ts")
    home_screen = _read(MOBILE_SRC / "screens/HomeScreen.tsx")
    household_page = _read(MOBILE_SRC / "screens/HouseholdPage.tsx")
    profile_form = _read(MOBILE_SRC / "components/ProfileForm.tsx")
    day_selector = _read(MOBILE_SRC / "components/ui/DaySelector.tsx")
    member_view = _read(MOBILE_SRC / "components/HouseholdMemberPlanView.tsx")
    plan_day_card = _read(MOBILE_SRC / "components/PlanDayCard.tsx")
    alternatives = _read(MOBILE_SRC / "components/RecipeAlternativesPanel.tsx")
    theme = _read(MOBILE_SRC / "theme/colors.ts")
    readme = _read(MOBILE_DIR / "README_mobile.md")

    forbidden_text_hits = _scan_forbidden_text()
    forbidden_mobile_hits = _scan_mobile_forbidden_imports()

    checks = {
        "login_401_friendly_message": _has_all(
            api_client,
            ["response.status === 401", "Invalid email or password"],
        ),
        "one_generate_meal_plan_action": "Generate meal plan" in home_screen
        and "Individual plan" not in home_screen
        and "Household plan" not in home_screen,
        "internal_endpoint_choice_by_profile_count": _has_all(
            home_screen,
            [
                "orderedProfiles.length === 1",
                "generateIndividualPlan(",
                "generateHouseholdPlan(",
            ],
        ),
        "profile_form_hides_household_id": "Household ID" not in profile_form,
        "profile_validation_exists": _has_all(
            profile_form,
            [
                "Name cannot contain numbers.",
                "Age must be between 4 and 120.",
                "Weight must be between 15 and 300 kg.",
                "Height must be between 80 and 230 cm.",
                "Sessions per week must be between 0 and 7.",
                "Meals per day must be between 1 and 5.",
                'goal === "maintain"',
            ],
        ),
        "settings_mark_coming_soon": _has_all(
            household_page,
            ["Change Email", "Change Password", "Coming soon", "Language", "English"],
        ),
        "pear_green_theme_exists": "#74B72E" in theme,
        "day_selector_has_disabled_days": _has_all(
            day_selector + member_view,
            ["1, 2, 3, 4, 5", "disabled", "buttonDisabled", "dayButtonDisabled"],
        ),
        "meal_order_enforced_in_ui": _has_all(
            home_screen + member_view + plan_day_card,
            ['"breakfast"', '"lunch"', '"snack"', '"dinner"', "sortMealsBySlot"],
        ),
        "target_macro_fallbacks_exist": _has_all(
            home_screen + member_view,
            ["target_protein_g", "target_carbs_g", "target_fat_g"],
        ),
        "alternatives_hide_debug_and_support_shuffle": _has_all(
            alternatives,
            ["Shuffle", "Preview changes", "Replace meal"],
        )
        and "Pool " not in alternatives
        and "why_similar" not in alternatives,
        "readme_mentions_ui2b": "UI-2B" in readme,
        "no_forbidden_visible_main_flow_text": not forbidden_text_hits,
        "no_forbidden_mobile_csv_or_generator_imports": not forbidden_mobile_hits,
    }

    failed = [name for name, ok in checks.items() if not ok]
    status_ok = not failed
    summary_lines = [
        "Mobile UI-2B productization structure summary",
        "status=ok" if status_ok else "status=failed",
        *[f"{name}={str(ok).lower()}" for name, ok in checks.items()],
        "forbidden_visible_hits="
        + (";".join(forbidden_text_hits) if forbidden_text_hits else "none"),
        "forbidden_mobile_hits="
        + (";".join(forbidden_mobile_hits) if forbidden_mobile_hits else "none"),
        "errors=" + (";".join(failed) if failed else "none"),
    ]

    SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)
    SUMMARY_PATH.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
    print("\n".join(summary_lines))
    return 0 if status_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
