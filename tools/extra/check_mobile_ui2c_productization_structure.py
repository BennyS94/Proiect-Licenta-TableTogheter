from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
MOBILE_DIR = PROJECT_ROOT / "mobile"
MOBILE_SRC = MOBILE_DIR / "src"
SUMMARY_PATH = PROJECT_ROOT / ".codex_runtime_logs/checks/recipesdb/mobile_ui2c_productization_structure_summary.txt"

FORBIDDEN_MEAL_PLAN_TEXT = [
    "1 / 3 / 5 days ready",
    "Generate individual",
    "Generate family",
    "Individual plan",
    "Household plan",
    "Demo Family Household",
    "Sample Household",
    "Generated household plan",
    "Status: ok",
    "Plan ID",
    "Accept/review/reject",
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


def _scan_forbidden_meal_plan_text() -> list[str]:
    files_to_scan = [
        MOBILE_SRC / "screens/MealPlanPage.tsx",
        MOBILE_SRC / "screens/HomeScreen.tsx",
        MOBILE_SRC / "components/HouseholdMemberPlanView.tsx",
        MOBILE_SRC / "components/PlanDayCard.tsx",
        MOBILE_SRC / "components/RecipeAlternativesPanel.tsx",
    ]
    hits: list[str] = []
    for path in files_to_scan:
        text = _read(path)
        for marker in FORBIDDEN_MEAL_PLAN_TEXT:
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
    meal_plan_page = _read(MOBILE_SRC / "screens/MealPlanPage.tsx")
    household_page = _read(MOBILE_SRC / "screens/HouseholdPage.tsx")
    profile_form = _read(MOBILE_SRC / "components/ProfileForm.tsx")
    day_selector = _read(MOBILE_SRC / "components/ui/DaySelector.tsx")
    member_view = _read(MOBILE_SRC / "components/HouseholdMemberPlanView.tsx")
    plan_day_card = _read(MOBILE_SRC / "components/PlanDayCard.tsx")
    readme = _read(MOBILE_DIR / "README_mobile.md")

    forbidden_meal_plan_hits = _scan_forbidden_meal_plan_text()
    forbidden_mobile_hits = _scan_mobile_forbidden_imports()

    checks = {
        "meal_plan_has_one_primary_generate_action": "Generate meal plan" in home_screen
        and "Generate individual" not in home_screen
        and "Generate family" not in home_screen,
        "meal_plan_header_is_clean": _has_all(
            meal_plan_page,
            ['<Text style={styles.title}>Meal Plan</Text>', "profileSelector ?"],
        )
        and "activeProfileName" not in meal_plan_page
        and "activeProfileMeta" not in meal_plan_page,
        "old_day_ready_copy_removed": "1 / 3 / 5 days ready" not in meal_plan_page,
        "logged_in_not_rendered_in_meal_plan": "Logged in." not in meal_plan_page,
        "internal_endpoint_choice_by_profile_count": _has_all(
            home_screen,
            ["orderedProfiles.length === 1", "generateIndividualPlan(", "generateHouseholdPlan("],
        ),
        "day_generation_slider_like_control_exists": _has_all(
            home_screen,
            ["PanResponder.create", "sliderTrack", "sliderTrackFill", "sliderDot", "sliderThumb"],
        ),
        "generated_day_selector_has_day_1_to_5": _has_all(
            day_selector + member_view,
            ["1, 2, 3, 4, 5", "disabled", "buttonDisabled", "dayButtonDisabled"],
        ),
        "generated_day_selector_single_row": _has_all(
            day_selector,
            ['flexDirection: "row"', 'width: "100%"', "flex: 1"],
        )
        and _has_all(member_view, ['flexDirection: "row"', 'width: "100%"', "flex: 1"])
        and 'flexWrap: "wrap"' not in member_view,
        "meal_order_enforced": _has_all(
            home_screen + member_view + plan_day_card,
            ['"breakfast"', '"lunch"', '"snack"', '"dinner"', "sortMealsBySlot"],
        ),
        "login_401_friendly_message": _has_all(
            api_client,
            ["response.status === 401", "Invalid email or password"],
        ),
        "settings_coming_soon_indicators_exist": _has_all(
            household_page,
            ["Change Email", "Change Password", "Coming soon", "Language", "English"],
        ),
        "profile_validation_code_exists": _has_all(
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
        "readme_mentions_ui2c": "UI-2C" in readme,
        "no_forbidden_meal_plan_text": not forbidden_meal_plan_hits,
        "no_forbidden_mobile_csv_or_generator_imports": not forbidden_mobile_hits,
    }

    failed = [name for name, ok in checks.items() if not ok]
    status_ok = not failed
    summary_lines = [
        "Mobile UI-2C productization structure summary",
        "status=ok" if status_ok else "status=failed",
        *[f"{name}={str(ok).lower()}" for name, ok in checks.items()],
        "forbidden_meal_plan_hits="
        + (";".join(forbidden_meal_plan_hits) if forbidden_meal_plan_hits else "none"),
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
