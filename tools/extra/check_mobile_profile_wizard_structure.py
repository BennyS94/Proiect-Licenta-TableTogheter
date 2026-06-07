from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
MOBILE_DIR = PROJECT_ROOT / "mobile"
MOBILE_SRC = MOBILE_DIR / "src"
SUMMARY_PATH = PROJECT_ROOT / "data/recipesdb/audit/mobile_profile_wizard_structure_summary.txt"

WIZARD_PATH = MOBILE_SRC / "components/AddMemberWizard.tsx"
HOME_SCREEN_PATH = MOBILE_SRC / "screens/HomeScreen.tsx"
TYPES_PATH = MOBILE_SRC / "types/api.ts"
README_PATH = MOBILE_DIR / "README_mobile.md"

WIZARD_MARKERS = [
    "AddMemberWizard",
    "Add Member",
    "Step {step} of 3",
    "General Info",
    "Food Preferences",
    "Activity & Goal",
    "Like",
    "Dislike",
    "Avoid",
    "Protein sources",
    "Carbs & staples",
    "Other foods",
    "Avoid something else?",
    "Save Member",
    "no_pork",
    "food_preferences",
    "ratings",
    "avoid_ingredients",
    "cooking_time_preference",
]

NO_SEPARATE_BLOCK_MARKERS = [
    "Favorite foods",
    "Less preferred foods",
    "Avoid completely",
]

README_MARKERS = [
    "PROFILE-WIZARD-1",
    "Add Member",
    "3-step",
    "no_pork",
    "food_preferences.ratings",
    "Neutral",
    "Avoid = hard filter",
    "Dislike = soft preference",
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


def _has_none(text: str, markers: list[str]) -> bool:
    return not any(marker in text for marker in markers)


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
    wizard = _read(WIZARD_PATH)
    home_screen = _read(HOME_SCREEN_PATH)
    types = _read(TYPES_PATH)
    readme = _read(README_PATH)

    checks = {
        "add_member_wizard_file_exists": WIZARD_PATH.exists(),
        "wizard_has_required_markers": _has_all(wizard, WIZARD_MARKERS),
        "wizard_uses_like_dislike_avoid_matrix": _has_all(
            wizard,
            ["PreferenceRow", "RatingButton", "Like", "Dislike", "Avoid"],
        ),
        "wizard_avoids_separate_preference_blocks": _has_none(
            wizard,
            NO_SEPARATE_BLOCK_MARKERS,
        ),
        "home_screen_uses_wizard": "AddMemberWizard" in home_screen,
        "old_profile_form_not_primary_add_member_ui": "ProfileForm" not in home_screen,
        "wizard_does_not_show_household_id": "Household ID" not in wizard,
        "mobile_types_include_food_preferences": _has_all(
            types,
            ["FoodPreferences", "FoodPreferenceRating", "food_preferences"],
        ),
        "mobile_types_include_no_pork": "no_pork" in wizard,
        "readme_mentions_profile_wizard": _has_all(readme, README_MARKERS),
    }

    forbidden_hits = _scan_mobile_forbidden()
    checks["no_forbidden_mobile_csv_or_generator_imports"] = not forbidden_hits

    failed = [name for name, ok in checks.items() if not ok]
    status_ok = not failed
    summary_lines = [
        "Mobile profile wizard structure summary",
        "status=ok" if status_ok else "status=failed",
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
