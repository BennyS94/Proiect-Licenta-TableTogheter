from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
MOBILE_DIR = PROJECT_ROOT / "mobile"
MOBILE_SRC = MOBILE_DIR / "src"
SUMMARY_PATH = PROJECT_ROOT / ".codex_runtime_logs/checks/recipesdb/mobile_home_page_structure_summary.txt"

HOME_PAGE_PATH = MOBILE_SRC / "screens/HomePage.tsx"
HOME_SCREEN_PATH = MOBILE_SRC / "screens/HomeScreen.tsx"
HOME_CONTENT_PATH = MOBILE_SRC / "data/homeContent.ts"
CAROUSEL_PATH = MOBILE_SRC / "components/home/ResourceCarouselSection.tsx"
APP_SCREEN_PATH = MOBILE_SRC / "components/ui/AppScreen.tsx"
FLOATING_NAV_PATH = MOBILE_SRC / "components/navigation/FloatingNav.tsx"
README_PATH = MOBILE_DIR / "README_mobile.md"

FORBIDDEN_HOME_API_MARKERS = [
    "API_BASE_URL",
    "../services/apiClient",
    "services/apiClient",
    "fetch(",
    "axios",
    "getHealth",
    "getProfiles",
    "generateIndividualPlan",
    "generateHouseholdPlan",
]

FORBIDDEN_HOME_VISIBLE_MARKERS = [
    "Demo",
    "Test",
    "MVP",
    "backend",
    "status ok",
    "plan_id",
    "endpoint",
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


def _scan_home_forbidden_api() -> list[str]:
    hits: list[str] = []
    for path in [HOME_PAGE_PATH, HOME_CONTENT_PATH, CAROUSEL_PATH]:
        text = _read(path)
        for marker in FORBIDDEN_HOME_API_MARKERS:
            if marker in text:
                hits.append(f"{path.relative_to(PROJECT_ROOT).as_posix()}:{marker}")
    return hits


def _scan_home_visible_text() -> list[str]:
    text = "\n".join([_read(HOME_PAGE_PATH), _read(HOME_CONTENT_PATH)])
    hits: list[str] = []
    for marker in FORBIDDEN_HOME_VISIBLE_MARKERS:
        if marker in text:
            hits.append(marker)
    return hits


def main() -> int:
    home_page = _read(HOME_PAGE_PATH)
    home_screen = _read(HOME_SCREEN_PATH)
    home_content = _read(HOME_CONTENT_PATH)
    carousel = _read(CAROUSEL_PATH)
    app_screen = _read(APP_SCREEN_PATH)
    floating_nav = _read(FLOATING_NAV_PATH)
    readme = _read(README_PATH)

    forbidden_mobile_hits = _scan_mobile_forbidden()
    forbidden_home_api_hits = _scan_home_forbidden_api()
    forbidden_home_visible_hits = _scan_home_visible_text()

    checks = {
        "home_page_exists": HOME_PAGE_PATH.exists(),
        "home_page_is_used_by_shell": _has_all(
            home_screen,
            ["<HomePage", "activePage === \"home\"", "setActivePage(\"mealPlan\")"],
        ),
        "home_content_exists": HOME_CONTENT_PATH.exists()
        and _has_all(
            home_content,
            ["dailyFoodTips", "weeklyHighlights", "familyKidsIdeas", "healthyHabits"],
        ),
        "resource_carousel_exists": CAROUSEL_PATH.exists()
        and _has_all(
            carousel,
            ["ResourceCarouselSection", "pagingEnabled", "snapToInterval", "dotsRow"],
        ),
        "daily_tip_cycle_exists": _has_all(
            home_page,
            ["tipIndex", "setTipIndex", "Daily Food Tip", "dailyFoodTips.length"],
        ),
        "home_uses_app_screen_safe_area": "AppScreen" in home_page
        and _has_all(app_screen, ["StatusBar", "TOP_SAFE_PADDING"]),
        "home_has_household_cta": _has_all(
            home_page,
            ["Set up your household", "Go to Meal Plan"],
        ),
        "home_has_see_all_state": _has_all(
            home_page,
            ["homeSubPage", "HomeSeeAllPage", "Highlights of the Week", "Healthy Habits"],
        ),
        "home_has_no_backend_calls": not forbidden_home_api_hits,
        "home_has_no_forbidden_visible_text": not forbidden_home_visible_hits,
        "readme_mentions_home_1": "HOME-1" in readme,
        "floating_nav_not_removed": _has_all(
            floating_nav,
            ["home", "mealPlan", "insights", "household", "FloatingNav"],
        ),
        "no_forbidden_mobile_csv_or_generator_imports": not forbidden_mobile_hits,
    }

    failed = [name for name, ok in checks.items() if not ok]
    status_ok = not failed
    summary_lines = [
        "Mobile HOME-1 page structure summary",
        "status=ok" if status_ok else "status=failed",
        *[f"{name}={str(ok).lower()}" for name, ok in checks.items()],
        "forbidden_home_api_hits="
        + (";".join(forbidden_home_api_hits) if forbidden_home_api_hits else "none"),
        "forbidden_home_visible_hits="
        + (";".join(forbidden_home_visible_hits) if forbidden_home_visible_hits else "none"),
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
