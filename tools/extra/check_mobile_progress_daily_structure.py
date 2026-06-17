from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
MOBILE_DIR = PROJECT_ROOT / "mobile"
MOBILE_SRC = MOBILE_DIR / "src"
SUMMARY_PATH = PROJECT_ROOT / "data/recipesdb/audit/mobile_progress_daily_structure_summary.txt"

INSIGHTS_PATH = MOBILE_SRC / "screens/InsightsPage.tsx"
HOME_SCREEN_PATH = MOBILE_SRC / "screens/HomeScreen.tsx"
API_CLIENT_PATH = MOBILE_SRC / "services/apiClient.ts"
TYPES_PATH = MOBILE_SRC / "types/api.ts"

INSIGHTS_MARKERS = [
    "DailyProgressControl",
    "Daily progress",
    "Progress not saved yet",
    "Save day",
    "Saved to progress history",
    "Saved",
    "Delete saved day",
    "Average mode cannot be saved as a day.",
    "buildDailyProgressSaveInput",
    "meal_completion",
    "day_snapshot",
]

HOME_MARKERS = [
    "dailyProgressByKey",
    "saveInsightsDailyProgress",
    "deleteInsightsDailyProgress",
    "refreshDailyProgressForProfile",
    "buildDailyProgressKey",
    "selectedDailyProgress",
    "insightsMemberProfileId",
    "insightsPlanId",
    "safeInsightsDay === \"average\"",
]

API_MARKERS = [
    "saveDailyProgress",
    "getDailyProgress",
    "deleteDailyProgress",
    "/progress/daily",
    "authHeaders(sessionToken)",
]

TYPE_MARKERS = [
    "DailyProgressSaveRequest",
    "DailyProgressSnapshot",
    "DailyProgressSaveResponse",
    "DailyProgressListResponse",
    "DailyProgressDeleteResponse",
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

FORBIDDEN_PROGRESS_MARKERS = [
    "VictoryPie",
    "react-native-chart-kit",
    "ProgressHistoryChart",
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
    insights = _read(INSIGHTS_PATH)
    home_screen = _read(HOME_SCREEN_PATH)
    api_client = _read(API_CLIENT_PATH)
    types = _read(TYPES_PATH)

    checks = {
        "insights_page_file_exists": INSIGHTS_PATH.exists(),
        "home_screen_file_exists": HOME_SCREEN_PATH.exists(),
        "api_client_file_exists": API_CLIENT_PATH.exists(),
        "types_file_exists": TYPES_PATH.exists(),
        "insights_has_progress_controls": _has_all(insights, INSIGHTS_MARKERS),
        "home_wires_progress_state_and_handlers": _has_all(home_screen, HOME_MARKERS),
        "api_client_has_progress_endpoints": _has_all(api_client, API_MARKERS),
        "types_include_progress_contract": _has_all(types, TYPE_MARKERS),
        "average_mode_blocks_save": "Average mode cannot be saved as a day." in insights
        and "safeInsightsDay === \"average\"" in home_screen,
        "no_progress_history_charts_added": not any(
            marker in insights + home_screen for marker in FORBIDDEN_PROGRESS_MARKERS
        ),
        "daily_balance_and_macro_targets_still_present": _has_all(
            insights,
            ["Daily Balance", "Meal contribution", "Macro Targets", "Micronutrients"],
        ),
    }

    forbidden_hits = _scan_mobile_forbidden()
    checks["no_forbidden_mobile_csv_or_generator_imports"] = not forbidden_hits

    failed = [name for name, ok in checks.items() if not ok]
    status_ok = not failed
    summary_lines = [
        "Mobile progress daily structure summary",
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
