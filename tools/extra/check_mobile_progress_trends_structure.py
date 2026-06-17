from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
MOBILE_DIR = PROJECT_ROOT / "mobile"
MOBILE_SRC = MOBILE_DIR / "src"
SUMMARY_PATH = PROJECT_ROOT / "data/recipesdb/audit/mobile_progress_trends_structure_summary.txt"

INSIGHTS_PATH = MOBILE_SRC / "screens/InsightsPage.tsx"
HOME_SCREEN_PATH = MOBILE_SRC / "screens/HomeScreen.tsx"
API_CLIENT_PATH = MOBILE_SRC / "services/apiClient.ts"
TYPES_PATH = MOBILE_SRC / "types/api.ts"
PACKAGE_PATH = MOBILE_DIR / "package.json"

INSIGHTS_MARKERS = [
    "TrendsSection",
    "Trends",
    "RangeSelector",
    "Last {value}",
    "Target adherence",
    "MetricSelector",
    "Calories",
    "Protein",
    "Carbs",
    "Fats",
    "Consistency",
    "days in range",
    "Current streak",
    "Macro pattern",
    "MacroPatternHeatmap",
    "No saved progress yet",
    "Save a day from Insights to start seeing trends.",
    "Micronutrients",
]

STATUS_MARKERS = [
    "getTrendStatus",
    "in_target",
    "close",
    "off",
    "0.9",
    "1.1",
    "1.3",
]

HOME_MARKERS = [
    "progressHistory={selectedProgressHistory}",
    "compareDailyProgressSnapshotsAscending",
    "target: input.target",
    "refreshDailyProgressForProfile(insightsMemberProfileId, true)",
]

TYPE_MARKERS = [
    "target: DailyProgressTotals",
    "target?: DailyProgressTotals",
]

API_MARKERS = [
    "getDailyProgress",
    "/progress/daily",
    "member_profile_id",
]

FORBIDDEN_TEXT_MARKERS = [
    "Top foods",
    "Top contributors",
    "deficiency",
    "diagnosis",
    "treatment",
    "disease management",
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

FORBIDDEN_CHART_DEPENDENCIES = [
    "react-native-chart-kit",
    "victory-native",
    "VictoryChart",
    "VictoryBar",
    "VictoryLine",
    "echarts",
    "d3-shape",
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
    package_json = _read(PACKAGE_PATH)
    all_mobile_text = insights + "\n" + home_screen + "\n" + api_client + "\n" + types

    forbidden_mobile_hits = _scan_mobile_forbidden()
    forbidden_text_hits = [
        marker for marker in FORBIDDEN_TEXT_MARKERS if marker in all_mobile_text
    ]
    chart_dependency_hits = [
        marker for marker in FORBIDDEN_CHART_DEPENDENCIES if marker in package_json + all_mobile_text
    ]

    checks = {
        "insights_page_file_exists": INSIGHTS_PATH.exists(),
        "home_screen_file_exists": HOME_SCREEN_PATH.exists(),
        "api_client_file_exists": API_CLIENT_PATH.exists(),
        "types_file_exists": TYPES_PATH.exists(),
        "trends_section_exists": _has_all(insights, INSIGHTS_MARKERS),
        "trend_status_helper_exists": _has_all(insights, STATUS_MARKERS),
        "home_wires_saved_history": _has_all(home_screen, HOME_MARKERS),
        "progress_types_include_target": _has_all(types, TYPE_MARKERS),
        "api_client_still_uses_progress_endpoint": _has_all(api_client, API_MARKERS),
        "daily_balance_structure_still_present": _has_all(
            insights,
            ["Daily Balance", "Meal contribution", "Macro Targets", "Micronutrients"],
        ),
        "no_top_foods_or_medical_terms": not forbidden_text_hits,
        "no_heavy_chart_dependency": not chart_dependency_hits,
        "no_forbidden_mobile_csv_or_generator_imports": not forbidden_mobile_hits,
    }
    failed = [name for name, ok in checks.items() if not ok]
    status_ok = not failed
    summary_lines = [
        "Mobile progress trends structure summary",
        "status=ok" if status_ok else "status=failed",
        *[f"{name}={str(ok).lower()}" for name, ok in checks.items()],
        "forbidden_text_hits=" + (";".join(forbidden_text_hits) if forbidden_text_hits else "none"),
        "chart_dependency_hits=" + (";".join(chart_dependency_hits) if chart_dependency_hits else "none"),
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
