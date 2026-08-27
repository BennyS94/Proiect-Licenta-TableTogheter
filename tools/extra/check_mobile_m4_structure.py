from __future__ import annotations

import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

MOBILE_DIR = PROJECT_ROOT / "mobile"
SUMMARY_PATH = PROJECT_ROOT / ".codex_runtime_logs/checks/recipesdb/mobile_m4_structure_summary.txt"

REQUIRED_FILES = [
    "src/components/MealFeedbackButtons.tsx",
    "src/components/MealRow.tsx",
    "src/components/PlanDayCard.tsx",
    "src/screens/HomeScreen.tsx",
    "src/services/apiClient.ts",
    "src/types/api.ts",
    "README_mobile.md",
]

CONTENT_MARKERS = {
    "src/components/MealFeedbackButtons.tsx": [
        "MealFeedbackButtons",
        "liked",
        "disliked",
        "too_long",
        "explicit_avoid",
        "Feedback unavailable for this meal.",
    ],
    "src/components/MealRow.tsx": [
        "MealFeedbackButtons",
        "onSubmitFeedback",
    ],
    "src/screens/HomeScreen.tsx": [
        "submitMealFeedback",
        "Feedback saved. Generate again to apply it.",
        "Feedback context",
        "feedback_enabled: true",
        "getPlanIdFromResponse",
        "getFeedbackStats",
    ],
    "src/services/apiClient.ts": [
        "submitFeedback",
        "getFeedbackContext",
        "/feedback",
        "/feedback/context",
    ],
    "src/types/api.ts": [
        "FeedbackEventRequest",
        "FeedbackEventResponse",
        "FeedbackContextResponse",
        "FeedbackType",
        'source: "mobile"',
    ],
    "README_mobile.md": [
        "Mobile M4 Flow",
        "POST /feedback",
        "feedback_enabled=true",
        "Feedback context",
        "SQLite",
    ],
}

FORBIDDEN_MOBILE_MARKERS = [
    "data/recipesdb/current",
    "data/fooddb/current",
    "src/generator_v1",
    "generator_v1",
    ".csv",
]


def _read(relative_path: str) -> str:
    path = MOBILE_DIR / relative_path
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def _load_package_json() -> dict[str, object]:
    path = MOBILE_DIR / "package.json"
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _mobile_source_files() -> list[Path]:
    src_dir = MOBILE_DIR / "src"
    if not src_dir.exists():
        return []
    return [
        path
        for path in src_dir.rglob("*")
        if path.is_file() and path.suffix in {".ts", ".tsx", ".js", ".jsx"}
    ]


def main() -> int:
    errors: list[str] = []
    missing_files: list[str] = []
    existing_files: list[str] = []

    for relative_path in REQUIRED_FILES:
        path = MOBILE_DIR / relative_path
        if path.exists():
            existing_files.append(relative_path)
        else:
            missing_files.append(relative_path)
            errors.append(f"missing_file={relative_path}")

    for relative_path, markers in CONTENT_MARKERS.items():
        content = _read(relative_path)
        for marker in markers:
            if marker not in content:
                errors.append(f"missing_marker={relative_path}:{marker}")

    for path in _mobile_source_files():
        content = path.read_text(encoding="utf-8", errors="replace")
        for marker in FORBIDDEN_MOBILE_MARKERS:
            if marker in content:
                errors.append(f"forbidden_mobile_marker={path.relative_to(MOBILE_DIR).as_posix()}:{marker}")

    package_json = _load_package_json()
    scripts = package_json.get("scripts") if isinstance(package_json.get("scripts"), dict) else {}
    if "android" not in scripts:
        errors.append("missing_script=android")

    status_ok = not errors
    summary_lines = [
        "Mobile M4 structure smoke summary",
        "status=ok" if status_ok else "status=failed",
        f"mobile_dir={MOBILE_DIR.as_posix()}",
        f"required_file_count={len(REQUIRED_FILES)}",
        f"existing_required_file_count={len(existing_files)}",
        "missing_files=" + (",".join(missing_files) if missing_files else "none"),
        "components=meal_feedback_buttons,meal_row,plan_day_card",
        "flow=generate_plan,submit_feedback,refresh_feedback_context,regenerate_with_feedback",
        "generation_flags=include_grocery_list,include_purchase_suggestions,include_price_estimates,feedback_enabled",
        "feedback_types=liked,disliked,too_long,explicit_avoid",
        "scripts=" + ",".join(sorted(str(key) for key in scripts.keys())),
        "forbidden_mobile_markers_checked=" + ",".join(FORBIDDEN_MOBILE_MARKERS),
        "errors=" + (";".join(errors) if errors else "none"),
    ]

    SUMMARY_PATH.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
    print("\n".join(summary_lines))
    return 0 if status_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
