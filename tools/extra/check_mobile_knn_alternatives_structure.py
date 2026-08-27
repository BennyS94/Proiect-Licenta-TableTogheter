from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SUMMARY_PATH = PROJECT_ROOT / ".codex_runtime_logs/checks/recipesdb/mobile_knn_alternatives_structure_summary.txt"

MOBILE_SRC = PROJECT_ROOT / "mobile/src"
API_CLIENT_PATH = MOBILE_SRC / "services/apiClient.ts"
API_TYPES_PATH = MOBILE_SRC / "types/api.ts"
MEAL_ROW_PATH = MOBILE_SRC / "components/MealRow.tsx"
HOUSEHOLD_MEAL_ROW_PATH = MOBILE_SRC / "components/HouseholdMealRow.tsx"
PANEL_PATH = MOBILE_SRC / "components/RecipeAlternativesPanel.tsx"
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
    meal_row = _read(MEAL_ROW_PATH)
    household_meal_row = _read(HOUSEHOLD_MEAL_ROW_PATH)
    panel = _read(PANEL_PATH)
    readme = _read(README_PATH)

    checks = {
        "api_client_has_function": _has_all(
            api_client,
            ["getRecipeAlternatives", "/recipes/similar"],
        ),
        "api_types_have_recipe_alternatives": _has_all(
            api_types,
            [
                "RecipeAlternativesRequest",
                "RecipeAlternativesResponse",
                "RecipeAlternativeItem",
                "RecipeAlternativesApprovalMode",
            ],
        ),
        "meal_row_has_alternatives_button": _has_all(
            meal_row,
            ["Alternatives", "RecipeAlternativesPanel"],
        ),
        "household_meal_row_has_alternatives_button": _has_all(
            household_meal_row,
            ["Alternatives", "RecipeAlternativesPanel"],
        ),
        "panel_exists_and_is_explicit_replacement_only": _has_all(
            panel,
            [
                "RecipeAlternativesPanel",
                "Alternatives are read-only until you preview and confirm a replacement.",
                "Preview replacement",
                "Replace meal",
                "approval_mode: \"include_review\"",
            ],
        ),
        "readme_mentions_knn_alternatives": _has_all(
            readme,
            ["KNN alternatives", "POST /recipes/similar", "read-only"],
        ),
    }

    forbidden_mobile_hits = _scan_mobile_forbidden(FORBIDDEN_MOBILE_MARKERS)
    checks["no_forbidden_mobile_csv_or_generator_imports"] = not forbidden_mobile_hits
    checks["replacement_requires_explicit_confirm"] = _has_all(
        panel,
        ["previewMealReplacement", "applyMealReplacement", "onReplacementApplied"],
    )

    failed = [name for name, ok in checks.items() if not ok]
    status_ok = not failed

    summary_lines = [
        "Mobile KNN alternatives structure summary",
        "status=ok" if status_ok else "status=failed",
        *[f"{name}={str(ok).lower()}" for name, ok in checks.items()],
        "forbidden_mobile_hits=" + (";".join(forbidden_mobile_hits) if forbidden_mobile_hits else "none"),
        "errors=" + (";".join(failed) if failed else "none"),
    ]
    SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)
    SUMMARY_PATH.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
    print("\n".join(summary_lines))
    return 0 if status_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
