from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

MOBILE_DIR = PROJECT_ROOT / "mobile"
SUMMARY_PATH = PROJECT_ROOT / "data/recipesdb/audit/mobile_m3_structure_summary.txt"

REQUIRED_FILES = [
    "src/components/GroceryListSection.tsx",
    "src/components/GroceryItemRow.tsx",
    "src/screens/HomeScreen.tsx",
    "src/services/apiClient.ts",
    "src/types/api.ts",
    "README_mobile.md",
]

CONTENT_MARKERS = {
    "src/components/GroceryListSection.tsx": [
        "GroceryListSection",
        "Estimated total",
        "Missing prices",
        "GroceryItemRow",
    ],
    "src/components/GroceryItemRow.tsx": [
        "GroceryItemRow",
        "Need:",
        "Buy:",
        "No price estimate",
    ],
    "src/screens/HomeScreen.tsx": [
        "GroceryListSection",
        "getGroceryListFromPlanResponse",
        "include_grocery_list: true",
        "include_purchase_suggestions: true",
        "include_price_estimates: true",
        "feedback_enabled: false",
    ],
    "src/services/apiClient.ts": [
        "generateIndividualPlan",
        "/plans/generate",
    ],
    "src/types/api.ts": [
        "GroceryListResponse",
        "GroceryListItem",
        "estimated_total_cost",
        "purchase_display",
        "price_confidence",
        "grocery_list?: GroceryListResponse",
    ],
    "README_mobile.md": [
        "Mobile M3 Flow",
        "include_grocery_list=true",
        "purchase suggestions",
        "Preturile sunt estimari demo",
    ],
}


def _read(relative_path: str) -> str:
    path = MOBILE_DIR / relative_path
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def _load_package_json() -> dict[str, Any]:
    path = MOBILE_DIR / "package.json"
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


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

    package_json = _load_package_json()
    scripts = package_json.get("scripts") if isinstance(package_json.get("scripts"), dict) else {}
    if "android" not in scripts:
        errors.append("missing_script=android")

    status_ok = not errors
    summary_lines = [
        "Mobile M3 structure smoke summary",
        "status=ok" if status_ok else "status=failed",
        f"mobile_dir={MOBILE_DIR.as_posix()}",
        f"required_file_count={len(REQUIRED_FILES)}",
        f"existing_required_file_count={len(existing_files)}",
        "missing_files=" + (",".join(missing_files) if missing_files else "none"),
        "components=grocery_list_section,grocery_item_row",
        "flow=generate_plan,display_plan,display_grocery_list",
        "generation_flags=include_grocery_list,include_purchase_suggestions,include_price_estimates",
        "scripts=" + ",".join(sorted(str(key) for key in scripts.keys())),
        "errors=" + (";".join(errors) if errors else "none"),
    ]

    SUMMARY_PATH.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
    print("\n".join(summary_lines))
    return 0 if status_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
