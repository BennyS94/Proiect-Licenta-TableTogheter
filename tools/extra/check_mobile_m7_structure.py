from __future__ import annotations

import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

MOBILE_DIR = PROJECT_ROOT / "mobile"
SUMMARY_PATH = PROJECT_ROOT / "data/recipesdb/audit/mobile_m7_structure_summary.txt"

REQUIRED_FILES = [
    "src/components/GroceryListSection.tsx",
    "src/components/HouseholdMemberPlanView.tsx",
    "src/components/HouseholdMemberSwitcher.tsx",
    "src/components/ProfileCard.tsx",
    "src/screens/HomeScreen.tsx",
    "src/services/apiClient.ts",
    "src/types/api.ts",
    "README_mobile.md",
]

CONTENT_MARKERS = {
    "src/screens/HomeScreen.tsx": [
        'type HouseholdSource = "demo" | "saved"',
        "householdSource",
        "Household source:",
        "Demo household",
        "Saved profiles",
        "selectedSavedHouseholdProfileIds",
        "toggleSavedHouseholdProfile",
        "buildSavedProfilesHouseholdGenerateRequest",
        "selected_member_ids: profiles.map",
        "getHouseholdDisplayMembers",
        "Household grocery list",
        "GroceryListSection",
    ],
    "src/services/apiClient.ts": [
        "generateHouseholdPlan",
        "/household-plans/generate",
        "getProfiles",
        "/profiles",
    ],
    "src/types/api.ts": [
        "HouseholdPlanGenerateRequest",
        "selected_member_ids: string[]",
        "HouseholdPlanGenerateResponse",
        "MemberProfileResponse",
    ],
    "README_mobile.md": [
        "Mobile M7 Flow",
        "selected_member_ids",
        "/household-plans/generate",
        "Household grocery list",
        "auth/login",
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
                relative_path = path.relative_to(MOBILE_DIR).as_posix()
                errors.append(f"forbidden_mobile_marker={relative_path}:{marker}")

    package_json = _load_package_json()
    scripts = package_json.get("scripts") if isinstance(package_json.get("scripts"), dict) else {}
    if "android" not in scripts:
        errors.append("missing_script=android")

    status_ok = not errors
    summary_lines = [
        "Mobile M7 structure smoke summary",
        "status=ok" if status_ok else "status=failed",
        f"mobile_dir={MOBILE_DIR.as_posix()}",
        f"required_file_count={len(REQUIRED_FILES)}",
        f"existing_required_file_count={len(existing_files)}",
        "missing_files=" + (",".join(missing_files) if missing_files else "none"),
        "flow=household_source_demo_or_saved,saved_profile_multi_select,selected_member_ids_request",
        "display=member_switcher,day_selector,per_member_menu,aggregate_household_grocery_list",
        "endpoint=POST /household-plans/generate",
        "forbidden_mobile_markers_checked=" + ",".join(FORBIDDEN_MOBILE_MARKERS),
        "scripts=" + ",".join(sorted(str(key) for key in scripts.keys())),
        "errors=" + (";".join(errors) if errors else "none"),
    ]

    SUMMARY_PATH.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
    print("\n".join(summary_lines))
    return 0 if status_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
