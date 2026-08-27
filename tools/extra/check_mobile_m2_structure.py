from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

MOBILE_DIR = PROJECT_ROOT / "mobile"
SUMMARY_PATH = PROJECT_ROOT / ".codex_runtime_logs/checks/recipesdb/mobile_m2_structure_summary.txt"

REQUIRED_FILES = [
    "package.json",
    "App.tsx",
    "src/types/api.ts",
    "src/config/api.ts",
    "src/services/apiClient.ts",
    "src/screens/HomeScreen.tsx",
    "src/components/StatusCard.tsx",
    "src/components/MemberCard.tsx",
    "src/components/PlanDayCard.tsx",
    "src/components/MealRow.tsx",
    "README_mobile.md",
]

CONTENT_MARKERS = {
    "src/services/apiClient.ts": [
        "getDemoHousehold",
        "generateIndividualPlan",
        "/households/demo",
        "/plans/generate",
    ],
    "src/types/api.ts": [
        "DemoHouseholdResponse",
        "DemoMemberProfile",
        "IndividualPlanGenerateRequest",
        "IndividualPlanGenerateResponse",
        "GeneratedDay",
        "GeneratedMeal",
    ],
    "src/screens/HomeScreen.tsx": [
        "Load demo household",
        "Generate plan for selected member",
        "selectedMemberId",
        "generatedPlan",
        "buildGenerateRequest",
    ],
    "README_mobile.md": [
        "Mobile M2 Flow",
        "GET /households/demo",
        "POST /plans/generate",
        "profilul demo inline",
    ],
}


def _load_package_json() -> dict[str, Any]:
    package_path = MOBILE_DIR / "package.json"
    if not package_path.exists():
        return {}
    try:
        payload = json.loads(package_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _read(relative_path: str) -> str:
    path = MOBILE_DIR / relative_path
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


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

    package_json = _load_package_json()
    scripts = package_json.get("scripts") if isinstance(package_json.get("scripts"), dict) else {}
    for script_name in ("start", "android"):
        if script_name not in scripts:
            errors.append(f"missing_script={script_name}")

    for relative_path, markers in CONTENT_MARKERS.items():
        content = _read(relative_path)
        for marker in markers:
            if marker not in content:
                errors.append(f"missing_marker={relative_path}:{marker}")

    api_config = _read("src/config/api.ts")
    if "http://10.0.2.2:8000" not in api_config:
        errors.append("android_emulator_api_url_missing")

    status_ok = not errors
    summary_lines = [
        "Mobile M2 structure smoke summary",
        "status=ok" if status_ok else "status=failed",
        f"mobile_dir={MOBILE_DIR.as_posix()}",
        f"required_file_count={len(REQUIRED_FILES)}",
        f"existing_required_file_count={len(existing_files)}",
        "missing_files=" + (",".join(missing_files) if missing_files else "none"),
        "scripts=" + ",".join(sorted(str(key) for key in scripts.keys())),
        "api_calls=get_health,get_demo_household,generate_individual_plan",
        "flow=health,demo_household,member_selection,individual_plan_generation",
        "errors=" + (";".join(errors) if errors else "none"),
    ]

    SUMMARY_PATH.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
    print("\n".join(summary_lines))
    return 0 if status_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
