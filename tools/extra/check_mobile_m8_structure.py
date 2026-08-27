from __future__ import annotations

import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

MOBILE_DIR = PROJECT_ROOT / "mobile"
SUMMARY_PATH = PROJECT_ROOT / ".codex_runtime_logs/checks/recipesdb/mobile_m8_structure_summary.txt"

REQUIRED_FILES = [
    "src/components/ProfileCard.tsx",
    "src/screens/HomeScreen.tsx",
    "src/services/apiClient.ts",
    "src/types/api.ts",
    "README_mobile.md",
]

CONTENT_MARKERS = {
    "src/services/apiClient.ts": [
        "deleteProfile",
        "/profiles/${encodedProfileId}?confirm=true",
        "clearFeedback",
        "/feedback?",
        'method: "DELETE"',
    ],
    "src/types/api.ts": [
        "DeleteProfileResponse",
        "deactivated: boolean",
        "deleted?: boolean",
        "FeedbackDeleteResponse",
    ],
    "src/components/ProfileCard.tsx": [
        "onDelete",
        "deleteDisabled",
        "Remove",
    ],
    "src/screens/HomeScreen.tsx": [
        "Alert.alert",
        "Remove this saved profile?",
        "deleteSavedProfile",
        "deleteProfile",
        "Clear feedback",
        "clearFeedbackForActiveHousehold",
        "clearFeedback(activeHouseholdId",
    ],
    "README_mobile.md": [
        "Mobile M8 Flow",
        "DELETE /profiles/{member_profile_id}?confirm=true",
        "Clear feedback",
        "soft-deactivated",
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
    SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)
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
        "Mobile M8 structure smoke summary",
        "status=ok" if status_ok else "status=failed",
        f"mobile_dir={MOBILE_DIR.as_posix()}",
        f"required_file_count={len(REQUIRED_FILES)}",
        f"existing_required_file_count={len(existing_files)}",
        "missing_files=" + (",".join(missing_files) if missing_files else "none"),
        "flow=saved_profile_remove,profile_soft_deactivate,feedback_clear",
        "endpoint=DELETE /profiles/{member_profile_id}?confirm=true",
        "feedback_endpoint=DELETE /feedback?confirm=true",
        "dev_generated_plans_reset=not_implemented",
        "forbidden_mobile_markers_checked=" + ",".join(FORBIDDEN_MOBILE_MARKERS),
        "scripts=" + ",".join(sorted(str(key) for key in scripts.keys())),
        "errors=" + (";".join(errors) if errors else "none"),
    ]

    SUMMARY_PATH.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
    print("\n".join(summary_lines))
    return 0 if status_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
