from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

MOBILE_DIR = PROJECT_ROOT / "mobile"
SUMMARY_PATH = PROJECT_ROOT / "data/recipesdb/audit/mobile_m1_structure_summary.txt"

REQUIRED_FILES = [
    "package.json",
    "app.json",
    "tsconfig.json",
    "App.tsx",
    "src/config/api.ts",
    "src/services/apiClient.ts",
    "src/screens/HomeScreen.tsx",
    "src/components/StatusCard.tsx",
    "README_mobile.md",
]

REQUIRED_SCRIPTS = ["start", "android"]


def _load_package_json() -> dict[str, Any]:
    package_path = MOBILE_DIR / "package.json"
    if not package_path.exists():
        return {}
    try:
        payload = json.loads(package_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def main() -> int:
    errors: list[str] = []
    existing_files: list[str] = []
    missing_files: list[str] = []

    for relative_path in REQUIRED_FILES:
        path = MOBILE_DIR / relative_path
        if path.exists():
            existing_files.append(relative_path)
        else:
            missing_files.append(relative_path)
            errors.append(f"missing_file={relative_path}")

    package_json = _load_package_json()
    scripts = package_json.get("scripts") if isinstance(package_json.get("scripts"), dict) else {}
    dependencies = (
        package_json.get("dependencies")
        if isinstance(package_json.get("dependencies"), dict)
        else {}
    )

    for script_name in REQUIRED_SCRIPTS:
        if script_name not in scripts:
            errors.append(f"missing_script={script_name}")

    if "expo" not in dependencies:
        errors.append("missing_dependency=expo")
    if "react-native" not in dependencies:
        errors.append("missing_dependency=react-native")

    api_config = (MOBILE_DIR / "src/config/api.ts").read_text(
        encoding="utf-8",
    ) if (MOBILE_DIR / "src/config/api.ts").exists() else ""
    if "http://10.0.2.2:8000" not in api_config:
        errors.append("android_emulator_api_url_missing")

    status_ok = not errors
    summary_lines = [
        "Mobile M1 structure smoke summary",
        "status=ok" if status_ok else "status=failed",
        f"mobile_dir={MOBILE_DIR.as_posix()}",
        f"required_file_count={len(REQUIRED_FILES)}",
        f"existing_required_file_count={len(existing_files)}",
        "missing_files=" + (",".join(missing_files) if missing_files else "none"),
        "scripts=" + ",".join(sorted(str(key) for key in scripts.keys())),
        "dependencies=" + ",".join(sorted(str(key) for key in dependencies.keys())),
        "api_base_url=http://10.0.2.2:8000",
        "errors=" + (";".join(errors) if errors else "none"),
    ]

    SUMMARY_PATH.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
    print("\n".join(summary_lines))
    return 0 if status_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
