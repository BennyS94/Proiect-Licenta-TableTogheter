from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
MOBILE_DIR = PROJECT_ROOT / "mobile"
MOBILE_SRC = MOBILE_DIR / "src"
SUMMARY_PATH = PROJECT_ROOT / ".codex_runtime_logs/checks/recipesdb/mobile_auth_m1_structure_summary.txt"

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

FORBIDDEN_VISIBLE_MARKERS = [
    "Continue as Demo",
    "Load demo",
    "Demo Family Household",
    "MVP Demo",
    "Household ID",
    "SQLite demo",
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


def _scan_mobile_forbidden_imports() -> list[str]:
    hits: list[str] = []
    for path in _mobile_source_files():
        text = _read(path)
        for marker in FORBIDDEN_MOBILE_MARKERS:
            if marker in text:
                hits.append(f"{path.relative_to(PROJECT_ROOT).as_posix()}:{marker}")
    return hits


def _scan_forbidden_visible_text() -> list[str]:
    files_to_scan = [
        MOBILE_SRC / "screens/HomePage.tsx",
        MOBILE_SRC / "screens/MealPlanPage.tsx",
        MOBILE_SRC / "screens/InsightsPage.tsx",
        MOBILE_SRC / "screens/HouseholdPage.tsx",
        MOBILE_SRC / "screens/HomeScreen.tsx",
        MOBILE_SRC / "components/ProfileForm.tsx",
    ]
    hits: list[str] = []
    for path in files_to_scan:
        text = _read(path)
        for marker in FORBIDDEN_VISIBLE_MARKERS:
            if marker in text:
                hits.append(f"{path.relative_to(PROJECT_ROOT).as_posix()}:{marker}")
    return hits


def main() -> int:
    api_client = _read(MOBILE_SRC / "services/apiClient.ts")
    api_types = _read(MOBILE_SRC / "types/api.ts")
    home_screen = _read(MOBILE_SRC / "screens/HomeScreen.tsx")
    household_page = _read(MOBILE_SRC / "screens/HouseholdPage.tsx")
    profile_form = _read(MOBILE_SRC / "components/ProfileForm.tsx")
    app_screen = _read(MOBILE_SRC / "components/ui/AppScreen.tsx")
    empty_state = _read(MOBILE_SRC / "components/ui/EmptyState.tsx")
    readme = _read(MOBILE_DIR / "README_mobile.md")

    checks = {
        "auth_api_functions_exist": _has_all(
            api_client,
            [
                "registerAccount",
                "loginAccount",
                "logoutAccount",
                "getCurrentAccount",
                "Authorization",
            ],
        ),
        "auth_types_exist": _has_all(
            api_types,
            [
                "RegisterRequest",
                "LoginRequest",
                "AuthAccount",
                "AuthResponse",
                "MeResponse",
            ],
        ),
        "register_login_ui_exists": _has_all(
            household_page,
            ["Create Account", "Log In", "Confirm password", "Password"],
        ),
        "login_create_not_placeholder": "placeholder" not in household_page.lower()
        and "onPlaceholderAction" not in home_screen,
        "home_screen_wires_real_auth": _has_all(
            home_screen,
            [
                "registerLocalAccount",
                "loginLocalAccount",
                "logoutAccount",
                "authSessionToken",
                "authAccount",
            ],
        ),
        "profile_calls_accept_token": _has_all(
            api_client,
            [
                "getProfiles(",
                "sessionToken",
                "createProfile(",
                "deleteProfile(",
            ],
        ),
        "add_profile_hides_household_id": "Household ID" not in profile_form,
        "load_demo_not_visible": not _scan_forbidden_visible_text(),
        "safe_area_global": _has_all(app_screen, ["StatusBar", "TOP_SAFE_PADDING"]),
        "empty_states_centered": _has_all(empty_state, ["justifyContent", "center"]),
        "readme_mentions_auth_m1": "Auth-M1" in readme,
    }

    forbidden_import_hits = _scan_mobile_forbidden_imports()
    forbidden_visible_hits = _scan_forbidden_visible_text()
    checks["no_forbidden_mobile_csv_or_generator_imports"] = not forbidden_import_hits
    checks["no_forbidden_visible_demo_text"] = not forbidden_visible_hits

    failed = [name for name, ok in checks.items() if not ok]
    status_ok = not failed
    summary_lines = [
        "Mobile Auth-M1 structure summary",
        "status=ok" if status_ok else "status=failed",
        *[f"{name}={str(ok).lower()}" for name, ok in checks.items()],
        "forbidden_mobile_hits="
        + (";".join(forbidden_import_hits) if forbidden_import_hits else "none"),
        "forbidden_visible_hits="
        + (";".join(forbidden_visible_hits) if forbidden_visible_hits else "none"),
        "errors=" + (";".join(failed) if failed else "none"),
    ]

    SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)
    SUMMARY_PATH.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
    print("\n".join(summary_lines))
    return 0 if status_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
