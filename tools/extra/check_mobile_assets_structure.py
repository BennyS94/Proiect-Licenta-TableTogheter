from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
MOBILE_DIR = PROJECT_ROOT / "mobile"
MOBILE_SRC = MOBILE_DIR / "src"
ASSETS_DIR = MOBILE_DIR / "assets"
SUMMARY_PATH = PROJECT_ROOT / ".codex_runtime_logs/checks/recipesdb/mobile_assets_structure_summary.txt"

REQUIRED_PATHS = [
    "README_assets.md",
    "brand/README.md",
    "brand/icon/README.md",
    "brand/splash/README.md",
    "common/README.md",
    "common/placeholders/README.md",
    "common/backgrounds/README.md",
    "common/patterns/README.md",
    "home/README.md",
    "home/welcome/README.md",
    "home/tips/README.md",
    "home/highlights/README.md",
    "home/family_kids/README.md",
    "home/healthy_habits/README.md",
    "navigation/README.md",
    "meal_plan/README.md",
    "meal_plan/meal_slots/README.md",
    "meal_plan/actions/README.md",
    "meal_plan/recipe_details/README.md",
    "meal_plan/cooking_steps/README.md",
    "grocery/README.md",
    "grocery/package_icons/README.md",
    "grocery/categories/README.md",
    "insights/README.md",
    "insights/macro/README.md",
    "insights/micronutrients/README.md",
    "household/README.md",
    "household/account/README.md",
    "household/members/README.md",
    "household/profile_wizard/README.md",
]

README_MARKERS = [
    "UI-ASSETS-1",
    "cooking_lottie.json",
    "cooking_loop.gif",
    "mobile/assets/home/welcome",
    "lottie-react-native",
]

ASSET_GUIDE_MARKERS = [
    "lowercase snake_case",
    "Lottie JSON",
    "GIF",
    "Avoid MP4",
    "Do not import missing assets",
    "lottie-react-native",
]

HOME_GUIDE_MARKERS = [
    "cooking_lottie.json",
    "cooking_loop.gif",
    "tip_snack.png",
    "highlight_meal_prep.png",
    "kids_vegetables.png",
    "habit_grocery_planning.png",
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

FORBIDDEN_ASSET_RUNTIME_MARKERS = [
    "from \"../../assets",
    "from '../assets",
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


def _scan_home_runtime_asset_imports() -> list[str]:
    hits: list[str] = []
    home_files = [
        MOBILE_SRC / "screens/HomePage.tsx",
        MOBILE_SRC / "components/home/ResourceCarouselSection.tsx",
    ]
    for path in home_files:
        text = _read(path)
        for marker in FORBIDDEN_ASSET_RUNTIME_MARKERS:
            if marker in text:
                hits.append(f"{path.relative_to(PROJECT_ROOT).as_posix()}:{marker}")
    return hits


def main() -> int:
    missing_paths = [
        relative_path
        for relative_path in REQUIRED_PATHS
        if not (ASSETS_DIR / relative_path).exists()
    ]

    asset_guide = _read(ASSETS_DIR / "README_assets.md")
    home_guide = _read(ASSETS_DIR / "home/README.md")
    readme = _read(MOBILE_DIR / "README_mobile.md")
    registry = _read(MOBILE_SRC / "assets/assetRegistry.ts")
    home_page = _read(MOBILE_SRC / "screens/HomePage.tsx")
    package_json = _read(MOBILE_DIR / "package.json")
    hero_lottie_path = ASSETS_DIR / "home/welcome/cooking_lottie.json"

    forbidden_mobile_hits = _scan_mobile_forbidden()
    runtime_asset_hits = _scan_home_runtime_asset_imports()

    checks = {
        "asset_root_readme_exists": (ASSETS_DIR / "README_assets.md").exists(),
        "required_asset_structure_exists": not missing_paths,
        "asset_guide_has_naming_rules": _has_all(asset_guide, ASSET_GUIDE_MARKERS),
        "home_asset_guide_exists": _has_all(home_guide, HOME_GUIDE_MARKERS),
        "grocery_package_icon_rules_exist": _has_all(
            _read(ASSETS_DIR / "grocery/package_icons/README.md"),
            ["package_scale.png", "package_bag.png", "package_warning.png"],
        ),
        "navigation_icon_rules_exist": _has_all(
            _read(ASSETS_DIR / "navigation/README.md"),
            ["nav_home.png", "nav_meal_plan.png", "nav_insights.png", "nav_household.png"],
        ),
        "brand_apk_rules_exist": _has_all(
            _read(ASSETS_DIR / "brand/README.md"),
            ["ANDROID-BUILD-1", "app_icon_foreground.png", "splash_logo.png"],
        ),
        "safe_asset_registry_exists": _has_all(
            registry,
            ["expectedHomeAssets", "expectedNavigationAssets", "expectedGroceryPackageAssets"],
        )
        and "require(" not in registry
        and "from " not in registry,
        "home_still_uses_placeholders": _has_all(
            home_page,
            ["animationCard", "tipIllustration", "ResourceCarouselSection"],
        ),
        "home_lottie_hero_asset_ready": hero_lottie_path.exists()
        and "lottie-react-native" in package_json
        and _has_all(home_page, ["LottieView", "cooking_lottie.json"]),
        "home_has_no_runtime_missing_asset_imports": not runtime_asset_hits,
        "readme_mentions_ui_assets_1": _has_all(readme, README_MARKERS),
        "no_forbidden_mobile_csv_or_generator_imports": not forbidden_mobile_hits,
    }

    failed = [name for name, ok in checks.items() if not ok]
    status_ok = not failed
    summary_lines = [
        "Mobile UI-ASSETS-1 structure summary",
        "status=ok" if status_ok else "status=failed",
        f"required_path_count={len(REQUIRED_PATHS)}",
        "missing_paths=" + (";".join(missing_paths) if missing_paths else "none"),
        *[f"{name}={str(ok).lower()}" for name, ok in checks.items()],
        "runtime_asset_hits=" + (";".join(runtime_asset_hits) if runtime_asset_hits else "none"),
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
