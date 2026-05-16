from __future__ import annotations

import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DASHBOARD_PATH = ROOT / "streamlit_app/generator_v1_dashboard.py"
AUDIT_OUT = ROOT / "data/recipesdb/audit/streamlit_round51_debug_dashboard_check.txt"

EXPECTED_PRESET_VALUES = {
    '"dataset_profile": V1_2_DEMO_FINAL_PROFILE': "recommended dataset",
    '"selection_mode": "balanced_day"': "recommended selection mode",
    '"portion_policy": "target_aware"': "recommended portion policy",
    '"meal_realism_mode": "practical"': "recommended realism mode",
    '"quality_gate": "demo_safe"': "recommended quality gate",
    '"profile_guard": "demo"': "recommended profile guard",
    '"alternative_count": 3': "recommended alternative count",
    '"diversity_mode": "none"': "recommended diversity mode",
    '"multi_day_mode": MULTI_DAY_MODE_GLOBAL': "recommended multi-day mode",
    '"multi_day_no_repeat_policy": "hard"': "recommended no-repeat policy",
    '"day_candidate_builder": "direct_from_slots"': "recommended day builder",
    '"multi_day_speed_mode": "fast"': "recommended speed mode",
}

ADVANCED_CONTROL_MARKERS = {
    "_render_dataset_selector()": "dataset selector",
    "_render_selection_mode_selector()": "selection mode selector",
    "_render_alternative_controls(selection_mode)": "alternative/diversity controls",
    "_render_portion_policy_selector()": "portion policy selector",
    "_render_meal_realism_selector()": "meal realism selector",
    "_render_quality_gate_selector()": "quality gate selector",
    "_render_profile_guard_selector(dataset_config)": "profile guard selector",
    "_render_multi_day_no_repeat_policy_selector()": "no-repeat selector",
    "_render_day_candidate_pool_size_selector()": "candidate pool selector",
    "_render_multi_day_speed_mode_selector()": "speed mode selector",
    "_render_day_candidate_builder_selector()": "day builder selector",
    "_render_direct_slot_shortlist_size_selector()": "direct shortlist selector",
}


def main() -> None:
    text = DASHBOARD_PATH.read_text(encoding="utf-8")
    checks: list[tuple[str, bool, str]] = []
    checks.extend(_check_recommended_preset(text))
    checks.extend(_check_dataset_profile(text))
    checks.extend(_check_advanced_controls(text))
    checks.extend(_check_generator_logic_unchanged())
    AUDIT_OUT.parent.mkdir(parents=True, exist_ok=True)
    AUDIT_OUT.write_text(_format_report(checks), encoding="utf-8")
    print(f"Round51 Streamlit debug dashboard check written: {AUDIT_OUT}")
    if not all(ok for _, ok, _ in checks):
        raise SystemExit(1)


def _check_recommended_preset(text: str) -> list[tuple[str, bool, str]]:
    checks = [
        (
            "recommended_preset_constant_exists",
            "RECOMMENDED_V1_2_TEST_PRESET" in text,
            "RECOMMENDED_V1_2_TEST_PRESET is present.",
        ),
        (
            "recommended_preset_section_exists",
            'st.expander("Recommended v1.2 test preset"' in text
            and "Apply recommended v1.2 preset" in text,
            "Preset expander and apply button are present.",
        ),
    ]
    for marker, label in EXPECTED_PRESET_VALUES.items():
        checks.append(
            (
                f"preset_value_{label.replace(' ', '_')}",
                marker in text,
                marker,
            )
        )
    return checks


def _check_dataset_profile(text: str) -> list[tuple[str, bool, str]]:
    return [
        (
            "v1_2_demo_final_profile_available",
            "V1_2_DEMO_FINAL_PROFILE" in text
            and "Recipes_DB v1.2 demo-final draft" in text,
            "v1_2_demo_final is available in DATASET_OPTIONS.",
        )
    ]


def _check_advanced_controls(text: str) -> list[tuple[str, bool, str]]:
    checks = [
        (
            "pipeline_sidebar_expander_exists",
            'st.sidebar.expander("Generator v1 pipeline"' in text,
            "Pipeline visualizer is in the sidebar expander.",
        ),
        (
            "run_controls_expander_exists",
            'st.expander("Run controls"' in text,
            "Run controls expander is present.",
        ),
        (
            "advanced_debug_expander_exists",
            'st.expander("Advanced debug controls"' in text,
            "Advanced debug controls expander is present.",
        ),
        (
            "multi_day_controls_expander_exists",
            'st.expander("3-day / multi-day controls"' in text,
            "3-day / multi-day controls expander is present.",
        ),
        (
            "reroll_controls_expander_exists",
            'st.expander("Reroll / diversity controls"' in text,
            "Reroll / diversity controls expander is present.",
        ),
        (
            "profile_quality_controls_expander_exists",
            'st.expander("Profile guard / quality controls"' in text,
            "Profile guard / quality controls expander is present.",
        ),
    ]
    for marker, label in ADVANCED_CONTROL_MARKERS.items():
        checks.append((f"advanced_{label.replace(' ', '_')}", marker in text, marker))
    return checks


def _check_generator_logic_unchanged() -> list[tuple[str, bool, str]]:
    result = subprocess.run(
        [
            "git",
            "status",
            "--short",
            "--",
            "src/generator_v1",
            "src/generator_v1_cli.py",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    output = result.stdout.strip()
    ok = result.returncode == 0 and output == ""
    return [
        (
            "generator_logic_unchanged",
            ok,
            output or "No src/generator_v1 or CLI changes detected.",
        )
    ]


def _format_report(checks: list[tuple[str, bool, str]]) -> str:
    lines = ["Streamlit Round51 debug dashboard check", ""]
    for name, ok, detail in checks:
        status = "PASS" if ok else "FAIL"
        lines.append(f"{status} {name}: {detail}")
    lines.append("")
    lines.append(f"overall={'PASS' if all(ok for _, ok, _ in checks) else 'FAIL'}")
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    main()
