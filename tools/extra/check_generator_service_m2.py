from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

SUMMARY_PATH = PROJECT_ROOT / "data/recipesdb/audit/backend_m2_generator_service_summary.txt"
API_EXAMPLES_DIR = PROJECT_ROOT / "docs/api_examples"


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _json_safe(name: str, payload: dict[str, Any]) -> str:
    try:
        json.dumps(payload, ensure_ascii=True, allow_nan=False, sort_keys=True)
    except Exception as exc:
        return f"{name}:json_failed:{type(exc).__name__}:{exc}"
    return f"{name}:json_ok"


def _status(payload: dict[str, Any]) -> str:
    return str(payload.get("status") or "missing")


def main() -> int:
    from src.generator_v1.service import (
        build_feedback_context_from_request,
        build_grocery_list_for_plan,
        generate_household_plan_from_request,
        generate_individual_plan_from_request,
        submit_feedback_event_from_request,
    )

    errors: list[str] = []
    individual_request = _load_json(API_EXAMPLES_DIR / "individual_plan_generate_request.json")
    household_request = _load_json(API_EXAMPLES_DIR / "household_plan_generate_request.json")
    feedback_request = _load_json(API_EXAMPLES_DIR / "feedback_event_request.json")

    individual_response: dict[str, Any] = {}
    individual_grocery_response: dict[str, Any] = {}
    household_response: dict[str, Any] = {}
    household_grocery_response: dict[str, Any] = {}
    feedback_context_response: dict[str, Any] = {}
    feedback_submit_response: dict[str, Any] = {}

    try:
        individual_response = generate_individual_plan_from_request(individual_request)
        if _status(individual_response) != "ok":
            errors.append(f"individual_status={_status(individual_response)}")
    except Exception as exc:
        errors.append(f"individual_exception={type(exc).__name__}:{exc}")

    try:
        individual_grocery_response = build_grocery_list_for_plan(
            individual_response.get("generator_plan", {}),
            {
                "dataset_profile": individual_request.get("dataset_profile"),
                "generation_type": "individual",
                **dict(individual_request.get("generation_options") or {}),
            },
        )
        if _status(individual_grocery_response) != "ok":
            errors.append(f"individual_grocery_status={_status(individual_grocery_response)}")
    except Exception as exc:
        errors.append(f"individual_grocery_exception={type(exc).__name__}:{exc}")

    try:
        household_response = generate_household_plan_from_request(household_request)
        if _status(household_response) != "ok":
            errors.append(f"household_status={_status(household_response)}")
    except Exception as exc:
        errors.append(f"household_exception={type(exc).__name__}:{exc}")

    try:
        household_grocery_response = build_grocery_list_for_plan(
            household_response.get("generator_plan", {}),
            {
                "dataset_profile": household_request.get("dataset_profile"),
                "generation_type": "household",
                **dict(household_request.get("generation_options") or {}),
            },
        )
        if _status(household_grocery_response) != "ok":
            errors.append(f"household_grocery_status={_status(household_grocery_response)}")
    except Exception as exc:
        errors.append(f"household_grocery_exception={type(exc).__name__}:{exc}")

    try:
        feedback_context_response = build_feedback_context_from_request(
            {
                "dataset_profile": household_request.get("dataset_profile"),
                "household_id": feedback_request.get("household_id"),
                "member_profile_id": feedback_request.get("member_profile_id"),
                "feedback_events": [],
            }
        )
        if _status(feedback_context_response) != "ok":
            errors.append(f"feedback_context_status={_status(feedback_context_response)}")
    except Exception as exc:
        errors.append(f"feedback_context_exception={type(exc).__name__}:{exc}")

    try:
        feedback_submit_response = submit_feedback_event_from_request(
            {
                "dataset_profile": household_request.get("dataset_profile"),
                "feedback_events": [],
                **feedback_request,
            }
        )
        if _status(feedback_submit_response) != "ok":
            errors.append(f"feedback_submit_status={_status(feedback_submit_response)}")
        if feedback_submit_response.get("stored") is not False:
            errors.append("feedback_submit_unexpected_persistence")
    except Exception as exc:
        errors.append(f"feedback_submit_exception={type(exc).__name__}:{exc}")

    json_checks = [
        _json_safe("individual_response", individual_response),
        _json_safe("individual_grocery_response", individual_grocery_response),
        _json_safe("household_response", household_response),
        _json_safe("household_grocery_response", household_grocery_response),
        _json_safe("feedback_context_response", feedback_context_response),
        _json_safe("feedback_submit_response", feedback_submit_response),
    ]
    errors.extend([check for check in json_checks if not check.endswith(":json_ok")])

    individual_grocery_summary = individual_grocery_response.get("summary", {})
    household_grocery_summary = household_grocery_response.get("summary", {})
    household_diagnostics = household_response.get("diagnostics_summary", {})

    status_ok = not errors
    summary_lines = [
        "Backend M2 generator service smoke summary",
        "status=ok" if status_ok else "status=failed",
        f"individual_status={_status(individual_response)}",
        f"individual_days={individual_response.get('days')}",
        f"individual_daily_plan_count={len(individual_response.get('daily_plan', []))}",
        f"individual_grocery_status={_status(individual_grocery_response)}",
        f"individual_grocery_items={individual_grocery_summary.get('shopping_item_count')}",
        f"household_status={_status(household_response)}",
        f"household_days={household_response.get('days')}",
        f"household_selected_members={len(household_response.get('selected_members', []))}",
        f"household_per_member_menu_count={len(household_response.get('per_member_menus', []))}",
        f"household_quality_status={household_diagnostics.get('household_quality_status')}",
        f"household_grocery_status={_status(household_grocery_response)}",
        f"household_grocery_items={household_grocery_summary.get('shopping_item_count')}",
        f"feedback_context_status={_status(feedback_context_response)}",
        f"feedback_context_event_count={feedback_context_response.get('summary', {}).get('event_count')}",
        f"feedback_submit_status={_status(feedback_submit_response)}",
        f"feedback_submit_stored={feedback_submit_response.get('stored')}",
        "json_checks=" + ";".join(json_checks),
        "errors=" + (";".join(errors) if errors else "none"),
    ]

    SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)
    SUMMARY_PATH.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
    print("\n".join(summary_lines))
    return 0 if status_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
