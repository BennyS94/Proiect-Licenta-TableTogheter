from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

SUMMARY_PATH = PROJECT_ROOT / "data/recipesdb/audit/backend_m3_generation_endpoints_summary.txt"
INDIVIDUAL_SAMPLE_PATH = PROJECT_ROOT / "data/recipesdb/audit/backend_m3_individual_response_sample.json"
HOUSEHOLD_SAMPLE_PATH = PROJECT_ROOT / "data/recipesdb/audit/backend_m3_household_response_sample.json"
API_EXAMPLES_DIR = PROJECT_ROOT / "docs/api_examples"


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _json_check(name: str, payload: dict[str, Any]) -> str:
    try:
        json.dumps(payload, ensure_ascii=True, allow_nan=False, sort_keys=True)
    except Exception as exc:
        return f"{name}:json_failed:{type(exc).__name__}:{exc}"
    return f"{name}:json_ok"


def _row_count(db_path: Path, table: str) -> int:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
    return int(row[0] if row else 0)


def _sample_response(payload: dict[str, Any]) -> dict[str, Any]:
    keys = [
        "status",
        "plan_id",
        "household_plan_id",
        "generation_type",
        "dataset_profile",
        "household_id",
        "member_profile_id",
        "selected_members",
        "days",
        "daily_plan",
        "per_member_menus",
        "shared_meals",
        "household_grocery_scaling",
        "member_macro_summaries",
        "feedback_context_summary",
        "warnings",
        "diagnostics_summary",
    ]
    sample = {key: payload.get(key) for key in keys if key in payload}
    if isinstance(sample.get("daily_plan"), list):
        sample["daily_plan"] = sample["daily_plan"][:1]
    if isinstance(sample.get("per_member_menus"), list):
        sample["per_member_menus"] = sample["per_member_menus"][:2]
    if isinstance(sample.get("shared_meals"), list):
        sample["shared_meals"] = sample["shared_meals"][:2]
    if isinstance(sample.get("household_grocery_scaling"), list):
        sample["household_grocery_scaling"] = sample["household_grocery_scaling"][:2]
    if isinstance(sample.get("member_macro_summaries"), list):
        sample["member_macro_summaries"] = sample["member_macro_summaries"][:2]

    grocery = payload.get("grocery_list") or payload.get("household_grocery_list")
    if isinstance(grocery, dict):
        compact_grocery = {
            "status": grocery.get("status"),
            "grocery_list_id": grocery.get("grocery_list_id"),
            "plan_id": grocery.get("plan_id"),
            "generation_type": grocery.get("generation_type"),
            "currency": grocery.get("currency"),
            "total_estimated_cost": grocery.get("total_estimated_cost"),
            "items": _sample_grocery_items(grocery.get("items", [])),
            "summary": grocery.get("summary", {}),
            "warnings": grocery.get("warnings", []),
        }
        if payload.get("grocery_list") is not None:
            sample["grocery_list"] = compact_grocery
        else:
            sample["household_grocery_list"] = compact_grocery
    return sample


def _sample_grocery_items(items: Any) -> list[dict[str, Any]]:
    if not isinstance(items, list):
        return []
    rows = []
    for item in items[:3]:
        if not isinstance(item, dict):
            continue
        rows.append(
            {
                "display_name": item.get("display_name")
                or item.get("display_name_clean")
                or item.get("canonical_name"),
                "category": item.get("category") or item.get("grocery_category"),
                "needed_grams": item.get("needed_grams")
                or item.get("display_grams_numeric")
                or item.get("total_grams"),
                "purchase_display": item.get("purchase_display"),
                "estimated_cost": item.get("estimated_cost"),
                "currency": item.get("currency"),
                "warnings": item.get("warnings", []),
            }
        )
    return rows


def main() -> int:
    from fastapi.testclient import TestClient

    from backend.app.db.database import get_sqlite_path, init_db
    from backend.app.main import app

    db_path = init_db()
    generated_plans_before = _row_count(db_path, "generated_plans")
    grocery_lists_before = _row_count(db_path, "grocery_lists")

    client = TestClient(app)
    individual_request = _load_json(API_EXAMPLES_DIR / "individual_plan_generate_request.json")
    household_request = _load_json(API_EXAMPLES_DIR / "household_plan_generate_request.json")
    errors: list[str] = []

    individual_response = client.post("/plans/generate", json=individual_request)
    if individual_response.status_code != 200:
        errors.append(f"individual_post_status={individual_response.status_code}")
    individual_payload = individual_response.json()
    if individual_payload.get("status") not in {"ok", "blocked"}:
        errors.append(f"individual_payload_status={individual_payload.get('status')}")

    individual_plan_id = individual_payload.get("plan_id")
    individual_retrieve_payload: dict[str, Any] = {}
    individual_grocery_payload: dict[str, Any] = {}
    individual_get_status = None
    individual_grocery_status = None
    if individual_payload.get("status") == "ok":
        if not individual_plan_id:
            errors.append("individual_plan_id_missing")
        else:
            individual_retrieve = client.get(f"/plans/{individual_plan_id}")
            individual_get_status = individual_retrieve.status_code
            if individual_retrieve.status_code != 200:
                errors.append(f"individual_get_status={individual_retrieve.status_code}")
            else:
                individual_retrieve_payload = individual_retrieve.json()
            if (individual_request.get("generation_options") or {}).get("include_grocery_list"):
                individual_grocery = client.get(f"/plans/{individual_plan_id}/grocery-list")
                individual_grocery_status = individual_grocery.status_code
                if individual_grocery.status_code != 200:
                    errors.append(f"individual_grocery_status={individual_grocery.status_code}")
                else:
                    individual_grocery_payload = individual_grocery.json()

    household_response = client.post("/household-plans/generate", json=household_request)
    if household_response.status_code != 200:
        errors.append(f"household_post_status={household_response.status_code}")
    household_payload = household_response.json()
    if household_payload.get("status") != "ok":
        errors.append(f"household_payload_status={household_payload.get('status')}")

    household_plan_id = household_payload.get("household_plan_id") or household_payload.get("plan_id")
    household_retrieve_payload: dict[str, Any] = {}
    household_grocery_payload: dict[str, Any] = {}
    household_get_status = None
    household_grocery_status = None
    if not household_plan_id:
        errors.append("household_plan_id_missing")
    else:
        household_retrieve = client.get(f"/household-plans/{household_plan_id}")
        household_get_status = household_retrieve.status_code
        if household_retrieve.status_code != 200:
            errors.append(f"household_get_status={household_retrieve.status_code}")
        else:
            household_retrieve_payload = household_retrieve.json()
        if (household_request.get("generation_options") or {}).get("include_grocery_list"):
            household_grocery = client.get(f"/plans/{household_plan_id}/grocery-list")
            household_grocery_status = household_grocery.status_code
            if household_grocery.status_code != 200:
                errors.append(f"household_grocery_status={household_grocery.status_code}")
            else:
                household_grocery_payload = household_grocery.json()

    generated_plans_after = _row_count(db_path, "generated_plans")
    grocery_lists_after = _row_count(db_path, "grocery_lists")
    generated_plan_days_after = _row_count(db_path, "generated_plan_days")
    generated_plan_meals_after = _row_count(db_path, "generated_plan_meals")
    grocery_list_items_after = _row_count(db_path, "grocery_list_items")

    json_checks = [
        _json_check("individual_post", individual_payload),
        _json_check("individual_get", individual_retrieve_payload),
        _json_check("individual_grocery", individual_grocery_payload),
        _json_check("household_post", household_payload),
        _json_check("household_get", household_retrieve_payload),
        _json_check("household_grocery", household_grocery_payload),
    ]
    errors.extend([check for check in json_checks if not check.endswith(":json_ok")])

    status_ok = not errors and generated_plans_after > generated_plans_before
    if grocery_lists_after <= grocery_lists_before:
        errors.append("grocery_list_row_not_created")
        status_ok = False

    INDIVIDUAL_SAMPLE_PATH.write_text(
        json.dumps(_sample_response(individual_payload), indent=2, ensure_ascii=True),
        encoding="utf-8",
    )
    HOUSEHOLD_SAMPLE_PATH.write_text(
        json.dumps(_sample_response(household_payload), indent=2, ensure_ascii=True),
        encoding="utf-8",
    )

    summary_lines = [
        "Backend M3 generation endpoints smoke summary",
        "status=ok" if status_ok else "status=failed",
        f"sqlite_path={get_sqlite_path().as_posix()}",
        f"individual_post_status={individual_response.status_code}",
        f"individual_payload_status={individual_payload.get('status')}",
        f"individual_plan_id={individual_plan_id or 'missing'}",
        f"individual_get_status={individual_get_status}",
        f"individual_grocery_status={individual_grocery_status}",
        f"household_post_status={household_response.status_code}",
        f"household_payload_status={household_payload.get('status')}",
        f"household_plan_id={household_plan_id or 'missing'}",
        f"household_get_status={household_get_status}",
        f"household_grocery_status={household_grocery_status}",
        f"generated_plans_before={generated_plans_before}",
        f"generated_plans_after={generated_plans_after}",
        f"generated_plans_created={generated_plans_after - generated_plans_before}",
        f"grocery_lists_before={grocery_lists_before}",
        f"grocery_lists_after={grocery_lists_after}",
        f"grocery_lists_created={grocery_lists_after - grocery_lists_before}",
        f"generated_plan_days_after={generated_plan_days_after}",
        f"generated_plan_meals_after={generated_plan_meals_after}",
        f"grocery_list_items_after={grocery_list_items_after}",
        "json_checks=" + ";".join(json_checks),
        "errors=" + (";".join(errors) if errors else "none"),
    ]

    SUMMARY_PATH.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
    print("\n".join(summary_lines))
    return 0 if status_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
