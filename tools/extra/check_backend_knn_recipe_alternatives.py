from __future__ import annotations

import csv
import json
import sys
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

AUDIT_DIR = PROJECT_ROOT / ".codex_runtime_logs/checks/recipesdb"
SUMMARY_PATH = AUDIT_DIR / "backend_knn_recipe_alternatives_summary.txt"
RESPONSE_SAMPLE_PATH = AUDIT_DIR / "backend_knn_recipe_alternatives_response_sample.json"
CANDIDATES_PATH = AUDIT_DIR / "backend_knn_recipe_alternatives_candidates.csv"

HOUSEHOLD_ID = "household_backend_knn_alt_smoke"
MEMBER_PROFILE_ID = "member_backend_knn_alt_smoke_001"
SOURCE_RECIPE_ID = "recipes_v1_2_round41_manual_012"
SOURCE_SLOT = "breakfast"


def _profile_request() -> dict[str, Any]:
    return {
        "household_id": HOUSEHOLD_ID,
        "member_profile_id": MEMBER_PROFILE_ID,
        "display_name": "KNN Alternatives Smoke Member",
        "age": 31,
        "sex": "male",
        "weight_kg": 78.0,
        "height_cm": 178.0,
        "activity_level": "moderately_active",
        "goal": "maintain",
        "goal_speed": "normal",
        "training": {
            "sessions_per_week": 3,
            "type": "mixed",
        },
        "meal_config": {
            "meals_per_day": 3,
            "include_snacks": True,
            "day_structure": "3_meals_plus_snack",
        },
        "dietary_preferences": {
            "no_beef": False,
            "no_chicken": False,
            "no_fish": False,
            "no_dairy": False,
            "vegetarian": False,
            "vegan": False,
            "gluten_free": False,
        },
        "bf_profile": "normal",
    }


def _alternatives_request(
    *,
    feedback_enabled: bool,
    approval_mode: str = "include_review",
    top_k: int = 5,
) -> dict[str, Any]:
    return {
        "recipe_id": SOURCE_RECIPE_ID,
        "slot": SOURCE_SLOT,
        "top_k": top_k,
        "candidate_pool_k": 20,
        "dataset_profile": "current",
        "household_id": HOUSEHOLD_ID,
        "member_profile_id": MEMBER_PROFILE_ID,
        "feedback_enabled": feedback_enabled,
        "approval_mode": approval_mode,
        "generation_options": {
            "selection_mode": "balanced_day",
            "portion_policy": "target_aware",
            "meal_realism_mode": "practical",
            "quality_gate": "demo_safe",
            "profile_guard": "demo",
        },
    }


def _generation_request() -> dict[str, Any]:
    return {
        "dataset_profile": "current",
        "days": 1,
        "household_id": HOUSEHOLD_ID,
        "member_profile_id": MEMBER_PROFILE_ID,
        "include_grocery_list": False,
        "include_purchase_suggestions": False,
        "include_price_estimates": False,
        "feedback_enabled": True,
        "generation_options": {
            "selection_mode": "balanced_day",
            "portion_policy": "target_aware",
            "meal_realism_mode": "practical",
            "quality_gate": "demo_safe",
            "profile_guard": "demo",
            "day_candidate_builder": "direct_from_slots",
        },
    }


def _json_check(name: str, payload: dict[str, Any]) -> str:
    try:
        json.dumps(payload, ensure_ascii=True, allow_nan=False, sort_keys=True)
    except Exception as exc:
        return f"{name}:json_failed:{type(exc).__name__}:{exc}"
    return f"{name}:json_ok"


def _first_alternative_recipe_id(payload: dict[str, Any]) -> str:
    alternatives = payload.get("alternatives", [])
    if not isinstance(alternatives, list):
        return ""
    for item in alternatives:
        if isinstance(item, dict) and item.get("recipe_id"):
            return str(item["recipe_id"])
    return ""


def _find_alternative(payload: dict[str, Any], recipe_id: str) -> dict[str, Any] | None:
    for item in payload.get("alternatives", []):
        if isinstance(item, dict) and str(item.get("recipe_id") or "") == recipe_id:
            return item
    return None


def _compact_response(payload: dict[str, Any]) -> dict[str, Any]:
    sample = {
        "status": payload.get("status"),
        "recipe_id": payload.get("recipe_id"),
        "source_recipe": payload.get("source_recipe"),
        "slot": payload.get("slot"),
        "dataset_profile": payload.get("dataset_profile"),
        "approval_mode": payload.get("approval_mode"),
        "summary": payload.get("summary"),
        "feedback_context_summary": payload.get("feedback_context_summary"),
        "warnings": payload.get("warnings", []),
        "alternatives": [],
    }
    alternatives = payload.get("alternatives", [])
    if isinstance(alternatives, list):
        sample["alternatives"] = alternatives[:5]
    return sample


def _write_candidates_csv(payload: dict[str, Any]) -> None:
    alternatives = payload.get("alternatives", [])
    rows = [item for item in alternatives if isinstance(item, dict)]
    fieldnames = [
        "recipe_id",
        "display_name",
        "similarity_score",
        "approval_status",
        "approval_reasons",
        "rejection_reasons",
        "macro_delta_kcal",
        "macro_delta_protein_g",
        "macro_delta_carbs_g",
        "macro_delta_fat_g",
        "time_delta_min",
        "why_similar",
        "warnings",
    ]
    with CANDIDATES_PATH.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for item in rows:
            macro_delta = item.get("macro_delta", {})
            if not isinstance(macro_delta, dict):
                macro_delta = {}
            writer.writerow(
                {
                    "recipe_id": item.get("recipe_id", ""),
                    "display_name": item.get("display_name", ""),
                    "similarity_score": item.get("similarity_score", ""),
                    "approval_status": item.get("approval_status", ""),
                    "approval_reasons": ";".join(item.get("approval_reasons", [])),
                    "rejection_reasons": ";".join(item.get("rejection_reasons", [])),
                    "macro_delta_kcal": macro_delta.get("kcal", ""),
                    "macro_delta_protein_g": macro_delta.get("protein_g", ""),
                    "macro_delta_carbs_g": macro_delta.get("carbs_g", ""),
                    "macro_delta_fat_g": macro_delta.get("fat_g", ""),
                    "time_delta_min": item.get("time_delta_min", ""),
                    "why_similar": ";".join(item.get("why_similar", [])),
                    "warnings": ";".join(item.get("warnings", [])),
                }
            )


def main() -> int:
    from fastapi.testclient import TestClient

    from backend.app.db.database import get_sqlite_path, init_db
    from backend.app.main import app

    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    init_db()
    client = TestClient(app)
    errors: list[str] = []

    profile_response = client.post("/profiles", json=_profile_request())
    if profile_response.status_code != 200:
        errors.append(f"profile_post_status={profile_response.status_code}")

    client.delete(
        "/feedback",
        params={
            "household_id": HOUSEHOLD_ID,
            "member_profile_id": MEMBER_PROFILE_ID,
            "confirm": "true",
        },
    )

    alternatives_response = client.post(
        "/recipes/similar",
        json=_alternatives_request(feedback_enabled=False),
    )
    alternatives_payload = alternatives_response.json()
    if alternatives_response.status_code != 200:
        errors.append(f"alternatives_status={alternatives_response.status_code}")
    if alternatives_payload.get("status") != "ok":
        errors.append(f"alternatives_payload_status={alternatives_payload.get('status')}")

    alternatives = alternatives_payload.get("alternatives", [])
    if not isinstance(alternatives, list) or not alternatives:
        errors.append("alternatives_empty")

    avoid_candidate_id = _first_alternative_recipe_id(alternatives_payload)
    feedback_status = None
    feedback_avoid_behavior = "not_tested"
    debug_payload: dict[str, Any] = alternatives_payload
    if avoid_candidate_id:
        feedback_response = client.post(
            "/feedback",
            json={
                "household_id": HOUSEHOLD_ID,
                "member_profile_id": MEMBER_PROFILE_ID,
                "recipe_id": avoid_candidate_id,
                "slot": SOURCE_SLOT,
                "feedback_type": "explicit_avoid",
                "notes": "KNN alternatives smoke explicit avoid.",
                "source": "api",
            },
        )
        feedback_status = feedback_response.status_code
        if feedback_response.status_code != 200:
            errors.append(f"feedback_post_status={feedback_response.status_code}")

        debug_response = client.post(
            "/recipes/similar",
            json=_alternatives_request(
                feedback_enabled=True,
                approval_mode="include_rejected_debug",
                top_k=20,
            ),
        )
        debug_payload = debug_response.json()
        if debug_response.status_code != 200:
            errors.append(f"debug_alternatives_status={debug_response.status_code}")
        debug_item = _find_alternative(debug_payload, avoid_candidate_id)
        if debug_item is None:
            feedback_avoid_behavior = "excluded"
        elif debug_item.get("approval_status") == "rejected" and "explicit_avoid" in debug_item.get("rejection_reasons", []):
            feedback_avoid_behavior = "rejected"
        else:
            feedback_avoid_behavior = f"unexpected:{debug_item.get('approval_status')}"
            errors.append("explicit_avoid_not_rejected_or_excluded")

    generation_response = client.post("/plans/generate", json=_generation_request())
    generation_payload = generation_response.json()
    if generation_response.status_code != 200:
        errors.append(f"plans_generate_status={generation_response.status_code}")
    if generation_payload.get("status") != "ok":
        errors.append(f"plans_generate_payload_status={generation_payload.get('status')}")

    json_checks = [
        _json_check("alternatives", alternatives_payload),
        _json_check("debug_alternatives", debug_payload),
        _json_check("plans_generate", generation_payload),
    ]
    errors.extend([check for check in json_checks if not check.endswith(":json_ok")])

    sample_payload = _compact_response(debug_payload)
    RESPONSE_SAMPLE_PATH.write_text(
        json.dumps(sample_payload, indent=2, ensure_ascii=True),
        encoding="utf-8",
    )
    _write_candidates_csv(debug_payload)

    summary = debug_payload.get("summary", {}) if isinstance(debug_payload.get("summary"), dict) else {}
    status_ok = not errors
    summary_lines = [
        "Backend KNN recipe alternatives smoke summary",
        "status=ok" if status_ok else "status=failed",
        f"sqlite_path={get_sqlite_path().as_posix()}",
        f"source_recipe_id={SOURCE_RECIPE_ID}",
        f"source_slot={SOURCE_SLOT}",
        f"profile_post_status={profile_response.status_code}",
        f"alternatives_status={alternatives_response.status_code}",
        f"alternatives_payload_status={alternatives_payload.get('status')}",
        f"candidate_count={summary.get('candidate_count')}",
        f"returned_count={summary.get('returned_count')}",
        f"approved_count={summary.get('approved_count')}",
        f"review_count={summary.get('review_count')}",
        f"rejected_count={summary.get('rejected_count')}",
        f"feedback_post_status={feedback_status}",
        f"avoid_candidate_id={avoid_candidate_id}",
        f"feedback_avoid_behavior={feedback_avoid_behavior}",
        f"plans_generate_status={generation_response.status_code}",
        f"plans_generate_payload_status={generation_payload.get('status')}",
        "json_checks=" + ";".join(json_checks),
        "errors=" + (";".join(errors) if errors else "none"),
    ]
    SUMMARY_PATH.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
    print("\n".join(summary_lines))
    return 0 if status_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
