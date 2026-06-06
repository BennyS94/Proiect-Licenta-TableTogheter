from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, HTTPException, Query

from backend.app.db.database import get_connection, init_db
from backend.app.db.repositories import (
    build_feedback_context_from_db,
    get_generated_plan_record,
    save_generated_plan,
    save_grocery_list,
    save_plan_days,
    save_plan_meals,
)
from backend.app.schemas.plan_replacement import (
    MealReplacementRequest,
    MealReplacementResponse,
)
from src.generator_v1.service import (
    apply_meal_replacement_from_request,
    preview_meal_replacement_from_request,
)


router = APIRouter(tags=["plan-replacements"])


@router.post("/plans/{plan_id}/replace-meal", response_model=MealReplacementResponse)
def replace_plan_meal(
    plan_id: str,
    payload: MealReplacementRequest,
    dry_run: bool = Query(default=True),
) -> dict[str, Any]:
    request_dict = _model_to_dict(payload)
    init_db()
    with get_connection() as conn:
        record = get_generated_plan_record(conn, plan_id)
        if record is None:
            raise HTTPException(status_code=404, detail="plan_not_found")
        service_request = _replacement_service_request(record, request_dict)
        _inject_sqlite_feedback_context(conn, service_request)

    try:
        response = (
            preview_meal_replacement_from_request(service_request)
            if dry_run
            else apply_meal_replacement_from_request(service_request)
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"meal_replacement_failed:{type(exc).__name__}",
        ) from exc

    if response.get("status") == "error":
        raise HTTPException(
            status_code=_error_status_code(str(response.get("error_code") or "")),
            detail=response.get("error_code") or "meal_replacement_failed",
        )
    if response.get("status") != "ok":
        raise HTTPException(status_code=500, detail="meal_replacement_failed")

    if dry_run:
        return response

    new_plan_id = _new_replacement_plan_id(record)
    updated_plan = response.get("updated_plan")
    if not isinstance(updated_plan, dict):
        raise HTTPException(status_code=500, detail="meal_replacement_missing_updated_plan")

    _assign_new_plan_ids(updated_plan, response, new_plan_id, source_plan_id=plan_id)
    generation_type = str(record.get("generation_type") or updated_plan.get("generation_type") or "")
    grocery_json = _grocery_from_response(response, updated_plan)
    household_id = str(record.get("household_id") or updated_plan.get("household_id") or "")
    member_profile_id = record.get("member_profile_id")

    with get_connection() as conn:
        save_generated_plan(
            conn,
            plan_id=new_plan_id,
            household_id=household_id,
            member_profile_id=str(member_profile_id) if member_profile_id else None,
            generation_type=generation_type or "individual",
            dataset_profile=str(record.get("dataset_profile") or updated_plan.get("dataset_profile") or ""),
            days=int(record.get("days") or updated_plan.get("days") or request_dict.get("day_index") or 1),
            request_json=_persisted_replacement_request(record, request_dict),
            response_json=updated_plan,
        )
        _save_optional_indexes(conn, new_plan_id, updated_plan)
        save_grocery_list(conn, new_plan_id, household_id, grocery_json)

    return response


def _replacement_service_request(
    record: dict[str, Any],
    request_dict: dict[str, Any],
) -> dict[str, Any]:
    original_request = dict(record.get("request_json") or {})
    service_request = dict(original_request)
    options = dict(original_request.get("generation_options") or {})
    options.update(dict(request_dict.get("generation_options") or {}))
    service_request.update(request_dict)
    service_request["generation_options"] = options
    service_request["source_plan"] = record.get("response_json") or {}
    service_request["source_plan_id"] = record.get("plan_id")
    service_request["plan_id"] = record.get("plan_id")
    service_request.setdefault("generation_type", record.get("generation_type"))
    service_request.setdefault("dataset_profile", record.get("dataset_profile"))
    service_request.setdefault("days", record.get("days"))
    service_request.setdefault("household_id", record.get("household_id"))
    service_request.setdefault("member_profile_id", record.get("member_profile_id"))
    return service_request


def _inject_sqlite_feedback_context(conn: Any, request_dict: dict[str, Any]) -> None:
    if not _feedback_enabled(request_dict):
        request_dict.pop("feedback_context", None)
        return
    household_id = _clean_text(request_dict.get("household_id"))
    member_profile_id = _clean_text(request_dict.get("member_profile_id") or request_dict.get("member_id"))
    if not household_id and not member_profile_id:
        return
    request_dict["feedback_context"] = build_feedback_context_from_db(
        conn,
        household_id=household_id,
        member_profile_id=member_profile_id,
    )
    request_dict["feedback_context_source"] = "sqlite"


def _assign_new_plan_ids(
    updated_plan: dict[str, Any],
    response: dict[str, Any],
    new_plan_id: str,
    *,
    source_plan_id: str,
) -> None:
    generation_type = str(updated_plan.get("generation_type") or response.get("generation_type") or "")
    updated_plan["plan_id"] = new_plan_id
    if generation_type == "household" or updated_plan.get("household_plan_id"):
        updated_plan["household_plan_id"] = new_plan_id
    updated_plan["replacement_parent_plan_id"] = source_plan_id
    updated_plan["replacement_source_plan_id"] = source_plan_id
    updated_plan["replacement_applied_at"] = datetime.now(timezone.utc).isoformat()

    grocery_json = _grocery_from_response(response, updated_plan)
    if grocery_json:
        grocery_json["plan_id"] = new_plan_id
        grocery_json["grocery_list_id"] = f"grocery_replacement_{uuid.uuid4().hex[:12]}"
    response["plan_id"] = new_plan_id
    response["new_plan_id"] = new_plan_id
    response["source_plan_id"] = source_plan_id


def _grocery_from_response(
    response: dict[str, Any],
    updated_plan: dict[str, Any],
) -> dict[str, Any] | None:
    grocery = response.get("grocery_list")
    if isinstance(grocery, dict):
        return grocery
    for key in ("household_grocery_list", "grocery_list"):
        candidate = updated_plan.get(key)
        if isinstance(candidate, dict):
            return candidate
    return None


def _persisted_replacement_request(
    record: dict[str, Any],
    request_dict: dict[str, Any],
) -> dict[str, Any]:
    return {
        "source_plan_id": record.get("plan_id"),
        "source_created_at": record.get("created_at"),
        "replacement_request": request_dict,
        "source_request_json": record.get("request_json") or {},
    }


def _save_optional_indexes(conn: Any, plan_id: str, response: dict[str, Any]) -> None:
    try:
        save_plan_days(conn, plan_id, response)
        save_plan_meals(conn, plan_id, response)
    except Exception:
        # Indexarea normalizata este best-effort in MVP; JSON-ul complet ramane sursa.
        return


def _new_replacement_plan_id(record: dict[str, Any]) -> str:
    generation_type = str(record.get("generation_type") or "individual").strip() or "individual"
    return f"plan_{generation_type}_replaced_{uuid.uuid4().hex[:12]}"


def _error_status_code(error_code: str) -> int:
    if error_code in {"day_not_found", "meal_not_found", "plan_not_found"}:
        return 404
    return 400


def _feedback_enabled(request_dict: dict[str, Any]) -> bool:
    options = request_dict.get("generation_options")
    if isinstance(options, dict):
        value = options.get("feedback_enabled")
    else:
        value = request_dict.get("feedback_enabled")
    if value is None:
        return True
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"none", "nan", "nat"}:
        return ""
    return text


def _model_to_dict(model: Any) -> dict[str, Any]:
    if hasattr(model, "model_dump"):
        return model.model_dump()
    return model.dict()
