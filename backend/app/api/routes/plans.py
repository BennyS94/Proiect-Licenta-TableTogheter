from __future__ import annotations

import uuid
from typing import Any

from fastapi import APIRouter, HTTPException

from backend.app.db.database import get_connection, init_db
from backend.app.db.repositories import (
    build_feedback_context_from_db,
    get_generated_plan,
    get_grocery_list_by_plan_id,
    get_member_profile_for_generation,
    save_generated_plan,
    save_grocery_list,
    save_plan_days,
    save_plan_meals,
)
from backend.app.schemas.generation import IndividualPlanGenerateRequest
from backend.app.schemas.generation import PlanGenerateResponse
from backend.app.schemas.grocery import GroceryListRetrieveResponse
from backend.app.schemas.plan import PlanRetrieveResponse
from src.generator_v1.service import generate_individual_plan_from_request


router = APIRouter(tags=["plans"])


@router.post("/plans/generate", response_model=PlanGenerateResponse)
def generate_plan(payload: IndividualPlanGenerateRequest) -> dict[str, Any]:
    request_dict = _model_to_dict(payload)
    _merge_generation_flags(request_dict)
    init_db()
    with get_connection() as conn:
        _resolve_individual_profile(conn, request_dict)
        _inject_sqlite_feedback_context(conn, request_dict)

    try:
        response = generate_individual_plan_from_request(request_dict)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"generation_failed:{type(exc).__name__}") from exc

    if response.get("status") == "blocked":
        return response
    if response.get("status") != "ok":
        raise HTTPException(status_code=500, detail=response.get("error_code") or "generation_failed")

    plan_id = _ensure_plan_id(response)
    household_id = _individual_household_id(request_dict, response)
    member_profile_id = _individual_member_profile_id(request_dict, response)
    response["household_id"] = household_id
    response["member_profile_id"] = member_profile_id
    grocery_json = response.get("grocery_list")

    with get_connection() as conn:
        save_generated_plan(
            conn,
            plan_id=plan_id,
            household_id=household_id,
            member_profile_id=member_profile_id,
            generation_type="individual",
            dataset_profile=str(response.get("dataset_profile") or request_dict.get("dataset_profile") or ""),
            days=int(response.get("days") or request_dict.get("days") or 1),
            request_json=request_dict,
            response_json=response,
        )
        _save_optional_indexes(conn, plan_id, response)
        save_grocery_list(conn, plan_id, household_id, grocery_json)
    return response


@router.get("/plans/{plan_id}", response_model=PlanRetrieveResponse)
def retrieve_plan(plan_id: str) -> dict[str, Any]:
    init_db()
    with get_connection() as conn:
        response = get_generated_plan(conn, plan_id)
    if response is None:
        raise HTTPException(status_code=404, detail="plan_not_found")
    return response


@router.get("/plans/{plan_id}/grocery-list", response_model=GroceryListRetrieveResponse)
def retrieve_plan_grocery_list(plan_id: str) -> dict[str, Any]:
    init_db()
    with get_connection() as conn:
        grocery_list = get_grocery_list_by_plan_id(conn, plan_id)
    if grocery_list is None:
        raise HTTPException(status_code=404, detail="grocery_list_not_found")
    return grocery_list


def _save_optional_indexes(conn: Any, plan_id: str, response: dict[str, Any]) -> None:
    try:
        save_plan_days(conn, plan_id, response)
        save_plan_meals(conn, plan_id, response)
    except Exception:
        # Indexarea normalizata este best-effort in MVP; JSON-ul complet ramane sursa.
        return


def _ensure_plan_id(response: dict[str, Any]) -> str:
    plan_id = str(response.get("plan_id") or "").strip()
    if not plan_id:
        plan_id = f"plan_individual_{uuid.uuid4().hex[:12]}"
        response["plan_id"] = plan_id
    return plan_id


def _individual_household_id(
    request_dict: dict[str, Any],
    response: dict[str, Any],
) -> str:
    if request_dict.get("household_id"):
        return str(request_dict["household_id"])
    if response.get("household_id"):
        return str(response["household_id"])
    profile = request_dict.get("member_profile")
    if isinstance(profile, dict):
        household_id = str(profile.get("household_id") or "").strip()
        if household_id:
            return household_id
    return "household_demo_001"


def _individual_member_profile_id(
    request_dict: dict[str, Any],
    response: dict[str, Any],
) -> str:
    if request_dict.get("member_profile_id"):
        return str(request_dict["member_profile_id"])
    if response.get("member_profile_id"):
        return str(response["member_profile_id"])
    profile = request_dict.get("member_profile")
    if isinstance(profile, dict):
        return str(profile.get("member_profile_id") or "")
    return ""


def _resolve_individual_profile(conn: Any, request_dict: dict[str, Any]) -> None:
    profile = request_dict.get("member_profile")
    if isinstance(profile, dict):
        member_profile_id = _clean_text(
            request_dict.get("member_profile_id") or profile.get("member_profile_id")
        )
        household_id = _clean_text(request_dict.get("household_id") or profile.get("household_id"))
        if member_profile_id:
            request_dict["member_profile_id"] = member_profile_id
        if household_id:
            request_dict["household_id"] = household_id
        return

    member_profile_id = _clean_text(request_dict.get("member_profile_id"))
    if not member_profile_id:
        return

    stored_profile = get_member_profile_for_generation(conn, member_profile_id)
    if stored_profile is None:
        raise HTTPException(status_code=404, detail="profile_not_found")
    request_dict["member_profile"] = stored_profile
    request_dict["member_profile_id"] = member_profile_id
    if stored_profile.get("household_id"):
        request_dict["household_id"] = stored_profile["household_id"]


def _inject_sqlite_feedback_context(conn: Any, request_dict: dict[str, Any]) -> None:
    if not _feedback_enabled(request_dict):
        request_dict.pop("feedback_context", None)
        return
    context = build_feedback_context_from_db(
        conn,
        household_id=_clean_text(request_dict.get("household_id")),
        member_profile_id=_clean_text(request_dict.get("member_profile_id")),
    )
    request_dict["feedback_context"] = context
    request_dict["feedback_context_source"] = "sqlite"


def _merge_generation_flags(request_dict: dict[str, Any]) -> None:
    options = dict(request_dict.get("generation_options") or {})
    for key in (
        "include_grocery_list",
        "include_purchase_suggestions",
        "include_price_estimates",
        "feedback_enabled",
    ):
        if key in request_dict and request_dict.get(key) is not None:
            options[key] = request_dict[key]
    request_dict["generation_options"] = options


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
