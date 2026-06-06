from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException

from backend.app.db.database import get_connection, init_db
from backend.app.db.repositories import (
    build_feedback_context_from_db,
    get_member_profile_for_generation,
)
from backend.app.schemas.recipe_alternatives import (
    RecipeAlternativesRequest,
    RecipeAlternativesResponse,
)
from src.generator_v1.service import get_recipe_alternatives_from_request


router = APIRouter(tags=["recipes"])


@router.post("/recipes/similar", response_model=RecipeAlternativesResponse)
def get_similar_recipes(payload: RecipeAlternativesRequest) -> dict[str, Any]:
    request_dict = _model_to_dict(payload)
    _merge_generation_flags(request_dict)
    init_db()
    with get_connection() as conn:
        _resolve_member_profile(conn, request_dict)
        _inject_sqlite_feedback_context(conn, request_dict)

    try:
        response = get_recipe_alternatives_from_request(request_dict)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"recipe_alternatives_failed:{type(exc).__name__}",
        ) from exc

    if response.get("status") == "error":
        error_code = str(response.get("error_code") or "recipe_alternatives_failed")
        status_code = 404 if error_code == "recipe_not_found" else 400
        raise HTTPException(status_code=status_code, detail=error_code)
    if response.get("status") != "ok":
        raise HTTPException(status_code=500, detail="recipe_alternatives_failed")
    return response


def _resolve_member_profile(conn: Any, request_dict: dict[str, Any]) -> None:
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
    household_id = _clean_text(request_dict.get("household_id"))
    member_profile_id = _clean_text(request_dict.get("member_profile_id"))
    if not household_id and not member_profile_id:
        return
    request_dict["feedback_context"] = build_feedback_context_from_db(
        conn,
        household_id=household_id,
        member_profile_id=member_profile_id,
    )
    request_dict["feedback_context_source"] = "sqlite"


def _merge_generation_flags(request_dict: dict[str, Any]) -> None:
    options = dict(request_dict.get("generation_options") or {})
    if request_dict.get("feedback_enabled") is not None:
        options["feedback_enabled"] = request_dict["feedback_enabled"]
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
