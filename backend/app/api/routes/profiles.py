from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Header, HTTPException, Query

from backend.app.db.auth_repository import get_session_context
from backend.app.db.database import get_connection, init_db
from backend.app.db.repositories import (
    deactivate_member_profile,
    get_member_profile,
    list_member_profiles,
    save_member_profile,
)
from backend.app.schemas.profile import (
    MemberProfileCreateRequest,
    MemberProfileResponse,
    ProfileDeleteResponse,
    ProfilesListResponse,
)


router = APIRouter(tags=["profiles"])


@router.get("/profiles", response_model=ProfilesListResponse)
def get_profiles(
    household_id: str | None = None,
    active_only: bool = Query(default=True),
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    init_db()
    session_context = _session_context_from_authorization(authorization)
    scoped_household_id = (
        session_context["household_id"] if session_context is not None else household_id
    )
    with get_connection() as conn:
        profiles = list_member_profiles(
            conn,
            household_id=scoped_household_id,
            active_only=active_only,
        )
    return {
        "household_id": scoped_household_id,
        "source": "sqlite",
        "profiles": profiles,
    }


@router.post("/profiles", response_model=MemberProfileResponse)
def create_profile(
    payload: MemberProfileCreateRequest,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    profile = _model_to_dict(payload)
    session_context = _session_context_from_authorization(authorization)
    if session_context is not None:
        profile["household_id"] = session_context["household_id"]
    init_db()
    with get_connection() as conn:
        return save_member_profile(conn, profile)


@router.get("/profiles/{member_profile_id}", response_model=MemberProfileResponse)
def get_profile(
    member_profile_id: str,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    init_db()
    session_context = _session_context_from_authorization(authorization)
    with get_connection() as conn:
        profile = get_member_profile(conn, member_profile_id)
    if profile is None:
        raise HTTPException(status_code=404, detail="profile_not_found")
    _ensure_profile_visible_for_session(profile, session_context)
    return profile


@router.delete("/profiles/{member_profile_id}", response_model=ProfileDeleteResponse)
def delete_profile(
    member_profile_id: str,
    confirm: bool = Query(default=False),
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    if not confirm:
        raise HTTPException(status_code=400, detail="confirm=true_required")

    init_db()
    session_context = _session_context_from_authorization(authorization)
    with get_connection() as conn:
        existing = get_member_profile(conn, member_profile_id)
        if existing is None:
            raise HTTPException(status_code=404, detail="profile_not_found")
        _ensure_profile_visible_for_session(existing, session_context)
        profile = deactivate_member_profile(conn, member_profile_id)

    if profile is None:
        raise HTTPException(status_code=404, detail="profile_not_found")

    return {
        "status": "ok",
        "member_profile_id": profile["member_profile_id"],
        "deleted": False,
        "deactivated": True,
    }


def _model_to_dict(model: Any) -> dict[str, Any]:
    if hasattr(model, "model_dump"):
        return model.model_dump()
    return model.dict()


def _session_context_from_authorization(
    authorization: str | None,
) -> dict[str, Any] | None:
    token = _bearer_token(authorization)
    if not token:
        return None
    context = get_session_context(token)
    if context is None:
        raise HTTPException(status_code=401, detail="unauthenticated")
    return context


def _bearer_token(authorization: str | None) -> str:
    value = str(authorization or "").strip()
    prefix = "bearer "
    if value.lower().startswith(prefix):
        return value[len(prefix) :].strip()
    return ""


def _ensure_profile_visible_for_session(
    profile: dict[str, Any],
    session_context: dict[str, Any] | None,
) -> None:
    if session_context is None:
        return
    profile_household_id = str(profile.get("household_id") or "").strip()
    session_household_id = str(session_context.get("household_id") or "").strip()
    if profile_household_id != session_household_id:
        raise HTTPException(status_code=404, detail="profile_not_found")
