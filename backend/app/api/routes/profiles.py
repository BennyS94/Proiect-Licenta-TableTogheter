from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from backend.app.db.database import get_connection, init_db
from backend.app.db.repositories import (
    get_member_profile,
    list_member_profiles,
    save_member_profile,
)
from backend.app.schemas.profile import (
    MemberProfileCreateRequest,
    MemberProfileResponse,
    ProfilesListResponse,
)


router = APIRouter(tags=["profiles"])


@router.get("/profiles", response_model=ProfilesListResponse)
def get_profiles(
    household_id: str | None = None,
    active_only: bool = Query(default=True),
) -> dict[str, Any]:
    init_db()
    with get_connection() as conn:
        profiles = list_member_profiles(
            conn,
            household_id=household_id,
            active_only=active_only,
        )
    return {
        "household_id": household_id,
        "source": "sqlite",
        "profiles": profiles,
    }


@router.post("/profiles", response_model=MemberProfileResponse)
def create_profile(payload: MemberProfileCreateRequest) -> dict[str, Any]:
    profile = _model_to_dict(payload)
    init_db()
    with get_connection() as conn:
        return save_member_profile(conn, profile)


@router.get("/profiles/{member_profile_id}", response_model=MemberProfileResponse)
def get_profile(member_profile_id: str) -> dict[str, Any]:
    init_db()
    with get_connection() as conn:
        profile = get_member_profile(conn, member_profile_id)
    if profile is None:
        raise HTTPException(status_code=404, detail="profile_not_found")
    return profile


def _model_to_dict(model: Any) -> dict[str, Any]:
    if hasattr(model, "model_dump"):
        return model.model_dump()
    return model.dict()
