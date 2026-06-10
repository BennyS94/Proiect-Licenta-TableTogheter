from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Header, HTTPException

from backend.app.db.auth_repository import (
    get_session_context,
    update_household_display_name,
)
from backend.app.schemas.household import (
    HouseholdDemoResponse,
    HouseholdSettingsUpdateRequest,
    HouseholdSettingsUpdateResponse,
)


router = APIRouter(tags=["households"])
DEMO_HOUSEHOLD_PATH = Path("profiles/household_profile_demo_v1.json")


@router.get("/households/demo", response_model=HouseholdDemoResponse)
def get_demo_household() -> dict[str, Any]:
    if not DEMO_HOUSEHOLD_PATH.exists():
        raise HTTPException(status_code=404, detail="demo_household_profile_missing")
    try:
        payload = json.loads(DEMO_HOUSEHOLD_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=500, detail="demo_household_profile_invalid") from exc
    return payload


@router.patch("/households/me", response_model=HouseholdSettingsUpdateResponse)
def update_current_household(
    payload: HouseholdSettingsUpdateRequest,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    session_context = _session_context_from_authorization(authorization)
    try:
        account = update_household_display_name(
            user_id=str(session_context["user_id"]),
            display_name=payload.display_name,
        )
    except ValueError as exc:
        detail = str(exc)
        status_code = 404 if detail in {"user_not_found", "household_not_found"} else 400
        raise HTTPException(status_code=status_code, detail=detail) from exc
    return {
        "status": "ok",
        "message": "Household updated",
        "account": account,
    }


def _session_context_from_authorization(
    authorization: str | None,
) -> dict[str, Any]:
    token = _bearer_token(authorization)
    if not token:
        raise HTTPException(status_code=401, detail="unauthenticated")
    session_context = get_session_context(token)
    if session_context is None:
        raise HTTPException(status_code=401, detail="unauthenticated")
    return session_context


def _bearer_token(authorization: str | None) -> str:
    value = str(authorization or "").strip()
    prefix = "bearer "
    if value.lower().startswith(prefix):
        return value[len(prefix) :].strip()
    return ""
