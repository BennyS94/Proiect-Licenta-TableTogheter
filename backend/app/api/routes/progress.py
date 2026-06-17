from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Header, HTTPException, Query

from backend.app.db.auth_repository import get_session_context
from backend.app.db.database import get_connection, init_db
from backend.app.db.repositories import (
    delete_daily_progress_snapshot,
    get_daily_progress_snapshot,
    get_generated_plan_record,
    get_member_profile,
    list_daily_progress_snapshots,
    save_daily_progress_snapshot,
)
from backend.app.schemas.progress import (
    DailyProgressDeleteResponse,
    DailyProgressListResponse,
    DailyProgressSaveRequest,
    DailyProgressSaveResponse,
)


router = APIRouter(prefix="/progress", tags=["progress"])
MAX_DAILY_PROGRESS_SNAPSHOTS_PER_PROFILE = 30


@router.post("/daily", response_model=DailyProgressSaveResponse)
def save_daily_progress(
    payload: DailyProgressSaveRequest,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    session_context = _session_context_from_authorization(authorization)
    household_id = str(session_context["household_id"])
    requested_household_id = str(payload.household_id or "").strip()
    if requested_household_id and requested_household_id != household_id:
        raise HTTPException(status_code=403, detail="household_scope_mismatch")

    init_db()
    with get_connection() as conn:
        _ensure_profile_in_household(
            conn,
            member_profile_id=payload.member_profile_id,
            household_id=household_id,
        )
        _ensure_plan_in_household(
            conn,
            plan_id=payload.plan_id,
            household_id=household_id,
        )
        snapshot, inserted = save_daily_progress_snapshot(
            conn,
            {
                **_model_to_dict(payload),
                "household_id": household_id,
            },
            max_snapshots_per_profile=MAX_DAILY_PROGRESS_SNAPSHOTS_PER_PROFILE,
        )
    return {
        "status": "saved" if inserted else "already_saved",
        "snapshot": snapshot,
        "already_saved": not inserted,
    }


@router.get("/daily", response_model=DailyProgressListResponse)
def list_daily_progress(
    member_profile_id: str | None = None,
    profile_id: str | None = Query(default=None),
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    session_context = _session_context_from_authorization(authorization)
    household_id = str(session_context["household_id"])
    scoped_profile_id = str(member_profile_id or profile_id or "").strip()
    if not scoped_profile_id:
        raise HTTPException(status_code=400, detail="member_profile_id_required")

    init_db()
    with get_connection() as conn:
        _ensure_profile_in_household(
            conn,
            member_profile_id=scoped_profile_id,
            household_id=household_id,
        )
        snapshots = list_daily_progress_snapshots(
            conn,
            household_id=household_id,
            member_profile_id=scoped_profile_id,
            limit=MAX_DAILY_PROGRESS_SNAPSHOTS_PER_PROFILE,
        )
    return {
        "status": "ok",
        "household_id": household_id,
        "member_profile_id": scoped_profile_id,
        "snapshots": snapshots,
    }


@router.delete("/daily/{progress_id}", response_model=DailyProgressDeleteResponse)
def delete_progress_snapshot(
    progress_id: str,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    session_context = _session_context_from_authorization(authorization)
    household_id = str(session_context["household_id"])

    init_db()
    with get_connection() as conn:
        existing = get_daily_progress_snapshot(conn, progress_id)
        if existing is None or str(existing.get("household_id") or "") != household_id:
            raise HTTPException(status_code=404, detail="progress_snapshot_not_found")
        deleted = delete_daily_progress_snapshot(conn, progress_id)
    if deleted is None:
        raise HTTPException(status_code=404, detail="progress_snapshot_not_found")
    return {
        "status": "deleted",
        "deleted": True,
        "progress_id": deleted["progress_id"],
        "member_profile_id": deleted["member_profile_id"],
    }


def _ensure_profile_in_household(
    conn: Any,
    *,
    member_profile_id: str,
    household_id: str,
) -> dict[str, Any]:
    profile = get_member_profile(conn, member_profile_id)
    if profile is None or str(profile.get("household_id") or "") != household_id:
        raise HTTPException(status_code=404, detail="profile_not_found")
    if not profile.get("is_active", True):
        raise HTTPException(status_code=404, detail="profile_not_found")
    return profile


def _ensure_plan_in_household(
    conn: Any,
    *,
    plan_id: str,
    household_id: str,
) -> dict[str, Any]:
    plan = get_generated_plan_record(conn, plan_id)
    if plan is None or str(plan.get("household_id") or "") != household_id:
        raise HTTPException(status_code=404, detail="plan_not_found")
    return plan


def _session_context_from_authorization(
    authorization: str | None,
) -> dict[str, Any]:
    token = _bearer_token(authorization)
    if not token:
        raise HTTPException(status_code=401, detail="unauthenticated")
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


def _model_to_dict(model: Any) -> dict[str, Any]:
    if hasattr(model, "model_dump"):
        return model.model_dump()
    return model.dict()
