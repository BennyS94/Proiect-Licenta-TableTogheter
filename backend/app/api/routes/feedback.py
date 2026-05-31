from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from backend.app.db.database import get_connection, init_db
from backend.app.db.repositories import (
    build_feedback_context_from_db,
    delete_feedback_events,
    save_feedback_event,
)
from backend.app.schemas.feedback import (
    FeedbackContextResponse,
    FeedbackDeleteResponse,
    FeedbackEventRequest,
    FeedbackEventResponse,
)


router = APIRouter(tags=["feedback"])


@router.post("/feedback", response_model=FeedbackEventResponse)
def create_feedback_event(payload: FeedbackEventRequest) -> dict[str, Any]:
    event = _model_to_dict(payload)
    init_db()
    with get_connection() as conn:
        saved = save_feedback_event(conn, event)
    return {
        "event_id": saved["event_id"],
        "status": "stored",
        "feedback_type": saved["feedback_type"],
        "recipe_id": saved["recipe_id"],
        "created_at": saved["created_at"],
    }


@router.get("/feedback/context", response_model=FeedbackContextResponse)
def get_feedback_context(
    household_id: str | None = None,
    member_profile_id: str | None = None,
) -> dict[str, Any]:
    init_db()
    with get_connection() as conn:
        return build_feedback_context_from_db(
            conn,
            household_id=household_id,
            member_profile_id=member_profile_id,
        )


@router.delete("/feedback", response_model=FeedbackDeleteResponse)
def delete_feedback(
    household_id: str | None = None,
    member_profile_id: str | None = None,
    confirm: bool = Query(default=False),
) -> dict[str, Any]:
    if not confirm:
        raise HTTPException(status_code=400, detail="confirm=true_required")
    init_db()
    with get_connection() as conn:
        deleted_count = delete_feedback_events(
            conn,
            household_id=household_id,
            member_profile_id=member_profile_id,
        )
    return {
        "deleted": True,
        "deleted_event_count": deleted_count,
        "household_id": household_id or "",
        "member_profile_id": member_profile_id or "",
    }


def _model_to_dict(model: Any) -> dict[str, Any]:
    if hasattr(model, "model_dump"):
        return model.model_dump()
    return model.dict()
