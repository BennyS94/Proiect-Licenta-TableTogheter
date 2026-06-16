from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


FeedbackType = Literal["liked", "disliked", "too_long", "explicit_avoid"]


class FeedbackEventRequest(BaseModel):
    event_id: str | None = None
    household_id: str
    member_profile_id: str | None = None
    recipe_id: str
    plan_id: str | None = None
    slot: str | None = None
    feedback_type: FeedbackType
    notes: str | None = None
    source: str = "api"


class FeedbackEventResponse(BaseModel):
    event_id: str
    status: str = "stored"
    feedback_type: FeedbackType
    recipe_id: str
    created_at: str


class FeedbackContextResponse(BaseModel):
    class Config:
        extra = "allow"

    household_id: str = ""
    member_profile_id: str = ""
    event_count: int = 0
    hard_filters: dict[str, Any] = Field(default_factory=dict)
    score_preferences: dict[str, Any] = Field(default_factory=dict)
    time_preferences: dict[str, Any] = Field(default_factory=dict)
    meta: dict[str, Any] = Field(default_factory=dict)


class FeedbackDeleteResponse(BaseModel):
    deleted: bool
    deleted_event_count: int
    household_id: str = ""
    member_profile_id: str = ""
