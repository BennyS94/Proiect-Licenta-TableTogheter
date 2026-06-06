from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class MemberProfileCreateRequest(BaseModel):
    class Config:
        extra = "allow"

    household_id: str
    member_profile_id: str | None = None
    display_name: str
    age: int
    sex: str
    weight_kg: float
    height_cm: float
    activity_level: str
    goal: str
    goal_speed: str
    training: dict[str, Any] = Field(default_factory=dict)
    meal_config: dict[str, Any] = Field(default_factory=dict)
    dietary_preferences: dict[str, Any] = Field(default_factory=dict)
    bf_profile: str | None = None
    is_active: bool = True


class MemberProfileResponse(BaseModel):
    class Config:
        extra = "allow"

    member_profile_id: str
    household_id: str
    display_name: str
    age: int
    sex: str
    weight_kg: float
    height_cm: float
    activity_level: str
    goal: str
    goal_speed: str
    training: dict[str, Any] = Field(default_factory=dict)
    meal_config: dict[str, Any] = Field(default_factory=dict)
    dietary_preferences: dict[str, Any] = Field(default_factory=dict)
    bf_profile: str | None = "normal"
    is_active: bool = True
    created_at: str
    updated_at: str


class ProfilesListResponse(BaseModel):
    household_id: str | None = None
    source: str = "sqlite"
    profiles: list[MemberProfileResponse] = Field(default_factory=list)


class ProfileDeleteResponse(BaseModel):
    status: str
    member_profile_id: str
    deactivated: bool
    deleted: bool = False
    message: str | None = None
