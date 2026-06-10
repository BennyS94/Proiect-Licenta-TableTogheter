from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from backend.app.schemas.auth import AuthAccount


class HouseholdDemoResponse(BaseModel):
    class Config:
        extra = "allow"

    household_id: str
    household_name: str
    active_member_ids: list[str] = Field(default_factory=list)
    members: list[dict[str, Any]] = Field(default_factory=list)
    household_preferences: dict[str, Any] = Field(default_factory=dict)
    meal_config: dict[str, Any] = Field(default_factory=dict)
    planning_config: dict[str, Any] = Field(default_factory=dict)


class HouseholdSettingsUpdateRequest(BaseModel):
    display_name: str


class HouseholdSettingsUpdateResponse(BaseModel):
    status: str
    message: str
    account: AuthAccount
