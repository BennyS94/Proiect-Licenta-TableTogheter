from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class FlexibleBaseModel(BaseModel):
    class Config:
        extra = "allow"


ReplacementScope = Literal["individual_meal", "household_member_meal", "household_shared_meal"]


class MealReplacementRequest(FlexibleBaseModel):
    day_index: int = Field(default=1, ge=1, le=5)
    slot: str
    current_recipe_id: str
    alternative_recipe_id: str
    generation_type: str | None = None
    replace_scope: ReplacementScope | None = None
    member_id: str | None = None
    member_profile_id: str | None = None
    dataset_profile: str = "current"
    feedback_enabled: bool = True
    generation_options: dict[str, Any] = Field(default_factory=dict)


class MealReplacementResponse(FlexibleBaseModel):
    status: str = "ok"
    dry_run: bool = True
    replacement_allowed: bool = False
    approval_status: str | None = None
    plan_id: str | None = None
    source_plan_id: str | None = None
    new_plan_id: str | None = None
    generation_type: str | None = None
    replacement: dict[str, Any] = Field(default_factory=dict)
    impact: dict[str, Any] = Field(default_factory=dict)
    updated_plan: dict[str, Any] | None = None
    grocery_list: dict[str, Any] | None = None
    warnings: list[Any] = Field(default_factory=list)
