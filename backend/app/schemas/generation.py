from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class FlexibleBaseModel(BaseModel):
    class Config:
        extra = "allow"


class IndividualPlanGenerateRequest(FlexibleBaseModel):
    dataset_profile: str = "current"
    days: int = Field(default=3, ge=1, le=5)
    household_id: str | None = None
    member_profile_id: str | None = None
    member_profile: dict[str, Any] | None = None
    profile_path: str | None = None
    generation_options: dict[str, Any] = Field(default_factory=dict)
    include_grocery_list: bool | None = None
    include_purchase_suggestions: bool | None = None
    include_price_estimates: bool | None = None
    feedback_enabled: bool = True


class HouseholdPlanGenerateRequest(FlexibleBaseModel):
    dataset_profile: str = "current"
    days: int = Field(default=3, ge=1, le=5)
    household_id: str | None = None
    household_profile: dict[str, Any] | None = None
    household_profile_path: str | None = None
    selected_member_ids: list[str] = Field(default_factory=list)
    household_mode: str = "individual_breakfast_shared_main"
    household_allocation_mode: str = "macro_aware_simple"
    generation_options: dict[str, Any] = Field(default_factory=dict)
    include_grocery_list: bool | None = None
    include_purchase_suggestions: bool | None = None
    include_price_estimates: bool | None = None
    feedback_enabled: bool = True


class PlanGenerateResponse(FlexibleBaseModel):
    status: str = "ok"
    plan_id: str | None = None
    generation_type: str = "individual"
    member_profile_id: str | None = None
    days: int | None = None
    daily_plan: list[dict[str, Any]] = Field(default_factory=list)
    grocery_list: dict[str, Any] | None = None
    feedback_context_summary: dict[str, Any] = Field(default_factory=dict)
    warnings: list[Any] = Field(default_factory=list)
    diagnostics_summary: dict[str, Any] = Field(default_factory=dict)


class HouseholdPlanGenerateResponse(FlexibleBaseModel):
    status: str = "ok"
    household_plan_id: str | None = None
    plan_id: str | None = None
    generation_type: str = "household"
    selected_members: list[dict[str, Any]] = Field(default_factory=list)
    member_targets: list[dict[str, Any]] = Field(default_factory=list)
    days: int | None = None
    per_member_menus: list[dict[str, Any]] = Field(default_factory=list)
    shared_meals: list[dict[str, Any]] = Field(default_factory=list)
    household_grocery_list: dict[str, Any] | None = None
    household_grocery_scaling: list[dict[str, Any]] = Field(default_factory=list)
    member_macro_summaries: list[dict[str, Any]] = Field(default_factory=list)
    feedback_context_summary: dict[str, Any] = Field(default_factory=dict)
    warnings: list[Any] = Field(default_factory=list)
    diagnostics_summary: dict[str, Any] = Field(default_factory=dict)
