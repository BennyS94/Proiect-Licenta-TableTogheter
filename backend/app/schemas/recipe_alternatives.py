from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


ApprovalMode = Literal["approved_only", "include_review", "include_rejected_debug"]
ApprovalStatus = Literal["approved", "review", "rejected"]


class FlexibleBaseModel(BaseModel):
    class Config:
        extra = "allow"


class RecipeAlternativesRequest(FlexibleBaseModel):
    recipe_id: str | None = None
    slot: str | None = None
    top_k: int = Field(default=5, ge=1, le=25)
    candidate_pool_k: int = Field(default=20, ge=1, le=75)
    dataset_profile: str = "v1_2_demo_final"
    household_id: str | None = None
    member_profile_id: str | None = None
    member_profile: dict[str, Any] | None = None
    feedback_enabled: bool = True
    approval_mode: ApprovalMode = "include_review"
    generation_options: dict[str, Any] = Field(default_factory=dict)
    diagnostics: dict[str, Any] = Field(default_factory=dict)


class RecipeAlternativeItem(FlexibleBaseModel):
    recipe_id: str
    display_name: str
    similarity_score: float | None = None
    approval_status: ApprovalStatus
    approval_reasons: list[str] = Field(default_factory=list)
    rejection_reasons: list[str] = Field(default_factory=list)
    macro_delta: dict[str, Any] = Field(default_factory=dict)
    time_delta_min: float | None = None
    why_similar: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    diagnostics: dict[str, Any] = Field(default_factory=dict)


class RecipeAlternativesResponse(FlexibleBaseModel):
    status: str = "ok"
    recipe_id: str
    source_recipe: dict[str, Any] = Field(default_factory=dict)
    slot: str | None = None
    dataset_profile: str = "v1_2_demo_final"
    approval_mode: str = "include_review"
    alternatives: list[RecipeAlternativeItem] = Field(default_factory=list)
    summary: dict[str, Any] = Field(default_factory=dict)
    feedback_context_summary: dict[str, Any] = Field(default_factory=dict)
    warnings: list[str] = Field(default_factory=list)
