from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class DailyProgressTotals(BaseModel):
    kcal: float | None = None
    protein_g: float | None = None
    carbs_g: float | None = None
    fat_g: float | None = None


class DailyProgressSaveRequest(BaseModel):
    household_id: str | None = None
    member_profile_id: str
    plan_id: str
    day_index: int
    planned: DailyProgressTotals = Field(default_factory=DailyProgressTotals)
    consumed: DailyProgressTotals = Field(default_factory=DailyProgressTotals)
    meal_completion: dict[str, Any] = Field(default_factory=dict)
    day_snapshot: dict[str, Any] = Field(default_factory=dict)


class DailyProgressSnapshot(BaseModel):
    progress_id: str
    household_id: str
    member_profile_id: str
    plan_id: str
    day_index: int
    saved_at: str
    planned: DailyProgressTotals
    consumed: DailyProgressTotals
    meal_completion: dict[str, Any] = Field(default_factory=dict)
    day_snapshot: dict[str, Any] = Field(default_factory=dict)
    created_at: str
    updated_at: str


class DailyProgressSaveResponse(BaseModel):
    status: str
    snapshot: DailyProgressSnapshot
    already_saved: bool = False


class DailyProgressListResponse(BaseModel):
    status: str = "ok"
    household_id: str
    member_profile_id: str
    snapshots: list[DailyProgressSnapshot] = Field(default_factory=list)


class DailyProgressDeleteResponse(BaseModel):
    status: str
    deleted: bool
    progress_id: str
    member_profile_id: str
