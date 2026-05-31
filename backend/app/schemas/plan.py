from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class PlanRetrieveResponse(BaseModel):
    class Config:
        extra = "allow"

    status: str = "ok"
    plan_id: str | None = None
    household_plan_id: str | None = None
    generation_type: str | None = None
    days: int | None = None
    daily_plan: list[dict[str, Any]] = Field(default_factory=list)
    warnings: list[Any] = Field(default_factory=list)
    diagnostics_summary: dict[str, Any] = Field(default_factory=dict)
