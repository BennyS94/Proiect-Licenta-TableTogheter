from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class GroceryListRetrieveResponse(BaseModel):
    class Config:
        extra = "allow"

    status: str = "ok"
    grocery_list_id: str | None = None
    plan_id: str | None = None
    household_id: str | None = None
    currency: str | None = None
    total_estimated_cost: float | None = None
    items: list[dict[str, Any]] = Field(default_factory=list)
    warnings: list[Any] = Field(default_factory=list)
