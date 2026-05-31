from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException

from backend.app.schemas.household import HouseholdDemoResponse


router = APIRouter(tags=["households"])
DEMO_HOUSEHOLD_PATH = Path("profiles/household_profile_demo_v1.json")


@router.get("/households/demo", response_model=HouseholdDemoResponse)
def get_demo_household() -> dict[str, Any]:
    if not DEMO_HOUSEHOLD_PATH.exists():
        raise HTTPException(status_code=404, detail="demo_household_profile_missing")
    try:
        payload = json.loads(DEMO_HOUSEHOLD_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=500, detail="demo_household_profile_invalid") from exc
    return payload
