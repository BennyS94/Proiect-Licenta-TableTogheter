from typing import Literal, TypedDict


class HealthResponse(TypedDict):
    status: Literal["ok"]
    service: str
    version: str
    database: Literal["ok", "not_initialized"]
