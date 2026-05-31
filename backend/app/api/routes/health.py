from fastapi import APIRouter

from backend.app.core.config import API_VERSION
from backend.app.db.database import check_db_connection
from backend.app.schemas.health import HealthResponse


router = APIRouter(tags=["health"])


@router.get("/health")
def get_health() -> HealthResponse:
    database_status = "ok" if check_db_connection() else "not_initialized"
    return {
        "status": "ok",
        "service": "tabletogether-api",
        "version": API_VERSION,
        "database": database_status,
    }
