from fastapi import FastAPI

from backend.app.api.routes.feedback import router as feedback_router
from backend.app.api.routes.health import router as health_router
from backend.app.api.routes.households import router as households_router
from backend.app.api.routes.household_plans import router as household_plans_router
from backend.app.api.routes.plans import router as plans_router
from backend.app.api.routes.profiles import router as profiles_router
from backend.app.api.routes.recipes import router as recipes_router
from backend.app.core.config import API_VERSION, APP_NAME


app = FastAPI(title=APP_NAME, version=API_VERSION)
app.include_router(health_router)
app.include_router(plans_router)
app.include_router(household_plans_router)
app.include_router(households_router)
app.include_router(profiles_router)
app.include_router(feedback_router)
app.include_router(recipes_router)


@app.get("/")
def get_root() -> dict[str, str]:
    return {
        "service": "tabletogether-api",
        "version": API_VERSION,
        "health": "/health",
    }
