from fastapi import APIRouter

from app.api.routes.health import router as health_router
from app.api.routes.import_routes import router as import_router
from app.api.routes.menu import router as menu_router
from app.api.routes.menu_periodes import router as menu_periodes_router
from app.api.routes.planning import router as planning_router
from app.api.routes.recipes import router as recipes_router
from app.api.routes.base_data import router as base_data_router
from app.api.routes.workfloor import router as workfloor_router

api_router = APIRouter()
api_router.include_router(health_router, tags=["health"])
api_router.include_router(planning_router, prefix="/planning", tags=["planning"])
api_router.include_router(import_router, prefix="/import", tags=["import"])
api_router.include_router(menu_router, prefix="/menu", tags=["menu"])
api_router.include_router(menu_periodes_router, prefix="/menu-periodes", tags=["menu-periodes"])
api_router.include_router(recipes_router, prefix="/recipes", tags=["recipes"])
api_router.include_router(base_data_router, prefix="/base-data", tags=["base-data"])
api_router.include_router(workfloor_router, prefix="/workfloor", tags=["workfloor"])