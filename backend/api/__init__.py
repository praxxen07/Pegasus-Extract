from fastapi import APIRouter

from .analyze import router as analyze_router
from .extract import router as extract_router
from .health import router as health_router


api_router = APIRouter(redirect_slashes=False)
api_router.include_router(health_router, tags=["health"])
api_router.include_router(analyze_router, prefix="/analyze", tags=["analyze"])
api_router.include_router(extract_router)

