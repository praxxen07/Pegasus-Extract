from fastapi import APIRouter

from core.ai_provider import ai_provider


router = APIRouter(redirect_slashes=False)


@router.get("/health")
async def health():
    return {
        "status": "ok",
        "service": "PEGASUS EXTRACT — Phase 1",
        "ai_providers": ai_provider.status(),
        "playwright": "ready",
    }

