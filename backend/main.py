import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api import api_router
from core.config import settings


app = FastAPI(title="PEGASUS EXTRACT", redirect_slashes=False)

# Allow the frontend origin in production, fall back to permissive for local dev
_frontend_url = os.getenv("FRONTEND_URL", "")
_allowed_origins = (
    [_frontend_url, "http://localhost:3001", "http://localhost:3000"]
    if _frontend_url
    else ["*"]
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)


@app.get("/")
async def root():
    return {"service": settings.project_name}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=settings.backend_port, reload=True)

