import os
from pathlib import Path

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = BASE_DIR / ".env"

# Load environment variables from backend/.env
load_dotenv(dotenv_path=ENV_PATH)


class Settings:
    project_name: str = "PEGASUS EXTRACT — Phase 1"
    backend_port: int = int(os.getenv("BACKEND_PORT", "8001"))

    anthropic_api_key: str = os.getenv("ANTHROPIC_API_KEY", "")
    groq_api_key: str = os.getenv("GROQ_API_KEY", "")


settings = Settings()

