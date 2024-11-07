from pathlib import Path
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    BASE_DIR: Path = Path(__file__).resolve().parent.parent.parent
    DATA_DIR: Path = BASE_DIR / "data"
    CURRENT_DIR: Path = DATA_DIR / "current"
    DAILY_DIR: Path = DATA_DIR / "daily"
    
    API_V1_STR: str = "/api/v1"
    PROJECT_NAME: str = "Market Data API"
    
    class Config:
        case_sensitive = True

settings = Settings()