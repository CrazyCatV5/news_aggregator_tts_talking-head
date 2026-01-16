from pydantic import BaseModel
import os

class Settings(BaseModel):
    db_path: str = os.getenv("DB_PATH", "/data/app.db")
    sources_path: str = os.getenv("SOURCES_PATH", "/app/sources.json")
    user_agent: str = os.getenv("USER_AGENT", "dfo-news-aggregator/3.0")
    request_timeout: float = float(os.getenv("REQUEST_TIMEOUT", "25"))
    redis_url: str = os.getenv("REDIS_URL", "redis://redis:6379/0")
    fetch_concurrency: int = int(os.getenv("FETCH_CONCURRENCY", "16"))
    article_concurrency: int = int(os.getenv("ARTICLE_CONCURRENCY", "32"))
    db_commit_every: int = int(os.getenv("DB_COMMIT_EVERY", "25"))
    llm_service_url: str = os.getenv("LLM_SERVICE_URL", "http://llm:8099")
    llm_model: str = os.getenv("LLM_MODEL", "qwen2.5:14b-instruct-q4_K_M")
    llm_prompt_version: str = os.getenv("LLM_PROMPT_VERSION", "v1.0")


settings = Settings()
