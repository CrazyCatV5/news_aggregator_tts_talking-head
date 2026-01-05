from fastapi import FastAPI, Query
from .db import init_db
from .query import list_news, build_digest
from .jobs import new_job_id, set_job, get_job, get_sources, get_errors
from .redis_client import get_redis
from .ui import router as ui_router

app = FastAPI(title="DFO Business News Aggregator", version="3.0.0")
app.include_router(ui_router)

@app.on_event("startup")
def startup():
    init_db()

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/ingest/run")
def ingest(limit_per_html_source: int = Query(20, ge=1, le=200)):
    job_id = new_job_id()
    set_job(job_id, status="queued", total_sources=0, done_sources=0, ingested=0, errors_count=0, message="Queued")
    r = get_redis()
    r.lpush("dfo:queue", f"{job_id}:{limit_per_html_source}")
    return {"ok": True, "job_id": job_id}

@app.get("/jobs/{job_id}")
def job(job_id: str):
    return get_job(job_id)

@app.get("/jobs/{job_id}/detail")
def job_detail(job_id: str, errors_limit: int = Query(50, ge=0, le=200)):
    return {"job": get_job(job_id), "sources": get_sources(job_id), "errors": get_errors(job_id, limit=errors_limit)}

@app.get("/news")
def news(
    window_hours: int = Query(24, ge=1, le=168),
    min_business: int = Query(2, ge=0, le=10),
    min_dfo: int = Query(2, ge=0, le=10),
    require_company: bool = Query(False),
    limit: int = Query(50, ge=1, le=200),
):
    items = list_news(window_hours=window_hours, min_business=min_business, min_dfo=min_dfo, require_company=require_company, limit=limit)
    return {"ok": True, "n": len(items), "items": items}

@app.get("/digest")
def digest(
    window_hours: int = Query(24, ge=1, le=168),
    min_business: int = Query(2, ge=0, le=10),
    min_dfo: int = Query(2, ge=0, le=10),
    require_company: bool = Query(False),
    limit: int = Query(50, ge=1, le=200),
):
    items = list_news(window_hours=window_hours, min_business=min_business, min_dfo=min_dfo, require_company=require_company, limit=limit)
    return {"ok": True, "n": len(items), "digest": build_digest(items)}
