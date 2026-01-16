from fastapi import FastAPI, Query, HTTPException
import logging
import time
from fastapi.staticfiles import StaticFiles
from pathlib import Path
from .db import init_db, connect
from .query import (

    list_news,
    build_digest,
    list_news_by_day,
    get_item,
    list_items,
    list_item_sources,
)
from .llm_queue import enqueue_candidates, list_llm_items, get_llm_item
from .daily_digests import (
    DigestParams,
    list_digests,
    get_digest_by_day,
    create_or_refill_daily_digest,
    generate_digest_script,
)
from .jobs import new_job_id, set_job, get_job, get_sources, get_errors, list_jobs
from .redis_client import get_redis
from .ingest import ingest_job_init
from .sources import list_source_names, queue_key_for_source
from .ui import router as ui_router
from .tts_api import router as tts_router

app = FastAPI(title="DFO Business News Aggregator", version="3.0.0")
app.include_router(ui_router)
app.include_router(tts_router)

logger = logging.getLogger(__name__)

# Static UI assets
BASE_DIR = Path(__file__).resolve().parent
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")

@app.on_event("startup")
def startup():
    init_db()

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/ingest/run")
def ingest(limit_per_html_source: int = Query(20, ge=1, le=200)):
    """Create a job and enqueue one task per source into its dedicated queue."""
    job_id = new_job_id()
    ingest_job_init(job_id)

    r = get_redis()
    for src_name in list_source_names():
        r.lpush(queue_key_for_source(src_name), f"{job_id}:{limit_per_html_source}")

    return {"ok": True, "job_id": job_id, "enqueued_sources": len(list_source_names())}


@app.get("/jobs")
def jobs(limit: int = Query(20, ge=1, le=100), offset: int = Query(0, ge=0, le=10000)):
    return list_jobs(limit=limit, offset=offset)

@app.get("/jobs/{job_id}")
def job(job_id: str):
    return get_job(job_id)

@app.get("/jobs/{job_id}/detail")
def job_detail(job_id: str, errors_limit: int = Query(50, ge=0, le=200)):
    return {"job": get_job(job_id), "sources": get_sources(job_id), "errors": get_errors(job_id, limit=errors_limit)}

@app.get("/news")
def news(
    window_hours: int = Query(0, ge=0, le=168),
    min_business: int = Query(2, ge=0, le=10),
    min_dfo: int = Query(2, ge=0, le=10),
    require_company: bool = Query(False),
    exclude_war: bool = Query(False),
    limit: int = Query(50, ge=1, le=200),
):
    items = list_news(
        window_hours=window_hours,
        min_business=min_business,
        min_dfo=min_dfo,
        require_company=require_company,
        exclude_war=exclude_war,
        limit=limit,
    )
    return {"ok": True, "n": len(items), "items": items}

@app.get("/digest")
def digest(
    window_hours: int = Query(0, ge=0, le=168),
    min_business: int = Query(2, ge=0, le=10),
    min_dfo: int = Query(2, ge=0, le=10),
    require_company: bool = Query(False),
    exclude_war: bool = Query(False),
    limit: int = Query(50, ge=1, le=200),
):
    items = list_news(
        window_hours=window_hours,
        min_business=min_business,
        min_dfo=min_dfo,
        require_company=require_company,
        exclude_war=exclude_war,
        limit=limit,
    )
    return {"ok": True, "n": len(items), "digest": build_digest(items)}


@app.get("/news/by-day")
def news_by_day(
    days: int = Query(7, ge=1, le=31),
    min_business: int = Query(2, ge=0, le=10),
    min_dfo: int = Query(2, ge=0, le=10),
    require_company: bool = Query(False),
    exclude_war: bool = Query(False),
    limit_per_day: int = Query(50, ge=1, le=200),
):
    items = list_news_by_day(
        days=days,
        min_business=min_business,
        min_dfo=min_dfo,
        require_company=require_company,
        exclude_war=exclude_war,
        limit_per_day=limit_per_day,
    )
    items = items or {}
    n = sum(len(v) for v in items.values())
    return {"ok": True, "n": n, "items": items}


# IMPORTANT: this route must be defined BEFORE /items/{item_id}
# otherwise FastAPI will try to parse "by-day" as an integer item_id and return 422.
@app.delete("/items/by-day")
def delete_items_by_day(day: str = Query(..., pattern=r"^\d{4}-\d{2}-\d{2}$")):
    like = day + "%"
    with connect() as con:
        cur = con.execute(
            "DELETE FROM items WHERE COALESCE(published_at, fetched_at) LIKE ?",
            (like,),
        )
        con.commit()
        deleted = cur.rowcount
    return {"ok": True, "deleted": deleted, "day": day}
@app.delete("/items/{item_id}")
def delete_item(item_id: int):
    with connect() as con:
        cur = con.execute("DELETE FROM items WHERE id = ?", (item_id,))
        con.commit()
        deleted = cur.rowcount
    if deleted == 0:
        raise HTTPException(status_code=404, detail="item not found")
    return {"ok": True, "deleted": deleted, "item_id": item_id}


@app.get("/items/{item_id}")
def read_item(item_id: int):
    """Fetch a single item including the full stored body text."""
    item = get_item(item_id)
    if not item:
        raise HTTPException(status_code=404, detail="item not found")
    return {"ok": True, "item": item}


# Same endpoint under /api for backwards-compat clients. Must be defined before /api/items/{item_id}.
@app.delete("/api/items/by-day")
def api_delete_items_by_day(day: str = Query(..., pattern=r"^\d{4}-\d{2}-\d{2}$")):
    return delete_items_by_day(day=day)


@app.get("/api/items/{item_id}")
def api_read_item(item_id: int):
    """Compatibility alias for item detail."""
    return read_item(item_id)


@app.get("/api/items")
def api_list_items(
    q: str | None = None,
    source: str | None = None,
    published_from: str | None = None,
    published_to: str | None = None,
    fetched_from: str | None = None,
    fetched_to: str | None = None,
    biz_min: int | None = None,
    biz_max: int | None = None,
    dfo_min: int | None = None,
    dfo_max: int | None = None,
    has_company: int | None = None,
    exclude_war: bool = Query(False),
    sort: str | None = None,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """Catalog endpoint for UI. Supports text/time/source/score filtering."""
    out = list_items(
        q=q,
        source=source,
        published_from=published_from,
        published_to=published_to,
        fetched_from=fetched_from,
        fetched_to=fetched_to,
        biz_min=biz_min,
        biz_max=biz_max,
        dfo_min=dfo_min,
        dfo_max=dfo_max,
        has_company=has_company,
        exclude_war=exclude_war,
        sort=sort,
        limit=limit,
        offset=offset,
    )

    # Backward compatibility: list_items may return either dict or (rows, total).
    if isinstance(out, dict):
        total = int(out.get("total", 0))
        rows = out.get("items", [])
    else:
        rows, total = out  # type: ignore[misc]

    has_more = (offset + len(rows)) < int(total)
    return {"ok": True, "total": int(total), "limit": limit, "offset": offset, "has_more": has_more, "items": rows}


@app.get("/api/items/sources")
def api_list_item_sources():
    """Distinct sources found in items table (for catalog filter dropdown)."""
    return {"ok": True, "sources": list_item_sources()}

@app.delete("/items/purge")
def purge_items(days: int = Query(30, ge=1, le=3650)):
    import datetime as dt
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=days)
    with connect() as con:
        cur = con.execute(
            "DELETE FROM items WHERE COALESCE(published_at, fetched_at) < ?",
            (cutoff.isoformat(),),
        )
        con.commit()
        deleted = cur.rowcount
    return {"ok": True, "deleted": deleted, "cutoff": cutoff.isoformat(), "days": days}


# ---------------------------------------------------------------------------
# Daily digests
# ---------------------------------------------------------------------------


@app.get("/digests")
def digests(limit: int = Query(50, ge=1, le=200), offset: int = Query(0, ge=0)):
    """List daily digests (metadata only)."""
    out = list_digests(limit=limit, offset=offset)
    return {"ok": True, "total": len(out.get("items", [])), **out}


@app.get("/digests/daily")
def daily_digest(day: str = Query(..., pattern=r"^\d{4}-\d{2}-\d{2}$")):
    """Get a digest by day (YYYY-MM-DD). Does not auto-create."""
    d = get_digest_by_day(day)
    if not d:
        return {"ok": True, "exists": False, "day": day}
    return {"ok": True, "exists": True, "digest": d}


@app.post("/digests/daily/create")
def daily_digest_create(
    day: str = Query(..., pattern=r"^\d{4}-\d{2}-\d{2}$"),
    top_n: int = Query(5, ge=1, le=20),
    prefer_days: int = Query(2, ge=1, le=7),
    max_lookback_days: int = Query(60, ge=1, le=365),
    min_interest: int = Query(5, ge=0, le=100),
    min_dfo: int = Query(2, ge=0, le=10),
    min_business: int = Query(2, ge=0, le=10),
    exclude_war: bool = Query(True),
    only_dfo_business: bool = Query(True),
    refill: bool = Query(True),
    force: bool = Query(False),
):
    """Create or refill a daily digest.

    - Idempotent: if already has >= top_n items, no changes.
    - refill=true: fill missing ranks.
    - force=true: rebuild composition from scratch.
    """
    params = DigestParams(
        top_n=top_n,
        prefer_days=prefer_days,
        max_lookback_days=max_lookback_days,
        min_interest=min_interest,
        min_dfo=min_dfo,
        min_business=min_business,
        exclude_war=exclude_war,
        only_dfo_business=only_dfo_business,
    )
    out = create_or_refill_daily_digest(day, params, refill=refill, force=force)
    return {"ok": True, "digest": out}


@app.post("/digests/daily/script")
async def daily_digest_script_generate(
    day: str = Query(..., pattern=r"^\d{4}-\d{2}-\d{2}$"),
    force: bool = Query(False),
):
    """Generate (or return existing) TTS-ready script for a daily digest.

    Uses the separate LLM service endpoint /digest_script and stores segments as JSON
    in daily_digests.script_json.
    """
    t0 = time.time()
    logger.info("/digests/daily/script start day=%s force=%s", day, force)
    try:
        out = await generate_digest_script(day, force=force)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.exception("/digests/daily/script failed day=%s force=%s: %s", day, force, e)
        raise HTTPException(status_code=502, detail=str(e))
    logger.info("/digests/daily/script done day=%s took=%.2fs", day, (time.time()-t0))
    return {"ok": True, "digest": out}


# ---------------------------------------------------------------------------
# Compatibility routes: expose the same API under /api/* as well.
# This is useful when the service is placed behind a reverse proxy that
# reserves "/" for UI and forwards "/api" to the backend.
# ---------------------------------------------------------------------------

_API_PREFIX = "/api"

# ---------------------------------------------------------------------------
# Compatibility routes: expose the same API under /api/* as well.
# ---------------------------------------------------------------------------

_API_PREFIX = "/api"

def _register_prefixed_routes(prefix: str = _API_PREFIX) -> None:
    # Health
    app.add_api_route(f"{prefix}/health", health, methods=["GET"])

    # Sources (system sources, not per-job)
    app.add_api_route(f"{prefix}/sources", list_source_names, methods=["GET"])

    # Ingest (keep the same semantics as /ingest/run)
    app.add_api_route(f"{prefix}/ingest/run", ingest, methods=["POST"])

    # Jobs
    app.add_api_route(f"{prefix}/jobs", jobs, methods=["GET"])
    app.add_api_route(f"{prefix}/jobs/{{job_id}}", job, methods=["GET"])
    app.add_api_route(f"{prefix}/jobs/{{job_id}}/detail", job_detail, methods=["GET"])

    # News + digest
    app.add_api_route(f"{prefix}/news", news, methods=["GET"])
    app.add_api_route(f"{prefix}/digest", digest, methods=["GET"])
    app.add_api_route(f"{prefix}/news/by-day", news_by_day, methods=["GET"])

    # Deletes
    app.add_api_route(f"{prefix}/items/{{item_id}}", read_item, methods=["GET"])
    app.add_api_route(f"{prefix}/items/{{item_id}}", delete_item, methods=["DELETE"])
    app.add_api_route(f"{prefix}/items/by-day", delete_items_by_day, methods=["DELETE"])
    app.add_api_route(f"{prefix}/items/purge", purge_items, methods=["DELETE"])

    # Daily digests
    app.add_api_route(f"{prefix}/digests", digests, methods=["GET"])
    app.add_api_route(f"{prefix}/digests/daily", daily_digest, methods=["GET"])
    app.add_api_route(f"{prefix}/digests/daily/create", daily_digest_create, methods=["POST"])
    app.add_api_route(f"{prefix}/digests/daily/script", daily_digest_script_generate, methods=["POST"])

_register_prefixed_routes()
@app.post("/llm/enqueue")
def llm_enqueue(
    window_hours: int = Query(0, ge=0, le=168),
    min_business: int = Query(2, ge=0, le=10),
    min_dfo: int = Query(2, ge=0, le=10),
    require_company: bool = Query(False),
    exclude_war: bool = Query(True),
    limit: int = Query(200, ge=1, le=1000),
):
    n = enqueue_candidates(
        window_hours=window_hours,
        min_business=min_business,
        min_dfo=min_dfo,
        require_company=require_company,
        exclude_war=exclude_war,
        limit=limit,
    )
    return {"ok": True, "enqueued": n}


@app.get("/llm/items")
def llm_items(
    limit: int = Query(200, ge=1, le=500),
    offset: int = Query(0, ge=0),
    only_dfo_business: bool = Query(True),
):
    return {"ok": True, **list_llm_items(limit=limit, offset=offset, only_dfo_business=only_dfo_business)}


@app.get("/llm/items/{item_id}")
def llm_item(item_id: int):
    it = get_llm_item(item_id)
    if not it:
        raise HTTPException(status_code=404, detail="LLM analysis not found for this item")
    return {"ok": True, "item": it}

