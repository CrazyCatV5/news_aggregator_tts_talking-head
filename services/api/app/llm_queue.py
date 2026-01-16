from __future__ import annotations

import datetime as dt
import json
from typing import Any, Dict, List, Optional

import httpx

from .config import settings
from .db import connect
from .redis_client import get_redis
from .query import _exclude_war_where_sql, _exclude_war_params

LLM_QUEUE_KEY = "llm:queue"
LLM_SERVICE_URL = getattr(settings, "llm_service_url", None) or "http://llm:8099"
LLM_PROMPT_VERSION = "v1.0"


def enqueue_candidates(
    window_hours: int = 0,
    min_business: int = 2,
    min_dfo: int = 2,
    require_company: bool = False,
    exclude_war: bool = False,
    prompt_version: str = LLM_PROMPT_VERSION,
    limit: int = 200,
) -> int:
    """Select candidate items and enqueue them for LLM analysis."""
    params: List[Any] = []
    where = ["business_score >= ?", "dfo_score >= ?"]
    params.extend([min_business, min_dfo])

    # If window_hours == 0, process *all* not-yet-analysed items that match the filters.
    if window_hours and window_hours > 0:
        now = dt.datetime.now(dt.timezone.utc)
        cutoff = now - dt.timedelta(hours=window_hours)
        where.insert(0, "COALESCE(published_at, fetched_at) >= ?")
        params.insert(0, cutoff.isoformat())

    if require_company:
        where.append("has_company = 1")
    if exclude_war:
        where.append(_exclude_war_where_sql())
        params.extend(_exclude_war_params())

    where.append(f"id NOT IN (SELECT item_id FROM llm_analyses WHERE prompt_version = ?)")
    params.append(prompt_version)

    sql = f"""
    SELECT id
    FROM items
    WHERE {' AND '.join(where)}
    ORDER BY COALESCE(published_at, fetched_at) DESC
    LIMIT ?
    """
    params.append(limit)

    with connect() as con:
        rows = con.execute(sql, params).fetchall()
        ids = [int(r["id"]) for r in rows]

    if not ids:
        return 0

    r = get_redis()
    # push as JSON lines: {"item_id": 123}
    pipe = r.pipeline()
    for item_id in ids:
        pipe.rpush(LLM_QUEUE_KEY, json.dumps({"item_id": item_id}))
    pipe.execute()
    return len(ids)


def list_llm_items(
    limit: int = 200,
    offset: int = 0,
    only_dfo_business: bool = True,
) -> Dict[str, Any]:
    where = []
    params: List[Any] = []
    if only_dfo_business:
        where.append("a.is_dfo_business = 1")
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    with connect() as con:
        total = con.execute(f"SELECT COUNT(*) AS c FROM llm_analyses a {where_sql}", params).fetchone()["c"]
        rows = con.execute(
            f"""
            SELECT
              a.*,
              i.source_name, i.url, i.title, i.body, i.published_at, i.fetched_at,
              i.business_score, i.dfo_score, i.has_company
            FROM llm_analyses a
            JOIN items i ON i.id = a.item_id
            {where_sql}
            ORDER BY COALESCE(i.published_at, i.fetched_at) DESC, a.interest_score DESC
            LIMIT ? OFFSET ?
            """,
            [*params, limit, offset],
        ).fetchall()

        items = [dict(r) for r in rows]
        return {"total": int(total), "limit": limit, "offset": offset, "items": items}


def get_llm_item(item_id: int, prompt_version: str = LLM_PROMPT_VERSION) -> Optional[Dict[str, Any]]:
    with connect() as con:
        row = con.execute(
            """
            SELECT
              a.*,
              i.source_name, i.url, i.title, i.body, i.published_at, i.fetched_at,
              i.business_score, i.dfo_score, i.has_company
            FROM llm_analyses a
            JOIN items i ON i.id = a.item_id
            WHERE a.item_id = ? AND a.prompt_version = ?
            """,
            [item_id, prompt_version],
        ).fetchone()
        return dict(row) if row else None
