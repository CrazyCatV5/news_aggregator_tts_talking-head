from __future__ import annotations

import datetime as dt
import json
import logging
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional, Tuple

import httpx
import time

from .db import connect
from .query import _exclude_war_params, _exclude_war_where_sql
from .config import settings

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DigestParams:
    """Selection params snapshot persisted with the digest."""

    top_n: int = 5
    prefer_days: int = 2
    max_lookback_days: int = 60
    min_interest: int = 5
    min_dfo: int = 2
    min_business: int = 2
    exclude_war: bool = True
    only_dfo_business: bool = True

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)


def _now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _parse_day(day: str) -> dt.date:
    return dt.date.fromisoformat(day)


def _day_str(d: dt.date) -> str:
    return d.isoformat()


def _status_for_count(n: int, top_n: int) -> str:
    if n >= top_n:
        return "ready"
    if n == 0:
        return "empty"
    return "partial"


def ensure_daily_digest(day: str, params: DigestParams) -> Dict[str, Any]:
    """Get or create a daily_digests row for a given day."""
    now = _now_iso()
    with connect() as con:
        row = con.execute("SELECT * FROM daily_digests WHERE day = ?", (day,)).fetchone()
        if row:
            return dict(row)

        con.execute(
            """
            INSERT INTO daily_digests (day, created_at, updated_at, params_json, status, note)
            VALUES (?, ?, ?, ?, 'draft', '')
            """,
            (day, now, now, params.to_json()),
        )
        con.commit()
        row2 = con.execute("SELECT * FROM daily_digests WHERE day = ?", (day,)).fetchone()
        return dict(row2)


def list_digests(limit: int = 50, offset: int = 0) -> Dict[str, Any]:
    with connect() as con:
        rows = con.execute(
            """
            SELECT d.id, d.day, d.status, d.created_at, d.updated_at,
                   (SELECT COUNT(1) FROM daily_digest_items x WHERE x.digest_id = d.id) AS items_count
            FROM daily_digests d
            ORDER BY d.day DESC
            LIMIT ? OFFSET ?
            """,
            (limit, offset),
        ).fetchall()
        return {"items": [dict(r) for r in rows], "limit": limit, "offset": offset}


def get_digest_by_day(day: str) -> Optional[Dict[str, Any]]:
    with connect() as con:
        d = con.execute("SELECT * FROM daily_digests WHERE day = ?", (day,)).fetchone()
        if not d:
            return None
        digest = dict(d)
        items = con.execute(
            """
            WITH latest AS (
              SELECT item_id, MAX(id) AS max_id
              FROM llm_analyses
              GROUP BY item_id
            )
            SELECT
              di.rank,
              i.id AS item_id,
              i.source_name, i.url, i.title,
              i.published_at, i.fetched_at,
              i.business_score, i.dfo_score,
              a.id AS llm_id,
              a.created_at AS llm_created_at,
              a.is_dfo_business,
              a.interest_score,
              a.title_short, a.bulletin, a.summary, a.why, a.tags
            FROM daily_digest_items di
            JOIN items i ON i.id = di.item_id
            LEFT JOIN latest l ON l.item_id = i.id
            LEFT JOIN llm_analyses a ON a.id = l.max_id
            WHERE di.digest_id = ?
            ORDER BY di.rank ASC
            """,
            (digest["id"],),
        ).fetchall()
        digest["items"] = [dict(r) for r in items]
        digest["items_count"] = len(digest["items"])
        return digest


def _base_candidates_sql(params: DigestParams) -> Tuple[str, List[Any]]:
    """Return SQL + params that yields candidate items joined with latest llm analysis.

    This selection is aligned with the semantics of the UI's LLM Dashboard, but:
    - always uses latest llm_analyses per item_id (MAX(id))
    - does NOT depend on prompt_version
    """
    sql = """
    WITH latest AS (
      SELECT item_id, MAX(id) AS max_id
      FROM llm_analyses
      GROUP BY item_id
    )
    SELECT
      i.id AS item_id,
      i.source_name, i.url, i.title,
      i.published_at, i.fetched_at,
      i.business_score, i.dfo_score,
      a.id AS llm_id,
      a.created_at AS llm_created_at,
      a.is_dfo_business,
      a.interest_score,
      a.title_short, a.bulletin, a.summary, a.why
    FROM items i
    JOIN latest l ON l.item_id = i.id
    JOIN llm_analyses a ON a.id = l.max_id
    WHERE 1=1
      AND i.business_score >= ?
      AND i.dfo_score >= ?
      AND a.interest_score >= ?
    """
    p: List[Any] = [params.min_business, params.min_dfo, params.min_interest]

    if params.only_dfo_business:
        sql += " AND a.is_dfo_business = 1\n"

    if params.exclude_war:
        sql += " AND " + _exclude_war_where_sql() + "\n"
        p.extend(_exclude_war_params())

    # global uniqueness: exclude items already used in any digest
    sql += " AND i.id NOT IN (SELECT item_id FROM daily_digest_items)\n"

    return sql, p


def _count(sql: str, params: List[Any]) -> int:
    with connect() as con:
        r = con.execute(f"SELECT COUNT(1) AS n FROM ({sql})", params).fetchone()
        return int(r["n"]) if r else 0


def compute_diagnostics(day: str, params: DigestParams) -> Dict[str, Any]:
    """Compute counts after major filters to make selection explainable."""
    base_sql, base_params = _base_candidates_sql(params)
    # base_sql already includes scores + interest + optional exclude_war + only_dfo_business + exclude used.
    # We also report bucket counts.
    d0 = _parse_day(day)
    days_in = [_day_str(d0 - dt.timedelta(days=i)) for i in range(params.prefer_days)]

    prefer_sql = base_sql + " AND substr(COALESCE(i.published_at, i.fetched_at), 1, 10) IN ({})".format(
        ",".join(["?"] * len(days_in))
    )
    prefer_params = base_params + days_in

    min_day = _day_str(d0 - dt.timedelta(days=params.max_lookback_days - 1))
    # fallback: older than last prefer day (i.e., < D-(prefer_days-1)) and >= min_day
    cutoff_day = _day_str(d0 - dt.timedelta(days=params.prefer_days - 1))
    fallback_sql = (
        base_sql
        + " AND substr(COALESCE(i.published_at, i.fetched_at), 1, 10) < ?\n"
        + " AND substr(COALESCE(i.published_at, i.fetched_at), 1, 10) >= ?\n"
    )
    fallback_params = base_params + [cutoff_day, min_day]

    return {
        "prefer_days": days_in,
        "max_lookback_days": params.max_lookback_days,
        "counts": {
            "candidates_total": _count(base_sql, base_params),
            "prefer_bucket": _count(prefer_sql, prefer_params),
            "fallback_bucket": _count(fallback_sql, fallback_params),
        },
    }


def _select_prefer(day: str, params: DigestParams, need: int) -> List[Dict[str, Any]]:
    base_sql, base_params = _base_candidates_sql(params)
    d0 = _parse_day(day)
    days_in = [_day_str(d0 - dt.timedelta(days=i)) for i in range(params.prefer_days)]
    sql = (
        base_sql
        + " AND substr(COALESCE(i.published_at, i.fetched_at), 1, 10) IN ({})\n".format(
            ",".join(["?"] * len(days_in))
        )
        + " ORDER BY a.interest_score DESC, COALESCE(i.published_at, i.fetched_at) DESC\n"
        + " LIMIT ?"
    )
    p = base_params + days_in + [need]
    with connect() as con:
        rows = con.execute(sql, p).fetchall()
        return [dict(r) for r in rows]


def _select_fallback(day: str, params: DigestParams, need: int) -> List[Dict[str, Any]]:
    base_sql, base_params = _base_candidates_sql(params)
    d0 = _parse_day(day)
    min_day = _day_str(d0 - dt.timedelta(days=params.max_lookback_days - 1))
    cutoff_day = _day_str(d0 - dt.timedelta(days=params.prefer_days - 1))
    sql = (
        base_sql
        + " AND substr(COALESCE(i.published_at, i.fetched_at), 1, 10) < ?\n"
        + " AND substr(COALESCE(i.published_at, i.fetched_at), 1, 10) >= ?\n"
        + " ORDER BY a.interest_score DESC, COALESCE(i.published_at, i.fetched_at) DESC\n"
        + " LIMIT ?"
    )
    p = base_params + [cutoff_day, min_day, need]
    with connect() as con:
        rows = con.execute(sql, p).fetchall()
        return [dict(r) for r in rows]


def create_or_refill_daily_digest(
    day: str,
    params: DigestParams,
    *,
    refill: bool = True,
    force: bool = False,
) -> Dict[str, Any]:
    """Create (if missing) and fill/refill a daily digest.

    Idempotent by default:
    - if digest already has >= top_n items, no changes.
    - if refill=True, it will try to fill missing ranks.
    - if force=True, it will delete the composition and rebuild it.
    """
    digest = ensure_daily_digest(day, params)
    digest_id = int(digest["id"])

    with connect() as con:
        cur = con.execute(
            "SELECT COUNT(1) AS n FROM daily_digest_items WHERE digest_id = ?", (digest_id,)
        ).fetchone()
        cur_count = int(cur["n"]) if cur else 0

    if force:
        with connect() as con:
            con.execute("DELETE FROM daily_digest_items WHERE digest_id = ?", (digest_id,))
            con.execute(
                "UPDATE daily_digests SET updated_at = ?, status = 'draft', note = '' WHERE id = ?",
                (_now_iso(), digest_id),
            )
            con.commit()
        cur_count = 0

    if (cur_count >= params.top_n) or (cur_count > 0 and not refill and not force):
        out = get_digest_by_day(day) or digest
        out["diagnostics"] = compute_diagnostics(day, params)
        out["changed"] = False
        return out

    need = max(0, params.top_n - cur_count)
    picked: List[Dict[str, Any]] = []
    if need > 0:
        picked.extend(_select_prefer(day, params, need))
        need2 = max(0, need - len(picked))
        if need2 > 0:
            picked.extend(_select_fallback(day, params, need2))

    # Insert selected items into digest with ranks.
    now = _now_iso()
    inserted = 0
    with connect() as con:
        con.execute("BEGIN")
        # recompute current count inside tx
        r = con.execute(
            "SELECT COUNT(1) AS n FROM daily_digest_items WHERE digest_id = ?", (digest_id,)
        ).fetchone()
        base_rank = int(r["n"]) if r else 0

        for idx, it in enumerate(picked, start=1):
            rank = base_rank + idx
            if rank > params.top_n:
                break
            try:
                con.execute(
                    """
                    INSERT INTO daily_digest_items (digest_id, item_id, rank, added_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (digest_id, int(it["item_id"]), rank, now),
                )
                inserted += 1
            except Exception as e:
                # Respect global uniqueness and idempotency: skip conflicts.
                logger.warning("digest insert skipped (digest_id=%s item_id=%s): %s", digest_id, it.get("item_id"), e)

        # Update digest status
        r2 = con.execute(
            "SELECT COUNT(1) AS n FROM daily_digest_items WHERE digest_id = ?", (digest_id,)
        ).fetchone()
        new_count = int(r2["n"]) if r2 else 0
        status = _status_for_count(new_count, params.top_n)
        con.execute(
            "UPDATE daily_digests SET updated_at = ?, status = ? WHERE id = ?",
            (_now_iso(), status, digest_id),
        )
        con.commit()

    out = get_digest_by_day(day) or ensure_daily_digest(day, params)
    out["diagnostics"] = compute_diagnostics(day, params)
    out["changed"] = True
    out["inserted"] = inserted
    return out


async def generate_digest_script(
    day: str,
    *,
    force: bool = False,
) -> Dict[str, Any]:
    """Generate and store a TTS-ready script for a daily digest.

    This uses the dedicated LLM service (Ollama-backed) and persists the resulting
    script as JSON segments in daily_digests.script_json.
    """
    digest = get_digest_by_day(day)
    if not digest:
        raise ValueError(f"digest not found for day={day}")

    digest_id = int(digest["id"])
    items: List[Dict[str, Any]] = digest.get("items") or []
    if not items:
        raise ValueError(f"digest has no items for day={day}")

    # Idempotency: if a script already exists and force=False, return it.
    if (not force) and digest.get("script_json"):
        try:
            parsed = json.loads(digest["script_json"]) if isinstance(digest["script_json"], str) else digest["script_json"]
        except Exception:
            parsed = digest["script_json"]
        digest["script"] = parsed
        return digest

    payload = {
        "day": day,
        "items": [
            {
                "rank": int(it.get("rank") or 0),
                "item_id": int(it.get("item_id") or 0),
                "title_short": it.get("title_short") or it.get("title") or "",
                "bulletin": it.get("bulletin") or "",
                "summary": it.get("summary") or "",
                "why": it.get("why") or "",
                "source_name": it.get("source_name") or "",
                "url": it.get("url") or "",
            }
            for it in items
        ],
        "tone": "деловой",
    }

    t0 = time.time()
    logger.info("digest_script start day=%s force=%s items=%s llm_url=%s", day, force, len(items), settings.llm_service_url)

    llm_url = settings.llm_service_url.rstrip("/")
    async with httpx.AsyncClient(timeout=180) as client:
        r = await client.post(f"{llm_url}/digest_script", json=payload)
        if r.status_code >= 400:
            logger.error("digest_script llm error day=%s status=%s body=%s", day, r.status_code, r.text[:500])
            raise RuntimeError(f"llm /digest_script error {r.status_code}: {r.text[:500]}")
        data = r.json()

    segments = data.get("segments")
    if not isinstance(segments, list) or not segments:
        logger.error("digest_script llm returned empty segments day=%s raw=%s", day, str(data)[:500])
        raise RuntimeError("llm /digest_script returned empty segments")

    now = _now_iso()
    with connect() as con:
        con.execute(
            """
            UPDATE daily_digests
            SET updated_at = ?, script_json = ?, script_model = ?, script_created_at = ?
            WHERE id = ?
            """,
            (now, json.dumps(segments, ensure_ascii=False), data.get("model") or "", now, digest_id),
        )
        con.commit()

    out = get_digest_by_day(day) or digest
    out["script"] = segments
    logger.info("digest_script done day=%s segments=%s dt=%.2fs", day, len(segments), (time.time()-t0))
    return out
