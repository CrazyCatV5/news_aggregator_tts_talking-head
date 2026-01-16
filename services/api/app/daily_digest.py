from __future__ import annotations

import datetime as dt
import json
from typing import Any, Dict, List, Optional

import httpx

from .config import settings
from .db import connect
from .query import _exclude_war_where_sql, _exclude_war_params

LLM_SERVICE_URL = getattr(settings, "llm_service_url", None) or "http://llm:8099"
DEFAULT_SCRIPT_PROMPT_VERSION = "digest_script_v1"


def _utc_today() -> dt.date:
    return dt.datetime.now(dt.timezone.utc).date()


def _parse_day(day: Optional[str]) -> dt.date:
    if not day:
        return _utc_today()
    return dt.date.fromisoformat(day)


def _day_str(d: dt.date) -> str:
    return d.isoformat()


def _coalesce_date_sql() -> str:
    # stored as ISO strings, we compare by prefix YYYY-MM-DD
    return "substr(COALESCE(i.published_at, i.fetched_at), 1, 10)"


def _latest_analysis_cte_sql(alias: str = "a") -> str:
    """
    SQLite CTE returning the latest llm_analyses row per item_id using MAX(id).
    """
    return f"""
        WITH latest AS (
            SELECT la.*
            FROM llm_analyses la
            JOIN (
                SELECT item_id, MAX(id) AS max_id
                FROM llm_analyses
                GROUP BY item_id
            ) x ON x.item_id = la.item_id AND x.max_id = la.id
        )
    """.strip()


def create_or_get_daily_digest(
    day: dt.date,
    top_n: int = 5,
    prefer_days: int = 2,
    max_lookback_days: int = 60,
    min_interest: int = 0,
    only_dfo_business: bool = True,
    exclude_war: bool = True,
    prompt_version: str = "v1",  # kept for compatibility, not used for selection anymore
) -> Dict[str, Any]:
    """
    Create (idempotently) a daily digest with fixed selection of items.

    Rules:
    - Prefer items from `day` and previous `prefer_days-1` days.
    - If insufficient, backfill from earlier days up to `max_lookback_days`.
    - Each item may be used in digests only once (enforced by UNIQUE(item_id) in digest_items).
    """

    day_s = _day_str(day)

    with connect() as con:
        row = con.execute("SELECT * FROM digests WHERE day = ?", (day_s,)).fetchone()
        if row:
            digest = dict(row)
            items = con.execute(
                f"""
                {_latest_analysis_cte_sql()}
                SELECT di.rank, i.*, a.interest_score, a.title_short, a.bulletin, a.summary, a.why, a.tags
                FROM digest_items di
                JOIN items i ON i.id = di.item_id
                LEFT JOIN latest a ON a.item_id = i.id
                WHERE di.digest_id = ?
                ORDER BY di.rank ASC
                """,
                (digest["id"],),
            ).fetchall()
            return {"created": False, "digest": digest, "items": [dict(r) for r in items]}

        # Select candidates
        prefer_list = [_day_str(day - dt.timedelta(days=k)) for k in range(max(1, prefer_days))]
        cutoff_backfill = _day_str(day - dt.timedelta(days=max_lookback_days))

        where = []
        params: List[Any] = []

        # Make the selection logic match what you described (score-based)
        if only_dfo_business:
            where.append("i.dfo_score >= 2")
            where.append("i.business_score >= 2")

        # need analysis for ranking and script inputs
        where.append("a.interest_score >= ?")
        params.append(int(min_interest))

        # never reuse any item already used in any digest
        where.append("i.id NOT IN (SELECT item_id FROM digest_items)")

        if exclude_war:
            where.append(_exclude_war_where_sql())
            params.extend(_exclude_war_params())

        base_where = " AND ".join(where)

        def _fetch(
            limit: int,
            restrict_days: Optional[List[str]] = None,
            backfill: bool = False,
        ) -> List[Dict[str, Any]]:
            w = base_where
            p = list(params)

            if restrict_days:
                placeholders = ",".join(["?"] * len(restrict_days))
                w = f"{w} AND {_coalesce_date_sql()} IN ({placeholders})"
                p.extend(restrict_days)

            if backfill:
                w = f"{w} AND {_coalesce_date_sql()} >= ?"
                p.append(cutoff_backfill)

            sql = f"""
                {_latest_analysis_cte_sql()}
                SELECT i.id, i.source_name, i.url, i.title, i.published_at, i.fetched_at,
                       a.interest_score, a.title_short, a.bulletin, a.summary, a.why, a.tags
                FROM items i
                JOIN latest a ON a.item_id = i.id
                WHERE {w}
                ORDER BY a.interest_score DESC, COALESCE(i.published_at, i.fetched_at) DESC, i.id DESC
                LIMIT ?
            """
            p.append(int(limit))
            rows = con.execute(sql, p).fetchall()
            return [dict(r) for r in rows]

        chosen: List[Dict[str, Any]] = []
        chosen_ids: set[int] = set()

        # First pass: prefer last `prefer_days`
        for r in _fetch(limit=top_n * 6, restrict_days=prefer_list, backfill=False):
            if r["id"] in chosen_ids:
                continue
            chosen.append(r)
            chosen_ids.add(r["id"])
            if len(chosen) >= top_n:
                break

        # Second pass: backfill older but recent (up to max_lookback_days)
        if len(chosen) < top_n:
            need = top_n - len(chosen)
            for r in _fetch(limit=max(need * 12, 60), restrict_days=None, backfill=True):
                if r["id"] in chosen_ids:
                    continue
                chosen.append(r)
                chosen_ids.add(r["id"])
                if len(chosen) >= top_n:
                    break

        created_at = dt.datetime.now(dt.timezone.utc).isoformat()
        params_json = json.dumps(
            {
                "top_n": top_n,
                "prefer_days": prefer_days,
                "max_lookback_days": max_lookback_days,
                "min_interest": min_interest,
                "only_dfo_business": only_dfo_business,
                "exclude_war": exclude_war,
                # keep for audit/debug even if not used for selection
                "prompt_version": prompt_version,
            },
            ensure_ascii=False,
        )

        # Create digest record
        cur = con.execute(
            "INSERT INTO digests(day, created_at, params_json) VALUES(?,?,?)",
            (day_s, created_at, params_json),
        )
        digest_id = cur.lastrowid

        # Insert digest items with ranks. UNIQUE(item_id) prevents reuse globally.
        rank = 1
        for it in chosen[:top_n]:
            try:
                con.execute(
                    "INSERT INTO digest_items(digest_id, item_id, rank) VALUES(?,?,?)",
                    (digest_id, int(it["id"]), rank),
                )
                rank += 1
            except Exception:
                # If insertion fails due to UNIQUE(item_id), skip and continue.
                continue

        con.commit()

        digest = dict(con.execute("SELECT * FROM digests WHERE id = ?", (digest_id,)).fetchone())
        items = con.execute(
            f"""
            {_latest_analysis_cte_sql()}
            SELECT di.rank, i.*, a.interest_score, a.title_short, a.bulletin, a.summary, a.why, a.tags
            FROM digest_items di
            JOIN items i ON i.id = di.item_id
            LEFT JOIN latest a ON a.item_id = i.id
            WHERE di.digest_id = ?
            ORDER BY di.rank ASC
            """,
            (digest_id,),
        ).fetchall()

        return {"created": True, "digest": digest, "items": [dict(r) for r in items]}


def get_daily_digest(day: dt.date, prompt_version: str = "v1") -> Optional[Dict[str, Any]]:
    # prompt_version kept for compatibility, latest analysis is returned
    day_s = _day_str(day)
    with connect() as con:
        row = con.execute("SELECT * FROM digests WHERE day = ?", (day_s,)).fetchone()
        if not row:
            return None
        digest = dict(row)
        items = con.execute(
            f"""
            {_latest_analysis_cte_sql()}
            SELECT di.rank, i.*, a.interest_score, a.title_short, a.bulletin, a.summary, a.why, a.tags
            FROM digest_items di
            JOIN items i ON i.id = di.item_id
            LEFT JOIN latest a ON a.item_id = i.id
            WHERE di.digest_id = ?
            ORDER BY di.rank ASC
            """,
            (digest["id"],),
        ).fetchall()
        return {"digest": digest, "items": [dict(r) for r in items]}


async def render_digest_script(
    digest_id: int,
    mode: str = "final",
    force: bool = False,
    duration_target_sec: int = 300,
    prompt_version: str = "v1",  # kept for compatibility, we now use latest analysis
    script_prompt_version: str = DEFAULT_SCRIPT_PROMPT_VERSION,
) -> Dict[str, Any]:
    """
    Render digest script using LLM service.

    mode:
      - draft: produce draft script JSON segments
      - final: produce draft then refine into final (or reuse existing final unless force=True)
    """
    mode = (mode or "final").lower()
    if mode not in ("draft", "final"):
        raise ValueError("mode must be 'draft' or 'final'")

    with connect() as con:
        dig = con.execute("SELECT * FROM digests WHERE id = ?", (digest_id,)).fetchone()
        if not dig:
            raise ValueError("digest not found")
        dig = dict(dig)

        if mode == "final" and dig.get("script_final_json") and not force:
            return {"updated": False, "digest": dig}

        if mode == "draft" and dig.get("script_draft_json") and not force:
            return {"updated": False, "digest": dig}

        rows = con.execute(
            f"""
            {_latest_analysis_cte_sql()}
            SELECT di.rank, i.source_name, i.url, i.title, i.published_at, i.fetched_at,
                   a.interest_score, a.title_short, a.bulletin, a.summary, a.why, a.tags
            FROM digest_items di
            JOIN items i ON i.id = di.item_id
            LEFT JOIN latest a ON a.item_id = i.id
            WHERE di.digest_id = ?
            ORDER BY di.rank ASC
            """,
            (digest_id,),
        ).fetchall()
        items = [dict(r) for r in rows]

        # Build payload for llm service
        payload_items = []
        for r in items:
            payload_items.append(
                {
                    "rank": int(r.get("rank") or 0),
                    "source_name": r.get("source_name") or "",
                    "url": r.get("url") or "",
                    "published_at": r.get("published_at") or r.get("fetched_at"),
                    "title_short": r.get("title_short") or (r.get("title") or "").strip(),
                    "bulletin": r.get("bulletin") or "",
                    "summary": r.get("summary") or "",
                    "why": r.get("why") or "",
                    "interest_score": int(r.get("interest_score") or 0),
                    "tags": r.get("tags") or "[]",
                }
            )

        req = {
            "digest_day": dig.get("day"),
            "digest_id": digest_id,
            "duration_target_sec": int(duration_target_sec),
            "script_prompt_version": script_prompt_version,
            "items": payload_items,
            "mode": mode,
            "draft_json": dig.get("script_draft_json") or "",
        }

    async with httpx.AsyncClient(timeout=float(getattr(settings, "llm_timeout", 240))) as client:
        r = await client.post(f"{LLM_SERVICE_URL}/digest_script", json=req)
        if r.status_code >= 400:
            raise RuntimeError(f"llm service error {r.status_code}: {r.text[:500]}")
        data = r.json()

    now = dt.datetime.now(dt.timezone.utc).isoformat()
    model = data.get("model") or ""
    pv = data.get("script_prompt_version") or script_prompt_version
    draft = data.get("draft_json") or ""
    final = data.get("final_json") or ""

    with connect() as con:
        if draft:
            con.execute(
                "UPDATE digests SET script_draft_json=?, script_model=?, script_prompt_version=?, rendered_at=? WHERE id=?",
                (draft, model, pv, now, digest_id),
            )
        if final:
            con.execute(
                "UPDATE digests SET script_final_json=?, script_model=?, script_prompt_version=?, rendered_at=? WHERE id=?",
                (final, model, pv, now, digest_id),
            )
        con.commit()
        dig = dict(con.execute("SELECT * FROM digests WHERE id = ?", (digest_id,)).fetchone())

    return {"updated": True, "digest": dig, "llm": data}
