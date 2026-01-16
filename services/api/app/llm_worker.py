from __future__ import annotations

import datetime as dt
import json
import os
import time
from typing import Any, Dict, Optional

import httpx

from .config import settings
from .db import connect
from .redis_client import get_redis
from .llm_queue import LLM_QUEUE_KEY


LLM_SERVICE_URL = settings.llm_service_url
PROMPT_VERSION = settings.llm_prompt_version
WORKER_LABEL = os.getenv("WORKER_LABEL", "llm-worker")
SLEEP_EMPTY = float(os.getenv("SLEEP_EMPTY", "0.5"))


def _insert_analysis(con, item: Dict[str, Any], analysis: Dict[str, Any]) -> None:
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    tags_json = json.dumps(analysis.get("tags") or [], ensure_ascii=False)
    raw_json = json.dumps(analysis.get("raw_json") or analysis, ensure_ascii=False)

    con.execute(
        """
        INSERT OR REPLACE INTO llm_analyses (
          item_id, model, prompt_version, created_at,
          is_dfo, is_business, is_dfo_business, interest_score,
          title_short, bulletin, summary, tags, why, raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            int(item["id"]),
            str(analysis.get("model") or settings.llm_model),
            str(analysis.get("prompt_version") or PROMPT_VERSION),
            now,
            int(analysis.get("is_dfo") or 0),
            int(analysis.get("is_business") or 0),
            int(analysis.get("is_dfo_business") or 0),
            int(analysis.get("interest_score") or 0),
            str(analysis.get("title_short") or item["title"])[:200],
            str(analysis.get("bulletin") or "")[:2000],
            str(analysis.get("summary") or "")[:5000],
            tags_json,
            str(analysis.get("why") or "")[:600],
            raw_json,
        ],
    )


def _get_item(con, item_id: int) -> Optional[Dict[str, Any]]:
    row = con.execute("SELECT * FROM items WHERE id = ?", [item_id]).fetchone()
    return dict(row) if row else None


def main() -> None:
    r = get_redis()
    print(f"[{WORKER_LABEL}] started; queue={LLM_QUEUE_KEY}; llm={LLM_SERVICE_URL}", flush=True)

    while True:
        raw = r.blpop(LLM_QUEUE_KEY, timeout=10)
        if not raw:
            time.sleep(SLEEP_EMPTY)
            continue

        try:
            payload = json.loads(raw[1])
            item_id = int(payload.get("item_id"))
        except Exception:
            continue

        with connect() as con:
            item = _get_item(con, item_id)
            if not item:
                continue

        # call llm service
        req = {
            "item_id": item["id"],
            "title": item["title"],
            "body": item["body"],
            "source_name": item.get("source_name", ""),
            "url": item.get("url", ""),
            "published_at": item.get("published_at"),
            "fetched_at": item.get("fetched_at"),
            "business_score": item.get("business_score"),
            "dfo_score": item.get("dfo_score"),
            "has_company": item.get("has_company"),
            "reasons": json.loads(item.get("reasons") or "{}") if isinstance(item.get("reasons"), str) else (item.get("reasons") or {}),
        }

        try:
            with httpx.Client(timeout=float(os.getenv("LLM_TIMEOUT", "240"))) as client:
                resp = client.post(f"{LLM_SERVICE_URL}/analyze", json=req)
                if resp.status_code >= 400:
                    print(f"[{WORKER_LABEL}] analyze failed item_id={item_id}: {resp.status_code} {resp.text[:300]}", flush=True)
                    continue
                analysis = resp.json()
        except Exception as e:
            print(f"[{WORKER_LABEL}] analyze error item_id={item_id}: {e}", flush=True)
            continue

        try:
            with connect() as con:
                _insert_analysis(con, item, analysis)
                con.commit()
            print(f"[{WORKER_LABEL}] OK item_id={item_id} is_dfo_business={analysis.get('is_dfo_business')} interest={analysis.get('interest_score')}", flush=True)
        except Exception as e:
            print(f"[{WORKER_LABEL}] DB error item_id={item_id}: {e}", flush=True)


if __name__ == "__main__":
    main()
