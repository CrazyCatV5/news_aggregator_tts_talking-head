from __future__ import annotations

import datetime as dt
import json
import os
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import httpx

from .config import settings


def _safe_filename(name: str) -> str:
    # Basic hardening: keep only safe chars
    keep = []
    for ch in name:
        if ch.isalnum() or ch in ("-", "_", ".",):
            keep.append(ch)
        else:
            keep.append("_")
    return "".join(keep)


def tts_out_dir() -> Path:
    p = Path(settings.tts_out_dir)
    p.mkdir(parents=True, exist_ok=True)
    return p


async def synthesize_via_tts_service(
    *,
    text: str,
    language: str,
    voice_wav: str,
    file_name: str,
) -> Tuple[Path, Dict[str, Any]]:
    """Call external TTS service and write output WAV into shared /data.

    Assumes TTS service supports POST /synthesize with JSON:
      {text, language, voice_wav, file_path}
    """
    out_dir = tts_out_dir()
    file_name = _safe_filename(file_name)
    out_path = out_dir / file_name

    payload = {
        "text": text,
        "language": language,
        "voice_wav": voice_wav,
        "file_path": str(out_path),
    }

    timeout = httpx.Timeout(300.0, connect=30.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await client.post(settings.tts_service_url.rstrip("/") + "/synthesize", json=payload)
        data = {}
        try:
            data = r.json()
        except Exception:
            data = {"raw": (r.text or "")[:2000]}
        if r.status_code >= 400:
            raise RuntimeError(data.get("detail") or f"TTS service error {r.status_code}")
    return out_path, data


def upsert_tts_output(
    *,
    conn,
    digest_id: int,
    day: str,
    language: str,
    voice_wav: str,
    file_name: str,
    file_path: str,
    meta: Dict[str, Any],
) -> int:
    now = dt.datetime.utcnow().isoformat() + "Z"
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO tts_outputs (digest_id, day, language, voice_wav, file_name, file_path, created_at, meta_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (digest_id, day, language, voice_wav, file_name, file_path, now, json.dumps(meta or {}, ensure_ascii=False)),
    )
    conn.commit()
    return int(cur.lastrowid)


def get_latest_tts_output(conn, *, day: str, language: str) -> Optional[Dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, digest_id, day, language, voice_wav, file_name, file_path, created_at, meta_json
        FROM tts_outputs
        WHERE day = ? AND language = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (day, language),
    )
    row = cur.fetchone()
    if not row:
        return None
    keys = ["id","digest_id","day","language","voice_wav","file_name","file_path","created_at","meta_json"]
    d = dict(zip(keys, row))
    try:
        d["meta"] = json.loads(d.get("meta_json") or "{}")
    except Exception:
        d["meta"] = {}
    return d
