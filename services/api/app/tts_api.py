from __future__ import annotations

import os
import json
import datetime as dt
from pathlib import Path
from typing import Optional, Dict, Any

import httpx
from fastapi import APIRouter, HTTPException, Query

from .config import settings
from .db import connect
from .daily_digests import get_digest_by_day, generate_digest_script


router = APIRouter(prefix="/tts", tags=["tts"])


def _utc_now_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _plain_text_from_script_json(script_json: str) -> str:
    """Best-effort conversion from daily_digests.script_json to plain TTS text.

    Expected format: a JSON list of segments (dicts) produced by generate_digest_script.
    We concatenate fields commonly used in the project: 'text', 'title', 'bulletin'.
    """
    try:
        data = json.loads(script_json or "[]")
    except Exception:
        return (script_json or "").strip()

    if not isinstance(data, list):
        return (script_json or "").strip()

    parts = []
    for seg in data:
        if isinstance(seg, str):
            s = seg.strip()
            if s:
                parts.append(s)
            continue
        if not isinstance(seg, dict):
            continue
        for key in ("text", "title", "bulletin", "summary"):
            val = seg.get(key)
            if isinstance(val, str) and val.strip():
                parts.append(val.strip())
                break
    return "\n\n".join(parts).strip()


def _tts_service_url() -> str:
    return os.getenv("TTS_SERVICE_URL", "http://tts:8101").rstrip("/")


def _tts_out_dir() -> Path:
    return Path(os.getenv("TTS_OUT_DIR", "/data/tts"))


def _default_ref_wav() -> str:
    return os.getenv("TTS_REF_WAV", "/data/voices/ref_clean_g.wav")


@router.get("/daily/{day}")
def tts_daily_status(day: str, language: str = Query("ru")):
    """Return latest TTS artifact for a given digest day (if any)."""
    with connect() as con:
        row = con.execute(
            """
            SELECT * FROM tts_outputs
            WHERE day = ? AND language = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (day, language),
        ).fetchone()
        if not row:
            return {"ok": True, "exists": False}
        return {
            "ok": True,
            "exists": True,
            "day": day,
            "language": language,
            "file_name": row["file_name"],
            "download_url": f"/tts/files/{row['file_name']}",
            "created_at": row["created_at"],
            "voice_wav": row["voice_wav"],
        }


@router.post("/daily/{day}/render")
async def tts_daily_render(
    day: str,
    language: str = Query("ru"),
    voice_wav: Optional[str] = Query(None),
    force_script: bool = Query(False),
):
    """Generate TTS audio for the given daily digest day.

    - Ensures digest exists.
    - Ensures script_json exists (generates if missing or force_script).
    - Calls the external TTS service.
    - Persists a row in tts_outputs.
    """
    digest = get_digest_by_day(day)
    if not digest:
        raise HTTPException(status_code=404, detail="daily digest not found")

    # Ensure script
    script_json = digest.get("script_json")
    if force_script or not script_json:
        script = await generate_digest_script(day=day, force=force_script)
        script_json = script.get("script_json") if isinstance(script, dict) else None
        if not script_json:
            raise HTTPException(status_code=400, detail="failed to generate script")

    text = _plain_text_from_script_json(script_json)
    if not text:
        raise HTTPException(status_code=400, detail="empty script")

    out_dir = _tts_out_dir()
    out_dir.mkdir(parents=True, exist_ok=True)
    ref = voice_wav or _default_ref_wav()
    file_name = f"daily_{day.replace('-', '')}_{Path(ref).stem}_{language}.wav"
    file_path = str(out_dir / file_name)

    url = _tts_service_url() + "/synthesize"
    payload: Dict[str, Any] = {
        "text": text,
        "language": language,
        "voice_wav": ref,
        "file_path": file_path,
    }
    try:
        async with httpx.AsyncClient(timeout=600.0) as client:
            resp = await client.post(url, json=payload)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"tts service unavailable: {e}")

    if resp.status_code != 200:
        body = resp.text
        raise HTTPException(status_code=502, detail=f"tts service error {resp.status_code}: {body[:500]}")

    created_at = _utc_now_iso()
    with connect() as con:
        con.execute(
            """
            INSERT INTO tts_outputs (digest_id, day, language, voice_wav, file_name, file_path, created_at, meta_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                digest["id"],
                day,
                language,
                ref,
                file_name,
                file_path,
                created_at,
                json.dumps({"tts_service": url}),
            ),
        )
        con.commit()

    return {
        "ok": True,
        "day": day,
        "language": language,
        "voice_wav": ref,
        "file_name": file_name,
        "download_url": f"/tts/files/{file_name}",
        "created_at": created_at,
    }


@router.get("/files/{file_name}")
def tts_file_download(file_name: str):
    # basic traversal protection
    if "/" in file_name or ".." in file_name or "\\" in file_name:
        raise HTTPException(status_code=400, detail="invalid file name")
    path = _tts_out_dir() / file_name
    if not path.exists():
        raise HTTPException(status_code=404, detail="file not found")
    # Return as static file (FastAPI will stream it)
    from fastapi.responses import FileResponse

    return FileResponse(str(path), media_type="audio/wav", filename=file_name)
