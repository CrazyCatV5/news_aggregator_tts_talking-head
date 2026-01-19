from __future__ import annotations

import os
import json
import datetime as dt
from pathlib import Path
from typing import Optional, Dict, Any

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse

from .db import connect
from .daily_digests import get_digest_by_day
from .tts_api import tts_daily_render

router = APIRouter(prefix="/video", tags=["video"])


def _utc_now_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _video_out_dir() -> Path:
    return Path(os.getenv("VIDEO_OUT_DIR", "/data/video"))


def _default_image_abs() -> Path:
    return Path(os.getenv("VIDEO_DEFAULT_IMAGE", "/data/images/talking_head/default.png"))


def _sadtalker_url() -> str:
    return os.getenv("SADTALKER_SERVICE_URL", "http://sadtalker:8102").rstrip("/")


def _tts_out_dir() -> Path:
    return Path(os.getenv("TTS_OUT_DIR", "/data/tts"))


def _safe_file_name(name: str) -> str:
    if not name or name.strip() == "":
        raise HTTPException(status_code=400, detail="invalid file name")
    if "/" in name or "\\" in name or ".." in name:
        raise HTTPException(status_code=400, detail="invalid file name")
    return name


def _safe_rel_under_data(rel: str) -> str:
    rel = (rel or "").strip().lstrip("/").replace("\\", "/")
    if not rel:
        raise HTTPException(status_code=400, detail="empty path")
    if ".." in rel.split("/"):
        raise HTTPException(status_code=400, detail="path traversal detected")
    return rel


def _mp4_path_from_db_path(video_path: str) -> Path:
    # video_path in DB is absolute inside container (preferred) or relative under /data.
    p = Path(video_path)
    if p.is_absolute():
        return p
    return (Path("/data") / _safe_rel_under_data(str(p))).resolve()


@router.get("/daily/{day}")
def video_daily_status(day: str, language: str = Query("ru")):
    # 1) Prefer DB (authoritative metadata), but only if file exists
    with connect() as con:
        rows = con.execute(
            """
            SELECT * FROM video_outputs
            WHERE day = ? AND language = ?
            ORDER BY id DESC
            LIMIT 10
            """,
            (day, language),
        ).fetchall()

    for row in rows or []:
        try:
            video_abs = _mp4_path_from_db_path(row["video_path"])
        except Exception:
            continue

        if video_abs.exists():
            r = dict(row)
            file_name = row["video_file_name"]
            file_name = r.get("video_file_name") or row["video_file_name"]
            return {
                "ok": True,
                "exists": True,
                "day": day,
                "language": language,
                "file_name": file_name,
                "download_url": f"/video/files/{file_name}",
                "created_at": r.get("created_at"),
                "image_path": r.get("image_path"),
                "audio_file_name": r.get("audio_file_name"),
                "video_path": r.get("video_path"),
            }

    # 2) Fallback to filesystem discovery (for legacy/manual files without DB rows)
    # day: YYYY-MM-DD -> YYYYMMDD
    day_compact = day.replace("-", "")
    stem = f"video_daily_{day_compact}_{language}"
    root = _video_out_dir().resolve()

    # a) canonical file directly under /data/video
    cand1 = (root / f"{stem}.mp4").resolve()
    if cand1.exists():
        file_name = cand1.name
        return {
            "ok": True,
            "exists": True,
            "day": day,
            "language": language,
            "file_name": file_name,
            "download_url": f"/video/files/{file_name}",
            "created_at": _utc_now_iso(),
            "video_path": str(cand1),
        }

    # b) canonical file inside /data/video/{stem}/
    cand2 = (root / stem / f"{stem}.mp4").resolve()
    if cand2.exists():
        file_name = cand2.name
        return {
            "ok": True,
            "exists": True,
            "day": day,
            "language": language,
            "file_name": file_name,
            "download_url": f"/video/files/{file_name}",
            "created_at": _utc_now_iso(),
            "video_path": str(cand2),
        }

    # c) newest mp4 inside /data/video/{stem}/
    d = (root / stem).resolve()
    if d.exists() and d.is_dir():
        mp4s = sorted(d.glob("*.mp4"), key=lambda p: p.stat().st_mtime, reverse=True)
        if mp4s:
            p = mp4s[0].resolve()
            file_name = p.name
            return {
                "ok": True,
                "exists": True,
                "day": day,
                "language": language,
                "file_name": file_name,
                "download_url": f"/video/files/{file_name}",
                "created_at": _utc_now_iso(),
                "video_path": str(p),
            }

    return {"ok": True, "exists": False}



@router.post("/daily/{day}/render")
async def video_daily_render(
    day: str,
    language: str = Query("ru"),
    force_tts: bool = Query(False),
    image: Optional[str] = Query(None),
):
    digest = get_digest_by_day(day)
    if not digest:
        raise HTTPException(status_code=404, detail="daily digest not found")

    # Ensure default image exists (or selected).
    if image:
        # allow passing relative path under /data/images
        rel = _safe_rel_under_data(image)
        if not rel.startswith("images/"):
            rel = "images/" + rel
        image_rel = rel
        image_abs = (Path("/data") / image_rel).resolve()
    else:
        image_abs = _default_image_abs().resolve()
        try:
            image_rel = image_abs.relative_to(Path("/data")).as_posix()
        except Exception:
            # fallback - still acceptable for SadTalker, but contract expects rel.
            image_rel = "images/talking_head/default.png"

    if not image_abs.exists():
        raise HTTPException(
            status_code=400,
            detail=(
                f"default image not found: {image_abs}. "
                "Place a PNG at ./data/images/talking_head/default.png on the host."
            ),
        )

    # Check existing WAV.
    with connect() as con:
        tts_row = con.execute(
            """
            SELECT * FROM tts_outputs
            WHERE day = ? AND language = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (day, language),
        ).fetchone()

    if not tts_row or force_tts:
        # Generate WAV through existing TTS endpoint.
        if force_tts:
            # in render we keep voice_wav None and force_script False; users can re-render script separately.
            await tts_daily_render(day=day, language=language, voice_wav=None, force_script=False)
        else:
            raise HTTPException(status_code=400, detail="no audio (WAV) for this day; render TTS first or pass force_tts=true")

        with connect() as con:
            tts_row = con.execute(
                """
                SELECT * FROM tts_outputs
                WHERE day = ? AND language = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (day, language),
            ).fetchone()

    if not tts_row:
        raise HTTPException(status_code=400, detail="no audio (WAV) for this day")

    wav_file = tts_row["file_name"]
    wav_abs = _tts_out_dir() / wav_file
    if not wav_abs.exists():
        raise HTTPException(status_code=500, detail=f"tts row exists but file is missing on disk: {wav_abs}")

    session_id = f"video_daily_{day.replace('-', '')}_{language}"
    audio_rel_path = f"tts/{wav_file}"

    # Call SadTalker service.
    payload: Dict[str, Any] = {
        "session_id": session_id,
        "audio_rel_path": audio_rel_path,
        "image_rel_path": image_rel,
    }

    url = _sadtalker_url() + "/animate"
    try:
        async with httpx.AsyncClient(timeout=3600.0) as client:
            resp = await client.post(url, json=payload)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"sadtalker service unavailable: {e}")

    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail=f"sadtalker service error {resp.status_code}: {resp.text[:2000]}")

    data = resp.json() if resp.content else {}
    video_rel_path = data.get("video_rel_path")
    if not video_rel_path or not isinstance(video_rel_path, str):
        raise HTTPException(status_code=502, detail="sadtalker returned no video_rel_path")

    video_rel_path = _safe_rel_under_data(video_rel_path)
    video_abs = (Path("/data") / video_rel_path).resolve()
    if not video_abs.exists():
        raise HTTPException(status_code=502, detail=f"sadtalker returned path but file not found: {video_abs}")

    video_file_name = video_abs.name
    _safe_file_name(video_file_name)

    created_at = _utc_now_iso()

    with connect() as con:
        con.execute(
            """
            INSERT INTO video_outputs (
              digest_id, day, language,
              image_path,
              audio_file_name,
              video_file_name,
              video_path,
              created_at,
              meta_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                digest["id"],
                day,
                language,
                str(image_abs),
                wav_file,
                video_file_name,
                str(video_abs),
                created_at,
                json.dumps({"sadtalker_url": url, "video_rel_path": video_rel_path}),
            ),
        )
        con.commit()

    return {
        "ok": True,
        "day": day,
        "language": language,
        "file_name": video_file_name,
        "download_url": f"/video/files/{video_file_name}",
        "image_path": str(image_abs),
        "audio_file_name": wav_file,
        "video_path": str(video_abs),
        "created_at": created_at,
    }


@router.get("/files/{file_name}")
def video_file_download(file_name: str):
    file_name = _safe_file_name(file_name)

    root = _video_out_dir().resolve()
    if not root.exists():
        raise HTTPException(status_code=404, detail="video directory not found")

    # 1. Прямой поиск по имени
    matches = list(root.rglob(file_name))
    if matches:
        path = sorted(matches, key=lambda p: p.stat().st_mtime, reverse=True)[0].resolve()
        if not str(path).startswith(str(root) + os.sep):
            raise HTTPException(status_code=400, detail="invalid path")
        return FileResponse(str(path), media_type="video/mp4", filename=path.name)

    # 2. Fallback: ищем newest mp4 в папке с именем stem
    stem = file_name[:-4] if file_name.lower().endswith(".mp4") else file_name
    candidate_dir = (root / stem).resolve()

    if candidate_dir.exists() and candidate_dir.is_dir():
        mp4s = sorted(candidate_dir.glob("*.mp4"), key=lambda p: p.stat().st_mtime, reverse=True)
        if mp4s:
            path = mp4s[0].resolve()
            if not str(path).startswith(str(root) + os.sep):
                raise HTTPException(status_code=400, detail="invalid path")
            return FileResponse(str(path), media_type="video/mp4", filename=path.name)

    raise HTTPException(status_code=404, detail="file not found")

