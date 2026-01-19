from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from fastapi.logger import logger

app = FastAPI(title="SadTalker Talking Head Service", version="1.0.0")

DATA_ROOT = Path("/data")
SADTALKER_ROOT = Path("/workspace/SadTalker")


class AnimateRequest(BaseModel):
    session_id: str = Field(..., min_length=1, max_length=200)
    audio_rel_path: str = Field(..., min_length=1)
    image_rel_path: str = Field(..., min_length=1)


class AnimateResponse(BaseModel):
    video_rel_path: str


def _safe_rel_path(rel: str) -> Path:
    """Resolve a user-provided relative path under /data with basic traversal protection."""
    if not rel or rel.strip() == "":
        raise HTTPException(status_code=400, detail="empty path")
    rel = rel.lstrip("/").replace("\\", "/")
    if ".." in rel.split("/"):
        raise HTTPException(status_code=400, detail="path traversal detected")
    p = (DATA_ROOT / rel).resolve()
    data_root = DATA_ROOT.resolve()
    if not str(p).startswith(str(data_root) + os.sep) and p != data_root:
        raise HTTPException(status_code=400, detail="invalid path")
    return p


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/animate", response_model=AnimateResponse)
def animate(req: AnimateRequest):
    audio_path = _safe_rel_path(req.audio_rel_path)
    image_path = _safe_rel_path(req.image_rel_path)

    if not audio_path.exists():
        raise HTTPException(status_code=404, detail=f"audio not found: {req.audio_rel_path}")
    if not image_path.exists():
        raise HTTPException(status_code=404, detail=f"image not found: {req.image_rel_path}")

    # Output directory
    out_dir = (DATA_ROOT / "video" / req.session_id)
    out_dir.mkdir(parents=True, exist_ok=True)

    # We will create a deterministic output filename.
    out_mp4 = out_dir / f"{req.session_id}.mp4"

    # SadTalker inference entrypoint.
    inference_py = SADTALKER_ROOT / "inference.py"
    if not inference_py.exists():
        raise HTTPException(status_code=500, detail="SadTalker not found: /workspace/SadTalker/inference.py")

    # Execute SadTalker.
    # NOTE: We keep args conservative; users can extend later.
    cmd = [
        "python",
        str(inference_py),
        "--driven_audio",
        str(audio_path),
        "--source_image",
        str(image_path),
        "--result_dir",
        str(out_dir),
        "--still",
        "--preprocess",
        "full",
        "--enhancer",
        "gfpgan",
    ]

    t0 = time.time()
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(SADTALKER_ROOT),
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"failed to run SadTalker: {e}")

    dt_s = round(time.time() - t0, 3)

    if proc.returncode != 0:
        stderr = (proc.stderr or "").strip()
        stdout = (proc.stdout or "").strip()
        msg = (stderr[-8000:] if stderr else stdout[-8000:]) or "SadTalker failed"
        logger.error("SadTalker failed rc=%s stdout_tail=%s stderr_tail=%s",
                     proc.returncode, (proc.stdout or "")[-2000:], (proc.stderr or "")[-2000:])

        raise HTTPException(
            status_code=502,
            detail=f"SadTalker inference failed (rc={proc.returncode}, {dt_s}s): {msg}",
        )

    # SadTalker writes result files into result_dir; we normalize to our expected mp4.
    # If SadTalker produced a single mp4 with another name, pick the newest.
    if not out_mp4.exists():
        mp4s = sorted(out_dir.glob("*.mp4"), key=lambda p: p.stat().st_mtime, reverse=True)
        if mp4s:
            try:
                mp4s[0].replace(out_mp4)
            except Exception:
                out_mp4 = mp4s[0]
        else:
            raise HTTPException(status_code=502, detail="SadTalker finished but no mp4 was produced")

    video_rel = out_mp4.relative_to(DATA_ROOT).as_posix()
    return AnimateResponse(video_rel_path=video_rel)
