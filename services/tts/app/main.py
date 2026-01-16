from __future__ import annotations

import os
import threading
import logging
from pathlib import Path

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field


logger = logging.getLogger("tts")


app = FastAPI(title="TTS Service", version="0.1.0")


class SynthesizeIn(BaseModel):
    text: str = Field(..., min_length=1)
    language: str = Field(default="ru")
    voice_wav: str = Field(default="")
    file_path: str = Field(..., min_length=1)


_LOCK = threading.Lock()
_TTS = None
_DEVICE = None
_MODEL_NAME = os.getenv("TTS_MODEL_NAME", "tts_models/multilingual/multi-dataset/xtts_v2")


def _patch_coqui_tos_prompt() -> None:
    """Avoid interactive ToS prompt inside non-interactive containers.

    Coqui TTS ModelManager may call input() to request agreement.
    In Docker without a TTY this raises EOFError and crashes startup.

    We allow non-interactive acceptance by setting COQUI_TOS_ACCEPTED=1.
    """
    try:
        from TTS.utils.manage import ModelManager  # noqa

        orig = ModelManager.ask_tos

        def ask_tos(self, output_path: str):  # type: ignore[no-redef]
            if os.getenv("COQUI_TOS_ACCEPTED", "").lower() in {"1", "true", "yes", "y"}:
                return True
            return orig(self, output_path)

        ModelManager.ask_tos = ask_tos  # type: ignore[assignment]
    except Exception as e:
        logger.warning("failed to patch Coqui ToS prompt: %s", e)


def _load_model():
    global _TTS, _DEVICE
    if _TTS is not None:
        return

    _patch_coqui_tos_prompt()

    # Heavy imports only when needed
    import torch
    from torch.serialization import add_safe_globals
    from TTS.tts.configs.xtts_config import XttsConfig
    from TTS.tts.models.xtts import XttsAudioConfig, XttsArgs
    from TTS.config.shared_configs import BaseDatasetConfig
    from TTS.api import TTS

    add_safe_globals([XttsConfig, XttsAudioConfig, BaseDatasetConfig, XttsArgs])

    _DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
    logger.info("Loading XTTS model=%s device=%s", _MODEL_NAME, _DEVICE)
    _TTS = TTS(_MODEL_NAME).to(_DEVICE)


@app.get("/health")
def health():
    return {"ok": True, "model": _MODEL_NAME, "loaded": _TTS is not None}


@app.post("/synthesize")
def synthesize(inp: SynthesizeIn):
    if not inp.text.strip():
        raise HTTPException(status_code=400, detail="empty text")

    out_path = Path(inp.file_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with _LOCK:
        if _TTS is None:
            try:
                _load_model()
            except EOFError:
                # Explicit message for the common ToS prompt issue
                raise HTTPException(
                    status_code=412,
                    detail="Coqui ToS acceptance required. Set COQUI_TOS_ACCEPTED=1 to run non-interactively.",
                )
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"failed to load model: {e}")

    # Run TTS outside lock (model is thread-safe enough for sequential calls; lock is only for load)
    try:
        _TTS.tts_to_file(
            text=inp.text,
            speaker_wav=inp.voice_wav or None,
            language=inp.language,
            file_path=str(out_path),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"tts failed: {e}")

    return {"ok": True, "file_path": str(out_path)}
