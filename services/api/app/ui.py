from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

router = APIRouter()

BASE_DIR = Path(__file__).resolve().parent
INDEX_PATH = BASE_DIR / "static" / "index.html"


@router.get("/ui", response_class=HTMLResponse)
def ui() -> HTMLResponse:
    """Simple dashboard UI.

    Served as a real HTML file with separate CSS/JS in /static.
    """
    return HTMLResponse(INDEX_PATH.read_text(encoding="utf-8"))
