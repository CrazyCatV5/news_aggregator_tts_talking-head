from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://ollama:11434")
MODEL = os.getenv("LLM_MODEL", "qwen2.5:14b-instruct-q4_K_M")
REQUEST_TIMEOUT = float(os.getenv("LLM_TIMEOUT", "180"))
MAX_CHARS = int(os.getenv("LLM_MAX_CHARS", "60000"))  # hard safety cap


app = FastAPI(title="DFO News LLM Service", version="1.0.0")


class AnalyzeIn(BaseModel):
    item_id: int
    title: str
    body: str
    source_name: str = ""
    url: str = ""
    published_at: Optional[str] = None
    fetched_at: Optional[str] = None

    # heuristic signals from ingest (optional, but helpful)
    business_score: Optional[int] = None
    dfo_score: Optional[int] = None
    has_company: Optional[int] = None
    reasons: Optional[Dict[str, Any]] = None


class AnalyzeOut(BaseModel):
    item_id: int
    model: str
    prompt_version: str

    # binary labels (0/1)
    is_dfo_business: int = Field(ge=0, le=1)
    is_business: int = Field(ge=0, le=1)
    is_dfo: int = Field(ge=0, le=1)

    # 0..10
    interest_score: int = Field(ge=0, le=10)

    # short outputs used in UI
    title_short: str
    bulletin: str
    summary: str

    tags: List[str] = Field(default_factory=list)
    why: str = ""  # 1-2 short sentences justification
    raw_json: Dict[str, Any] = Field(default_factory=dict)


class DigestScriptItem(BaseModel):
    rank: int
    item_id: int
    title_short: str
    bulletin: str
    summary: str = ""
    why: str = ""
    source_name: str = ""
    url: str = ""


class DigestScriptIn(BaseModel):
    day: str  # YYYY-MM-DD
    items: List[DigestScriptItem]
    tone: str = "деловой"


class DigestScriptOut(BaseModel):
    day: str
    model: str
    segments: List[Dict[str, Any]]
    raw_json: Dict[str, Any] = Field(default_factory=dict)
    style: str = "business"  # reserved


class DigestScriptOut(BaseModel):
    model: str
    day: str
    segments: List[Dict[str, Any]]


PROMPT_VERSION = "v1.0"


def _strip(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def _truncate(text: str, max_chars: int) -> str:
    if not text:
        return ""
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + "\n\n[TRUNCATED]"


def _chunk_text(text: str, chunk_chars: int = 12000, overlap: int = 1200) -> List[str]:
    text = text or ""
    if len(text) <= chunk_chars:
        return [text]
    out = []
    i = 0
    while i < len(text):
        out.append(text[i : i + chunk_chars])
        i += max(1, chunk_chars - overlap)
        if len(out) > 20:  # avoid pathological cases
            break
    return out


async def _ollama_generate(prompt: str, model: str = MODEL) -> str:
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": 0.2,
            "top_p": 0.9,
            "num_ctx": int(os.getenv("LLM_NUM_CTX", "32768")),
        },
    }
    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        r = await client.post(f"{OLLAMA_URL}/api/generate", json=payload)
        if r.status_code >= 400:
            raise HTTPException(status_code=502, detail=f"Ollama error: {r.status_code} {r.text[:500]}")
        data = r.json()
        return data.get("response", "")


def _extract_json(text: str) -> Dict[str, Any]:
    # robust extraction: find first {...} block
    text = text.strip()
    if not text:
        return {}
    # remove markdown fences
    text = re.sub(r"^```(json)?\s*", "", text)
    text = re.sub(r"```\s*$", "", text)
    m = re.search(r"\{.*\}", text, flags=re.S)
    if not m:
        return {}
    blob = m.group(0)
    try:
        return json.loads(blob)
    except Exception:
        # try to fix common issues (trailing commas)
        blob2 = re.sub(r",\s*([}\]])", r"\1", blob)
        try:
            return json.loads(blob2)
        except Exception:
            return {}


async def _summarize_long(title: str, body: str) -> Tuple[str, str]:
    """Return (summary, bulletin) for long body via chunking."""
    body = _truncate(body, MAX_CHARS)
    chunks = _chunk_text(body)
    if len(chunks) == 1:
        return "", ""
    partials = []
    for i, ch in enumerate(chunks, 1):
        p = f"""Ты редактор делового дайджеста. Сожми фрагмент статьи в 2–3 предложения, строго по фактам.
Заголовок: {title}
Фрагмент {i}/{len(chunks)}:
{ch}
Ответ: """
        resp = await _ollama_generate(p)
        partials.append(_strip(resp))
    merged = "\n".join(f"- {p}" for p in partials if p)
    p2 = f"""У тебя есть конспект по фрагментам статьи. Собери единое краткое резюме.
Требования:
- summary: 3–5 предложений, деловой стиль, без воды
- bulletin: 2 предложения как для ведущего выпуска новостей
Верни JSON: {{"summary": "...", "bulletin": "..."}}
Заголовок: {title}
Конспект:
{merged}
"""
    resp2 = await _ollama_generate(p2)
    j = _extract_json(resp2)
    return _strip(j.get("summary","")), _strip(j.get("bulletin",""))


@app.get("/health")
async def health():
    return {"ok": True, "ollama_url": OLLAMA_URL, "model": MODEL, "prompt_version": PROMPT_VERSION}


@app.post("/analyze", response_model=AnalyzeOut)
async def analyze(inp: AnalyzeIn) -> AnalyzeOut:
    title = _strip(inp.title)
    body = _strip(inp.body)
    if not title or not body:
        raise HTTPException(status_code=400, detail="title and body are required")

    body = _truncate(body, MAX_CHARS)

    # If extremely long, pre-summarize to keep the main classification prompt stable.
    pre_summary, pre_bulletin = await _summarize_long(title, body)

    heur = {
        "business_score": inp.business_score,
        "dfo_score": inp.dfo_score,
        "has_company": inp.has_company,
        "reasons": inp.reasons or {},
    }

    prompt = f"""Ты редактор и аналитик деловых новостей по Дальнему Востоку РФ.
Задача: по статье определить, относится ли она к бизнесу на Дальнем Востоке, оценить интересность и подготовить краткую подводку для выпуска новостей.

Входные данные:
- Заголовок: {title}
- Источник: {inp.source_name}
- Дата публикации: {inp.published_at or inp.fetched_at or ""}
- URL: {inp.url}
- Эвристические сигналы (для справки, могут ошибаться): {json.dumps(heur, ensure_ascii=False)}

Текст статьи:
{body}

Если текст длинный, можешь опираться на предварительное резюме:
summary_hint: {pre_summary}
bulletin_hint: {pre_bulletin}

Сформируй СТРОГО JSON без markdown и без лишних полей, по схеме:
{{
  "is_dfo": 0|1,
  "is_business": 0|1,
  "is_dfo_business": 0|1,
  "interest_score": 0..10,
  "title_short": "короткий заголовок (до 90 символов)",
  "summary": "3–5 предложений, деловой стиль, только факты",
  "bulletin": "1–2 предложения как для ведущего выпуска новостей",
  "tags": ["инфраструктура|логистика|инвестиции|промышленность|энергетика|финансы|ритейл|IT|госрегулирование|экспорт|импорт|рынки|другое", ...],
  "why": "краткое обоснование (1–2 предложения)"
}}

Правила:
- Если статья не про ДФО, is_dfo=0.
- Если статья не про бизнес/экономику, is_business=0.
- is_dfo_business=1 только если одновременно is_dfo=1 и is_business=1.
- Не придумывай цифры/факты/компании.
"""

    resp = await _ollama_generate(prompt)
    j = _extract_json(resp)
    if not j:
        raise HTTPException(status_code=502, detail=f"Failed to parse model JSON. Raw: {resp[:500]}")

    def _b01(x: Any) -> int:
        try:
            return 1 if int(x) == 1 else 0
        except Exception:
            return 0

    def _i010(x: Any) -> int:
        try:
            v = int(x)
            return 0 if v < 0 else 10 if v > 10 else v
        except Exception:
            return 0

    out = AnalyzeOut(
        item_id=inp.item_id,
        model=MODEL,
        prompt_version=PROMPT_VERSION,
        is_dfo=_b01(j.get("is_dfo")),
        is_business=_b01(j.get("is_business")),
        is_dfo_business=_b01(j.get("is_dfo_business")),
        interest_score=_i010(j.get("interest_score")),
        title_short=_strip(j.get("title_short")) or title[:90],
        summary=_strip(j.get("summary")) or pre_summary or "",
        bulletin=_strip(j.get("bulletin")) or pre_bulletin or "",
        tags=[_strip(t) for t in (j.get("tags") or []) if _strip(t)],
        why=_strip(j.get("why"))[:400],
        raw_json=j,
    )

    # basic sanity
    if not out.summary:
        out.summary = pre_summary or out.bulletin or title
    if not out.bulletin:
        out.bulletin = out.summary[:220]
    return out


@app.post("/digest_script", response_model=DigestScriptOut)
async def digest_script(inp: DigestScriptIn) -> DigestScriptOut:
    """Generate a ready-to-TTS script skeleton: intro -> 5 items with transitions -> outro.

    The API service stores the returned `segments` JSON as-is.
    """
    if not inp.items:
        raise HTTPException(status_code=400, detail="items are required")
    # Keep only first 5 by rank, but do not assume exactly 5.
    items = sorted(inp.items, key=lambda x: x.rank)[:5]
    pack = []
    for it in items:
        pack.append(
            {
                "rank": it.rank,
                "item_id": it.item_id,
                "title_short": _strip(it.title_short)[:120],
                "bulletin": _strip(it.bulletin)[:700],
                "summary": _strip(it.summary)[:1200],
                "why": _strip(it.why)[:500],
                "source_name": _strip(it.source_name)[:120],
                "url": _strip(it.url)[:600],
            }
        )

    prompt = f"""Ты редактор и сценарист ежедневного делового выпуска новостей по Дальнему Востоку РФ. Собери связный сценарий выпуска на дату {inp.day}. Дано 5 новостей (каждая уже отфильтрована и кратко описана). Нужно: 1) Приветствие + Вступление (intro) 1–2 предложения. 2) 5 блоков новостей (rank 1..5) — у каждого: - text: 2–4 предложения для ведущего (можно опираться на bulletin/summary) - transition: 1 короткое предложение-переход к следующей новости (для последней transition можно опустить) 3) Заключение (outro) 1–2 предложения + Прощание с аудиторией. Требования: - Стиль: {inp.tone}, без воды, без выдуманных фактов, цифр и компаний. - Нельзя повторять одно и то же разными словами. - Упоминай географию ДФО, когда она есть в материале. - В каждом блоке используй item_id и rank, чтобы сценарий был привязан к данным. - Числа и даты пиши полными словами с правильными падежами и окончаниями без цифр. - Названия на английском - транслитом, чтобы tts мог нормально воспринимать. Верни СТРОГО JSON без markdown и без лишних полей по схеме: {{ "segments": [ {{"type":"intro","text":"..."}}, {{"type":"item","rank":1,"item_id":123,"text":"...","transition":"..."}}, ..., {{"type":"outro","text":"..."}} ] }} Входные новости: {json.dumps(pack, ensure_ascii=False)} """

    resp = await _ollama_generate(prompt)
    j = _extract_json(resp)
    segs = j.get("segments") if isinstance(j, dict) else None
    if not isinstance(segs, list) or not segs:
        raise HTTPException(status_code=502, detail=f"Failed to parse digest script JSON. Raw: {resp[:500]}")

    # Light validation / normalization
    norm: List[Dict[str, Any]] = []
    for s in segs:
        if not isinstance(s, dict):
            continue
        t = _strip(str(s.get("type", "")))
        if t not in {"intro", "item", "outro"}:
            continue
        out_seg: Dict[str, Any] = {"type": t}
        if t == "item":
            try:
                out_seg["rank"] = int(s.get("rank"))
            except Exception:
                out_seg["rank"] = 0
            try:
                out_seg["item_id"] = int(s.get("item_id"))
            except Exception:
                out_seg["item_id"] = 0
            out_seg["text"] = _strip(str(s.get("text", "")))
            tr = s.get("transition")
            if tr is not None:
                out_seg["transition"] = _strip(str(tr))
        else:
            out_seg["text"] = _strip(str(s.get("text", "")))
        if out_seg.get("text"):
            norm.append(out_seg)

    if not norm:
        raise HTTPException(status_code=502, detail="digest_script: empty segments after normalization")

    return DigestScriptOut(day=inp.day, model=MODEL, segments=norm, raw_json=j)
