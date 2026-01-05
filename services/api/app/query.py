import datetime as dt
from typing import Dict, List, Any
from .db import connect

def list_news(window_hours: int = 24, min_business: int = 2, min_dfo: int = 2, require_company: bool = False, limit: int = 50):
    now = dt.datetime.now(dt.timezone.utc)
    cutoff = now - dt.timedelta(hours=window_hours)

    with connect() as con:
        q = """
        SELECT id, source_name, url, title, body, published_at, fetched_at,
               business_score, dfo_score, has_company, reasons
        FROM items
        WHERE (published_at IS NULL OR published_at >= ?)
          AND business_score >= ?
          AND dfo_score >= ?
        """
        params = [cutoff.isoformat(), min_business, min_dfo]
        if require_company:
            q += " AND has_company = 1"
        q += " ORDER BY COALESCE(published_at, fetched_at) DESC LIMIT ?"
        params.append(limit)
        rows = con.execute(q, params).fetchall()
        return [dict(r) for r in rows]

def build_digest(items: List[Dict[str, Any]]) -> str:
    if not items:
        return "За последние 24 часа не найдено достаточно релевантных бизнес-новостей Дальнего Востока по выбранным источникам."

    infra, invest, corp, other = [], [], [], []

    def bucket(text: str):
        tl = (text or "").lower()
        if any(k in tl for k in ["трасс", "дорог", "порт", "терминал", "логист", "жд", "аэропорт", "мост", "концесс"]):
            return "infra"
        if any(k in tl for k in ["инвест", "резидент", "тор", "спв", "проект", "строительств", "завод", "производств"]):
            return "invest"
        if any(k in tl for k in ["банкрот", "акци", "доля", "сделк", "выручк", "прибыл", "кредит", "банк"]):
            return "corp"
        return "other"

    for it in items[:30]:
        b = bucket((it.get("title","") or "") + " " + (it.get("body","") or ""))
        if b == "infra": infra.append(it)
        elif b == "invest": invest.append(it)
        elif b == "corp": corp.append(it)
        else: other.append(it)

    def line(it):
        src = it.get("source_name","")
        title = (it.get("title","") or "").strip()
        url = it.get("url","") or ""
        return f"- {title} ({src}). {url}"

    parts = []
    parts.append("Добрый день. В эфире краткий деловой дайджест по Дальнему Востоку за последние сутки.")
    parts.append("")

    if invest:
        parts.append("В инвестиционной повестке выделяются следующие сообщения:")
        for it in invest[:6]:
            parts.append(line(it))
        parts.append("")

    if infra:
        parts.append("По инфраструктуре и логистике — события, которые могут влиять на издержки бизнеса:")
        for it in infra[:6]:
            parts.append(line(it))
        parts.append("")

    if corp:
        parts.append("Корпоративные и финансовые сюжеты:")
        for it in corp[:6]:
            parts.append(line(it))
        parts.append("")

    if other:
        parts.append("Прочие заметные новости:")
        for it in other[:6]:
            parts.append(line(it))
        parts.append("")

    parts.append("Это были ключевые сообщения. При необходимости подготовлю расширенный выпуск: краткие пересказы, выделение компаний и проектов, и финальную редактуру под «голос ведущего».")
    return "\n".join(parts)
