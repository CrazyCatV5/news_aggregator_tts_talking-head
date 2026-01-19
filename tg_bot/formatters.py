from __future__ import annotations

from typing import Any, Dict, List

TG_MSG_LIMIT = 4096


def split_message(text: str, limit: int = TG_MSG_LIMIT) -> List[str]:
    text = (text or "").strip()
    if not text:
        return [""]
    if len(text) <= limit:
        return [text]

    parts: List[str] = []
    buf: List[str] = []
    buf_len = 0
    for line in text.splitlines(True):
        if buf_len + len(line) > limit:
            if buf:
                parts.append("".join(buf).rstrip())
                buf, buf_len = [], 0
            while len(line) > limit:
                parts.append(line[:limit].rstrip())
                line = line[limit:]
        buf.append(line)
        buf_len += len(line)
    if buf:
        parts.append("".join(buf).rstrip())
    return [p for p in parts if p]


def _short_time(iso_ts: str | None) -> str:
    if not iso_ts:
        return ""
    try:
        if "T" in iso_ts:
            t = iso_ts.split("T", 1)[1]
            return t[:5]
    except Exception:
        return ""
    return ""


def format_digest_text(day: str, digest: Dict[str, Any], *, max_bullets: int = 10) -> str:
    items = (digest or {}).get("items") or []
    bullets: List[str] = []
    for it in items[:max_bullets]:
        b = (it.get("bulletin") or it.get("title_short") or it.get("title") or "").strip()
        if not b:
            continue
        if not b.startswith("-"):
            b = "- " + b
        bullets.append(b)

    header = f"Деловой дайджест ДФО за {day}"
    if not bullets:
        return header + "\n\n" + "Данных для дайджеста пока нет. Попробуй /days."

    footer = (
        "\n\n"
    )
    return header + "\n\n" + "\n".join(bullets) + footer


def format_news_list(day: str, digest: Dict[str, Any]) -> str:
    items = (digest or {}).get("items") or []
    header = f"Новости в дайджесте за {day}"
    if not items:
        return header + "\n\n" + "Нет новостей для отображения. Попробуй /days."

    lines: List[str] = [header, ""]
    for idx, it in enumerate(items, start=1):
        title = (it.get("title") or it.get("title_short") or "").strip()
        source = (it.get("source_name") or "").strip()
        ts = it.get("published_at") or it.get("fetched_at")
        t = _short_time(ts)
        url = (it.get("url") or "").strip()
        summary = (it.get("summary") or it.get("bulletin") or "").strip()

        lines.append(f"{idx}. {title}" if title else f"{idx}.")
        meta = " • ".join([p for p in [source, t] if p])
        if meta:
            lines.append(meta)
        if url:
            lines.append(url)
        if summary:
            s = summary.replace("\n", " ").strip()
            if len(s) > 400:
                s = s[:400].rstrip() + "…"
            lines.append(s)
        lines.append("")
    return "\n".join(lines).strip()


def format_days_list(items: List[Dict[str, Any]], *, limit: int = 14) -> str:
    lines = [f"Доступные дни (последние {limit})", ""]
    if not items:
        return "Доступных дней пока нет."
    for d in items[:limit]:
        day = d.get("day")
        status = d.get("status")
        cnt = d.get("items_count")
        extra = []
        if status:
            extra.append(str(status))
        if cnt is not None:
            extra.append(f"items={cnt}")
        tail = " — " + ", ".join(extra) if extra else ""
        lines.append(f"- {day}{tail}")
    lines.append("\nПодсказка: /today, /day YYYY-MM-DD, /news YYYY-MM-DD")
    return "\n".join(lines)
