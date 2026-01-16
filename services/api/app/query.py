import datetime as dt
from typing import Dict, List, Any
from .db import connect

# ---------------------------------------------------------------------------
# Lightweight content filter: exclude war/combat reports.
# Opt-in via query params (exclude_war=true).
# NOTE: This is heuristic by design; it should be reviewed on real corpus.
# ---------------------------------------------------------------------------

WAR_TERMS = [
    # RU stems
    "всу",
    "сво",
    "украин",
    "обстрел",
    "удар",
    "дрон",
    "бпла",
    "fpv",
    "ракет",
    "пво",
    "фронт",
    "боев",
    "военн",
    "миномет",
    "артиллер",
    "снайпер",
    "пехот",
    "противник",
    "тыл",
    "диверс",
    "мобилиз",
    "оккупац",
]


def _exclude_war_where_sql() -> str:
    """Return SQL fragment to exclude war-like items based on title/body."""
    # SQLite LIKE is case-insensitive for ASCII; for Cyrillic reliability we
    # normalize with lower().
    # Use COALESCE to avoid NULL concatenation.
    hay = "lower(COALESCE(title,'') || ' ' || COALESCE(body,''))"
    parts = [f"{hay} NOT LIKE ?" for _ in WAR_TERMS]
    return " AND ".join(parts)


def _exclude_war_params() -> List[str]:
    return [f"%{t}%" for t in WAR_TERMS]

def list_news(
    window_hours: int = 24,
    min_business: int = 2,
    min_dfo: int = 2,
    require_company: bool = False,
    exclude_war: bool = False,
    limit: int = 50,
):
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
        if exclude_war:
            q += " AND " + _exclude_war_where_sql()
            params.extend(_exclude_war_params())
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


def list_news_by_day(
    days: int = 7,
    min_business: int = 2,
    min_dfo: int = 2,
    require_company: bool = False,
    exclude_war: bool = False,
    limit_per_day: int = 50,
):
    """Group news by day using COALESCE(published_at, fetched_at)."""
    import datetime as dt
    from collections import defaultdict

    now = dt.datetime.now(dt.timezone.utc)
    cutoff = now - dt.timedelta(days=days)

    where = ["COALESCE(published_at, fetched_at) >= ?",
             "business_score >= ?",
             "dfo_score >= ?"]
    params = [cutoff.isoformat(), int(min_business), int(min_dfo)]
    if require_company:
        where.append("has_company = 1")
    if exclude_war:
        where.append(_exclude_war_where_sql())
        params.extend(_exclude_war_params())
    where_sql = " AND ".join(where)

    sql = f"""
    SELECT id, source_name, url, title, published_at, fetched_at,
           business_score, dfo_score, has_company, reasons
    FROM items
    WHERE {where_sql}
    ORDER BY COALESCE(published_at, fetched_at) DESC
    LIMIT ?
    """

    params.append(int(days) * int(limit_per_day) * 5)

    grouped = defaultdict(list)

    with connect() as con:
        rows = con.execute(sql, params).fetchall()

    for r in rows:
        ts = r["published_at"] or r["fetched_at"]
        if not ts:
            continue
        day = str(ts)[:10]
        if len(grouped[day]) >= limit_per_day:
            continue
        grouped[day].append({
            "id": r["id"],
            "source_name": r["source_name"],
            "url": r["url"],
            "title": r["title"],
            "published_at": r["published_at"],
            "fetched_at": r["fetched_at"],
            "business_score": r["business_score"],
            "dfo_score": r["dfo_score"],
            "has_company": bool(r["has_company"]),
            "reasons": r["reasons"],
        })

    return dict(grouped)


def get_item(item_id: int) -> Dict[str, Any] | None:
    """Fetch a single item including full body text."""
    with connect() as con:
        row = con.execute(
            """
            SELECT id, source_name, url, title, body, published_at, fetched_at,
                   business_score, dfo_score, has_company, reasons
            FROM items
            WHERE id = ?
            """,
            (item_id,),
        ).fetchone()

    return dict(row) if row else None


def list_item_sources() -> List[str]:
    """Return distinct source names for UI filters."""
    with connect() as con:
        rows = con.execute(
            """
            SELECT DISTINCT source_name
            FROM items
            WHERE source_name IS NOT NULL AND TRIM(source_name) <> ''
            ORDER BY source_name ASC
            """
        ).fetchall()
        return [r[0] for r in rows]


def list_items(
    *,
    q: str | None = None,
    source: str | None = None,
    published_from: str | None = None,
    published_to: str | None = None,
    fetched_from: str | None = None,
    fetched_to: str | None = None,
    biz_min: int | None = None,
    biz_max: int | None = None,
    dfo_min: int | None = None,
    dfo_max: int | None = None,
    has_company: int | None = None,
    exclude_war: bool | None = None,
    sort: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> Dict[str, Any]:
    """
    List items with filters + pagination.

    - q: searches in title/body/url (LIKE)
    - published_* / fetched_*: ISO datetimes, compared lexicographically
    - biz_* / dfo_*: inclusive bounds
    - has_company: 0/1
    """
    limit = max(1, min(int(limit), 500))
    offset = max(0, int(offset))

    where = []
    params: List[Any] = []

    if q:
        q_like = f"%{q.strip()}%"
        where.append("(title LIKE ? OR body LIKE ? OR url LIKE ?)")
        params.extend([q_like, q_like, q_like])

    if source:
        where.append("source_name = ?")
        params.append(source)

    if published_from:
        where.append("published_at >= ?")
        params.append(published_from)
    if published_to:
        where.append("published_at <= ?")
        params.append(published_to)

    if fetched_from:
        where.append("fetched_at >= ?")
        params.append(fetched_from)
    if fetched_to:
        where.append("fetched_at <= ?")
        params.append(fetched_to)

    if biz_min is not None:
        where.append("business_score >= ?")
        params.append(int(biz_min))
    if biz_max is not None:
        where.append("business_score <= ?")
        params.append(int(biz_max))

    if dfo_min is not None:
        where.append("dfo_score >= ?")
        params.append(int(dfo_min))
    if dfo_max is not None:
        where.append("dfo_score <= ?")
        params.append(int(dfo_max))

    if has_company is not None:
        where.append("has_company = ?")
        params.append(int(has_company))

    if exclude_war:
        where.append(_exclude_war_where_sql())
        params.extend(_exclude_war_params())

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    # Sorting: keep it whitelisted to avoid SQL injection.
    sort_key = (sort or "").strip().lower()
    if sort_key == "source_asc":
        order_sql = "ORDER BY source_name ASC, COALESCE(published_at, fetched_at) DESC, id DESC"
    elif sort_key == "source_desc":
        order_sql = "ORDER BY source_name DESC, COALESCE(published_at, fetched_at) DESC, id DESC"
    elif sort_key == "published_asc":
        order_sql = "ORDER BY COALESCE(published_at, fetched_at) ASC, id ASC"
    else:
        # default
        order_sql = "ORDER BY COALESCE(published_at, fetched_at) DESC, id DESC"

    with connect() as con:
        total = con.execute(
            f"SELECT COUNT(1) AS cnt FROM items {where_sql}",
            params,
        ).fetchone()[0]

        rows = con.execute(
            f"""
            SELECT
                id,
                source_name,
                url,
                title,
                published_at,
                fetched_at,
                business_score,
                dfo_score,
                has_company
            FROM items
            {where_sql}
            {order_sql}
            LIMIT ? OFFSET ?
            """,
            [*params, limit, offset],
        ).fetchall()

        items = [dict(r) for r in rows]
        has_more = (offset + len(items)) < int(total)
        return {
            "total": int(total),
            "limit": limit,
            "offset": offset,
            "has_more": bool(has_more),
            "items": items,
        }
