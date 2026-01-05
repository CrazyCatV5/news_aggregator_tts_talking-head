import re
from typing import Dict, List, Tuple

DFO_TERMS = [
    "дальний восток", "дфо", "крдк", "крдв", "спв", "свободный порт", "тор", "вэф",
    "приморск", "хабаровск", "амурск", "сахалин", "якут", "саха", "камчат", "магадан", "чукот",
    "еврейск", "еао", "забайкал", "бурят",
    "владивосток", "находк", "артем", "уссурийск", "большой камень", "порт восточный",
    "комсомольск-на-амуре", "благовещенск", "южно-сахалинск", "петропавловск-камчатск",
    "анадырь", "нерюнгри", "чита", "улан-удэ",
]

BUSINESS_TERMS = [
    "инвестици", "проект", "строительств", "завод", "производств", "контракт", "сделк",
    "акци", "доля", "прибыл", "выручк", "банкрот", "торги", "концесс",
    "логист", "порт", "терминал", "экспорт", "импорт", "резидент", "предприят",
    "поставк", "тариф", "кредит", "финансир", "инфраструктур",
]

COMPANY_PATTERNS = [
    r"\bпао\b", r"\bоао\b", r"\bзао\b", r"\bооо\b", r"\bао\b", r"\bгк\b",
    r"\bбанк\b", r"\bхолдинг\b", r"\bкорпорац",
]

def _count_hits(text: str, terms: List[str]) -> int:
    t = text.lower()
    return sum(1 for term in terms if term in t)

def score(text: str) -> Tuple[int, int, int, Dict]:
    t = (text or "").lower()
    dfo_hits = _count_hits(t, DFO_TERMS)
    biz_hits = _count_hits(t, BUSINESS_TERMS)

    dfo_score = 0
    if dfo_hits >= 1: dfo_score += 1
    if dfo_hits >= 2: dfo_score += 1
    if dfo_hits >= 4: dfo_score += 1
    if dfo_hits >= 7: dfo_score += 1

    business_score = 0
    if biz_hits >= 1: business_score += 1
    if biz_hits >= 2: business_score += 1
    if biz_hits >= 4: business_score += 1
    if biz_hits >= 7: business_score += 1

    has_company = 1 if any(re.search(pat, t, flags=re.IGNORECASE) for pat in COMPANY_PATTERNS) else 0
    reasons = {"dfo_hits": dfo_hits, "biz_hits": biz_hits, "has_company": bool(has_company)}
    return business_score, dfo_score, has_company, reasons
