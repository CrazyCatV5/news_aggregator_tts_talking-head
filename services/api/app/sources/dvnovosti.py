from __future__ import annotations

"""DVnovosti

DVnovosti is a JS-heavy site, and parsing HTML index pages is unreliable.
The project therefore uses the public RSS feed as a stable listing source.

Goal for this source:
  - Keep RSS as listing.
  - Enrich each item by downloading the article page (handled by RssSource).
  - Improve relevance for *business/economy* by applying a conservative filter
    after enrichment (title+full text), using the project's existing scoring.

We do not introduce any new ingest contracts: `fetch_items()` still returns
the same item dict keys expected by ingest.
"""

from typing import Any, Dict, List

from .base import SourceConfig, RssSource
from ..scoring import score


class DVNovostiRssBusiness(RssSource):
    """RSS listing + full-text enrichment, then mild business filter.

    Rationale:
    - DVnovosti is strongly DFO by nature, but the general RSS contains many
      non-business items. Once we have full text, we can filter more accurately.
    - We use the existing `scoring.score()` (no new heuristics/terms added here).
    """

    # A minimal threshold: 1+ business points keeps most economic items, while
    # dropping obvious non-business noise.
    BIZ_MIN_SCORE = 1

    def fetch_items(self, limit_per_html_source: int = 500) -> List[Dict[str, Any]]:
        # First, get enriched items via the base implementation.
        items = super().fetch_items(limit_per_html_source=limit_per_html_source)
        if not items:
            return []

        out: List[Dict[str, Any]] = []
        for it in items:
            title = (it.get("title") or "").strip()
            body = (it.get("body") or "").strip()
            text = f"{title}\n{body}".strip()
            biz_score, _dfo_score, _has_company, _reasons = score(text)

            # Keep if it looks businessy enough, OR if RSS taxonomy explicitly
            # tags it as economics/business (when available).
            keep = biz_score >= self.BIZ_MIN_SCORE
            tags = it.get("tags") or []
            if not keep and tags:
                tags_join = " ".join(str(t).lower() for t in tags)
                if any(k in tags_join for k in ["эконом", "бизнес", "финанс", "инвест", "рынок", "компан"]):
                    keep = True

            if keep:
                out.append(it)

        return out


PARSER = DVNovostiRssBusiness(
    SourceConfig(
        name="DVnovosti",
        kind="rss",
        url="https://www.dvnovosti.ru/rss/",
    )
)
