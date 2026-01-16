from __future__ import annotations

"""Shared lightweight types.

The ingestion pipeline stores news items as plain JSON-serializable dicts.
Some sources import `NewsItem` for clarity; it is intentionally a type alias.
"""

from typing import Any, Dict

# JSON-serializable news item structure (keys vary by source).
NewsItem = Dict[str, Any]
