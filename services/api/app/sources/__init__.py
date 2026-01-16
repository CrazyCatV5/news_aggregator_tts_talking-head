from __future__ import annotations

from typing import Dict, List

from .base import SourceParser
from .vedomosti_economics import PARSER as vedomosti_economics
from .tass_rss_v2 import PARSER as tass_rss_v2
from .rbc_export import PARSER as rbc_export
from .rg_doc import PARSER as rg_doc
from .forbes_russia import PARSER as forbes_russia
from .eastrussia import PARSER as eastrussia
from .dvnovosti import PARSER as dvnovosti

# Registry keyed by source name exactly as in sources.json
REGISTRY: Dict[str, SourceParser] = {
    vedomosti_economics.config.name: vedomosti_economics,
    tass_rss_v2.config.name: tass_rss_v2,
    rbc_export.config.name: rbc_export,
    rg_doc.config.name: rg_doc,
    forbes_russia.config.name: forbes_russia,
    eastrussia.config.name: eastrussia,
    dvnovosti.config.name: dvnovosti,
}

def list_source_names() -> List[str]:
    return list(REGISTRY.keys())

def get_parser(source_name: str) -> SourceParser:
    try:
        return REGISTRY[source_name]
    except KeyError:
        raise KeyError(f"Unknown source: {source_name}")

def queue_key_for_source(source_name: str) -> str:
    # Keep keys stable and ASCII-safe (redis keys are bytes-safe, but this makes logs nicer).
    # We intentionally do not depend on sources.json here.
    import re
    slug = source_name.lower()
    slug = re.sub(r"[^a-z0-9]+", "_", slug).strip("_")
    return f"dfo:queue:{slug}"


def queue_keys() -> List[str]:
    return [queue_key_for_source(n) for n in list_source_names()]

def source_name_from_queue_key(key: str) -> str:
    # reverse lookup; used by worker that listens to multiple queues
    for n in list_source_names():
        if queue_key_for_source(n) == key:
            return n
    raise KeyError(f"Unknown queue key: {key}")
