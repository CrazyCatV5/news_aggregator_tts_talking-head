from __future__ import annotations

from .base import SourceConfig, RssSource

PARSER = RssSource(SourceConfig(
    name='TASS RSS v2',
    kind='rss',
    url='https://tass.ru/rss/v2.xml',
))
