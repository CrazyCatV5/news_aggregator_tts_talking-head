from __future__ import annotations

from .base import SourceConfig, RssSource

PARSER = RssSource(SourceConfig(
    name='RBC Export',
    kind='rss',
    url='https://rssexport.rbc.ru/rbcnews/news/30/full.rss',
))
