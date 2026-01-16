from __future__ import annotations

from .base import SourceConfig, RssSource

PARSER = RssSource(SourceConfig(
    name='Vedomosti Economics',
    kind='rss',
    url='https://www.vedomosti.ru/rss/rubric/economics',
))
