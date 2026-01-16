from __future__ import annotations

from .base import SourceConfig, HtmlSource

PARSER = HtmlSource(SourceConfig(
    name='Forbes Russia',
    kind='html',
    url='https://www.forbes.ru/',
))
