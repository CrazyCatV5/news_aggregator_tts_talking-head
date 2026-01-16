from __future__ import annotations

from .base import SourceConfig, HtmlSource

PARSER = HtmlSource(SourceConfig(
    name='RG/doc',
    kind='html',
    url='https://rg.ru/doc',
))
