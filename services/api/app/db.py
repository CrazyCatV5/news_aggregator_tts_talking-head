import sqlite3
from contextlib import contextmanager
from .config import settings

SCHEMA = '''
CREATE TABLE IF NOT EXISTS items (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_name TEXT NOT NULL,
  url TEXT NOT NULL,
  url_canon TEXT NOT NULL,
  title TEXT NOT NULL,
  body TEXT NOT NULL,
  published_at TEXT,
  fetched_at TEXT NOT NULL,
  fingerprint TEXT NOT NULL,
  business_score INTEGER NOT NULL DEFAULT 0,
  dfo_score INTEGER NOT NULL DEFAULT 0,
  has_company INTEGER NOT NULL DEFAULT 0,
  reasons TEXT NOT NULL DEFAULT '{}',
  UNIQUE(source_name, fingerprint)
);
CREATE INDEX IF NOT EXISTS idx_items_published ON items(published_at);
CREATE INDEX IF NOT EXISTS idx_items_scores ON items(business_score, dfo_score);
'''

def _apply_pragmas(con: sqlite3.Connection):
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    con.execute("PRAGMA busy_timeout=30000;")
    con.execute("PRAGMA temp_store=MEMORY;")

def init_db():
    with sqlite3.connect(settings.db_path, timeout=30) as con:
        _apply_pragmas(con)
        con.executescript(SCHEMA)
        con.commit()

@contextmanager
def connect():
    con = sqlite3.connect(settings.db_path, timeout=30, check_same_thread=False)
    con.row_factory = sqlite3.Row
    _apply_pragmas(con)
    try:
        yield con
    finally:
        con.close()
