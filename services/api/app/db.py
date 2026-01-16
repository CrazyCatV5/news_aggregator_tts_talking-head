import sqlite3
from contextlib import contextmanager
from .config import settings
import logging

SCHEMA_CORE = '''
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
CREATE TABLE IF NOT EXISTS llm_analyses (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  item_id INTEGER NOT NULL,
  model TEXT NOT NULL,
  prompt_version TEXT NOT NULL,
  created_at TEXT NOT NULL,
  is_dfo INTEGER NOT NULL,
  is_business INTEGER NOT NULL,
  is_dfo_business INTEGER NOT NULL,
  interest_score INTEGER NOT NULL,
  title_short TEXT NOT NULL,
  bulletin TEXT NOT NULL,
  summary TEXT NOT NULL,
  tags TEXT NOT NULL DEFAULT '[]',
  why TEXT NOT NULL DEFAULT '',
  raw_json TEXT NOT NULL DEFAULT '{}',
  UNIQUE(item_id, prompt_version),
  FOREIGN KEY(item_id) REFERENCES items(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_llm_analyses_item ON llm_analyses(item_id);
CREATE INDEX IF NOT EXISTS idx_llm_analyses_scores ON llm_analyses(is_dfo_business, interest_score);
'''

# NOTE:
# Daily-digest and TTS tables are created/migrated imperatively in init_db().
# This avoids startup failures when an older DB already contains these tables
# but with a different schema (e.g., missing the `day` column).

def _apply_pragmas(con: sqlite3.Connection):
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    con.execute("PRAGMA busy_timeout=30000;")
    con.execute("PRAGMA temp_store=MEMORY;")


def _table_exists(con: sqlite3.Connection, name: str) -> bool:
    row = con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return row is not None


def _table_columns(con: sqlite3.Connection, name: str) -> set[str]:
    try:
        rows = con.execute(f"PRAGMA table_info({name});").fetchall()
        return {r[1] for r in rows}  # (cid, name, type, notnull, dflt, pk)
    except Exception:
        return set()


def _ensure_daily_digest_schema(con: sqlite3.Connection) -> None:
    """Create/migrate daily digests tables in a backwards-compatible way.

    The project historically evolved through several iterations.
    If an older DB already contains some of these tables, they may miss
    newer columns (e.g., `day`). We must not crash on startup.
    """

    # --- daily_digests ---
    if not _table_exists(con, "daily_digests"):
        con.executescript(
            '''
            CREATE TABLE IF NOT EXISTS daily_digests (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              day TEXT NOT NULL,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              params_json TEXT NOT NULL DEFAULT '{}',
              status TEXT NOT NULL DEFAULT 'draft',
              note TEXT NOT NULL DEFAULT '',
              script_json TEXT,
              script_model TEXT NOT NULL DEFAULT '',
              script_created_at TEXT
            );
            '''
        )
    else:
        cols = _table_columns(con, "daily_digests")
        if "day" not in cols:
            con.execute("ALTER TABLE daily_digests ADD COLUMN day TEXT;")
        if "params_json" not in cols:
            con.execute("ALTER TABLE daily_digests ADD COLUMN params_json TEXT NOT NULL DEFAULT '{}';")
        if "status" not in cols:
            con.execute("ALTER TABLE daily_digests ADD COLUMN status TEXT NOT NULL DEFAULT 'draft';")
        if "note" not in cols:
            con.execute("ALTER TABLE daily_digests ADD COLUMN note TEXT NOT NULL DEFAULT ''; ")
        if "script_json" not in cols:
            con.execute("ALTER TABLE daily_digests ADD COLUMN script_json TEXT;")
        if "script_model" not in cols:
            con.execute("ALTER TABLE daily_digests ADD COLUMN script_model TEXT NOT NULL DEFAULT ''; ")
        if "script_created_at" not in cols:
            con.execute("ALTER TABLE daily_digests ADD COLUMN script_created_at TEXT;")

    # Best-effort backfill of `day`.
    cols = _table_columns(con, "daily_digests")
    if "day" in cols:
        if "created_at" in cols:
            con.execute(
                "UPDATE daily_digests SET day = COALESCE(NULLIF(day,''), substr(created_at, 1, 10))"
            )
        else:
            con.execute("UPDATE daily_digests SET day = COALESCE(NULLIF(day,''), '1970-01-01')")

        # If duplicates exist for the same day, keep the newest row.
        try:
            con.execute(
                """
                DELETE FROM daily_digests
                WHERE id NOT IN (
                    SELECT MAX(id) FROM daily_digests GROUP BY day
                )
                """
            )
        except Exception as e:
            logging.getLogger(__name__).warning("dedup daily_digests skipped: %s", e)

        # Indexes (may still fail if old data is inconsistent; do not crash).
        for stmt in (
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_daily_digests_day ON daily_digests(day);",
            "CREATE INDEX IF NOT EXISTS idx_daily_digests_day ON daily_digests(day);",
        ):
            try:
                con.execute(stmt)
            except Exception as e:
                logging.getLogger(__name__).warning("daily_digests index skipped: %s", e)

    # --- daily_digest_items ---
    if not _table_exists(con, "daily_digest_items"):
        con.executescript(
            '''
            CREATE TABLE IF NOT EXISTS daily_digest_items (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              digest_id INTEGER NOT NULL,
              item_id INTEGER NOT NULL,
              rank INTEGER NOT NULL,
              added_at TEXT NOT NULL,
              FOREIGN KEY(digest_id) REFERENCES daily_digests(id) ON DELETE CASCADE,
              FOREIGN KEY(item_id) REFERENCES items(id) ON DELETE CASCADE
            );
            '''
        )
    # Indexes for mapping table
    for stmt in (
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_daily_digest_items_item ON daily_digest_items(item_id);",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_daily_digest_items_digest_rank ON daily_digest_items(digest_id, rank);",
        "CREATE INDEX IF NOT EXISTS idx_daily_digest_items_digest ON daily_digest_items(digest_id);",
    ):
        try:
            con.execute(stmt)
        except Exception as e:
            logging.getLogger(__name__).warning("daily_digest_items index skipped: %s", e)

    # --- tts_outputs ---
    if not _table_exists(con, "tts_outputs"):
        con.executescript(
            '''
            CREATE TABLE IF NOT EXISTS tts_outputs (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              digest_id INTEGER NOT NULL,
              day TEXT NOT NULL,
              language TEXT NOT NULL DEFAULT 'ru',
              voice_wav TEXT NOT NULL DEFAULT '',
              file_name TEXT NOT NULL,
              created_at TEXT NOT NULL,
              meta_json TEXT NOT NULL DEFAULT '{}',
              FOREIGN KEY(digest_id) REFERENCES daily_digests(id) ON DELETE CASCADE
            );
            '''
        )
    else:
        cols = _table_columns(con, "tts_outputs")
        if "day" not in cols:
            con.execute("ALTER TABLE tts_outputs ADD COLUMN day TEXT;")
        if "language" not in cols:
            con.execute("ALTER TABLE tts_outputs ADD COLUMN language TEXT NOT NULL DEFAULT 'ru';")
        if "voice_wav" not in cols:
            con.execute("ALTER TABLE tts_outputs ADD COLUMN voice_wav TEXT NOT NULL DEFAULT ''; ")
        if "file_name" not in cols:
            con.execute("ALTER TABLE tts_outputs ADD COLUMN file_name TEXT NOT NULL DEFAULT ''; ")
        if "created_at" not in cols:
            con.execute("ALTER TABLE tts_outputs ADD COLUMN created_at TEXT NOT NULL DEFAULT ''; ")
        if "meta_json" not in cols:
            con.execute("ALTER TABLE tts_outputs ADD COLUMN meta_json TEXT NOT NULL DEFAULT '{}';")

        # Backfill day from digest if possible
        try:
            con.execute(
                """
                UPDATE tts_outputs
                SET day = COALESCE(NULLIF(day,''), (SELECT d.day FROM daily_digests d WHERE d.id = tts_outputs.digest_id))
                WHERE day IS NULL OR day = ''
                """
            )
        except Exception:
            pass

    for stmt in (
        "CREATE INDEX IF NOT EXISTS idx_tts_outputs_day ON tts_outputs(day);",
        "CREATE INDEX IF NOT EXISTS idx_tts_outputs_digest ON tts_outputs(digest_id);",
    ):
        try:
            con.execute(stmt)
        except Exception as e:
            logging.getLogger(__name__).warning("tts_outputs index skipped: %s", e)

def init_db():
    with sqlite3.connect(settings.db_path, timeout=30) as con:
        _apply_pragmas(con)
        con.executescript(SCHEMA_CORE)
        _ensure_daily_digest_schema(con)
        # Enforce uniqueness by canonical URL (project requirement).
        # If duplicates exist, keep the newest row (max(id)) for each url_canon.
        try:
            con.execute("""
                DELETE FROM items
                WHERE id NOT IN (
                    SELECT MAX(id) FROM items GROUP BY url_canon
                )
            """)
        except Exception as e:
            logging.getLogger(__name__).warning("dedup by url_canon skipped: %s", e)

        # Create unique index for url_canon (after dedup).
        try:
            con.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_items_url_canon ON items(url_canon);")
        except Exception as e:
            logging.getLogger(__name__).error("failed to create uq_items_url_canon: %s", e)

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
