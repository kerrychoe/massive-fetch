"""SQLite manifest — tracks ingestion progress (SPEC §6.3).

Slice 0 creates the schema and reports emptiness. Read/write of ingestion
state arrives in Slice 2.

The manifest is always a true local file path: SQLite cannot run over object
storage, so it is configured separately from the data ``StorageBackend`` (§6.0.1).
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS ingestion_state (
    asset_class    TEXT NOT NULL,            -- 'stocks' | 'futures' | 'crypto'
    symbol         TEXT NOT NULL,            -- e.g., 'AAPL', 'ESH25', 'X:BTCUSD'
    timeframe      TEXT NOT NULL,            -- 'daily' | 'minute'
    earliest_date  TEXT NOT NULL,            -- ISO date, oldest bar present
    last_complete_date TEXT NOT NULL,        -- ISO date, newest bar present
    last_updated_at TEXT NOT NULL,           -- ISO datetime UTC
    bar_count      INTEGER NOT NULL,
    PRIMARY KEY (asset_class, symbol, timeframe)
);

CREATE TABLE IF NOT EXISTS run_log (
    run_id         TEXT PRIMARY KEY,         -- uuid
    started_at     TEXT NOT NULL,
    finished_at    TEXT,
    command        TEXT NOT NULL,            -- the CLI invocation
    status         TEXT NOT NULL,            -- 'running' | 'success' | 'failed'
    error_message  TEXT,
    symbols_attempted INTEGER,
    symbols_succeeded INTEGER,
    bars_written   INTEGER
);

CREATE INDEX IF NOT EXISTS idx_run_log_started ON run_log(started_at DESC);
"""


class Manifest:
    """Thin wrapper over the SQLite manifest file."""

    def __init__(self, path: Path):
        self.path = Path(path)

    def initialize(self) -> None:
        """Create the manifest file and schema if absent (idempotent)."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.path) as conn:
            conn.executescript(SCHEMA)

    def tracked_series_count(self) -> int:
        """Number of ``(asset_class, symbol, timeframe)`` series recorded."""
        with sqlite3.connect(self.path) as conn:
            (count,) = conn.execute("SELECT COUNT(*) FROM ingestion_state").fetchone()
        return count

    def is_empty(self) -> bool:
        """True when no ingestion state has been recorded yet."""
        if not self.path.exists():
            return True
        return self.tracked_series_count() == 0
