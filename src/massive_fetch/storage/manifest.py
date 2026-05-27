"""SQLite manifest — tracks ingestion progress (SPEC §6.3).

Slice 0 creates the schema and reports emptiness. Slice 2 adds read/write of
ingestion state (``get_state`` / ``upsert_state``).

The manifest is always a true local file path: SQLite cannot run over object
storage, so it is configured separately from the data ``StorageBackend`` (§6.0.1).
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

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

    # -- ingestion_state read/write (Slice 2) -------------------------------

    def get_state(
        self, asset_class: str, symbol: str, timeframe: str
    ) -> dict[str, Any] | None:
        """Return the ``ingestion_state`` row for a series, or ``None`` if absent."""
        if not self.path.exists():
            return None
        with sqlite3.connect(self.path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM ingestion_state "
                "WHERE asset_class = ? AND symbol = ? AND timeframe = ?",
                (asset_class, symbol, timeframe),
            ).fetchone()
        return dict(row) if row is not None else None

    def upsert_state(
        self,
        asset_class: str,
        symbol: str,
        timeframe: str,
        *,
        earliest_date: str,
        last_complete_date: str,
        bar_count: int,
        last_updated_at: str,
    ) -> None:
        """Insert or update one series' state in a single transaction.

        Called *after* the Parquet commit (write-then-record, SPEC §8): if the
        data write fails the manifest is never advanced, so a rerun retries the
        symbol; a stale manifest is corrected on the next run via append+dedupe.
        """
        with sqlite3.connect(self.path) as conn:
            conn.execute(
                """
                INSERT INTO ingestion_state
                    (asset_class, symbol, timeframe,
                     earliest_date, last_complete_date, last_updated_at, bar_count)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(asset_class, symbol, timeframe) DO UPDATE SET
                    earliest_date      = excluded.earliest_date,
                    last_complete_date = excluded.last_complete_date,
                    last_updated_at    = excluded.last_updated_at,
                    bar_count          = excluded.bar_count
                """,
                (
                    asset_class,
                    symbol,
                    timeframe,
                    earliest_date,
                    last_complete_date,
                    last_updated_at,
                    bar_count,
                ),
            )
