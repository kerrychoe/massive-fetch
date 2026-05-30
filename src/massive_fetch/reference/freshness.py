"""Reference-data freshness (SPEC §10.3).

Slice 4 implements only the **stocks universe** row; the full §10.3 ``status``
table (futures contracts, market calendar, splits, dividends) and its dedicated
``StatusConfig`` thresholds are deferred until those datasets exist (Slices 7–8).
This module is shaped so that table extends it later: each dataset gets one
``*_freshness`` function returning an age + OK/WARN/STALE flag, and ``status`` will
consume them.

The stocks-universe thresholds derive from the **existing**
``ingest.stocks.refresh_interval_days`` (default 7), not a new config block:

- ``OK``    — age ≤ refresh_interval_days
- ``WARN``  — refresh_interval_days < age ≤ 2 × refresh_interval_days
- ``STALE`` — age > 2 × refresh_interval_days
- ``MISSING`` — the parquet does not exist

Freshness reads the universe parquet's ``generated_at`` column (SPEC §10.3: the
stored timestamp, not file mtime), which on the frozen-YAML fallback path carries
the data's true vintage — so a long-broken scraper correctly reads as STALE.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from massive_fetch.config import AppConfig
from massive_fetch.storage import paths
from massive_fetch.storage.backend import StorageBackend

FLAG_OK = "OK"
FLAG_WARN = "WARN"
FLAG_STALE = "STALE"
FLAG_MISSING = "MISSING"


@dataclass
class UniverseFreshness:
    """Freshness of the stocks ``universe_stocks.parquet`` (SPEC §10.3)."""

    exists: bool
    flag: str
    generated_at: datetime | None = None
    age_days: int | None = None
    ticker_count: int | None = None


def _flag_for_age(age_days: int, refresh_interval_days: int) -> str:
    if age_days <= refresh_interval_days:
        return FLAG_OK
    if age_days <= 2 * refresh_interval_days:
        return FLAG_WARN
    return FLAG_STALE


def universe_freshness(backend: StorageBackend, config: AppConfig) -> UniverseFreshness:
    """Return the stocks-universe freshness, reading the parquet's ``generated_at``.

    Thresholds come from ``config.ingest.stocks.refresh_interval_days`` (see module
    docstring). A missing parquet returns ``flag=MISSING`` so callers can suggest
    ``massive-fetch reference update``.
    """
    key = paths.universe_stocks_key()
    if not backend.exists(key):
        return UniverseFreshness(exists=False, flag=FLAG_MISSING)

    df = backend.read_parquet(key)
    generated_at = df.get_column("generated_at").max()
    if generated_at is None:
        # Present but no usable timestamp — treat as stale rather than silently OK.
        return UniverseFreshness(
            exists=True, flag=FLAG_STALE, ticker_count=df.height
        )

    if generated_at.tzinfo is None:
        generated_at = generated_at.replace(tzinfo=timezone.utc)
    age_days = (datetime.now(timezone.utc) - generated_at).days
    flag = _flag_for_age(age_days, config.ingest.stocks.refresh_interval_days)
    return UniverseFreshness(
        exists=True,
        flag=flag,
        generated_at=generated_at,
        age_days=age_days,
        ticker_count=df.height,
    )
