"""Stocks ingestion — daily (SPEC §8.1, §13 Slice 5).

A thin asset profile over the shared ingestion core (``ingest/base.py``). Unlike
crypto, a stock's symbol **is** its Massive ticker (no ``X:`` prefix), so the
storage symbol, api ticker, manifest key, and ``symbol`` column are all the same
string — including dot tickers like ``BRK.B`` (the Massive form, §8.1), which flow
straight to the on-disk key (``ohlcv/stocks/daily/BRK.B.parquet``) and the ``symbol``
column. The universe comes from ``reference/universe_stocks.parquet``.

"Yesterday" is the last *complete* NYSE session, not a UTC day (stocks don't trade
24/7) — resolved via ``reference.calendar.nyse_target_end``. The S6 probe confirmed
daily bars are stamped at midnight ET (04:00Z EDT / 05:00Z EST), so the bar's UTC
date equals the ET session date and the shared core's UTC date derivation
(``_record_manifest``) is reused as-is. Bars are stored RAW (``adjusted=false``, the
``AggsRequest`` default per SPEC §6.1; never overridden); split/dividend adjustment
is a read-time concern (Slice 7). Minute ingestion is Slice 6 — not built here.
"""

from __future__ import annotations

import structlog

from massive_fetch.clients.rest import MassiveRESTClient
from massive_fetch.config import AppConfig
from massive_fetch.ingest.base import AssetProfile, IngestResult, run_ingest
from massive_fetch.reference.calendar import nyse_target_end
from massive_fetch.reference.universe import load_universe_tickers
from massive_fetch.storage import paths
from massive_fetch.storage.backend import StorageBackend
from massive_fetch.storage.manifest import Manifest

_ASSET_CLASS = "stocks"

# A stock's symbol is its own Massive ticker, so storage_symbol == api_ticker.
# minute_key is wired for completeness (the base minute path uses it) but the
# Slice 5 daily entrypoint never exercises it — stocks minute is Slice 6.
_STOCKS_PROFILE = AssetProfile(
    asset_class=_ASSET_CLASS,
    to_identifiers=lambda symbol: (symbol, symbol),
    daily_key=paths.stocks_daily_key,
    minute_key=paths.stocks_minute_key,
)


async def ingest_stocks_daily(
    *,
    config: AppConfig,
    backend: StorageBackend,
    manifest: Manifest,
    client: MassiveRESTClient,
    logger: structlog.stdlib.BoundLogger,
    symbols: list[str] | None = None,
    start: str | None = None,
    end: str | None = None,
    dry_run: bool = False,
) -> IngestResult:
    """Backfill/update stocks **daily** bars for the universe (SPEC §13 Slice 5).

    Explicit ``symbols`` bypass the universe file (used by the gated smoke); otherwise
    the universe is loaded from ``reference/universe_stocks.parquet`` — raising
    :class:`~massive_fetch.reference.universe.MissingUniverseError` if it is absent or
    empty. Callers pass a clean ticker list: the CLI normalizes ``--symbols``
    (upper-case + order-preserving de-dupe) and the universe parquet is already
    de-duplicated, so no ticker is processed twice (``fetch_many`` keys results by
    ticker). ``target_end`` defaults to the last complete NYSE session; ``start`` to
    ``config.defaults.stocks_daily_start`` (the tier may clamp an early start to the
    real history floor — SDK_NOTES §11).
    """
    entries = symbols if symbols is not None else load_universe_tickers(backend)
    return await run_ingest(
        config=config,
        backend=backend,
        manifest=manifest,
        client=client,
        logger=logger,
        profile=_STOCKS_PROFILE,
        timeframe="daily",
        entries=entries,
        default_start=start or config.defaults.stocks_daily_start,
        target_end=end or nyse_target_end(),
        dry_run=dry_run,
    )
