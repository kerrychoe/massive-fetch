"""Crypto ingestion — daily and minute (SPEC §8.3, §13 Slice 2 + Slice 3).

Fetches OHLCV bars for the configured crypto symbols through the REST client,
normalizes to the canonical §6.1 schema, appends to per-symbol Parquet via the
storage backend, then records progress in the manifest.

A single timeframe-aware core (``ingest_crypto``) drives both timeframes; only the
write+record step differs (SPEC §6):

- **daily** — one file per symbol (``crypto_daily_key``), full history.
- **minute** — year-partitioned per symbol (``crypto_minute_key(symbol, year)``,
  ``ohlcv/crypto/minute/{SYMBOL}/{YYYY}.parquet``). One fetch covers the whole
  range; the SDK paginates; the normalized frame is split by UTC year and each
  year is appended to its ``{YYYY}.parquet`` (read-merge-write + dedupe). The
  single manifest row is recorded once, after every year file is committed.

Resumability (SPEC §6.3) is identical for both: resume from
``last_complete_date + 1 day``. Because ``target_end`` is always a complete past
UTC day (yesterday), the recorded ``last_complete_date`` is always a fully-present
day, so ``+1`` never skips an un-fetched bar. The one crash window — between the
Parquet commit and the manifest upsert (disk holds more than the manifest knows)
— is healed dupe-free on rerun by ``append_parquet``'s dedupe on
``(symbol, timestamp)`` at minute (nanosecond) precision.

Crypto trades 24/7, so there is no market calendar and "yesterday" is measured in
**UTC**, not ET (SPEC §8.3). Two identifiers per symbol: the bare symbol (``BTC``)
is the file/storage key, while the Massive ticker (``X:BTCUSD``) is the API arg,
the ``symbol`` column value, and the manifest key (SPEC §5.3, §6.1, §6.3).
"""

from __future__ import annotations

import posixpath
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone

import polars as pl
import structlog

from massive_fetch.clients.rest import (
    AggsRequest,
    MassiveAuthError,
    MassiveClientError,
    MassiveRESTClient,
)
from massive_fetch.config import AppConfig
from massive_fetch.storage import paths
from massive_fetch.storage.backend import StorageBackend
from massive_fetch.storage.manifest import Manifest
from massive_fetch.transform.normalize import normalize

_ASSET_CLASS = "crypto"

# SPEC §7.1 / SDK_NOTES §3: list_aggs timespan tokens, keyed by our timeframe name.
_TIMESPAN = {"daily": "day", "minute": "minute"}


@dataclass(frozen=True)
class SymbolPlan:
    """What would be (or was) fetched for one symbol."""

    ticker: str
    from_date: str
    to_date: str


@dataclass
class CryptoIngestResult:
    """Per-run outcome, by symbol disposition. Drives the CLI summary + exit code."""

    succeeded: list[str] = field(default_factory=list)
    skipped_error: list[str] = field(default_factory=list)
    skipped_uptodate: list[str] = field(default_factory=list)
    zero_bar: list[str] = field(default_factory=list)
    plan: list[SymbolPlan] = field(default_factory=list)
    dry_run: bool = False

    @property
    def exit_code(self) -> int:
        """0 success · 1 partial failure · 2 total failure (SPEC §10.2)."""
        attempted = len(self.succeeded) + len(self.skipped_error) + len(self.zero_bar)
        if not self.skipped_error:
            return 0
        if len(self.skipped_error) == attempted:
            return 2
        return 1


def _yesterday_utc() -> str:
    return (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()


def _next_day(iso_date: str) -> str:
    return (date.fromisoformat(iso_date) + timedelta(days=1)).isoformat()


async def ingest_crypto(
    *,
    config: AppConfig,
    backend: StorageBackend,
    manifest: Manifest,
    client: MassiveRESTClient,
    logger: structlog.stdlib.BoundLogger,
    timeframe: str,  # "daily" | "minute"
    symbols: list[str] | None = None,
    start: str | None = None,
    end: str | None = None,
    dry_run: bool = False,
) -> CryptoIngestResult:
    """Backfill/update crypto bars at ``timeframe``. See the module docstring."""
    timespan = _TIMESPAN[timeframe]
    quote = config.ingest.crypto.quote_currency
    bare_symbols = symbols if symbols is not None else config.ingest.crypto.symbols
    default_start = start or config.defaults.crypto_start
    target_end = end or _yesterday_utc()

    result = CryptoIngestResult(dry_run=dry_run)

    # 1. Build the fetch plan. The manifest "already up to date" check runs BEFORE
    #    any fetch and short-circuits the API call entirely (SPEC §6.3, §12).
    planned: list[tuple[str, AggsRequest]] = []  # (bare_symbol, request)
    for bare in bare_symbols:
        ticker = f"X:{bare}{quote}"  # Massive crypto ticker, e.g. X:BTCUSD (§5.3)
        state = manifest.get_state(_ASSET_CLASS, ticker, timeframe)
        if state is not None and state["last_complete_date"] >= target_end:
            logger.info(
                "ingest.skip_uptodate",
                symbol=ticker,
                timeframe=timeframe,
                last_complete_date=state["last_complete_date"],
                target_end=target_end,
            )
            result.skipped_uptodate.append(ticker)
            continue
        # Resume from last_complete_date + 1 when a row exists (SPEC §6.3),
        # otherwise from the configured/explicit start.
        from_date = _next_day(state["last_complete_date"]) if state is not None else default_start
        planned.append((bare, AggsRequest(ticker, 1, timespan, from_date, target_end)))
        result.plan.append(SymbolPlan(ticker, from_date, target_end))

    if dry_run or not planned:
        # Nothing to fetch -> zero SDK calls (the same-UTC-day rerun no-op).
        return result

    # 2. Fan out. Concurrency is bounded by the client's own semaphore (SPEC §7.4).
    results = await client.fetch_many(req for _, req in planned)

    # 3. Apply per-symbol policy, then write-then-record per symbol.
    for bare, req in planned:
        ticker = req.ticker
        res = results[ticker]

        if isinstance(res, MassiveAuthError):
            raise res  # auth failure affects every symbol -> abort the whole run
        if isinstance(res, MassiveClientError):
            # MassiveBadRequest / MassiveRetriesExhausted -> skip this symbol.
            logger.warning(
                "ingest.symbol_failed",
                symbol=ticker,
                error_type=type(res).__name__,
                error=str(res),
            )
            result.skipped_error.append(ticker)
            continue
        if isinstance(res, BaseException):
            raise res  # unexpected exception type -> never swallow

        bars = res
        if not bars:
            # Empty iterator == success with zero bars: no write, no manifest row.
            # A never-fetched symbol stays unrecorded; the next run re-queries it
            # from the configured start.
            logger.info(
                "ingest.zero_bars", symbol=ticker, from_date=req.from_date, to_date=req.to_date
            )
            result.zero_bar.append(ticker)
            continue

        df = normalize(bars, ticker)
        if timeframe == "daily":
            total_bars = _write_and_record_daily(backend, manifest, bare, ticker, df)
        else:
            total_bars = _write_and_record_minute(backend, manifest, bare, ticker, df)

        result.succeeded.append(ticker)
        logger.info(
            "ingest.symbol_done",
            symbol=ticker,
            timeframe=timeframe,
            bars_fetched=len(bars),
            total_bars=total_bars,
        )

    return result


async def ingest_crypto_daily(
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
) -> CryptoIngestResult:
    """Backfill/update crypto **daily** bars (Slice 2 entry point, preserved)."""
    return await ingest_crypto(
        config=config,
        backend=backend,
        manifest=manifest,
        client=client,
        logger=logger,
        timeframe="daily",
        symbols=symbols,
        start=start,
        end=end,
        dry_run=dry_run,
    )


def _write_and_record_daily(
    backend: StorageBackend, manifest: Manifest, bare: str, ticker: str, df: pl.DataFrame
) -> int:
    """Append the full-history daily file, then record the manifest from it (§6, §8)."""
    key = paths.crypto_daily_key(bare)
    backend.append_parquet(key, df, ["symbol", "timestamp"])
    merged = backend.read_parquet(key)
    _record_manifest(manifest, ticker, "daily", merged)
    return merged.height


def _write_and_record_minute(
    backend: StorageBackend, manifest: Manifest, bare: str, ticker: str, df: pl.DataFrame
) -> int:
    """Split one frame by UTC year, append each ``{YYYY}.parquet``, then record once.

    Years are written in ascending order (deterministic). The normalized frame is
    filtered per year — no year helper column is ever added, so each on-disk file
    keeps the exact canonical §6.1 schema. The single manifest row is computed from
    ALL of the symbol's year files (read back authoritatively, post-dedupe) and
    upserted only after every year file is committed (write-then-record, SPEC §8).
    """
    years = sorted(df.get_column("timestamp").dt.year().unique().to_list())
    for year in years:
        year_df = df.filter(pl.col("timestamp").dt.year() == year)
        backend.append_parquet(paths.crypto_minute_key(bare, year), year_df, ["symbol", "timestamp"])

    # Read back every year file for this symbol — authoritative earliest/last/count
    # across years (mirrors the daily read-back). The minute directory is derived
    # from the key builder so storage/paths.py stays the single source of truth.
    minute_dir = posixpath.dirname(paths.crypto_minute_key(bare, 0))
    keys = backend.list_keys(minute_dir)
    merged = pl.concat([backend.read_parquet(k) for k in keys])
    _record_manifest(manifest, ticker, "minute", merged)
    return merged.height


def _record_manifest(
    manifest: Manifest, ticker: str, timeframe: str, merged: pl.DataFrame
) -> None:
    ts = merged.get_column("timestamp")
    manifest.upsert_state(
        _ASSET_CLASS,
        ticker,
        timeframe,
        earliest_date=ts.min().date().isoformat(),
        last_complete_date=ts.max().date().isoformat(),
        bar_count=merged.height,
        last_updated_at=datetime.now(timezone.utc).isoformat(),
    )
