"""Crypto daily ingestion (SPEC §8.3, §13 Slice 2).

Fetches daily OHLCV bars for the configured crypto symbols through the REST
client, normalizes to the canonical §6.1 schema, appends to a per-symbol Parquet
via the storage backend, then records progress in the manifest.

Crypto trades 24/7, so there is no market calendar and "yesterday" is measured in
**UTC**, not ET (SPEC §8.3). Two identifiers per symbol: the bare symbol (``BTC``)
is the file/storage key, while the Massive ticker (``X:BTCUSD``) is the API arg,
the ``symbol`` column value, and the manifest key (SPEC §5.3, §6.1, §6.3).
"""

from __future__ import annotations

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
_TIMEFRAME = "daily"


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
    """Backfill/update crypto daily bars. See the module docstring."""
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
        state = manifest.get_state(_ASSET_CLASS, ticker, _TIMEFRAME)
        if state is not None and state["last_complete_date"] >= target_end:
            logger.info(
                "ingest.skip_uptodate",
                symbol=ticker,
                last_complete_date=state["last_complete_date"],
                target_end=target_end,
            )
            result.skipped_uptodate.append(ticker)
            continue
        # Resume from last_complete_date + 1 when a row exists (SPEC §6.3),
        # otherwise from the configured/explicit start.
        from_date = _next_day(state["last_complete_date"]) if state is not None else default_start
        planned.append((bare, AggsRequest(ticker, 1, "day", from_date, target_end)))
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
        key = paths.crypto_daily_key(bare)
        backend.append_parquet(key, df, ["symbol", "timestamp"])

        # Read the merged file back for authoritative manifest values, then record
        # in a single transaction AFTER the Parquet commit (write-then-record, §8).
        merged = backend.read_parquet(key)
        _record_manifest(manifest, ticker, merged)
        result.succeeded.append(ticker)
        logger.info(
            "ingest.symbol_done",
            symbol=ticker,
            bars_fetched=len(bars),
            total_bars=merged.height,
            key=key,
        )

    return result


def _record_manifest(manifest: Manifest, ticker: str, merged: pl.DataFrame) -> None:
    ts = merged.get_column("timestamp")
    manifest.upsert_state(
        _ASSET_CLASS,
        ticker,
        _TIMEFRAME,
        earliest_date=ts.min().date().isoformat(),
        last_complete_date=ts.max().date().isoformat(),
        bar_count=merged.height,
        last_updated_at=datetime.now(timezone.utc).isoformat(),
    )
