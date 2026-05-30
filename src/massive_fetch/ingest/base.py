"""Shared ingestion plumbing (SPEC §3, §8).

Every asset class (crypto, stocks, …) backfills the same way (SPEC §8):

1. Load the universe.
2. Per instrument, ask the manifest where to resume; **short-circuit** if already
   up to date (``last_complete_date >= target_end``).
3. Fan out the fetch through the REST client (concurrency-bounded by its semaphore).
4. Normalize to the canonical §6.1 schema.
5. **Append** to per-symbol Parquet (read-merge-write + dedupe on
   ``(symbol, timestamp)``), then **record** the manifest (write-then-record, §8).

Only a handful of points differ per asset class; they are captured by
:class:`AssetProfile` so this core stays asset-agnostic:

- ``asset_class``    — the manifest namespace.
- ``to_identifiers`` — maps one universe entry to ``(storage_symbol, api_ticker)``.
- ``daily_key`` / ``minute_key`` — the on-disk key builders (``storage/paths.py``).

**Identifier contract (load-bearing).** ``to_identifiers(entry)`` returns
``(storage_symbol, api_ticker)``. The **api_ticker** is what flows everywhere
externally observable — every :class:`IngestResult` list, ``SymbolPlan.ticker``,
every ``symbol=`` log field, the **manifest key**, and the ``symbol`` column. The
**storage_symbol** is used for one thing only: the storage key argument. For crypto
they differ (``BTC`` vs ``X:BTCUSD``); for stocks they are identical (``AAPL``).
Because stocks make them equal, a swap of the two is invisible to stocks tests and
is caught **only** by the crypto regression suite — which is why that suite must
stay green and unedited across this extraction.
"""

from __future__ import annotations

import posixpath
from collections.abc import Callable
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
from massive_fetch.storage.backend import StorageBackend
from massive_fetch.storage.manifest import Manifest
from massive_fetch.transform.normalize import normalize

# SPEC §7.1 / SDK_NOTES §3: list_aggs timespan tokens, keyed by our timeframe name.
_TIMESPAN = {"daily": "day", "minute": "minute"}


@dataclass(frozen=True)
class SymbolPlan:
    """What would be (or was) fetched for one symbol. ``ticker`` is the api_ticker."""

    ticker: str
    from_date: str
    to_date: str


@dataclass
class IngestResult:
    """Per-run outcome, by symbol disposition. Drives the CLI summary + exit code.

    Every list holds the **api_ticker** (the manifest key / ``symbol``-column value),
    never the storage symbol — see the module's identifier contract.
    """

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


@dataclass(frozen=True)
class AssetProfile:
    """The per-asset-class seams the shared core needs (see module docstring)."""

    asset_class: str
    to_identifiers: Callable[[str], tuple[str, str]]  # entry -> (storage_symbol, api_ticker)
    daily_key: Callable[[str], str]
    minute_key: Callable[[str, int], str]


def _yesterday_utc() -> str:
    return (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()


def _next_day(iso_date: str) -> str:
    return (date.fromisoformat(iso_date) + timedelta(days=1)).isoformat()


async def run_ingest(
    *,
    config: AppConfig,
    backend: StorageBackend,
    manifest: Manifest,
    client: MassiveRESTClient,
    logger: structlog.stdlib.BoundLogger,
    profile: AssetProfile,
    timeframe: str,  # "daily" | "minute"
    entries: list[str],
    default_start: str,  # resolved by the caller
    target_end: str,  # resolved by the caller (UTC yesterday / NYSE session)
    dry_run: bool = False,
) -> IngestResult:
    """Backfill/update ``entries`` at ``timeframe`` for one asset class.

    ``default_start`` and ``target_end`` arrive already resolved so this core stays
    free of any clock or market-calendar dependency (the entrypoints own that).
    """
    timespan = _TIMESPAN[timeframe]
    result = IngestResult(dry_run=dry_run)

    # 1. Build the fetch plan. The manifest "already up to date" check runs BEFORE
    #    any fetch and short-circuits the API call entirely (SPEC §6.3, §12).
    planned: list[tuple[str, AggsRequest]] = []  # (storage_symbol, request)
    for entry in entries:
        storage_symbol, api_ticker = profile.to_identifiers(entry)
        state = manifest.get_state(profile.asset_class, api_ticker, timeframe)
        if state is not None and state["last_complete_date"] >= target_end:
            logger.info(
                "ingest.skip_uptodate",
                symbol=api_ticker,
                timeframe=timeframe,
                last_complete_date=state["last_complete_date"],
                target_end=target_end,
            )
            result.skipped_uptodate.append(api_ticker)
            continue
        # Resume from last_complete_date + 1 when a row exists (SPEC §6.3),
        # otherwise from the configured/explicit start.
        from_date = _next_day(state["last_complete_date"]) if state is not None else default_start
        planned.append((storage_symbol, AggsRequest(api_ticker, 1, timespan, from_date, target_end)))
        result.plan.append(SymbolPlan(api_ticker, from_date, target_end))

    if dry_run or not planned:
        # Nothing to fetch -> zero SDK calls (the same-day rerun no-op).
        return result

    # 2. Fan out. Concurrency is bounded by the client's own semaphore (SPEC §7.4).
    results = await client.fetch_many(req for _, req in planned)

    # 3. Apply per-symbol policy, then write-then-record per symbol.
    for storage_symbol, req in planned:
        api_ticker = req.ticker
        res = results[api_ticker]

        if isinstance(res, MassiveAuthError):
            raise res  # auth failure affects every symbol -> abort the whole run
        if isinstance(res, MassiveClientError):
            # MassiveBadRequest / MassiveRetriesExhausted -> skip this symbol.
            logger.warning(
                "ingest.symbol_failed",
                symbol=api_ticker,
                error_type=type(res).__name__,
                error=str(res),
            )
            result.skipped_error.append(api_ticker)
            continue
        if isinstance(res, BaseException):
            raise res  # unexpected exception type -> never swallow

        bars = res
        if not bars:
            # Empty iterator == success with zero bars: no write, no manifest row.
            # A never-fetched symbol stays unrecorded; the next run re-queries it
            # from the configured start.
            logger.info(
                "ingest.zero_bars",
                symbol=api_ticker,
                from_date=req.from_date,
                to_date=req.to_date,
            )
            result.zero_bar.append(api_ticker)
            continue

        df = normalize(bars, api_ticker)
        if timeframe == "daily":
            total_bars = _write_and_record_daily(backend, manifest, profile, storage_symbol, api_ticker, df)
        else:
            total_bars = _write_and_record_minute(backend, manifest, profile, storage_symbol, api_ticker, df)

        result.succeeded.append(api_ticker)
        logger.info(
            "ingest.symbol_done",
            symbol=api_ticker,
            timeframe=timeframe,
            bars_fetched=len(bars),
            total_bars=total_bars,
        )

    return result


def _write_and_record_daily(
    backend: StorageBackend,
    manifest: Manifest,
    profile: AssetProfile,
    storage_symbol: str,
    api_ticker: str,
    df: pl.DataFrame,
) -> int:
    """Append the full-history daily file, then record the manifest from it (§6, §8)."""
    key = profile.daily_key(storage_symbol)
    backend.append_parquet(key, df, ["symbol", "timestamp"])
    merged = backend.read_parquet(key)
    _record_manifest(manifest, profile.asset_class, api_ticker, "daily", merged)
    return merged.height


def _write_and_record_minute(
    backend: StorageBackend,
    manifest: Manifest,
    profile: AssetProfile,
    storage_symbol: str,
    api_ticker: str,
    df: pl.DataFrame,
) -> int:
    """Split one frame by UTC year, append each ``{YYYY}.parquet``, then record once.

    Years are written in ascending order (deterministic). The normalized frame is
    filtered per year — no year helper column is ever added, so each on-disk file
    keeps the exact canonical §6.1 schema. The single manifest row is computed from
    ALL of the symbol's year files (read back authoritatively, post-dedupe) and
    upserted only after every year file is committed (write-then-record, SPEC §8).

    The year directory is derived from the profile's key builder with a sentinel
    year (``minute_key(symbol, 0)`` -> ``…/{symbol}/0.parquet``), so ``storage/paths.py``
    stays the single source of truth for the on-disk layout.
    """
    years = sorted(df.get_column("timestamp").dt.year().unique().to_list())
    for year in years:
        year_df = df.filter(pl.col("timestamp").dt.year() == year)
        backend.append_parquet(profile.minute_key(storage_symbol, year), year_df, ["symbol", "timestamp"])

    minute_dir = posixpath.dirname(profile.minute_key(storage_symbol, 0))
    keys = backend.list_keys(minute_dir)
    merged = pl.concat([backend.read_parquet(k) for k in keys])
    _record_manifest(manifest, profile.asset_class, api_ticker, "minute", merged)
    return merged.height


def _record_manifest(
    manifest: Manifest, asset_class: str, api_ticker: str, timeframe: str, merged: pl.DataFrame
) -> None:
    ts = merged.get_column("timestamp")
    manifest.upsert_state(
        asset_class,
        api_ticker,
        timeframe,
        earliest_date=ts.min().date().isoformat(),
        last_complete_date=ts.max().date().isoformat(),
        bar_count=merged.height,
        last_updated_at=datetime.now(timezone.utc).isoformat(),
    )
