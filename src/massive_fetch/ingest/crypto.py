"""Crypto ingestion — daily and minute (SPEC §8.3, §13 Slice 2 + Slice 3).

A thin asset profile over the shared ingestion core (``ingest/base.py``): it maps
each bare symbol to its Massive ticker and Parquet keys, resolves the crypto
``target_end``, and delegates everything else (planning, manifest short-circuit /
resume, fan-out, normalize, append+dedupe, write-then-record) to ``run_ingest``.

- **daily** — one file per symbol (``crypto_daily_key``), full history.
- **minute** — year-partitioned per symbol (``crypto_minute_key(symbol, year)``).

Crypto trades 24/7, so there is no market calendar and "yesterday" is measured in
**UTC**, not ET (SPEC §8.3). Two identifiers per symbol: the bare symbol (``BTC``)
is the file/storage key, while the Massive ticker (``X:BTCUSD``) is the API arg,
the ``symbol`` column value, and the manifest key (SPEC §5.3, §6.1, §6.3).
"""

from __future__ import annotations

import structlog

from massive_fetch.clients.rest import MassiveRESTClient
from massive_fetch.config import AppConfig
from massive_fetch.ingest.base import (
    AssetProfile,
    IngestResult,
    SymbolPlan,
    _yesterday_utc,
    run_ingest,
)
from massive_fetch.storage import paths
from massive_fetch.storage.backend import StorageBackend
from massive_fetch.storage.manifest import Manifest

_ASSET_CLASS = "crypto"

# Back-compat aliases: the CLI (and prior callers) import these names from here.
CryptoIngestResult = IngestResult
__all__ = ["CryptoIngestResult", "SymbolPlan", "ingest_crypto", "ingest_crypto_daily"]


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
) -> IngestResult:
    """Backfill/update crypto bars at ``timeframe``. See the module docstring."""
    quote = config.ingest.crypto.quote_currency
    bare_symbols = symbols if symbols is not None else config.ingest.crypto.symbols
    profile = AssetProfile(
        asset_class=_ASSET_CLASS,
        # bare symbol is the storage key; X:{SYMBOL}{QUOTE} is the api_ticker (§5.3).
        to_identifiers=lambda bare: (bare, f"X:{bare}{quote}"),
        daily_key=paths.crypto_daily_key,
        minute_key=paths.crypto_minute_key,
    )
    return await run_ingest(
        config=config,
        backend=backend,
        manifest=manifest,
        client=client,
        logger=logger,
        profile=profile,
        timeframe=timeframe,
        entries=bare_symbols,
        default_start=start or config.defaults.crypto_start,
        target_end=end or _yesterday_utc(),  # crypto: yesterday UTC (24/7, no calendar)
        dry_run=dry_run,
    )


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
) -> IngestResult:
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
