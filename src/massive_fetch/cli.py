"""massive-fetch CLI (SPEC §10).

Slice 0 implements ``init`` and ``status``. The remaining commands
(``reference``, ``backfill``, ``corporate-actions``, ``update``, ``validate``)
arrive in later slices.
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Optional

import typer
from dotenv import load_dotenv

from massive_fetch import __version__
from massive_fetch.clients.rest import MassiveAuthError, MassiveRESTClient
from massive_fetch.config import load_config
from massive_fetch.ingest.base import IngestResult
from massive_fetch.ingest.crypto import CryptoIngestResult, ingest_crypto
from massive_fetch.ingest.stocks import ingest_stocks_daily
from massive_fetch.logging_setup import setup_logging
from massive_fetch.reference.universe import (
    MissingUniverseError,
    UniverseUnavailable,
    update_stocks_universe,
)
from massive_fetch.storage import paths
from massive_fetch.storage.backend import LocalBackend
from massive_fetch.storage.manifest import Manifest

# Load a local .env (MASSIVE_API_KEY, etc.) before any command reads the
# environment. Credentials never come from YAML — only from env vars (§4.1).
load_dotenv()

app = typer.Typer(
    name="massive-fetch",
    help="Download historical market data from Massive.com into local Parquet.",
    no_args_is_help=True,
    add_completion=False,
)

ConfigOption = typer.Option(None, "--config", help="Path to a config YAML override.")
VerboseOption = typer.Option(False, "--verbose", "-v", help="Elevate console logging to DEBUG.")


@app.command()
def init(
    config: Optional[Path] = ConfigOption,
    verbose: bool = VerboseOption,
) -> None:
    """Create the data directory tree and an empty manifest."""
    cfg = load_config(config)
    data_dir = cfg.storage.data_dir

    for subdir in paths.DATA_SUBDIRS:
        (data_dir / subdir).mkdir(parents=True, exist_ok=True)

    log = setup_logging(cfg.logging, logs_dir=data_dir / "logs", verbose=verbose)

    manifest_path = data_dir / paths.manifest_key()
    Manifest(manifest_path).initialize()

    log.info("init.complete", data_dir=str(data_dir), manifest=str(manifest_path))
    typer.echo(f"Initialized massive-fetch data directory: {data_dir}")
    typer.echo(f"Manifest: {manifest_path}")


@app.command()
def status(
    config: Optional[Path] = ConfigOption,
    verbose: bool = VerboseOption,
) -> None:
    """Print a summary of locally stored data."""
    cfg = load_config(config)
    data_dir = cfg.storage.data_dir
    logs_dir = data_dir / "logs"
    log = setup_logging(
        cfg.logging,
        logs_dir=logs_dir if logs_dir.exists() else None,
        verbose=verbose,
    )

    typer.echo(f"massive-fetch {__version__}")
    typer.echo(f"Data directory: {data_dir}")

    manifest_path = data_dir / paths.manifest_key()
    if not manifest_path.exists():
        typer.echo("No data yet. Run `massive-fetch init` to create the data directory.")
        log.info("status.no_manifest", data_dir=str(data_dir))
        return

    manifest = Manifest(manifest_path)
    if manifest.is_empty():
        typer.echo("No data yet. Run a `backfill` command to ingest market data.")
        log.info("status.no_data")
        return

    count = manifest.tracked_series_count()
    typer.echo(f"Tracked (symbol, timeframe) series: {count}")
    log.info("status.summary", tracked=count)


reference_app = typer.Typer(
    name="reference",
    help="Refresh reference/universe data (SPEC §5, §10.1).",
    no_args_is_help=True,
)
app.add_typer(reference_app, name="reference")


@reference_app.command("update")
def reference_update(
    scope: str = typer.Option("all", "--scope", help="all | stocks | futures."),
    force: bool = typer.Option(
        False, "--force", help="Bypass the 7-day cache short-circuit and re-scrape."
    ),
    config: Optional[Path] = ConfigOption,
    verbose: bool = VerboseOption,
) -> None:
    """Refresh universe lists (SPEC §10.1).

    Stocks: scrapes Wikipedia (SP500 ∪ NDX), writes ``universe_stocks.parquet`` plus
    the frozen-YAML fallback; a rerun within ``refresh_interval_days`` (7) uses the
    cache. Futures contract discovery is Slice 8 — a no-op here.

    ``--force`` is not in the SPEC §10.1 signature; it is documented in the DESIGN_LOG
    Slice 4 entry pending a later doc-sync.
    """
    if scope not in ("all", "stocks", "futures"):
        typer.echo(
            f"--scope={scope!r} is invalid; expected 'all', 'stocks', or 'futures'.",
            err=True,
        )
        raise typer.Exit(code=3)

    cfg = load_config(config)
    data_dir = cfg.storage.data_dir
    logs_dir = data_dir / "logs"
    log = setup_logging(
        cfg.logging,
        logs_dir=logs_dir if logs_dir.exists() else None,
        verbose=verbose,
    )

    if scope in ("all", "stocks"):
        backend = LocalBackend(
            root=data_dir,
            compression=cfg.storage.parquet_compression,
            row_group_size=cfg.storage.parquet_row_group_size,
        )
        try:
            result = update_stocks_universe(backend=backend, config=cfg, logger=log, force=force)
        except UniverseUnavailable as exc:
            typer.echo(f"Stocks universe unavailable: {exc}", err=True)
            raise typer.Exit(code=2)

        if result.cached:
            typer.echo(
                f"Stocks universe is fresh ({result.ticker_count} tickers, "
                f"{result.generated_at:%Y-%m-%d}) — using cache, no scrape. "
                f"Pass --force to refresh."
            )
        elif result.used_fallback:
            typer.echo(
                f"Wikipedia scrape failed — fell back to frozen YAML: "
                f"{result.ticker_count} tickers (vintage {result.generated_at:%Y-%m-%d})."
            )
        else:
            frozen = cfg.ingest.stocks.frozen_fallback_path
            typer.echo(
                f"Stocks universe updated: {result.ticker_count} tickers "
                f"(SP500 ∪ NDX)\n"
                f"  parquet: {paths.universe_stocks_key()}\n"
                f"  frozen fallback: {frozen}"
            )

    if scope in ("all", "futures"):
        # Futures contract discovery is Slice 8 (SPEC §5.2, §13). No-op for now.
        typer.echo("Futures contract discovery is not implemented yet (Slice 8) — skipping.")
        log.info("reference.futures.skipped_not_implemented")


backfill_app = typer.Typer(
    name="backfill",
    help="Backfill historical OHLCV bars into local Parquet.",
    no_args_is_help=True,
)
app.add_typer(backfill_app, name="backfill")


@backfill_app.command("crypto")
def backfill_crypto(
    timeframe: str = typer.Option("daily", "--timeframe", help="daily | minute."),
    start: Optional[str] = typer.Option(None, "--start", help="YYYY-MM-DD; default config.defaults.crypto_start."),
    end: Optional[str] = typer.Option(None, "--end", help="YYYY-MM-DD; default yesterday (UTC)."),
    symbols: Optional[str] = typer.Option(None, "--symbols", help="Comma-separated bare symbols, e.g. BTC,ETH."),
    concurrency: Optional[int] = typer.Option(None, "--concurrency", help="Max concurrent requests; default from config."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Print the per-symbol fetch plan; make no API calls."),
    config: Optional[Path] = ConfigOption,
    verbose: bool = VerboseOption,
) -> None:
    """Backfill crypto daily or minute bars (SPEC §10.1, §13 Slice 2–3)."""
    cfg = load_config(config)

    if timeframe not in ("daily", "minute"):
        typer.echo(
            f"--timeframe={timeframe!r} is invalid; expected 'daily' or 'minute'.",
            err=True,
        )
        raise typer.Exit(code=3)

    data_dir = cfg.storage.data_dir
    logs_dir = data_dir / "logs"
    log = setup_logging(
        cfg.logging,
        logs_dir=logs_dir if logs_dir.exists() else None,
        verbose=verbose,
    )

    if concurrency is not None:
        cfg.api.max_concurrent_requests = concurrency

    backend = LocalBackend(
        root=data_dir,
        compression=cfg.storage.parquet_compression,
        row_group_size=cfg.storage.parquet_row_group_size,
    )
    manifest = Manifest(data_dir / paths.manifest_key())
    manifest.initialize()  # idempotent; makes backfill safe even before `init`

    bare = [s.strip() for s in symbols.split(",") if s.strip()] if symbols else None
    api_key = os.getenv("MASSIVE_API_KEY")

    async def _run() -> CryptoIngestResult:
        async with MassiveRESTClient(api_key, cfg.api, log) as client:
            return await ingest_crypto(
                config=cfg,
                backend=backend,
                manifest=manifest,
                client=client,
                logger=log,
                timeframe=timeframe,
                symbols=bare,
                start=start,
                end=end,
                dry_run=dry_run,
            )

    try:
        result = asyncio.run(_run())
    except MassiveAuthError as exc:
        typer.echo(f"Authentication failed (is MASSIVE_API_KEY set?): {exc}", err=True)
        raise typer.Exit(code=3)

    if result.dry_run:
        typer.echo("Dry run — no API calls made.")
        for sp in result.plan:
            typer.echo(f"  would fetch {sp.ticker}: {sp.from_date} -> {sp.to_date}")
        for ticker in result.skipped_uptodate:
            typer.echo(f"  up to date, skip: {ticker}")
        return

    typer.echo(
        f"crypto {timeframe}: {len(result.succeeded)} updated, "
        f"{len(result.zero_bar)} no-data, "
        f"{len(result.skipped_uptodate)} up-to-date, "
        f"{len(result.skipped_error)} failed"
    )
    if result.exit_code != 0:
        raise typer.Exit(code=result.exit_code)


@backfill_app.command("stocks")
def backfill_stocks(
    timeframe: str = typer.Option("daily", "--timeframe", help="daily (minute is Slice 6)."),
    start: Optional[str] = typer.Option(None, "--start", help="YYYY-MM-DD; default config.defaults.stocks_daily_start."),
    end: Optional[str] = typer.Option(None, "--end", help="YYYY-MM-DD; default last complete NYSE session."),
    symbols: Optional[str] = typer.Option(None, "--symbols", help="Comma-separated tickers, e.g. AAPL,MSFT. Default: the stocks universe."),
    concurrency: Optional[int] = typer.Option(None, "--concurrency", help="Max concurrent requests; default from config."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Print the per-symbol fetch plan; make no API calls."),
    config: Optional[Path] = ConfigOption,
    verbose: bool = VerboseOption,
) -> None:
    """Backfill stocks daily bars for the universe (SPEC §10.1, §13 Slice 5)."""
    cfg = load_config(config)

    if timeframe != "daily":
        typer.echo(
            f"--timeframe={timeframe!r} is not available: stocks ingestion is daily only "
            "(minute is Slice 6).",
            err=True,
        )
        raise typer.Exit(code=3)

    data_dir = cfg.storage.data_dir
    logs_dir = data_dir / "logs"
    log = setup_logging(
        cfg.logging,
        logs_dir=logs_dir if logs_dir.exists() else None,
        verbose=verbose,
    )

    if concurrency is not None:
        cfg.api.max_concurrent_requests = concurrency

    backend = LocalBackend(
        root=data_dir,
        compression=cfg.storage.parquet_compression,
        row_group_size=cfg.storage.parquet_row_group_size,
    )
    manifest = Manifest(data_dir / paths.manifest_key())
    manifest.initialize()  # idempotent; makes backfill safe even before `init`

    # Normalize --symbols input here, where it belongs: upper-case (so a lower-case
    # arg like brk.b matches the dot-form universe -> BRK.B) AND order-preserving
    # de-dupe (so aapl,AAPL collapses to one) before handing a clean list to the
    # entrypoint. Both are input normalization and live together.
    tickers = (
        list(dict.fromkeys(s.strip().upper() for s in symbols.split(",") if s.strip()))
        if symbols
        else None
    )
    api_key = os.getenv("MASSIVE_API_KEY")

    async def _run() -> IngestResult:
        async with MassiveRESTClient(api_key, cfg.api, log) as client:
            return await ingest_stocks_daily(
                config=cfg,
                backend=backend,
                manifest=manifest,
                client=client,
                logger=log,
                symbols=tickers,
                start=start,
                end=end,
                dry_run=dry_run,
            )

    try:
        result = asyncio.run(_run())
    except MissingUniverseError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=3)
    except MassiveAuthError as exc:
        typer.echo(f"Authentication failed (is MASSIVE_API_KEY set?): {exc}", err=True)
        raise typer.Exit(code=3)

    if result.dry_run:
        typer.echo("Dry run — no API calls made.")
        for sp in result.plan:
            typer.echo(f"  would fetch {sp.ticker}: {sp.from_date} -> {sp.to_date}")
        for ticker in result.skipped_uptodate:
            typer.echo(f"  up to date, skip: {ticker}")
        return

    typer.echo(
        f"stocks daily: {len(result.succeeded)} updated, "
        f"{len(result.zero_bar)} no-data, "
        f"{len(result.skipped_uptodate)} up-to-date, "
        f"{len(result.skipped_error)} failed"
    )
    if result.exit_code != 0:
        raise typer.Exit(code=result.exit_code)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
