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
from massive_fetch.ingest.crypto import CryptoIngestResult, ingest_crypto
from massive_fetch.logging_setup import setup_logging
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


def main() -> None:
    app()


if __name__ == "__main__":
    main()
