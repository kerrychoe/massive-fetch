"""Unit tests for ingest.stocks — SDK fully mocked, no network (SPEC §13 Slice 5).

Stocks daily reuses the shared core (``ingest/base.py``); these tests cover the
stocks-specific surface: universe loading + the missing/empty-universe error path,
the dot-ticker flow, the order-preserving symbol dedupe, cross-asset manifest
isolation, a deterministic "429 storm" exit-code contract, and the inherited
resume / append-dedupe / write-then-record / zero-bar invariants exercised through
the stocks entrypoint. Plus a few CLI checks (dedupe/upper-case, minute rejection,
missing-universe exit code).
"""

from __future__ import annotations

import datetime as dt

import polars as pl
import pytest
from massive.rest.models import Agg
from typer.testing import CliRunner
from urllib3.exceptions import MaxRetryError

from massive_fetch.cli import app
from massive_fetch.clients.rest import MassiveRESTClient
from massive_fetch.config import AppConfig
from massive_fetch.ingest.base import IngestResult
from massive_fetch.ingest.stocks import ingest_stocks_daily
from massive_fetch.reference.universe import MissingUniverseError, build_universe_df
from massive_fetch.storage import paths
from massive_fetch.storage.backend import LocalBackend
from massive_fetch.storage.manifest import Manifest

_DAY_MS = 86_400_000


def _ms(*args: int) -> int:
    """UTC datetime -> Unix milliseconds."""
    return int(dt.datetime(*args, tzinfo=dt.timezone.utc).timestamp() * 1000)


def _daily_aggs(start_ms: int, n: int) -> list[Agg]:
    """``n`` contiguous daily bars from ``start_ms`` (one UTC day apart)."""
    return [
        Agg.from_dict(
            {"o": 1.0, "h": 2.0, "l": 0.5, "c": 1.5, "v": 10.0 + i, "vw": 1.2, "t": start_ms + i * _DAY_MS, "n": 5}
        )
        for i in range(n)
    ]


_JAN2 = _ms(2020, 1, 2)  # an arbitrary fixed start; synthetic bars, mocked SDK


@pytest.fixture
def env(tmp_path):
    """A backend + initialized manifest under tmp_path."""
    backend = LocalBackend(tmp_path)
    manifest = Manifest(tmp_path / "manifest.sqlite")
    manifest.initialize()
    return backend, manifest


def _client(api_config, rec_logger) -> MassiveRESTClient:
    return MassiveRESTClient("key", api_config, rec_logger)


def _seed_universe(backend, tickers: list[str]) -> None:
    df = build_universe_df(
        tickers, source="wikipedia", generated_at=dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
    )
    backend.write_parquet(paths.universe_stocks_key(), df)


async def _run(client, backend, manifest, rec_logger, **kw):
    return await ingest_stocks_daily(
        config=AppConfig(), backend=backend, manifest=manifest, client=client, logger=rec_logger, **kw
    )


# --- explicit symbols: end-to-end write + manifest -------------------------


async def test_symbols_write_files_and_manifest(api_config, rec_logger, fake_sdk, env):
    backend, manifest = env
    fake_sdk.list_aggs.return_value = _daily_aggs(_JAN2, 5)  # 2020-01-02 .. 01-06

    async with _client(api_config, rec_logger) as client:
        result = await _run(
            client, backend, manifest, rec_logger,
            symbols=["AAPL", "MSFT"], start="2020-01-02", end="2020-01-08",
        )

    assert result.exit_code == 0
    assert set(result.succeeded) == {"AAPL", "MSFT"}
    assert backend.exists(paths.stocks_daily_key("AAPL"))

    df = backend.read_parquet(paths.stocks_daily_key("AAPL"))
    assert df.height == 5
    assert df["symbol"].to_list() == ["AAPL"] * 5  # symbol column == api_ticker

    row = manifest.get_state("stocks", "AAPL", "daily")
    assert row["bar_count"] == 5
    assert row["earliest_date"] == "2020-01-02"
    assert row["last_complete_date"] == "2020-01-06"
    assert manifest.tracked_series_count() == 2


# --- universe loading + bypass ---------------------------------------------


async def test_loads_universe_from_parquet(api_config, rec_logger, fake_sdk, env):
    backend, manifest = env
    _seed_universe(backend, ["AAPL", "MSFT", "NVDA"])
    fake_sdk.list_aggs.return_value = _daily_aggs(_JAN2, 3)

    async with _client(api_config, rec_logger) as client:
        result = await _run(client, backend, manifest, rec_logger, start="2020-01-02", end="2020-01-08")

    assert set(result.succeeded) == {"AAPL", "MSFT", "NVDA"}
    assert manifest.tracked_series_count() == 3


async def test_explicit_symbols_bypass_universe_read(api_config, rec_logger, fake_sdk, env):
    backend, manifest = env
    # No universe parquet written; explicit symbols must NOT trigger the load.
    fake_sdk.list_aggs.return_value = _daily_aggs(_JAN2, 3)

    async with _client(api_config, rec_logger) as client:
        result = await _run(client, backend, manifest, rec_logger, symbols=["AAPL"], start="2020-01-02", end="2020-01-08")

    assert result.succeeded == ["AAPL"]


async def test_missing_universe_absent_raises(api_config, rec_logger, fake_sdk, env):
    backend, manifest = env  # no universe parquet
    async with _client(api_config, rec_logger) as client:
        with pytest.raises(MissingUniverseError):
            await _run(client, backend, manifest, rec_logger, start="2020-01-02", end="2020-01-08")


async def test_missing_universe_zero_rows_raises(api_config, rec_logger, fake_sdk, env):
    backend, manifest = env
    # File present but ZERO data rows -> must raise, not silently ingest nothing.
    empty = pl.DataFrame(
        schema={"ticker": pl.String, "source": pl.String, "generated_at": pl.Datetime("us", "UTC")}
    )
    backend.write_parquet(paths.universe_stocks_key(), empty)

    async with _client(api_config, rec_logger) as client:
        with pytest.raises(MissingUniverseError):
            await _run(client, backend, manifest, rec_logger, start="2020-01-02", end="2020-01-08")


# --- dot ticker: filename + symbol column ----------------------------------


async def test_dot_ticker_brkb_flows_to_filename_and_symbol_column(api_config, rec_logger, fake_sdk, env):
    backend, manifest = env
    fake_sdk.list_aggs.return_value = _daily_aggs(_JAN2, 4)

    async with _client(api_config, rec_logger) as client:
        result = await _run(client, backend, manifest, rec_logger, symbols=["BRK.B"], start="2020-01-02", end="2020-01-08")

    assert result.succeeded == ["BRK.B"]
    assert paths.stocks_daily_key("BRK.B") == "ohlcv/stocks/daily/BRK.B.parquet"
    assert backend.exists("ohlcv/stocks/daily/BRK.B.parquet")
    df = backend.read_parquet(paths.stocks_daily_key("BRK.B"))
    assert df["symbol"].to_list() == ["BRK.B"] * 4
    assert manifest.get_state("stocks", "BRK.B", "daily") is not None


# --- resume from last_complete_date + 1 ------------------------------------


async def test_resume_from_manifest(api_config, rec_logger, fake_sdk, env):
    backend, manifest = env
    manifest.upsert_state(
        "stocks", "AAPL", "daily",
        earliest_date="2020-01-02", last_complete_date="2020-01-06",
        bar_count=5, last_updated_at="2026-01-01T00:00:00+00:00",
    )
    fake_sdk.list_aggs.return_value = _daily_aggs(_ms(2020, 1, 7), 2)

    async with _client(api_config, rec_logger) as client:
        await _run(client, backend, manifest, rec_logger, symbols=["AAPL"], start="2020-01-02", end="2020-01-08")

    args, _ = fake_sdk.list_aggs.call_args
    assert args[0] == "AAPL"          # stock symbol IS its Massive ticker (no X:)
    assert args[3] == "2020-01-07"    # resume = last_complete_date + 1 (SPEC §6.3)
    assert args[4] == "2020-01-08"    # target end


# --- append + dedupe across reruns -----------------------------------------


async def test_append_dedupe_across_reruns(api_config, rec_logger, fake_sdk, env):
    backend, manifest = env

    async with _client(api_config, rec_logger) as client:
        fake_sdk.list_aggs.return_value = _daily_aggs(_JAN2, 5)  # 01-02 .. 01-06
        await _run(client, backend, manifest, rec_logger, symbols=["AAPL"], start="2020-01-02", end="2020-01-08")

        # Re-fetch overlaps 01-06; append_parquet dedupes on (symbol, timestamp).
        fake_sdk.list_aggs.return_value = _daily_aggs(_ms(2020, 1, 6), 4)  # 01-06 .. 01-09
        await _run(client, backend, manifest, rec_logger, symbols=["AAPL"], start="2020-01-02", end="2020-01-10")

    df = backend.read_parquet(paths.stocks_daily_key("AAPL"))
    assert df.height == 8                        # 01-02 .. 01-09 unique
    assert df["timestamp"].n_unique() == 8       # dupe-free
    row = manifest.get_state("stocks", "AAPL", "daily")
    assert row["bar_count"] == 8
    assert row["last_complete_date"] == "2020-01-09"


# --- write-then-record crash -> dupe-free recovery -------------------------


async def test_crash_between_write_and_record_then_resume(api_config, rec_logger, fake_sdk, env, monkeypatch):
    backend, manifest = env
    orig_upsert = manifest.upsert_state
    calls = {"n": 0}

    def flaky_upsert(*a, **k):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("simulated crash after parquet write, before manifest record")
        return orig_upsert(*a, **k)

    monkeypatch.setattr(manifest, "upsert_state", flaky_upsert)

    async with _client(api_config, rec_logger) as client:
        fake_sdk.list_aggs.return_value = _daily_aggs(_JAN2, 3)
        with pytest.raises(RuntimeError):
            await _run(client, backend, manifest, rec_logger, symbols=["AAPL"], start="2020-01-02", end="2020-01-08")

        # Parquet on disk; manifest has NO row (record never landed).
        assert backend.read_parquet(paths.stocks_daily_key("AAPL")).height == 3
        assert manifest.get_state("stocks", "AAPL", "daily") is None

        # Resume: manifest empty -> re-fetch from start; append dedupes.
        fake_sdk.list_aggs.return_value = _daily_aggs(_JAN2, 3)
        await _run(client, backend, manifest, rec_logger, symbols=["AAPL"], start="2020-01-02", end="2020-01-08")

    df = backend.read_parquet(paths.stocks_daily_key("AAPL"))
    assert df.height == 3
    assert df["timestamp"].n_unique() == 3
    row = manifest.get_state("stocks", "AAPL", "daily")
    assert row["bar_count"] == 3


# --- empty iterator = zero-bar (no write, no manifest row) ------------------


async def test_empty_iterator_zero_bar(api_config, rec_logger, fake_sdk, env):
    backend, manifest = env
    fake_sdk.list_aggs.return_value = []

    async with _client(api_config, rec_logger) as client:
        result = await _run(client, backend, manifest, rec_logger, symbols=["AAPL"], start="2020-01-02", end="2020-01-08")

    assert result.zero_bar == ["AAPL"]
    assert result.succeeded == []
    assert result.exit_code == 0
    assert not backend.exists(paths.stocks_daily_key("AAPL"))
    assert manifest.get_state("stocks", "AAPL", "daily") is None


# --- S3: deterministic 429 storm -> exact exit code ------------------------


async def test_429_storm_partial_failure_exit_1(api_config, rec_logger, fake_sdk, env):
    backend, manifest = env
    fail = {"AAA", "BBB", "CCC"}

    def side_effect(ticker, *a, **k):
        if ticker in fail:
            raise MaxRetryError(pool=None, url="/v2/aggs", reason=None)  # -> MassiveRetriesExhausted
        return _daily_aggs(_JAN2, 3)

    fake_sdk.list_aggs.side_effect = side_effect

    async with _client(api_config, rec_logger) as client:
        result = await _run(
            client, backend, manifest, rec_logger,
            symbols=["AAA", "DDD", "BBB", "EEE", "CCC"], start="2020-01-02", end="2020-01-08",
        )

    assert result.exit_code == 1                                  # EXACTLY 1 (partial)
    assert set(result.skipped_error) == {"AAA", "BBB", "CCC"}
    assert set(result.succeeded) == {"DDD", "EEE"}
    assert manifest.get_state("stocks", "DDD", "daily") is not None
    assert manifest.get_state("stocks", "EEE", "daily") is not None
    assert manifest.get_state("stocks", "AAA", "daily") is None
    assert manifest.tracked_series_count() == 2


async def test_429_storm_total_failure_exit_2(api_config, rec_logger, fake_sdk, env):
    backend, manifest = env
    fake_sdk.list_aggs.side_effect = MaxRetryError(pool=None, url="/v2/aggs", reason=None)

    async with _client(api_config, rec_logger) as client:
        result = await _run(
            client, backend, manifest, rec_logger,
            symbols=["AAA", "BBB", "CCC"], start="2020-01-02", end="2020-01-08",
        )

    assert result.exit_code == 2                                  # EXACTLY 2 (total)
    assert set(result.skipped_error) == {"AAA", "BBB", "CCC"}
    assert result.succeeded == []
    assert manifest.tracked_series_count() == 0


# --- S4: cross-asset isolation in a shared manifest DB ---------------------


async def test_cross_asset_isolation_shared_manifest(api_config, rec_logger, fake_sdk, env):
    backend, manifest = env
    # Pre-existing crypto row in the SAME manifest DB.
    manifest.upsert_state(
        "crypto", "X:BTCUSD", "daily",
        earliest_date="2018-01-01", last_complete_date="2018-01-03",
        bar_count=3, last_updated_at="2026-01-01T00:00:00+00:00",
    )
    before = manifest.get_state("crypto", "X:BTCUSD", "daily")
    fake_sdk.list_aggs.return_value = _daily_aggs(_JAN2, 5)

    async with _client(api_config, rec_logger) as client:
        await _run(client, backend, manifest, rec_logger, symbols=["AAPL"], start="2020-01-02", end="2020-01-08")

    assert manifest.get_state("stocks", "AAPL", "daily") is not None      # stocks recorded
    assert manifest.get_state("crypto", "X:BTCUSD", "daily") == before    # crypto untouched
    assert manifest.get_state("crypto", "AAPL", "daily") is None          # no namespace bleed


# --- CLI: dedupe + upper-case, minute rejection, missing-universe exit ------


def _cli_config(tmp_path) -> str:
    cfg = tmp_path / "config.yaml"
    cfg.write_text(f"storage:\n  data_dir: {tmp_path / 'data'}\n")
    return str(cfg)


def _text(result) -> str:
    out = result.output or ""
    try:
        out += result.stderr or ""
    except (ValueError, Exception):
        pass
    return out


def test_cli_uppercases_and_dedupes_symbols(tmp_path, monkeypatch, fake_sdk):
    """The CLI normalizes --symbols (upper-case + order-preserving dedupe) BEFORE the
    entrypoint sees them. Pass condition: the spy on ingest_stocks_daily captured
    exactly ["AAPL"] for input "aapl,AAPL" — NOT merely a clean exit code."""
    captured = {}

    async def spy(**kw):
        captured["symbols"] = kw.get("symbols")
        return IngestResult(dry_run=True)

    monkeypatch.setattr("massive_fetch.cli.ingest_stocks_daily", spy)

    result = CliRunner().invoke(
        app,
        ["backfill", "stocks", "--symbols", "aapl,AAPL", "--start", "2020-01-02",
         "--end", "2020-01-08", "--dry-run", "--config", _cli_config(tmp_path)],
    )

    assert captured["symbols"] == ["AAPL"], (
        f"spy saw {captured.get('symbols')!r}; cli output: {_text(result)}"
    )


def test_cli_rejects_minute_timeframe(tmp_path):
    result = CliRunner().invoke(
        app, ["backfill", "stocks", "--timeframe", "minute", "--config", _cli_config(tmp_path)]
    )
    assert result.exit_code == 3
    assert "minute is Slice 6" in _text(result)


def test_cli_missing_universe_exit_3(tmp_path, fake_sdk):
    # No --symbols and no universe parquet -> MissingUniverseError -> exit 3.
    result = CliRunner().invoke(
        app,
        ["backfill", "stocks", "--start", "2020-01-02", "--end", "2020-01-08",
         "--config", _cli_config(tmp_path)],
    )
    assert result.exit_code == 3
    assert "reference update --scope stocks" in _text(result)
