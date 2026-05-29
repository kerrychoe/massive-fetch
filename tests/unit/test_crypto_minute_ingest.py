"""Unit tests for ingest.crypto minute path — SDK fully mocked (SPEC §13 Slice 3).

Covers the three fixed acceptance properties (year-partitioned file produced;
rerun is a no-op; mid-run kill within a single year resumes gap-free/dupe-free),
plus year-split correctness and a multi-year crash recovery. No network.
"""

from __future__ import annotations

import datetime as dt

import polars as pl
import pytest
from massive.rest.models import Agg

from massive_fetch.config import AppConfig
from massive_fetch.ingest.crypto import ingest_crypto
from massive_fetch.storage import paths
from massive_fetch.storage.backend import LocalBackend
from massive_fetch.storage.manifest import Manifest
from massive_fetch.transform.normalize import CANONICAL_SCHEMA

_MIN_MS = 60_000


def _ms(*args: int) -> int:
    """UTC datetime -> Unix milliseconds."""
    return int(dt.datetime(*args, tzinfo=dt.timezone.utc).timestamp() * 1000)


def _minute_aggs(start_ms: int, n: int) -> list[Agg]:
    """``n`` contiguous 1-minute BTC bars from ``start_ms`` (60_000 ms apart)."""
    return [
        Agg.from_dict(
            {"o": 1.0, "h": 2.0, "l": 0.5, "c": 1.5, "v": 10.0 + i, "vw": 1.2, "t": start_ms + i * _MIN_MS, "n": 5}
        )
        for i in range(n)
    ]


# A clean contiguous block inside one UTC day (2024-03-01 00:00..02:59, 180 bars).
_MAR1 = _ms(2024, 3, 1)
# A cross-year block: 2024-12-31 23:55..2025-01-01 00:04 (5 bars each side).
_NYE = _ms(2024, 12, 31, 23, 55)


@pytest.fixture
def env(tmp_path):
    """A backend + initialized manifest under tmp_path."""
    backend = LocalBackend(tmp_path)
    manifest = Manifest(tmp_path / "manifest.sqlite")
    manifest.initialize()
    return backend, manifest


def _client(api_config, rec_logger):
    from massive_fetch.clients.rest import MassiveRESTClient

    return MassiveRESTClient("key", api_config, rec_logger)


def _minute_key(year: int) -> str:
    return paths.crypto_minute_key("BTC", year)


async def _run(client, backend, manifest, rec_logger, **kw):
    return await ingest_crypto(
        config=AppConfig(), backend=backend, manifest=manifest, client=client,
        logger=rec_logger, timeframe="minute", symbols=["BTC"], **kw,
    )


# --- (A) year-partitioned file produced ------------------------------------


async def test_minute_writes_year_partition_file(api_config, rec_logger, fake_sdk, env):
    backend, manifest = env
    fake_sdk.list_aggs.return_value = _minute_aggs(_MAR1, 180)

    async with _client(api_config, rec_logger) as client:
        result = await _run(client, backend, manifest, rec_logger, start="2024-03-01", end="2024-03-01")

    assert result.exit_code == 0
    assert result.succeeded == ["X:BTCUSD"]

    out = backend.read_parquet(_minute_key(2024))
    # Canonical §6.1 columns exactly — proves no year helper column was persisted.
    assert out.columns == list(CANONICAL_SCHEMA.keys())
    assert out.schema["timestamp"] == pl.Datetime("ns", "UTC")
    assert out.schema["volume"] == pl.Float64
    assert out.height == 180
    assert out["symbol"].to_list() == ["X:BTCUSD"] * 180

    row = manifest.get_state("crypto", "X:BTCUSD", "minute")
    assert row["bar_count"] == 180
    assert row["earliest_date"] == "2024-03-01"
    assert row["last_complete_date"] == "2024-03-01"
    assert manifest.tracked_series_count() == 1


# --- (B) re-running adds nothing -------------------------------------------


async def test_rerun_is_noop(api_config, rec_logger, fake_sdk, env):
    backend, manifest = env
    fake_sdk.list_aggs.return_value = _minute_aggs(_MAR1, 180)

    async with _client(api_config, rec_logger) as client:
        await _run(client, backend, manifest, rec_logger, start="2024-03-01", end="2024-03-01")
        before = backend.read_parquet(_minute_key(2024)).height
        row_before = manifest.get_state("crypto", "X:BTCUSD", "minute")

        fake_sdk.list_aggs.reset_mock()
        r2 = await _run(client, backend, manifest, rec_logger, start="2024-03-01", end="2024-03-01")

    assert fake_sdk.list_aggs.call_count == 0  # short-circuited BEFORE any SDK call
    assert r2.skipped_uptodate == ["X:BTCUSD"]
    assert r2.succeeded == []
    assert backend.read_parquet(_minute_key(2024)).height == before
    assert manifest.get_state("crypto", "X:BTCUSD", "minute") == row_before


# --- (C) mid-year kill within a single year: gap-free + dupe-free ----------


async def test_crash_between_write_and_record_then_resume(
    api_config, rec_logger, fake_sdk, env, monkeypatch
):
    backend, manifest = env

    # Simulate SIGKILL in the write-then-record window: the year-file Parquet is
    # committed, but the manifest upsert never lands (raises the first time).
    orig_upsert = manifest.upsert_state
    calls = {"n": 0}

    def flaky_upsert(*a, **k):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("simulated crash after parquet write, before manifest record")
        return orig_upsert(*a, **k)

    monkeypatch.setattr(manifest, "upsert_state", flaky_upsert)

    async with _client(api_config, rec_logger) as client:
        # Run 1: only the first 100 of 180 bars are fetched, then the crash.
        fake_sdk.list_aggs.return_value = _minute_aggs(_MAR1, 100)
        with pytest.raises(RuntimeError):
            await _run(client, backend, manifest, rec_logger, start="2024-03-01", end="2024-03-01")

        # Partial data is on disk; the manifest has NO row (record never happened).
        assert backend.read_parquet(_minute_key(2024)).height == 100
        assert manifest.get_state("crypto", "X:BTCUSD", "minute") is None

        # Run 2: manifest empty -> resume from start, re-fetch the full 180 (the
        # first 100 overlap). append_parquet dedupes on (symbol, timestamp).
        fake_sdk.list_aggs.return_value = _minute_aggs(_MAR1, 180)
        await _run(client, backend, manifest, rec_logger, start="2024-03-01", end="2024-03-01")

    out = backend.read_parquet(_minute_key(2024))
    ts = out["timestamp"].to_list()
    assert out.height == 180
    assert out["timestamp"].n_unique() == 180  # dupe-free
    assert ts == sorted(ts)  # post-resume merged file is sorted ascending
    diffs = out["timestamp"].diff().drop_nulls().dt.total_milliseconds().to_list()
    assert all(d == _MIN_MS for d in diffs)  # gap-free: every consecutive minute

    row = manifest.get_state("crypto", "X:BTCUSD", "minute")
    assert row["bar_count"] == 180
    assert row["last_complete_date"] == "2024-03-01"


# --- year-split correctness on a synthetic multi-year fixture --------------


async def test_year_boundary_split(api_config, rec_logger, fake_sdk, env):
    backend, manifest = env
    fake_sdk.list_aggs.return_value = _minute_aggs(_NYE, 10)  # 5 in 2024, 5 in 2025

    async with _client(api_config, rec_logger) as client:
        await _run(client, backend, manifest, rec_logger, start="2024-12-31", end="2025-01-01")

    f24 = backend.read_parquet(_minute_key(2024))
    f25 = backend.read_parquet(_minute_key(2025))

    assert f24.height == 5 and f25.height == 5
    assert f24.columns == list(CANONICAL_SCHEMA.keys())  # no helper column
    assert all(t.year == 2024 for t in f24["timestamp"].to_list())
    assert all(t.year == 2025 for t in f25["timestamp"].to_list())
    assert f24.height + f25.height == 10  # nothing lost/duplicated at the boundary

    row = manifest.get_state("crypto", "X:BTCUSD", "minute")
    assert row["earliest_date"] == "2024-12-31"
    assert row["last_complete_date"] == "2025-01-01"
    assert row["bar_count"] == 10
    assert manifest.tracked_series_count() == 1


# --- multi-year crash between year files, then resume ----------------------


async def test_multiyear_crash_between_year_files_then_resume(
    api_config, rec_logger, fake_sdk, env, monkeypatch
):
    backend, manifest = env
    fake_sdk.list_aggs.return_value = _minute_aggs(_NYE, 10)  # 5 in 2024, 5 in 2025

    # Crash after the FIRST year file (2024, ascending order) is written, before
    # the second (2025) and the manifest record.
    orig_append = backend.append_parquet
    calls = {"n": 0}

    def flaky_append(*a, **k):
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("simulated crash before second year file")
        return orig_append(*a, **k)

    monkeypatch.setattr(backend, "append_parquet", flaky_append)

    async with _client(api_config, rec_logger) as client:
        with pytest.raises(RuntimeError):
            await _run(client, backend, manifest, rec_logger, start="2024-12-31", end="2025-01-01")

        # 2024 committed; 2025 absent; no manifest row.
        assert backend.exists(_minute_key(2024))
        assert not backend.exists(_minute_key(2025))
        assert manifest.get_state("crypto", "X:BTCUSD", "minute") is None

        # Resume: re-fetch the full cross-year fixture; 2024 dedupes to a no-op,
        # 2025 is written, one manifest row recorded across both years.
        await _run(client, backend, manifest, rec_logger, start="2024-12-31", end="2025-01-01")

    assert backend.read_parquet(_minute_key(2024)).height == 5
    assert backend.read_parquet(_minute_key(2025)).height == 5
    row = manifest.get_state("crypto", "X:BTCUSD", "minute")
    assert row["earliest_date"] == "2024-12-31"
    assert row["last_complete_date"] == "2025-01-01"
    assert row["bar_count"] == 10
    assert manifest.tracked_series_count() == 1


# --- resume start = last_complete_date + 1 day (SPEC §6.3) -----------------


async def test_resume_from_manifest_minute(api_config, rec_logger, fake_sdk, env):
    backend, manifest = env
    manifest.upsert_state(
        "crypto", "X:BTCUSD", "minute",
        earliest_date="2024-03-01", last_complete_date="2024-03-01",
        bar_count=180, last_updated_at="2026-01-01T00:00:00+00:00",
    )
    fake_sdk.list_aggs.return_value = _minute_aggs(_ms(2024, 3, 2), 60)

    async with _client(api_config, rec_logger) as client:
        await _run(client, backend, manifest, rec_logger, start="2024-03-01", end="2024-03-02")

    args, _kwargs = fake_sdk.list_aggs.call_args
    assert args[0] == "X:BTCUSD"     # ticker construction X:{BASE}{QUOTE}
    assert args[2] == "minute"       # timespan token
    assert args[3] == "2024-03-02"   # resume = last_complete_date + 1 (SPEC §6.3)
    assert args[4] == "2024-03-02"   # target end


# --- empty iterator: no file, no manifest row ------------------------------


async def test_zero_bars_no_file_no_row(api_config, rec_logger, fake_sdk, env):
    backend, manifest = env
    fake_sdk.list_aggs.return_value = []

    async with _client(api_config, rec_logger) as client:
        result = await _run(client, backend, manifest, rec_logger, start="2024-03-01", end="2024-03-01")

    assert result.zero_bar == ["X:BTCUSD"]
    assert result.succeeded == []
    assert result.exit_code == 0
    assert backend.list_keys("ohlcv/crypto/minute/BTC") == []
    assert manifest.get_state("crypto", "X:BTCUSD", "minute") is None
