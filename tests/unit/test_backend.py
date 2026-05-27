"""Unit tests for storage.backend.LocalBackend (SPEC §6.0.1, §6.1)."""

from __future__ import annotations

import datetime as dt

import polars as pl

from massive_fetch.storage.backend import LocalBackend

_BASE = dt.datetime(2018, 1, 1, tzinfo=dt.timezone.utc)
_KEY = "ohlcv/crypto/daily/BTC.parquet"


def _df(rows: list[tuple[int, float]]) -> pl.DataFrame:
    """Build a tiny canonical-ish frame from (day_offset, volume) rows."""
    return pl.DataFrame(
        {
            "timestamp": pl.Series(
                [_BASE + dt.timedelta(days=d) for d, _ in rows], dtype=pl.Datetime("ns", "UTC")
            ),
            "symbol": pl.Series(["X:BTCUSD"] * len(rows), dtype=pl.Categorical),
            "volume": pl.Series([v for _, v in rows], dtype=pl.Float64),
        }
    )


def test_write_read_roundtrip(tmp_path):
    be = LocalBackend(tmp_path)
    df = _df([(0, 1.0), (1, 2.0)])
    be.write_parquet(_KEY, df)
    out = be.read_parquet(_KEY)
    assert out.sort("timestamp").equals(df.sort("timestamp"))


def test_append_creates_when_absent(tmp_path):
    be = LocalBackend(tmp_path)
    assert not be.exists(_KEY)
    be.append_parquet(_KEY, _df([(0, 1.0), (1, 2.0)]), ["symbol", "timestamp"])
    assert be.exists(_KEY)
    assert be.read_parquet(_KEY).height == 2


def test_append_dedupes_keep_first_and_sorts(tmp_path):
    be = LocalBackend(tmp_path)
    be.append_parquet(_KEY, _df([(0, 1.0), (1, 2.0)]), ["symbol", "timestamp"])
    # day 1 overlaps (re-fetched vol 99.0); day 2 is new; out of order to test sort.
    be.append_parquet(_KEY, _df([(2, 3.0), (1, 99.0)]), ["symbol", "timestamp"])
    out = be.read_parquet(_KEY)
    assert out.height == 3
    by_day = dict(zip((t.day for t in out["timestamp"].to_list()), out["volume"].to_list()))
    # keep_first: existing day-1 row (2.0) wins over the re-fetched 99.0.
    assert by_day == {1: 1.0, 2: 2.0, 3: 3.0}
    ts = out["timestamp"].to_list()
    assert ts == sorted(ts)


def test_atomic_write_leaves_no_temp(tmp_path):
    be = LocalBackend(tmp_path)
    be.write_parquet(_KEY, _df([(0, 1.0)]))
    leftovers = list((tmp_path / "ohlcv/crypto/daily").glob(".*tmp"))
    assert leftovers == []


def test_exists_size_listkeys_localpath(tmp_path):
    be = LocalBackend(tmp_path)
    be.write_parquet(_KEY, _df([(0, 1.0)]))
    assert be.exists(_KEY)
    assert be.size_bytes(_KEY) > 0
    assert _KEY in be.list_keys("ohlcv/crypto/daily")
    assert be.local_path(_KEY) == tmp_path.resolve() / _KEY


def test_delete(tmp_path):
    be = LocalBackend(tmp_path)
    be.write_parquet(_KEY, _df([(0, 1.0)]))
    be.delete(_KEY)
    assert not be.exists(_KEY)
