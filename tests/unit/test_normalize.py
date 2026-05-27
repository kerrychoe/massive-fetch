"""Unit tests for transform.normalize — canonical §6.1 schema."""

from __future__ import annotations

from datetime import datetime, timezone

import polars as pl
from massive.rest.models import Agg

from massive_fetch.transform.normalize import CANONICAL_SCHEMA, normalize

_DAY_MS = 86_400_000
_JAN1_2018_MS = 1_514_764_800_000  # 2018-01-01 00:00:00 UTC


def _crypto_aggs() -> list[Agg]:
    """3 daily BTC bars, fractional volume, no ``otc`` key (crypto)."""
    raw = [
        {"o": 13800.0, "h": 14000.0, "l": 13000.0, "c": 13900.0, "v": 1234.56, "vw": 13650.1, "t": _JAN1_2018_MS, "n": 100},
        {"o": 13900.0, "h": 15400.0, "l": 13800.0, "c": 14900.0, "v": 2222.22, "vw": 14550.5, "t": _JAN1_2018_MS + _DAY_MS, "n": 200},
        {"o": 14900.0, "h": 15500.0, "l": 14200.0, "c": 15100.0, "v": 999.99, "vw": 15000.0, "t": _JAN1_2018_MS + 2 * _DAY_MS, "n": 150},
    ]
    return [Agg.from_dict(d) for d in raw]


def test_schema_matches_canonical_exactly():
    df = normalize(_crypto_aggs(), "X:BTCUSD")
    assert df.columns == list(CANONICAL_SCHEMA.keys())
    for col, dtype in CANONICAL_SCHEMA.items():
        assert df.schema[col] == dtype, f"{col}: {df.schema[col]} != {dtype}"


def test_volume_is_float64_and_fractional_preserved():
    df = normalize(_crypto_aggs(), "X:BTCUSD")
    assert df.schema["volume"] == pl.Float64
    assert df["volume"].to_list() == [1234.56, 2222.22, 999.99]


def test_symbol_injected_as_categorical_ticker():
    df = normalize(_crypto_aggs(), "X:BTCUSD")
    assert df.schema["symbol"] == pl.Categorical
    assert df["symbol"].to_list() == ["X:BTCUSD"] * 3


def test_timestamp_ms_to_ns_utc():
    df = normalize(_crypto_aggs(), "X:BTCUSD")
    assert df.schema["timestamp"] == pl.Datetime("ns", "UTC")
    assert df["timestamp"][0] == datetime(2018, 1, 1, tzinfo=timezone.utc)


def test_otc_all_null_for_crypto():
    df = normalize(_crypto_aggs(), "X:BTCUSD")
    assert df.schema["otc"] == pl.Boolean
    assert df["otc"].null_count() == df.height


def test_sorted_ascending_by_timestamp():
    df = normalize(list(reversed(_crypto_aggs())), "X:BTCUSD")
    ts = df["timestamp"].to_list()
    assert ts == sorted(ts)


def test_empty_input_yields_empty_typed_frame():
    df = normalize([], "X:BTCUSD")
    assert df.height == 0
    assert df.columns == list(CANONICAL_SCHEMA.keys())
    for col, dtype in CANONICAL_SCHEMA.items():
        assert df.schema[col] == dtype


def test_mixed_int_and_float_json_numbers():
    """Regression: the SDK keeps round JSON numbers as ``int`` (e.g. volume 70295)
    and others as ``float`` (70295.78). The mixed list must still normalize to the
    canonical float dtypes without error (caught by the live BTC backfill)."""
    raw = [
        # round numbers -> Python ints out of Agg.from_dict
        {"o": 14000, "h": 14000, "l": 13000, "c": 13900, "v": 70295, "vw": 13650, "t": _JAN1_2018_MS, "n": 100},
        # fractional -> floats
        {"o": 13900.5, "h": 15400.25, "l": 13800.1, "c": 14900.75, "v": 70295.78, "vw": 14550.5, "t": _JAN1_2018_MS + _DAY_MS, "n": 200},
    ]
    df = normalize([Agg.from_dict(d) for d in raw], "X:BTCUSD")
    assert df.schema["open"] == pl.Float64
    assert df.schema["volume"] == pl.Float64
    assert df["volume"].to_list() == [70295.0, 70295.78]
    assert df["open"].to_list() == [14000.0, 13900.5]


def test_aapl_fixture_volume_and_transactions(sample_aggs):
    """The committed AAPL minute fixture has fractional volume and transactions."""
    df = normalize(sample_aggs, "AAPL")
    assert df.schema["volume"] == pl.Float64
    assert df["volume"][0] == 25370.68
    assert df.schema["transactions"] == pl.Int32
    assert df["transactions"][0] == 2649
