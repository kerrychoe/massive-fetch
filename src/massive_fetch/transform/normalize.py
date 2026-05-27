"""Normalize raw SDK ``Agg`` bars into the canonical §6.1 schema.

This is where the wrapper's pass-through ``Agg`` objects (SPEC §7.1) become a
Polars DataFrame matching SPEC §6.1 exactly. The SDK's attribute names already
line up 1:1 with the canonical columns (``SDK_NOTES.md`` §3), so this layer
converts *types and units*, it does not rename.
"""

from __future__ import annotations

from collections.abc import Iterable

import polars as pl

from massive_fetch.clients.rest import Aggregate

# Canonical column dtypes (SPEC §6.1). Order is the on-disk column order.
# - ``volume`` is float64: the SDK returns fractional volume (SDK_NOTES §3) and
#   crypto / fractional-share volume is genuinely non-integer (SPEC §6.1, amended).
# - ``symbol`` is Categorical -> dictionary<string> in Parquet (index width unpinned).
CANONICAL_SCHEMA: dict[str, pl.DataType] = {
    "timestamp": pl.Datetime("ns", "UTC"),
    "symbol": pl.Categorical,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Float64,
    "vwap": pl.Float64,
    "transactions": pl.Int32,
    "otc": pl.Boolean,
}


def _f(v: object) -> float | None:
    """Coerce to float, preserving None. The SDK keeps JSON numbers as their
    natural Python type, so a round value (e.g. volume ``70295``) arrives as an
    ``int`` while ``70295.78`` arrives as a ``float``; left mixed, Polars would
    infer Int64 from the first value and reject the later float."""
    return float(v) if v is not None else None


def _i(v: object) -> int | None:
    """Coerce to int, preserving None."""
    return int(v) if v is not None else None


def normalize(aggs: Iterable[Aggregate], ticker: str) -> pl.DataFrame:
    """Convert raw SDK ``Agg`` bars for ``ticker`` into the canonical §6.1 schema.

    Conversion steps:

    1. **timestamp**: Unix milliseconds -> nanoseconds, tz attached as UTC.
    2. **symbol**: inject ``ticker`` (the Massive ticker, e.g. ``X:BTCUSD``) as a
       dictionary-encoded (Categorical) column.
    3. **dtype enforcement** per §6.1 (notably ``volume`` -> float64).
    4. **sort** ascending by timestamp.

    An empty input yields an empty DataFrame carrying the exact schema, so callers
    can treat "no bars" as a zero-row success without having to special-case dtypes.
    """
    bars = list(aggs)
    if not bars:
        return pl.DataFrame(schema=CANONICAL_SCHEMA)

    raw = pl.DataFrame(
        {
            "timestamp": [_i(b.timestamp) for b in bars],
            "open": [_f(b.open) for b in bars],
            "high": [_f(b.high) for b in bars],
            "low": [_f(b.low) for b in bars],
            "close": [_f(b.close) for b in bars],
            "volume": [_f(b.volume) for b in bars],
            "vwap": [_f(b.vwap) for b in bars],
            "transactions": [_i(b.transactions) for b in bars],
            "otc": [b.otc for b in bars],
        }
    )

    df = raw.select(
        pl.from_epoch(pl.col("timestamp").cast(pl.Int64), time_unit="ms")
        .dt.replace_time_zone("UTC")
        .cast(pl.Datetime("ns", "UTC"))
        .alias("timestamp"),
        pl.lit(ticker).cast(pl.Categorical).alias("symbol"),
        pl.col("open").cast(pl.Float64),
        pl.col("high").cast(pl.Float64),
        pl.col("low").cast(pl.Float64),
        pl.col("close").cast(pl.Float64),
        pl.col("volume").cast(pl.Float64),
        pl.col("vwap").cast(pl.Float64),
        pl.col("transactions").cast(pl.Int32),
        pl.col("otc").cast(pl.Boolean),
    )
    return df.sort("timestamp")
