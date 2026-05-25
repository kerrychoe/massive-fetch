"""Storage path layout — the single source of truth for where data lives (SPEC §6).

No module outside ``storage/`` should construct paths against the data directory
directly. Data is addressed by logical key (a POSIX-style relative path under the
configured ``data_dir``); reorganizing the on-disk layout is a one-file change here.
"""

from __future__ import annotations

# Directory tree created by ``massive-fetch init`` (§6).
DATA_SUBDIRS: tuple[str, ...] = (
    "ohlcv/stocks/daily",
    "ohlcv/stocks/minute",
    "ohlcv/futures/daily",
    "ohlcv/futures/minute",
    "ohlcv/crypto/daily",
    "ohlcv/crypto/minute",
    "corporate_actions",
    "reference",
    "logs",
)


# --- OHLCV key builders ---

def stocks_daily_key(symbol: str) -> str:
    return f"ohlcv/stocks/daily/{symbol}.parquet"


def stocks_minute_key(symbol: str, year: int) -> str:
    return f"ohlcv/stocks/minute/{symbol}/{year}.parquet"


def futures_daily_key(contract: str) -> str:
    return f"ohlcv/futures/daily/{contract}.parquet"


def futures_minute_key(contract: str, year: int) -> str:
    return f"ohlcv/futures/minute/{contract}/{year}.parquet"


def crypto_daily_key(symbol: str) -> str:
    return f"ohlcv/crypto/daily/{symbol}.parquet"


def crypto_minute_key(symbol: str, year: int) -> str:
    return f"ohlcv/crypto/minute/{symbol}/{year}.parquet"


# --- Corporate actions & reference key builders ---

def splits_key() -> str:
    return "corporate_actions/splits.parquet"


def dividends_key() -> str:
    return "corporate_actions/dividends.parquet"


def universe_stocks_key() -> str:
    return "reference/universe_stocks.parquet"


def futures_contracts_key() -> str:
    return "reference/futures_contracts.parquet"


def manifest_key() -> str:
    return "reference/manifest.sqlite"
