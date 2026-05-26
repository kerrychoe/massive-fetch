"""Pydantic configuration models and loader (SPEC §4).

Configuration precedence (§4.1):

1. ``config/default.yaml``                       — committed baseline
2. ``~/.config/massive-fetch/config.yaml``       — user overrides, optional
3. ``--config /path/to/config.yaml``             — CLI flag, highest precedence

API credentials are **never** read from YAML — only from environment variables
(see ``.env.example``).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, field_validator


class APIConfig(BaseModel):
    rest_base_url: str = "https://api.massive.com"
    max_retries: int = 5                    # -> SDK RESTClient(retries=...); see SPEC §7.2
    request_timeout_seconds: int = 30
    max_concurrent_requests: int = 3        # conservative default
    page_limit: int = 50000                 # max per Massive aggregates endpoint


class StorageConfig(BaseModel):
    data_dir: Path = Path("~/market_data").expanduser()
    parquet_compression: Literal["zstd", "snappy", "gzip"] = "zstd"
    parquet_row_group_size: int = 100_000

    @field_validator("data_dir", mode="before")
    @classmethod
    def _expand_data_dir(cls, v: Any) -> Any:
        if isinstance(v, (str, Path)):
            return Path(v).expanduser()
        return v


class StocksUniverseConfig(BaseModel):
    source: Literal["wiki_scrape", "frozen_yaml"] = "wiki_scrape"
    indexes: list[Literal["SP500", "NDX"]] = ["SP500", "NDX"]
    frozen_fallback_path: Path = Path("config/universes/stocks_sp500_qqq.yaml")
    refresh_interval_days: int = 7


class FuturesUniverseConfig(BaseModel):
    product_codes: list[str] = ["ES", "NQ", "RTY", "YM", "CL", "NG", "GC", "SI", "ZN", "ZB", "6E", "6J"]
    discover_contracts: bool = True            # via Massive contracts endpoint
    min_first_trade_date: str = "2022-01-01"   # don't backfill contracts older than this


class CryptoUniverseConfig(BaseModel):
    symbols: list[str] = ["BTC", "ETH"]
    quote_currency: str = "USD"                # builds X:{SYMBOL}{QUOTE} ticker


class IngestConfig(BaseModel):
    stocks: StocksUniverseConfig = StocksUniverseConfig()
    futures: FuturesUniverseConfig = FuturesUniverseConfig()
    crypto: CryptoUniverseConfig = CryptoUniverseConfig()
    extended_hours: bool = True                # stocks: include pre/post-market
    adjusted_for_splits_at_fetch: bool = False # store RAW; we adjust at read time


class DefaultsConfig(BaseModel):
    stocks_daily_start: str = "2005-01-01"
    stocks_minute_start: str = "2020-01-01"
    crypto_start: str = "2018-01-01"
    futures_minute_start: str = "2022-01-01"
    futures_daily_start: str = "2010-01-01"


class ValidationConfig(BaseModel):
    on_zero_bars_open_market: Literal["warn", "fail"] = "warn"
    on_ohlc_violation: Literal["drop", "warn", "fail"] = "warn"
    on_duplicate_timestamp: Literal["keep_first", "keep_last", "fail"] = "keep_first"


class LoggingConfig(BaseModel):
    console_level: str = "INFO"
    file_level: str = "DEBUG"
    file_max_bytes: int = 10 * 1024 * 1024     # 10 MB
    file_backup_count: int = 5


class AppConfig(BaseModel):
    api: APIConfig = APIConfig()
    storage: StorageConfig = StorageConfig()
    ingest: IngestConfig = IngestConfig()
    defaults: DefaultsConfig = DefaultsConfig()
    validation: ValidationConfig = ValidationConfig()
    logging: LoggingConfig = LoggingConfig()


# --- Loader ---------------------------------------------------------------

# config/default.yaml lives at the repo root, two levels above this file
# (src/massive_fetch/config.py -> src -> repo root).
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "default.yaml"
USER_CONFIG_PATH = Path("~/.config/massive-fetch/config.yaml").expanduser()


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Config file {path} must contain a YAML mapping at the top level.")
    return data


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge ``override`` onto ``base`` without mutating either."""
    merged = dict(base)
    for key, value in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_config(config_path: Path | str | None = None) -> AppConfig:
    """Load configuration honoring the §4.1 precedence.

    Missing files at any layer are skipped; if ``default.yaml`` cannot be found
    the Pydantic model defaults (which mirror it) still produce a valid config.
    An explicit ``--config`` path that does not exist is an error.
    """
    merged = _load_yaml(DEFAULT_CONFIG_PATH)
    merged = _deep_merge(merged, _load_yaml(USER_CONFIG_PATH))
    if config_path is not None:
        explicit = Path(config_path).expanduser()
        if not explicit.exists():
            raise FileNotFoundError(f"Config file not found: {explicit}")
        merged = _deep_merge(merged, _load_yaml(explicit))
    return AppConfig.model_validate(merged)
