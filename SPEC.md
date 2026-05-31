# massive-fetch — Specification

A Python CLI tool that downloads historical market data from [Massive.com](https://massive.com) and stores it locally as Parquet for use by downstream backtesting and research workflows.

This is the design contract. Read it end-to-end before generating code. Every architectural decision is fixed here so the implementation is unambiguous.

---

## 1. Goals & Scope

### In scope (v1)

- REST API ingestion for **stocks**, **futures**, and **crypto** OHLCV bars at **daily** and **minute** timeframes.
- Static, config-driven universe definitions for each asset class.
- Local Parquet storage, partitioned for fast read access by downstream backtesters.
- Idempotent, resumable backfills with a manifest tracking what's been downloaded.
- Daily incremental updates (`update` command) suitable for cron scheduling.
- Corporate actions (splits, dividends) ingestion — stored separately, applied at read time.
- Data validation with warn-and-continue semantics.

### Out of scope (v1, may be Phase 2+)

- Flat Files (S3) ingestion mode — design must leave the door open without forcing it.
- Tick / trade / quote data.
- Options, forex, indices.
- Continuous futures construction (per-contract only in v1).
- Cloud storage backends.
- WebSocket / real-time data.
- A backtester. This tool only produces data; backtesting is a downstream concern.
- Point-in-time / bias-free index membership. v1 uses today's universe and documents the survivorship bias loudly.

### Explicit non-goals

- Will not provide trading signals, recommendations, or strategy logic.
- Will not modify or "clean" data beyond schema normalization. Raw values from Massive are preserved.
- Will not be a wrapper around the Massive Python SDK with no value-add. The value-add is: durable local storage, resumability, normalization, validation, scheduling, and a CLI surface.

---

## 2. Tech Stack

| Concern | Choice | Notes |
|---|---|---|
| Language | Python ≥ 3.11 | Type hints required throughout. |
| Package manager | `uv` | Lockfile committed. `pyproject.toml` is canonical. |
| CLI framework | `typer` | With rich-formatted help. |
| Config | `pydantic` v2 + YAML via `pyyaml` | Pydantic models are the schema. |
| HTTP / SDK | [`massive-com/client-python`](https://github.com/massive-com/client-python) | Official SDK. Handles pagination. |
| Concurrency | `asyncio` + `aiohttp` for parallel REST | Or thread pool; SDK call style decides. See §11. |
| DataFrames | `polars` (preferred) or `pandas` | Polars for speed and Parquet integration. |
| Parquet | `pyarrow` | Compression: `zstd`. |
| Manifest store | `sqlite3` (stdlib) | One file: `data/reference/manifest.sqlite`. |
| Logging | `structlog` + stdlib `logging` | JSON to file, human-readable to console. |
| Testing | `pytest`, `pytest-asyncio`, `pytest-mock` | Mock the SDK, never hit the live API in tests. |
| Time / calendar | `pandas_market_calendars` | For NYSE, CME, and crypto (24/7) calendars. |
| Env / secrets | `python-dotenv` for local dev | API keys via env vars only, never in config files. |

### AI assistant rules

The repo should include `.claude/CLAUDE.md` (or equivalent for other assistants), populated from the [`massive-com/massive-ai-rules`](https://github.com/massive-com/massive-ai-rules) repo, so future code generation respects Massive's SDK patterns, ticker formats, and plan-tier nuances.

---

## 3. Project Layout

```
massive-fetch/
├── pyproject.toml
├── uv.lock
├── README.md
├── SPEC.md                              # this document
├── .env.example
├── .gitignore
├── .claude/
│   └── CLAUDE.md                        # from massive-ai-rules
├── config/
│   ├── default.yaml                     # default config, committed
│   └── universes/
│       ├── stocks_sp500_qqq.yaml        # frozen fallback snapshot
│       ├── futures_majors.yaml
│       └── crypto_btc_eth.yaml
├── src/
│   └── massive_fetch/
│       ├── __init__.py
│       ├── __main__.py                  # python -m massive_fetch
│       ├── cli.py                       # Typer app
│       ├── config.py                    # Pydantic models
│       ├── logging_setup.py
│       ├── clients/
│       │   ├── __init__.py
│       │   └── rest.py                  # SDK wrapper, retry, rate limit
│       ├── reference/
│       │   ├── __init__.py
│       │   ├── universe.py              # SP500/QQQ scrape + frozen fallback
│       │   ├── futures_contracts.py     # discovers contracts via API
│       │   └── calendar.py              # market calendars
│       ├── ingest/
│       │   ├── __init__.py
│       │   ├── base.py                  # shared ingestion plumbing
│       │   ├── stocks.py
│       │   ├── futures.py
│       │   ├── crypto.py
│       │   └── corporate_actions.py
│       ├── storage/
│       │   ├── __init__.py
│       │   ├── manifest.py              # SQLite manifest API
│       │   ├── parquet_io.py            # read/write helpers
│       │   └── paths.py                 # path layout (single source of truth)
│       ├── transform/
│       │   ├── __init__.py
│       │   └── normalize.py             # SDK response → canonical schema
│       ├── validate/
│       │   ├── __init__.py
│       │   ├── runner.py                # `validate` CLI entry point
│       │   ├── gaps.py
│       │   ├── schema_check.py
│       │   └── duplicates.py
│       └── utils/
│           ├── __init__.py
│           ├── time_utils.py            # ms ↔ datetime, tz handling
│           └── retry.py                 # tenacity wrapper
└── tests/
    ├── conftest.py
    ├── fixtures/                        # canned SDK responses
    ├── unit/
    └── integration/                     # gated behind env var
```

---

## 4. Configuration

### 4.1 Files & precedence

1. `config/default.yaml` (committed, baseline values)
2. `~/.config/massive-fetch/config.yaml` (user overrides, optional)
3. CLI flags (highest precedence)

API credentials are **never** read from YAML. Only from env vars:

- `MASSIVE_API_KEY` — REST API key (required)
- `MASSIVE_S3_ACCESS_KEY` — S3 access key (Phase 2, ignored in v1)
- `MASSIVE_S3_SECRET_KEY` — S3 secret key (Phase 2, ignored in v1)

### 4.2 Pydantic models (`src/massive_fetch/config.py`)

```python
from pathlib import Path
from typing import Literal
from pydantic import BaseModel, Field, field_validator

class APIConfig(BaseModel):
    rest_base_url: str = "https://api.massive.com"
    max_retries: int = 5                    # -> SDK RESTClient(retries=...); see §7.2
    request_timeout_seconds: int = 30
    max_concurrent_requests: int = 3        # conservative default
    page_limit: int = 50000                 # max per Massive aggregates endpoint

# NOTE (Slice 1): retry_backoff_base_seconds / retry_backoff_max_seconds were
# removed. The SDK owns retry with a fixed urllib3 backoff that the constructor
# does not expose, so those fields could never be wired. See §7.2.

class StorageConfig(BaseModel):
    data_dir: Path = Path("~/market_data").expanduser()
    parquet_compression: Literal["zstd", "snappy", "gzip"] = "zstd"
    parquet_row_group_size: int = 100_000

class StocksUniverseConfig(BaseModel):
    source: Literal["wiki_scrape", "frozen_yaml"] = "wiki_scrape"
    indexes: list[Literal["SP500", "NDX"]] = ["SP500", "NDX"]
    frozen_fallback_path: Path = Path("config/universes/stocks_sp500_qqq.yaml")
    refresh_interval_days: int = 7

class FuturesUniverseConfig(BaseModel):
    product_codes: list[str] = ["ES","NQ","RTY","YM","CL","NG","GC","SI","ZN","ZB","6E","6J"]
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
```

### 4.3 `config/default.yaml`

```yaml
api:
  rest_base_url: https://api.massive.com
  max_retries: 5
  request_timeout_seconds: 30
  max_concurrent_requests: 3
  page_limit: 50000

storage:
  data_dir: ~/market_data
  parquet_compression: zstd
  parquet_row_group_size: 100000

ingest:
  stocks:
    source: wiki_scrape
    indexes: [SP500, NDX]
    refresh_interval_days: 7
  futures:
    product_codes: [ES, NQ, RTY, YM, CL, NG, GC, SI, ZN, ZB, 6E, 6J]
    discover_contracts: true
    min_first_trade_date: "2022-01-01"
  crypto:
    symbols: [BTC, ETH]
    quote_currency: USD
  extended_hours: true
  adjusted_for_splits_at_fetch: false

defaults:
  stocks_daily_start: "2005-01-01"
  stocks_minute_start: "2020-01-01"
  crypto_start: "2018-01-01"
  futures_minute_start: "2022-01-01"
  futures_daily_start: "2010-01-01"

validation:
  on_zero_bars_open_market: warn
  on_ohlc_violation: warn
  on_duplicate_timestamp: keep_first

logging:
  console_level: INFO
  file_level: DEBUG
  file_max_bytes: 10485760
  file_backup_count: 5
```

---

## 5. Universe Definition

### 5.1 Stocks (S&P 500 ∪ Nasdaq-100)

Default flow on `massive-fetch reference update`:

1. Attempt to scrape Wikipedia:
   - S&P 500 list: `https://en.wikipedia.org/wiki/List_of_S%26P_500_companies`
   - Nasdaq-100 list: `https://en.wikipedia.org/wiki/Nasdaq-100`
2. Parse with `pandas.read_html`, extract ticker columns.
3. Normalize tickers (e.g., `BRK.B` ↔ `BRK-B` — Massive uses `.`).
4. Deduplicate union.
5. Write to `config/universes/stocks_sp500_qqq.yaml` with timestamp.
6. On scrape failure, log warning and use the existing frozen YAML.

Frozen YAML schema:

```yaml
generated_at: "2025-04-15T12:00:00Z"
source: "wikipedia"
indexes_included: [SP500, NDX]
tickers:
  - AAPL
  - MSFT
  - GOOGL
  # ... ~516 tickers
```

The frozen YAML is committed as a fallback, regenerated on every successful scrape. **The README must clearly state that this universe is "today's membership only" and creates survivorship bias.**

### 5.2 Futures (auto-discovery)

On `massive-fetch reference update`, for each product code in config:

1. Call Massive's "All Contracts" endpoint (`/v3/reference/contracts` or SDK equivalent) with `product_code=<code>`, paginate.
2. Filter contracts where `last_trade_date >= min_first_trade_date`.
3. Persist to `data/reference/futures_contracts.parquet` with columns:
   - `ticker` (str, e.g., `ESH25`)
   - `product_code` (str, e.g., `ES`)
   - `name` (str)
   - `month` (str, calendar month code or YYYYMM)
   - `first_trade_date` (date)
   - `last_trade_date` (date)
   - `exchange_mic` (str)
   - `tick_size` (float)
   - `discovered_at` (timestamp)

**Handle gracefully**: Massive's futures REST is documented as "beta / coming soon" in their docs. The futures ingestion module must:

- Catch any "endpoint not available" / 404 / 501 and emit a clear warning.
- Allow `--skip-futures` on backfill commands.
- Exit cleanly without breaking other ingestion.

### 5.3 Crypto (BTC, ETH)

Massive crypto ticker format is `X:{BASE}{QUOTE}`, e.g., `X:BTCUSD`, `X:ETHUSD`.

The config holds the bare symbol list (`[BTC, ETH]`); the ingest layer constructs the Massive ticker. No discovery needed — the symbol set is hardcoded by user choice.

---

## 6. Storage Layout

```
{data_dir}/
├── ohlcv/
│   ├── stocks/
│   │   ├── daily/
│   │   │   └── {SYMBOL}.parquet                  # full history one file
│   │   └── minute/
│   │       └── {SYMBOL}/
│   │           └── {YYYY}.parquet                # year-partitioned
│   ├── futures/
│   │   ├── daily/
│   │   │   └── {CONTRACT_TICKER}.parquet
│   │   └── minute/
│   │       └── {CONTRACT_TICKER}/
│   │           └── {YYYY}.parquet
│   └── crypto/
│       ├── daily/
│       │   └── {SYMBOL}.parquet
│       └── minute/
│           └── {SYMBOL}/
│               └── {YYYY}.parquet
├── corporate_actions/
│   ├── splits.parquet
│   └── dividends.parquet
├── reference/
│   ├── universe_stocks.parquet
│   ├── futures_contracts.parquet
│   ├── market_calendar.parquet
│   └── manifest.sqlite
└── logs/
    ├── massive-fetch.log
    └── massive-fetch.log.1            # rotated
```

### 6.0.1 Storage Backend Abstraction

To preserve the option of moving storage to cloud (S3, GCS) without project-wide refactoring, all file I/O goes through a `StorageBackend` interface. v1 ships with a single `LocalBackend` implementation; cloud implementations are explicitly Phase 2.

**Design principle**: `storage/paths.py` and `storage/parquet_io.py` are the only modules that know about *where* data lives. All other modules accept a `StorageBackend` instance and address data by logical key (e.g., `"ohlcv/stocks/daily/AAPL.parquet"`), never by absolute path.

```python
# src/massive_fetch/storage/backend.py

from typing import Protocol, BinaryIO
from polars import DataFrame as PolarsDF

class StorageBackend(Protocol):
    """Abstract storage interface. v1: LocalBackend. Phase 2: S3Backend, GCSBackend."""

    def write_parquet(self, key: str, df: PolarsDF) -> None: ...
    def read_parquet(self, key: str) -> PolarsDF: ...
    def append_parquet(self, key: str, df: PolarsDF, dedupe_on: list[str]) -> None:
        """Read existing (if any), concat with df, dedupe, write back."""

    def exists(self, key: str) -> bool: ...
    def delete(self, key: str) -> None: ...
    def list_keys(self, prefix: str) -> list[str]: ...
    def size_bytes(self, key: str) -> int: ...

    # Used by the manifest module — must be local-only in v1.
    # In Phase 2, manifest may stay local or move to a real DB.
    def local_path(self, key: str) -> Path | None:
        """Return a local filesystem path if available, None for remote-only backends."""


class LocalBackend:
    """File-system implementation. Default for v1."""
    def __init__(self, root: Path):
        self.root = root.expanduser().resolve()

    def _path(self, key: str) -> Path:
        return self.root / key

    # ... implements protocol via Path / pyarrow / polars


class S3Backend:
    """Phase 2 stub — not implemented in v1."""
    def __init__(self, *args, **kwargs):
        raise NotImplementedError(
            "S3Backend is a Phase 2 feature. See SPEC.md §17."
        )
```

**v1 requirements**:

- All ingestion modules, validation modules, and the manifest layer accept a `StorageBackend` via dependency injection (passed in from `cli.py`).
- No module outside `storage/` constructs `Path` objects against the data directory directly.
- `storage/paths.py` exposes a small set of key-builder helpers:

```python
def stocks_daily_key(symbol: str) -> str:
    return f"ohlcv/stocks/daily/{symbol}.parquet"

def stocks_minute_key(symbol: str, year: int) -> str:
    return f"ohlcv/stocks/minute/{symbol}/{year}.parquet"

def futures_daily_key(contract: str) -> str: ...
def futures_minute_key(contract: str, year: int) -> str: ...
def crypto_daily_key(symbol: str) -> str: ...
def crypto_minute_key(symbol: str, year: int) -> str: ...
def splits_key() -> str: ...
def dividends_key() -> str: ...
def universe_stocks_key() -> str: ...
def futures_contracts_key() -> str: ...
```

This is the only module that knows the directory hierarchy. Reorganizing the layout in the future is a one-file change.

**Manifest exception**: `manifest.sqlite` requires a true local file path because SQLite doesn't work over object stores. The `StorageBackend.local_path()` method exists to surface this. In v1, `LocalBackend` returns the actual path; in Phase 2, S3/GCS backends will either:
- Return `None`, requiring the manifest to live on a different (local) backend, OR
- Return a path to a synced local cache.

The manifest module accepts a `Path` directly in its constructor and is configured separately from the data backend, allowing flexibility:

```python
# v1
data_backend = LocalBackend(root=config.storage.data_dir)
manifest = Manifest(path=config.storage.data_dir / "reference" / "manifest.sqlite")

# Phase 2 example: cloud data, local manifest
data_backend = S3Backend(bucket="my-quant-data")
manifest = Manifest(path=Path("~/.massive-fetch/manifest.sqlite"))
```

**Cost in v1**: ~80 lines of code in `storage/backend.py`, ~40 lines in `storage/paths.py`. Negligible.
**Benefit in Phase 2**: cloud migration becomes a single new backend file plus a config field, not a project-wide refactor.

### 6.1 Parquet schemas (canonical, post-normalization)

All OHLCV files share this schema, stored with `zstd` compression:

| Column | Arrow type | Notes |
|---|---|---|
| `timestamp` | `timestamp[ns, tz="UTC"]` | UTC, nanosecond precision. Convert at read time for display. |
| `symbol` | `dictionary<string>` | The instrument ticker as passed to Massive. Index width unpinned — Polars writes uint32; readers handle either. |
| `open` | `float64` | |
| `high` | `float64` | |
| `low` | `float64` | |
| `close` | `float64` | |
| `volume` | `float64` | Raw SDK value is fractional (fractional-share / crypto volume); stored as float64 to avoid lossy truncation. |
| `vwap` | `float64` | Nullable; absent for some asset classes. |
| `transactions` | `int32` | Nullable. |
| `otc` | `bool` | Stocks only; nullable elsewhere. |

**Sort order**: ascending by `timestamp` within each file.
**Row groups**: target 100k rows.
**Adjustment policy**: stored RAW (`adjusted=false` at the API call). Adjustment is a read-time concern.

### 6.2 Splits & dividends schemas

`corporate_actions/splits.parquet`:

| Column | Type |
|---|---|
| `symbol` | string |
| `execution_date` | date |
| `split_from` | float64 |
| `split_to` | float64 |
| `ratio` | float64 |

`corporate_actions/dividends.parquet`:

| Column | Type |
|---|---|
| `symbol` | string |
| `ex_dividend_date` | date |
| `cash_amount` | float64 |
| `currency` | string |
| `dividend_type` | string |
| `frequency` | int32 |
| `pay_date` | date |
| `record_date` | date |
| `declaration_date` | date |

### 6.3 Manifest schema (`manifest.sqlite`)

Single table tracking ingestion progress at `(symbol, timeframe)` granularity:

```sql
CREATE TABLE IF NOT EXISTS ingestion_state (
    asset_class    TEXT NOT NULL,            -- 'stocks' | 'futures' | 'crypto'
    symbol         TEXT NOT NULL,            -- e.g., 'AAPL', 'ESH25', 'X:BTCUSD'
    timeframe      TEXT NOT NULL,            -- 'daily' | 'minute'
    earliest_date  TEXT NOT NULL,            -- ISO date, oldest bar present
    last_complete_date TEXT NOT NULL,        -- ISO date, newest bar present
    last_updated_at TEXT NOT NULL,           -- ISO datetime UTC
    bar_count      INTEGER NOT NULL,
    PRIMARY KEY (asset_class, symbol, timeframe)
);

CREATE TABLE IF NOT EXISTS run_log (
    run_id         TEXT PRIMARY KEY,         -- uuid
    started_at     TEXT NOT NULL,
    finished_at    TEXT,
    command        TEXT NOT NULL,            -- the CLI invocation
    status         TEXT NOT NULL,            -- 'running' | 'success' | 'failed'
    error_message  TEXT,
    symbols_attempted INTEGER,
    symbols_succeeded INTEGER,
    bars_written   INTEGER
);

CREATE INDEX IF NOT EXISTS idx_run_log_started ON run_log(started_at DESC);
```

**Resumability rule**: any `backfill` command for `(asset_class, symbol, timeframe)` queries `ingestion_state`. If a row exists, the new fetch starts from `last_complete_date + 1 day`. If not, from the configured default start.

---

## 7. REST Client (`src/massive_fetch/clients/rest.py`)

A thin async facade over the synchronous `massive` SDK (`massive-com/client-python`).
The SDK owns retry; this wrapper adds exactly three things (see `SDK_NOTES.md` for
the discovery that drove this):

- Concurrency-limited execution (`asyncio.Semaphore`, default 3) bridging the sync
  SDK through a thread pool.
- Structured logging of every call (§11).
- Typed exception mapping (§7.2). The wrapper never swallows errors.

### 7.1 Public interface

`Aggregate` is the SDK's `Agg` model, re-exported — the wrapper yields it unchanged
and leaves canonical normalization to `transform/normalize.py` (Slice 2+).

```python
# Typed exception hierarchy (see §7.2)
class MassiveClientError(Exception): ...        # base — callers catch this
class MassiveAuthError(MassiveClientError): ...        # wraps SDK AuthError (construction only)
class MassiveBadRequest(MassiveClientError): ...       # wraps SDK BadResponse (never retryable)
class MassiveRetriesExhausted(MassiveClientError): ... # wraps urllib3 MaxRetryError/HTTPError

class MassiveRESTClient:
    def __init__(self, api_key: str, config: APIConfig, logger): ...
    async def __aenter__(self) -> "MassiveRESTClient": ...
    async def __aexit__(self, *exc) -> None: ...
    async def aclose(self) -> None: ...         # shuts down the thread pool

    async def list_aggs(
        self,
        ticker: str,
        multiplier: int,
        timespan: Literal["minute", "day"],
        from_date: str,         # 'YYYY-MM-DD'
        to_date: str,           # 'YYYY-MM-DD'
        adjusted: bool = False, # RAW per §6.1 (SDK default is True — we override)
        sort: Literal["asc", "desc"] = "asc",
    ) -> AsyncIterator[Aggregate]: ...
```

**Deferred to their own slices** (same thread-pool bridge, not built in Slice 1):
`list_splits` / `list_dividends` (Slice 7) and `list_futures_contracts` (Slice 8 —
note futures *bars* use the SDK's separate `list_futures_aggregates`, not `list_aggs`).

### 7.2 Retry policy

**Retry is owned by the SDK, not by this wrapper.** `massive-com/client-python`
configures a `urllib3.util.Retry` internally with:

- `status_forcelist = [413, 429, 499, 500, 502, 503, 504]`,
- `backoff_factor = 0.1` (→ 0.2s, 0.4s, 0.8s, 1.6s …),
- `Retry-After` honored for 413/429/503,
- connection/timeout errors covered, and
- total attempts = the SDK's `retries` argument, which we set from `APIConfig.max_retries`.

The wrapper adds **no** retry layer (avoiding double-retry). Only the attempt count
and request timeouts are configurable through the SDK constructor; the forcelist and
backoff curve are fixed by the SDK. Consequently the former
`retry_backoff_base_seconds` / `retry_backoff_max_seconds` config fields were removed
(§4.2) — they could not alter the SDK's fixed backoff.

When the SDK exhausts its retries it raises `urllib3.exceptions.MaxRetryError`, mapped
to **`MassiveRetriesExhausted`** and re-raised. Non-retryable responses
(400/401/403/404) surface from the SDK as `BadResponse`, mapped to
**`MassiveBadRequest`** (never retried). Construction with a missing key raises the
SDK's `AuthError`, mapped to **`MassiveAuthError`**. The wrapper never swallows these
— the caller decides whether to skip the symbol or abort the job. (A bad ticker is not
an error: the SDK returns an empty result, which the wrapper passes through.)

*Rationale: the pre-implementation version of this section specified a bespoke
`tenacity` policy (base=1.0s/max=60s/jitter). SDK discovery showed the SDK already
implements urllib3-based retry with `Retry-After` handling; that earlier policy was
aspirational. We accept the SDK's behavior and add only the layer above.*

### 7.3 Rate limiting

- A single `asyncio.Semaphore(max_concurrent_requests)` gates all outbound calls; it
  is held only across the (blocking, thread-pooled) fetch, released before results are
  yielded.
- `Retry-After` on 429 is handled inside the SDK's urllib3 `Retry` (see §7.2).

### 7.4 Concurrency model

The SDK's `list_aggs` is a sync generator that paginates internally. The wrapper
materializes the full page-walk in a worker thread (`run_in_executor`, pool sized to
`max_concurrent_requests`) under the semaphore, then yields. A `fetch_many` worker-pool
helper for fanning out across symbols is deferred to Slice 2, where the first call site
(crypto ingestion) appears; since the semaphore lives inside the client, any
`asyncio.gather` over client calls is already concurrency-bounded.

---

## 8. Ingestion Modules

Each ingestion module follows the same shape:

1. Load universe.
2. For each instrument, query manifest to determine start date.
3. Skip if `last_complete_date >= target_end`.
4. Fetch via REST client, paginated.
5. Normalize to canonical schema (`transform/normalize.py`).
6. **Append** to existing Parquet (read-merge-write — Parquet doesn't support true append; read existing, concat, dedupe, write back).
7. Update manifest.
8. Emit metrics for the run log.

### 8.1 Stocks (`ingest/stocks.py`)

- Universe: from `reference/universe_stocks.parquet`.
- Daily: one file per symbol, full history.
- Minute: year-partitioned per symbol — must determine which year files need updating.
- `extended_hours: true` is the default; the SDK's aggregates endpoint already returns extended-hours bars when present.
- Normalization handles ticker dot/dash variants (`BRK.B` is the Massive form).

### 8.2 Futures (`ingest/futures.py`)

- Universe: from `reference/futures_contracts.parquet`.
- For each contract, fetch from `max(first_trade_date, configured_start)` to `min(last_trade_date, today-1)`.
- Skip contracts with `last_trade_date < configured_start`.
- **Beta gracefully**: wrap all calls in try/except; if futures endpoints return 404/501, log once at WARN, mark futures as unavailable for this run, continue with other asset classes.

### 8.3 Crypto (`ingest/crypto.py`)

- Universe: derived from config (`X:BTCUSD`, `X:ETHUSD`).
- Calendar: 24/7. No "market closed" days. Validation logic differs accordingly.
- Daily: file per symbol. Minute: year-partitioned per symbol.

### 8.4 Corporate actions (`ingest/corporate_actions.py`)

- Two functions: `ingest_splits()`, `ingest_dividends()`.
- Pull all splits / dividends across the stocks universe.
- Append to `splits.parquet` / `dividends.parquet`, deduplicated on natural keys.
- Run as part of `massive-fetch update` daily.

---

## 9. Validation (`src/massive_fetch/validate/`)

Validation runs in two contexts:

1. **Inline during ingestion** — schema sanity, OHLC violations, duplicate timestamps. Behavior governed by `validation.*` config.
2. **Standalone via `massive-fetch validate`** — full sweep, produces a report.

### 9.1 Checks

- **Schema check**: every column present with expected dtype.
- **OHLC sanity**: `low <= open, close <= high`. Violations: drop / warn / fail per config.
- **Duplicate timestamps**: per (symbol, timestamp). Resolution per config.
- **Gap detection**: for each (symbol, timeframe), compare actual bar timestamps against the relevant market calendar:
  - Stocks: NYSE calendar.
  - Futures: per-product CME schedules (or fallback to "weekdays excluding US holidays").
  - Crypto: continuous (no gaps expected).
- **Coverage**: each manifest row's `last_complete_date` is within N trading days of "now".

### 9.2 Report format

`massive-fetch validate report` writes a markdown summary:

```
# Validation Report — 2025-05-02 14:23 UTC

## Stocks (daily)
- Symbols: 516
- Healthy: 509
- Warnings: 7
  - AMC: 3 missing trading days in 2024 (2024-01-15, 2024-02-19, 2024-09-02)
  - ...

## Stocks (minute)
...
```

---

## 10. CLI (`src/massive_fetch/cli.py`)

Built with Typer. Every command is idempotent.

### 10.1 Commands

```
massive-fetch init
    Creates {data_dir} structure, writes empty manifest.sqlite,
    optionally pulls reference data.

massive-fetch reference update [--scope=all|stocks|futures] [--force]
    Refreshes universe lists.
    --force bypasses the 7-day cache short-circuit (forces a refresh).
    Stocks: scrapes Wikipedia, writes universe_stocks.parquet.
    Futures: discovers contracts via API, writes futures_contracts.parquet.

massive-fetch backfill stocks  --timeframe=daily|minute
                                [--start=YYYY-MM-DD] [--end=YYYY-MM-DD]
                                [--symbols=AAPL,MSFT]
                                [--concurrency=N]
massive-fetch backfill futures --timeframe=daily|minute
                                [--start=YYYY-MM-DD] [--end=YYYY-MM-DD]
                                [--products=ES,NQ]
                                [--concurrency=N]
massive-fetch backfill crypto  --timeframe=daily|minute
                                [--start=YYYY-MM-DD] [--end=YYYY-MM-DD]
                                [--symbols=BTC,ETH]
                                [--concurrency=N]
    --start defaults from config.defaults.<asset>_<timeframe>_start.
    --end defaults to yesterday (ET).

massive-fetch corporate-actions [--start=YYYY-MM-DD]
    Refreshes splits and dividends.

massive-fetch update [--asset=stocks|futures|crypto]
                     [--timeframe=daily|minute]
                     [--include-corporate-actions]
    Cron target. No flags = update everything to latest available.
    Determines per-symbol gap from manifest, fetches the delta only.

massive-fetch validate [--asset=...] [--timeframe=...] [--report]
    Runs full validation. With --report, writes validation_report.md
    to data/reports/.

massive-fetch status
    Prints summary: per asset/timeframe, # symbols tracked, oldest/newest dates,
    total disk usage, last update timestamp. Also prints reference data freshness
    (see §10.3).
```

### 10.2 Conventions

- All commands accept `--config /path/to/config.yaml` to override defaults.
- All commands accept `--verbose` / `-v` to elevate console log level to DEBUG.
- All commands accept `--dry-run` where applicable (prints what it would do).
- Exit codes: 0 = success, 1 = partial failure, 2 = total failure, 3 = config/usage error.

### 10.3 `status` command — reference data freshness

The `status` output must surface staleness of reference data so silent failures (e.g., a broken Wikipedia scraper, a futures discovery that hasn't run in months) are visible without opening files manually.

For each reference dataset, print a one-line summary including the age in days and a colorized health flag.

**Required output section**:

```
Reference data:
  Stocks universe       (SP500 ∪ NDX):  516 tickers   · refreshed 2 days ago    [OK]
  Futures contracts     (12 products):  84 contracts  · refreshed 5 days ago    [OK]
  Market calendar:                                    · refreshed 1 day ago     [OK]
  Splits:                                3,412 rows   · refreshed 1 day ago     [OK]
  Dividends:                            18,209 rows   · refreshed 1 day ago     [OK]
```

**Freshness thresholds** (driven by config, defaults shown):

| Dataset | OK | WARN | STALE |
|---|---|---|---|
| Stocks universe | ≤ `refresh_interval_days` (default 7) | ≤ 2× threshold | > 2× threshold |
| Futures contracts | ≤ 14 days | ≤ 30 days | > 30 days |
| Market calendar | ≤ 30 days | ≤ 90 days | > 90 days |
| Splits / dividends | ≤ 2 days | ≤ 7 days | > 7 days |

Health flags use color when the terminal supports it: `[OK]` green, `[WARN]` yellow, `[STALE]` red. Plain text otherwise.

**Implementation**:

- Reference freshness comes from each dataset's stored timestamp:
  - Stocks universe: `generated_at` field in `universe_stocks.parquet` metadata, OR file mtime as fallback.
  - Futures contracts: `discovered_at` column max value in `futures_contracts.parquet`.
  - Splits / dividends: file mtime, OR a `last_updated` row stored in the file.
  - Market calendar: file mtime.
- A small helper `reference/freshness.py` exposes `get_freshness_summary() -> list[FreshnessRow]` that the `status` command consumes.
- If a reference file is missing entirely, print `[MISSING]` in red and suggest the command to create it (e.g., `Run: massive-fetch reference update`).

**Config additions** (added to `default.yaml` and `LoggingConfig` / new `StatusConfig`):

```yaml
status:
  freshness_thresholds:
    stocks_universe_days: 7        # ties to ingest.stocks.refresh_interval_days
    futures_contracts_days: 14
    market_calendar_days: 30
    corporate_actions_days: 2
```

**Exit code**: `status` always exits 0 unless a fatal error occurs reading state. STALE flags do not cause non-zero exit — they're informational. (Use `validate` for actionable health checks that affect exit codes.)


---

## 11. Logging

- **Console**: human-readable, level INFO by default.
- **File**: JSON-structured (one event per line), level DEBUG, written to `{data_dir}/logs/massive-fetch.log`, rotated at 10 MB × 5 files.
- Every API call logs: ticker, timespan, from, to, response time, bar count, status.
- Every backfill run logs: run_id, command, total symbols, success count, failure count, total bars written, total duration.

---

## 12. The `update` Command — Cron Target

This is the only command intended for unattended scheduled execution.

### 12.1 Behavior

`massive-fetch update` (no flags):

1. For each asset class × timeframe combination present in manifest, find symbols with `last_complete_date < yesterday`.
2. Fetch the delta from `last_complete_date + 1 day` to yesterday.
3. Append-and-dedupe into the appropriate Parquet files.
4. Update manifest.
5. Refresh corporate actions (last 30 days).
6. Run inline validation, log warnings, exit 0.

### 12.2 Recommended schedule

- **Stocks**: 4:30 PM ET on weekdays after market close, OR 11:30 AM ET T+1 (latency-safe).
- **Futures**: same as stocks.
- **Crypto**: any time, ideally daily.

### 12.3 Cron example

```cron
# 11:30 AM ET (16:30 UTC during EDT, 17:30 UTC during EST) daily
30 16 * * * cd /path/to/massive-fetch && /path/to/uv run massive-fetch update >> ~/cron-massive.log 2>&1
```

A "Claude routine" wrapper would invoke the same command and additionally summarize the run output via the LLM, but the data fetch itself does not need an LLM in the loop.

---

## 13. Build Order (Vertical Slices)

Each slice is independently testable and produces something demonstrable. Do not start slice N+1 until slice N is green.

### Slice 0 — Skeleton
- [ ] `pyproject.toml`, `uv.lock`, project structure.
- [ ] Pydantic config models load `default.yaml` cleanly.
- [ ] `massive-fetch init` creates the data directory tree and an empty manifest.
- [ ] `massive-fetch status` runs and prints "no data yet."
- **Acceptance**: `uv run massive-fetch init && uv run massive-fetch status` works.

### Slice 1 — REST client
- [ ] SDK wrapper class with retry/concurrency.
- [ ] Unit tests with mocked SDK.
- **Acceptance**: tests pass; manual smoke test fetching 1 day of AAPL minute bars succeeds.

### Slice 2 — Crypto ingestion (smallest universe)
- [ ] `ingest/crypto.py` end-to-end.
- [ ] Manifest read/write.
- [ ] Parquet write to canonical schema.
- [ ] `massive-fetch backfill crypto --timeframe=daily --start=2018-01-01` produces correct files.
- **Acceptance**: BTC and ETH daily files present; manifest reflects them; `status` shows 2 symbols.

### Slice 3 — Crypto minute + resumability
- [ ] Year-partitioned writes for minute data.
- [ ] Resumability — re-running backfill is a no-op when manifest is current.
- [ ] Killing mid-run and re-running picks up correctly.
- **Acceptance**: BTC minute backfill from 2024-01-01 produces a 2024.parquet; re-running adds nothing; killing mid-2024 and re-running completes correctly.

### Slice 4 — Stocks reference data
- [ ] Wikipedia scrape for SP500 + NDX.
- [ ] Frozen YAML fallback.
- [ ] `reference update` writes universe_stocks.parquet.
- **Acceptance**: ~516 unique tickers in the parquet today (S&P 500 ∪ NDX, current membership — drifts; the live smoke asserts a loose 450–650 band); rerun within 7 days uses cache.

### Slice 5 — Stocks daily ingestion
- [x] Loops over universe.
- [x] Concurrent fetch with semaphore.
- [x] Append-and-dedupe to per-symbol parquet.
- **Acceptance**: backfill from 2020-01-01 for the universe completes; spot-check AAPL has correct bar count vs. expected trading days.

### Slice 6 — Stocks minute ingestion
- [ ] Year-partitioned writes.
- [ ] Handles extended hours.
- **Acceptance**: 1 month of minute data for the full universe completes within reasonable wall time at concurrency=3.

### Slice 7 — Corporate actions
- [ ] Splits & dividends ingestion.
- [ ] Read-time adjustment helper in `storage/parquet_io.py` — `read_adjusted(symbol, ...)`.
- **Acceptance**: a known split (e.g., AAPL 2020-08-31 4:1) produces correct adjusted prices when read with `adjusted=True`.

### Slice 8 — Futures
- [ ] Contract discovery.
- [ ] Per-contract ingestion.
- [ ] Graceful handling of beta-API failures.
- **Acceptance**: at least ES contracts ingest cleanly, OR the failure path is hit and logged correctly without breaking other commands.

### Slice 9 — Validation
- [ ] Gap detection against market calendars.
- [ ] OHLC sanity, duplicates.
- [ ] `validate report` writes markdown.
- **Acceptance**: known-good data produces a clean report; injected synthetic gaps produce expected warnings.

### Slice 10 — `update` command
- [ ] Per-symbol delta computation.
- [ ] Cron-friendly: structured exit codes, file logging, no interactive prompts.
- **Acceptance**: 2 consecutive `update` runs on the same day; second is a no-op.

---

## 14. Acceptance Tests (Cross-Cutting)

Run these after the full build:

1. **Cold install**: `uv sync && uv run massive-fetch init && uv run massive-fetch reference update && uv run massive-fetch backfill crypto --timeframe=daily` succeeds end-to-end.
2. **Idempotency**: any backfill rerun within 1 hour writes zero new bars.
3. **Resumability**: killing a backfill mid-run with SIGTERM, then rerunning, completes without duplicates and without gaps.
4. **Disk budget**: full backfill at default scope fits under 25 GB.
5. **Survivorship disclosure**: `README.md` contains an explicit, prominent section describing the bias.

---

## 15. README Skeleton

The committed `README.md` must contain at minimum:

- Project description (1 paragraph).
- Quickstart: install via `uv`, set `MASSIVE_API_KEY`, run `init`, `reference update`, `backfill`.
- CLI command reference (link to or inline summary of §10).
- **A "Survivorship Bias Disclosure" section** stating, at minimum:
  > This tool's default stocks universe is "today's S&P 500 ∪ Nasdaq-100". Companies that were members of these indexes historically but have since been delisted, acquired, or removed are NOT included. Any backtest run on this dataset will be subject to survivorship bias — historical strategy performance will appear better than reality, particularly for long-biased and mean-reversion strategies. This is a known limitation of the v1 universe definition. To eliminate this bias, see Phase 2 plans for full Flat Files ingestion or point-in-time index membership.
- Link to `SPEC.md`.
- Link to Massive's docs.
- License.

---

## 16. Known Issues & Caveats

- **Futures REST is in beta.** Ingestion may fail until Massive promotes it to GA.
- **Wikipedia scraping is fragile.** If Wikipedia restructures the SP500/NDX pages, the scraper breaks. Mitigation: frozen YAML fallback.
- **Survivorship bias in stocks universe** — see README.
- **Per-symbol Parquet append requires read-merge-write.** For very large minute files (multi-GB), this is expensive. The year partitioning keeps individual writes manageable. If a year file grows beyond ~500 MB, consider month partitioning (config knob, Phase 2).
- **Timestamp unit mismatch between REST (ms) and Flat Files (ns)**. The normalization layer must explicitly handle whichever it's given. v1 only handles REST; Phase 2 must add the FF path.
- **Plan tier limits**: Massive's data history depth and rate limits depend on subscription tier. The tool surfaces 403/limit errors clearly but does not pre-validate plan capability.

---

## 17. Phase 2 Roadmap (not for v1)

Phase 2 is structured as ordered checkpoints. Each checkpoint is independently shippable and unlocks specific capabilities. The order reflects dependencies: 2a must come before 2b for survivorship bias elimination to actually work.

### Phase 2a — Flat Files ingestion (hybrid backfill)

**Goal**: enable bias-free historical backfill while keeping REST as the path for daily incrementals.

- Add `clients/flatfiles.py` — S3-compatible client (boto3) using `MASSIVE_S3_ACCESS_KEY` / `MASSIVE_S3_SECRET_KEY`.
- Add `ingest/flatfiles_stocks.py` — date-loop ingestion of `day_aggs/{date}.csv.gz` and `minute_aggs/{date}.csv.gz`.
- Add `transform/repartition.py` — converts date-partitioned raw files into the existing per-symbol Parquet layout.
- Extend the `manifest` to track Flat Files ingestion granularity (per `(asset, date)`) without disturbing the existing `(symbol, timeframe)` rows.
- New CLI: `massive-fetch backfill stocks --source=flatfiles --start=YYYY-MM-DD`.
- The `update` command continues to use REST for daily incrementals — no change.

**Outcome**: a "hybrid" steady state — historical bulk via Flat Files (bias-free), daily delta via REST (current-universe-only).
**Caveat**: REST incrementals still drift toward "today's universe" only. Eliminating that requires Phase 2c.

### Phase 2b — Point-in-time universe membership

**Goal**: knowing who was in the index on any historical date, so backtests can use the actual historical universe.

- Scrape Wikipedia's "List of S&P 500 changes" and "Nasdaq-100 historical components" pages.
- Persist as `reference/universe_membership.parquet` with schema: `(index, ticker, added_date, removed_date)`.
- New helper in storage: `get_universe_at(index, as_of_date) -> list[str]`.
- Existing `universe_stocks.parquet` (today's snapshot) is preserved for v1 compatibility.

**Outcome**: backtest code can ask "what was the S&P 500 on 2015-03-15?" and get the right answer for that date.
**Caveat**: only meaningful if the data layer has bars for delisted tickers — i.e., Phase 2a must be done first, or the membership lookup will return tickers you don't have data for.

### Phase 2c — Flat Files for daily incrementals

**Goal**: full bias-free pipeline. Replace REST-based `update` with Flat Files-based `update` for stocks.

- Daily Flat Files arrive ~11 AM ET T+1 — the cron schedule shifts accordingly.
- The same `update` command, with stocks now using the Flat Files daily file. Other asset classes unchanged.
- REST stocks ingestion remains available as a fallback / ad-hoc tool.

**Outcome**: every ticker that traded each day, including those subsequently delisted, lands in the cache automatically. Combined with Phase 2b, you have a fully bias-free historical dataset.

### Phase 2d — Cloud storage backends

**Goal**: realize the abstraction from §6.0.1.

- Implement `S3Backend` and `GCSBackend` against the `StorageBackend` protocol.
- Add fsspec dependency.
- Decide manifest strategy (local-only vs. Postgres/RDS vs. cached-from-cloud).
- Add deployment recipes: GitHub Actions on schedule writing to S3.

**Outcome**: set-and-forget cloud operation, accessible from any device.

### Phase 2e — Continuous futures

**Goal**: synthetic rolled series suitable for systematic backtesting.

- Implement multiple roll methods: calendar (last trade date), open-interest crossover, volume crossover.
- Output `processed/futures/continuous/{root}_{method}.parquet`.
- Phase 2 only because v1 per-contract data is sufficient for indicator validation; rolled series matter only at the strategy-backtest stage.

### Phase 2f — Operational polish

These are quality-of-life additions, not capability changes:

- `massive-fetch alert` command — posts run summaries / failures to Slack/Discord/email webhooks.
- Optional Streamlit dashboard reading from `manifest.sqlite`.
- A read-side library (`massive_data` companion package) with `read_adjusted()`, `get_universe_at()`, etc.

### Phase 3 (out of scope for now)

- Tick / trade / quote ingestion.
- Options data.
- Forex, indices.
- Alternative data (news, sentiment, fundamentals).

---

## 18. Glossary

- **Aggregate / bar**: an OHLCV summary over a time window.
- **Manifest**: SQLite database tracking what's been downloaded.
- **Universe**: the set of tickers/contracts/symbols being managed.
- **Survivorship bias**: see README and §1.
- **Adjusted prices**: prices retroactively modified to reflect splits/dividends.
- **Raw prices**: prices as they were on the trading day, unadjusted.
- **Extended hours**: pre-market (4:00–9:30 AM ET) and after-hours (4:00–8:00 PM ET) trading sessions.
- **Continuous futures**: a synthetic series stitching front-month contracts together — Phase 2 only.

---

End of spec.
