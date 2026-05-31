"""Stocks universe: S&P 500 ∪ Nasdaq-100 (SPEC §5.1, §13 Slice 4).

This is **reference** data, not OHLCV ingestion: it scrapes **Wikipedia**, not the
Massive API. None of the ingestion machinery (SDK wrapper, ``fetch_many``,
``normalize``, ``AggsRequest``, manifest resume) is used here.

Flow on ``reference update --scope=stocks`` (SPEC §5.1):

1. **Cache short-circuit.** If a ``universe_stocks.parquet`` already exists and its
   ``generated_at`` is within ``ingest.stocks.refresh_interval_days`` (7), do nothing
   — no network (``--force`` overrides). Missing parquet ⇒ not fresh ⇒ scrape.
2. **Scrape success.** Fetch each configured index page, parse with
   ``pandas.read_html``, normalize tickers to Massive's **dot** form, dedupe the
   union, stamp ``generated_at = now (UTC)``, and write **both**: the frozen YAML
   (repo config) and ``universe_stocks.parquet`` (StorageBackend data). Both carry
   the *same* ``generated_at``.
3. **Scrape failure** (network / page restructure / no matching table): log WARN
   once, read the frozen YAML, and write the parquet **from it** — stamping the
   parquet's ``generated_at`` with the **YAML's** ``generated_at`` (the data's true
   vintage), never ``now()``. A scraper broken for weeks therefore reports the old
   vintage so §10.3 staleness detection still flags STALE (this is the entire point
   of preserving the vintage). If no frozen YAML exists, raise.

The universe parquet is a **full snapshot replaced on every refresh** — written
with a full-replace ``write_parquet``, never ``append_parquet``/``dedupe_on`` (that
is the daily/minute *incremental* pattern and does not apply to a snapshot).

**Source-of-truth split (SPEC §6.0.1):** the YAML is repo config, written directly;
the parquet is data, written via the ``StorageBackend`` keyed by
``paths.universe_stocks_key()``. No path logic against the data dir lives here.

**Discovery (Slice 4, captured live before this code):** ``pandas.read_html`` returns
3 tables for the S&P 500 page (constituents = table 0, ticker column ``"Symbol"``)
and ~20 for the Nasdaq-100 page (constituents = table 5, ticker column ``"Ticker"``).
The NDX table index is *not* stable across edits, so constituents tables are selected
by **column signature**, not a fixed index; the changes-log tables (which also carry a
ticker column) have ``MultiIndex`` tuple headers and are skipped by requiring flat
string columns. Tickers render in **dot** form on the page (``BRK.B``, ``BF.B``; zero
dash forms), which already matches Massive — normalization maps any dash form to a dot
and is otherwise a pass-through.
"""

from __future__ import annotations

import io
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import polars as pl
import structlog

from massive_fetch.config import AppConfig
from massive_fetch.reference import freshness
from massive_fetch.storage import paths
from massive_fetch.storage.backend import StorageBackend

# Source markers stored in the parquet's ``source`` column and the YAML.
SOURCE_WIKIPEDIA = "wikipedia"
SOURCE_FROZEN_YAML = "frozen_yaml"

# Canonical universe parquet columns (SPEC §5.1 / §10.3). ``generated_at`` is a
# column (every row identical) so it survives the parquet roundtrip and the
# fallback-vintage rule without touching the StorageBackend abstraction; §10.3
# reads ``generated_at.max()``.
UNIVERSE_COLUMNS: tuple[str, ...] = ("ticker", "source", "generated_at")

# Per-index Wikipedia source: (URL, the literal ticker column on that page).
# Verified by Slice-4 discovery; see the module docstring.
_INDEX_SOURCES: dict[str, tuple[str, str]] = {
    "SP500": ("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies", "Symbol"),
    "NDX": ("https://en.wikipedia.org/wiki/Nasdaq-100", "Ticker"),
}

# Wikipedia 403s the default urllib User-Agent; send a descriptive one.
_USER_AGENT = "Mozilla/5.0 (compatible; massive-fetch/0.1; +https://massive.com)"
_HTTP_TIMEOUT_SECONDS = 30

# A constituents table must be at least this many rows — guards against picking a
# small spurious table that happens to carry the ticker column header.
_MIN_CONSTITUENTS_ROWS = 50


class ScrapeError(Exception):
    """Any Wikipedia scrape/parse failure (network, restructure, no matching table).

    Caught by :func:`update_stocks_universe`, which falls back to the frozen YAML.
    """


class UniverseUnavailable(Exception):
    """Scrape failed **and** no frozen YAML exists — cannot produce a universe."""


class MissingUniverseError(Exception):
    """The stocks universe parquet is absent or empty when ingestion needs it."""


def load_universe_tickers(backend: StorageBackend) -> list[str]:
    """Return the stocks universe tickers for ingestion (SPEC §8.1).

    Reads ``reference/universe_stocks.parquet`` (column ``ticker``, dot form e.g.
    ``BRK.B`` — the Massive form). Raises :class:`MissingUniverseError` when the
    parquet is **absent** OR present but holds **zero data rows**: an empty universe
    must not silently ingest nothing. Build it first with
    ``massive-fetch reference update --scope stocks``.
    """
    key = paths.universe_stocks_key()
    if not backend.exists(key):
        raise MissingUniverseError(
            f"Stocks universe is missing ({key}). "
            "Build it first: massive-fetch reference update --scope stocks"
        )
    df = backend.read_parquet(key)
    tickers = df["ticker"].to_list() if "ticker" in df.columns else []
    if not tickers:
        raise MissingUniverseError(
            f"Stocks universe is empty ({key}). "
            "Rebuild it: massive-fetch reference update --scope stocks --force"
        )
    return tickers


@dataclass
class UniverseUpdateResult:
    """Outcome of :func:`update_stocks_universe`. Drives the CLI summary."""

    source: str  # SOURCE_WIKIPEDIA | SOURCE_FROZEN_YAML | "cache"
    ticker_count: int
    generated_at: datetime
    cached: bool = False        # short-circuited on fresh cache: no scrape, no write
    used_fallback: bool = False  # scrape failed, parquet written from frozen YAML


# --- Pure helpers (no IO; unit-tested directly) ---------------------------

def normalize_ticker(raw: str) -> str:
    """Normalize a raw ticker to Massive's canonical **dot** form.

    Idempotent: ``BRK.B`` → ``BRK.B``, ``BRK-B`` → ``BRK.B``, ``bf.b`` → ``BF.B``,
    surrounding whitespace stripped. Massive and the Wikipedia pages both use the
    dot form (Slice-4 discovery), so for present data this is effectively a
    pass-through; the dash→dot mapping future-proofs against either rendering.
    """
    return raw.strip().upper().replace("-", ".")


def select_constituents_table(
    tables: list[pd.DataFrame], ticker_col: str, *, min_rows: int = _MIN_CONSTITUENTS_ROWS
) -> pd.DataFrame:
    """Pick the constituents table by column signature, not a fixed index (§16).

    Returns the first table whose columns are all flat strings (excludes the
    ``MultiIndex`` changes-log tables), contains ``ticker_col`` exactly, and has at
    least ``min_rows`` rows. Raises :class:`ScrapeError` if none matches — the
    "Wikipedia restructured the page" signal that triggers the frozen-YAML fallback.
    """
    for table in tables:
        cols = list(table.columns)
        if any(not isinstance(c, str) for c in cols):
            continue  # MultiIndex (changes log) or numeric-header nav table
        if ticker_col in cols and len(table) >= min_rows:
            return table
    raise ScrapeError(
        f"no constituents table with a flat {ticker_col!r} column "
        f"(>= {min_rows} rows) — Wikipedia page may have been restructured"
    )


def extract_tickers(
    tables: list[pd.DataFrame], ticker_col: str, *, min_rows: int = _MIN_CONSTITUENTS_ROWS
) -> list[str]:
    """Select the constituents table and return its normalized tickers (may dupe)."""
    table = select_constituents_table(tables, ticker_col, min_rows=min_rows)
    out: list[str] = []
    for raw in table[ticker_col].tolist():
        if raw is None:
            continue
        s = str(raw).strip()
        if not s or s.lower() == "nan":
            continue
        out.append(normalize_ticker(s))
    return out


def dedupe_union(*ticker_lists: list[str]) -> list[str]:
    """Sorted, de-duplicated union of one or more ticker lists (SPEC §5.1 step 4)."""
    merged: set[str] = set()
    for lst in ticker_lists:
        merged.update(lst)
    return sorted(merged)


# --- Scrape (raw network path) --------------------------------------------

def _http_get(url: str) -> str:
    request = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    with urllib.request.urlopen(request, timeout=_HTTP_TIMEOUT_SECONDS) as response:
        return response.read().decode("utf-8")


def _read_tables(url: str) -> list[pd.DataFrame]:
    """Fetch ``url`` and parse all HTML tables. The single seam unit tests patch."""
    return pd.read_html(io.StringIO(_http_get(url)))


def scrape(
    indexes: list[str], *, logger: structlog.stdlib.BoundLogger | None = None
) -> list[str]:
    """Scrape the configured index pages → normalized, deduped union of tickers.

    Hits Wikipedia. Raises :class:`ScrapeError` on any failure (unknown index,
    network error, page restructure, empty extraction) so callers — including the
    gated live smoke — see a hard failure rather than a silent fallback.
    """
    per_index: list[list[str]] = []
    for index in indexes:
        if index not in _INDEX_SOURCES:
            raise ScrapeError(f"unknown index {index!r}; known: {sorted(_INDEX_SOURCES)}")
        url, ticker_col = _INDEX_SOURCES[index]
        try:
            tables = _read_tables(url)
        except ScrapeError:
            raise
        except Exception as exc:  # network / decode / pandas parse errors
            raise ScrapeError(f"failed to fetch/parse {index} from {url}: {exc}") from exc
        tickers = extract_tickers(tables, ticker_col)
        if not tickers:
            raise ScrapeError(f"no tickers extracted for {index} from {url}")
        if logger is not None:
            logger.debug("reference.universe.index_scraped", index=index, count=len(tickers))
        per_index.append(tickers)
    return dedupe_union(*per_index)


# --- Frozen YAML (repo config) --------------------------------------------

def _repo_root() -> Path:
    # src/massive_fetch/reference/universe.py -> reference -> massive_fetch -> src -> root
    return Path(__file__).resolve().parents[3]


def _resolve_frozen_path(config: AppConfig) -> Path:
    """Resolve the frozen YAML path: absolute as-is, relative against the repo root."""
    path = Path(config.ingest.stocks.frozen_fallback_path)
    return path if path.is_absolute() else _repo_root() / path


def _iso_z(dt: datetime) -> str:
    """Format a UTC datetime as ``YYYY-MM-DDTHH:MM:SSZ`` (SPEC §5.1 example form)."""
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_iso(value: str) -> datetime:
    """Parse an ISO-8601 timestamp (``Z`` or offset) into a tz-aware UTC datetime."""
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def write_frozen_yaml(
    path: Path,
    tickers: list[str],
    *,
    source: str,
    indexes: list[str],
    generated_at: datetime,
) -> None:
    """Write the frozen fallback YAML (SPEC §5.1 schema). Direct repo write, not a
    StorageBackend key — the YAML is config, not data (§6.0.1)."""
    import yaml  # local import: yaml is a config concern, kept off the hot path

    path.parent.mkdir(parents=True, exist_ok=True)
    document = {
        "generated_at": _iso_z(generated_at),
        "source": source,
        "indexes_included": list(indexes),
        "tickers": list(tickers),
    }
    with path.open("w", encoding="utf-8") as f:
        yaml.safe_dump(document, f, sort_keys=False, default_flow_style=False)


def read_frozen_yaml(path: Path) -> tuple[list[str], datetime]:
    """Read tickers and the **stored** ``generated_at`` (vintage) from the YAML."""
    import yaml

    with path.open("r", encoding="utf-8") as f:
        document = yaml.safe_load(f)
    return list(document["tickers"]), _parse_iso(document["generated_at"])


# --- Parquet snapshot (StorageBackend data) -------------------------------

def build_universe_df(tickers: list[str], *, source: str, generated_at: datetime) -> pl.DataFrame:
    """Build the canonical universe snapshot frame (sorted ascending by ticker)."""
    ordered = sorted(tickers)
    return pl.DataFrame(
        {
            "ticker": ordered,
            "source": [source] * len(ordered),
            "generated_at": [generated_at] * len(ordered),
        }
    ).with_columns(pl.col("generated_at").cast(pl.Datetime("us", "UTC")))


# --- Orchestrator ---------------------------------------------------------

def update_stocks_universe(
    *,
    backend: StorageBackend,
    config: AppConfig,
    logger: structlog.stdlib.BoundLogger,
    force: bool = False,
) -> UniverseUpdateResult:
    """Refresh the stocks universe. See the module docstring for the full contract."""
    indexes = list(config.ingest.stocks.indexes)
    key = paths.universe_stocks_key()

    # 1. Cache short-circuit (SPEC §13: rerun within 7 days uses cache). "OK" means
    #    age <= refresh_interval_days; --force bypasses. Missing parquet -> MISSING
    #    -> not fresh -> scrape.
    if not force:
        fresh = freshness.universe_freshness(backend, config)
        if fresh.exists and fresh.flag == "OK":
            logger.info(
                "reference.universe.cache_fresh",
                age_days=fresh.age_days,
                ticker_count=fresh.ticker_count,
                refresh_interval_days=config.ingest.stocks.refresh_interval_days,
            )
            return UniverseUpdateResult(
                source="cache",
                ticker_count=fresh.ticker_count or 0,
                generated_at=fresh.generated_at,  # not None when exists
                cached=True,
            )

    # 2. Scrape; on failure fall back to the frozen YAML, preserving its vintage.
    try:
        tickers = scrape(indexes, logger=logger)
    except ScrapeError as exc:
        logger.warning("reference.universe.scrape_failed", error=str(exc))
        frozen_path = _resolve_frozen_path(config)
        if not frozen_path.exists():
            raise UniverseUnavailable(
                f"Wikipedia scrape failed and no frozen fallback exists at {frozen_path}. "
                f"Cannot produce a stocks universe."
            ) from exc
        frozen_tickers, vintage = read_frozen_yaml(frozen_path)
        # HARD CONSTRAINT (SPEC §10.3): stamp the parquet with the YAML's vintage,
        # NEVER now() — else a long-broken scraper reports "0 days ago" and staleness
        # detection silently breaks.
        df = build_universe_df(frozen_tickers, source=SOURCE_FROZEN_YAML, generated_at=vintage)
        backend.write_parquet(key, df)
        logger.info(
            "reference.universe.fallback_written",
            ticker_count=len(frozen_tickers),
            generated_at=_iso_z(vintage),
            frozen_path=str(frozen_path),
        )
        return UniverseUpdateResult(
            source=SOURCE_FROZEN_YAML,
            ticker_count=len(frozen_tickers),
            generated_at=vintage,
            used_fallback=True,
        )

    # 3. Success: stamp once, write BOTH (YAML first — the durable fallback — then
    #    parquet), carrying the same generated_at.
    generated_at = datetime.now(timezone.utc).replace(microsecond=0)
    write_frozen_yaml(
        _resolve_frozen_path(config),
        tickers,
        source=SOURCE_WIKIPEDIA,
        indexes=indexes,
        generated_at=generated_at,
    )
    df = build_universe_df(tickers, source=SOURCE_WIKIPEDIA, generated_at=generated_at)
    backend.write_parquet(key, df)
    logger.info(
        "reference.universe.scraped",
        ticker_count=len(tickers),
        generated_at=_iso_z(generated_at),
        indexes=indexes,
    )
    return UniverseUpdateResult(
        source=SOURCE_WIKIPEDIA, ticker_count=len(tickers), generated_at=generated_at
    )
