"""Unit tests for reference.universe — pandas.read_html mocked, no network (Slice 4).

The deterministic bar: every path (scrape success, scrape-failure fallback,
vintage preservation, cache short-circuit, ticker normalization, dedupe union,
no-YAML hard failure) is proven here. The live smoke (test_universe_smoke.py) is
not a substitute for any of these.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from massive_fetch.config import AppConfig
from massive_fetch.reference import freshness, universe
from massive_fetch.reference.universe import (
    ScrapeError,
    UniverseUnavailable,
    build_universe_df,
    dedupe_union,
    extract_tickers,
    normalize_ticker,
    select_constituents_table,
    update_stocks_universe,
    write_frozen_yaml,
)
from massive_fetch.storage import paths
from massive_fetch.storage.backend import LocalBackend

SP500_URL = universe._INDEX_SOURCES["SP500"][0]
NDX_URL = universe._INDEX_SOURCES["NDX"][0]


# --- canned Wikipedia tables ----------------------------------------------

def _changes_log_table(n: int = 100) -> pd.DataFrame:
    """A MultiIndex changes-log table (carries a ticker column, must be skipped)."""
    cols = pd.MultiIndex.from_tuples(
        [
            ("Date", "Date"),
            ("Added", "Ticker"),
            ("Added", "Security"),
            ("Removed", "Ticker"),
            ("Removed", "Security"),
            ("Reason", "Reason"),
        ]
    )
    return pd.DataFrame(
        [["2020-01-01", "XYZ", "X Co", "ABC", "A Co", "reason"]] * n, columns=cols
    )


def _constituents_table(symbols: list[str], ticker_col: str) -> pd.DataFrame:
    return pd.DataFrame({ticker_col: symbols, "Security": [f"{s} Inc" for s in symbols]})


# SP500: 61 rows incl. the dotted name BRK.B. NDX: 61 rows incl. dash-form BF-B
# (normalizes to BF.B) and an overlap (S50..S59) with SP500.
SP500_SYMS = ["BRK.B"] + [f"S{i}" for i in range(60)]
NDX_SYMS = ["BF-B"] + [f"S{i}" for i in range(50, 60)] + [f"N{i}" for i in range(50)]


def _sp500_tables() -> list[pd.DataFrame]:
    return [_constituents_table(SP500_SYMS, "Symbol"), _changes_log_table()]


def _ndx_tables() -> list[pd.DataFrame]:
    # Mirror the real page: nav/numeric-header tables, constituents, changes-log.
    nav = pd.DataFrame({0: ["a", "b"], 1: ["c", "d"]})
    return [nav, _constituents_table(NDX_SYMS, "Ticker"), _changes_log_table(220)]


def _expected_union() -> list[str]:
    return dedupe_union(
        [normalize_ticker(s) for s in SP500_SYMS],
        [normalize_ticker(s) for s in NDX_SYMS],
    )


@pytest.fixture
def patch_scrape_ok(mocker):
    """Patch the network seam so scrape() returns canned tables. Returns the mock."""

    def fake_read_tables(url: str) -> list[pd.DataFrame]:
        return _ndx_tables() if "Nasdaq" in url else _sp500_tables()

    return mocker.patch.object(
        universe, "_read_tables", side_effect=fake_read_tables
    )


@pytest.fixture
def cfg(tmp_path):
    """Default config with the frozen YAML pointed at tmp (never the repo file)."""
    c = AppConfig()
    c.ingest.stocks.frozen_fallback_path = tmp_path / "frozen.yaml"
    return c


@pytest.fixture
def backend(tmp_path):
    return LocalBackend(tmp_path)


# --- pure helpers ----------------------------------------------------------

@pytest.mark.parametrize(
    "raw,expected",
    [
        ("BRK.B", "BRK.B"),   # dot form passes through (Massive + Wikipedia form)
        ("BRK-B", "BRK.B"),   # dash -> dot
        ("BF.B", "BF.B"),     # second real dotted name (reviewer #3)
        ("BF-B", "BF.B"),
        ("bf.b", "BF.B"),     # lowercase -> upper
        ("  AAPL  ", "AAPL"),  # whitespace stripped
    ],
)
def test_normalize_ticker(raw, expected):
    assert normalize_ticker(raw) == expected


def test_normalize_ticker_is_idempotent():
    assert normalize_ticker(normalize_ticker("brk-b")) == "BRK.B"


def test_select_constituents_table_picks_flat_skips_changes_log():
    tables = [_changes_log_table(), _constituents_table(SP500_SYMS, "Symbol")]
    chosen = select_constituents_table(tables, "Symbol")
    assert "Symbol" in chosen.columns
    assert len(chosen) == len(SP500_SYMS)


def test_select_constituents_table_raises_when_only_changes_log():
    # The changes-log has a ('Added','Ticker') tuple column, not a flat "Ticker".
    with pytest.raises(ScrapeError):
        select_constituents_table([_changes_log_table(220)], "Ticker")


def test_select_constituents_table_respects_min_rows():
    small = _constituents_table(["AAA", "BBB"], "Symbol")
    with pytest.raises(ScrapeError):
        select_constituents_table([small], "Symbol", min_rows=50)


def test_extract_tickers_normalizes_and_drops_nan():
    df = pd.DataFrame({"Symbol": ["AAPL", "brk-b", None, float("nan"), "  msft "],
                       "Security": [1, 2, 3, 4, 5]})
    got = extract_tickers([df], "Symbol", min_rows=1)
    assert got == ["AAPL", "BRK.B", "MSFT"]


def test_dedupe_union_sorted_unique():
    assert dedupe_union(["B", "A", "B"], ["C", "A"]) == ["A", "B", "C"]


# --- orchestrator: scrape success -----------------------------------------

def test_success_writes_parquet_and_yaml(cfg, backend, rec_logger, patch_scrape_ok):
    result = update_stocks_universe(backend=backend, config=cfg, logger=rec_logger)

    expected = _expected_union()
    assert result.source == universe.SOURCE_WIKIPEDIA
    assert result.ticker_count == len(expected)
    assert not result.cached and not result.used_fallback

    # Parquet: canonical columns, sorted, dot-formed normalization applied.
    df = backend.read_parquet(paths.universe_stocks_key())
    assert df.columns == list(universe.UNIVERSE_COLUMNS)
    assert df["ticker"].to_list() == expected
    assert df["ticker"].to_list() == sorted(df["ticker"].to_list())
    assert set(df["source"].unique().to_list()) == {universe.SOURCE_WIKIPEDIA}
    assert "BRK.B" in expected and "BF.B" in expected  # both dotted names present

    # YAML: §5.1 schema, same generated_at as the parquet (single stamp).
    yaml_tickers, yaml_vintage = universe.read_frozen_yaml(cfg.ingest.stocks.frozen_fallback_path)
    assert yaml_tickers == expected
    parquet_vintage = df.get_column("generated_at").max()
    assert parquet_vintage == yaml_vintage


def test_success_with_missing_parquet_does_scrape(cfg, backend, rec_logger, patch_scrape_ok):
    # No parquet exists yet -> MISSING -> not fresh -> scrape runs.
    assert not backend.exists(paths.universe_stocks_key())
    update_stocks_universe(backend=backend, config=cfg, logger=rec_logger)
    assert patch_scrape_ok.call_count == 2  # SP500 + NDX


# --- orchestrator: scrape failure -> frozen-YAML fallback ------------------

def _seed_frozen_yaml(path, tickers, *, generated_at, source=universe.SOURCE_WIKIPEDIA):
    write_frozen_yaml(
        path, tickers, source=source, indexes=["SP500", "NDX"], generated_at=generated_at
    )


def test_scrape_failure_falls_back_to_frozen_yaml(cfg, backend, rec_logger, mocker):
    frozen_tickers = ["AAA", "BBB", "BRK.B"]
    recent = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(days=1)
    _seed_frozen_yaml(cfg.ingest.stocks.frozen_fallback_path, frozen_tickers, generated_at=recent)

    mocker.patch.object(universe, "_read_tables", side_effect=ScrapeError("boom"))
    result = update_stocks_universe(backend=backend, config=cfg, logger=rec_logger)

    assert result.used_fallback
    assert result.source == universe.SOURCE_FROZEN_YAML
    assert result.ticker_count == len(frozen_tickers)

    df = backend.read_parquet(paths.universe_stocks_key())
    assert df["ticker"].to_list() == sorted(frozen_tickers)
    assert set(df["source"].unique().to_list()) == {universe.SOURCE_FROZEN_YAML}

    # WARN logged exactly once.
    warns = [e for e in rec_logger.events if e["event"] == "reference.universe.scrape_failed"]
    assert len(warns) == 1 and warns[0]["level"] == "warning"


def test_fallback_preserves_vintage_flags_stale(cfg, backend, rec_logger, mocker):
    """The §10.3-critical test: a broken scraper must NOT report 'fresh'.

    Frozen YAML is stamped ~30 days ago; on fallback the parquet's generated_at must
    be that same ~30-day-old vintage (never now()), so freshness flags STALE.
    """
    vintage = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(days=30)
    _seed_frozen_yaml(cfg.ingest.stocks.frozen_fallback_path, ["AAA", "BBB"], generated_at=vintage)

    mocker.patch.object(universe, "_read_tables", side_effect=ScrapeError("dead"))
    result = update_stocks_universe(backend=backend, config=cfg, logger=rec_logger)

    # Parquet carries the OLD vintage, not write-time.
    assert result.generated_at.date() == vintage.date()
    df = backend.read_parquet(paths.universe_stocks_key())
    assert df.get_column("generated_at").max().date() == vintage.date()

    # And freshness flags STALE (30 > 2 x 7), not OK.
    fr = freshness.universe_freshness(backend, cfg)
    assert fr.flag == freshness.FLAG_STALE
    assert fr.age_days >= 29


def test_no_frozen_yaml_hard_failure(cfg, backend, rec_logger, mocker):
    assert not cfg.ingest.stocks.frozen_fallback_path.exists()
    mocker.patch.object(universe, "_read_tables", side_effect=ScrapeError("dead"))

    with pytest.raises(UniverseUnavailable):
        update_stocks_universe(backend=backend, config=cfg, logger=rec_logger)

    assert not backend.exists(paths.universe_stocks_key())


# --- orchestrator: cache short-circuit -------------------------------------

def test_cache_short_circuit_skips_scrape(cfg, backend, rec_logger, mocker):
    # Seed a fresh parquet (generated_at = now) directly.
    now = datetime.now(timezone.utc).replace(microsecond=0)
    df = build_universe_df(["AAA", "BBB"], source=universe.SOURCE_WIKIPEDIA, generated_at=now)
    backend.write_parquet(paths.universe_stocks_key(), df)

    spy = mocker.patch.object(universe, "_read_tables")
    result = update_stocks_universe(backend=backend, config=cfg, logger=rec_logger)

    assert spy.call_count == 0  # short-circuited BEFORE any network call
    assert result.cached
    assert result.source == "cache"
    assert result.ticker_count == 2


def test_force_bypasses_cache(cfg, backend, rec_logger, patch_scrape_ok):
    # Seed a fresh parquet, then --force must re-scrape anyway.
    now = datetime.now(timezone.utc).replace(microsecond=0)
    df = build_universe_df(["OLD"], source=universe.SOURCE_WIKIPEDIA, generated_at=now)
    backend.write_parquet(paths.universe_stocks_key(), df)

    result = update_stocks_universe(backend=backend, config=cfg, logger=rec_logger, force=True)

    assert patch_scrape_ok.call_count == 2  # scraped despite fresh cache
    assert not result.cached
    assert result.source == universe.SOURCE_WIKIPEDIA
    assert result.ticker_count == len(_expected_union())
