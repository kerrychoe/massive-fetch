"""Live smoke test (SPEC §13 Slice 4): scrape Wikipedia SP500 ∪ NDX.

Gated — runs only when MASSIVE_INTEGRATION=1. Unlike the OHLCV smokes this needs
**no** API key: it hits Wikipedia, not Massive. So the default `uv run pytest`
never touches the network.

This calls the **raw scrape()** directly (not update_stocks_universe). That is
deliberate: update_stocks_universe falls back to the frozen YAML on a broken scrape
and would still yield ~516 tickers — passing a loose count check while the scraper
is actually dead. That silent failure is the exact §10.3 hazard this slice exists to
surface, so the smoke asserts the live scrape itself succeeds. A Wikipedia failure
raises ScrapeError here and the test errors loudly.
"""

from __future__ import annotations

import os

import pytest

from massive_fetch.reference.universe import normalize_ticker, scrape

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("MASSIVE_INTEGRATION") != "1",
        reason="set MASSIVE_INTEGRATION=1 to run the live Wikipedia scrape smoke test",
    ),
]


def test_wikipedia_scrape_yields_full_universe():
    # Raw scrape -> a Wikipedia failure RAISES (no silent frozen-YAML fallback).
    tickers = scrape(["SP500", "NDX"])

    # Loose band: membership drifts; today's real union is ~516 ("~600" in §13 is
    # the un-deduped 503+100 figure). 450–650 brackets reality without letting a
    # partial scrape through.
    assert 450 <= len(tickers) <= 650, f"unexpected universe size {len(tickers)}"

    # Deduped + sorted, no blanks.
    assert tickers == sorted(set(tickers))
    assert all(t and t.strip() for t in tickers)

    # Canonical dot form: the multi-class names are present and dotted, none dashed.
    assert "BRK.B" in tickers
    assert "BF.B" in tickers
    assert not any("-" in t for t in tickers)
    assert all(normalize_ticker(t) == t for t in tickers)  # already canonical
