"""Live smoke test (SPEC §13 Slice 1 acceptance): 1 day of AAPL minute bars.

Gated — only runs when MASSIVE_INTEGRATION=1 and MASSIVE_API_KEY is set, so the
default `uv run pytest` never touches the network.
"""

from __future__ import annotations

import os

import pytest
import structlog

from massive_fetch.clients.rest import MassiveRESTClient
from massive_fetch.config import APIConfig

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("MASSIVE_INTEGRATION") != "1" or not os.getenv("MASSIVE_API_KEY"),
        reason="set MASSIVE_INTEGRATION=1 and MASSIVE_API_KEY to run live smoke tests",
    ),
]

# Fixed, known-good regular trading session (validated during SDK discovery).
# Hardcoded rather than computed so the bar-count assertion stays stable across
# runs and is never thrown off by weekends/holidays.
SMOKE_DAY = "2026-05-22"


async def test_aapl_minute_smoke():
    config = APIConfig()
    async with MassiveRESTClient(os.environ["MASSIVE_API_KEY"], config, structlog.get_logger()) as c:
        bars = [b async for b in c.list_aggs("AAPL", 1, "minute", SMOKE_DAY, SMOKE_DAY)]

    # Regular session is ~390 bars; extended hours push it higher.
    assert 350 <= len(bars) <= 1000, f"unexpected bar count {len(bars)}"

    # Timestamps strictly monotonic increasing.
    ts = [b.timestamp for b in bars]
    assert all(b > a for a, b in zip(ts, ts[1:])), "timestamps not strictly increasing"

    for b in bars:
        # No nulls in required fields.
        assert None not in (b.open, b.high, b.low, b.close, b.volume, b.timestamp)
        # OHLC sanity.
        assert b.low <= b.open <= b.high
        assert b.low <= b.close <= b.high
        assert b.low <= b.high
