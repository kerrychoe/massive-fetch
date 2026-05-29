"""Live smoke test (SPEC §13 Slice 3): a tight, recent BTC minute window, normalized.

Gated — only runs when MASSIVE_INTEGRATION=1 and MASSIVE_API_KEY is set, so the
default `uv run pytest` never touches the network.

Uses a RECENT window (yesterday-2d .. yesterday, UTC) rather than a fixed 2024
date: the plan-tier minute history is capped (see the api-tier-history-cap note),
so an old start may return nothing. We assert against what the tier returns
(minute-resolution data exists, canonical schema), not an exact bar count.
"""

from __future__ import annotations

import datetime as dt
import os

import pytest
import structlog

from massive_fetch.clients.rest import MassiveRESTClient
from massive_fetch.config import APIConfig
from massive_fetch.transform.normalize import CANONICAL_SCHEMA, normalize

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("MASSIVE_INTEGRATION") != "1" or not os.getenv("MASSIVE_API_KEY"),
        reason="set MASSIVE_INTEGRATION=1 and MASSIVE_API_KEY to run live smoke tests",
    ),
]

TICKER = "X:BTCUSD"


def _recent_window() -> tuple[str, str]:
    yesterday = dt.datetime.now(dt.timezone.utc).date() - dt.timedelta(days=1)
    return (yesterday - dt.timedelta(days=2)).isoformat(), yesterday.isoformat()


async def test_btc_minute_smoke():
    from_date, to_date = _recent_window()
    config = APIConfig()
    async with MassiveRESTClient(os.environ["MASSIVE_API_KEY"], config, structlog.get_logger()) as c:
        bars = [b async for b in c.list_aggs(TICKER, 1, "minute", from_date, to_date)]

    df = normalize(bars, TICKER)

    # 24/7 crypto over ~3 days at 1-min resolution -> thousands of bars normally;
    # assert a loose floor that still proves minute (not daily) data was returned.
    assert df.height > 100, f"unexpectedly few minute bars: {df.height}"

    # Canonical §6.1 schema, exactly.
    for col, dtype in CANONICAL_SCHEMA.items():
        assert df.schema[col] == dtype, f"{col}: {df.schema[col]} != {dtype}"

    # Strictly increasing timestamps.
    ts = df["timestamp"].to_list()
    assert all(b > a for a, b in zip(ts, ts[1:])), "timestamps not strictly increasing"

    # No nulls in required fields.
    for col in ("timestamp", "open", "high", "low", "close", "volume"):
        assert df[col].null_count() == 0, f"nulls in {col}"
