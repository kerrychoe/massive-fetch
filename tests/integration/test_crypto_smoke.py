"""Live smoke test (SPEC §13 Slice 2): 7 days of BTC daily bars, normalized.

Gated — only runs when MASSIVE_INTEGRATION=1 and MASSIVE_API_KEY is set, so the
default `uv run pytest` never touches the network.
"""

from __future__ import annotations

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

# Fixed historical window for a stable bar count (BTC trades 24/7).
TICKER = "X:BTCUSD"
FROM_DATE = "2024-01-01"
TO_DATE = "2024-01-07"


async def test_btc_daily_smoke():
    config = APIConfig()
    async with MassiveRESTClient(os.environ["MASSIVE_API_KEY"], config, structlog.get_logger()) as c:
        bars = [b async for b in c.list_aggs(TICKER, 1, "day", FROM_DATE, TO_DATE)]

    df = normalize(bars, TICKER)

    # 7 inclusive days, 24/7 -> expect ~7 daily bars.
    assert 6 <= df.height <= 9, f"unexpected bar count {df.height}"

    # Canonical §6.1 schema, exactly.
    for col, dtype in CANONICAL_SCHEMA.items():
        assert df.schema[col] == dtype, f"{col}: {df.schema[col]} != {dtype}"

    # Strictly increasing timestamps.
    ts = df["timestamp"].to_list()
    assert all(b > a for a, b in zip(ts, ts[1:])), "timestamps not strictly increasing"

    # No nulls in required fields.
    for col in ("timestamp", "open", "high", "low", "close", "volume"):
        assert df[col].null_count() == 0, f"nulls in {col}"
