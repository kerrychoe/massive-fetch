"""Live smoke test (SPEC §13 Slice 5): stocks daily for a small subset, normalized.

Gated — only runs when MASSIVE_INTEGRATION=1 and MASSIVE_API_KEY is set, so the
default `uv run pytest` never touches the network. Exercises the real
``ingest_stocks_daily`` path end-to-end (fetch RAW adjusted=false -> normalize ->
append -> manifest) over a recent, tier-safe window at the default concurrency=3.
"""

from __future__ import annotations

import datetime as dt
import os

import pytest
import structlog

from massive_fetch.clients.rest import MassiveRESTClient
from massive_fetch.config import AppConfig
from massive_fetch.ingest.stocks import ingest_stocks_daily
from massive_fetch.reference.calendar import nyse_session_count, nyse_target_end
from massive_fetch.storage import paths
from massive_fetch.storage.backend import LocalBackend
from massive_fetch.storage.manifest import Manifest
from massive_fetch.transform.normalize import CANONICAL_SCHEMA

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("MASSIVE_INTEGRATION") != "1" or not os.getenv("MASSIVE_API_KEY"),
        reason="set MASSIVE_INTEGRATION=1 and MASSIVE_API_KEY to run live smoke tests",
    ),
]

SUBSET = ["AAPL", "MSFT"]


async def test_stocks_daily_smoke(tmp_path):
    cfg = AppConfig()  # concurrency=3 default — not tuned
    backend = LocalBackend(tmp_path)
    manifest = Manifest(tmp_path / "manifest.sqlite")
    manifest.initialize()
    log = structlog.get_logger()

    # Recent, tier-safe window: ~12 calendar days back from the last complete session.
    end = nyse_target_end()
    start = (dt.date.fromisoformat(end) - dt.timedelta(days=12)).isoformat()
    expected_sessions = nyse_session_count(start, end)

    async with MassiveRESTClient(os.environ["MASSIVE_API_KEY"], cfg.api, log) as client:
        result = await ingest_stocks_daily(
            config=cfg, backend=backend, manifest=manifest, client=client,
            logger=log, symbols=SUBSET, start=start, end=end,
        )

    assert set(result.succeeded) == set(SUBSET), f"unexpected dispositions: {result}"

    for sym in SUBSET:
        df = backend.read_parquet(paths.stocks_daily_key(sym))

        # Canonical §6.1 schema, exactly.
        for col, dtype in CANONICAL_SCHEMA.items():
            assert df.schema[col] == dtype, f"{sym} {col}: {df.schema[col]} != {dtype}"

        assert df["symbol"].to_list() == [sym] * df.height
        ts = df["timestamp"].to_list()
        assert all(b > a for a, b in zip(ts, ts[1:])), f"{sym}: timestamps not strictly increasing"

        # Loose band around the expected NYSE sessions in the window (tier/closure slack).
        assert expected_sessions - 2 <= df.height <= expected_sessions + 2, (
            f"{sym}: {df.height} bars vs ~{expected_sessions} sessions [{start}..{end}]"
        )

        row = manifest.get_state("stocks", sym, "daily")
        assert row["bar_count"] == df.height
