"""Unit tests for ingest.crypto — SDK fully mocked, no network (SPEC §13 Slice 2)."""

from __future__ import annotations

import datetime as dt

import pytest
from massive.exceptions import BadResponse
from massive.rest.models import Agg
from urllib3.exceptions import MaxRetryError

from massive_fetch.clients.rest import MassiveAuthError, MassiveRESTClient
from massive_fetch.config import AppConfig
from massive_fetch.ingest.crypto import ingest_crypto_daily
from massive_fetch.storage import paths
from massive_fetch.storage.backend import LocalBackend
from massive_fetch.storage.manifest import Manifest

_DAY_MS = 86_400_000
_JAN1_2018_MS = 1_514_764_800_000  # 2018-01-01 00:00:00 UTC


def _daily_aggs(start_ms: int, n: int) -> list[Agg]:
    return [
        Agg.from_dict(
            {"o": 1.0, "h": 2.0, "l": 0.5, "c": 1.5, "v": 10.0 + i, "vw": 1.2, "t": start_ms + i * _DAY_MS, "n": 5}
        )
        for i in range(n)
    ]


def _yesterday_ms() -> int:
    y = dt.datetime.now(dt.timezone.utc).date() - dt.timedelta(days=1)
    return int(dt.datetime(y.year, y.month, y.day, tzinfo=dt.timezone.utc).timestamp() * 1000)


@pytest.fixture
def env(tmp_path):
    """A backend + initialized manifest under tmp_path."""
    backend = LocalBackend(tmp_path)
    manifest = Manifest(tmp_path / "manifest.sqlite")
    manifest.initialize()
    return backend, manifest


def _client(api_config, rec_logger) -> MassiveRESTClient:
    return MassiveRESTClient("key", api_config, rec_logger)


async def test_end_to_end_writes_files_and_manifest(api_config, rec_logger, fake_sdk, env):
    backend, manifest = env
    fake_sdk.list_aggs.return_value = _daily_aggs(_JAN1_2018_MS, 5)

    async with _client(api_config, rec_logger) as client:
        result = await ingest_crypto_daily(
            config=AppConfig(), backend=backend, manifest=manifest, client=client,
            logger=rec_logger, symbols=["BTC", "ETH"], start="2018-01-01", end="2018-01-05",
        )

    assert result.exit_code == 0
    assert set(result.succeeded) == {"X:BTCUSD", "X:ETHUSD"}
    assert backend.exists(paths.crypto_daily_key("BTC"))
    assert backend.exists(paths.crypto_daily_key("ETH"))

    df = backend.read_parquet(paths.crypto_daily_key("BTC"))
    assert df.height == 5
    assert df["symbol"].to_list() == ["X:BTCUSD"] * 5

    row = manifest.get_state("crypto", "X:BTCUSD", "daily")
    assert row["bar_count"] == 5
    assert row["earliest_date"] == "2018-01-01"
    assert row["last_complete_date"] == "2018-01-05"
    assert manifest.tracked_series_count() == 2


async def test_rerun_same_utc_day_makes_zero_sdk_calls(api_config, rec_logger, fake_sdk, env):
    backend, manifest = env
    # Newest bar == yesterday(UTC), so after run 1 the manifest is "current".
    fake_sdk.list_aggs.return_value = _daily_aggs(_yesterday_ms() - 2 * _DAY_MS, 3)

    async with _client(api_config, rec_logger) as client:
        r1 = await ingest_crypto_daily(  # end defaults to yesterday (UTC)
            config=AppConfig(), backend=backend, manifest=manifest, client=client,
            logger=rec_logger, symbols=["BTC"],
        )
        assert r1.succeeded == ["X:BTCUSD"]

        fake_sdk.list_aggs.reset_mock()
        r2 = await ingest_crypto_daily(
            config=AppConfig(), backend=backend, manifest=manifest, client=client,
            logger=rec_logger, symbols=["BTC"],
        )

    assert fake_sdk.list_aggs.call_count == 0  # short-circuited BEFORE any SDK call
    assert r2.skipped_uptodate == ["X:BTCUSD"]
    assert r2.succeeded == []
    assert r2.exit_code == 0


async def test_auth_error_aborts_run(api_config, rec_logger, fake_sdk, env, monkeypatch):
    backend, manifest = env

    async def fake_fetch_many(requests):
        return {r.ticker: MassiveAuthError("bad key") for r in requests}

    async with _client(api_config, rec_logger) as client:
        monkeypatch.setattr(client, "fetch_many", fake_fetch_many)
        with pytest.raises(MassiveAuthError):
            await ingest_crypto_daily(
                config=AppConfig(), backend=backend, manifest=manifest, client=client,
                logger=rec_logger, symbols=["BTC", "ETH"], start="2018-01-01", end="2018-01-05",
            )

    assert not backend.exists(paths.crypto_daily_key("BTC"))
    assert manifest.tracked_series_count() == 0


async def test_bad_request_skips_symbol_and_continues(api_config, rec_logger, fake_sdk, env):
    backend, manifest = env

    def side_effect(ticker, *args, **kwargs):
        if ticker == "X:BTCUSD":
            raise BadResponse('{"status":"ERROR","error":"bad symbol"}')
        return _daily_aggs(_JAN1_2018_MS, 5)

    fake_sdk.list_aggs.side_effect = side_effect

    async with _client(api_config, rec_logger) as client:
        result = await ingest_crypto_daily(
            config=AppConfig(), backend=backend, manifest=manifest, client=client,
            logger=rec_logger, symbols=["BTC", "ETH"], start="2018-01-01", end="2018-01-05",
        )

    assert result.skipped_error == ["X:BTCUSD"]
    assert result.succeeded == ["X:ETHUSD"]
    assert result.exit_code == 1
    assert not backend.exists(paths.crypto_daily_key("BTC"))
    assert backend.exists(paths.crypto_daily_key("ETH"))
    assert manifest.get_state("crypto", "X:BTCUSD", "daily") is None
    assert manifest.get_state("crypto", "X:ETHUSD", "daily") is not None


async def test_all_symbols_fail_exit_2(api_config, rec_logger, fake_sdk, env):
    backend, manifest = env
    fake_sdk.list_aggs.side_effect = MaxRetryError(pool=None, url="/v2/aggs", reason=None)

    async with _client(api_config, rec_logger) as client:
        result = await ingest_crypto_daily(
            config=AppConfig(), backend=backend, manifest=manifest, client=client,
            logger=rec_logger, symbols=["BTC", "ETH"], start="2018-01-01", end="2018-01-05",
        )

    assert set(result.skipped_error) == {"X:BTCUSD", "X:ETHUSD"}
    assert result.succeeded == []
    assert result.exit_code == 2
    assert manifest.tracked_series_count() == 0


async def test_empty_iterator_zero_bars_no_manifest_row(api_config, rec_logger, fake_sdk, env):
    backend, manifest = env
    fake_sdk.list_aggs.return_value = []

    async with _client(api_config, rec_logger) as client:
        result = await ingest_crypto_daily(
            config=AppConfig(), backend=backend, manifest=manifest, client=client,
            logger=rec_logger, symbols=["BTC"], start="2018-01-01", end="2018-01-05",
        )

    assert result.zero_bar == ["X:BTCUSD"]
    assert result.succeeded == []
    assert result.exit_code == 0
    assert not backend.exists(paths.crypto_daily_key("BTC"))
    assert manifest.get_state("crypto", "X:BTCUSD", "daily") is None
    assert manifest.tracked_series_count() == 0


async def test_resumes_from_manifest_and_builds_ticker(api_config, rec_logger, fake_sdk, env):
    backend, manifest = env
    # Seed a prior state: last complete 2018-01-03.
    manifest.upsert_state(
        "crypto", "X:BTCUSD", "daily",
        earliest_date="2018-01-01", last_complete_date="2018-01-03",
        bar_count=3, last_updated_at="2026-01-01T00:00:00+00:00",
    )
    fake_sdk.list_aggs.return_value = _daily_aggs(_JAN1_2018_MS + 3 * _DAY_MS, 2)  # 01-04, 01-05

    async with _client(api_config, rec_logger) as client:
        await ingest_crypto_daily(
            config=AppConfig(), backend=backend, manifest=manifest, client=client,
            logger=rec_logger, symbols=["BTC"], start="2018-01-01", end="2018-01-05",
        )

    args, _kwargs = fake_sdk.list_aggs.call_args
    assert args[0] == "X:BTCUSD"      # ticker construction X:{BASE}{QUOTE}
    assert args[3] == "2018-01-04"    # resume = last_complete_date + 1 (SPEC §6.3)
    assert args[4] == "2018-01-05"    # target end


async def test_dry_run_makes_no_calls(api_config, rec_logger, fake_sdk, env):
    backend, manifest = env
    async with _client(api_config, rec_logger) as client:
        result = await ingest_crypto_daily(
            config=AppConfig(), backend=backend, manifest=manifest, client=client,
            logger=rec_logger, symbols=["BTC", "ETH"], start="2018-01-01", end="2018-01-05",
            dry_run=True,
        )

    assert result.dry_run is True
    assert {sp.ticker for sp in result.plan} == {"X:BTCUSD", "X:ETHUSD"}
    assert all(sp.from_date == "2018-01-01" and sp.to_date == "2018-01-05" for sp in result.plan)
    assert fake_sdk.list_aggs.call_count == 0
    assert not backend.exists(paths.crypto_daily_key("BTC"))
