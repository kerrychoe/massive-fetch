"""Unit tests for MassiveRESTClient — SDK fully mocked, no network (SPEC §13 Slice 1)."""

from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import AsyncIterator
from typing import TypeVar

import pytest
from massive.exceptions import AuthError, BadResponse
from urllib3.exceptions import MaxRetryError

from massive_fetch.clients.rest import (
    MassiveAuthError,
    MassiveBadRequest,
    MassiveClientError,
    MassiveRESTClient,
    MassiveRetriesExhausted,
)

T = TypeVar("T")


async def consume(agen: AsyncIterator[T]) -> list[T]:
    return [item async for item in agen]


# --- happy path -----------------------------------------------------------


async def test_list_aggs_yields_all_bars(api_config, rec_logger, fake_sdk, sample_aggs):
    fake_sdk.list_aggs.return_value = sample_aggs

    async with MassiveRESTClient("key", api_config, rec_logger) as client:
        bars = await consume(client.list_aggs("AAPL", 1, "minute", "2026-05-22", "2026-05-22"))

    assert bars == sample_aggs
    # SDK called with RAW (adjusted=False), asc sort, and the configured page limit.
    _, kwargs = fake_sdk.list_aggs.call_args
    assert kwargs["adjusted"] is False
    assert kwargs["sort"] == "asc"
    assert kwargs["limit"] == api_config.page_limit

    events = [e for e in rec_logger.events if e["event"] == "api_call"]
    assert len(events) == 1
    ev = events[0]
    assert ev["level"] == "debug"
    assert ev["status"] == "success"
    assert ev["bar_count"] == len(sample_aggs)
    assert ev["ticker"] == "AAPL"
    assert ev["timespan"] == "minute"
    assert ev["from_date"] == "2026-05-22"
    assert ev["to_date"] == "2026-05-22"
    assert "response_time_ms" in ev


async def test_unknown_ticker_yields_nothing(api_config, rec_logger, fake_sdk):
    # An unknown ticker returns [] from the SDK — "no data", not an error.
    fake_sdk.list_aggs.return_value = []

    async with MassiveRESTClient("key", api_config, rec_logger) as client:
        bars = await consume(client.list_aggs("ZZZZNOPE", 1, "day", "2026-05-01", "2026-05-22"))

    assert bars == []
    ev = next(e for e in rec_logger.events if e["event"] == "api_call")
    assert ev["status"] == "success"
    assert ev["bar_count"] == 0


# --- exception mapping ----------------------------------------------------


def test_auth_error_maps_at_construction(api_config, rec_logger, mocker):
    mocker.patch(
        "massive_fetch.clients.rest.RESTClient",
        side_effect=AuthError("Must specify env var MASSIVE_API_KEY"),
    )
    with pytest.raises(MassiveAuthError) as exc_info:
        MassiveRESTClient("", api_config, rec_logger)
    assert isinstance(exc_info.value.__cause__, AuthError)


async def test_bad_response_maps_to_bad_request(api_config, rec_logger, fake_sdk):
    fake_sdk.list_aggs.side_effect = BadResponse('{"status":"ERROR","error":"Unknown API Key"}')

    async with MassiveRESTClient("key", api_config, rec_logger) as client:
        with pytest.raises(MassiveBadRequest) as exc_info:
            await consume(client.list_aggs("AAPL", 1, "minute", "2026-05-22", "2026-05-22"))

    assert isinstance(exc_info.value.__cause__, BadResponse)
    assert "Unknown API Key" in str(exc_info.value)
    ev = next(e for e in rec_logger.events if e["event"] == "api_call")
    assert ev["level"] == "error"
    assert ev["status"] == "BadResponse"
    assert "Unknown API Key" in ev["error"]


async def test_max_retry_maps_to_retries_exhausted(api_config, rec_logger, fake_sdk):
    fake_sdk.list_aggs.side_effect = MaxRetryError(pool=None, url="/v2/aggs", reason=None)

    async with MassiveRESTClient("key", api_config, rec_logger) as client:
        with pytest.raises(MassiveRetriesExhausted) as exc_info:
            await consume(client.list_aggs("AAPL", 1, "minute", "2026-05-22", "2026-05-22"))

    assert isinstance(exc_info.value.__cause__, MaxRetryError)
    # Both typed errors share the common base for blanket catches.
    assert isinstance(exc_info.value, MassiveClientError)
    ev = next(e for e in rec_logger.events if e["event"] == "api_call")
    assert ev["level"] == "error"
    assert ev["status"] == "MaxRetryError"


# --- concurrency ----------------------------------------------------------


async def test_semaphore_limits_concurrency(api_config, rec_logger, fake_sdk, sample_aggs):
    # api_config.max_concurrent_requests == 3. Track peak in-flight worker
    # threads; the wrapper's semaphore must keep it at or below the limit.
    # The counter is touched from worker threads, so guard it with a Lock.
    lock = threading.Lock()
    state = {"current": 0, "peak": 0}

    def blocking_fetch(*args, **kwargs):
        with lock:
            state["current"] += 1
            state["peak"] = max(state["peak"], state["current"])
        time.sleep(0.05)
        with lock:
            state["current"] -= 1
        return list(sample_aggs)

    fake_sdk.list_aggs.side_effect = blocking_fetch

    async with MassiveRESTClient("key", api_config, rec_logger) as client:
        await asyncio.gather(
            *(
                consume(client.list_aggs(f"SYM{i}", 1, "minute", "2026-05-22", "2026-05-22"))
                for i in range(10)
            )
        )

    assert state["peak"] <= api_config.max_concurrent_requests
    assert state["peak"] >= 2  # confirm calls actually overlapped (not serialized)
