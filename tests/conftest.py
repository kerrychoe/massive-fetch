"""Shared fixtures for the test suite."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from massive.rest.models import Agg

from massive_fetch.config import APIConfig

FIXTURES = Path(__file__).parent / "fixtures"


class RecordingLogger:
    """Minimal stand-in for a structlog BoundLogger that records calls.

    The wrapper only ever calls ``.debug()`` / ``.error()`` with an event name
    and keyword fields, so this captures everything we need to assert on without
    depending on structlog's global configuration.
    """

    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []

    def debug(self, event: str, **kw: Any) -> None:
        self.events.append({"level": "debug", "event": event, **kw})

    def error(self, event: str, **kw: Any) -> None:
        self.events.append({"level": "error", "event": event, **kw})


@pytest.fixture
def api_config() -> APIConfig:
    """Default API config with the conservative concurrency=3 default."""
    return APIConfig()


@pytest.fixture
def rec_logger() -> RecordingLogger:
    return RecordingLogger()


@pytest.fixture
def sample_aggs() -> list[Agg]:
    """Canned AAPL minute bars as SDK ``Agg`` objects (via ``Agg.from_dict``)."""
    raw = json.loads((FIXTURES / "aapl_minute_sample.json").read_text())
    return [Agg.from_dict(d) for d in raw]


@pytest.fixture
def fake_sdk(mocker) -> Any:
    """Patch the SDK ``RESTClient`` so construction returns a controllable mock."""
    sdk = mocker.Mock(name="RESTClient_instance")
    mocker.patch("massive_fetch.clients.rest.RESTClient", return_value=sdk)
    return sdk
