"""Unit tests for storage.manifest ingestion-state read/write (SPEC §6.3)."""

from __future__ import annotations

from massive_fetch.storage.manifest import Manifest

_NOW = "2026-05-26T00:00:00+00:00"


def _mk(tmp_path) -> Manifest:
    m = Manifest(tmp_path / "manifest.sqlite")
    m.initialize()
    return m


def test_get_state_absent_before_file(tmp_path):
    m = Manifest(tmp_path / "manifest.sqlite")  # never initialized -> no file
    assert m.get_state("crypto", "X:BTCUSD", "daily") is None


def test_get_state_absent_after_init(tmp_path):
    m = _mk(tmp_path)
    assert m.get_state("crypto", "X:BTCUSD", "daily") is None
    assert m.is_empty()


def test_upsert_then_get(tmp_path):
    m = _mk(tmp_path)
    m.upsert_state(
        "crypto", "X:BTCUSD", "daily",
        earliest_date="2018-01-01", last_complete_date="2018-01-05",
        bar_count=5, last_updated_at=_NOW,
    )
    row = m.get_state("crypto", "X:BTCUSD", "daily")
    assert row is not None
    assert row["earliest_date"] == "2018-01-01"
    assert row["last_complete_date"] == "2018-01-05"
    assert row["bar_count"] == 5
    assert m.tracked_series_count() == 1


def test_upsert_updates_in_place(tmp_path):
    m = _mk(tmp_path)
    for last, n in [("2018-01-05", 5), ("2018-01-09", 9)]:
        m.upsert_state(
            "crypto", "X:BTCUSD", "daily",
            earliest_date="2018-01-01", last_complete_date=last,
            bar_count=n, last_updated_at=_NOW,
        )
    row = m.get_state("crypto", "X:BTCUSD", "daily")
    assert row["last_complete_date"] == "2018-01-09"
    assert row["bar_count"] == 9
    assert m.tracked_series_count() == 1  # updated, not duplicated


def test_two_symbols_tracked(tmp_path):
    m = _mk(tmp_path)
    for sym in ["X:BTCUSD", "X:ETHUSD"]:
        m.upsert_state(
            "crypto", sym, "daily",
            earliest_date="2018-01-01", last_complete_date="2018-01-05",
            bar_count=5, last_updated_at=_NOW,
        )
    assert m.tracked_series_count() == 2
