"""Unit tests for reference.calendar — NYSE helpers, no network (SPEC §13 Slice 5, S7).

``pandas_market_calendars`` ships its own holiday/closure data, so these are
deterministic offline. ``nyse_target_end`` takes an injected ``today_et`` so the
"last *complete* session" logic is testable without a real clock.
"""

from __future__ import annotations

import datetime as dt

from massive_fetch.reference.calendar import (
    nyse_first_session_on_or_after,
    nyse_session_count,
    nyse_target_end,
)


# --- nyse_target_end: last complete session on/before yesterday-ET (never today) ---


def test_target_end_monday_returns_prior_friday():
    # Mon 2024-01-08 -> yesterday Sun 01-07 -> last session = Fri 2024-01-05.
    assert nyse_target_end(dt.date(2024, 1, 8)) == "2024-01-05"


def test_target_end_midweek_returns_prior_day():
    # Wed 2024-01-10 -> yesterday Tue 01-09 (a session) -> 2024-01-09.
    assert nyse_target_end(dt.date(2024, 1, 10)) == "2024-01-09"


def test_target_end_never_returns_today():
    # Tue 2024-01-09 -> must return Mon 2024-01-08 (yesterday), NOT today's session.
    assert nyse_target_end(dt.date(2024, 1, 9)) == "2024-01-08"


def test_target_end_skips_holiday():
    # Tue 2024-01-16 -> yesterday Mon 01-15 is MLK Day (closed) -> Fri 2024-01-12.
    assert nyse_target_end(dt.date(2024, 1, 16)) == "2024-01-12"


def test_target_end_after_weekend_and_holiday_run():
    # Wed 2025-01-01 (New Year) -> yesterday Tue 2024-12-31 (a session) -> 2024-12-31.
    assert nyse_target_end(dt.date(2025, 1, 1)) == "2024-12-31"


# --- nyse_session_count: inclusive session count over a window -------------


def test_session_count_full_year_2020():
    # 2020 had 253 NYSE trading sessions.
    assert nyse_session_count("2020-01-01", "2020-12-31") == 253


def test_session_count_small_window_excludes_holiday():
    # Jan 1 2024 (Mon) is a holiday; Jan 2 Tue .. Jan 5 Fri = 4 sessions.
    assert nyse_session_count("2024-01-02", "2024-01-05") == 4


def test_session_count_single_session_day():
    assert nyse_session_count("2024-07-15", "2024-07-15") == 1


def test_session_count_weekend_is_zero():
    # Sat 2024-01-06 .. Sun 2024-01-07: no sessions.
    assert nyse_session_count("2024-01-06", "2024-01-07") == 0


# --- nyse_first_session_on_or_after: HARD-CODED literal anchors (not self-compared) ---


def test_first_session_on_or_after_2020():
    # 2020-01-01 (Wed) is a holiday -> first session is Thu 2020-01-02.
    assert nyse_first_session_on_or_after("2020-01-01") == "2020-01-02"


def test_first_session_on_or_after_2021():
    # 2021-01-01 (Fri) holiday, 01-02/01-03 weekend -> first session Mon 2021-01-04.
    assert nyse_first_session_on_or_after("2021-01-01") == "2021-01-04"
