"""NYSE market-calendar helpers (SPEC §2, §3 — ``reference/calendar.py``).

The single home for the ``pandas_market_calendars`` dependency. Ingestion uses
:func:`nyse_target_end` to bound a stocks daily backfill at the last *complete*
session; the Slice 5 acceptance uses :func:`nyse_session_count` and
:func:`nyse_first_session_on_or_after` to check AAPL's bar count against the
expected trading days. Keeping the import here means ``ingest/base.py`` stays
calendar-free.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pandas_market_calendars as mcal

_NYSE = mcal.get_calendar("NYSE")
_ET = ZoneInfo("America/New_York")


def _today_et() -> date:
    return datetime.now(_ET).date()


def _last_session_on_or_before(day: date) -> date:
    """The latest NYSE session date ``<= day`` (handles weekends/holidays)."""
    # A 10-day lookback always spans at least one session; widen if a freak gap.
    sessions = _NYSE.valid_days(start_date=(day - timedelta(days=10)).isoformat(), end_date=day.isoformat())
    if len(sessions) == 0:
        sessions = _NYSE.valid_days(start_date=(day - timedelta(days=40)).isoformat(), end_date=day.isoformat())
    return sessions[-1].date()


def nyse_target_end(today_et: date | None = None) -> str:
    """Last *complete* NYSE session — the last session on/before **yesterday ET**.

    Mirrors crypto's "yesterday" rule (a guaranteed-settled past day) but snapped to
    the NYSE calendar. This keeps the manifest short-circuit
    (``last_complete_date >= target_end``) engaging cleanly across weekends/holidays
    instead of re-querying to zero bars forever. It never returns *today's* session,
    whose daily bar may not be published yet. ``today_et`` is injectable for tests.
    """
    today = today_et if today_et is not None else _today_et()
    return _last_session_on_or_before(today - timedelta(days=1)).isoformat()


def nyse_session_count(start: str, end: str) -> int:
    """Number of NYSE sessions in ``[start, end]`` inclusive (ISO date strings)."""
    return len(_NYSE.valid_days(start_date=start, end_date=end))


def nyse_first_session_on_or_after(day: str) -> str:
    """The first NYSE session date ``>= day`` (ISO date string)."""
    start = date.fromisoformat(day)
    sessions = _NYSE.valid_days(start_date=start.isoformat(), end_date=(start + timedelta(days=10)).isoformat())
    if len(sessions) == 0:
        sessions = _NYSE.valid_days(start_date=start.isoformat(), end_date=(start + timedelta(days=40)).isoformat())
    return sessions[0].date().isoformat()
