# SDK Notes — `massive` Python client

Findings from direct inspection and a live test call, performed during **Slice 1**
(REST client) discovery. These are verified facts about the actual SDK, recorded
*before* the wrapper was designed so the `clients/rest.py` design can be built on
reality rather than the assumptions baked into SPEC §7.

- **Package**: `massive` (PyPI), version **2.7.0** tested.
- **Identity**: "Official Massive (formerly Polygon.io) REST and Websocket client."
  The API surface is Polygon.io's (`RESTClient`, `list_aggs`, `/v2/aggs/...`).
- **Repo / docs**: `github.com/massive-com/client-python` · `https://massive.com/docs`.
- **Runtime deps**: `certifi`, `urllib3>=1.26.9`, `websockets>=14.0`. **No async HTTP
  library, no `requests`.**
- **Discovery method**: SDK installed into the venv with `uv pip install massive==2.7.0`
  (NOT `uv add` — `pyproject.toml`/`uv.lock` were left untouched, confirmed via
  `git status`). Live calls used the `MASSIVE_API_KEY` already present in the env.
  Throwaway probe scripts lived in `/tmp`, not committed.

---

## 1. Sync vs async

**Synchronous.** Transport is `urllib3.PoolManager` (`massive/rest/base.py`). There
is no `asyncio` anywhere in the REST client. SPEC §7's async interface therefore has
to be built *on top* — the SPEC's own note ("a clean async interface even if the SDK
is sync (run sync calls in a thread pool)") matches what we found.

## 2. Pagination

**The SDK auto-paginates; the caller does not drive it.** `list_aggs` returns a
**generator** (`_paginate_iter` in `base.py`) that transparently follows `next_url`
until exhausted, as long as `pagination=True` (the constructor default). Confirmed
live: a single `list_aggs(...)` call for one day of AAPL minute bars yielded all
787 bars across pages with no caller intervention.

- `raw=True` short-circuits pagination and returns the first page's raw
  `urllib3.HTTPResponse` instead of a generator.
- `get_aggs(...)` is the non-paginating sibling: one page, returns a `List[Agg]`.

SPEC §7.4's assumption ("`list_aggs` is a generator that paginates internally") is
**correct**.

## 3. `list_aggs` signature & the Agg model

```python
client.list_aggs(
    ticker: str,
    multiplier: int,
    timespan: str,                       # "minute" | "hour" | "day" | "week" | "month" | "quarter" | "year"
    from_: str|int|datetime|date,        # NOTE: the kwarg is `from_`, not `from` (Python keyword)
    to:    str|int|datetime|date,        # "YYYY-MM-DD", Unix MS int, datetime, or date all accepted
    adjusted: Optional[bool] = None,     # None -> param omitted -> API DEFAULTS TO adjusted=true
    sort:  Optional[str|Sort] = None,    # None -> API default; pass "asc"/"desc"
    limit: Optional[int] = None,         # None -> API default 5000; max 50000
    params=None, raw=False, options=None,
) -> Iterator[Agg]                       # or HTTPResponse if raw=True
```

The yielded `Agg` is a dataclass (`massive/rest/models/aggs.py`) whose `from_dict`
**already maps the terse API keys to friendly attribute names**:

| API key | `Agg` attribute | Live AAPL sample (first bar) |
|---|---|---|
| `o`   | `open`         | `305.61` |
| `h`   | `high`         | `306.2` |
| `l`   | `low`          | `305.4259` |
| `c`   | `close`        | `305.69` |
| `v`   | `volume`       | `25370.684006`  ← **float, not int** |
| `vw`  | `vwap`         | `305.7921` |
| `t`   | `timestamp`    | `1779436800000` ← **Unix ms** |
| `n`   | `transactions` | `2649` |
| `otc` | `otc`          | `None` (field omitted when false) |

These attribute names line up 1:1 with the canonical schema in SPEC §6.1, so the
`transform/normalize.py` layer mostly renames nothing — it converts types/units.

**Two normalization gotchas surfaced by the live data** (defer the fix to the
ingestion/normalize slices, but record them now):
- **`volume` is a `float`** (`25370.684006`), and the `Agg` type hint is
  `Optional[float]`. SPEC §6.1 wants `volume: int64`. Normalization must cast (and
  decide how to treat fractional volume from fractional-share trades).
- **`timestamp` stays a raw int** (no datetime conversion in the SDK). Normalization
  owns ms → `timestamp[ns, tz=UTC]`.

## 4. Timestamp unit  ✅ (verified, matches SPEC §16)

**Unix milliseconds.** First AAPL minute bar `t = 1779436800000` → `2026-05-22
04:00:00 ET`; consecutive bars differ by exactly `60000`. The endpoint doc and the
`Agg.from_dict` mapping agree.

## 5. Bar count sanity  ✅

One day of AAPL 1-minute bars (`2026-05-22`, `adjusted=False`, `sort=asc`):
- **787 bars total.**
- **390** fall in the regular session (09:30–16:00 ET) — exactly the expected count.
- **397** are extended-hours (pre-market from 04:00 ET, after-hours to 19:59 ET).

So extended-hours bars come back by default (consistent with SPEC §8.1: "the
aggregates endpoint already returns extended-hours bars when present").

## 6. Auth configuration

`RESTClient(api_key=os.getenv("MASSIVE_API_KEY"), ...)`. **Both paths work**: the
constructor's `api_key` default reads env var `MASSIVE_API_KEY`, and an explicit
`api_key=` arg overrides it. Auth header is `Authorization: Bearer <key>`.

⚠️ The env-var default is evaluated **once at import time** (it's a default arg
value). For the wrapper, pass `api_key` explicitly rather than relying on the
import-time default.

## 7. Built-in retry  ⚠️ (overlaps SPEC §7.2 — design tension)

The SDK **already retries transient failures itself**, via a `urllib3.util.Retry`
configured in `base.py`:

- `total = retries` (constructor `retries=3` default).
- `status_forcelist = [413, 429, 499, 500, 502, 503, 504]`.
- `backoff_factor = 0.1` → 0.2s, 0.4s, 0.8s, 1.6s … (much smaller than SPEC's
  base=1.0s / max=60s schedule).
- urllib3's `Retry` honors the `Retry-After` header for 413/429/503 by default.
- Connection errors and timeouts are also covered by `total`.

**Implication for the wrapper (decision deferred to design, not made here):** SPEC
§7.2 specifies our own `tenacity` retry on 429/5xx/network/timeout with exp backoff +
jitter and a `MassiveRetriesExhausted`. Layering tenacity on top of the SDK's retry
**double-retries** unless we neutralize one layer (e.g., construct the client with
`retries=0` and let tenacity own the policy, or keep the SDK's retry and have the
wrapper add only the semaphore + logging + jitter we want). This is the main design
question Slice 1 has to answer.

## 8. Exception taxonomy  ⚠️ (verified empirically — differs from SPEC's mental model)

Only two exception classes exist (`massive/exceptions.py`): `AuthError` and
`BadResponse`. Observed behavior:

| Situation | What actually happens |
|---|---|
| `api_key=None` at construction | `AuthError` (raised in `__init__`, before any call) |
| HTTP 400 (bad timespan) | `BadResponse`; message = raw JSON body, e.g. `{"status":"ERROR",...,"error":"Invalid time span..."}` |
| HTTP 401 (wrong key) | `BadResponse`; body `{"...","error":"Unknown API Key"}` — **not** `AuthError` |
| Unknown ticker (`ZZZZNOPE`) | **No exception** — returns an empty list/generator |
| 429 / 5xx after SDK retries exhausted | `urllib3.exceptions.MaxRetryError` (from `client.request`), **not** `BadResponse` |
| Network / connect / read timeout | `urllib3` exceptions (`MaxRetryError`, `ConnectTimeoutError`, `ReadTimeoutError`) |

**Sharp edges for the wrapper:**
- `BadResponse` covers *every* non-200 the SDK doesn't retry, and it **does not
  expose the HTTP status code** as an attribute — the status is only inferrable by
  parsing the JSON body string. SPEC §7.2's "don't retry 400/401/403/404, do retry
  429/5xx" can't key off the exception type alone.
- `raw=True` does **not** help here: `base.py` raises `BadResponse` on `status != 200`
  *before* the `if raw: return resp` line, so you cannot inspect a non-200 status via
  the raw response either.
- An empty result is a normal "no data" signal, **not** an error — ingestion must not
  treat `[]` as a failure.

## 9. Other constructor knobs (relevant to SPEC's APIConfig)

`RESTClient(connect_timeout=10.0, read_timeout=10.0, num_pools=10, retries=3,
base="https://api.massive.com", pagination=True, verbose=False, trace=False,
custom_json=None)`.

- One timeout per phase (connect vs read). SPEC's single `request_timeout_seconds`
  must map onto one or both.
- `base` default is `https://api.massive.com` — matches SPEC `rest_base_url`.
- `verbose=True` bumps the SDK's own logger to DEBUG; `trace=True` logs full request
  URLs/headers (with the key redacted). Concurrency is **not** an SDK concern —
  there's no rate limiter; the semaphore is entirely the wrapper's job.

## 10. Methods present for later slices (existence confirmed)

- Stocks/crypto aggs: `list_aggs`, `get_aggs`, `get_grouped_daily_aggs`,
  `get_previous_close_agg`, `get_daily_open_close_agg`.
- Corporate actions (Slice 7): `list_splits`, `list_dividends` (in `rest/reference.py`).
- Futures (Slice 8): `list_futures_contracts`, **`list_futures_aggregates`** (futures
  bars use their *own* method — **not** `list_aggs`), plus products/quotes/trades/
  schedules. SPEC §7.1's single `list_aggs` covers stocks + crypto only.

## 11. Plan-tier limits observed live  ⚠️ (history depth + rate limit)

Verified against the live API key on this machine (Slice 2 daily acceptance 2026-05-26;
Slice 3 minute acceptance 2026-05-29). These are *account/tier* limits (SPEC §16), not SDK
bugs — the API silently returns a shorter window than requested, or rate-limits a large pull.

- **History depth is capped, and minute is shorter than daily.** Daily history returns only
  ~2 years regardless of `from_` / `--start` (Slice 2: exactly 730 daily bars, 2024-05-27 →
  2026-05-26). **Minute** history is shorter still: a BTC minute request from 2024-01-01 only
  began returning bars at `t ≈ 1731940800000` (~2024-11-18, ~18 months back). Assert bar
  counts against "what the tier returns," never against the full requested range;
  `earliest_date` in the manifest reflects the real earliest bar, not the requested start.
- **A long single-shot minute pull trips the 429 rate limit.** The SDK auto-paginates one
  `list_aggs` call (§2) into many 50k-bar pages; a ~2.5-year BTC minute pull issued enough
  pages fast enough to raise urllib3 `MaxRetryError('too many 429 error responses')` after the
  SDK's own `Retry-After`-aware retries (§7) were exhausted — mapped to `MassiveRetriesExhausted`.
  A tight window (4 days → 5758 bars) succeeds cleanly. **Slice 6 constraint (stocks minute,
  ~600 symbols × years):** long minute backfills must be *windowed* (e.g. per-year sub-ranges)
  and/or *paced*, not issued as one open-ended range per symbol. The conservative
  `max_concurrent_requests=3` default bounds cross-symbol fan-out but **not** the page rate
  within a single symbol's pagination — which is what tripped the limit here.
