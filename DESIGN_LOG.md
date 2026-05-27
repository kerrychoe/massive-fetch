# massive-fetch — Design Conversation Log

A record of the design discussion that produced `SPEC.md`. The reasoning behind decisions is often more valuable than the final spec, especially when revisiting choices later.

**Date**: May 2026
**Topic**: Designing a historical market data ingestion tool against the Massive.com API, intended to feed downstream backtesting workflows.

---

## Initial framing

The user asked whether Claude Code could build an app that downloads historical market data from Massive.com given an API key, supporting symbol/asset-type/date inputs and optional scheduled execution. The user was open to ideas about whether this should be a separate tool or built into a backtester.

### Key initial response points

- **Massive offers REST API, WebSocket stream, and Flat Files (S3-compatible).** Flat Files dramatically faster for bulk historical pulls.
- **Massive provides an official MCP server** (`github.com/massive-com/mcp_massive`) that integrates with Claude Code for ad-hoc data exploration — useful but not a replacement for a real downloader.
- **Recommendation: build as a separate ingestion tool, not inside the backtester.** Reasons:
  - Reproducibility (vendor revisions, corporate actions, ticker changes)
  - Speed (local Parquet ~100× faster than REST round-trips)
  - Rate limits / quota
  - Separation of concerns (independent testing of each layer)
  - Reusability (one cache feeds every future strategy)

### Three-layer architecture proposed

1. **Ingestion layer** — pulls from Massive, normalizes, writes to local storage
2. **Storage layer** — Parquet files on disk (or DuckDB/SQLite for metadata)
3. **Research/backtest layer** — reads from storage, never touches the API

### Suggested ingestion app shape

- CLI-driven Python app
- Symbols-file batch input
- Smart routing (Flat Files for bulk, REST for incremental)
- Idempotent + resumable (manifest file or SQLite table)
- Parquet output, partitioned by symbol and date
- Corporate actions handled separately (raw bars + adjustment factors)
- Logging + retry with exponential backoff
- `validate` command for gap/dupe/missing-day detection

### Scheduling options discussed

1. Plain cron / Task Scheduler / systemd timer — boring, bulletproof, no LLM
2. GitHub Actions on a schedule — same determinism plus logs, secrets, notifications
3. Claude scheduled task — only useful when the LLM adds value (e.g., summarizing run output)

Recommendation: cron for the deterministic data fetch; Claude Code with the Massive MCP server for exploration/research workflows around it.

### Five upfront decisions identified

1. Asset classes (stocks/options/forex/crypto/futures)
2. Timeframes (daily/minute/tick)
3. Universe size
4. Local vs. cloud storage
5. Adjusted vs. raw

---

## User's first round of decisions

- Asset classes: **stocks, futures, crypto** (no forex/options)
- Timeframes: **daily and minute** (no tick)
- Universe: all stocks, major blue-chip cryptos, futures
- Storage: local (cloud later)
- Storage policy: store raw, adjust at read time

### Response: pushed back on universe size

- Daily across full US universe: 5–15 GB compressed (manageable)
- Minute across full US universe: ~1–3 TB (impractical without serious disk)
- **Flat Files mandatory at that scale** (REST cannot handle it)
- **Survivorship bias becomes the user's problem** — covered later
- Futures has continuous-contract complexity — Phase 2

### Storage architecture proposed

```
data/
├── raw/                          # Vendor-format cache
│   ├── stocks/flatfiles/
│   ├── futures/{contract}/
│   ├── crypto/{symbol}/
│   └── corporate_actions/
├── processed/                    # Repartitioned for fast reads
├── reference/
│   ├── tickers.parquet
│   ├── market_calendar.parquet
│   └── manifest.sqlite
└── logs/
```

Two-tier (raw + processed) so re-downloading is never necessary and reads are fast.

---

## User's descope: limit the universe

User correctly recognized terabyte-scale storage was impractical and descoped to:
- **Stocks**: S&P 500 ∪ QQQ (~600 tickers)
- **Futures**: major contracts only
- **Crypto**: BTC and ETH only

User also pointed at the Massive flat files docs.

### Response: facts pulled from Massive's docs

- Endpoint `https://files.massive.com`, bucket `flatfiles`, S3-compatible
- **Separate credentials** — Flat Files use dedicated S3 keys, *not* the REST API key
- Schema for minute aggs: `ticker,volume,open,close,high,low,window_start,transactions`
  - `window_start` is **nanoseconds since epoch (UTC)**
  - Column order is **O, C, H, L** — easy to miswire
- Latency: ~11 AM ET T+1 for the prior day's data
- One file per day, all tickers; no per-symbol files
- Futures Flat Files availability is ambiguous in docs — needs verification

### Architecture pivot for the descoped universe

- **REST-per-symbol becomes the right default** for ~600 symbols
- ~6,000 calls for 5 yrs of minute data, ~10 min wall time
- Output ~15–20 GB Parquet directly, no raw intermediate
- Flat Files becomes Phase 2 (when widening universe or capturing delisted tickers)
- Storage shrinks: ~20 GB total under descoped scope

### Survivorship bias options surfaced

1. Accept it; document loudly in README (fast)
2. Static "expanded" list — union of historical S&P 500 / QQQ membership (less biased)
3. Date-aware membership — backtest only includes a symbol on dates it was actually in the index (most rigorous)

Recommendation: build (1) now, design symbol-list module so (2) is a config change, defer (3).

---

## User asked for clarification on REST vs Flat Files, then on survivorship bias

### REST vs Flat Files — pros/cons summary

**REST API**
- Pros: pull only what you need; symbol-centric; simpler auth; smaller code surface; better incremental updates; date-range flexible; trivial to filter universe
- Cons: slow for huge universes; rate limits; pagination overhead; need symbol list upfront (creates survivorship bias); more API calls = more failure surface

**Flat Files**
- Pros: massive throughput; no per-call rate limits; naturally bias-free (every ticker that traded each day); atomic units; predictable cost; only sane way to do full-universe ingestion
- Cons: wasteful for narrow universes (95% bytes thrown away); two-step pipeline (date-partitioned → symbol-partitioned); more peak disk during processing; heavier auth (S3 client); less granular for top-up; futures coverage ambiguous

**Decision rule**: < 1,000 symbols you know about → REST. "Everything that ever traded" → Flat Files. Need fresher than ~T+1 11 AM → REST.

### Survivorship bias mechanics (explanation)

- "Today's S&P 500" backtested 2010–today contains only survivors by definition
- Excludes Lehman, GM 2009, BBBY, SVB, Time Warner, Monsanto, EMC, Allergan, etc.
- Inflates strategy results because:
  1. Survivors had positive drift
  2. Catastrophes are missing
  3. Mean reversion looks magical
  4. Trend following losses partially missing
  5. Stop losses appear unnecessary
- **Empirically inflates returns ~1–4% / year**; larger for small caps; especially bad for "buy the dip" strategies
- Index membership is itself a survivorship filter (companies added when doing well, removed when failing)

### When it matters for the user's specific work

- Pattern detection (did the pattern form?) — small effect
- Pattern outcomes (did it win, by how much?) — material effect
- Strategy edge claims (R-multiples, expectancy, max DD) — bias dominates
- For fib stacking GZ-B React % validation: bias is small and acceptable
- For strategy backtest with P&L: must address before trusting numbers

---

## User asked: what does a bias-free universe look like?

### Four kinds of bias

1. **Survivorship** — fixed by including delisted tickers
2. **Index reconstitution** — fixed by point-in-time membership
3. **Look-ahead** — fixed by ensuring no post-T information used at T
4. **Pre-listing** — fixed by not including tickers before IPO date

### Required data structures

- Historical securities master (every ticker ever, with first/last trade dates, delisting reasons, ticker changes)
- Point-in-time index membership table `(index, ticker, start_date, end_date)`
- OHLCV including delisted (Flat Files give this naturally; REST does not)
- Corporate actions with effective dates
- As-of fundamental data (less relevant for pure TA)
- Halt and trading-status history

### Build options (increasing effort)

1. Static expanded list (Wikipedia historical change logs) — best v1 upgrade
2. Point-in-time membership table
3. Full Flat Files ingestion + membership
4. Commercial securities master (CRSP, Norgate, Sharadar)

---

## User's locked decisions

After understanding the tradeoffs, user chose:
1. **REST API**
2. **Accept survivorship bias, use today's universe, document in README, make universe definition a config**

---

## Settling 10 implementation details

User answered the following before code generation:

| # | Decision |
|---|---|
| 1 | Python tooling: **uv** |
| 2 | Defaults: stocks daily 2005-01-01, stocks minute 2020-01-01, crypto 2018-01-01, futures 2022-01-01 |
| 3 | Universe source: Wikipedia scrape with frozen YAML fallback |
| 4 | Futures: auto-discover via Massive contracts endpoint; suggested contract list accepted |
| 5 | Extended hours: include all sessions, filter at read time |
| 6 | Concurrency: user-tunable, conservative default = 3 |
| 7 | Logging: both console and file with rotation |
| 8 | Manifest granularity: per `(symbol, timeframe)` with `last_complete_date` |
| 9 | Validation: warn-and-continue, log details, expose via `validate report` |
| 10 | `update`: no flags = update everything; flags = scoped update |

Two facts confirmed from Massive docs that influenced the spec:

- Futures REST is in **beta / "coming soon"** — must be handled gracefully
- Official Python client (`massive-com/client-python`) handles pagination automatically
- `massive-com/massive-ai-rules` repo provides Claude Code instruction files for the SDK
- REST returns ms timestamps, Flat Files return ns — normalization layer must handle both

---

## SPEC.md generated (829 lines)

Comprehensive specification covering:
- Goals & scope (in/out/non-goals)
- Tech stack
- Project layout
- Configuration (Pydantic models + YAML)
- Universe definition (stocks/futures/crypto)
- Storage layout & Parquet schemas
- Manifest schema (SQLite)
- REST client design (retry, concurrency, rate limiting)
- Per-asset ingestion modules
- Validation
- CLI surface
- Logging
- The `update` command for cron
- Build order (10 vertical slices, each independently testable)
- Acceptance tests
- README skeleton requirements
- Known issues & caveats
- Phase 2 roadmap
- Glossary

---

## Six logistics questions

### Q1: Local Claude Code vs. web?

**Local install required** — web Claude can't write to local disk. Recommended WSL2 on Windows (or just use macOS — easier).

### Q2: Cloud storage migration later — is it a big change?

**Small change if architected correctly.** Add a thin `StorageBackend` interface now (~80 lines), keep cloud impl as a stub. Half-day migration later vs. project-wide refactor without it.

Manifest is trickier — SQLite-on-cloud-storage doesn't work well. Pragmatic answer: keep manifest local, only Parquet files cloud-resident.

### Q3: Pattern/strategy agnosticism?

**Yes, fully.** Tool is pure data plumbing. No knowledge of fib stacking or any strategy. Cost of testing next idea is reading from disk, not re-downloading.

### Q4: Hybrid Flat Files backfill + REST incrementals?

**Yes — this is the recommended pattern.** One-time bulk via Flat Files (bias-free historical), daily delta via REST. The schema is the abstraction — both paths produce identical canonical Parquet.

Long-term steady state: Flat Files for everything. REST becomes ad-hoc tool.

### Q5: UI / dashboard?

**Defer. CLI-only in v1.** Reasons:
- Don't dashboard a system that's still changing
- SQLite Browser / DBeaver pointed at manifest.sqlite is free and adequate
- For unattended cron jobs, notifications matter more than dashboards
- Streamlit is easy to add later (~200 lines) when stable

### Q6: Cloud + scheduled, set-and-forget?

**Yes, achievable.** Progression:
- **Phase A** (now): local + cron/Task Scheduler
- **Phase B** (when stable): cheap Linux VM ($5–10/month, Hetzner / DigitalOcean)
- **Phase C** (final): GitHub Actions on schedule + S3 + Slack notifications. Zero servers, zero LLM at runtime.

Spec already accommodates this — `update` is structured-exit-code-clean and produces structured logs.

---

## Hardware decision

### Mac mini recommendation

User noted 2018 Intel Mac mini was insufficient for Claude Code work and asked for sub-$1K recommendations.

**Recommendation locked**:
- Base **M4 Mac mini at $599** (16 GB / 256 GB)
- Samsung **T7 Shield 1TB external SSD** ($90) for data
- BT keyboard/mouse already owned or cheap Logitech (~$60)
- Optional AppleCare ($99) — skip if cash-tight
- **Total: ~$750–850 all-in**

Reasoning:
- Don't bump RAM — 16 GB is plenty for this workload
- Don't bump internal SSD — external T7 is faster and more flexible dollar-for-dollar
- Trade in 2018 mini through Apple for $100–200 off (easiest path)
- Avoid Intel Macs entirely — Python toolchain progressively dropping x86 wheels

### Cloud-from-day-one alternative — declined

User asked if cloud-only could replace the hardware spend. Tradeoffs:

**Downsides of cloud-first**:
- Still need a local thin client (the 2018 mini or similar)
- Development latency hits every iteration cycle
- Claude Code prefers local files
- Backtesting eventually wants low-latency reads — local NVMe ~10–100× faster than network-attached cloud storage
- Egress costs ($0.09/GB on AWS) sneaky for data pulls
- Recurring cost has psychological overhead

**Conclusion**: local hardware wins for this use case over multi-year horizon. Cloud is for set-and-forget *deployment* of finished tool, not *development*.

---

## Two spec amendments accepted

### Amendment 1: StorageBackend abstraction (§6.0.1)

Add a `StorageBackend` Protocol with `LocalBackend` (v1) and stubbed `S3Backend` (Phase 2). All ingestion/validation/manifest modules accept a backend via dependency injection. `storage/paths.py` becomes single source of truth for directory layout via key-builder helpers. Manifest stays local even if data goes cloud (`StorageBackend.local_path()` method surfaces this).

Cost in v1: ~80 lines code. Benefit later: cloud migration is one new file + config field.

### Amendment 2: Phase 2 roadmap restructured into ordered checkpoints (§17)

- **Phase 2a**: Flat Files ingestion (hybrid bulk historical + REST incrementals)
- **Phase 2b**: Point-in-time universe membership (Wikipedia change logs)
- **Phase 2c**: Flat Files for daily incrementals (full bias-free pipeline)
- **Phase 2d**: Cloud storage backends (realize §6.0.1 abstraction)
- **Phase 2e**: Continuous futures
- **Phase 2f**: Operational polish (alerts, optional Streamlit, read-side library)
- **Phase 3**: Tick data, options, forex, alternative data

### Amendment 3: `status` command shows reference data freshness (§10.3)

Per asset, print refresh-age and health flag (OK/WARN/STALE). Covers: stocks universe, futures contracts, market calendar, splits, dividends. Configurable thresholds. STALE is informational only (does not affect exit code).

Surfaces silent failures (broken Wikipedia scraper, futures discovery not run in months, etc.).

---

## Final review feedback

### What's solid

- Architecture is right-sized: 3 layers, no premature abstractions except earned ones (storage)
- Descope was disciplined: small enough to actually validate
- Phasing is honest: documents what v1 doesn't do; clear unlocks per phase
- Build slices are vertical, not horizontal — produces something demonstrable after each

### What to watch

- **Futures REST is in beta** — Slice 8 may simply not work; mitigations in spec
- **Wikipedia scraper fragility** — frozen YAML fallback present; freshness in `status` surfaces silent failures
- **Read-merge-write Parquet append** has a hidden cost ceiling — fine for current scope
- **Concurrency=3 is conservative** — bump after first successful backfill, not during

### Operational practices to add later (not spec changes)

- Run `validate report` weekly via cron
- Keep a "known good" sample dataset for diffing when data looks weird
- Maintain a `DATA_NOTES.md` with manually-verified facts
- Log manifest `last_updated_at` per symbol when running backtests

### Explicit YAGNI list

- Web UI / dashboard — SQLite Browser exists
- Plugin system for vendors — write a new module if switching
- Generic asset-class abstractions — three concrete modules easier to read
- A backtester — separate project
- Prometheus / Grafana — structured file logs are enough for one user
- REST API around your own data — just `polars.read_parquet()`

### Suggested addition

`massive-fetch doctor` command — preflight checks for API key, disk space, calendar currency, manifest readability, recent run failures. Three minutes to write, saves debugging pain later.

---

## Path forward

1. **This week**: order Mac mini; create empty repo with SPEC.md and DESIGN_LOG.md
2. **Day one with new mini**: install Homebrew, uv, Claude Code, Massive MCP server; clone repo
3. **First Claude Code session**: implement Slice 0 only, stop at acceptance test
4. **Cadence**: one slice per session, verify each acceptance test before continuing
5. **Expect friction at**: Slice 1 (REST client / SDK) and Slice 8 (futures beta endpoint)
6. **Decision point at Slice 7**: ~80% of v1 done; reassess if project is paying off
7. **After v1**: pick a non-fib pattern, validate or invalidate it systematically

---

## Final reminder

The goal of this whole project is to **test more strategy ideas faster**, not to build the perfect data system. Every hour spent on infrastructure beyond what's necessary is an hour not spent on strategy research. Build it functional, build it boring, then go back to testing patterns.

---

## Slice 1 — REST client: retry ownership (Option B)

SDK discovery (recorded in `SDK_NOTES.md`) found the `massive` SDK already implements
retry itself: a `urllib3.util.Retry` with `status_forcelist=[413,429,499,500,502,503,504]`,
`backoff_factor=0.1`, and `Retry-After` handling. SPEC §7.2's bespoke `tenacity` policy
(base=1.0s/max=60s/jitter) was written *before* we'd seen the SDK — it was aspirational.

**Decision (with outside reviewer): Option B — the SDK owns retry.** The wrapper adds
only asyncio concurrency limiting (semaphore, default 3, bridging the sync SDK via a
thread pool), structured per-call logging, and typed exception mapping
(`MassiveAuthError` / `MassiveBadRequest` / `MassiveRetriesExhausted`). Layering our own
tenacity on top would double-retry; rejected. The SDK constructor exposes only attempt
count (`retries`, from `APIConfig.max_retries`) and timeouts — not the backoff curve — so
the inert `retry_backoff_base_seconds` / `retry_backoff_max_seconds` fields were removed
from `APIConfig` and `default.yaml`. SPEC §7.2 was rewritten to document the SDK's actual
behavior and §4.2 amended for the field removal.

---

## Slice 2 — Crypto ingestion: canonical schema amendments (§6.1)

Slice 2 (crypto daily ingestion) is the first slice to *materialize* the canonical
§6.1 Parquet schema, via `transform/normalize.py`. Two corrections to §6.1 were made
before writing any normalization code (with the outside reviewer), so the schema the
data is written against is right the first time.

**1. `volume`: `int64` → `float64`.** SDK discovery (`SDK_NOTES.md` §3) and the
committed test fixture both show the SDK returns `volume` as a *fractional* float
(e.g. `25370.68`), and crypto / fractional-share volumes are routinely non-integer.
Casting to `int64` would silently truncate real data; storing `float64` preserves the
raw value losslessly. This is consistent with the project's stated non-goal of not
modifying or "cleaning" data beyond schema normalization (§1). A precondition entering
the slice asserted this had already been committed as `float64`; it had not — the spec
still read `int64` and no such commit existed in git history — so the change is made
here explicitly.

**2. `symbol`: `dictionary<int32, string>` → `dictionary<string>`.** The index-width
pin (`int32`) bought nothing: Polars writes Categorical columns with **uint32**
dictionary indices, and Parquet readers handle either width transparently. Pinning
`int32` would have forced an explicit pyarrow schema rewrite on every write for no
functional gain. Dropping the pin keeps the dictionary-encoding intent (compact,
repeated ticker strings) while letting the writer use its natural index type.

Both are the only §6.1 changes; `normalize.py` enforces the amended schema downstream.

---

End of design log.
