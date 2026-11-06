# Astra Architecture & Agent Execution Brief
## VPS-Based Options Chain + Underlying OHLCV Market Data Platform

**Document type:** Planning and architecture handoff — NOT a request to immediately implement everything  
**Primary objective:** Ask Astra to design a buildable, agent-executable architecture and phased implementation plan.  
**Priority:** Research on 30–90 DTE equity/ETF options aligned to daily, 4-hour and 15-minute underlying charts, with future 1–5-minute research and real-time provider upgrades.

---

## 1. Your Assignment (Astra)

You are the **lead architect and planning agent**. Translate the requirements below into a realistic, production-minded system architecture that multiple coding agents can implement independently and test in stages.

**First produce a plan, not a monolithic implementation.** Make explicit architectural decisions and tradeoffs, identify external API limitations, and deliver actionable tasks, file/module boundaries, database migrations, interface contracts, test plans, dependencies, risks, milestones, and acceptance criteria. Mark assumptions and open questions. Favor an executable, maintainable MVP over unnecessary distributed-system complexity.

Deliverables requested from Astra:

1. **Architecture Decision Record (ADR)** explaining technology choices and alternatives, including single VPS constraints.
2. **Repository/module design** with a proposed directory tree, service responsibilities, boundaries, and interfaces.
3. **Data schemas** (SQL DDL or detailed schema specs) for instruments, options contracts, OHLCV, quote snapshots, job health, ingestion gaps, provenance, derived metrics, and archives.
4. **An implementation roadmap** broken into agent-sized tickets with prerequisites, exact expected artifacts, tests, and clear acceptance criteria.
5. **An ingestion and historical-backfill plan** with source-specific limitations, schedules, retries, idempotency, market calendars, gap detection and recovery.
6. **A validation/backtesting plan** including temporal correctness, no lookahead, survivorship risks, corporate actions, stale options quotes, and transaction-cost sensitivity.
7. **An operations plan** for Docker Compose deployment, config/secrets, metrics, backups, storage budgets, alerting, and disaster recovery.
8. **A provider migration plan** for swapping yfinance for a broker or licensed real-time options/underlying feed without changing the research API.
9. **A security and compliance checklist** including Yahoo terms, exchange licensing, API terms, quote entitlement and data redistribution restrictions.
10. **A final ordered agent task list** that downstream agents can execute with minimal ambiguity and low merge-conflict risk.

Astra should not assert unsupported Yahoo capabilities, zero latency, guaranteed real-time status, or historical options candles that yfinance does not provide.

---

## 2. Product Vision

Build a Python-first self-hosted market-data aggregator running on an affordable VPS. It continuously accumulates synchronized price information for underlying equities/ETFs and their listed options contracts, primarily for quantitative research and secondarily for near-real-time opportunity detection.

The user wants to:

- Analyze and trade **30–90 days-to-expiration (DTE)** options as the first priority; retain the possibility to expand to 0–7 DTE later.
- Plot and compare options metrics against the underlying instrument's **1m, 5m, 15m, 4h and daily OHLCV candlestick charts**.
- Prioritize **daily and 4h swing analysis**, add **15m analysis**, and preserve the ability to investigate **1–5m intraday strategies** when a better data feed is available.
- Build a growing private historical database rather than depend on limited historical options-chain access.
- Inspect contract-level bid/ask, midpoint, implied volatility, volume, open interest, expirations, skew, term structure, theoretical value, and other modeled quantities over time.
- Eventually evaluate and alert on **persistent potential mispricing** while clearly separating research-only signals from tradeable/executable pricing.
- Browse individual symbols, expirations, strikes and contracts; chart historical options data and compare it with underlying price charts.
- Run cheaply on one VPS initially and support a future streaming provider through modular adapters.

**Non-goals for MVP:** automated order execution, production-grade high-frequency arbitrage, guaranteed tick-by-tick data from Yahoo, full US options market coverage, and guaranteed precise intraday backtests using stale/non-executable quotes.

---

## 3. Data Sources and Reality Constraints

### Initial source: yfinance / Yahoo Finance (prototype and research)

- Retrieve underlying OHLCV and available current options-chain snapshots with Python.
- Treat Yahoo Finance as an **unofficial, potentially delayed, incomplete, rate-limited source**. Review current source behavior, package version/API details and terms before implementing.
- **Options-chain endpoints are not a general historical chain-snapshot/candle service**: build our own history by polling, timestamping, and storing observations.
- `lastTradeDate` is the last-trade timestamp, **not proof that a bid/ask quote is fresh**.
- Intraday OHLCV access has lookback limits and interval-specific restrictions; verify current restrictions and design short-window rolling backfill, redundant refresh, and permanent local storage. Avoid assuming a universally available 4h Yahoo interval.
- Do not fabricate bid/ask quote timestamps or assume a snapshot is real-time.
- Market data timestamps must distinguish **provider quote/event time (if actually available), fetch start/end, server ingestion time, and the last trade time**.
- Respect rate limits, permitted use, exchange entitlements, and relevant licensing. Do not implement attempts to bypass access restrictions.

### Later sources

Define clear interfaces for:

- A licensed live options WebSocket source (e.g., broker data feed or other authorized market-data vendor).
- Higher-quality underlying trades/bars, ideally from the same or timestamp-compatible source.
- Historical tick/quote vendor for research backfill if economically justified.
- Source identifiers, reliability tiers, freshness metadata and provenance.

**Requirement:** adding a provider must not require rewriting the core schemas, research calculations or dashboard endpoints.

---

## 4. Collection Strategy

### Underlying OHLCV

**Canonical intraday interval: 1 minute.** Store the original 1m bars (and raw source provenance). Derive:

- 5m bars from aligned 1m constituents.
- 15m bars from aligned 1m constituents.
- 4h bars from 1m with an **explicit exchange-session alignment policy** (US regular trading session is 6.5h; avoid pretending a uniform market-day grid naturally yields full 4h candles).
- Daily bars retrieved separately from a trusted historical source (initially yfinance), reconciling them against intraday aggregation where possible.

**Daily and intraday data should be distinguished** as adjusted/unadjusted and regular/extended-hours according to their actual source configuration. Define split/dividend adjustment handling explicitly; do not silently combine adjusted daily series with unadjusted intraday prices.

**Aggregation:** `open=first`, `high=max`, `low=min`, `close=last`, `volume=sum`; only publish a fully complete bar or label partial bars explicitly. Track expected minute counts, missing/minutes, corrections and session/timezone policies. US equities/ETFs default to regular trading hours unless configured otherwise. Use exchange calendars for holidays, early closes and daylight-saving transitions.

**Proposed polling:** 1m bars in regular trading hours with a safe lag and periodic overlap/re-fetch to catch late updates; exact cadence and provider constraints to be verified. Daily historical backfill and daily reconciliation independently. Backfill longer-history daily bars before first launch.

### Options chain snapshots

Prioritize a manageable set of underlying assets and a dynamic contract universe. Proposed target cadence:

| Scope | Intended collection cadence | Notes |
|---|---:|---|
| 30–90 DTE, ATM and nearby strikes | Every 5 minutes | Highest research priority; rate limits may require reduction |
| 30–90 DTE, wider strikes | Every 15 minutes | Explicit coverage/liquidity rules |
| Other available expirations | Hourly | Budget and endpoint availability dependent |
| Entire available chain | Once per trading day | To preserve broad market structure and discover new contracts |
| 0–7 DTE and 1–5m-oriented coverage | Later phase | Likely requires licensed live feed for reliable signal/execution |

**Caution:** yfinance often retrieves entire option-chain data per expiration, rather than individually querying only a subset of strikes. Different storage frequencies do **not** automatically reduce upstream API calls. Astra should devise an honest fetch-plan/caching strategy based on how the API actually behaves and calculate expected requests for symbols × expiries × polls.

Save available fields such as contract symbol, symbol, expiration, option type, strike, bid, ask, last, bid/ask size if supported, last trade time, reported IV, volume, open interest, in-the-money flag, source/retrieval metadata. **Do not assume all fields are available in Yahoo**; store nullable fields and provenance.

Do not overwrite past snapshots; implement append-only or immutable corrections. Distinguish `NULL`/missing data from valid zeroes; flag crossed/locked markets, zero bids, stale last trades, impossible IV and illiquid contracts. Volume/open interest are not instantaneous quote events: model their observation timing and semantics accurately. Consider event-based changed-quote storage for true streaming providers later, plus checkpoint snapshots, but preserve periodic comparable snapshots for quantitative surfaces.

### Synchronization of underlying and options

- Store all canonical timestamps in UTC using timezone-aware database types, retain exchange timezone and session date.
- Define **as-of joins**, not naive equality of timestamps, for options observations against the most recently available eligible underlying price.
- Enforce no-future-data and max-age rules per feature/timeframe and provider.
- Define whether snapshot labels represent poll completion, provider event time or rounded interval boundaries.
- Use causal aggregation: a 15m indicator becomes available only once its underlying bar has closed and its source data actually arrived.
- Explicitly represent delayed provider data and propagate uncertainty into research/alert labels.

---

## 5. Recommended MVP Stack (Astra should validate)

- **Python 3.12+**, typed modules, dependency management with uv or Poetry.
- **PostgreSQL** on a single VPS for transactional metadata, 1m/derived bars, options snapshots and queries. TimescaleDB is **optional**, only if deployment and retention/query benefits justify its overhead.
- **Parquet + Zstandard** for compact partitioned historical archives, preferably using PyArrow or DuckDB for analytical reads.
- **FastAPI** for a research/data API and health/readiness endpoints.
- **Simple initial UI**: Streamlit or lightweight frontend with candlesticks, synced panels, interval selection, contracts and metrics.
- **Docker Compose** for database, worker/collector, API, UI, scheduler and (optional) monitoring.
- **Redis optional**, only if streaming/latest quote sharing creates a demonstrated need; in-process latest-values cache or PostgreSQL may suffice for MVP.
- **Job scheduling** with one clear authoritative scheduler and distributed/DB locks as appropriate; prevent duplicate concurrent collection on restarts.
- Structured logging, collection audit tables, Prometheus-compatible metrics optional.

Target low-friction local development and VPS operation. Suggest a starting envelope of 2–4 vCPUs, 4–8 GB RAM and 100–200 GB SSD for a *small targeted universe*, with capacity planning based on measured throughput rather than assumed sufficiency.

---

## 6. Suggested Logical Entities / Tables

Astra should normalize the list below, produce actual DDL/migration files in its plan, and explain primary keys/indexes/partitioning/upserts:

- `instruments`: underlying symbol, exchange, calendar, identifiers, asset class, active status.
- `option_contracts`: unique provider/exchange contract symbol, underlying ID, put/call, strike, expiration, multiplier, listing lifecycle, identifiers.
- `ohlcv_1m`: symbol ID, interval start/end, OHLCV, adjusted/raw status, RTH/ETH, source, ingestion/revision metadata, completeness flag.
- `ohlcv_daily`: separately sourced/reconciled daily data with adjustment flags and corporate-action awareness.
- `ohlcv_derived`: optional materialized timeframe bars (5m/15m/4h), with deterministic recomputation and versioned aggregation policy; alternatively derive on query and cache.
- `option_snapshots`: contract ID, source, retrieval/source timestamps, nullable bid/ask/last/IV/volume/open interest/size fields, quality flags, raw payload checksum and/or payload path.
- `underlying_quotes_or_marks`: optional temporally aligned spot/reference pricing apart from OHLCV (especially relevant for eventual live feeds).
- `collection_jobs` / `ingestion_runs`: scheduled scope, status, counts, request latency, errors, started/completed timestamps, retries.
- `data_gaps` / `quality_issues`: observed missing periods, stale markets, recoverability status and impact.
- `corporate_actions`: splits/dividends/symbol changes and provenance as needed.
- `derived_option_metrics`: versioned calculation ID, contract/snapshot or surface timestamp, theoretical values, computed Greeks, IV fitting diagnostics, liquidity filters.
- `surfaces` / `constant_maturity_features`: underlying, snapshot as-of, target DTE/delta, fitted/interpolated values, model/method/version and diagnostics.
- `archive_manifest`: partitions, coverage, checksums, source, generation timestamp and recoverability.

Decide raw payload retention and indexing tradeoffs after sizing. The immutable raw observation + deterministic derived feature pattern is strongly preferred.

---

## 7. Research Features and Modeling Boundaries

### MVP analytics

1. Synchronized underlying candlestick charts for **1m, 5m, 15m, 4h and daily**.
2. Contract browser: symbol → expiration → calls/puts → strike → historical bid/ask/mid/IV charts.
3. Underlying-versus-option panel at selected chart interval, with sampling method and quote freshness visible.
4. ATM IV, IV by moneyness/delta, expiry term structure, put-call skew, spreads and liquidity measures.
5. Constant-maturity **30d/60d/90d** analytics and constant-delta **25Δ** measures, with documented interpolation/extrapolation boundaries and no extrapolation by default.
6. Reproducible contract/metric export to CSV and Parquet (CSV export is fine; it should **not** be the primary write-intensive storage layer).
7. Point-in-time dataset creation for causal backtests.

### Later analytics

- Historical implied-volatility surfaces with diagnostics.
- Model-implied theoretical option value and uncertainty intervals.
- Realized vs implied volatility, IV regime modeling, term structure changes, skew shifts.
- Research on potentially persistent pricing discrepancies, requiring comparison against contemporaneous **bid/ask**, slippage, fees and liquidity (not stale last-price comparisons).
- Mispricing alert engine and live source upgrade.
- Optional ML feature store only when required by actual research workflows.

**Modeling caution:** delta, gamma, theta, vega, option valuation and IV surface fits depend on underlying price, rates, dividends, contract style and model assumptions. Do not present model-generated Greeks as native Yahoo quotes. Model and version each calculation.

---

## 8. Correctness, Tests and Data Integrity Requirements

The architecture and agent plan must explicitly cover:

- Idempotent ingestion keyed by contract, interval, provider and observation identity, without accidentally collapsing distinct polls into one row.
- Retries with capped exponential backoff and jitter; safe request pacing; failed poll accounting.
- Partial/empty/malformed option chain handling and schema drift detection.
- Detection of timestamp disorder, duplicate bars, bar gaps, symbol changes, delistings, split adjustments, bad strikes and contracts.
- Calendar correctness, US DST, trading halts, half-days, options expiry calendars, regular vs extended-hours policy.
- Overlap re-fetch for 1m data, deterministic correction and data provenance; do not overwrite raw histories without an audit trail.
- Historical replay/restart behavior, crash recovery, and archive round-trip tests.
- Tests for 1m → 5m/15m/4h aggregation with missing minutes and nonstandard session endings.
- Query and join tests confirming no use of information fetched **after** a hypothetical historical trade decision.
- Market-data timeliness tests: delays/staleness quantified rather than implied away.
- Backtest guardrails: spread, slippage, fees, realistic fillability and latency; volume/OI semantics.
- Configurable provider selection and fake/mock provider for deterministic testing.
- API contract tests and integration tests with a disposable PostgreSQL database.
- License/ToS review and opt-in handling for provider restrictions.

Provide sample end-to-end acceptance tests: ingest a real or fixture daily chain, persist versioned snapshots, fetch underlying 1m bars, aggregate 15m and 4h bars, align quotes point-in-time, serve both panels via API, export a causal research dataset and restore from archive.

---

## 9. Operational Requirements

- Deploy with Docker Compose to a Linux VPS; services restart cleanly and recover collector state.
- Configuration via `.env.example` and typed settings; **no hard-coded secrets**.
- Scheduling bounded to exchange sessions for high-frequency jobs; off-hours jobs for archives, backfill, QC and backups.
- Basic observability dashboard or queryable tables for: most recent successful poll per scope, quote age, request counts/errors, incomplete bars, storage growth, pending backfills, processing lag.
- Alert on ingestion stalled, unexpected null rates, provider schema changes, low disk space, unsuccessful backup or database corruption risk.
- Logical backups + periodic tested restores; defined retention policies; Parquet archives with checksums/manifests and off-VPS backup strategy.
- Support configurable ticker allowlists, expiration bands, delta/moneyness filters, requests-per-minute caps and max storage budget.
- Estimate storage as `(contracts observed × polls × bytes per record)` plus database/index/WAL/backups and quote-event bursts. Validate with an actual 1-week pilot before scaling.
- Support service-level safety: API authentication or VPN-only access for private dashboards; no public database port; least-privilege credentials.

---

## 10. Proposed Phases for Downstream Agents

Astra should refine this order into discrete tickets and parallelizable workstreams.

**Phase 0 — Feasibility/validation**
- Confirm current yfinance capabilities, quote fields, per-interval OHLCV lookbacks, rate limits/ToS, options expiry discovery and likely request budget.
- Decide initial target universe (configurable: e.g., 2–5 liquid ETFs/stocks), exchange-hours conventions and pilot storage budget.
- Provide an API proof-of-concept and record observed field quality/latency without overclaiming freshness.

**Phase 1 — Infrastructure & schemas**
- Python project scaffold, typed models, Docker Compose, PostgreSQL, schema migrations, logging, config, fake provider, scheduler and health checks.

**Phase 2 — Underlying price collector**
- Historical daily backfill; rolling 1m ingestion; overlap reconciliation; 5m, 15m, 4h aggregation; calendar handling, gaps and deterministic unit tests.

**Phase 3 — Options chain collector**
- Discovery, contract normalization, chain snapshot ingestion, priority scopes, quality flags, throttling, retries, completeness reports and tested idempotency.

**Phase 4 — Synchronization + analytics foundation**
- As-of joins, source timestamps, quote-age limits; bid/ask/mid; initial IV term/skew calculations; derived feature versioning and export API.

**Phase 5 — Visualization and data exploration**
- Chart selected underlying timeframes and synchronized options panels; contract browser; freshness/coverage indicators; export and data-quality overlays.

**Phase 6 — Long-term robustness**
- Parquet archiving, retention, backfill/gap repair, operational alerting, benchmark/capacity plan, off-site backups and restore test.

**Phase 7 — Enhanced quant models**
- Constant-maturity/delta analytics, fair value and risk models, point-in-time backtester and paper alerts with realistic costs.

**Phase 8 — Real-time provider adapter (optional)**
- Licensed live WebSocket ingestion, event-based updates, source sequencing/timestamps, fast quote cache, lower-latency indicators, coexistence with historical ingestion.

For each phase, specify definition of done, dependencies, estimated complexity, agent assignment boundaries, review checklist and concrete reproducible tests. **Do not allow later agents to silently skip source-quality and timestamp correctness work.**

---

## 11. Decisions Astra Must Resolve Explicitly

1. **4-hour candle convention:** exchange-session anchored 4h + remainder vs fixed wall-clock 4h; show labeling and incomplete final bar policy. Ensure UI explains this convention.
2. **Source data treatment:** adjusted vs raw and RTH vs ETH, and how daily bar reconciliation works.
3. **Options polling cost:** API calls per expiry and symbol, not just number of contracts retained.
4. **Quote timestamp truth:** distinguish retrieval time from provider quote time; represent uncertainty.
5. **Persistence shape:** append-only snapshots vs deduplicated changed-state storage vs hybrid, with query semantics.
6. **Historical bar construction:** precompute 5m/15m/4h or compute on demand; correction propagation.
7. **Underlying mapping:** contract symbol uniqueness, ticker history and corporate actions.
8. **Temporal research joins:** max permissible quote age by timeframe, lag assumptions and out-of-session rows.
9. **Initial deployment:** simplest viable single-VPS architecture, no premature Kafka/Kubernetes/microservices.
10. **Operational growth trigger:** when to increase storage, move to TimescaleDB, use Redis, adopt a licensed feed or add dedicated compute.

Where a decision requires experiments, define the experiment, evidence needed, and provisional safe default.

---

## 12. Response Format Requested From Astra

Produce the following markdown artifacts (or sections), with practical detail:

- `00-assumptions-and-constraints.md`
- `01-architecture-and-adrs.md`
- `02-schema-and-data-contracts.md`
- `03-collection-scheduling-and-recovery.md`
- `04-agent-execution-roadmap.md`
- `05-tests-and-acceptance.md`
- `06-deployment-operations-and-costs.md`
- `07-provider-upgrade-strategy.md`

Each coding-agent ticket should include:

```text
Ticket ID / short title:
Goal:
Prerequisites:
Scope and owned paths:
Inputs / outputs / interfaces:
Implementation steps:
Acceptance criteria:
Test commands and expected results:
Risks / edge cases:
Do not change:
```

Finish with:

1. A concise **MVP cutline** separating what is essential from optional.
2. A **dependency graph** of tickets and a suggested safe parallelization strategy.
3. A **first agent prompt** that can be copied directly to launch Phase 1 implementation.
4. Any essential open questions, each paired with a sensible provisional default, so planning does not stall.

**Overall guiding principle:** A trustworthy, timestamp-correct 5-minute historical options snapshot system plus reliable 1-minute underlying candles is more valuable than a superficially “real-time” platform whose quote freshness and historical reproducibility cannot be verified.
