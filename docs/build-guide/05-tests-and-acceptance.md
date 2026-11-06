# 05 — Correctness, research validation, and acceptance

## Test strategy and evidence

Test the risky behavior: time, identity, revisions, failure recovery, and data-use boundaries. Do not write tests merely asserting a configuration constant equals itself. Unit/contract tests run offline with a fake clock. PostgreSQL integration tests use a disposable real PostgreSQL 18 instance, not SQLite substitutes. Freeze the calendar package and synthetic fixtures; random/property tests use recorded seeds. Live source probes are marked `live`, opt-in, rate-bounded, and separate from CI.

T01 supplies pytest configuration and tools. Standard CI command after implementation:

```text
uv sync --frozen --all-extras
uv run ruff check .
uv run mypy src/marketagg
uv run pytest tests/unit tests/contracts
uv run pytest tests/integration -m "not live"
uv run python scripts/run_acceptance.py --profile fixture
```

Tests create/restore only an explicitly disposable database. Their fixture requires a database name prefix `marketagg_test_` and refuses the configured production DSN. These commands do not exist as a working application in this planning handoff; T01–T18 create them. Capture versions, image digest, seed, fixture checksum, test results, and elapsed time in each acceptance report.

## Required fixture pack

One two-symbol synthetic week with normal sessions, a holiday, a 13:00 early close, separate March DST transition cases, two 30–90 DTE expiries, 12 strikes/side, and one nonstandard contract. Include:

- All 390 normal-session minute bars and 210 early-close minute bars, distinguishable OHLCV for checking first/max/min/last/sum; one omitted minute, one duplicate, one disorder case, one confirmed halt interval, one invalid OHLC row.
- A minute corrected after the corresponding 15m and 4h bars were published; a previously accepted observation invalidated later; a corrected parser result from the same payload.
- Two equal-price polls with different response IDs; replay of one ID; two expiries fetched 90s apart and a third 180s late; calls-only response, empty discovery, and changed source column type.
- Positive bid/ask, zero bid, locked, crossed, negative, missing/NaN values, stale trade with unknown quote time, zero OI vs null OI, cumulative volume resets, unknown OI date and absent sizes.
- Symbol change, delisting, known split, dividend announced later, contract deliverable change, unknown exercise cutoff, wrong underlying symbol, and two possible source ID mappings.
- Clock skew, timeout, 429/Retry-After, denied access, corrupt payload, disk-full, lease expiry, archive corruption, and process termination at durable boundaries.

No fixture pretends to be licensed historical data. Real fixture candidates require rights review and removal of credentials/account identifiers.

## Temporal adversaries (release-blocking)

| Case | Setup | Required result |
|---|---|---|
| Delayed publication | 09:45 bar fetched at 09:46:00, known 09:46:03 | Decision 09:46:02 cannot use it |
| Future bar closure | Complete constituent data observed before bucket end due to malformed fixture | Bucket cannot publish as closed before actual interval end |
| Later correction | Minute known 10:01, corrected known 11:00 | 10:30 query sees original; 11:01 sees correction |
| Later invalidation | Accepted quote at 10:00, invalidated at 10:10 | 10:05 can see original with then-known flags; 10:11 cannot silently reuse it |
| Delayed option commit | Fetch completes 10:00:00, durable knowledge 10:00:04 | 10:00:02 decision excludes quote |
| Last trade confusion | Old last trade, recent response, quote time missing | Quote age remains null; execution eligibility false |
| Backfilled history | Last year's daily data fetched today | Last-year observed-mode query has no such data |
| Late feature compute | Inputs known 10:00, indicator published 10:05 | Observed 10:02 excludes; recomputed mode may include only with labeled simulated lag |
| Universe leakage | Contract first discovered at 11:00 | 10:00 universe cannot use hindsight discovery |
| Terms/action leakage | Split or dividend correction known after D | Earlier dataset uses only then-known terms/actions |
| Session boundary | Last option observation Friday afternoon | Monday early decision cannot carry it across session |
| Mixed sources | One source delayed, another timestamped; policy unspecified | Reject comparability or return an explicitly descriptive diagnostic |

Add a mutation-style assertion: removing `known_at <= D` or moving revision selection ahead of eligibility must make adversarial tests fail. Test exact boundary equality (`<=`) and one-microsecond differences. Store exact nanoseconds if supplied; avoid claiming finer accuracy than receive clocks.

## End-to-end acceptance scenarios

### A01 — Normal fixture session

Apply migrations to an empty DB, load policies and fake provider, ingest one complete daily chain sweep and rolling underlying 1m. Run a second five-minute poll with identical prices. Expected: two distinct observations per selected contract; reprocessing either response changes no counts; source response metadata and unknown freshness preserved. Query chain coverage and find both sides/expiries with actual fetch windows.

### A02 — Candles and synchronized panels

Aggregate all intervals. Normal session yields 78 five-minute, 26 fifteen-minute and two session-4h candles with durations 240 and 150 minutes. Early close yields 42 five-minute, 14 fifteen-minute and one 210-minute 4h bucket. Verify OHLCV by independent fixture calculation. One missing minute makes its containing buckets incomplete; default research excludes them while UI can show explicit partials. Both API panels share a declared decision/sampling convention and show unknown quote-event age.

### A03 — Correction and causal export

Export at a frozen 10:30 decision cutoff; inject an 11:00 correction and recalculate. Old export semantic hash and old asof remain unchanged. Current view shows the correction. Recompute an old strategy using only inputs known at the historical cutoff and label its availability mode. Prove that freshly backfilled last-year data do not leak into observed-mode history.

### A04 — Scheduler contention and restart

Run two planners and two workers against the same queue. Kill a worker after response spool, after staging commit, and before/after final publication. Expected: one logical scheduled job, no stale-lease commit, no duplicate replay of a response, conservative known time after recovery, and missed past options slots recorded rather than backfilled with current quotes.

### A05 — Provider failures and source drift

Inject 429, 403, timeout, empty chain, missing puts, renamed/changed type column and contract ambiguity. Expected: bounded retries, cooldown respecting Retry-After, access-denied disablement, successful valid rows only with partial coverage, schema incident, and no guessed values. Pipeline continues independent permitted work. Recovery never rotates proxies/accounts to bypass a restriction.

### A06 — Archive round trip

Archive a fixture partition with all required batch/identity/policy metadata; verify row counts, canonical hashes, types, UTC, nulls, Decimal and optional nanoseconds. Query hot+archive overlap without duplicates. After authorized test-only hot pruning, identical asof/panel/export queries return the same semantic result. A late correction creates a new catalog generation and preserves old pinned datasets.

### A07 — Clean disaster recovery

Create encrypted off-host backup, discard only the designated disposable test stack, restore to a differently named DB/storage root, and disable live network. Resolve all manifests and reproduce A03's dataset hash. Record actual RPO (latest recoverable knowledge time) and RTO (start to validated API readiness). A backup file existing is not passing this test.

### A08 — Private API and UI

Unauthenticated data routes deny access; health contains no secrets; API rejects injection/path traversal and unbounded export requests. UI browses symbol -> expiry -> side -> strike; shows five interval choices, bid/ask/mid/reported-IV history, shortened-4h explanation, quality/gap overlays, source/basis and unknown freshness. Review screenshots at desktop and narrow widths. No direct UI DB credentials.

### A09 — Resource pressure

Load 90-day synthetic pilot-equivalent data and a one-hour 3x request/result burst, using configurable rate scaling only against the fake provider. Impose memory and disk thresholds. Expected: bounded queues, bulk job throttling, useful errors and alert state, no unverified pruning, no loss concealed as success, no OOM under the chosen limits. Query/report latency targets are measured, not assumed.

### A10 — Licensed provider substitution (Phase 8)

Replay the same canonical fixture through a second adapter; all API contract tests pass. Add out-of-order events, reconnect gap, timestamp precision, duplicate sequence, and indicative feed. Source versions remain distinct; strict eligibility rejects inadequate feed capabilities. Shadow cutover/rollback changes future policy selection and leaves old dataset manifests intact.

## Quant validation requirements for Phase 7

Descriptive statistics remain available before modeled features. Reported IV is provider-reported, not a verified model input. Price fitting requires a declared pricing model, expiry mechanics, source-compatible spot, rates, dividends and units; missing information gives a diagnostic, not a default silently borrowed from today.

Constant-maturity method v1: for bracket maturities `T1 <= T* <= T2`, compute total variance `w_i = sigma_i^2 * T_i`; interpolate `w* = (1-a)w1 + a*w2`, `a=(T*-T1)/(T2-T1)`; output `sqrt(w*/T*)`. Require positive maturity, finite nonnegative variance, same source/method and comparable strike/delta definition. Exact knots need no interpolation. Never extrapolate. A 90-calendar-DTE endpoint can still fail a 90/365 target depending on exact expiry time; report missing support. Wider expiry collection needs a separate budget review.

25Δ uses **signed spot delta** from the selected model: +0.25 call and -0.25 put, not absolute delta without side. First interpolate IV within each expiry using eligible strikes bracketing the target delta; reject nonmonotonic/ambiguous curves. Then interpolate total variance across expiries. State skew convention explicitly: `IV(25Δ put) - IV(25Δ call)` in volatility percentage points. ATM v1 selects strike closest to compatible spot, ties by lower strike; do not conflate ATM strike with 50Δ. Surface diagnostics flag calendar/butterfly consistency issues without claiming American-model surfaces obey all European identities unchanged.

Model tests cover grid convergence, analytical European limit cases, monotonicity where applicable, payoff bounds, expiry limits, early exercise/dividend cases, Greek finite-difference stability, IV solver brackets/repricing error, and unit conversion. A convergence failure returns null and error code. Use bid/ask-derived valuation ranges and input perturbations rather than a falsely precise fair-value scalar alone.

Backtests split by time with walk-forward training, purge/embargo overlapping holding periods, and fit transformations on training data only. Frozen point-in-time universe includes inactive/delisted securities and lost/expired contracts; missing data are reported by cohort. No selecting today's liquid survivors or the ultimately best expiry with future knowledge. Record training cutoff, parameter search count, out-of-sample period, and coverage attrition. Adjusted returns and actual contract cashflows are separate accounting paths.

Strict execution replay requires contemporaneous timestamped quotes, appropriate sizes/conditions, verified terms, and cost modeling. Long buys enter at ask and sell at bid; shorts reverse sides. A midpoint scenario is a labeled optimistic sensitivity, never the default fill. Unknown size/event-time Yahoo history may support hypothetical cost scenarios but not validated executable P&L. Multi-leg trades need a joint timing/legging rule; single-leg MVP only.

Required sensitivity grid (experimental inputs, not claimed fees): slippage 0/0.5/1 additional spread units; fees USD 0/0.65/1.00 per contract per side plus configurable exchange charges; event/feed delay 0/1/5/15 seconds for licensed replay and observation-lag scenarios 0/1/5/15 minutes for Yahoo descriptive data; spread caps 10/20/40% of midpoint; participation caps 1/5/10% of **eligible incremental traded volume**, if available. Cumulative daily volume cannot establish instantaneous fillability; reject volume-participation modeling when its timing is unknown. Include turnover, drawdown, gross/net P&L, excluded decisions, unfilled orders, and outcome sensitivity rather than one best-case return.

Overnight/expiry positions need explicit settlement, dividends, exercise/assignment and borrow accounting. Until implemented, reject strategies holding through those events and report that selection limitation. Never mark assumed assignment handling as tested reality.

## Release checklist and requirement traceability

| Brief requirement | Implementation / proof |
|---|---|
| ADRs and one-VPS boundaries | 01; T01/T17 |
| Schemas, provenance, immutable observations | 02 + SQL; T03/A01/A03 |
| Source capability/terms limitations | 00; T00/T06/A05 |
| Polling economics, backfill, recovery | 03; T05/T07/T08/T10/A04/A05 |
| 4h/session/adjustment decisions | 01–03; T04/T09/A02 |
| Causal alignment and derived publication | 02; T11/A03 + temporal adversaries |
| Browser/chart/export | T12–T15/A08 |
| Model/constant maturity/delta/backtest | T20–T22 + quant tests |
| Archive/retention/backup/cost | 06; T16–T19/A06/A07/A09 |
| Provider migration | 07; T24/A10 |
| Final tasks/dependencies/launch prompt | 04 |

MVP release requires A01–A09, no unresolved critical temporal/security defects, documented live-source eligibility, complete five-session evidence, and a successful off-host restore within provisional targets. If live access is unresolved, publish only the fixture implementation milestone; do not declare the live aggregator complete. Phase 7/8 gates do not block the explicitly bounded MVP.
