# 00 — Assumptions and constraints

Status: proposed implementation baseline, 2026-10-24. Requirements originate in [the brief](../../options_market_data_aggregator_astra_brief.md). Words **must**, **reject**, and **gate** are acceptance requirements for downstream agents. Configuration defaults are project decisions, not provider guarantees.

## Scope and provisional defaults

| Decision | Default | Evidence needed to change |
|---|---|---|
| Initial instruments | SPY and QQQ; configurable allowlist, no auto-expansion | Five-session request/storage pilot and approved data use |
| Instruments supported | US USD equity/ETF, standard deliverable contracts | Contract reference data and settlement tests for other products |
| Priority | 30–90 calendar DTE inclusive; daily/4h then 15m research | Explicit product expansion |
| History | Five years of daily data where returned; rolling recent 1m; options only from first observation | Entitled historical source |
| Collection session | Underlying RTH intersection, normally 09:30–16:00 America/New_York | Per-product session metadata; do not assume all options stop at 16:00 |
| Deployment | Linux VPS, 4 vCPU / 8 GiB / 160–200 GiB SSD planning envelope | Measured pilot, actual supplier quote |
| Users | One private research operator; read-only dashboard | Authentication/entitlement redesign before broader access |
| Storage | PostgreSQL 18, Parquet/Zstd, off-host encrypted backups | Benchmarks and licensing/retention rules |
| Live source | Fake provider by default; Yahoo explicitly enabled after source review | Recorded rights assessment and successful capability probe |
| Price policy | Explicit provider adjustment basis; never assume `auto_adjust=False` restores as-traded historical prices | Corporate-action verification |
| Latency claim | Collection cadence only; source freshness may be unknown | Actual event timestamps and entitled feed |

30–90 DTE means `(expiration_date - exchange_session_date).days`, not business days. Valuation time-to-expiry is a separate ACT/365 year fraction to a verified settlement/exercise cutoff. Do not infer the cutoff from the expiration date alone. Adjusted deliverables, unknown multiplier/style, and uncertain expiration mechanics may be browsed but are ineligible for valuation and simulated fills.

## Source evidence checked for this plan

These are documentation/source-code checks, **not live API experiments**. Pin the actual package version and commit in Phase 0; mutable `main` links below are evidence pointers, not reproducible dependencies. No confirmed sustainable Yahoo rate limit is available from these checks.

| Evidence | Verified implication | Implementation consequence |
|---|---|---|
| [yfinance option implementation](https://github.com/ranaroussi/yfinance/blob/main/yfinance/ticker.py) | Fetches an expiry chain; expiry discovery can add a request; standard output includes last-trade time, bid/ask, IV, volume/OI, but not bid/ask event times or sizes | Cache discovery, share expiry fetches across retention tiers, leave unsupported fields null |
| [yfinance download reference](https://ranaroussi.github.io/yfinance/reference/yfinance.functions.html) | Documents supported intervals and general intraday restrictions; no 4h interval is listed | Derive 4h locally; probe individual interval limits |
| [yfinance history implementation](https://github.com/ranaroussi/yfinance/blob/main/yfinance/scrapers/history.py) | Current code uses an eight-day default for 1m `max`, and 60-day defaults for several other intraday intervals | These are code defaults, not promised retention or query limits; begin with seven-day 1m requests and record actual boundaries |
| [Yahoo data and delay notice](https://help.yahoo.com/kb/SLN2310.html) | Lists OPRA delay as 15 minutes and restricts redistribution | Store advertised delay separately from unknown actual quote age; private outputs only within permitted use |
| [yfinance README](https://github.com/ranaroussi/yfinance/blob/main/README.md) | Describes the package as unofficial and directs users to Yahoo data-use terms | Package license does not grant market-data rights |
| [NYSE calendars](https://www.nyse.com/markets/hours-calendars) | Publishes core hours, holidays, and early closes; eligible options may have different closing times | Use product/session calendars, never a weekday-only schedule |
| [exchange_calendars source](https://github.com/gerrymanoim/exchange_calendars/blob/master/exchange_calendars/exchange_calendar.py) | Minute inclusion is configurable | Explicitly select left-labeled minutes and test counts |
| [PostgreSQL partitioning](https://www.postgresql.org/docs/18/ddl-partitioning.html) | Partitioned unique constraints must include partition keys | Snapshot identities and foreign keys include observation time |
| [Alpaca option stream](https://docs.alpaca.markets/us/docs/real-time-option-data) | Example candidate offers subscription-dependent indicative/OPRA feeds and quote timestamps/sizes | Capability and entitlement checks are mandatory; it is an example, not a selected vendor |
| [OPRA](https://www.opraplan.com/) | Publishes options data licensing material and operational notices | Review subscriber, display/non-display, retention, and redistribution rights before upgrade |

The [Yahoo terms page](https://legal.yahoo.com/us/en/yahoo/terms/otos/index.html) could not be retrieved during planning (HTTP 999). Applicable jurisdiction/account terms, automated collection permissions, and retention rights remain **unverified**. A configuration acknowledgement records a review; it cannot create permission. If intended collection is prohibited or unresolved, use fixtures or an authorized alternative. Do not bypass access restrictions.

## Phase 0 experiments and stop conditions

The feasibility agent creates `reports/feasibility/{environment.json,capabilities.json,request-budget.json,source-review.md,probe-results.parquet}`. Include UTC check time, package versions, source links/commit, platform, approved test scope, and sanitized request accounting. Never commit real payloads without permission; synthetic fixtures are the default committed evidence.

1. With approved live use, probe two symbols, expiry discovery, one 30–90 DTE expiry each, and daily/1m histories. Record returned columns/types, null rates, duplicates, times, exceptions, endpoint/network request counts, and the distinction between cold/warm clients. Stop immediately on access denial; do not rotate identities or proxies.
2. Probe 1m windows of one day and seven days, then one older short window within the documented/observed horizon. Separately test 5m/15m/60m boundaries only if needed for fallback history. Record maximum request span **and** oldest retrievable point separately. Empty results are inconclusive until calendar/status checks. Never infer that splitting requests recovers expired data.
3. Fetch daily around known split/dividend events; inspect adjustment settings, volume conventions, date indexing, and whether historical OHLC are already split normalized. Store source semantics and a test fixture. If as-traded prices cannot be established, label `provider_native`, exclude incompatible option/spot valuation, and preserve charts with an explicit basis label.
4. Run a five-trading-session pilot at the computed budget. Measure actual network calls, discovery overhead, error rates, p50/p95 fetch-to-commit lag, row counts, payload/DB/index/WAL/archive bytes, late bar corrections, expiry coverage, and time skew across a cycle. Do not call fetch latency quote latency.
5. Gate live MVP on reviewed use, documented capabilities, no unresolved schema drift, explicit unknown freshness, and sustainable scheduling. If five-minute coverage is infeasible, reduce expiries/symbols first; otherwise declare a measured slower cadence. Never quietly label slower polling as five-minute coverage.

Fixture development is unblocked while these experiments are pending. A source outage is an observed limitation, not a reason to weaken timestamp requirements.

## Essential open questions and safe defaults

| Open question | Provisional default / owner |
|---|---|
| Exact tickers and expiry density? | SPY/QQQ, at most four high-priority expiries per symbol; nearest to 30/45/60/90 DTE with distinct-expiry tie breaking; collector owner |
| Full 30–90 coverage or bounded sample? | Bounded sampled expiries initially; expose omitted expiries and selection policy; product owner |
| Are research automation, storage, and exports permitted? | Yahoo live disabled until documented resolution; operator/source-review owner |
| Budget and chosen VPS region? | Planning allowance only, USD 50/month infrastructure target excluding data; operator gets a quote |
| Must this support execution decisions? | No; execution eligibility always false for timestamp-unknown Yahoo observations |
| How long to retain history? | Archive normalized history indefinitely within rights and budget; 90 days hot; verify rights first |
| Need extended hours or adjusted contracts? | Excluded from default analysis, retained with flags if encountered |
| Rates/dividends/settlement data source? | Synthetic curves for tests; model outputs disabled on real data until verified inputs exist |
| What performance is acceptable? | Pilot objectives in document 06, then revise with measured evidence |

No open question requires inventing a provider capability. All unresolved fields remain null/unknown, and every override is versioned in configuration and dataset manifests.
