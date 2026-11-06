# UPDATE1: first-session review and fixes (2026-10-27)

This covers what changed after the review of the first live session (Mon 2026-10-26), and what the aggregator collects, so you can check the data by hand.

## 1. Edits

| # | Change | Files | Why |
|---|---|---|---|
| 1 | **Request-budget gate measures correctly, and checks what the design specifies** | `src/marketagg/release.py`, `tests/integration/test_release.py` | The gate credited a whole run's requests to the minute the run started. The 24-request broad sweep therefore showed as 34 req/min, while real pacing held 10/min (24 requests over 144 s). Requests are now spread over the time each run actually took. The gate now checks the two rules in design doc 03: **planned utilization ≤ 70%** (session requests ÷ 10/min × session minutes) and **measured peak ≤ cap + burst (10 + 1)/min**. |
| 2 | **Broad sweep split into chunks before the close** | `src/marketagg/scheduler/planner.py`, `src/marketagg/collectors/options.py`, `tests/unit/test_budget.py` | The sweep was one 24-expiry job per symbol at 19:55 UTC. It held the single worker for ~2.5 min each, which skipped 6 `bars_1m` and 4 `derive_bars` jobs and made the QQQ 20:00 chain 2m09s late. It now runs as 8 chunks of 3 expiries per symbol, starting 19:40, ~2 min apart, all due by the 20:00 close. Higher-priority jobs (1m bars, priority chains) run between chunks. Expiries covered are unchanged: still 24 per symbol. |
| 3 | **Dashboard/API port 8000 reachable only over Tailscale** | `deploy/firewall.sh` (new), `/etc/systemd/system/marketagg-firewall.service` (new, enabled), `deploy/runbooks/deploy.md`, `deploy/compose.yaml` (comment) | Internet scanners were probing port 8000. Basic auth rejected all 328 attempts, but the port no longer needs to be public. Docker-published ports bypass ufw, so the rule sits in Docker's `DOCKER-USER` chain. It drops port-8000 traffic that doesn't arrive on `tailscale0`, and re-applies after reboots and Docker restarts. **SSH (22) is unchanged.** Lightsail's browser SSH connects from a public AWS address, so restricting 22 would lock you out of it. |

### Verification

- **Unit, contract and integration tests:** 267 passed. 2 role-bootstrap tests failed only because my throwaway test DB required passwords. Rerun on a trust-auth DB (what they expect), all 18 tests in that group passed. Ruff and mypy are clean.
- **Acceptance suite:** 18 of 18 passed (section 4).
- **Gate on real data:** re-measuring Oct 26 with the new code gives planned utilization **39.8%** and peak **10.88/min**, so `request_budget` now passes. The only gate still failing is `five_sessions`, which is expected until Friday's session is recorded.
- **Firewall:** from the server, the public IP `3.96.212.84:8000` now times out and `100.66.225.1:8000` returns 200. Check `sudo iptables -L DOCKER-USER -v -n`: the DROP counter rises as outside traffic is blocked.
- **Deploy:** a verified backup was taken first (`/backups/20261027T031309Z-795a462b`). Then the new image was built and scheduler, worker, api and ui were restarted, while markets were closed. `/health/ready` reports ok.
- **Rollback:**
  - Previous image: `marketagg:pre-update1`.
  - Firewall: `sudo systemctl disable --now marketagg-firewall.service && sudo iptables -D DOCKER-USER 1`.

### Things to watch

- **Tue 2026-10-27 19:40–20:05 UTC:** in `collection_jobs`, look for 16 `chain_broad` jobs, all `succeeded`, and no `deadline_passed` skips (query C below).
- **Firewall:** the dashboard now only opens at `http://100.66.225.1:8000`, from a device signed in to Tailscale. In the Lightsail console you can also remove the port-8000 rule; it isn't needed any more.
- **Peak margin:** the peak measurement is an estimate, and 10.88 is close to the 11 limit. If a session ever shows a peak above 11, check whether pacing really broke before assuming it did.

## 2. What we aggregate

Source: Yahoo Finance via yfinance, for **SPY and QQQ**. Session hours are 13:30–20:00 UTC (9:30–16:00 ET).

| Data | Cadence | Scope | Oct 26 result |
|---|---|---|---|
| **Option chains, priority** | Every 5 min, 13:35–20:00 | 4 expiries per symbol nearest 30/45/60/90 DTE: Nov 27, Dec 4, Dec 31, Jan 15 | 156 polls (78 per symbol), 206,310 rows. SPY 78/78 on time, QQQ 77/78 |
| **Option chains, other** | Hourly, 13:35–19:35 | Up to 6 nearest non-priority expiries (0DTE through ~1 week) | 14 polls, 22,183 rows |
| **Option chains, broad** | Once a day before close (now 19:40–19:53) | Up to 24 expiries per symbol | 48 expiries, 14,250 rows |
| **Option contracts** | As discovered | SPY 7,269 (3,723 C / 3,546 P), QQQ 6,981 (3,599 C / 3,382 P) | Expiries Oct 26 2026 – Sep 2027 |
| **Underlying 1m bars** | Every minute (rolling 20-min window, revisions kept) | Regular session | 390/390 minutes per symbol |
| **Derived 5m / 15m / 4h bars** | Every minute, from 1m | Regular session | 156 / 52 / 4 buckets |
| **Daily bars** | Pre-open and 30 min after close | 5-year history | 1,255+ per symbol through Oct 26 |
| **Dividends** | With daily refresh | 2021→ | SPY 20, QQQ 21 |

**Fields on every option snapshot** (fill rate on Oct 26):
- **Always filled:** bid, ask, last, implied volatility, in-the-money, last-trade time (100%).
- **Open interest:** ~100%.
- **Volume:** 95–97%; the gaps are contracts that haven't traded that day.
- **Also on each row:** observation time, quality flags, a content hash and a link to the raw payload.

## 3. What we do **not** get, by design or from the source

- **Bid/ask sizes:** Yahoo doesn't supply them (0%).
- **Quote event times:** Yahoo gives none, so every row is flagged `unknown_quote_time`. Freshness is only "when we polled", which is not good enough to trade on.
- **Open-interest date:** unknown, flagged `unknown_oi_date`.
- **Spot price per chain snapshot:** `underlying_marks` is empty. Use the 1m bar for the matching minute instead.
- **Greeks and model values:** not computed yet (T20+).
- **Quality flags seen:**
  - `old_last_trade`: last trade more than 60 min old; 75% of rows, normal for far strikes.
  - `zero_bid`: 20,731 rows.
  - `locked`: bid equals ask; 13 rows.
  - Crossed markets: none.
- **Known open issue, not fixed in this update: 1m volume.**
  - **Bad spikes:** 5–35 bars per symbol per day carry roughly the running day total in a single minute. This comes from Yahoo and is also in the downloaded history. As a result, summed 1m volume runs about 4–12× the daily volume.
  - **Flip-flops:** re-fetches flip some minutes' volume between the real value and 0.
  - **Final state:** 3 bars ended with volume 0 (SPY/QQQ 13:30, SPY 19:55).
  - **Effect:** 5m/15m/4h volumes inherit these errors. **Prices are fine. Don't use 1m volume yet.**
- **Known open issue, not fixed in this update: one blank chain poll.**
  - The entire **13:45 UTC priority poll on Oct 26** came back from Yahoo with bid = ask = 0 for 82–100% of rows. That's both symbols, all 8 expiries, near-the-money strikes included. `last` was still populated.
  - It was stored, and counted as an on-time poll, with only row-level `zero_bid` flags. No other poll that day looks like this.
  - **Until a batch-level check exists:** treat a poll where most near-the-money contracts show 0/0 as missing, not as a real quote. Query E finds them.

## 4. Acceptance suite

**18 passed** (`tests/acceptance -m "not live"`, 17 min, on a throwaway PostgreSQL 18 that was removed afterwards). This includes the backlog-shedding scenario, which exercises the new chunked broad-sweep jobs.

## 5. Manual check queries

Run with `docker exec -it marketagg-db-1 psql -U marketagg_owner -d marketagg`.

```sql
-- A. Chain polls per symbol/expiry for a day (chain_priority: 78 per expiry on a full session)
SELECT scope->>'symbol' sym, scope->>'expiration' exp, scope->>'kind' tier, count(*) polls
FROM observation_batches
WHERE batch_kind='chain' AND fetch_started_at::date = '2026-10-27'
GROUP BY 1,2,3 ORDER BY 1,3,2;

-- B. 1m bar coverage (expect 390 per symbol on a full session)
SELECT ii.symbol, count(DISTINCT b.interval_start)
FROM bar_observations b JOIN instrument_identifiers ii USING (instrument_id)
WHERE b.timeframe='1m' AND b.session_date='2026-10-27' GROUP BY 1;

-- C. Anything skipped, failed or late today (expect no rows)
SELECT kind, status, last_error_code, scheduled_for, scope
FROM collection_jobs
WHERE scheduled_for::date='2026-10-27' AND status <> 'succeeded' ORDER BY scheduled_for;

-- D. Spot-check one contract through the day (compare against Yahoo/your broker)
SELECT o.observed_at, o.bid, o.ask, o.last, o.reported_iv, o.reported_volume, o.reported_open_interest
FROM option_snapshots o JOIN option_contracts c ON c.id=o.contract_id
JOIN instrument_identifiers ii ON ii.instrument_id=c.underlying_id
WHERE ii.symbol='SPY' AND c.expiration_date='2026-12-31' AND c.option_type='C' AND c.strike=765
ORDER BY o.observed_at;

-- E. Blank polls: chain responses where most quotes are 0/0 (Oct 26 shows the 13:45 poll)
SELECT o.observed_at, ob.scope->>'symbol' sym, ob.scope->>'expiration' exp, count(*) rows,
       count(*) FILTER (WHERE o.bid=0 AND o.ask=0) zero_both
FROM option_snapshots o JOIN observation_batches ob ON ob.id=o.batch_id
WHERE o.observed_at::date='2026-10-27'
GROUP BY 1,2,3 HAVING count(*) FILTER (WHERE o.bid=0 AND o.ask=0) > 0.5*count(*) ORDER BY 1;
```

Overall health: `docker compose -f deploy/compose.yaml run --rm --no-deps scheduler ops status --profile production --json`. Expect `"severity": "ok"` and `"alerts": []`.
