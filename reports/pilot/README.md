# Five-session pilot (T19): running

Status as of 2026-10-26: **the live pilot is collecting.** The release decision is due after the sessions of 2026-10-26 to 10-30.

- **Source:** Yahoo Finance via yfinance, approved by operator decision `operator-2026-10-26` (`reports/feasibility/live/yahoo/`). This is the operator's acceptance of responsibility, not an independent legal review.
- **Universe:** SPY and QQQ (`config/universe.yahoo.yaml`).
  - Four priority expiries per symbol (30–90 DTE) are polled every 5 minutes. The planned budget uses 39% of the self-imposed 10 requests/minute.
  - Rolling 1m bars feed the derived 5m/15m/4h candles.
- **History bootstrap (done 2026-10-26):** 5 years of daily bars (1,255 per symbol), the last 5 sessions of 1m, and derived candles for those sessions.
- **Quote freshness is unknown.** Yahoo supplies no quote event times, so nothing here is execution-grade.

## Automatic decision

`marketagg-pilot-report.timer` runs `deploy/pilot_report.sh` at 2026-10-31 02:00 UTC. It writes `capacity.json`, `capacity.md` and `release-decision.json` here, and `release-check.log` records the exit status (0 means the release is allowed).

A dry run on 2026-10-26, before any live session, failed only the gates that need live sessions. Every other gate passed.

## If a gate fails

- **Priority coverage below 95%:** reduce expiries or symbols, or publish `cadence-limitation.json` with the measured cadence. Never relabel slower polling as five-minute coverage.
- **No backup during a session:** the daily 17:00 UTC backup timer covers this once a session has run.
