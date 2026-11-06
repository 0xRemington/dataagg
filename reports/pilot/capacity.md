# Pilot capacity report: yahoo

Measured at 2026-10-31T02:01:02.015227+00:00. Live source: **True**.

| Measure | Value |
|---|---|
| sessions_measured | 5 |
| min_scope_chain_coverage | 0.9871794871794872 |
| skipped_slots | 0 |
| min_minute_coverage | 0.8307692307692308 |
| max_fetch_to_known_p95_seconds | 4.817308000000001 |
| peak_requests_per_minute | 10.9 |
| request_budget_utilization | 1.09 |
| planned_request_utilization | 0.4123 |
| quote_event_time_known_ratio | 0.0 |
| freshness_claim | collection_cadence_only |
| verified_backups_during_sessions | 5 |
| projected_headroom_days | 192.02132923405202 |
| bytes_per_session | 176046468.0 |

## Per session

| Session | Requests | Peak/min | Skipped slots | Option rows | Errors |
|---|---|---|---|---|---|
| 2026-10-26 | 1553 | 10.88 | 0 | 242743 | {'unavailable': 3} |
| 2026-10-27 | 1549 | 10.89 | 0 | 241720 | {'circuit_open': 10, 'unavailable': 3} |
| 2026-10-28 | 1561 | 10.9 | 0 | 305924 | {'unavailable': 5} |
| 2026-10-29 | 1554 | 10.84 | 0 | 334332 | {'circuit_open': 10, 'unavailable': 4} |
| 2026-10-30 | 1608 | 10.73 | 0 | 333861 | {'circuit_open': 10, 'unavailable': 5} |

## Uncertainty

- One measured week may be unrepresentative (events, expiries, volatility).
- Provider throttling behaviour is observed, not documented; limits may change.
- Quote event times are unknown unless the provider supplies them.
