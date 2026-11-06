# 03 — Collection, scheduling, and recovery

## Authoritative schedule

The scheduler runs every five seconds using UTC and the versioned exchange calendar. It holds a dedicated PostgreSQL session advisory lock; lock loss stops planning immediately. It inserts durable job intents with a unique deterministic `job_key = hash(kind,provider,scope,scheduled_for,policy_version)`. All competing processes see the same queue. A restart may regenerate intents safely but may not recreate the past as live data.

| Job | Default schedule | Scope / deadline |
|---|---|---|
| Expiry/contract discovery | 09:15 ET and after unexpected expiry errors | Two symbols; refresh TTL one session; retain full discovery metadata |
| Underlying rolling 1m | Every minute at second 20, from open+2m through close+5m | Each symbol; request trailing 20m; only closed bars; safe-lag target 80s after a minute start |
| Priority chain | 78 five-minute slots starting 09:35 through 16:00 on normal day | Up to four distinct expiries/symbol in 30–90 DTE; distribute starts, finish within 120s of slot |
| Other available expiries | At 09:35 then hourly through 15:35 | Bounded discovered universe; finish within 10m; omit expired/unsupported |
| Broad daily chain | 15:55 ET or five minutes before early close | All configured available expiries; reuse response only when actual fetch window meets same five-minute slot; report per-expiry times |
| Daily source refresh | Close+30m, then next session preopen | Last 10 sessions; do not assume a final daily bar at the closing bell |
| 1m correction sweep | Close+10m and next preopen | Last two sessions within observed provider horizon |
| Gap scan | Every 15m and after correction sweep | Past due minutes/slots only, calendar aware |
| Derived bars | After new/changed 1m publication; coalesce dirty intervals | Deadline two minutes after eligible source availability |
| QC / archives / backup | Off-hours; bounded jobs | Archive after grace period, backup independently; see document 06 |

Close and early-close schedules derive from the calendar, not strings in cron. The final 16:00 chain slot is a **collection label**, not a guaranteed closing quote. Mark its source/session uncertainty. Default observation panels accept only same-session RTH data; a product trading later can be added under a new session policy. Weekend/holiday schedules do not create missing-minute alerts. Warm-up discovery and after-close recovery have separate budgets.

## Request accounting and honest strike tiers

For S symbols, priority expiry counts `E_p(s)`, other expiry counts `E_o(s)`, priority polls P, other polls H:

```text
chain_endpoint_calls = sum_s(E_p(s)*P + E_o(s)*H)
daily_extra_calls = broad expiries not already fetched in the broad slot
total_network_calls = chain + broad_extra + discovery + bars + daily + repair
                      + measured cookie/auth/internal retry overhead
```

Each expiry response includes both calls and puts when available. Do not double-count sides; do count cold-client discovery/authentication overhead. Cache one Ticker/client per symbol in the worker, but never rely on that cache for durable correctness. Reconstruct safely after restart. An in-flight request may satisfy several collection scopes only for its **actual** observation slot, with all scopes recorded in coverage.

Worked budget (assumption, not a Yahoo allowance): 2 symbols, 4 priority expiries each, 6 other expiries each, 78 priority slots, 7 hourly slots, and 390 one-minute polls per symbol. Calls = `624 + 84 + 780 + 2 discovery + 4 daily + 16 repair + <=12 broad extras = <=1,522 endpoint calls/session`. Rough mean is 3.9/minute over 390 minutes, excluding hidden transport overhead and tails. At a five-minute boundary, 8 priority chain requests plus 2 bar requests consume 10 tokens; the scheduler must spread them across at least a minute at the proposed cap. A mean below a cap does not prove peak feasibility.

Project budget starts at **10 network requests/minute**, burst one, concurrency one, and a 70% planned utilization ceiling. This is self-imposed pacing, **not a documented safe/provider limit**. Instrument actual transport where supported by the pinned package; otherwise treat the endpoint estimate as a lower bound, reserve additional headroom, and report `transport_requests=null`. Pin and inspect library retry settings. Never claim a strict network cap if hidden calls cannot be counted. Reduce scope/cadence or switch provider if pilot behavior is unstable. Stop on access denial.

Retention policy v1 stores all contracts returned by a selected priority expiry every five minutes. This deliberately exceeds the minimum wide-strike storage frequency and preserves comparability. Optional v2 tiering stores near strikes every five minutes, wide strikes at 15-minute slots, and all strikes in daily sweeps; it saves storage only, not expiry fetch requests. Its near-strike definition is `abs(log(strike / eligible_spot)) <= 0.10`, using spot known before fetch planning. If spot is missing, retain the full returned expiry and flag filter fallback. Never select contracts using a later spot or future delta. Store omitted contract counts and membership decisions so research knows what was sampled.

If an instrument has 15 priority expiries, do not imply all 15 are collected by the four-expiry pilot policy. Select distinct expiries nearest 30/45/60/90 calendar DTE, ties to earlier expiration, only within [30,90]. List all omitted expiries in discovery/coverage. Hourly other-expiry jobs and broad sweeps have independent request ceilings; if those ceilings are exceeded, emit partial scope and a visible skip reason. Expansion requires a recalculated budget.

## Claims, leases, and publication

1. Claim ready jobs with `SELECT ... FOR UPDATE SKIP LOCKED` in a short transaction. Set status running, lease owner, expiry, and increment fencing token. Create attempt row atomically. Initial lease 180s, heartbeat every 20s; each provider operation has a 30s timeout and a job deadline.
2. Acquire provider-wide pacing token. The one-worker default owns the token clock; if worker count increases, persist/serialize token state in PostgreSQL under a provider advisory lock. Independent in-memory token buckets are forbidden with multiple request workers.
3. Fetch outside DB transaction; persist response UUID, fetch times, and immutable payload to a private atomic-spool file before parsing. If library exposes only processed data, store that as `adapter_result`, not `provider_wire`. Hash bodies before redacting only if they contain no secrets; never persist cookies/authorization headers. If stripping secrets is required, label the representation and hash the retained sanitized bytes.
4. Normalize and validate; stage valid rows and batch, log rejected rows. Within the write transaction, lock the job row and verify fencing token/lease still valid; reject stale worker writes. Commit facts, then finalize `known_at` after acknowledgement. A stale worker's payload can remain in quarantine but cannot complete the job.
5. Record counts, publish coverage, dirty derived windows, and mark job succeeded or partial. A durable response replay has the same response identity. A new HTTP response after an uncertain crash has a new response identity and remains a distinct observation.
6. Job success and batch publication must be recoverable independently: startup scans staged batches, unfinished attempts, expired leases, and unreferenced spool files. Verify checksums and fences before replay. It is acceptable to conservatively label a recovered batch known at recovery time.

Idempotency levels: job intent uniqueness prevents duplicate scheduling; response identity prevents duplicate persistence of the same response; row identity prevents duplicate contracts inside a response; bar revision hashing suppresses unchanged refetches. None of these collapses distinct polls merely because prices are unchanged. A parser correction has a new publication batch and revision linked to the old row; do not mutate its original ingestion times.

## Retry, degradation, and failed polls

Retry transient transport/5xx failures with full-jitter exponential backoff: uniform `[0, min(60s, 2s*2^attempt)]`, at most three total attempts, constrained by deadline and token budget. Honor a larger provider Retry-After. A 429 opens a 15-minute circuit initially, with probe-based reopening and longer cooldown on repeat; this policy is independent of three-attempt transport retries. A 401/403/access denial disables the provider until configuration/permission is resolved. Schema drift opens the affected endpoint circuit and preserves sanitized evidence; no aggressive retry.

Expired live chain jobs become skipped and create unrecoverable missing-snapshot gaps. Fetching the current chain later cannot repair an old snapshot. Retrying a 09:35 slot at 09:37 produces a 09:37 observation, not a 09:35 quote. On restart, resume the next live slot rather than flooding overdue chain requests. Historical bar jobs resume at durable chunk cursors. Bound queues; shed broad/hourly scopes before high-priority underlying and option jobs, and record every omission.

Partial chain behavior: valid rows may publish with partial scope. Missing puts/calls, severe row-count collapse relative to recent history, missing required identity fields, or malformed prices create quality incidents. A legitimately empty expired chain differs from an unexpectedly empty current chain. Re-discover once within budget; never erase previously discovered contracts due to one empty result. Unknown completeness stays unknown even if row counts look normal.

## Historical bootstrap and correction handling

1. Seed stable underlying IDs and explicit symbol/calendar policies. Record universe start and exclusions.
2. Retrieve daily history in bounded annual chunks for up to five years, with small overlap and inclusive-start/exclusive-end semantics. Persist arrival knowledge today. Record actual earliest/latest rows and missing intervals. Older requested data are not assumed available.
3. Fetch recent 1m in short windows, initially up to seven days total, and shrink request spans on legitimate provider range errors. The maximum age and maximum request span are separate capability fields. Unsupported old minutes become `unrecoverable`; coarser bars can be retained as separately labeled fallback history, never upsampled into canonical minutes.
4. Start rolling collection immediately after basic identity setup; daily backfill is lower priority so it cannot consume live headroom. Discover expiries and begin chain history. There is no Yahoo historical options backfill step.
5. On overlap fetch, compare canonical content per logical bar key; append changed revisions, mark old supersession, and enqueue affected 5m/15m/4h buckets. For daily changes, version daily reconciliation and action-linked returns. Recompute dependent metrics into new calculation runs. Old exported datasets remain pinned to their original inputs.
6. Corrections after archive publication go into a new generation or immutable delta overlay, with catalog activation after verification. They never overwrite archived files in place. Hot/cold query deduplicates by record identity/revision, not by arbitrary row order.

Underlying reconciliation compares provider daily to locally aggregated RTH only when basis and session semantics match. Initial price tolerance: `max(0.01 currency units, 1e-5 * daily_close)`; initial volume difference warning 1%, never an automatic overwrite. Differences can reflect auctions, late trades, adjustment policies, or coverage; classify them with evidence. These are project QC thresholds, not exchange correctness claims.

## Quality and gaps

Minute gap detection builds the expected calendar index after publication grace (three minutes initially), compares available distinct interval starts, and excludes future/unclosed minutes. Halts require authoritative evidence; missing quotes do not prove a halt. Gap records include observed evidence, retry cursor, source horizon, recoverability deadline, and affected downstream buckets. Repair earliest expiring recoverable history first. Three failed repair cycles escalate; crossing the source horizon marks unrecoverable with explanation.

Option flags include `missing_bid`, `missing_ask`, `zero_bid`, `crossed`, `locked`, `negative_price`, `unknown_quote_time`, `old_last_trade`, `invalid_iv`, `unknown_oi_date`, `partial_scope`, `ambiguous_contract`, and `basis_unverified`. A stale last-trade flag applies to the trade field only. Descriptive midpoint requires finite `0 <= bid <= ask` and ask > 0; zero-bid and locked markets remain separately flagged and excluded from default modeled/fill-eligible sets. Relative spread uses midpoint denominator only when positive. OI/volume missing, zero, daily cumulative, or unknown timing are distinct; do not sum repeated OI or cumulative volumes across polls.

Maintain `last_success` by `(provider,symbol,expiry,scope)`, not merely one global successful request. Report poll completion skew across expiries. Surfaces reject a combined cycle with >120s observation skew by default even though each individual expiry may have succeeded. This limit bounds collection skew, not unknown source-event skew.
