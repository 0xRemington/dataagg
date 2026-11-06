# 07 — Provider upgrade strategy

## Stable boundary

Providers own authentication, endpoint/stream protocol, source-symbol translation, field semantics, entitlement declarations, retries supported by the transport, and capability evidence. Core collectors own publication, identity resolution, quality rules, schedules, durable jobs and archive lineage. Research/API/UI consume canonical contracts from 02 and select sources by versioned policy; they do not import vendor SDKs.

Keep immutable source IDs (`yahoo`, `fake`, and a selected vendor+feed identity). Distinguish indicative feed, consolidated feed, exchange-specific feed and entitlement level. Never overwrite Yahoo rows with licensed values or silently treat fallback sources as equivalent. A new source may add capability/metadata and additive migrations, but must not require changing endpoint names, historical query semantics, or canonical research calculations.

## Capability negotiation

Each adapter publishes versioned `ProviderCapabilities`:

| Field | Meaning / Yahoo provisional value |
|---|---|
| `bars.intervals`, `max_request_span`, `oldest_history` | Verified per interval with dates/confidence; unknown until probe |
| `options.current_chain`, `historical_snapshots`, `streaming_quotes` | Current chains true after probe; other capabilities false for the selected Yahoo chain adapter |
| `quote_event_time`, `quote_sizes`, `sequence`, `replay` | False/unknown where absent; not inferred from last trade time |
| `delay_kind`, `advertised_delay_seconds`, `timestamp_precision` | Delayed/unknown actual age; provider evidence retained |
| `adjustment_basis`, `session_coverage`, `volume_semantics`, `oi_semantics` | Explicit verified or unknown |
| `contract_reference` | Style, deliverable, multiplier, cutoff confidence |
| `entitlements` | Reviewed feed/account scope, expiry of review, retention/display/export constraints |
| `request_limits`, `subscription_limits` | Provider-approved/measured limits, not invented global defaults |

Unsupported queries return an explicit capability error. A generic library WebSocket feature does not prove entitlement to a full timestamped options quote feed. The selected Yahoo adapter remains chain polling unless separately qualified.

## Vendor selection experiment

Evaluate at least two authorized candidates when needed. Score actual coverage for the configured symbols/expiries, consolidated vs venue-specific quotes, event time/sequence reliability, sizes/conditions, clock precision, underlying-feed compatibility, historical options depth, corporate actions, contract reference data, reconnect/replay facilities, subscription caps, retention/research rights, and full monthly cost. Record account-specific entitlement evidence; marketing “real time” is insufficient.

[Alpaca's option streaming documentation](https://docs.alpaca.markets/us/docs/real-time-option-data) is one concrete example: it differentiates subscription-dependent indicative/OPRA feeds and documents quote event times, sizes, conditions and MsgPack transport. This plan does not assert a particular account has OPRA access, wildcard capacity, historical completeness, or sufficient licensing. Recheck documentation and account terms at implementation. [OPRA's official site](https://www.opraplan.com/) supplies licensing and operational material for the exchange-feed review.

For a broker adapter, also verify session/login lifecycle, whether market data requires a funded account/subscription, concurrent connection limits and permissions for server-hosted research. Do not assume brokerage order permissions are needed: this system does not execute orders.

## Streaming data path

Extend the provider interface with `subscribe_quotes(SubscriptionRequest) -> AsyncIterator[QuoteEvent]` and optional `replay(ReplayRequest)`. Protocol types include provider event ID, exact timestamp precision, connection/session ID, sequence domain, receive UTC, receive monotonic clock delta, symbol, sides/sizes/conditions and correction/cancel semantics. Event ID deduplication must use the provider-documented sequence domain (possibly connection/channel/session), never a globally assumed sequence counter.

Receiver -> bounded spool -> normalization -> microbatch durable commit -> published events -> periodic checkpoints. Preserve every accepted changed event under `observation_kind=event`; represent deletions/quote withdrawals explicitly rather than carrying the last good quote forever. Use `provider_event_ns` when needed; PostgreSQL `timestamptz` alone cannot preserve nanoseconds. Store receive time separately. An optional in-memory/Redis latest cache is derived from published state and must be rebuildable.

Checkpoint snapshots at five-minute boundaries select only events durably known by the checkpoint decision time. Record original event/receive/knowledge times, checkpoint production time, sequence high-water marks, and whether fields are carried from an older event. Never replace event time with checkpoint time. Checkpoints become available when actually produced; causal recomputation can generate separate historical checkpoints from eligible raw events under its declared mode. These rules keep periodic surfaces comparable across providers while exposing their different source quality.

Bound buffers and microbatch size/time (initial 1,000 events or 100ms, whichever first, subject to pilot). If consumer lag exceeds capacity, spool to disk within hard limits, reduce subscriptions using explicit policy, or disconnect and mark a gap. Do not silently drop ticks and continue claiming completeness. Concurrent ingest requires the same fencing/idempotency/publication guarantees as polling.

## Reconnect, disorder, and integrity

Persist sequence high-water marks only with durable events. On reconnect, request supported replay from the last durable sequence; deduplicate repeats. If replay is unavailable, mark the missing interval unrecoverable, refresh authorized latest snapshots, and keep strict replay/fill queries ineligible across the gap. Distinguish live delivery from vendor historical catch-up: actual local knowledge time is the later arrival, not the original event time.

Validate clock bounds and source sequencing. Preserve out-of-order events with receipt metadata; latest-by-event views cannot make late messages historically available earlier. Provider cancels/corrections append linked events and update derived views with new knowledge times. Feed heartbeat success is not per-symbol quote freshness. Event-time and receive-time stale thresholds are separate. Entitlement loss stops subscriptions and raises an operator-visible incident; no automatic downgrade to an indicative feed without a changed source policy.

## Historical licensed backfill

Historical quote/tick imports retain vendor event time and actual import knowledge time, import job/version, delivery checksum, entitlement and correction policy. Offer an explicitly labeled historical-vendor research mode that uses documented availability assumptions; do not merge it into observed-as-collected history and pretend it was available locally earlier. Preserve expired/delisted contracts and point-in-time reference data. Match compatible underlying histories before execution-oriented research. Large historical imports use low-priority chunks and the same archive format/capacity controls.

## Cutover sequence and acceptance

1. Select vendor/feed and resolve rights; pin SDK/protocol version and capability manifest. Run all fake/adapter contract fixtures before connecting.
2. Map a small common universe to stable underlying and contract IDs, including standard/adjusted deliverables. Ambiguous cross-vendor mapping stays quarantined.
3. Shadow for at least five trading sessions: both sources retain distinct rows. Compare coverage, event/receive delay where available, spreads, IV definitions, basis, quote conditions, differences around open/close and expiry. Comparing prices alone does not prove either source correct.
4. Replay failure tests: disconnect, sequence gaps, burst, slow consumer, entitlement loss, duplicate/corrected events, nanosecond timestamps and process restart. Prove API schema unchanged and archived joins still resolve.
5. Change **future** source-policy version at an explicit effective time. New default research uses the qualified source; old datasets retain original source selection. The UI visibly shows source and any mixed historical coverage.
6. Rollback by switching future policy and disabling the new subscription; retain both datasets. A fallback to Yahoo is descriptive with unknown quote freshness, never a continuation of execution-grade status. Record the downgrade in alerts and all subsequent datasets.

Cutover gate: documented entitlements; supported replay/gap behavior; no unresolved mapping or precision loss; per-scope five-session targets met; bounded peak storage/CPU; checked backup/restore; all old API contract tests and A10 pass. Optional Redis or separate streaming compute is justified only by the measured event burst and latency needs.
