# 02 — Schemas and data contracts

## Migration contract

[001_reference.sql](schema/001_reference.sql) is concrete PostgreSQL DDL for the planned data model. It defines empty partitioned parents; implementation must provision partitions before inserts. Do not deploy it without the migration and integration tests in T03. UUIDs are application-generated; timestamps use aware UTC datetimes; prices/strikes are Decimal, never binary floating point. IV and calculated numerical outputs may be floating point but must be finite or null.

Migration sequence: `0001_reference_and_jobs` (providers, policies, identities, terms, memberships, jobs/runs); `0002_observations` (payloads, batches, actions, bars, snapshots, marks, gaps, quality); `0003_research_and_archives` (calculation runs, metrics, surfaces, archive/dataset manifests); `0004_roles_and_partitions` (roles, grants, current/next partitions and partition creation function). Sequence names are reserved for the persistence owner. Migrations must preserve data; downgrade of populated fact tables is refused outside a disposable environment.

The DDL intentionally uses one physical bar table with three views rather than three divergent schemas. All views expose historical revisions; none promises “latest”. Consumers must apply the selection rules below. Derived bars reference the original provider plus aggregation policy, not a pretend independent price feed. A registered internal provider may own derived publication batches.

## Entity contracts and invariants

| Relation | Identity / semantics | Write and validation rule |
|---|---|---|
| `providers`, `policy_versions` | Source capabilities/rights and immutable algorithm/config versions | Runtime source capabilities in a dataset are copied into its manifest; changed policies get new IDs |
| `instruments`, `instrument_identifiers` | Stable underlying plus bitemporal source symbols/status | No deletion on delisting; select effective symbol as known at decision time |
| `option_contracts`, `contract_identifiers`, `contract_terms` | Economic contract plus source IDs and versioned terms | Serialize mapping by provider+symbol; resolve effective and knowledge time; quarantine ambiguous aliases |
| `universe_memberships` | Selection and exclusion decisions including reasons | Append membership changes; point-in-time universe uses only known decisions |
| `collection_jobs`, `ingestion_runs` | One schedule intent; one row per attempt | Unique job key; monotonic fencing token; attempt results never silently reset |
| `payload_objects` | Content-addressed retained payload/result | Hash exact bytes; representation explicitly states whether original wire data were available |
| `observation_batches` | One accepted response or deterministic derived publication | Stable response identity on replay; staged then published after durable commit |
| `bar_observations` | Versioned bar, source, basis, session, policy | Append only changed versions; unchanged refetch audited in run/payload; lock logical bar key during revision insertion |
| `option_snapshots` | Contract row in one poll/event/checkpoint | Retain identical prices in distinct polls; reprocessing same response is idempotent |
| `underlying_marks` | Optional independently observed reference | Last-trade time does not become bid/ask event time; enforce compatible basis |
| `corporate_actions` | Effective action plus knowledge through batch | Corrections supersede, never mutate a historical action |
| `data_gaps`, `quality_issues` | Recoverability and diagnostic state | Mutable operational state with evidence/history in runs; raw facts remain immutable |
| `calculation_runs`, metrics, surfaces | Versioned algorithm + exact inputs + cutoff | Append calculations; idempotent by policy/asof/input hash/mode |
| `archive_manifest` | Immutable generation files plus atomic active catalog | Activate only verified files; old generations stay until all pinned readers expire |
| `dataset_manifests` | Reproducible query, inputs, policies, output hashes | Append only; no “latest source” dependency in a frozen dataset |

The reference DDL enforces local constraints. T03 must additionally enforce cross-row invariants in repositories and test them: nonoverlapping active provider aliases at a given knowledge cutoff; underlying/contract consistency; same-source batch relationships; observation time equals chain fetch completion; valid correction parents; idempotent parser revision; `known_at >= recorded_at`; complete derived bars have observed = expected; no source/basis mixing; a calculation never precedes input availability. Use advisory transaction locks for logical alias/bar keys and revoke direct application writes that bypass repositories. Parent references in JSON or archive-surviving logical references require an explicit resolver and integrity scan.

Store anomalous numeric quotes as observations with quality flags, including negative bids if the source returns them. Do not force these into valid prices via clamps. Reject malformed/non-finite representations into payload + quality issue rather than letting PostgreSQL NaN enter prices. Invalid bars fail OHLC invariants and stay in quarantine. Nullable fields distinguish missing from zero. Contract identity errors quarantine the affected row; they do not create a plausible guessed contract.

### JSON field schemas (versioned Pydantic types, `extra=forbid` for canonical data)

- `coverage`: requested expiries, returned expiries, expected sides, observed sides, rows before/after filters, exclusions by reason, discovery time, scope complete boolean, completeness confidence (`known/unknown`), empty reason, source schema fingerprint. An HTTP success never proves full chain completeness.
- `field_semantics`: `quote_time_basis=provider_event|unknown`, `volume_kind=session_cumulative|unknown`, `oi_kind=reported_total`, `volume_date_basis=source|inferred_session|unknown`, `oi_date_basis=source|unknown`, size units, IV units (`decimal_annualized`), source field mapping version. Unknown OI date remains null.
- Contract `deliverable`: schema version, units of each security, cash amount/currency, adjustment memo reference, verification status. Null does not mean 100 shares.
- `input_refs`: ordered `{dataset,id,partition_time,content_sha256}` records or a checksummed external lineage manifest. Never store thousands of IDs in a hot row when a lineage Parquet object suffices.
- Metric `values_json` v1: nullable midpoint, absolute/relative spread, reported IV; later modeled IV, delta, gamma, vega per one volatility percentage point, theta per calendar day, theoretical price, model name. Include units. Greek and IV origins are distinct.
- Surface `points`: input expiries, years-to-expiry, selected strikes/deltas, total variances, interpolation weights, output IV, model/curve IDs. `diagnostics`: coverage, max observation skew, filters, rejected count, extrapolation boolean, failure code, solver convergence.
- Calculation `assumptions`: rate/dividend/borrow input object references, contract-terms IDs, valuation clock convention, cost assumptions, source compatibility policy, and synthetic flag. Curves/dividend schedules live as immutable checksummed objects with `effective_at`, `published_at`, `known_at`, and source metadata; they use the same input cutoff rules as quotes.

## Timestamp and publication contract

| Field | Meaning |
|---|---|
| `scheduled_for` | Intended collection slot; never a quote timestamp |
| `fetch_started_at`, `fetch_completed_at` | Server UTC around provider operation |
| `observed_at` | Poll response completion; for streams, receive time; immutable |
| `provider_event_at` | Actual provider quote event timestamp, nullable |
| `provider_event_ns` | Optional exact epoch nanoseconds for a future feed; preserve precision beyond PostgreSQL microseconds |
| `last_trade_at` | Reported last trade only |
| `recorded_at` | Database insert time, not visibility/commit proof |
| `known_at` | Conservative durable ingestion acknowledgement time, never backdated to market time |
| `computed_at`, `published_at` | Computation completion and durable output acknowledgement |

Publication is two-stage: (1) persist payload/facts and staged batch in a short transaction; (2) after commit acknowledgement, capture UTC and finalize batch `known_at` in a separate transaction. Readers only use published batches. A crash between stages leaves invisible staged rows; the finalizer assigns its recovery-time acknowledgement, not the old insert time. This deliberately overestimates availability. Timestamp the acknowledgement after durable persistence; no test may use a precommit insert timestamp as knowledge. Clock drift larger than two seconds suspends publication and creates a health incident. This is the project's conservative ingestion-knowledge model, not a reconstruction of a specific UI client's network receipt time; live decision logs must separately record when that client received inputs.

For immutable facts corrected later, append a new revision with a new published batch. The original `observed_at` can remain the original observation label for a parser correction, while its new batch knowledge time records when the correction became known. New network requests always receive new response identities even if bodies match. Response identity is a persisted UUID assigned per actual response; payload checksum is never the identity of a poll.

## Point-in-time query contract

Every causal query specifies `decision_at D`, source policy, adjustment/session policy, and availability mode. Resolve identities, membership, terms, actions, and inputs using knowledge cutoffs as well as effective dates.

1. Filter batches to published with `known_at <= D`. Never select latest revisions first and then filter knowledge time.
2. For each logical bar key `(instrument,source,interval_start,timeframe,basis,session,policy)`, select the most recently known eligible revision, deterministic tie break by revision/ID. Require `interval_end <= D` and `complete=true` unless caller explicitly requests partial display.
3. For each option observation identity, choose the most recently known parser correction, then the latest eligible observation at or before D. If provider event time exists, it must also be `<= D`; future events are quarantined.
4. For a quote/spot panel, set spot join cutoff to `min(D, option_batch.known_at)`. Pick the most recent completed eligible spot bar within the **same session**, then enforce spot age. For event-comparable valuation, also require verified option event time and spot event alignment; unknown event times fail this stricter mode.
5. Return both component availability times and ages. A chart point sampled at a bar end may only use observations known by that decision time; a later quote must not be pulled backward into a bar.

SQL shape for selecting bars (bind all parameters; include source/basis/session/policy predicates):

```sql
WITH eligible AS (
  SELECT b.*, x.known_at,
         row_number() OVER (
           PARTITION BY b.instrument_id,b.provider_id,b.interval_start,
                        b.timeframe,b.price_basis,b.session_kind,b.policy_id
           ORDER BY x.known_at DESC,b.id DESC
         ) AS rn
  FROM bar_observations b
  JOIN observation_batches x ON x.id=b.batch_id
  WHERE x.status='published' AND x.known_at <= :decision_at
    AND b.interval_end <= :decision_at
    AND b.instrument_id=:instrument_id
    AND b.provider_id=:provider_id AND b.timeframe=:timeframe
    AND b.price_basis=:price_basis AND b.session_kind=:session_kind
    AND b.policy_id=:policy_id
)
SELECT * FROM eligible WHERE rn=1 AND complete;
```

The completeness filter follows revision selection: a newer incomplete/corrected-invalid observation must not silently revive an older accepted version. Quarantine invalidating an earlier record must publish a tombstone/quality revision that the resolver understands; retaining a raw quarantine payload alone is insufficient to invalidate a previously accepted price.

Invalidation representation v1 uses an appended revision with `quality_flags` containing `invalidated`, a `supersedes_id` pointing to the affected revision, and a newly published batch. A bar invalidation copies the prior syntactically valid OHLC fields solely to satisfy storage constraints and sets `complete=false`; these copied values are never eligible data. An option invalidation sets price fields null. Resolver selection follows the supersession chain first and rejects an invalidated result without falling back to its predecessor. A later explicit repair must supersede the invalidation. Exports retain invalidation lineage even when no priced row is returned. T03/T11 must test this representation end to end.

`availability_mode=observed` requires actual output publication by D, including derived bars and indicators. `recomputed` allows a new algorithm to recompute from raw inputs all known by D; its dataset is labeled “causal recomputation”, with a declared simulated computation lag and current code version. It cannot claim the feature existed in production then. Historical bars downloaded today are not eligible for an observed-mode decision last year. An explicitly separate `historical_vendor` dataset may model historical availability with assumptions, but cannot claim the observed guarantee.

### Default age and comparability rules

| Use | Option observation age ceiling | Underlying spot age ceiling | Additional rule |
|---|---:|---:|---|
| 1m/5m descriptive panel | 7 minutes | 3 minutes | Label five-minute option sampling; no 1m option candles invented |
| 15m descriptive panel | 7 minutes priority / 20 minutes wide | 3 minutes | Same session; selected tier included |
| 4h/daily descriptive panel | 20 minutes at session decision | 5 minutes for last 1m close | No overnight carry; daily provider bar only after actual arrival |
| Event-comparable live research | 5 seconds event age | 5 seconds event age, pair skew <=2 seconds | Licensed timestamped feed; not supported by Yahoo adapter |

Observation age is `D - fetch_completed_at`; it is not quote age. If provider event time is null, `quote_age_seconds=null`, freshness is `unknown`, execution eligibility is false, and any Yahoo option/spot-derived moneyness is marked temporally uncertain. Advertised 15-minute delay is metadata, not permission to assign event time = retrieval minus 15 minutes. No plot or model should disguise this distinction. All limits are versioned project defaults subject to pilot evidence.

## Provider and repository Python interfaces

```python
class MarketDataProvider(Protocol):
    def capabilities(self) -> ProviderCapabilities: ...
    def discover(self, request: DiscoveryRequest) -> DiscoveryResult: ...
    def fetch_bars(self, request: BarRequest) -> FetchResult[BarObservation]: ...
    def fetch_chain(self, request: ChainRequest) -> FetchResult[OptionObservation]: ...

class ResearchRepository(Protocol):
    def bars(self, query: BarQuery) -> Page[BarRecord]: ...
    def options(self, query: OptionQuery) -> Page[OptionRecord]: ...
    def panel(self, query: PanelQuery) -> PanelResult: ...
    def export(self, request: DatasetRequest) -> DatasetJob: ...
```

All types are frozen/versioned models in `domain/`. Requests include instrument/provider identity, half-open window or expiration, policy ID, deadline, and correlation ID. Results include response ID, fetch window, rows, payload reference/representation, source fingerprint, request counts, coverage, and errors. Adapters return typed failures: `RateLimited(retry_after)`, `Unauthorized`, `Unavailable`, `UnsupportedCapability`, `SchemaChanged`, `PartialResult`. Missing chain sides are partial results, not empty success. Fake provider has injectable clock, deterministic seeds, latency, malformed payload, retry, and correction scripts.

## HTTP API v1

| Route | Contract |
|---|---|
| `GET /health/live` | Process alive; no secrets, no expensive dependencies |
| `GET /health/ready` | DB reachable, compatible migration, required partitions; collection status separately reported |
| `GET /v1/instruments` | IDs, display symbols, effective/known identity and coverage |
| `GET /v1/contracts` | `instrument_id`, expiration/type/strike filters, `asof`; cursor pagination |
| `GET /v1/bars` | instrument, timeframe, start/end, provider/policy/basis, decision cutoff/mode, optional partials |
| `GET /v1/options/history` | contract, start/end, provider, cutoff/mode; raw observations, no implied candle data |
| `GET /v1/panel` | instrument, optional contract, timeframe, window, sampling policy, source policy, cutoff/mode |
| `GET /v1/surfaces` | instrument, asof, metric/method, target DTE/delta; null with diagnostics if unsupported |
| `GET /v1/quality` | Coverage, unresolved gaps, schema incidents, latest scope success |
| `POST /v1/datasets` | Authenticated bounded async export; returns 202 + job ID |
| `GET /v1/datasets/{id}` | State, manifest, authorized download URL; never filesystem paths supplied by client |

All data responses include `schema_version`, `request_id`, source/policy IDs, effective coverage, availability semantics, and warnings. Decimal JSON fields are strings; timestamps are ISO 8601 UTC `Z`; nulls never become zero. Cursor order is stable `(timestamp,id)` with an immutable cutoff embedded in the cursor. Limits: 10,000 rows/page, default 31-day intraday request, 5-year daily request, and asynchronous export above 100,000 rows. Configurable hard byte/time limits protect the VPS. Errors use `{code,message,details,request_id}`: 401/403 auth, 409 incompatible basis/policy, 422 invalid/unsupported query, 429 local capacity, 503 dependency unavailable. Empty valid coverage returns 200 with gaps, not fabricated values.

Panel sample: underlying candle at 13:30 has end 13:45 and known time 13:46:05; a decision at 13:45 cannot use it. An option fetched at 13:44:58 but committed/acknowledged at 13:45:03 is likewise unavailable at 13:45. API tests must exercise these precise distinctions.
