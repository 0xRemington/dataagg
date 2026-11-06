"""Ingestion and publication repository (T03).

Two-stage publication (02): ``stage_*`` persists payload references, a staged
batch and its facts in one short transaction (verifying the job fence inside it);
``publish`` runs afterwards in a separate transaction and stamps ``known_at`` from
the clock *after* the staging commit returned. Readers only see published
batches. ``finalize_staged`` publishes batches left staged by a crash at recovery
time, which deliberately overstates (never understates) availability latency.

Idempotency levels (03): response identity -> one batch per response; contract
row identity inside a response; bar content hashing suppresses unchanged
refetches. Distinct polls are never collapsed because prices are equal.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import Connection, Engine, text
from sqlalchemy.exc import DBAPIError

from marketagg.clock import SYSTEM_CLOCK, Clock, is_system_clock
from marketagg.db.errors import (
    ClockSkewError,
    IdentityConflict,
    InvariantViolation,
    PartitionMissing,
    StaleLease,
)
from marketagg.domain.capabilities import ProviderCapabilities
from marketagg.domain.enums import (
    BatchKind,
    QualityFlag,
    Timeframe,
)
from marketagg.domain.identifiers import ContractRef, occ_mismatches
from marketagg.domain.observations import (
    BarObservation,
    CorporateActionObservation,
    Coverage,
    DiscoveryResult,
    FetchResult,
    OptionObservation,
    PayloadRef,
    RejectedRow,
)
from marketagg.domain.policies import PolicyVersion
from marketagg.domain.primitives import canonical_dumps, format_decimal

MAX_CLOCK_SKEW = timedelta(seconds=2)


@dataclass(frozen=True)
class Fence:
    job_id: UUID
    lease_token: int


@dataclass(frozen=True)
class StageOutcome:
    batch_id: UUID
    created: bool
    rows_written: int = 0
    rows_unchanged: int = 0
    rows_quarantined: int = 0
    written_ids: tuple[UUID, ...] = ()


@dataclass(frozen=True)
class BarWrite:
    """A bar plus the collector's completeness decision and lineage."""

    observation: BarObservation
    complete: bool
    expected_minutes: int | None = None
    observed_minutes: int | None = None
    is_short_session_bucket: bool = False
    input_refs: tuple[Mapping[str, Any], ...] = ()
    extra_flags: tuple[QualityFlag, ...] = ()

    def content_sha256(self) -> str:
        obs = self.observation.model_dump(mode="json", exclude={"instrument"})
        payload = {
            "bar": obs,
            "complete": self.complete,
            "expected_minutes": self.expected_minutes,
            "observed_minutes": self.observed_minutes,
            "is_short_session_bucket": self.is_short_session_bucket,
            "flags": sorted({*obs["quality_flags"], *self.extra_flags}),
            "input_refs": list(self.input_refs),
        }
        return hashlib.sha256(canonical_dumps(payload).encode()).hexdigest()


@dataclass(frozen=True)
class BatchMeta:
    provider_id: str
    response_identity: str
    batch_kind: BatchKind
    fetch_started_at: datetime
    fetch_completed_at: datetime
    scope: Mapping[str, Any]
    coverage: Mapping[str, Any]
    run_id: UUID | None = None
    payload_id: UUID | None = None
    advertised_delay_seconds: int | None = None


@dataclass
class _Counts:
    written: int = 0
    unchanged: int = 0
    quarantined: int = 0
    ids: list[UUID] = field(default_factory=list)


def _json(value: Any) -> str:
    return canonical_dumps(value)


def _privileged_write(conn: Connection, what: str, statement: Any,
                      params: Mapping[str, Any]) -> None:
    """Reference writes belong to the owner's bootstrap step, not least-privilege roles."""
    try:
        conn.execute(statement, params)
    except DBAPIError as exc:
        if getattr(exc.orig, "sqlstate", None) == "42501":  # insufficient_privilege
            raise InvariantViolation(
                f"{what} is missing or out of date and this role may not write it; "
                "run `marketagg migrate --bootstrap` with the owner role") from exc
        raise


def _lock(conn: Connection, key: str) -> None:
    conn.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:k, 0))"), {"k": key})


def _translate(exc: DBAPIError) -> Exception:
    message = str(exc.orig) if exc.orig is not None else str(exc)
    if "no partition of relation" in message:
        return PartitionMissing(message.splitlines()[0])
    return exc


class IngestRepository:
    def __init__(
        self,
        engine: Engine,
        clock: Clock = SYSTEM_CLOCK,
        max_clock_skew: timedelta = MAX_CLOCK_SKEW,
    ) -> None:
        self.engine = engine
        self.clock = clock
        self.max_clock_skew = max_clock_skew

    # -- Reference data ---------------------------------------------------------------

    def register_provider(
        self, capabilities: ProviderCapabilities, terms_review: Mapping[str, Any] | None = None
    ) -> None:
        caps, terms = capabilities.canonical_json(), _json(terms_review or {})
        with self.engine.begin() as conn:
            stored = conn.execute(text(
                "SELECT reliability_tier, capabilities, terms_review FROM providers WHERE id=:id"),
                {"id": capabilities.provider_id}).one_or_none()
            if stored is not None and tuple(stored) == (
                    capabilities.reliability_tier, json.loads(caps), json.loads(terms)):
                return  # read-only service roles need no write when nothing changed
            _privileged_write(conn, "provider registration", text("""
                    INSERT INTO providers (id, reliability_tier, capabilities, terms_review,
                                           created_at)
                    VALUES (:id, :tier, CAST(:caps AS jsonb), CAST(:terms AS jsonb), :now)
                    ON CONFLICT (id) DO UPDATE SET
                        reliability_tier = EXCLUDED.reliability_tier,
                        capabilities = EXCLUDED.capabilities,
                        terms_review = EXCLUDED.terms_review
                """),
                {"id": capabilities.provider_id, "tier": capabilities.reliability_tier,
                 "caps": caps, "terms": terms, "now": self.clock.now()})

    def register_policy(self, policy: PolicyVersion) -> None:
        with self.engine.begin() as conn:
            stored = conn.execute(
                text("SELECT content_sha256 FROM policy_versions WHERE id = :id"),
                {"id": policy.id},
            ).scalar_one_or_none()
            if stored is None:
                _privileged_write(conn, "policy registration", text("""
                    INSERT INTO policy_versions (id, kind, config, code_sha, content_sha256,
                                                 created_at)
                    VALUES (:id, :kind, CAST(:config AS jsonb), :code, :sha, :now)
                    ON CONFLICT (id) DO NOTHING
                """), {"id": policy.id, "kind": policy.kind, "config": _json(policy.config),
                       "code": policy.code_sha, "sha": policy.content_sha256,
                       "now": self.clock.now()})
                stored = conn.execute(
                    text("SELECT content_sha256 FROM policy_versions WHERE id = :id"),
                    {"id": policy.id},
                ).scalar_one()
        if stored != policy.content_sha256:
            raise InvariantViolation(f"policy {policy.id} already exists with different content")

    def register_instrument(
        self,
        *,
        provider_id: str,
        symbol: str,
        asset_class: str,
        valid_from: date,
        currency: str = "USD",
        calendar_id: str = "XNYS",
        exchange_mic: str | None = None,
        evidence: Mapping[str, Any] | None = None,
        exchange_timezone: str = "America/New_York",
    ) -> UUID:
        """Stable underlying + active source symbol. Re-registering returns the same ID."""
        with self.engine.begin() as conn:
            _lock(conn, f"instrument:{provider_id}:{symbol}")
            existing = conn.execute(
                text("""
                    SELECT i.instrument_id FROM instrument_identifiers i
                    WHERE i.provider_id = :p AND i.symbol = :s AND i.valid_to IS NULL
                      AND i.status = 'active'
                      AND NOT EXISTS (SELECT 1 FROM instrument_identifiers n
                                      WHERE n.supersedes_id = i.id)
                """),
                {"p": provider_id, "s": symbol},
            ).scalars().all()
            if len(set(existing)) > 1:
                raise IdentityConflict(f"{provider_id}:{symbol} maps to several instruments")
            if existing:
                return UUID(str(existing[0]))
            now = self.clock.now()
            instrument_id = uuid4()
            conn.execute(
                text("""
                    INSERT INTO instruments (id, asset_class, currency, calendar_id,
                                             exchange_timezone, created_at)
                    VALUES (:id, :ac, :cur, :cal, :tz, :now)
                """),
                {"id": instrument_id, "ac": asset_class, "cur": currency, "cal": calendar_id,
                 "tz": exchange_timezone, "now": now},
            )
            conn.execute(
                text("""
                    INSERT INTO instrument_identifiers (id, instrument_id, provider_id, symbol,
                        exchange_mic, valid_from, known_at, status, evidence)
                    VALUES (:id, :iid, :p, :s, :mic, :vf, :now, 'active', CAST(:ev AS jsonb))
                """),
                {"id": uuid4(), "iid": instrument_id, "p": provider_id, "s": symbol,
                 "mic": exchange_mic, "vf": valid_from, "now": now,
                 "ev": _json(evidence or {})},
            )
            return instrument_id

    def supersede_instrument_identifier(
        self,
        *,
        instrument_id: UUID,
        provider_id: str,
        symbol: str,
        valid_to: date | None,
        status: str,
        evidence: Mapping[str, Any],
        new_symbol: str | None = None,
        new_valid_from: date | None = None,
    ) -> None:
        """Close a symbol mapping (delisting/symbol change) by appending, never updating."""
        with self.engine.begin() as conn:
            _lock(conn, f"instrument:{provider_id}:{symbol}")
            current = conn.execute(
                text("""
                    SELECT id, valid_from, exchange_mic FROM instrument_identifiers i
                    WHERE instrument_id = :iid AND provider_id = :p AND symbol = :s
                      AND NOT EXISTS (SELECT 1 FROM instrument_identifiers n
                                      WHERE n.supersedes_id = i.id)
                """),
                {"iid": instrument_id, "p": provider_id, "s": symbol},
            ).one_or_none()
            if current is None:
                raise IdentityConflict(f"no current identifier {provider_id}:{symbol}")
            now = self.clock.now()
            conn.execute(
                text("""
                    INSERT INTO instrument_identifiers (id, instrument_id, provider_id, symbol,
                        exchange_mic, valid_from, valid_to, known_at, status, evidence,
                        supersedes_id)
                    VALUES (:id, :iid, :p, :s, :mic, :vf, :vt, :now, :st, CAST(:ev AS jsonb),
                            :sup)
                """),
                {"id": uuid4(), "iid": instrument_id, "p": provider_id, "s": symbol,
                 "mic": current.exchange_mic, "vf": current.valid_from, "vt": valid_to,
                 "now": now, "st": status, "ev": _json(evidence), "sup": current.id},
            )
            if new_symbol is not None:
                if new_valid_from is None:
                    raise ValueError("new_valid_from is required with new_symbol")
                conn.execute(
                    text("""
                        INSERT INTO instrument_identifiers (id, instrument_id, provider_id,
                            symbol, exchange_mic, valid_from, known_at, status, evidence)
                        VALUES (:id, :iid, :p, :s, :mic, :vf, :now, 'active',
                                CAST(:ev AS jsonb))
                    """),
                    {"id": uuid4(), "iid": instrument_id, "p": provider_id, "s": new_symbol,
                     "mic": current.exchange_mic, "vf": new_valid_from, "now": now,
                     "ev": _json(evidence)},
                )

    def resolve_instrument(
        self, provider_id: str, symbol: str, *, known_by: datetime, effective: date
    ) -> UUID | None:
        """Instrument for a symbol as known at ``known_by`` and effective on a date."""
        with self.engine.connect() as conn:
            return resolve_instrument(conn, provider_id, symbol, known_by=known_by,
                                      effective=effective)

    # -- Staging ---------------------------------------------------------------------

    def stage_chain(
        self,
        result: FetchResult[OptionObservation],
        *,
        instrument_id: UUID,
        scope: Mapping[str, Any],
        payload_uri: str | None = None,
        fence: Fence | None = None,
        run_id: UUID | None = None,
        rows: Sequence[OptionObservation] | None = None,
    ) -> StageOutcome:
        """Stage one chain response. ``rows`` may carry collector quality flags."""
        observations = tuple(rows) if rows is not None else result.rows
        meta = BatchMeta(
            provider_id=result.provider_id,
            response_identity=str(result.response_id),
            batch_kind=BatchKind.CHAIN,
            fetch_started_at=result.fetch_started_at,
            fetch_completed_at=result.fetch_completed_at,
            scope=scope,
            coverage=_coverage_json(result.coverage),
            run_id=run_id,
            advertised_delay_seconds=result.advertised_delay_seconds,
        )
        try:
            with self.engine.begin() as conn:
                self._check_fence(conn, fence)
                meta = _with_payload(meta, self._payload_id(conn, result, payload_uri))
                batch_id, created = self._insert_batch(conn, meta)
                if not created:
                    return StageOutcome(batch_id, created=False)
                symbol = _active_symbol(conn, instrument_id, result.provider_id)
                counts = _Counts()
                seen: set[str] = set()
                for row in observations:
                    if row.contract.provider_symbol in seen:
                        self._quality(conn, batch_id, {"contract": row.contract.provider_symbol},
                                      "duplicate_contract_row", "warning", {})
                        counts.quarantined += 1
                        continue
                    seen.add(row.contract.provider_symbol)
                    if row.contract.underlying_symbol != symbol:
                        self._quality(conn, batch_id, {"contract": row.contract.provider_symbol},
                                      "wrong_underlying", "error",
                                      {"expected": symbol,
                                       "got": row.contract.underlying_symbol})
                        counts.quarantined += 1
                        continue
                    contract_id = self._resolve_contract(conn, row.contract, instrument_id,
                                                         batch_id)
                    if contract_id is None:
                        counts.quarantined += 1
                        continue
                    counts.ids.append(self._insert_snapshot(conn, batch_id, contract_id, row))
                    counts.written += 1
                self._rejected(conn, batch_id, result.rejected)
                return StageOutcome(batch_id, True, counts.written, 0, counts.quarantined,
                                    tuple(counts.ids))
        except DBAPIError as exc:
            raise _translate(exc) from exc

    def stage_bars(
        self,
        result: FetchResult[BarObservation],
        *,
        instrument_id: UUID,
        policy_id: str,
        scope: Mapping[str, Any],
        writes: Sequence[BarWrite] | None = None,
        payload_uri: str | None = None,
        fence: Fence | None = None,
        run_id: UUID | None = None,
    ) -> StageOutcome:
        """Stage provider bars, appending only changed revisions per logical bar key."""
        bar_writes = list(writes) if writes is not None else [
            _default_bar_write(b) for b in result.rows
        ]
        meta = BatchMeta(
            provider_id=result.provider_id,
            response_identity=str(result.response_id),
            batch_kind=BatchKind.BARS,
            fetch_started_at=result.fetch_started_at,
            fetch_completed_at=result.fetch_completed_at,
            scope=scope,
            coverage=_coverage_json(result.coverage),
            run_id=run_id,
            advertised_delay_seconds=result.advertised_delay_seconds,
        )
        try:
            with self.engine.begin() as conn:
                self._check_fence(conn, fence)
                meta = _with_payload(meta, self._payload_id(conn, result, payload_uri))
                batch_id, created = self._insert_batch(conn, meta)
                if not created:
                    return StageOutcome(batch_id, created=False)
                symbol = _active_symbol(conn, instrument_id, result.provider_id)
                for write in bar_writes:
                    if write.observation.instrument.symbol != symbol:
                        raise InvariantViolation(
                            f"bar symbol {write.observation.instrument.symbol} is not the "
                            f"active symbol {symbol} of instrument {instrument_id}")
                counts = self._insert_bars(conn, batch_id, instrument_id, result.provider_id,
                                           policy_id, bar_writes)
                self._rejected(conn, batch_id, result.rejected)
                return StageOutcome(batch_id, True, counts.written, counts.unchanged, 0,
                                    tuple(counts.ids))
        except DBAPIError as exc:
            raise _translate(exc) from exc

    def stage_derived_bars(
        self,
        *,
        provider_id: str,
        instrument_id: UUID,
        policy_id: str,
        response_identity: str,
        computed_from: datetime,
        computed_at: datetime,
        scope: Mapping[str, Any],
        writes: Sequence[BarWrite],
        fence: Fence | None = None,
    ) -> StageOutcome:
        """Deterministic derived publication (T09). The batch window is the computation."""
        meta = BatchMeta(
            provider_id=provider_id,
            response_identity=response_identity,
            batch_kind=BatchKind.DERIVED,
            fetch_started_at=computed_from,
            fetch_completed_at=computed_at,
            scope=scope,
            coverage={"derived": True, "buckets": len(writes)},
        )
        try:
            with self.engine.begin() as conn:
                self._check_fence(conn, fence)
                batch_id, created = self._insert_batch(conn, meta)
                if not created:
                    return StageOutcome(batch_id, created=False)
                counts = self._insert_bars(conn, batch_id, instrument_id, provider_id,
                                           policy_id, writes)
                return StageOutcome(batch_id, True, counts.written, counts.unchanged, 0,
                                    tuple(counts.ids))
        except DBAPIError as exc:
            raise _translate(exc) from exc

    def stage_chain_correction(
        self,
        original_batch_id: UUID,
        corrected: FetchResult[OptionObservation],
        *,
        instrument_id: UUID,
        payload_uri: str | None = None,
        fence: Fence | None = None,
    ) -> StageOutcome:
        """Parser correction: new batch, new revisions linked to the originals.

        Original ``observed_at`` values are kept; the new batch's ``known_at`` records
        when the correction became known. Replaying the same correction is a no-op.
        """
        if corrected.payload is None:
            raise InvariantViolation("a parser correction needs its payload reference")
        try:
            with self.engine.begin() as conn:
                self._check_fence(conn, fence)
                original = conn.execute(
                    text("""
                        SELECT provider_id, response_identity, fetch_started_at,
                               fetch_completed_at, scope
                        FROM observation_batches WHERE id = :id
                    """),
                    {"id": original_batch_id},
                ).one()
                if (original.response_identity != str(corrected.response_id)
                        or original.fetch_completed_at != corrected.fetch_completed_at
                        or original.provider_id != corrected.provider_id):
                    raise InvariantViolation("correction does not match the original response")
                meta = BatchMeta(
                    provider_id=corrected.provider_id,
                    response_identity=(f"{corrected.response_id}"
                                       f"#parser={corrected.payload.parser_version}"),
                    batch_kind=BatchKind.CHAIN,
                    fetch_started_at=corrected.fetch_started_at,
                    fetch_completed_at=corrected.fetch_completed_at,
                    scope={**original.scope, "correction_of": str(original_batch_id)},
                    coverage=_coverage_json(corrected.coverage),
                )
                meta = _with_payload(meta, self._payload_id(conn, corrected, payload_uri))
                batch_id, created = self._insert_batch(conn, meta)
                if not created:
                    return StageOutcome(batch_id, created=False)
                counts = _Counts()
                for row in corrected.rows:
                    contract_id = self._resolve_contract(conn, row.contract, instrument_id,
                                                         batch_id)
                    if contract_id is None:
                        counts.quarantined += 1
                        continue
                    prior = conn.execute(
                        text("""
                            SELECT s.id, s.record_revision, s.content_sha256
                            FROM option_snapshots s
                            JOIN observation_batches b ON b.id = s.batch_id
                            WHERE s.contract_id = :c AND s.observed_at = :obs
                              AND b.provider_id = :p
                              AND (b.response_identity = :rid
                                   OR b.response_identity LIKE :rid || '#%')
                            ORDER BY s.record_revision DESC LIMIT 1
                        """),
                        {"c": contract_id, "obs": row.observed_at, "p": corrected.provider_id,
                         "rid": str(corrected.response_id)},
                    ).one_or_none()
                    if prior is None:
                        raise InvariantViolation(
                            f"correction row {row.contract.provider_symbol} has no original")
                    if prior.content_sha256 == row.canonical_sha256():
                        counts.unchanged += 1
                        continue
                    counts.ids.append(self._insert_snapshot(
                        conn, batch_id, contract_id, row,
                        revision=prior.record_revision + 1,
                        supersedes=(prior.id, row.observed_at)))
                    counts.written += 1
                return StageOutcome(batch_id, True, counts.written, counts.unchanged,
                                    counts.quarantined, tuple(counts.ids))
        except DBAPIError as exc:
            raise _translate(exc) from exc

    def stage_actions(
        self,
        result: FetchResult[CorporateActionObservation],
        *,
        instrument_id: UUID,
        scope: Mapping[str, Any],
        payload_uri: str | None = None,
        fence: Fence | None = None,
        run_id: UUID | None = None,
    ) -> StageOutcome:
        """Append new/changed actions; a changed action supersedes, never mutates (02)."""
        meta = BatchMeta(
            provider_id=result.provider_id, response_identity=str(result.response_id),
            batch_kind=BatchKind.ACTION, fetch_started_at=result.fetch_started_at,
            fetch_completed_at=result.fetch_completed_at, scope=scope,
            coverage=_coverage_json(result.coverage), run_id=run_id,
        )
        with self.engine.begin() as conn:
            self._check_fence(conn, fence)
            meta = _with_payload(meta, self._payload_id(conn, result, payload_uri))
            batch_id, created = self._insert_batch(conn, meta)
            if not created:
                return StageOutcome(batch_id, created=False)
            counts = _Counts()
            for action in result.rows:
                details = action.model_dump(mode="json", exclude={"instrument"})
                latest = conn.execute(
                    text("""
                        SELECT a.id, a.details FROM corporate_actions a
                        JOIN observation_batches b ON b.id = a.batch_id
                        WHERE a.instrument_id=:i AND a.provider_id=:p AND a.action_type=:t
                          AND a.effective_date=:d AND b.status IN ('staged','published')
                        ORDER BY b.recorded_at DESC, a.id DESC LIMIT 1
                    """),
                    {"i": instrument_id, "p": result.provider_id, "t": action.action_type,
                     "d": action.effective_date},
                ).one_or_none()
                if latest is not None and latest.details == details:
                    counts.unchanged += 1
                    continue
                action_id = uuid4()
                conn.execute(
                    text("""
                        INSERT INTO corporate_actions (id, instrument_id, provider_id, batch_id,
                            action_type, effective_date, announced_at, details, supersedes_id)
                        VALUES (:id, :i, :p, :b, :t, :d, :ann, CAST(:det AS jsonb), :sup)
                    """),
                    {"id": action_id, "i": instrument_id, "p": result.provider_id,
                     "b": batch_id, "t": action.action_type, "d": action.effective_date,
                     "ann": action.announced_at, "det": _json(details),
                     "sup": latest.id if latest is not None else None},
                )
                counts.ids.append(action_id)
                counts.written += 1
            return StageOutcome(batch_id, True, counts.written, counts.unchanged, 0,
                                tuple(counts.ids))

    def stage_reference(
        self,
        result: DiscoveryResult,
        *,
        scope: Mapping[str, Any],
        payload_uri: str | None = None,
        fence: Fence | None = None,
        run_id: UUID | None = None,
    ) -> StageOutcome:
        """Discovery output as a reference batch (expirations live in its scope)."""
        meta = BatchMeta(
            provider_id=result.provider_id, response_identity=str(result.response_id),
            batch_kind=BatchKind.REFERENCE, fetch_started_at=result.fetch_started_at,
            fetch_completed_at=result.fetch_completed_at,
            scope={**scope, "kind": "discovery", "symbol": result.instrument.symbol,
                   "expirations": [d.isoformat() for d in result.expirations]},
            coverage={"expirations": len(result.expirations),
                      "empty_reason": None if result.expirations else "no expirations"},
            run_id=run_id,
        )
        with self.engine.begin() as conn:
            self._check_fence(conn, fence)
            payload_id = None
            if result.payload is not None:
                payload_id = self._payload_ref_id(conn, result.provider_id, result.payload,
                                                  payload_uri)
            batch_id, created = self._insert_batch(conn, _with_payload(meta, payload_id))
            return StageOutcome(batch_id, created)

    def mark_dirty(self, windows: Iterable[Mapping[str, Any]]) -> int:
        """Queue derived buckets for (re)aggregation; re-dirties processed windows."""
        rows = list(windows)
        if not rows:
            return 0
        with self.engine.begin() as conn:
            for row in rows:
                conn.execute(
                    text("""
                        INSERT INTO derived_dirty_windows (instrument_id, provider_id,
                            timeframe, bucket_start, bucket_end, price_basis, session_kind,
                            source_policy_id, dirtied_at)
                        VALUES (:i, :p, :tf, :s, :e, :pb, :sk, :pol, :now)
                        ON CONFLICT (instrument_id, provider_id, timeframe, bucket_start,
                                     price_basis, session_kind, source_policy_id)
                        DO UPDATE SET processed_at = NULL, dirtied_at = EXCLUDED.dirtied_at
                    """),
                    {**row, "now": self.clock.now()},
                )
        return len(rows)

    def invalidate_snapshot(
        self, snapshot_id: UUID, observed_at: datetime, *, reason: str
    ) -> UUID:
        """Append an invalidation revision (prices null, ``invalidated`` flag)."""
        with self.engine.begin() as conn:
            row = conn.execute(
                text("""
                    SELECT s.*, b.fetch_started_at, b.fetch_completed_at, b.scope AS bscope
                    FROM option_snapshots s JOIN observation_batches b ON b.id = s.batch_id
                    WHERE s.id = :id AND s.observed_at = :obs
                """),
                {"id": snapshot_id, "obs": observed_at},
            ).mappings().one()
            meta = BatchMeta(
                provider_id=row["provider_id"],
                response_identity=f"invalidate:{snapshot_id}:{row['record_revision']}",
                batch_kind=BatchKind.CHAIN,
                fetch_started_at=row["fetch_started_at"],
                fetch_completed_at=row["fetch_completed_at"],
                scope={**row["bscope"], "invalidates": str(snapshot_id), "reason": reason},
                coverage={"invalidation": True},
            )
            batch_id, created = self._insert_batch(conn, meta)
            if not created:
                return batch_id
            flags = sorted({*row["quality_flags"], QualityFlag.INVALIDATED.value})
            content = {"invalidates": str(snapshot_id), "reason": reason}
            conn.execute(
                text("""
                    INSERT INTO option_snapshots (observed_at, id, batch_id, contract_id,
                        contract_terms_id, provider_id, observation_kind, provider_event_at,
                        provider_event_ns, provider_sequence, last_trade_at, field_semantics,
                        quality_flags, record_revision, supersedes_id, supersedes_observed_at,
                        content_sha256)
                    VALUES (:obs, :id, :b, :c, :t, :p, :kind, :ev, :evns, :seq, :lt,
                            CAST(:fs AS jsonb), :flags, :rev, :sup, :obs, :sha)
                """),
                {"obs": observed_at, "id": uuid4(), "b": batch_id, "c": row["contract_id"],
                 "t": row["contract_terms_id"], "p": row["provider_id"],
                 "kind": row["observation_kind"], "ev": row["provider_event_at"],
                 "evns": row["provider_event_ns"], "seq": row["provider_sequence"],
                 "lt": row["last_trade_at"], "fs": _json(row["field_semantics"]),
                 "flags": flags, "rev": row["record_revision"] + 1, "sup": snapshot_id,
                 "sha": hashlib.sha256(_json(content).encode()).hexdigest()},
            )
            return batch_id

    def invalidate_bar(self, bar_id: UUID, interval_start: datetime, *, reason: str) -> UUID:
        """Append an invalidation revision; copied OHLC are storage filler, never data."""
        with self.engine.begin() as conn:
            row = conn.execute(
                text("""
                    SELECT o.*, b.fetch_started_at, b.fetch_completed_at, b.scope AS bscope,
                           b.batch_kind
                    FROM bar_observations o JOIN observation_batches b ON b.id = o.batch_id
                    WHERE o.id = :id AND o.interval_start = :start
                """),
                {"id": bar_id, "start": interval_start},
            ).mappings().one()
            meta = BatchMeta(
                provider_id=row["provider_id"],
                response_identity=f"invalidate:{bar_id}",
                batch_kind=BatchKind(row["batch_kind"]),
                fetch_started_at=row["fetch_started_at"],
                fetch_completed_at=row["fetch_completed_at"],
                scope={**row["bscope"], "invalidates": str(bar_id), "reason": reason},
                coverage={"invalidation": True},
            )
            batch_id, created = self._insert_batch(conn, meta)
            if not created:
                return batch_id
            flags = sorted({*row["quality_flags"], QualityFlag.INVALIDATED.value})
            content = {"invalidates": str(bar_id), "reason": reason}
            conn.execute(
                text(_BAR_INSERT),
                {**_bar_params_from_row(dict(row)), "id": uuid4(), "batch": batch_id,
                 "complete": False, "flags": flags, "sup": bar_id,
                 "sha": hashlib.sha256(_json(content).encode()).hexdigest()},
            )
            return batch_id

    def record_quality_issue(
        self, batch_id: UUID | None, entity_ref: Mapping[str, Any], code: str, severity: str,
        details: Mapping[str, Any],
    ) -> None:
        with self.engine.begin() as conn:
            self._quality(conn, batch_id, entity_ref, code, severity, details)

    # -- Publication --------------------------------------------------------------------

    def publish(self, batch_id: UUID) -> datetime:
        """Finalize ``known_at``. Must be called after the staging transaction committed."""
        known_at = self.clock.now()
        if is_system_clock(self.clock):
            with self.engine.begin() as conn:
                db_now = conn.execute(text("SELECT clock_timestamp()")).scalar_one()
                skewed = abs(db_now - known_at) > self.max_clock_skew
                if skewed:  # recorded in its own committed transaction
                    self._incident(conn, "clock_skew", "critical",
                                   {"app": known_at.isoformat(), "db": db_now.isoformat()})
            if skewed:
                raise ClockSkewError(f"clock skew {db_now - known_at} exceeds limit")
        with self.engine.begin() as conn:
            published = conn.execute(
                text("""
                    UPDATE observation_batches SET status = 'published', known_at = :k
                    WHERE id = :id AND status = 'staged' RETURNING known_at
                """),
                {"k": known_at, "id": batch_id},
            ).scalar_one_or_none()
            if published is not None:
                return published  # type: ignore[no-any-return]
            current = conn.execute(
                text("SELECT status, known_at FROM observation_batches WHERE id = :id"),
                {"id": batch_id},
            ).one()
        if current.status != "published":
            raise InvariantViolation(f"batch {batch_id} is {current.status}, not publishable")
        return current.known_at  # type: ignore[no-any-return]

    def quarantine(self, batch_id: UUID, reason: str) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text("UPDATE observation_batches SET status='quarantined' "
                     "WHERE id=:id AND status='staged'"),
                {"id": batch_id},
            )
            self._quality(conn, batch_id, {"batch": str(batch_id)}, "batch_quarantined",
                          "error", {"reason": reason})

    def finalize_staged(self) -> list[UUID]:
        """Recovery: publish every staged batch with a recovery-time ``known_at``."""
        with self.engine.connect() as conn:
            staged = [UUID(str(r)) for r in conn.execute(text(
                "SELECT id FROM observation_batches WHERE status = 'staged' "
                "ORDER BY recorded_at")).scalars()]
        for batch_id in staged:
            self.publish(batch_id)
        return staged

    # -- Internals -------------------------------------------------------------------------

    def _check_fence(self, conn: Connection, fence: Fence | None) -> None:
        if fence is None:
            return
        job = conn.execute(
            text("SELECT status, lease_token, lease_until FROM collection_jobs "
                 "WHERE id = :id FOR UPDATE"),
            {"id": fence.job_id},
        ).one_or_none()
        if (job is None or job.status != "running" or job.lease_token != fence.lease_token
                or job.lease_until is None or job.lease_until <= self.clock.now()):
            raise StaleLease(f"job {fence.job_id} fence {fence.lease_token} is no longer valid")

    def _payload_id(
        self, conn: Connection, result: FetchResult[Any], uri: str | None
    ) -> UUID | None:
        ref: PayloadRef | None = result.payload
        if ref is None:
            return None
        return self._payload_ref_id(conn, result.provider_id, ref, uri)

    def _payload_ref_id(
        self, conn: Connection, provider_id: str, ref: PayloadRef, uri: str | None
    ) -> UUID:
        now = self.clock.now()
        inserted = conn.execute(
            text("""
                INSERT INTO payload_objects (id, provider_id, sha256, uri, representation,
                    parser_version, byte_count, created_at, body_deleted_at, missing_body_reason)
                VALUES (:id, :p, :sha, :uri, :rep, :pv, :bytes, :now, :deleted, :reason)
                ON CONFLICT (provider_id, sha256, representation, parser_version) DO NOTHING
                RETURNING id
            """),
            {"id": uuid4(), "p": provider_id, "sha": ref.sha256,
             "uri": uri or "unretained:", "rep": ref.representation.value,
             "pv": ref.parser_version, "bytes": ref.byte_count, "now": now,
             "deleted": None if uri else now, "reason": None if uri else "not retained"},
        ).scalar_one_or_none()
        if inserted is not None:
            return UUID(str(inserted))
        return UUID(str(conn.execute(
            text("SELECT id FROM payload_objects WHERE provider_id=:p AND sha256=:sha "
                 "AND representation=:rep AND parser_version=:pv"),
            {"p": provider_id, "sha": ref.sha256, "rep": ref.representation.value,
             "pv": ref.parser_version},
        ).scalar_one()))

    def _insert_batch(self, conn: Connection, meta: BatchMeta) -> tuple[UUID, bool]:
        new_id = uuid4()
        inserted = conn.execute(
            text("""
                INSERT INTO observation_batches (id, run_id, provider_id, response_identity,
                    payload_id, batch_kind, scope, fetch_started_at, fetch_completed_at,
                    recorded_at, status, advertised_delay_seconds, coverage)
                VALUES (:id, :run, :p, :rid, :payload, :kind, CAST(:scope AS jsonb), :fs, :fc,
                        :rec, 'staged', :delay, CAST(:cov AS jsonb))
                ON CONFLICT (provider_id, response_identity) DO NOTHING
                RETURNING id
            """),
            {"id": new_id, "run": meta.run_id, "p": meta.provider_id,
             "rid": meta.response_identity, "payload": meta.payload_id,
             "kind": meta.batch_kind.value, "scope": _json(meta.scope),
             "fs": meta.fetch_started_at, "fc": meta.fetch_completed_at,
             "rec": self.clock.now(), "delay": meta.advertised_delay_seconds,
             "cov": _json(meta.coverage)},
        ).scalar_one_or_none()
        if inserted is not None:
            return new_id, True
        existing = conn.execute(
            text("SELECT id FROM observation_batches WHERE provider_id=:p "
                 "AND response_identity=:rid"),
            {"p": meta.provider_id, "rid": meta.response_identity},
        ).scalar_one()
        return UUID(str(existing)), False

    def _resolve_contract(
        self, conn: Connection, ref: ContractRef, underlying_id: UUID, batch_id: UUID
    ) -> UUID | None:
        """Map a source symbol to a stable contract; quarantine anything ambiguous."""
        # A differing OCC root alone marks an adjusted/nonstandard series: kept (and
        # flagged by the adapter) for browsing; economic disagreement is quarantined.
        mismatch = [m for m in occ_mismatches(ref) if m != "root"]
        if mismatch:
            self._quality(conn, batch_id, {"contract": ref.provider_symbol},
                          QualityFlag.AMBIGUOUS_CONTRACT.value, "error",
                          {"mismatch": mismatch})
            return None
        _lock(conn, f"contract:{ref.provider_id}:{ref.provider_symbol}")
        candidates = conn.execute(
            text("""
                SELECT c.id, c.underlying_id, c.option_type, c.strike, c.expiration_date
                FROM contract_identifiers i JOIN option_contracts c ON c.id = i.contract_id
                WHERE i.provider_id = :p AND i.symbol = :s
                  AND i.valid_from <= :exp AND (i.valid_to IS NULL OR i.valid_to > :exp)
                  AND NOT EXISTS (SELECT 1 FROM contract_identifiers n
                                  WHERE n.supersedes_id = i.id)
            """),
            {"p": ref.provider_id, "s": ref.provider_symbol, "exp": ref.expiration_date},
        ).all()
        if len({c.id for c in candidates}) > 1:
            self._quality(conn, batch_id, {"contract": ref.provider_symbol},
                          QualityFlag.AMBIGUOUS_CONTRACT.value, "error",
                          {"candidates": sorted(str(c.id) for c in candidates)})
            return None
        if candidates:
            c = candidates[0]
            if (c.underlying_id != underlying_id or c.option_type != ref.option_type.value
                    or c.strike != ref.strike or c.expiration_date != ref.expiration_date):
                self._quality(conn, batch_id, {"contract": ref.provider_symbol},
                              QualityFlag.AMBIGUOUS_CONTRACT.value, "error",
                              {"reason": "source symbol now reports different economics",
                               "existing": str(c.id)})
                return None
            return UUID(str(c.id))
        now = self.clock.now()
        contract_id = uuid4()
        conn.execute(
            text("""
                INSERT INTO option_contracts (id, underlying_id, option_type, strike,
                    expiration_date, created_at)
                VALUES (:id, :u, :t, :k, :e, :now)
            """),
            {"id": contract_id, "u": underlying_id, "t": ref.option_type.value,
             "k": ref.strike, "e": ref.expiration_date, "now": now},
        )
        conn.execute(
            text("""
                INSERT INTO contract_identifiers (id, contract_id, provider_id, symbol,
                    valid_from, known_at, evidence)
                VALUES (:id, :c, :p, :s, :vf, :now, CAST(:ev AS jsonb))
            """),
            {"id": uuid4(), "c": contract_id, "p": ref.provider_id, "s": ref.provider_symbol,
             "vf": date(1970, 1, 1), "now": now,
             "ev": _json({"first_seen_batch": str(batch_id), "occ_consistent": True})},
        )
        return contract_id

    def _insert_snapshot(
        self, conn: Connection, batch_id: UUID, contract_id: UUID, row: OptionObservation,
        *, revision: int = 1, supersedes: tuple[UUID, datetime] | None = None,
    ) -> UUID:
        snapshot_id = uuid4()
        conn.execute(
            text("""
                INSERT INTO option_snapshots (observed_at, id, batch_id, contract_id,
                    provider_id, observation_kind, provider_event_at, provider_event_ns,
                    provider_sequence, last_trade_at, bid, ask, last, bid_size, ask_size,
                    reported_iv, reported_volume, reported_open_interest, volume_session_date,
                    open_interest_asof_date, in_the_money, field_semantics, quality_flags,
                    record_revision, supersedes_id, supersedes_observed_at, content_sha256)
                VALUES (:obs, :id, :b, :c, :p, :kind, :ev, :evns, :seq, :lt, :bid, :ask, :last,
                        :bs, :as_, :iv, :vol, :oi, :vsd, :oid, :itm, CAST(:fs AS jsonb),
                        :flags, :rev, :sup, :supobs, :sha)
            """),
            {"obs": row.observed_at, "id": snapshot_id, "b": batch_id, "c": contract_id,
             "p": row.contract.provider_id, "kind": row.observation_kind.value,
             "ev": row.provider_event_at, "evns": row.provider_event_ns,
             "seq": row.provider_sequence, "lt": row.last_trade_at, "bid": row.bid,
             "ask": row.ask, "last": row.last, "bs": row.bid_size, "as_": row.ask_size,
             "iv": row.reported_iv, "vol": row.reported_volume,
             "oi": row.reported_open_interest, "vsd": row.volume_session_date,
             "oid": row.open_interest_asof_date, "itm": row.in_the_money,
             "fs": row.field_semantics.canonical_json(),
             "flags": [f.value for f in row.quality_flags], "rev": revision,
             "sup": supersedes[0] if supersedes else None,
             "supobs": supersedes[1] if supersedes else None,
             "sha": row.canonical_sha256()},
        )
        return snapshot_id

    def _insert_bars(
        self, conn: Connection, batch_id: UUID, instrument_id: UUID, provider_id: str,
        policy_id: str, writes: Iterable[BarWrite],
    ) -> _Counts:
        counts = _Counts()
        for write in sorted(writes, key=lambda w: w.observation.interval_start):
            bar = write.observation
            key = (f"bar:{instrument_id}:{provider_id}:{bar.interval_start.isoformat()}:"
                   f"{bar.timeframe.value}:{bar.price_basis.value}:{bar.session_kind.value}:"
                   f"{policy_id}")
            _lock(conn, key)
            sha = write.content_sha256()
            latest = conn.execute(
                text("""
                    SELECT o.id, o.content_sha256 FROM bar_observations o
                    JOIN observation_batches x ON x.id = o.batch_id
                    WHERE o.instrument_id = :i AND o.provider_id = :p
                      AND o.interval_start = :s AND o.timeframe = :tf
                      AND o.price_basis = :pb AND o.session_kind = :sk AND o.policy_id = :pol
                      AND x.status IN ('staged', 'published')
                    ORDER BY x.recorded_at DESC, o.id DESC LIMIT 1
                """),
                {"i": instrument_id, "p": provider_id, "s": bar.interval_start,
                 "tf": bar.timeframe.value, "pb": bar.price_basis.value,
                 "sk": bar.session_kind.value, "pol": policy_id},
            ).one_or_none()
            if latest is not None and latest.content_sha256 == sha:
                counts.unchanged += 1
                continue
            bar_id = uuid4()
            conn.execute(
                text(_BAR_INSERT),
                {"start": bar.interval_start, "id": bar_id, "instrument": instrument_id,
                 "provider": provider_id, "batch": batch_id, "tf": bar.timeframe.value,
                 "end": bar.interval_end, "sd": bar.session_date,
                 "sk": bar.session_kind.value, "pb": bar.price_basis.value, "pol": policy_id,
                 "o": bar.open, "h": bar.high, "l": bar.low, "c": bar.close,
                 "v": bar.volume, "em": write.expected_minutes, "om": write.observed_minutes,
                 "complete": write.complete, "short": write.is_short_session_bucket,
                 "sha": sha, "refs": _json(list(write.input_refs)),
                 "sup": latest.id if latest is not None else None,
                 "flags": sorted({*(f.value for f in bar.quality_flags),
                                  *(f.value for f in write.extra_flags)})},
            )
            counts.ids.append(bar_id)
            counts.written += 1
        return counts

    def _rejected(self, conn: Connection, batch_id: UUID, rejected: Iterable[RejectedRow]) -> None:
        for row in rejected:
            self._quality(conn, batch_id,
                          {"row_index": row.row_index, "source_key": row.source_key},
                          f"rejected_{row.reason.value}", "warning", {"detail": row.detail})

    def _quality(
        self, conn: Connection, batch_id: UUID | None, entity: Mapping[str, Any], code: str,
        severity: str, details: Mapping[str, Any],
    ) -> None:
        conn.execute(
            text("""
                INSERT INTO quality_issues (id, batch_id, entity_ref, code, severity, details,
                                            detected_at)
                VALUES (:id, :b, CAST(:e AS jsonb), :code, :sev, CAST(:d AS jsonb), :now)
            """),
            {"id": uuid4(), "b": batch_id, "e": _json(entity), "code": code, "sev": severity,
             "d": _json(details), "now": self.clock.now()},
        )

    def _incident(
        self, conn: Connection, kind: str, severity: str, details: Mapping[str, Any]
    ) -> None:
        conn.execute(
            text("""
                INSERT INTO operational_incidents (id, kind, severity, details, opened_at)
                VALUES (:id, :k, :s, CAST(:d AS jsonb), :now)
            """),
            {"id": uuid4(), "k": kind, "s": severity, "d": _json(details),
             "now": self.clock.now()},
        )


_BAR_INSERT = """
    INSERT INTO bar_observations (interval_start, id, instrument_id, provider_id, batch_id,
        timeframe, interval_end, session_date, session_kind, price_basis, policy_id, open, high,
        low, close, volume, expected_minutes, observed_minutes, complete,
        is_short_session_bucket, content_sha256, input_refs, supersedes_id, quality_flags)
    VALUES (:start, :id, :instrument, :provider, :batch, :tf, :end, :sd, :sk, :pb, :pol, :o, :h,
            :l, :c, :v, :em, :om, :complete, :short, :sha, CAST(:refs AS jsonb), :sup, :flags)
"""


def _bar_params_from_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "start": row["interval_start"], "instrument": row["instrument_id"],
        "provider": row["provider_id"], "tf": row["timeframe"], "end": row["interval_end"],
        "sd": row["session_date"], "sk": row["session_kind"], "pb": row["price_basis"],
        "pol": row["policy_id"], "o": row["open"], "h": row["high"], "l": row["low"],
        "c": row["close"], "v": row["volume"], "em": row["expected_minutes"],
        "om": row["observed_minutes"], "short": row["is_short_session_bucket"],
        "refs": _json(row["input_refs"]),
    }


def _default_bar_write(bar: BarObservation) -> BarWrite:
    if bar.timeframe is Timeframe.M1:
        return BarWrite(bar, complete=True, expected_minutes=1, observed_minutes=1)
    return BarWrite(bar, complete=True)


def _with_payload(meta: BatchMeta, payload_id: UUID | None) -> BatchMeta:
    return BatchMeta(**{**meta.__dict__, "payload_id": payload_id})


def _coverage_json(coverage: Coverage) -> dict[str, Any]:
    data: dict[str, Any] = coverage.model_dump(mode="json")
    return data


def _active_symbol(conn: Connection, instrument_id: UUID, provider_id: str) -> str:
    symbols = conn.execute(
        text("""
            SELECT symbol FROM instrument_identifiers i
            WHERE instrument_id = :iid AND provider_id = :p
              AND NOT EXISTS (SELECT 1 FROM instrument_identifiers n WHERE n.supersedes_id = i.id)
            ORDER BY known_at DESC
        """),
        {"iid": instrument_id, "p": provider_id},
    ).scalars().all()
    if not symbols:
        raise InvariantViolation(
            f"instrument {instrument_id} has no {provider_id} identifier (wrong source?)")
    return str(symbols[0])


def resolve_instrument(
    conn: Connection, provider_id: str, symbol: str, *, known_by: datetime, effective: date
) -> UUID | None:
    rows = conn.execute(
        text("""
            SELECT DISTINCT i.instrument_id FROM instrument_identifiers i
            WHERE i.provider_id = :p AND i.symbol = :s AND i.known_at <= :k
              AND i.valid_from <= :d AND (i.valid_to IS NULL OR i.valid_to > :d)
              AND NOT EXISTS (SELECT 1 FROM instrument_identifiers n
                              WHERE n.supersedes_id = i.id AND n.known_at <= :k)
        """),
        {"p": provider_id, "s": symbol, "k": known_by, "d": effective},
    ).scalars().all()
    if len(rows) > 1:
        raise IdentityConflict(f"{provider_id}:{symbol} is ambiguous at {known_by}")
    return UUID(str(rows[0])) if rows else None


def decimal_text(value: Any) -> str | None:
    return None if value is None else format_decimal(value)
