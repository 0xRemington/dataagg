"""Point-in-time (causal) queries (02 "Point-in-time query contract", T11).

Rules implemented here:

1. Filter to *published* batches with ``known_at <= D`` **before** choosing the
   latest revision per logical key (knowledge-first; never latest-then-filter).
2. Bars must have closed (``interval_end <= D``); default research requires
   ``complete``. Completeness/invalidation are applied *after* revision
   selection, so a newer invalid revision never revives an older accepted one.
3. Option rows: latest parser revision known by D per observation identity;
   provider event times, when present, must also be ``<= D``.
4. Panel spot joins use cutoff ``min(D, option.known_at)``, the same session,
   and age ceilings. Unknown quote-event time stays unknown and is never
   execution-eligible.

The SQL predicates are module constants so mutation tests can prove that
removing the knowledge cutoff makes the adversarial fixtures fail.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import Connection, text

from marketagg.domain.enums import AvailabilityMode, PriceBasis, SessionKind, Timeframe

# Mutation-test seams: each is a SQL boolean expression.
BATCH_KNOWLEDGE_PREDICATE = "x.known_at <= :decision_at"
BAR_CLOSED_PREDICATE = "b.interval_end <= :decision_at"
OPTION_EVENT_PREDICATE = "(s.provider_event_at IS NULL OR s.provider_event_at <= :decision_at)"
# Mutation seam: True reproduces the forbidden "latest revision first, then filter
# knowledge" bug. Must stay False; tests flip it to prove the adversaries catch it.
SELECT_LATEST_BEFORE_KNOWLEDGE = False


@dataclass(frozen=True)
class BarKey:
    instrument_id: UUID
    provider_id: str
    timeframe: Timeframe
    price_basis: PriceBasis
    session_kind: SessionKind
    policy_id: str


@dataclass(frozen=True)
class BarRecord:
    id: UUID
    batch_id: UUID
    interval_start: datetime
    interval_end: datetime
    session_date: date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int | None
    complete: bool
    expected_minutes: int | None
    observed_minutes: int | None
    is_short_session_bucket: bool
    quality_flags: tuple[str, ...]
    known_at: datetime
    content_sha256: str
    timeframe: str
    price_basis: str
    policy_id: str
    provider_id: str

    @property
    def invalidated(self) -> bool:
        return "invalidated" in self.quality_flags


@dataclass(frozen=True)
class OptionRecord:
    id: UUID
    batch_id: UUID
    contract_id: UUID
    provider_id: str
    observed_at: datetime
    known_at: datetime
    fetch_completed_at: datetime
    provider_event_at: datetime | None
    last_trade_at: datetime | None
    bid: Decimal | None
    ask: Decimal | None
    last: Decimal | None
    bid_size: int | None
    ask_size: int | None
    reported_iv: float | None
    reported_volume: int | None
    reported_open_interest: int | None
    in_the_money: bool | None
    quality_flags: tuple[str, ...]
    record_revision: int
    content_sha256: str
    field_semantics: dict[str, Any]

    @property
    def invalidated(self) -> bool:
        return "invalidated" in self.quality_flags


def eligible_bars(
    conn: Connection,
    key: BarKey,
    *,
    start: datetime,
    end: datetime,
    decision_at: datetime,
    include_partial: bool = False,
    after_start: datetime | None = None,
    limit: int | None = None,
) -> list[BarRecord]:
    """Bars of one logical series with ``start <= interval_start < end`` as known at D."""
    knowledge = "TRUE" if SELECT_LATEST_BEFORE_KNOWLEDGE else BATCH_KNOWLEDGE_PREDICATE
    late_filter = (f"AND {BATCH_KNOWLEDGE_PREDICATE.replace('x.known_at', 'known_at')}"
                   if SELECT_LATEST_BEFORE_KNOWLEDGE else "")
    sql = f"""
        WITH eligible AS (
          SELECT b.*, x.known_at,
                 row_number() OVER (
                   PARTITION BY b.instrument_id, b.provider_id, b.interval_start,
                                b.timeframe, b.price_basis, b.session_kind, b.policy_id
                   ORDER BY x.known_at DESC, b.id DESC) AS rn
          FROM bar_observations b
          JOIN observation_batches x ON x.id = b.batch_id
          WHERE x.status = 'published' AND {knowledge}
            AND {BAR_CLOSED_PREDICATE}
            AND b.instrument_id = :instrument_id AND b.provider_id = :provider_id
            AND b.timeframe = :timeframe AND b.price_basis = :price_basis
            AND b.session_kind = :session_kind AND b.policy_id = :policy_id
            AND b.interval_start >= :start AND b.interval_start < :end
        )
        SELECT * FROM eligible
        WHERE rn = 1 AND NOT ('invalidated' = ANY(quality_flags))
          AND (complete OR :include_partial) {late_filter}
          AND (CAST(:after AS timestamptz) IS NULL OR interval_start > :after)
        ORDER BY interval_start
        LIMIT :limit
    """
    rows = conn.execute(text(sql), {
        "decision_at": decision_at, "instrument_id": key.instrument_id,
        "provider_id": key.provider_id, "timeframe": key.timeframe.value,
        "price_basis": key.price_basis.value, "session_kind": key.session_kind.value,
        "policy_id": key.policy_id, "start": start, "end": end,
        "include_partial": include_partial, "after": after_start, "limit": limit,
    }).mappings().all()
    return [_bar(r) for r in rows]


def _bar(r: Any) -> BarRecord:
    return BarRecord(
        id=r["id"], batch_id=r["batch_id"], interval_start=r["interval_start"],
        interval_end=r["interval_end"], session_date=r["session_date"], open=r["open"],
        high=r["high"], low=r["low"], close=r["close"], volume=r["volume"],
        complete=r["complete"], expected_minutes=r["expected_minutes"],
        observed_minutes=r["observed_minutes"],
        is_short_session_bucket=r["is_short_session_bucket"],
        quality_flags=tuple(r["quality_flags"]), known_at=r["known_at"],
        content_sha256=r["content_sha256"], timeframe=r["timeframe"],
        price_basis=r["price_basis"], policy_id=r["policy_id"], provider_id=r["provider_id"],
    )


def eligible_options(
    conn: Connection,
    contract_ids: Sequence[UUID],
    *,
    start: datetime,
    end: datetime,
    decision_at: datetime,
    provider_id: str | None = None,
    include_invalidated: bool = False,
    after: tuple[datetime, UUID] | None = None,
    limit: int | None = None,
) -> list[OptionRecord]:
    """Option observations with ``start <= observed_at < end`` as known at D."""
    if not contract_ids:
        return []
    sql = f"""
        WITH eligible AS (
          SELECT s.*, x.known_at, x.fetch_completed_at,
                 row_number() OVER (
                   PARTITION BY s.contract_id, s.provider_id, s.observed_at
                   ORDER BY x.known_at DESC, s.record_revision DESC, s.id DESC) AS rn
          FROM option_snapshots s
          JOIN observation_batches x ON x.id = s.batch_id
          WHERE x.status = 'published' AND {BATCH_KNOWLEDGE_PREDICATE}
            AND s.contract_id = ANY(:contracts)
            AND s.observed_at >= :start AND s.observed_at < :end
            AND (CAST(:provider AS text) IS NULL OR s.provider_id = :provider)
        )
        SELECT * FROM eligible s
        WHERE rn = 1 AND {OPTION_EVENT_PREDICATE}
          AND (:include_invalidated OR NOT ('invalidated' = ANY(quality_flags)))
          AND (CAST(:after_t AS timestamptz) IS NULL
               OR (observed_at, id) > (CAST(:after_t AS timestamptz), CAST(:after_id AS uuid)))
        ORDER BY observed_at, id
        LIMIT :limit
    """
    rows = conn.execute(text(sql), {
        "decision_at": decision_at, "contracts": list(contract_ids), "start": start,
        "end": end, "provider": provider_id, "include_invalidated": include_invalidated,
        "after_t": after[0] if after else None, "after_id": after[1] if after else None,
        "limit": limit,
    }).mappings().all()
    return [_option(r) for r in rows]


def _option(r: Any) -> OptionRecord:
    return OptionRecord(
        id=r["id"], batch_id=r["batch_id"], contract_id=r["contract_id"],
        provider_id=r["provider_id"], observed_at=r["observed_at"], known_at=r["known_at"],
        fetch_completed_at=r["fetch_completed_at"], provider_event_at=r["provider_event_at"],
        last_trade_at=r["last_trade_at"], bid=r["bid"], ask=r["ask"], last=r["last"],
        bid_size=r["bid_size"], ask_size=r["ask_size"], reported_iv=r["reported_iv"],
        reported_volume=r["reported_volume"],
        reported_open_interest=r["reported_open_interest"], in_the_money=r["in_the_money"],
        quality_flags=tuple(r["quality_flags"]), record_revision=r["record_revision"],
        content_sha256=r["content_sha256"], field_semantics=r["field_semantics"],
    )


@dataclass(frozen=True)
class ContractInfo:
    id: UUID
    underlying_id: UUID
    option_type: str
    strike: Decimal
    expiration_date: date
    provider_symbol: str
    first_known_at: datetime
    has_terms: bool


def contracts_asof(
    conn: Connection,
    instrument_id: UUID,
    *,
    decision_at: datetime,
    provider_id: str,
    expiration_date: date | None = None,
    option_type: str | None = None,
    strike_min: Decimal | None = None,
    strike_max: Decimal | None = None,
) -> list[ContractInfo]:
    """Contracts observable by D: first seen in a batch already known at D (no hindsight)."""
    sql = f"""
        SELECT c.id, c.underlying_id, c.option_type, c.strike, c.expiration_date,
               min(i.symbol) AS provider_symbol, min(x.known_at) AS first_known_at,
               EXISTS (SELECT 1 FROM contract_terms t WHERE t.contract_id = c.id
                       AND t.known_at <= :decision_at) AS has_terms
        FROM option_contracts c
        JOIN contract_identifiers i ON i.contract_id = c.id AND i.provider_id = :provider
        JOIN option_snapshots s ON s.contract_id = c.id
        JOIN observation_batches x ON x.id = s.batch_id
        WHERE c.underlying_id = :instrument AND x.status = 'published'
          AND {BATCH_KNOWLEDGE_PREDICATE}
          AND (CAST(:exp AS date) IS NULL OR c.expiration_date = :exp)
          AND (CAST(:otype AS text) IS NULL OR c.option_type = :otype)
          AND (CAST(:kmin AS numeric) IS NULL OR c.strike >= :kmin)
          AND (CAST(:kmax AS numeric) IS NULL OR c.strike <= :kmax)
        GROUP BY c.id
        ORDER BY c.expiration_date, c.option_type, c.strike
    """
    rows = conn.execute(text(sql), {
        "decision_at": decision_at, "provider": provider_id, "instrument": instrument_id,
        "exp": expiration_date, "otype": option_type, "kmin": strike_min, "kmax": strike_max,
    }).mappings().all()
    return [ContractInfo(**dict(r)) for r in rows]


def actions_asof(conn: Connection, instrument_id: UUID, *, decision_at: datetime,
                 provider_id: str) -> list[dict[str, Any]]:
    """Corporate actions as known at D (latest known revision per action key)."""
    sql = f"""
        WITH eligible AS (
          SELECT a.*, x.known_at, row_number() OVER (
                   PARTITION BY a.instrument_id, a.provider_id, a.action_type, a.effective_date
                   ORDER BY x.known_at DESC, a.id DESC) AS rn
          FROM corporate_actions a JOIN observation_batches x ON x.id = a.batch_id
          WHERE x.status = 'published' AND {BATCH_KNOWLEDGE_PREDICATE}
            AND a.instrument_id = :i AND a.provider_id = :p
        )
        SELECT action_type, effective_date, details, known_at FROM eligible WHERE rn = 1
        ORDER BY effective_date
    """
    return [dict(r) for r in conn.execute(text(sql), {
        "decision_at": decision_at, "i": instrument_id, "p": provider_id}).mappings()]


# -- Panel sampling -----------------------------------------------------------------------


@dataclass(frozen=True)
class AgeLimits:
    option: timedelta
    spot: timedelta


@dataclass(frozen=True)
class PanelPoint:
    bar: BarRecord
    decision_at: datetime
    option: OptionRecord | None
    spot: BarRecord | None
    option_observation_age_s: float | None
    quote_age_s: float | None
    spot_age_s: float | None
    freshness: str
    execution_eligible: bool
    exclusions: tuple[str, ...] = field(default_factory=tuple)


def sample_panel(
    conn: Connection,
    *,
    bars: Sequence[BarRecord],
    contract_id: UUID | None,
    spot_key: BarKey,
    session_of: Any,
    decision_cap: datetime,
    limits: AgeLimits,
    provider_id: str,
) -> list[PanelPoint]:
    """Sample one option observation and one spot bar per underlying bar end.

    ``session_of(datetime) -> date | None`` maps instants to session dates so no
    observation is carried across sessions.
    """
    points: list[PanelPoint] = []
    for bar in bars:
        decision = min(bar.interval_end, decision_cap)
        session = bar.session_date
        exclusions: list[str] = []
        option = None
        if contract_id is not None:
            candidates = eligible_options(
                conn, [contract_id], start=decision - limits.option * 4, end=decision,
                decision_at=decision, provider_id=provider_id)
            same_session = [o for o in candidates if session_of(o.fetch_completed_at) == session]
            if candidates and not same_session:
                exclusions.append("option_other_session")
            fresh = [o for o in same_session
                     if decision - o.fetch_completed_at <= limits.option]
            if same_session and not fresh:
                exclusions.append("option_too_old")
            option = fresh[-1] if fresh else None
        spot = None
        if option is not None:
            cutoff = min(decision, option.known_at)
            spot_rows = eligible_bars(conn, spot_key, start=cutoff - limits.spot * 4,
                                      end=cutoff, decision_at=cutoff)
            spot_rows = [s for s in spot_rows if s.session_date == session
                         and s.interval_end <= cutoff]
            spot = spot_rows[-1] if spot_rows else None
            if spot is not None and cutoff - spot.interval_end > limits.spot:
                exclusions.append("spot_too_old")
                spot = None
            if spot is None and "spot_too_old" not in exclusions:
                exclusions.append("spot_unavailable")
        quote_age = None
        if option is not None and option.provider_event_at is not None:
            quote_age = (decision - option.provider_event_at).total_seconds()
        points.append(PanelPoint(
            bar=bar, decision_at=decision, option=option, spot=spot,
            option_observation_age_s=(None if option is None
                                      else (decision - option.fetch_completed_at)
                                      .total_seconds()),
            quote_age_s=quote_age,
            spot_age_s=(None if spot is None or option is None
                        else (min(decision, option.known_at) - spot.interval_end)
                        .total_seconds()),
            freshness="known" if quote_age is not None else "unknown",
            # Yahoo-style observations without event time are never execution-eligible.
            execution_eligible=False,
            exclusions=tuple(exclusions),
        ))
    return points


def availability_label(mode: AvailabilityMode) -> str:
    return ("observed: outputs actually published by the decision time"
            if mode is AvailabilityMode.OBSERVED
            else "causal recomputation: current code over inputs known by the decision time")
