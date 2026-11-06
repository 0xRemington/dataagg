"""Research query contract (02 ``ResearchRepository``) shared by API and exports.

Every query carries an explicit decision cutoff and availability mode. When a
caller asks for "latest", the API fixes the cutoff at the first page and embeds
it in the cursor, so pagination stays stable while ingestion continues.
"""

from __future__ import annotations

import base64
import hashlib
import json
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import Engine, text

from marketagg.analytics.aggregate import aggregate_bucket
from marketagg.analytics.asof import (
    AgeLimits,
    BarKey,
    BarRecord,
    ContractInfo,
    OptionRecord,
    PanelPoint,
    availability_label,
    contracts_asof,
    eligible_bars,
    eligible_options,
    sample_panel,
)
from marketagg.calendar import SessionCalendar
from marketagg.crypto import binance
from marketagg.domain import AvailabilityMode, InstrumentRef, PriceBasis, SessionKind, Timeframe

MAX_PAGE = 10_000
DEFAULT_INTRADAY_SPAN = timedelta(days=31)
MAX_DAILY_SPAN = timedelta(days=5 * 366)


class QueryError(ValueError):
    """Invalid or unsupported query (API 422)."""


class IncompatibleQuery(ValueError):
    """Incompatible basis/source/policy combination (API 409)."""


@dataclass(frozen=True)
class Page[T]:
    items: list[T]
    next_cursor: str | None
    decision_at: datetime


@dataclass(frozen=True)
class Cursor:
    decision_at: datetime
    after_time: datetime
    after_id: str
    query_hash: str

    def encode(self) -> str:
        raw = json.dumps({"d": self.decision_at.isoformat(), "t": self.after_time.isoformat(),
                          "i": self.after_id, "q": self.query_hash}).encode()
        return base64.urlsafe_b64encode(raw).decode().rstrip("=")

    @classmethod
    def decode(cls, token: str, query_hash: str) -> Cursor:
        try:
            raw = json.loads(base64.urlsafe_b64decode(token + "=" * (-len(token) % 4)))
            cursor = cls(datetime.fromisoformat(raw["d"]), datetime.fromisoformat(raw["t"]),
                         str(raw["i"]), str(raw["q"]))
        except (ValueError, KeyError, TypeError) as exc:
            raise QueryError("malformed cursor") from exc
        if cursor.query_hash != query_hash:
            raise QueryError("cursor belongs to a different query")
        return cursor


def query_hash(*parts: Any) -> str:
    return hashlib.sha256(json.dumps([str(p) for p in parts]).encode()).hexdigest()[:24]


@dataclass(frozen=True)
class PanelResult:
    points: list[PanelPoint]
    decision_at: datetime
    availability: str
    warnings: list[str] = field(default_factory=list)


class ResearchRepository:
    def __init__(self, engine: Engine, calendar: SessionCalendar, policy_ids: dict[str, str],
                 *, recompute_lag: timedelta = timedelta(seconds=60)) -> None:
        self.engine = engine
        self.calendar = calendar
        self.policy_ids = policy_ids
        self.recompute_lag = recompute_lag

    # -- Reference ---------------------------------------------------------------

    def instruments(self) -> list[dict[str, Any]]:
        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT i.id, i.asset_class, i.currency, i.calendar_id, s.provider_id, s.symbol,
                       s.valid_from, s.valid_to, s.known_at, s.status
                FROM instruments i JOIN instrument_identifiers s ON s.instrument_id = i.id
                WHERE NOT EXISTS (SELECT 1 FROM instrument_identifiers n
                                  WHERE n.supersedes_id = s.id)
                ORDER BY s.symbol""")).mappings().all()
        return [dict(r) for r in rows]

    def provider_basis(self, provider_id: str) -> str | None:
        with self.engine.connect() as conn:
            caps = conn.execute(text("SELECT capabilities FROM providers WHERE id=:p"),
                                {"p": provider_id}).scalar_one_or_none()
        if caps is None:
            raise QueryError(f"unknown provider {provider_id}")
        basis = caps.get("semantics", {}).get("adjustment_basis")
        return None if basis is None else str(basis)

    def contracts(self, instrument_id: UUID, *, provider_id: str, decision_at: datetime,
                  expiration: date | None = None, option_type: str | None = None,
                  strike_min: Decimal | None = None, strike_max: Decimal | None = None,
                  ) -> list[ContractInfo]:
        with self.engine.connect() as conn:
            return contracts_asof(conn, instrument_id, decision_at=decision_at,
                                  provider_id=provider_id, expiration_date=expiration,
                                  option_type=option_type, strike_min=strike_min,
                                  strike_max=strike_max)

    # -- Bars ------------------------------------------------------------------------

    def bar_key(self, instrument_id: UUID, provider_id: str, timeframe: Timeframe,
                price_basis: PriceBasis) -> BarKey:
        provider_basis = self.provider_basis(provider_id)
        if provider_basis is not None and price_basis.value != provider_basis:
            raise IncompatibleQuery(
                f"provider {provider_id} supplies {provider_basis}, not {price_basis.value}")
        derived = timeframe in (Timeframe.M5, Timeframe.M15, Timeframe.H4)
        policy = self.policy_ids["aggregation" if derived else "bars_ingest"]
        if provider_id == binance.PROVIDER_ID:  # native klines, one policy for every timeframe
            policy = binance.BARS_POLICY.id
        return BarKey(instrument_id, provider_id, timeframe, price_basis, SessionKind.RTH,
                      policy)

    def bars(self, *, instrument_id: UUID, provider_id: str, timeframe: Timeframe,
             start: datetime, end: datetime, decision_at: datetime,
             price_basis: PriceBasis = PriceBasis.PROVIDER_NATIVE,
             mode: AvailabilityMode = AvailabilityMode.OBSERVED,
             include_partial: bool = False, limit: int = MAX_PAGE,
             cursor: str | None = None) -> Page[BarRecord]:
        if end <= start:
            raise QueryError("end must be after start")
        span_limit = MAX_DAILY_SPAN if timeframe is Timeframe.D1 else DEFAULT_INTRADAY_SPAN
        if end - start > span_limit:
            raise QueryError(f"window exceeds {span_limit.days} days; use an export job")
        limit = min(max(limit, 1), MAX_PAGE)
        key = self.bar_key(instrument_id, provider_id, timeframe, price_basis)
        qh = query_hash("bars", key, start, end, mode, include_partial)
        after = None
        if cursor is not None:
            decoded = Cursor.decode(cursor, qh)
            decision_at, after = decoded.decision_at, decoded.after_time
        if mode is AvailabilityMode.RECOMPUTED and key.timeframe in (
                Timeframe.M5, Timeframe.M15, Timeframe.H4):
            items = self._recomputed(key, start, end, decision_at, include_partial)
            items = [b for b in items if after is None or b.interval_start > after][:limit + 1]
        else:
            with self.engine.connect() as conn:
                items = eligible_bars(conn, key, start=start, end=end, decision_at=decision_at,
                                      include_partial=include_partial, after_start=after,
                                      limit=limit + 1)
        next_cursor = None
        if len(items) > limit:
            items = items[:limit]
            last = items[-1]
            next_cursor = Cursor(decision_at, last.interval_start, str(last.id), qh).encode()
        return Page(items, next_cursor, decision_at)

    def _recomputed(self, key: BarKey, start: datetime, end: datetime, decision_at: datetime,
                    include_partial: bool) -> list[BarRecord]:
        """Causal recomputation: current code over 1m inputs known by D - simulated lag."""
        cutoff = decision_at - self.recompute_lag
        m1 = BarKey(key.instrument_id, key.provider_id, Timeframe.M1, key.price_basis,
                    key.session_kind, self.policy_ids["bars_ingest"])
        with self.engine.connect() as conn:
            minutes = eligible_bars(conn, m1, start=start, end=end, decision_at=cutoff)
        sessions = self.calendar.sessions_overlapping(start, end)
        out: list[BarRecord] = []
        ref = InstrumentRef(provider_id=key.provider_id, symbol="recomputed")
        for session in sessions:
            for bucket in self.calendar.buckets(session, key.timeframe):
                if not (start <= bucket.start < end) or bucket.end > cutoff:
                    continue
                write = aggregate_bucket(bucket, minutes, ref, key.price_basis)
                if write is None or (not write.complete and not include_partial):
                    continue
                bar = write.observation
                out.append(BarRecord(
                    id=UUID(write.content_sha256()[:32]), batch_id=UUID(int=0),
                    interval_start=bar.interval_start, interval_end=bar.interval_end,
                    session_date=bar.session_date, open=bar.open, high=bar.high, low=bar.low,
                    close=bar.close, volume=bar.volume, complete=write.complete,
                    expected_minutes=write.expected_minutes,
                    observed_minutes=write.observed_minutes,
                    is_short_session_bucket=write.is_short_session_bucket,
                    quality_flags=("recomputed",), known_at=decision_at,
                    content_sha256=write.content_sha256(), timeframe=key.timeframe.value,
                    price_basis=key.price_basis.value, policy_id=key.policy_id,
                    provider_id=key.provider_id))
        return out

    # -- Options ---------------------------------------------------------------------

    def options(self, *, contract_id: UUID, start: datetime, end: datetime,
                decision_at: datetime, provider_id: str | None = None,
                limit: int = MAX_PAGE, cursor: str | None = None) -> Page[OptionRecord]:
        if end <= start:
            raise QueryError("end must be after start")
        if end - start > DEFAULT_INTRADAY_SPAN * 3:
            raise QueryError("option history window exceeds 93 days; use an export job")
        limit = min(max(limit, 1), MAX_PAGE)
        qh = query_hash("options", contract_id, start, end, provider_id)
        after: tuple[datetime, UUID] | None = None
        if cursor is not None:
            decoded = Cursor.decode(cursor, qh)
            decision_at = decoded.decision_at
            after = (decoded.after_time, UUID(decoded.after_id))
        with self.engine.connect() as conn:
            items = eligible_options(conn, [contract_id], start=start, end=end,
                                     decision_at=decision_at, provider_id=provider_id,
                                     after=after, limit=limit + 1)
        next_cursor = None
        if len(items) > limit:
            items = items[:limit]
            last = items[-1]
            next_cursor = Cursor(decision_at, last.observed_at, str(last.id), qh).encode()
        return Page(items, next_cursor, decision_at)

    # -- Panel ---------------------------------------------------------------------------

    def panel(self, *, instrument_id: UUID, provider_id: str, timeframe: Timeframe,
              start: datetime, end: datetime, decision_at: datetime,
              contract_id: UUID | None = None, option_provider_id: str | None = None,
              age_limits: AgeLimits, allow_mixed_sources: bool = False,
              price_basis: PriceBasis = PriceBasis.PROVIDER_NATIVE,
              mode: AvailabilityMode = AvailabilityMode.OBSERVED) -> PanelResult:
        warnings: list[str] = []
        option_provider = option_provider_id or provider_id
        if option_provider != provider_id:
            if not allow_mixed_sources:
                raise IncompatibleQuery(
                    f"spot source {provider_id} and option source {option_provider} differ; "
                    "specify a source policy that allows a descriptive mixed panel")
            warnings.append("mixed sources: descriptive only, timestamps not comparable")
        bars = self.bars(instrument_id=instrument_id, provider_id=provider_id,
                         timeframe=timeframe, start=start, end=end, decision_at=decision_at,
                         price_basis=price_basis, mode=mode).items
        spot_key = self.bar_key(instrument_id, provider_id, Timeframe.M1, price_basis)
        with self.engine.connect() as conn:
            points = sample_panel(conn, bars=bars, contract_id=contract_id, spot_key=spot_key,
                                  session_of=self._session_date, decision_cap=decision_at,
                                  limits=age_limits, provider_id=option_provider)
        if contract_id is not None:
            warnings.append("option samples are periodic polls; no option candles are implied")
        if any(p.option is not None and p.freshness == "unknown" for p in points):
            warnings.append("quote event time unknown: freshness unknown, not execution-grade")
        if timeframe is Timeframe.H4:
            warnings.append("4h buckets are session-anchored: 240m then a shorter remainder")
        return PanelResult(points, decision_at, availability_label(mode), warnings)

    def _session_date(self, at: datetime) -> date | None:
        session = self.calendar.session_at(at)
        if session is not None:
            return session.session_date
        # A post-close fetch still belongs to that day's session for carry rules.
        local = self.calendar.local_date(at)
        same_day = self.calendar.session(local)
        return same_day.session_date if same_day is not None else None

    # -- Quality --------------------------------------------------------------------

    def quality(self, *, provider_id: str | None = None) -> dict[str, Any]:
        with self.engine.connect() as conn:
            gaps = [dict(r) for r in conn.execute(text("""
                SELECT kind, status, count(*) AS n, min(start_at) AS first, max(end_at) AS last
                FROM data_gaps WHERE (CAST(:p AS text) IS NULL OR provider_id = :p)
                  AND status IN ('open','retrying','unrecoverable')
                GROUP BY kind, status ORDER BY kind, status"""), {"p": provider_id}).mappings()]
            issues = [dict(r) for r in conn.execute(text("""
                SELECT code, severity, count(*) AS n FROM quality_issues
                WHERE resolved_at IS NULL GROUP BY code, severity ORDER BY code""")).mappings()]
            last_success = [dict(r) for r in conn.execute(text("""
                SELECT x.provider_id, x.scope->>'symbol' AS symbol,
                       x.scope->>'expiration' AS expiration, x.scope->>'kind' AS scope,
                       max(x.fetch_completed_at) AS last_fetch, max(x.known_at) AS last_known
                FROM observation_batches x
                WHERE x.status='published' AND x.batch_kind IN ('chain','bars')
                  AND (CAST(:p AS text) IS NULL OR x.provider_id = :p)
                GROUP BY 1, 2, 3, 4 ORDER BY 1, 2, 3, 4"""), {"p": provider_id}).mappings()]
            incidents = [dict(r) for r in conn.execute(text("""
                SELECT kind, severity, count(*) AS n FROM operational_incidents
                WHERE resolved_at IS NULL GROUP BY kind, severity""")).mappings()]
        return {"gaps": gaps, "quality_issues": issues, "last_success": last_success,
                "incidents": incidents}


def contract_ids(infos: Sequence[ContractInfo]) -> list[UUID]:
    return [c.id for c in infos]
