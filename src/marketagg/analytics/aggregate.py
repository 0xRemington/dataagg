"""Deterministic 5m/15m/session-4h aggregation from canonical 1m (T09).

``open=first, high=max, low=min, close=last, volume=sum``. A bucket is complete
only when every scheduled constituent minute is present (no synthesized minutes).
Partial buckets are published with ``complete=false`` for explicit display only.
A bucket is never published before its interval end, even if constituent data
somehow exist earlier. Corrections produce new revisions; old as-of results stay
unchanged because they select by knowledge time.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import Engine, text

from marketagg.analytics.asof import BarKey, BarRecord, eligible_bars
from marketagg.calendar import Bucket, SessionCalendar
from marketagg.db.ingest import BarWrite, IngestRepository
from marketagg.domain.enums import PriceBasis, SessionKind, Timeframe
from marketagg.domain.identifiers import InstrumentRef
from marketagg.domain.observations import BarObservation
from marketagg.domain.policies import PolicyVersion
from marketagg.domain.primitives import canonical_dumps

DERIVED_TIMEFRAMES = (Timeframe.M5, Timeframe.M15, Timeframe.H4)


def aggregation_policy(calendar_policy_id: str, code_sha: str = "aggregate-v1") -> PolicyVersion:
    return PolicyVersion.create("aggregation", {
        "version": 1,
        "rules": "open=first,high=max,low=min,close=last,volume=sum",
        "completeness": "all_scheduled_constituents",
        "four_hour": "session_open_anchored_with_remainder",
        "volume_nulls": "any_null_constituent_gives_null_volume",
        "calendar_policy": calendar_policy_id,
    }, code_sha)


@dataclass(frozen=True)
class DerivedBucket:
    bucket: Bucket
    write: BarWrite | None  # None when no constituent exists (nothing to publish)


def aggregate_bucket(bucket: Bucket, minutes: Sequence[BarRecord],
                     instrument: InstrumentRef, price_basis: PriceBasis) -> BarWrite | None:
    """Pure aggregation of eligible 1m records inside one bucket."""
    inside = sorted((m for m in minutes if bucket.start <= m.interval_start < bucket.end),
                    key=lambda m: m.interval_start)
    if not inside:
        return None
    starts = [m.interval_start for m in inside]
    if len(starts) != len(set(starts)):
        raise ValueError("duplicate minutes passed to aggregation (select revisions first)")
    volumes = [m.volume for m in inside]
    volume = None if any(v is None for v in volumes) else sum(v for v in volumes if v is not None)
    observation = BarObservation(
        instrument=instrument, timeframe=bucket.timeframe, interval_start=bucket.start,
        interval_end=bucket.end, session_date=bucket.session_date,
        session_kind=SessionKind.RTH, price_basis=price_basis,
        open=inside[0].open, high=max(m.high for m in inside), low=min(m.low for m in inside),
        close=inside[-1].close, volume=volume,
    )
    refs = tuple({"dataset": "bar_observations", "id": str(m.id),
                  "partition_time": m.interval_start.isoformat(),
                  "content_sha256": m.content_sha256} for m in inside)
    return BarWrite(
        observation=observation, complete=len(inside) == bucket.expected_minutes,
        expected_minutes=bucket.expected_minutes, observed_minutes=len(inside),
        is_short_session_bucket=bucket.is_short_session_bucket, input_refs=refs,
    )


def buckets_touching(calendar: SessionCalendar, minute_starts: Iterable[datetime]
                     ) -> set[tuple[Timeframe, datetime, datetime]]:
    out: set[tuple[Timeframe, datetime, datetime]] = set()
    for minute in minute_starts:
        for timeframe in DERIVED_TIMEFRAMES:
            bucket = calendar.bucket_for(minute, timeframe)
            if bucket is not None:
                out.add((timeframe, bucket.start, bucket.end))
    return out


def dirty_rows(calendar: SessionCalendar, minute_starts: Iterable[datetime], *,
               instrument_id: UUID, provider_id: str, price_basis: PriceBasis,
               source_policy_id: str) -> list[dict[str, Any]]:
    return [
        {"i": instrument_id, "p": provider_id, "tf": tf.value, "s": start, "e": end,
         "pb": price_basis.value, "sk": SessionKind.RTH.value, "pol": source_policy_id}
        for tf, start, end in sorted(buckets_touching(calendar, minute_starts),
                                     key=lambda t: (t[1], t[0].value))
    ]


@dataclass(frozen=True)
class DeriveSummary:
    processed_windows: int
    written: int
    unchanged: int
    deferred: int


def derive_pending(
    engine: Engine,
    repo: IngestRepository,
    calendar: SessionCalendar,
    *,
    aggregation_policy_id: str,
    now: datetime,
    instruments: dict[UUID, str],
    limit: int = 500,
) -> DeriveSummary:
    """Process dirty windows whose bucket has closed; publish changed derived bars."""
    with engine.connect() as conn:
        pending = conn.execute(text("""
            SELECT * FROM derived_dirty_windows
            WHERE processed_at IS NULL
            ORDER BY bucket_end, instrument_id, timeframe LIMIT :limit
        """), {"limit": limit}).mappings().all()
    written = unchanged = processed = deferred = 0
    groups: dict[tuple[Any, ...], list[Any]] = {}
    for row in pending:
        if row["bucket_end"] > now:
            deferred += 1  # never publish a bucket before its interval end
            continue
        groups.setdefault((row["instrument_id"], row["provider_id"], row["price_basis"],
                           row["source_policy_id"]), []).append(row)
    for (instrument_id, provider_id, basis, source_policy), rows in groups.items():
        key = BarKey(instrument_id, provider_id, Timeframe.M1, PriceBasis(basis),
                     SessionKind.RTH, source_policy)
        writes: list[BarWrite] = []
        identity: list[str] = []
        ref = InstrumentRef(provider_id=provider_id, symbol=instruments[instrument_id])
        with engine.connect() as conn:
            for row in rows:
                minutes = eligible_bars(conn, key, start=row["bucket_start"],
                                        end=row["bucket_end"], decision_at=now)
                session = calendar.session_at(row["bucket_start"])
                if session is None:
                    continue
                bucket = next(b for b in calendar.buckets(session, Timeframe(row["timeframe"]))
                              if b.start == row["bucket_start"])
                write = aggregate_bucket(bucket, minutes, ref, PriceBasis(basis))
                if write is not None:
                    writes.append(write)
                    identity.append(write.content_sha256())
        if writes:
            response_identity = "derived:" + hashlib.sha256(canonical_dumps({
                "policy": aggregation_policy_id, "instrument": str(instrument_id),
                "source_policy": source_policy, "contents": sorted(identity),
            }).encode()).hexdigest()
            outcome = repo.stage_derived_bars(
                provider_id=provider_id, instrument_id=instrument_id,
                policy_id=aggregation_policy_id, response_identity=response_identity,
                computed_from=min(w.observation.interval_end for w in writes),
                computed_at=now, scope={"kind": "derived", "source_policy": source_policy},
                writes=writes)
            if outcome.created:
                repo.publish(outcome.batch_id)
            written += outcome.rows_written
            unchanged += outcome.rows_unchanged
        with engine.begin() as conn:
            for row in rows:
                conn.execute(text("""
                    UPDATE derived_dirty_windows SET processed_at = :now
                    WHERE instrument_id=:i AND provider_id=:p AND timeframe=:tf
                      AND bucket_start=:s AND price_basis=:pb AND session_kind=:sk
                      AND source_policy_id=:pol AND dirtied_at <= :dirtied
                """), {"now": now, "i": row["instrument_id"], "p": row["provider_id"],
                       "tf": row["timeframe"], "s": row["bucket_start"],
                       "pb": row["price_basis"], "sk": row["session_kind"],
                       "pol": row["source_policy_id"], "dirtied": row["dirtied_at"]})
                processed += 1
    return DeriveSummary(processed, written, unchanged, deferred)


def reconcile_daily(daily: BarRecord, minutes: Sequence[BarRecord], *,
                    price_abs: Decimal, price_rel: float, volume_rel: float
                    ) -> dict[str, Any]:
    """Compare a provider daily bar with locally aggregated RTH minutes (report only)."""
    if daily.price_basis != (minutes[0].price_basis if minutes else daily.price_basis):
        return {"comparable": False, "reason": "price basis differs"}
    if not minutes:
        return {"comparable": False, "reason": "no eligible minutes"}
    agg = {
        "open": minutes[0].open, "high": max(m.high for m in minutes),
        "low": min(m.low for m in minutes), "close": minutes[-1].close,
    }
    tolerance = max(price_abs, daily.close * Decimal(str(price_rel)))
    diffs = {k: getattr(daily, k) - v for k, v in agg.items()
             if abs(getattr(daily, k) - v) > tolerance}
    report: dict[str, Any] = {"comparable": True, "price_differences": {
        k: str(v) for k, v in diffs.items()}, "minutes": len(minutes)}
    volumes = [m.volume for m in minutes]
    if daily.volume and all(v is not None for v in volumes):
        total = sum(v for v in volumes if v is not None)
        rel = abs(daily.volume - total) / daily.volume
        report["volume_relative_difference"] = round(rel, 6)
        report["volume_warning"] = rel > volume_rel
    return report
