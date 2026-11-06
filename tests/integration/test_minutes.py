from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import Engine, text
from testkit import BAR_POLICY, count, ingest_minutes, m1_key, make_world

from marketagg.analytics.asof import eligible_bars
from marketagg.calendar import SessionCalendar
from marketagg.collectors.bars import closed_writes
from marketagg.collectors.recovery import (
    confirm_halt,
    invalidate_minute,
    repair_gaps,
    scan_minute_gaps,
)
from marketagg.providers.fake import BarRevision, FakeClock, FakeProvider, FakeScenario

CAL = SessionCalendar()
OPEN = datetime(2025, 3, 7, 14, 30, tzinfo=UTC)
GAP = datetime(2025, 3, 7, 14, 37, tzinfo=UTC)


def test_only_closed_in_session_bars_are_persisted(engine: Engine) -> None:
    world = make_world(engine, start=datetime(2025, 3, 7, 14, 41, 10, tzinfo=UTC))
    result = world.provider.fetch_bars(world.bar_request(OPEN - timedelta(minutes=30),
                                                         OPEN + timedelta(minutes=20)))
    writes, dropped = closed_writes(result, safe_lag=timedelta(seconds=80), calendar=CAL)
    # fetch started 14:41:10: the 14:40 bar (end 14:41) needs 14:41:20 -> dropped
    assert writes[-1].observation.interval_start == datetime(2025, 3, 7, 14, 39, tzinfo=UTC)
    assert dropped == 1
    assert all(CAL.is_open(w.observation.interval_start) for w in writes)


def test_overlap_refetch_and_disorder_are_idempotent(engine: Engine) -> None:
    world = make_world(engine, start=datetime(2025, 3, 7, 15, 0, 30, tzinfo=UTC))
    first = ingest_minutes(world, OPEN, OPEN + timedelta(minutes=30))
    world.clock.advance(timedelta(minutes=1))
    second = ingest_minutes(world, OPEN + timedelta(minutes=10), OPEN + timedelta(minutes=31))
    # 14:30..14:59 closed at 15:00:30; the second window adds only the 15:00 minute.
    assert first["written"] == 30 and second["unchanged"] == 20 and second["written"] == 1
    result = world.provider.fetch_bars(world.bar_request(OPEN, OPEN + timedelta(minutes=5)))
    shuffled = result.model_copy(update={"rows": tuple(reversed(result.rows)) + result.rows[:1],
                                         "response_id": uuid4()})
    outcome = world.repo.stage_bars(shuffled, instrument_id=world.instruments["SYNA"],
                                    policy_id=BAR_POLICY.id, scope={})
    assert outcome.rows_written == 0 and outcome.rows_unchanged == 6


def test_revised_bar_appends_revision_and_dirties_buckets(engine: Engine) -> None:
    target = OPEN + timedelta(minutes=3)
    scenario = FakeScenario(bar_revisions=(BarRevision(target, 2, Decimal("0.11")),))
    world = make_world(engine, scenario, start=datetime(2025, 3, 7, 15, 0, 30, tzinfo=UTC))
    ingest_minutes(world, OPEN, OPEN + timedelta(minutes=20))
    with engine.begin() as conn:
        conn.execute(text("UPDATE derived_dirty_windows SET processed_at = :t"),
                     {"t": world.clock.now()})
    stats = ingest_minutes(world, OPEN, OPEN + timedelta(minutes=20))
    assert stats["written"] == 1
    with engine.connect() as conn:
        dirty = conn.execute(text("SELECT timeframe, bucket_start FROM derived_dirty_windows "
                                  "WHERE processed_at IS NULL ORDER BY timeframe")).all()
    assert {(r.timeframe, r.bucket_start) for r in dirty} == {
        ("15m", OPEN), ("4h", OPEN), ("5m", OPEN)}


def test_missing_minute_is_detected_never_synthesized_and_repaired(engine: Engine) -> None:
    now = datetime(2025, 3, 7, 15, 0, 30, tzinfo=UTC)
    world = make_world(engine, FakeScenario(omitted_minutes=frozenset({GAP})), start=now)
    ingest_minutes(world, OPEN, OPEN + timedelta(minutes=30))
    key = m1_key(world)
    now = world.clock.now()  # after publication (fetch latency moved the clock)
    result = scan_minute_gaps(engine, CAL, key=key, start=OPEN, now=now,
                              grace=timedelta(minutes=3), horizon=timedelta(days=7),
                              horizon_source="test")
    # minutes 14:30..14:57 are past grace at ~15:00:31; one of them is missing
    assert (result.expected, result.present, result.opened) == (28, 27, 1)
    with engine.connect() as conn:
        gap = conn.execute(text("SELECT * FROM data_gaps")).mappings().one()
    assert (gap["start_at"], gap["end_at"], gap["status"]) == (GAP, GAP + timedelta(minutes=1),
                                                               "open")
    assert count(engine, "SELECT count(*) FROM bar_observations "
                         "WHERE interval_start = :g", g=GAP) == 0
    # Re-scanning does not duplicate the gap.
    scan_minute_gaps(engine, CAL, key=key, start=OPEN, now=now, grace=timedelta(minutes=3),
                     horizon=timedelta(days=7), horizon_source="test")
    assert count(engine, "SELECT count(*) FROM data_gaps") == 1

    # Source still lacks the minute: repair fails, escalates after max cycles.
    for _ in range(3):
        stats = repair_gaps(engine=engine, provider=world.provider, repo=world.repo,
                            calendar=CAL, key=key, symbol="SYNA", now=now, max_cycles=3)
    assert stats["escalated"] == 1
    # The source later serves it (e.g. a late correction): the gap becomes repaired.
    healed = FakeProvider(FakeScenario(), FakeClock(now))
    stats = repair_gaps(engine=engine, provider=healed, repo=world.repo, calendar=CAL,
                        key=key, symbol="SYNA", now=now, max_cycles=3)
    assert stats["repaired"] == 1
    assert count(engine, "SELECT count(*) FROM data_gaps WHERE status='repaired'") == 1


def test_gap_beyond_source_horizon_is_unrecoverable(engine: Engine) -> None:
    later = datetime(2025, 3, 20, 15, 0, tzinfo=UTC)
    world = make_world(engine, FakeScenario(omitted_minutes=frozenset({GAP})),
                       start=datetime(2025, 3, 7, 15, 0, 30, tzinfo=UTC))
    ingest_minutes(world, OPEN, OPEN + timedelta(minutes=30))
    key = m1_key(world)
    scan_minute_gaps(engine, CAL, key=key, start=OPEN, now=world.clock.now(),
                     grace=timedelta(minutes=3), horizon=timedelta(days=7),
                     horizon_source="assumed")
    stats = repair_gaps(engine=engine, provider=world.provider, repo=world.repo, calendar=CAL,
                        key=key, symbol="SYNA", now=later, max_cycles=3)
    assert stats == {"attempted": 0, "repaired": 0, "unrecoverable": 1, "escalated": 0}


def test_halt_requires_authoritative_evidence(engine: Engine) -> None:
    world = make_world(engine, FakeScenario(omitted_minutes=frozenset({GAP})),
                       start=datetime(2025, 3, 7, 15, 0, 30, tzinfo=UTC))
    ingest_minutes(world, OPEN, OPEN + timedelta(minutes=30))
    scan_minute_gaps(engine, CAL, key=m1_key(world), start=OPEN, now=world.clock.now(),
                     grace=timedelta(minutes=3), horizon=timedelta(days=7),
                     horizon_source="test")
    with engine.connect() as conn:
        gap_id = conn.execute(text("SELECT id FROM data_gaps")).scalar_one()
    with pytest.raises(ValueError, match="authoritative"):
        confirm_halt(engine, gap_id, {"note": "no trades seen"}, world.clock.now())
    confirm_halt(engine, gap_id, {"source": "exchange notice", "reference": "TEST-1"},
                 world.clock.now())
    assert count(engine, "SELECT count(*) FROM data_gaps WHERE status='halt_confirmed'") == 1


def test_invalidation_hides_minute_without_reviving_predecessor(engine: Engine) -> None:
    world = make_world(engine, start=datetime(2025, 3, 7, 15, 0, 30, tzinfo=UTC))
    ingest_minutes(world, OPEN, OPEN + timedelta(minutes=10))
    key = m1_key(world)
    before = world.clock.now()
    with engine.connect() as conn:
        target = eligible_bars(conn, key, start=OPEN, end=OPEN + timedelta(minutes=10),
                               decision_at=before)[2]
    world.clock.advance(timedelta(minutes=5))
    with engine.begin() as conn:
        conn.execute(text("UPDATE derived_dirty_windows SET processed_at = :t"),
                     {"t": world.clock.now()})
    invalidate_minute(world.repo, CAL, bar_id=target.id, interval_start=target.interval_start,
                      key=key, reason="test")
    with engine.connect() as conn:
        old_view = eligible_bars(conn, key, start=OPEN, end=OPEN + timedelta(minutes=10),
                                 decision_at=before)
        new_view = eligible_bars(conn, key, start=OPEN, end=OPEN + timedelta(minutes=10),
                                 decision_at=world.clock.now(), include_partial=True)
    assert target.interval_start in {b.interval_start for b in old_view}
    assert target.interval_start not in {b.interval_start for b in new_view}
    assert len(new_view) == 9
    assert count(engine, "SELECT count(*) FROM derived_dirty_windows "
                         "WHERE processed_at IS NULL") == 3  # its 5m, 15m and 4h buckets


def test_weekend_scan_creates_no_gaps(engine: Engine) -> None:
    world = make_world(engine, start=datetime(2025, 3, 9, 15, 0, tzinfo=UTC))
    result = scan_minute_gaps(engine, CAL, key=m1_key(world),
                              start=datetime(2025, 3, 8, tzinfo=UTC), now=world.clock.now(),
                              grace=timedelta(minutes=3), horizon=timedelta(days=7),
                              horizon_source="test")
    assert result.expected == 0 and result.opened == 0
