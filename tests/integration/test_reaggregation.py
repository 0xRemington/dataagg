from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

from sqlalchemy import Engine
from testkit import World, aggregation_policy_id, ingest_minutes, m1_key, make_world

from marketagg.analytics.aggregate import derive_pending, dirty_rows
from marketagg.analytics.asof import BarKey, eligible_bars
from marketagg.calendar import SessionCalendar
from marketagg.domain import PriceBasis, SessionKind, Timeframe
from marketagg.providers.fake import BarRevision, FakeScenario

CAL = SessionCalendar()


def _session(world: World, day: date) -> tuple[datetime, datetime]:
    session = CAL.session(day)
    assert session is not None
    ingest_minutes(world, session.open_at, session.close_at)
    return session.open_at, session.close_at


def _derive(world: World, policy: str) -> object:
    return derive_pending(world.engine, world.repo, CAL, aggregation_policy_id=policy,
                          now=world.clock.now(),
                          instruments={v: k for k, v in world.instruments.items()})


def _derived(world: World, policy: str, timeframe: Timeframe, start: datetime,
             end: datetime, decision: datetime, partial: bool = False) -> list:
    key = BarKey(world.instruments["SYNA"], "fake", timeframe, PriceBasis.PROVIDER_NATIVE,
                 SessionKind.RTH, policy)
    with world.engine.connect() as conn:
        return eligible_bars(conn, key, start=start, end=end, decision_at=decision,
                             include_partial=partial)


def test_full_session_derives_expected_buckets(engine: Engine) -> None:
    world = make_world(engine, start=datetime(2025, 3, 7, 21, 5, tzinfo=UTC))
    policy = aggregation_policy_id(world)
    open_at, close_at = _session(world, date(2025, 3, 7))
    summary = _derive(world, policy)
    assert summary.deferred == 0  # type: ignore[attr-defined]
    now = world.clock.now()
    m5 = _derived(world, policy, Timeframe.M5, open_at, close_at, now)
    m15 = _derived(world, policy, Timeframe.M15, open_at, close_at, now)
    h4 = _derived(world, policy, Timeframe.H4, open_at, close_at, now)
    assert (len(m5), len(m15), len(h4)) == (78, 26, 2)
    assert [(b.expected_minutes, b.is_short_session_bucket) for b in h4] == [(240, False),
                                                                          (150, True)]
    with engine.connect() as conn:
        minutes = eligible_bars(conn, m1_key(world), start=open_at, end=open_at + timedelta(
            minutes=5), decision_at=now)
    assert m5[0].open == minutes[0].open and m5[0].close == minutes[-1].close
    assert m5[0].high == max(m.high for m in minutes)
    assert m5[0].volume == sum(m.volume or 0 for m in minutes)
    # Re-running derivation over unchanged inputs publishes nothing new.
    assert _derive(world, policy).written == 0  # type: ignore[attr-defined]


def test_early_close_has_single_210_minute_bucket(engine: Engine) -> None:
    world = make_world(engine, start=datetime(2025, 11, 28, 18, 30, tzinfo=UTC))
    policy = aggregation_policy_id(world)
    open_at, close_at = _session(world, date(2025, 11, 28))
    _derive(world, policy)
    now = world.clock.now()
    assert len(_derived(world, policy, Timeframe.M5, open_at, close_at, now)) == 42
    assert len(_derived(world, policy, Timeframe.M15, open_at, close_at, now)) == 14
    h4 = _derived(world, policy, Timeframe.H4, open_at, close_at, now)
    assert [(b.expected_minutes, b.observed_minutes, b.complete) for b in h4] == [(210, 210,
                                                                                   True)]


def test_late_correction_creates_new_version_old_asof_unchanged(engine: Engine) -> None:
    target = datetime(2025, 3, 7, 14, 33, tzinfo=UTC)
    scenario = FakeScenario(bar_revisions=(BarRevision(target, 2, Decimal("0.40")),))
    world = make_world(engine, scenario, start=datetime(2025, 3, 7, 21, 5, tzinfo=UTC))
    policy = aggregation_policy_id(world)
    open_at, _ = _session(world, date(2025, 3, 7))
    _derive(world, policy)
    original_view = world.clock.now()
    world.clock.advance(timedelta(minutes=30))
    _session(world, date(2025, 3, 7))  # correction sweep finds the revised minute
    summary = _derive(world, policy)
    assert summary.written == 3  # type: ignore[attr-defined]  # its 5m, 15m and 4h buckets
    old = _derived(world, policy, Timeframe.M5, open_at, open_at + timedelta(minutes=5),
                   original_view)
    new = _derived(world, policy, Timeframe.M5, open_at, open_at + timedelta(minutes=5),
                   world.clock.now())
    assert new[0].id != old[0].id and new[0].content_sha256 != old[0].content_sha256
    assert old[0].known_at <= original_view < new[0].known_at


def test_missing_minute_bucket_excluded_from_research_but_displayable(engine: Engine) -> None:
    gap = datetime(2025, 3, 7, 14, 37, tzinfo=UTC)
    world = make_world(engine, FakeScenario(omitted_minutes=frozenset({gap})),
                       start=datetime(2025, 3, 7, 21, 5, tzinfo=UTC))
    policy = aggregation_policy_id(world)
    open_at, close_at = _session(world, date(2025, 3, 7))
    _derive(world, policy)
    now = world.clock.now()
    research = _derived(world, policy, Timeframe.M5, open_at, close_at, now)
    display = _derived(world, policy, Timeframe.M5, open_at, close_at, now, partial=True)
    assert len(research) == 77 and len(display) == 78
    partial = next(b for b in display if not b.complete)
    assert (partial.interval_start, partial.observed_minutes) == (
        datetime(2025, 3, 7, 14, 35, tzinfo=UTC), 4)


def test_bucket_never_published_before_interval_end(engine: Engine) -> None:
    # Malformed situation: constituents are "known" while the 15m bucket is still open.
    world = make_world(engine, start=datetime(2025, 3, 7, 14, 41, tzinfo=UTC))
    policy = aggregation_policy_id(world)
    ingest_minutes(world, datetime(2025, 3, 7, 14, 30, tzinfo=UTC),
                   datetime(2025, 3, 7, 14, 40, tzinfo=UTC))
    world.repo.mark_dirty(dirty_rows(CAL, [datetime(2025, 3, 7, 14, 30, tzinfo=UTC)],
                                     instrument_id=world.instruments["SYNA"],
                                     provider_id="fake", price_basis=PriceBasis.PROVIDER_NATIVE,
                                     source_policy_id=m1_key(world).policy_id))
    summary = _derive(world, policy)
    assert summary.deferred >= 2  # type: ignore[attr-defined]  # 15m and 4h still open
    m15 = _derived(world, policy, Timeframe.M15, datetime(2025, 3, 7, 14, 30, tzinfo=UTC),
                   datetime(2025, 3, 7, 15, 0, tzinfo=UTC), datetime(2030, 1, 1, tzinfo=UTC),
                   partial=True)
    assert m15 == []
