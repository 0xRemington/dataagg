from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

from sqlalchemy import Engine, text
from testkit import BAR_POLICY, World, count, make_world

from marketagg.analytics.asof import BarKey, actions_asof, eligible_bars
from marketagg.calendar import SessionCalendar
from marketagg.collectors.bars import annual_chunks, backfill_daily, cursor_key, read_cursor
from marketagg.domain import PriceBasis, SessionKind, Timeframe
from marketagg.providers.fake import ActionSpec, FakeScenario

AFTER_CLOSE = datetime(2025, 3, 10, 22, 0, tzinfo=UTC)
CAL = SessionCalendar()


def _backfill(world: World, years: int = 1, max_chunks: int | None = None) -> dict:
    return backfill_daily(provider=world.provider, repo=world.repo, calendar=CAL,
                          clock=world.clock, symbol="SYNA",
                          instrument_id=world.instruments["SYNA"], policy_id=BAR_POLICY.id,
                          years=years, overlap_days=5, max_chunks=max_chunks)


def _key(world: World) -> BarKey:
    return BarKey(world.instruments["SYNA"], "fake", Timeframe.D1, PriceBasis.PROVIDER_NATIVE,
                  SessionKind.RTH, BAR_POLICY.id)


def test_annual_chunks_are_half_open_with_overlap() -> None:
    chunks = annual_chunks(date(2021, 3, 10), date(2025, 3, 11), overlap_days=5)
    assert chunks[0] == (date(2021, 3, 10), date(2022, 3, 10))
    assert chunks[1][0] == date(2022, 3, 5) and chunks[-1][1] == date(2025, 3, 11)


def test_repeated_backfill_does_not_duplicate_unchanged_bars(engine: Engine) -> None:
    world = make_world(engine, start=AFTER_CLOSE)
    first = _backfill(world)
    assert first["written"] > 240
    world.repo.engine.dispose()
    rows = count(engine, "SELECT count(*) FROM bar_observations WHERE timeframe='1d'")
    # Overlapping chunk boundaries and a full re-run append nothing new.
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM backfill_cursors"))
    second = _backfill(world)
    assert second["written"] == 0 and second["unchanged"] > 0
    assert count(engine, "SELECT count(*) FROM bar_observations WHERE timeframe='1d'") == rows
    dupes = count(engine, """SELECT count(*) FROM (SELECT interval_start FROM bar_observations
                             GROUP BY interval_start HAVING count(*) > 1) d""")
    assert dupes == 0


def test_crash_resume_continues_from_durable_cursor(engine: Engine) -> None:
    world = make_world(engine, start=datetime(2025, 3, 10, 22, 0, tzinfo=UTC))
    partial = _backfill(world, years=3, max_chunks=1)
    assert partial["chunks"] == 1
    cursor = read_cursor(engine, cursor_key("fake", "SYNA", BAR_POLICY.id))
    assert cursor is not None
    resumed = _backfill(world, years=3)
    assert resumed["resumed_from"] == cursor["next_start"]
    assert resumed["chunks"] == 3  # remaining chunks only
    sessions = len(CAL.sessions(date(2022, 3, 10), date(2025, 3, 10)))
    assert count(engine, "SELECT count(DISTINCT interval_start) FROM bar_observations") == sessions


def test_backfilled_history_is_not_observed_by_old_decisions(engine: Engine) -> None:
    world = make_world(engine, start=AFTER_CLOSE)
    _backfill(world)
    with engine.connect() as conn:
        last_year = eligible_bars(conn, _key(world), start=datetime(2024, 3, 1, tzinfo=UTC),
                                  end=datetime(2024, 7, 1, tzinfo=UTC),
                                  decision_at=datetime(2024, 7, 1, tzinfo=UTC))
        today = eligible_bars(conn, _key(world), start=datetime(2024, 3, 1, tzinfo=UTC),
                              end=datetime(2024, 7, 1, tzinfo=UTC),
                              decision_at=world.clock.now())
    assert last_year == [] and len(today) > 70


def test_split_known_later_flags_basis_and_actions_are_bitemporal(engine: Engine) -> None:
    announce = AFTER_CLOSE + timedelta(days=1)
    scenario = FakeScenario(actions=(
        ActionSpec("SYNA", "split", date(2024, 6, 10), AFTER_CLOSE, ratio=Decimal(2)),
        ActionSpec("SYNA", "dividend", date(2024, 12, 20), announce,
                   cash_amount=Decimal("1.25")),
    ))
    world = make_world(engine, scenario, start=AFTER_CLOSE)
    first = _backfill(world)
    assert first["actions_written"] == 1  # dividend not yet reported
    world.clock.set(announce + timedelta(hours=1))
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM backfill_cursors"))
    _backfill(world)
    with engine.connect() as conn:
        before = actions_asof(conn, world.instruments["SYNA"], decision_at=announce,
                              provider_id="fake")
        after = actions_asof(conn, world.instruments["SYNA"], decision_at=world.clock.now(),
                             provider_id="fake")
        flagged = conn.execute(text("""SELECT count(*) FROM bar_observations
            WHERE 'basis_unverified' = ANY(quality_flags)""")).scalar_one()
    assert [a["action_type"] for a in before] == ["split"]
    assert [a["action_type"] for a in after] == ["split", "dividend"]
    # Once a split is known, newly fetched provider-native history is labelled unverified.
    assert flagged > 0


def test_empty_history_does_not_imply_delisting(engine: Engine) -> None:
    world = make_world(engine, FakeScenario(symbols=("SYNB",)), start=AFTER_CLOSE)
    totals = _backfill(world)
    assert totals["written"] == 0
    with engine.connect() as conn:
        statuses = conn.execute(text("SELECT status FROM instrument_identifiers "
                                     "WHERE symbol='SYNA'")).scalars().all()
    assert statuses == ["active"]
