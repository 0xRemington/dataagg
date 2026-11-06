from __future__ import annotations

import threading
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import Engine, text
from testkit import World, advance, count, make_world

from marketagg.db.errors import ClockSkewError
from marketagg.db.ingest import IngestRepository


@pytest.fixture
def world(engine: Engine) -> World:
    return make_world(engine)


def _batch(engine: Engine, batch_id: object) -> tuple[str, datetime | None, datetime]:
    with engine.connect() as conn:
        row = conn.execute(text(
            "SELECT status, known_at, recorded_at FROM observation_batches WHERE id=:id"),
            {"id": batch_id}).one()
    return row.status, row.known_at, row.recorded_at


def test_staged_batches_have_no_knowledge_time_until_published(world: World) -> None:
    result = world.provider.fetch_chain(world.chain_request())
    staged = world.repo.stage_chain(result, instrument_id=world.instruments["SYNA"], scope={})
    status, known, recorded = _batch(world.engine, staged.batch_id)
    assert status == "staged" and known is None
    advance(world, 3)  # durable acknowledgement happens after the commit
    known_at = world.repo.publish(staged.batch_id)
    assert known_at > recorded and known_at >= result.fetch_completed_at
    assert known_at == world.clock.now()
    assert world.repo.publish(staged.batch_id) == known_at  # idempotent


def test_crash_between_stage_and_publish_uses_recovery_time(world: World) -> None:
    result = world.provider.fetch_chain(world.chain_request())
    staged = world.repo.stage_chain(result, instrument_id=world.instruments["SYNA"], scope={})
    # The worker dies here. Recovery runs later on a fresh repository instance.
    advance(world, 600)
    recovered = IngestRepository(world.engine, clock=world.clock).finalize_staged()
    assert recovered == [staged.batch_id]
    _, known, recorded = _batch(world.engine, staged.batch_id)
    assert known == world.clock.now() and known - recorded >= timedelta(seconds=600)


def test_concurrent_staging_of_one_response_creates_one_batch(world: World) -> None:
    result = world.provider.fetch_chain(world.chain_request())
    outcomes: list[object] = []
    errors: list[BaseException] = []
    barrier = threading.Barrier(4)

    def stage() -> None:
        try:
            barrier.wait()
            outcomes.append(world.repo.stage_chain(
                result, instrument_id=world.instruments["SYNA"], scope={}))
        except BaseException as exc:  # pragma: no cover - surfaced below
            errors.append(exc)

    threads = [threading.Thread(target=stage) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    assert sum(o.created for o in outcomes) == 1  # type: ignore[attr-defined]
    assert count(world.engine, "SELECT count(*) FROM observation_batches") == 1
    assert count(world.engine, "SELECT count(*) FROM option_snapshots") == 24
    assert count(world.engine, "SELECT count(*) FROM option_contracts") == 24


class _SkewedSystemClock:
    is_system = True

    def __init__(self, skew: timedelta) -> None:
        self.skew = skew

    def now(self) -> datetime:
        return datetime.now(UTC) + self.skew


def test_clock_skew_suspends_publication_and_opens_incident(world: World) -> None:
    result = world.provider.fetch_chain(world.chain_request())
    staged = world.repo.stage_chain(result, instrument_id=world.instruments["SYNA"], scope={})
    skewed = IngestRepository(world.engine, clock=_SkewedSystemClock(timedelta(seconds=30)))
    with pytest.raises(ClockSkewError):
        skewed.publish(staged.batch_id)
    assert _batch(world.engine, staged.batch_id)[0] == "staged"
    assert count(world.engine, "SELECT count(*) FROM operational_incidents "
                               "WHERE kind='clock_skew'") == 1
    healthy = IngestRepository(world.engine, clock=_SkewedSystemClock(timedelta(0)))
    assert healthy.publish(staged.batch_id) is not None


def test_publish_cannot_backdate_before_recorded_at(world: World) -> None:
    result = world.provider.fetch_chain(world.chain_request())
    staged = world.repo.stage_chain(result, instrument_id=world.instruments["SYNA"], scope={})
    with pytest.raises(Exception, match="precedes"), world.engine.begin() as conn:
        conn.execute(text("""
            UPDATE observation_batches SET status='published',
                   known_at = recorded_at - interval '1 second' WHERE id=:id"""),
            {"id": staged.batch_id})
