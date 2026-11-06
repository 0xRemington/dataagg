"""Initial history (03 steps 2-3) is enqueued once per collection policy and runs."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from sqlalchemy import Engine
from testkit import count

from marketagg.config import load_policies, load_universe
from marketagg.providers.fake import FakeClock, FakeProvider, FakeScenario
from marketagg.runtime import bootstrap, enqueue_history_bootstrap, make_worker

ROOT = Path(__file__).resolve().parents[2]


def test_history_bootstrap_is_enqueued_once_and_backfills(engine: Engine) -> None:
    clock = FakeClock(datetime(2025, 3, 10, 2, 0, tzinfo=UTC))  # Sunday night ET
    rt = bootstrap(engine, FakeProvider(FakeScenario(), clock),
                   load_universe(ROOT / "config" / "universe.yaml"),
                   load_policies(ROOT / "config" / "policies.yaml"), clock=clock,
                   requests_per_minute=600)
    # 2 symbols x (daily backfill + minute sweep), then one derive pass for those sessions.
    assert enqueue_history_bootstrap(rt) == 5
    assert enqueue_history_bootstrap(rt) == 0  # restart / re-migrate does not repeat it
    worker = make_worker(rt, "w", sleep=lambda s: clock.advance(timedelta(seconds=s)))
    while worker.run_once(["daily_backfill", "minute_sweep", "derive_bars"]) is not None:
        pass
    assert count(engine, "SELECT count(*) FROM collection_jobs WHERE status <> 'succeeded'") == 0
    assert count(engine, "SELECT count(*) FROM bar_observations WHERE timeframe='1d'") > 2 * 250
    # The week before 2025-03-10 has five sessions of 390 minutes per symbol.
    assert count(engine, "SELECT count(*) FROM bar_observations WHERE timeframe='1m'") == (
        2 * 5 * 390)
    # Derived candles exist for every backfilled session: 78 x 5m, 26 x 15m, 2 x 4h.
    assert count(engine, "SELECT count(*) FROM bar_observations WHERE timeframe IN "
                         "('5m','15m','4h')") == 2 * 5 * (78 + 26 + 2)


def test_handler_error_is_retried_then_failed_without_crashing_the_worker(
        engine: Engine) -> None:
    clock = FakeClock(datetime(2025, 3, 10, 2, 0, tzinfo=UTC))
    rt = bootstrap(engine, FakeProvider(FakeScenario(), clock),
                   load_universe(ROOT / "config" / "universe.yaml"),
                   load_policies(ROOT / "config" / "policies.yaml"), clock=clock,
                   requests_per_minute=600)
    enqueue_history_bootstrap(rt)

    def broken(ctx: object) -> object:
        raise PermissionError("[Errno 13] Permission denied: '/data/payloads'")

    worker = make_worker(rt, "w", handlers={"minute_sweep": broken},  # type: ignore[dict-item]
                         sleep=lambda s: clock.advance(timedelta(seconds=s)))
    for _ in range(20):  # each call returns normally; retries back off, then fail
        worker.run_once(["minute_sweep"])
        clock.advance(timedelta(minutes=10))
    assert count(engine, "SELECT count(*) FROM collection_jobs WHERE kind='minute_sweep' "
                         "AND status='failed' AND last_error_code='internal_error'") == 2
    assert count(engine, "SELECT count(*) FROM operational_incidents "
                         "WHERE kind='internal_error'") >= 2
