from __future__ import annotations

import random
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from marketagg.calendar import SessionCalendar
from marketagg.config import load_policies, load_universe, policy_version
from marketagg.scheduler.budgets import BudgetInputs, full_jitter_delay, next_retry_at, plan_budget
from marketagg.scheduler.planner import SchedulePlanner
from marketagg.settings import ConfigError

ROOT = Path(__file__).resolve().parents[2]


def test_worked_budget_matches_guide_arithmetic() -> None:
    report = plan_budget(BudgetInputs(symbols=2, priority_expiries=4, other_expiries=6))
    # 03: 624 priority + 84 hourly + 780 bars + 2 discovery + 4 daily + 16 repair + 12 broad
    assert report.chain_calls == 624 + 84
    assert report.bar_calls == 780
    assert report.total_calls == 1522
    assert round(report.mean_per_minute, 1) == 3.9
    assert report.boundary_tokens == 10  # 8 priority chains + 2 bar requests
    assert report.boundary_spread_seconds == 60.0
    assert report.feasible and report.planned_capacity == 2730


def test_infeasible_budget_explains_why() -> None:
    report = plan_budget(BudgetInputs(symbols=10, priority_expiries=8, other_expiries=10))
    assert not report.feasible
    assert any("exceed planned capacity" in r for r in report.reasons)
    assert any("120s" in r for r in report.reasons)


def test_full_jitter_is_bounded_and_respects_deadline_and_retry_after() -> None:
    rng = random.Random(1)  # noqa: S311 - seeded jitter under test, not cryptography
    assert all(0 <= full_jitter_delay(a, rng) <= min(60, 2 * 2**a) for a in range(8)
               for _ in range(50))
    now = datetime(2025, 3, 7, 15, tzinfo=UTC)
    assert next_retry_at(attempt=3, max_attempts=3, now=now, deadline=None, rng=rng) is None
    late = next_retry_at(attempt=1, max_attempts=3, now=now, deadline=None, rng=rng,
                         retry_after=300)
    assert late is not None and late - now >= timedelta(seconds=300)
    assert next_retry_at(attempt=1, max_attempts=3, now=now,
                         deadline=now + timedelta(seconds=10), rng=rng,
                         retry_after=30) is None


@pytest.fixture(scope="module")
def planner() -> SchedulePlanner:
    universe = load_universe(ROOT / "config" / "universe.yaml")
    policies = load_policies(ROOT / "config" / "policies.yaml")
    return SchedulePlanner(SessionCalendar(), universe, policies,
                           policy_version("collection", universe).id)


def _counts(planner: SchedulePlanner, day: date) -> dict[str, int]:
    session = planner.calendar.session(day)
    assert session is not None
    counts: dict[str, int] = {}
    for intent in planner.intents_for_session(session):
        counts[intent.kind] = counts.get(intent.kind, 0) + 1
    return counts


def test_normal_session_schedule_counts(planner: SchedulePlanner) -> None:
    counts = _counts(planner, date(2025, 3, 7))
    assert counts["chain_priority"] == 2 * 78  # 09:35 .. 16:00 per symbol
    assert counts["chain_other"] == 2 * 7  # 09:35 then hourly through 15:35
    assert counts["chain_broad"] == 2 * 8  # 24 broad expiries in chunks of 3
    assert counts["bars_1m"] == 2 * 394  # open+2m .. close+5m
    assert counts["discovery"] == 2


def test_early_close_moves_close_relative_jobs(planner: SchedulePlanner) -> None:
    counts = _counts(planner, date(2025, 11, 28))
    assert counts["chain_priority"] == 2 * 42  # 09:35 .. 13:00
    session = planner.calendar.session(date(2025, 11, 28))
    assert session is not None
    broad = [i for i in planner.intents_for_session(session) if i.kind == "chain_broad"]
    assert min(i.scheduled_for for i in broad) == session.close_at - timedelta(minutes=20)
    assert all(i.deadline_at == session.close_at for i in broad)
    assert max(i.not_before for i in broad) < session.close_at - timedelta(minutes=5)


def test_broad_chunks_cover_every_expiry_once(planner: SchedulePlanner) -> None:
    session = planner.calendar.session(date(2025, 3, 7))
    assert session is not None
    symbol = planner.universe.symbols[0].symbol
    broad = [i for i in planner.intents_for_session(session)
             if i.kind == "chain_broad" and i.scope["symbol"] == symbol]
    expiries = list(range(planner.universe.options.broad_expiries_max))
    picked = [e for i in broad for e in expiries[int(i.scope["chunk"])::int(i.scope["chunks"])]]
    assert sorted(picked) == expiries
    assert all(len(expiries[int(i.scope["chunk"])::int(i.scope["chunks"])]) <= 3 for i in broad)


def test_holiday_produces_no_jobs_and_restart_does_not_recreate_past(
    planner: SchedulePlanner,
) -> None:
    holiday = datetime(2025, 7, 4, 15, 0, tzinfo=UTC)
    assert all(i.scheduled_for.date() != date(2025, 7, 4) for i in planner.due_intents(holiday))
    midday = datetime(2025, 3, 7, 17, 0, 30, tzinfo=UTC)  # 12:00:30 ET
    due = planner.due_intents(midday, horizon=timedelta(minutes=5))
    priority = [i for i in due if i.kind == "chain_priority"]
    assert priority and min(i.deadline_at for i in priority if i.deadline_at) > midday
    assert all(i.scheduled_for >= midday - timedelta(minutes=2) for i in priority)


def test_job_keys_are_deterministic(planner: SchedulePlanner) -> None:
    now = datetime(2025, 3, 7, 15, 0, tzinfo=UTC)
    first = [i.key for i in planner.due_intents(now)]
    assert first == [i.key for i in planner.due_intents(now)]
    assert len(first) == len(set(first))


def test_contradictory_universe_rejected(tmp_path: Path) -> None:
    bad = tmp_path / "u.yaml"
    bad.write_text("version: 1\nprovider: fake\nsymbols: [{symbol: SYNA, asset_class: etf}]\n"
                   "options: {dte_min: 30, dte_max: 90, dte_targets: [120]}\n")
    with pytest.raises(ConfigError, match="DTE target"):
        load_universe(bad)
