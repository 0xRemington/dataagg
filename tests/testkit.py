"""Shared helpers for integration/acceptance tests (synthetic data only)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import Engine, text

from marketagg.analytics.aggregate import aggregation_policy
from marketagg.analytics.asof import BarKey
from marketagg.calendar import SessionCalendar
from marketagg.collectors.bars import ingest_bars
from marketagg.db.ingest import IngestRepository
from marketagg.db.partitions import ensure_partitions
from marketagg.domain import (
    BarRequest,
    ChainRequest,
    DiscoveryRequest,
    InstrumentRef,
    PolicyVersion,
    PriceBasis,
    SessionKind,
    Timeframe,
)
from marketagg.providers.fake import FakeClock, FakeProvider, FakeScenario

START = datetime(2025, 3, 7, 15, 0, tzinfo=UTC)  # Friday 10:00 ET
FAR = datetime(2035, 1, 1, tzinfo=UTC)
EXPIRY_35 = date(2025, 4, 11)
BAR_POLICY = PolicyVersion.create("bars_ingest", {"source": "fake", "version": 1}, "test")


@dataclass
class World:
    engine: Engine
    clock: FakeClock
    provider: FakeProvider
    repo: IngestRepository
    instruments: dict[str, UUID]

    def ref(self, symbol: str = "SYNA") -> InstrumentRef:
        return InstrumentRef(provider_id="fake", symbol=symbol)

    def chain_request(self, expiration: date = EXPIRY_35, symbol: str = "SYNA") -> ChainRequest:
        return ChainRequest(provider_id="fake", policy_id=BAR_POLICY.id, deadline_at=FAR,
                            correlation_id="t", instrument=self.ref(symbol),
                            expiration_date=expiration)

    def bar_request(self, start: datetime, end: datetime, timeframe: Timeframe = Timeframe.M1,
                    symbol: str = "SYNA") -> BarRequest:
        return BarRequest(provider_id="fake", policy_id=BAR_POLICY.id, deadline_at=FAR,
                          correlation_id="t", instrument=self.ref(symbol),
                          timeframe=timeframe, start=start, end=end)

    def discovery_request(self, symbol: str = "SYNA") -> DiscoveryRequest:
        return DiscoveryRequest(provider_id="fake", policy_id=BAR_POLICY.id, deadline_at=FAR,
                                correlation_id="t", instrument=self.ref(symbol))


def make_world(engine: Engine, scenario: FakeScenario | None = None,
               start: datetime = START) -> World:
    clock = FakeClock(start)
    provider = FakeProvider(scenario or FakeScenario(), clock)
    repo = IngestRepository(engine, clock=clock)
    repo.register_provider(provider.capabilities())
    repo.register_policy(BAR_POLICY)
    ensure_partitions(engine, date(2025, 1, 1), date(2025, 12, 31))
    instruments = {
        s: repo.register_instrument(provider_id="fake", symbol=s, asset_class="etf",
                                    valid_from=date(2020, 1, 1),
                                    evidence={"source": "synthetic"})
        for s in ("SYNA", "SYNB")
    }
    return World(engine, clock, provider, repo, instruments)


def count(engine: Engine, sql: str, **params: object) -> int:
    with engine.connect() as conn:
        return int(conn.execute(text(sql), params).scalar_one())


def advance(world: World, seconds: float) -> None:
    world.clock.advance(timedelta(seconds=seconds))


def aggregation_policy_id(world: World) -> str:
    policy = aggregation_policy(SessionCalendar().policy_id)
    world.repo.register_policy(policy)
    return policy.id


def m1_key(world: World, symbol: str = "SYNA") -> Any:
    return BarKey(world.instruments[symbol], "fake", Timeframe.M1, PriceBasis.PROVIDER_NATIVE,
                  SessionKind.RTH, BAR_POLICY.id)


def ingest_minutes(world: World, start: datetime, end: datetime, symbol: str = "SYNA",
                   ) -> dict[str, int]:
    return ingest_bars(provider=world.provider, repo=world.repo, calendar=SessionCalendar(),
                       request=world.bar_request(start, end, symbol=symbol),
                       instrument_id=world.instruments[symbol], policy_id=BAR_POLICY.id,
                       safe_lag=timedelta(seconds=80))
