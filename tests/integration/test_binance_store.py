"""Binance bars land in the shared tables and come back through the research API (UPDATE2)."""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import Engine, text

from marketagg.calendar import SessionCalendar
from marketagg.crypto import binance
from marketagg.db.ingest import IngestRepository
from marketagg.domain import Timeframe
from marketagg.research.repository import ResearchRepository

pytestmark = pytest.mark.integration

T0 = int(datetime(2026, 11, 1, tzinfo=UTC).timestamp() * 1000)  # a Sunday: 24/7 market


def _rows(tf: Timeframe, n: int) -> list[list[object]]:
    step = binance.MINUTES[tf] * 60_000
    return [[T0 + i * step, "100", "102", "99", "101", "1", T0 + (i + 1) * step - 1,
             f"{1000 + i}.4", 1, "0", "0", "0"] for i in range(n)]


def test_store_is_idempotent_and_served_by_api(engine: Engine) -> None:
    repo = IngestRepository(engine)
    now = datetime(2026, 11, 8, tzinfo=UTC)  # after the 6th daily bar closes
    instrument = binance.bootstrap(engine, repo, now)
    assert binance.bootstrap(engine, repo, now) == instrument  # re-registration is a no-op

    for tf in binance.TIMEFRAMES:
        bars = binance.rest_rows_to_bars(tf, _rows(tf, 6), now)
        wall = datetime.now(UTC)  # fetch time must not be after publication time
        assert binance.store(repo, instrument, tf, bars, started=wall, kind="fill") == 6
        assert binance.store(repo, instrument, tf, bars, started=wall, kind="recheck") == 0

    with engine.connect() as conn:
        row = conn.execute(text("SELECT asset_class, currency, calendar_id, exchange_timezone "
                                "FROM instruments WHERE id=:i"), {"i": instrument}).one()
    assert tuple(row) == ("crypto", "USDT", "24x7", "UTC")
    assert binance.last_stored(engine, instrument, Timeframe.H4) == \
        datetime(2026, 11, 1, 20, tzinfo=UTC)

    research = ResearchRepository(engine, SessionCalendar(),
                                  {"bars_ingest": "unused", "aggregation": "unused"})
    page = research.bars(instrument_id=instrument, provider_id="binance",
                         timeframe=Timeframe.H4, start=datetime(2026, 11, 1, tzinfo=UTC),
                         end=datetime(2026, 11, 1, tzinfo=UTC) + timedelta(days=1),
                         decision_at=datetime.now(UTC))  # batches are known at wall time
    assert [b.interval_start.hour for b in page.items] == [0, 4, 8, 12, 16, 20]
    assert all(b.complete for b in page.items)
    assert page.items[0].volume == 1000
