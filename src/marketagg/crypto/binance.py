"""Binance BTCUSDT candles: native klines over WebSocket, gaps filled from Binance REST.

Live and history come from the same source (Binance spot BTCUSDT), so the local
reconciler compares like with like. Every timeframe the API serves (1m, 5m, 15m,
4h, 1d) is a native Binance kline, not derived here.

- Buckets are Binance's: UTC-anchored, 24/7. 4h = 00/04/08/12/16/20 UTC, 1d = the
  UTC day. ``session_kind`` is stored as RTH, meaning "the instrument's regular
  session", which for a 24/7 venue is the whole UTC day.
- Only closed klines are stored. The WebSocket writes a kline when Binance marks it
  final (``x=true``); REST drops the still-forming last kline.
- ``volume`` is the quote-asset volume (USDT notional), rounded to a whole number,
  because the shared column is an integer. BTC volume would round to almost nothing
  on 1m bars.
- Writes go through ``IngestRepository.stage_bars``, so revisions, hashing and
  immutability work exactly as for Yahoo bars. Unchanged bars are skipped.
- Startup and every reconnect fill from the last stored bar to now via REST. A
  trailing REST re-check every few minutes catches any message the stream dropped.
"""

from __future__ import annotations

import json
import signal
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

import httpx
from sqlalchemy import Engine, text

from marketagg.db.ingest import BarWrite, IngestRepository
from marketagg.db.partitions import ensure_partitions
from marketagg.domain import (
    BarObservation,
    FetchResult,
    InstrumentRef,
    PolicyVersion,
    PriceBasis,
    SessionKind,
    Timeframe,
)
from marketagg.domain.capabilities import (
    CapabilityFact,
    CapabilityStatus,
    Confidence,
    DelayKind,
    Entitlements,
    IntervalCapability,
    ProviderCapabilities,
    SemanticsCapabilities,
    TimestampPrecision,
    TimingCapabilities,
)
from marketagg.domain.enums import FetchStatus
from marketagg.domain.errors import RequestCounts
from marketagg.domain.observations import Coverage
from marketagg.logging import get_logger

log = get_logger(__name__)

PROVIDER_ID = "binance"
SYMBOL = "BTCUSDT"
REST_URL = "https://data-api.binance.vision"  # Binance's public market-data-only host
STREAM_URL = "wss://data-stream.binance.vision/stream"
CHECKED_AT = datetime(2026, 11, 4, tzinfo=UTC)

TIMEFRAMES: tuple[Timeframe, ...] = (Timeframe.M1, Timeframe.M5, Timeframe.M15, Timeframe.H4,
                                     Timeframe.D1)
MINUTES: dict[Timeframe, int] = {Timeframe.M1: 1, Timeframe.M5: 5, Timeframe.M15: 15,
                                 Timeframe.H4: 240, Timeframe.D1: 1440}
# How far back to fill on the first run, per timeframe.
HISTORY: dict[Timeframe, timedelta] = {
    Timeframe.M1: timedelta(days=30),
    Timeframe.M5: timedelta(days=365),
    Timeframe.M15: timedelta(days=365),
    Timeframe.H4: timedelta(days=5 * 365),
    Timeframe.D1: timedelta(days=5 * 365),
}
PAGE = 1000            # Binance klines limit per request
TRAILING_BARS = 60     # trailing re-check depth per timeframe
RECHECK_SECONDS = 300  # trailing re-check interval
REST_PAUSE = 0.2       # seconds between REST pages (Binance allows far more)

BARS_POLICY = PolicyVersion.create("bars_ingest", {
    "version": 1, "provider": PROVIDER_ID, "symbol": SYMBOL, "closed_only": True,
    "source": "native klines: WebSocket live, REST gap fill",
    "session": "24x7, UTC-anchored buckets", "session_kind_column": "RTH = full UTC day",
    "volume": "quote_asset_volume (USDT), rounded to integer",
}, "binance-v1")


def _fact(evidence: str) -> CapabilityFact:
    return CapabilityFact(status=CapabilityStatus.SUPPORTED, confidence=Confidence.DOCUMENTED,
                          evidence=evidence, checked_at=CHECKED_AT)


def capabilities() -> ProviderCapabilities:
    kline = IntervalCapability(available=_fact(
        "Binance spot API /api/v3/klines and <symbol>@kline_<interval> streams"))
    return ProviderCapabilities(
        provider_id=PROVIDER_ID, capability_version="binance-capabilities/1",
        reliability_tier="public_exchange_feed", synthetic=False,
        bars=dict.fromkeys(TIMEFRAMES, kline),
        timing=TimingCapabilities(delay_kind=DelayKind.REALTIME,
                                  timestamp_precision=TimestampPrecision.MILLISECOND,
                                  evidence=_fact("exchange kline open/close times in ms")),
        semantics=SemanticsCapabilities(
            adjustment_basis=PriceBasis.PROVIDER_NATIVE, session_coverage=(SessionKind.RTH,),
            evidence=_fact("spot prices, no adjustments; 24/7 market")),
        entitlements=Entitlements(
            reviewed=True, review_id="operator-2026-11-04",
            notes="Public Binance market data (data-api / data-stream .binance.vision); "
                  "operator decision to collect BTCUSDT candles."),
    )


# -- Mapping ----------------------------------------------------------------------------


def _utc(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=UTC)


def to_bar(tf: Timeframe, start_ms: int, o: str, h: str, low: str, c: str,
           quote_volume: str) -> BarObservation:
    start = _utc(start_ms)
    return BarObservation(
        instrument=InstrumentRef(provider_id=PROVIDER_ID, symbol=SYMBOL), timeframe=tf,
        interval_start=start, interval_end=start + timedelta(minutes=MINUTES[tf]),
        session_date=start.date(), session_kind=SessionKind.RTH,
        price_basis=PriceBasis.PROVIDER_NATIVE, open=Decimal(o), high=Decimal(h),
        low=Decimal(low), close=Decimal(c), volume=round(Decimal(quote_volume)))


def rest_rows_to_bars(tf: Timeframe, rows: Iterable[Sequence[Any]],
                      now: datetime) -> list[BarObservation]:
    """REST kline arrays -> closed bars (the still-forming kline is dropped)."""
    out = []
    for r in rows:
        bar = to_bar(tf, int(r[0]), r[1], r[2], r[3], r[4], r[7])
        if bar.interval_end <= now:
            out.append(bar)
    return out


def stream_kline_to_bar(message: Mapping[str, Any]) -> BarObservation | None:
    """Combined-stream message -> bar, only when Binance marks the kline closed."""
    k = message.get("data", {}).get("k")
    if not k or not k.get("x"):
        return None
    tf = Timeframe(k["i"])
    return to_bar(tf, int(k["t"]), k["o"], k["h"], k["l"], k["c"], k["q"])


# -- Storage ----------------------------------------------------------------------------


def bootstrap(engine: Engine, repo: IngestRepository, now: datetime) -> UUID:
    """Register provider, policy and instrument (idempotent) and ensure partitions."""
    repo.register_provider(capabilities())
    repo.register_policy(BARS_POLICY)
    instrument_id = repo.register_instrument(
        provider_id=PROVIDER_ID, symbol=SYMBOL, asset_class="crypto", currency="USDT",
        calendar_id="24x7", exchange_timezone="UTC", valid_from=now.date(),
        evidence={"source": "binance collector", "version": 1})
    ensure_partitions(engine, now - max(HISTORY.values()) - timedelta(days=1),
                      now + timedelta(days=62))
    return instrument_id


def store(repo: IngestRepository, instrument_id: UUID, tf: Timeframe,
          bars: Sequence[BarObservation], *, started: datetime, kind: str) -> int:
    """Stage and publish one batch of closed bars; returns rows written (changed)."""
    if not bars:
        return 0
    completed = datetime.now(UTC)
    result: FetchResult[BarObservation] = FetchResult[BarObservation](
        provider_id=PROVIDER_ID, response_id=uuid4(), correlation_id=f"binance:{kind}",
        fetch_started_at=started, fetch_completed_at=max(completed, started),
        status=FetchStatus.COMPLETE, rows=tuple(bars),
        coverage=Coverage(requested_start=bars[0].interval_start,
                          requested_end=bars[-1].interval_end, rows_before_filters=len(bars),
                          rows_after_filters=len(bars)),
        request_counts=RequestCounts(endpoint_requests=1, transport_requests=1))
    one = tf is Timeframe.M1
    writes = [BarWrite(b, complete=True, expected_minutes=MINUTES[tf],
                       observed_minutes=1 if one else None) for b in bars]
    outcome = repo.stage_bars(
        result, instrument_id=instrument_id, policy_id=BARS_POLICY.id, writes=writes,
        scope={"kind": f"binance_{kind}_{tf.value}", "symbol": SYMBOL,
               "start": bars[0].interval_start.isoformat(),
               "end": bars[-1].interval_end.isoformat()})
    if outcome.created:
        repo.publish(outcome.batch_id)
    return outcome.rows_written


def last_stored(engine: Engine, instrument_id: UUID, tf: Timeframe) -> datetime | None:
    with engine.connect() as conn:
        value: datetime | None = conn.execute(text("""
            SELECT max(b.interval_start) FROM bar_observations b
            JOIN observation_batches x ON x.id = b.batch_id
            WHERE x.status = 'published' AND b.instrument_id = :i AND b.provider_id = :p
              AND b.timeframe = :tf AND b.policy_id = :pol
        """), {"i": instrument_id, "p": PROVIDER_ID, "tf": tf.value,
               "pol": BARS_POLICY.id}).scalar_one()
    return value


# -- REST -------------------------------------------------------------------------------


def fetch_klines(client: httpx.Client, tf: Timeframe, *, start: datetime | None = None,
                 limit: int = PAGE) -> list[list[Any]]:
    params: dict[str, Any] = {"symbol": SYMBOL, "interval": tf.value, "limit": limit}
    if start is not None:
        params["startTime"] = int(start.timestamp() * 1000)
    response = client.get(f"{REST_URL}/api/v3/klines", params=params)
    response.raise_for_status()
    rows: list[list[Any]] = response.json()
    return rows


def fill_from(client: httpx.Client, repo: IngestRepository, instrument_id: UUID,
              tf: Timeframe, start: datetime, *, kind: str = "fill") -> int:
    """Page forward from ``start`` to now, storing closed bars. Returns rows written."""
    written = 0
    cursor = start
    while True:
        started = datetime.now(UTC)
        rows = fetch_klines(client, tf, start=cursor)
        bars = rest_rows_to_bars(tf, rows, started)
        written += store(repo, instrument_id, tf, bars, started=started, kind=kind)
        if len(rows) < PAGE or not bars:
            return written
        cursor = bars[-1].interval_end
        time.sleep(REST_PAUSE)


def catch_up(client: httpx.Client, repo: IngestRepository, instrument_id: UUID,
             now: datetime) -> dict[str, int]:
    """Fill every timeframe from its last stored bar (or the history horizon) to now."""
    totals = {}
    for tf in TIMEFRAMES:
        last = last_stored(repo.engine, instrument_id, tf)
        start = (last + timedelta(minutes=MINUTES[tf])) if last else now - HISTORY[tf]
        totals[tf.value] = fill_from(client, repo, instrument_id, tf, start)
    return totals


def recheck_trailing(client: httpx.Client, repo: IngestRepository,
                     instrument_id: UUID) -> dict[str, int]:
    """Re-fetch the last few closed bars per timeframe; only differences are written."""
    totals = {}
    for tf in TIMEFRAMES:
        started = datetime.now(UTC)
        bars = rest_rows_to_bars(tf, fetch_klines(client, tf, limit=TRAILING_BARS), started)
        totals[tf.value] = store(repo, instrument_id, tf, bars, started=started,
                                 kind="recheck")
    return totals


# -- Stream -----------------------------------------------------------------------------


def stream_url() -> str:
    names = "/".join(f"{SYMBOL.lower()}@kline_{tf.value}" for tf in TIMEFRAMES)
    return f"{STREAM_URL}?streams={names}"


def run(engine: Engine, *, connect: Callable[..., Any] | None = None) -> None:
    """Run until SIGTERM/SIGINT: catch up, stream closed klines, re-check periodically."""
    if connect is None:
        from websockets.sync.client import connect as ws_connect  # noqa: PLC0415
        connect = ws_connect
    repo = IngestRepository(engine)
    instrument_id = bootstrap(engine, repo, datetime.now(UTC))
    stopping = False

    def stop(*_: object) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    backoff = 1.0
    with httpx.Client(timeout=30.0, headers={"User-Agent": "marketagg-binance/1"}) as client:
        while not stopping:
            try:
                filled = catch_up(client, repo, instrument_id, datetime.now(UTC))
                log.info("binance catch-up done", extra={"written": filled})
                with connect(stream_url(), open_timeout=15, ping_interval=20,
                             ping_timeout=20, close_timeout=5) as ws:
                    log.info("binance stream connected")
                    backoff = 1.0
                    next_check = time.monotonic() + RECHECK_SECONDS
                    while not stopping:
                        try:
                            raw = ws.recv(timeout=5)
                        except TimeoutError:
                            raw = None
                        if raw is not None:
                            bar = stream_kline_to_bar(json.loads(raw))
                            if bar is not None:
                                store(repo, instrument_id, bar.timeframe, [bar],
                                      started=datetime.now(UTC), kind="stream")
                        if time.monotonic() >= next_check:
                            fixed = recheck_trailing(client, repo, instrument_id)
                            if any(fixed.values()):
                                log.warning("binance re-check wrote bars",
                                            extra={"written": fixed})
                            next_check = time.monotonic() + RECHECK_SECONDS
            except Exception as exc:  # reconnect on any failure; never exit the service
                if stopping:
                    break
                log.warning("binance stream error; reconnecting",
                            extra={"error": f"{type(exc).__name__}: {exc}"[:500],
                                   "retry_in_s": backoff})
                deadline = time.monotonic() + backoff
                while not stopping and time.monotonic() < deadline:
                    time.sleep(0.5)
                backoff = min(backoff * 2, 60.0)
    log.info("binance collector stopped")


__all__ = ["BARS_POLICY", "PROVIDER_ID", "SYMBOL", "bootstrap", "capabilities", "catch_up",
           "rest_rows_to_bars", "run", "store", "stream_kline_to_bar", "to_bar"]
