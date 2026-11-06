"""Phase 0 provider feasibility probe (T00).

    uv run python scripts/probe_provider.py --provider fake --output reports/feasibility/fake
    uv run marketagg probe --provider fake --output reports/feasibility/fake

Writes environment.json, capabilities.json, request-budget.json, source-review.md
and probe-results.parquet. The fake run uses a fixed synthetic clock, so its
report is deterministic. ``--provider yahoo`` performs live network requests and
therefore refuses to run unless settings pass the source-review gate; its output
must go under ``reports/feasibility/live/`` so live evidence stays separate.
"""

from __future__ import annotations

import argparse
import importlib.metadata
import json
import platform
import sys
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from marketagg.calendar import SessionCalendar, calendar_dte
from marketagg.domain import (
    BarRequest,
    ChainRequest,
    DiscoveryRequest,
    InstrumentRef,
    OptionObservation,
    ProviderError,
    Timeframe,
)
from marketagg.providers.base import MarketDataProvider
from marketagg.providers.fake import FakeClock, FakeProvider, FakeScenario
from marketagg.scheduler.budgets import BudgetInputs, plan_budget

EXIT_BLOCKED = 3
FAKE_START = datetime(2025, 3, 7, 15, 0, tzinfo=UTC)
FAR = datetime(2100, 1, 1, tzinfo=UTC)
PACKAGES = ("marketagg", "pydantic", "pandas", "exchange-calendars", "yfinance", "pyarrow")


@dataclass
class ProbeLog:
    rows: list[dict[str, Any]] = field(default_factory=list)

    def add(self, **row: Any) -> None:
        base = {"probe": "", "symbol": "", "detail": "", "status": "", "rows": 0,
                "endpoint_requests": 0, "transport_requests": None, "null_fraction": None,
                "duplicates": 0, "error": None, "fetch_seconds": None}
        self.rows.append({**base, **row})


def _versions() -> dict[str, str | None]:
    out: dict[str, str | None] = {}
    for name in PACKAGES:
        try:
            out[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            out[name] = None
    return out


def _null_fraction(rows: Sequence[OptionObservation]) -> dict[str, float]:
    fields = ("bid", "ask", "last", "bid_size", "ask_size", "reported_iv", "reported_volume",
              "reported_open_interest", "last_trade_at", "provider_event_at")
    if not rows:
        return {}
    return {f: round(sum(getattr(r, f) is None for r in rows) / len(rows), 4) for f in fields}


def run_probe(provider: MarketDataProvider, symbols: Sequence[str], now: datetime,
              calendar: SessionCalendar) -> tuple[ProbeLog, dict[str, Any]]:
    log = ProbeLog()
    nulls: dict[str, Any] = {}
    pid = provider.provider_id
    for symbol in symbols:
        ref = InstrumentRef(provider_id=pid, symbol=symbol)
        try:
            found = provider.discover(DiscoveryRequest(
                provider_id=pid, policy_id="probe", deadline_at=FAR, correlation_id="probe",
                instrument=ref))
        except ProviderError as exc:
            log.add(probe="discovery", symbol=symbol, status="error", error=exc.code.value,
                    endpoint_requests=exc.request_counts.endpoint_requests)
            if exc.code.value == "unauthorized":
                break  # stop immediately on access denial (00)
            continue
        log.add(probe="discovery", symbol=symbol, status="ok", rows=len(found.expirations),
                endpoint_requests=found.request_counts.endpoint_requests,
                transport_requests=found.request_counts.transport_requests,
                fetch_seconds=(found.fetch_completed_at - found.fetch_started_at)
                .total_seconds())
        session_day = calendar.local_date(now)
        eligible = [e for e in found.expirations if 30 <= calendar_dte(e, session_day) <= 90]
        for expiry in eligible[:1]:
            for label in ("cold_or_first", "warm"):
                try:
                    chain = provider.fetch_chain(ChainRequest(
                        provider_id=pid, policy_id="probe", deadline_at=FAR,
                        correlation_id="probe", instrument=ref, expiration_date=expiry))
                except ProviderError as exc:
                    log.add(probe="chain", symbol=symbol, detail=label, status="error",
                            error=exc.code.value)
                    continue
                symbols_seen = [r.contract.provider_symbol for r in chain.rows]
                nulls[f"{symbol}:{expiry}:{label}"] = _null_fraction(chain.rows)
                log.add(probe="chain", symbol=symbol, detail=f"{label} {expiry}",
                        status=chain.status.value, rows=len(chain.rows),
                        endpoint_requests=chain.request_counts.endpoint_requests,
                        transport_requests=chain.request_counts.transport_requests,
                        duplicates=len(symbols_seen) - len(set(symbols_seen)),
                        null_fraction=max(_null_fraction(chain.rows).values(), default=None),
                        fetch_seconds=(chain.fetch_completed_at - chain.fetch_started_at)
                        .total_seconds())
        for timeframe, span, label in ((Timeframe.D1, timedelta(days=30), "daily 30d"),
                                       (Timeframe.M1, timedelta(days=1), "1m span 1d"),
                                       (Timeframe.M1, timedelta(days=7), "1m span 7d")):
            end = now.replace(second=0, microsecond=0)
            try:
                bars = provider.fetch_bars(BarRequest(
                    provider_id=pid, policy_id="probe", deadline_at=FAR,
                    correlation_id="probe", instrument=ref, timeframe=timeframe,
                    start=end - span, end=end))
            except ProviderError as exc:
                log.add(probe="bars", symbol=symbol, detail=label, status="error",
                        error=exc.code.value)
                continue
            starts = [b.interval_start for b in bars.rows]
            log.add(probe="bars", symbol=symbol, detail=label, status=bars.status.value,
                    rows=len(bars.rows), endpoint_requests=bars.request_counts.endpoint_requests,
                    transport_requests=bars.request_counts.transport_requests,
                    duplicates=len(starts) - len(set(starts)))
    return log, nulls


def write_report(provider: MarketDataProvider, log: ProbeLog, nulls: dict[str, Any],
                 checked_at: datetime, output: Path, symbols: Sequence[str],
                 live: bool) -> None:
    output.mkdir(parents=True, exist_ok=True)
    caps = provider.capabilities()
    environment = {
        "checked_at_utc": checked_at.isoformat().replace("+00:00", "Z"),
        "clock": "wall clock" if live else "fixed synthetic clock",
        "evidence_kind": "live probe" if live else "synthetic dry run (no network)",
        "python": platform.python_version(),
        "implementation": platform.python_implementation(),
        "platform": f"{platform.system()} {platform.machine()}",
        "packages": _versions(),
        "provider": provider.provider_id,
        "symbols": list(symbols),
    }
    budget = plan_budget(BudgetInputs(symbols=len(symbols), priority_expiries=4,
                                      other_expiries=6))
    requests = {
        "planned": budget.as_dict(),
        "observed_probe_requests": {
            "endpoint": sum(r["endpoint_requests"] or 0 for r in log.rows),
            "transport": (None if any(r["transport_requests"] is None for r in log.rows)
                          else sum(r["transport_requests"] for r in log.rows)),
        },
        "null_rates": nulls,
    }
    (output / "environment.json").write_text(json.dumps(environment, indent=2,
                                                        sort_keys=True) + "\n")
    (output / "capabilities.json").write_text(json.dumps(caps.model_dump(mode="json"),
                                                         indent=2, sort_keys=True) + "\n")
    (output / "request-budget.json").write_text(json.dumps(requests, indent=2,
                                                           sort_keys=True) + "\n")
    (output / "source-review.md").write_text(_source_review(provider.provider_id, live))
    table = pa.Table.from_pylist(log.rows, schema=pa.schema([
        ("probe", pa.string()), ("symbol", pa.string()), ("detail", pa.string()),
        ("status", pa.string()), ("rows", pa.int64()), ("endpoint_requests", pa.int64()),
        ("transport_requests", pa.int64()), ("null_fraction", pa.float64()),
        ("duplicates", pa.int64()), ("error", pa.string()), ("fetch_seconds", pa.float64()),
    ]))
    pq.write_table(table, output / "probe-results.parquet", compression="zstd")


_NOT_RECHECKED = "not re-checked by this probe"


def _source_review(provider_id: str, live: bool) -> str:
    if provider_id == "fake":
        status = ("Synthetic provider. No market data, no network, no data-use rights "
                  "involved or conferred.")
    else:
        status = ("Live evidence. Rights status must be copied from the recorded review "
                  "referenced by SOURCE_REVIEW_ID; this file does not establish permission.")
    return f"""# Source review — {provider_id}

{status}

| Item | Status |
|---|---|
| Yahoo terms (legal.yahoo.com) | UNRESOLVED: HTTP 999 during planning; {_NOT_RECHECKED} |
| Automated collection permission | UNRESOLVED |
| Retention / backup / private export rights | UNRESOLVED |
| Redistribution | Not permitted by default (Yahoo SLN2310 notice) |
| Live probe executed | {"yes" if live else "no"} |

Live Yahoo collection stays disabled until a recorded review resolves the items
above. Empty results are inconclusive; request-span and oldest-history limits
are separate and unknown until a permitted live probe measures them.
"""


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="marketagg probe",
                                     description="Phase 0 provider feasibility probe")
    parser.add_argument("--provider", choices=["fake", "yahoo"], default="fake")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--symbols", nargs="+")
    args = parser.parse_args(argv)
    calendar = SessionCalendar()
    if args.provider == "fake":
        symbols = args.symbols or ["SYNA", "SYNB"]
        provider: MarketDataProvider = FakeProvider(FakeScenario(), FakeClock(FAKE_START))
        log, nulls = run_probe(provider, symbols, FAKE_START, calendar)
        write_report(provider, log, nulls, FAKE_START, args.output, symbols, live=False)
        print(f"fake probe written to {args.output}")
        return 0
    from marketagg.providers.yahoo import (  # noqa: PLC0415 - optional live dependency
        LiveAuthorization,
        LiveSourceBlocked,
        YahooProvider,
    )
    from marketagg.settings import ConfigError, load_settings  # noqa: PLC0415

    if "live" not in args.output.parts:
        print("error: live evidence must be written under reports/feasibility/live/",
              file=sys.stderr)
        return EXIT_BLOCKED
    try:
        auth = LiveAuthorization.from_settings(load_settings())
        provider = YahooProvider(calendar=calendar, authorization=auth)
    except (LiveSourceBlocked, ConfigError) as exc:
        print(f"blocked: {exc}", file=sys.stderr)
        return EXIT_BLOCKED
    now = datetime.now(UTC)
    symbols = args.symbols or ["SPY", "QQQ"]
    log, nulls = run_probe(provider, symbols, now, calendar)
    write_report(provider, log, nulls, now, args.output, symbols, live=True)
    return 0


__all__ = ["EXIT_BLOCKED", "main", "run_probe", "write_report"]
