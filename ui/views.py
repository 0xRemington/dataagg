"""Pure chart/view helpers for the dashboard (testable without Streamlit).

- One shared UTC x-axis; the option panel is sampled at underlying bar ends.
- Missing intervals and session boundaries break lines (``None``); nothing is
  forward-filled across a gap or a night/weekend by default.
- Unknown quote event time, partial bars, short 4h buckets and the source/price
  basis are always surfaced as explicit labels.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Any

import plotly.graph_objects as go
from plotly.subplots import make_subplots

TIMEFRAMES = ("1m", "5m", "15m", "4h", "1d")
FOUR_HOUR_NOTE = ("4h candles are session-anchored: 09:30-13:30 ET (240m) then a "
                  "13:30-16:00 remainder (2h30). Early closes give one shorter bucket "
                  "(e.g. 3h30). Short buckets are labelled, not stretched.")


def _ts(value: str | None) -> datetime | None:
    return None if value is None else datetime.fromisoformat(value.replace("Z", "+00:00"))


def with_breaks(points: Sequence[tuple[datetime, float | None]], expected_step: timedelta,
                ) -> tuple[list[datetime | None], list[float | None]]:
    """Insert ``None`` wherever consecutive points are further apart than one step."""
    xs: list[datetime | None] = []
    ys: list[float | None] = []
    previous: datetime | None = None
    for x, y in points:
        if previous is not None and x - previous > expected_step * 1.5:
            xs.append(None)
            ys.append(None)
        xs.append(x)
        ys.append(y)
        previous = x
    return xs, ys


def banners(response: dict[str, Any], *, has_contract: bool) -> list[tuple[str, str]]:
    """(level, message) pairs shown above the chart."""
    out: list[tuple[str, str]] = []
    source = response.get("source", {})
    availability = response.get("availability", {})
    out.append(("info", f"Source {source.get('provider_id')} · price basis "
                        f"{source.get('price_basis')} · session {source.get('session_kind')} · "
                        f"{availability.get('label')} · cutoff {availability.get('decision_at')}"))
    if response.get("timeframe") == "4h":
        out.append(("info", FOUR_HOUR_NOTE))
    items = response.get("items", [])
    if not items:
        out.append(("info", "No eligible bars in this window (holiday, weekend, outside the "
                            "collected history, or not yet known at the cutoff)."))
    if has_contract and any(p.get("option") and p.get("freshness") == "unknown"
                            for p in items):
        out.append(("warning", "Quote event time UNKNOWN for option samples: the time shown "
                               "is when we fetched the chain, not when the quote was set. "
                               "Not execution-grade."))
    incomplete = sum(1 for p in items if not _bar(p).get("complete", True))
    if incomplete:
        out.append(("warning", f"{incomplete} partial bar(s) shown for display only."))
    for warning in response.get("warnings", []):
        if "4h" not in warning and "freshness" not in warning:
            out.append(("info", warning))
    return out


def _bar(item: dict[str, Any]) -> dict[str, Any]:
    return item.get("bar", item)


STEP = {"1m": timedelta(minutes=1), "5m": timedelta(minutes=5),
        "15m": timedelta(minutes=15), "4h": timedelta(hours=4), "1d": timedelta(days=4)}


def build_figure(response: dict[str, Any], *, series: Sequence[str] = ("bid", "ask", "midpoint"),
                 show_iv: bool = True) -> go.Figure:
    items = response.get("items", [])
    bars = [_bar(p) for p in items]
    has_option_panel = any("option" in p for p in items)
    fig = make_subplots(rows=2 if has_option_panel else 1, cols=1, shared_xaxes=True,
                        vertical_spacing=0.04,
                        row_heights=[0.6, 0.4] if has_option_panel else [1.0],
                        specs=[[{}], [{"secondary_y": True}]] if has_option_panel else [[{}]])
    fig.add_trace(go.Candlestick(
        x=[_ts(b["interval_start"]) for b in bars],
        open=[float(b["open"]) for b in bars], high=[float(b["high"]) for b in bars],
        low=[float(b["low"]) for b in bars], close=[float(b["close"]) for b in bars],
        name="underlying", text=[b.get("label", "") for b in bars]), row=1, col=1)
    partial = [b for b in bars if not b.get("complete", True)]
    if partial:
        fig.add_trace(go.Scatter(
            x=[_ts(b["interval_start"]) for b in partial],
            y=[float(b["high"]) for b in partial], mode="markers", name="partial bar",
            marker={"symbol": "x-thin-open", "size": 10}), row=1, col=1)
    if has_option_panel:
        step = STEP.get(str(response.get("timeframe")), timedelta(minutes=5))
        for field in series:
            points = []
            for p in items:
                option = p.get("option")
                end = _ts(_bar(p)["interval_end"])
                if end is None:
                    continue
                value = option.get(field) if option else None
                points.append((end, float(value) if value is not None else None))
            xs, ys = with_breaks(points, step)
            fig.add_trace(go.Scatter(x=xs, y=ys, mode="lines+markers", name=f"option {field}",
                                     connectgaps=False), row=2, col=1)
        if show_iv:
            points = [(_ts(_bar(p)["interval_end"]),
                       (p.get("option") or {}).get("reported_iv")) for p in items]
            xs, ys = with_breaks([(x, y) for x, y in points if x is not None], step)
            fig.add_trace(go.Scatter(x=xs, y=ys, mode="lines", name="reported IV (provider)",
                                     line={"dash": "dot"}, connectgaps=False),
                          row=2, col=1, secondary_y=True)
    fig.update_layout(height=720 if has_option_panel else 480, xaxis_rangeslider_visible=False,
                      margin={"l": 40, "r": 20, "t": 30, "b": 30},
                      legend={"orientation": "h"})
    fig.update_xaxes(title_text="UTC", row=2 if has_option_panel else 1, col=1)
    return fig
