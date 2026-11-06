"""Private research dashboard (T14). Run: ``streamlit run ui/app.py``.

Reads only through the research API with a read-scope token.
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime, time, timedelta
from pathlib import Path

import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent))

from client import ApiUnavailable, make_client
from views import TIMEFRAMES, banners, build_figure

st.set_page_config(page_title="marketagg research", layout="wide")
st.title("Options and underlying research")
st.caption("Private research view. Option samples are periodic polls with unknown quote "
           "freshness unless the source provides event times; nothing here is execution-grade.")

try:
    client = make_client()
    instruments = client.get("/v1/instruments")["items"]
except ApiUnavailable as exc:
    st.error(f"Research API unavailable: {exc}")
    st.stop()

if not instruments:
    st.info("No instruments configured yet. Run `marketagg bootstrap`.")
    st.stop()

with st.sidebar:
    symbols = {i["symbol"]: i for i in instruments}
    symbol = st.selectbox("Instrument", sorted(symbols))
    instrument = symbols[symbol]
    timeframe = st.radio("Interval", TIMEFRAMES, index=2, horizontal=True)
    today = datetime.now(UTC).date()
    window = st.date_input("Window (UTC dates)", (today - timedelta(days=1), today))
    use_cutoff = st.checkbox("Historical decision cutoff", value=False,
                             help="Only data known at this instant are shown.")
    cutoff = None
    if use_cutoff:
        cutoff_day = st.date_input("Cutoff date", today, key="cutoff_day")
        cutoff_time = st.time_input("Cutoff time (UTC)", time(20, 0))
        cutoff = datetime.combine(cutoff_day, cutoff_time, UTC).isoformat()
    mode = st.radio("Availability", ("observed", "recomputed"), horizontal=True)
    series = st.multiselect("Option series", ["bid", "ask", "midpoint"],
                            default=["bid", "ask", "midpoint"])
    show_iv = st.checkbox("Provider-reported IV", value=True)

start_day, end_day = (window if isinstance(window, tuple) and len(window) == 2
                      else (today - timedelta(days=1), today))
start = datetime.combine(start_day, time(0), UTC).isoformat()
end = datetime.combine(end_day + timedelta(days=1), time(0), UTC).isoformat()

st.subheader("Contract browser")
try:
    contracts = client.get("/v1/contracts", instrument_id=instrument["id"],
                           provider_id=instrument["provider_id"], asof=cutoff)["items"]
except ApiUnavailable as exc:
    st.error(f"Contracts unavailable: {exc}")
    contracts = []
contract_id = None
if contracts:
    cols = st.columns(3)
    expiries = sorted({c["expiration_date"] for c in contracts})
    expiry = cols[0].selectbox("Expiration", expiries)
    side = cols[1].radio("Side", ("C", "P"), horizontal=True,
                         format_func=lambda s: "Calls" if s == "C" else "Puts")
    chain = sorted((c for c in contracts if c["expiration_date"] == expiry
                    and c["option_type"] == side), key=lambda c: float(c["strike"]))
    if chain:
        chosen = cols[2].selectbox("Strike", chain, format_func=lambda c: c["strike"])
        contract_id = chosen["id"]
        if not chosen["terms_known"]:
            st.caption("Contract terms (multiplier, deliverable, exercise cut-off) are not "
                       "verified: browse only, excluded from models.")
else:
    st.info("No contracts observed for this instrument at the selected cutoff.")

try:
    if contract_id is not None:
        response = client.get("/v1/panel", instrument_id=instrument["id"],
                              provider_id=instrument["provider_id"], timeframe=timeframe,
                              start=start, end=end, contract_id=contract_id,
                              decision_at=cutoff, mode=mode)
    else:
        response = client.get("/v1/bars", instrument_id=instrument["id"],
                              provider_id=instrument["provider_id"], timeframe=timeframe,
                              start=start, end=end, decision_at=cutoff, mode=mode,
                              include_partial=True)
except ApiUnavailable as exc:
    st.error(f"Chart data unavailable: {exc}")
    st.stop()

for level, message in banners(response, has_contract=contract_id is not None):
    getattr(st, level)(message)
if response.get("items"):
    st.plotly_chart(build_figure(response, series=series, show_iv=show_iv),
                    use_container_width=True)

with st.expander("Data quality"):
    try:
        st.json(client.get("/v1/quality")["report"])
    except ApiUnavailable as exc:
        st.error(f"Quality report unavailable: {exc}")

with st.expander("Export status"):
    job = st.text_input("Dataset job ID")
    if job:
        try:
            status = client.get(f"/v1/datasets/{job}")
            st.write(f"State: **{status['state']}**")
            for name, link in status.get("downloads", {}).items():
                st.markdown(f"- [{name}]({link}) (link expires)")
        except ApiUnavailable as exc:
            st.error(f"Export status unavailable: {exc}")
