"""Job kind -> handler mapping used by the worker process."""

from __future__ import annotations

from marketagg.collectors.bars import (
    collect_minutes,
    daily_backfill_job,
    daily_refresh,
    minute_sweep,
)
from marketagg.collectors.discovery import discovery_job
from marketagg.collectors.options import collect_chain
from marketagg.collectors.recovery import derive_job, gap_scan_job
from marketagg.worker import Handler

HANDLERS: dict[str, Handler] = {
    "discovery": discovery_job,
    "bars_1m": collect_minutes,
    "chain_priority": collect_chain,
    "chain_other": collect_chain,
    "chain_broad": collect_chain,
    "daily_refresh": daily_refresh,
    "daily_backfill": daily_backfill_job,
    "minute_sweep": minute_sweep,
    "gap_scan": gap_scan_job,
    "derive_bars": derive_job,
}
