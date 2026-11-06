"""Acceptance evidence capture.

``scripts/run_acceptance.py`` sets ``MARKETAGG_ACCEPTANCE_OUT``; each scenario's
measurements (RPO/RTO, latencies, row counts) are written there as JSON so the
evidence manifest reports measured values. Without the variable nothing is written.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

OUT_ENV = "MARKETAGG_ACCEPTANCE_OUT"


@pytest.fixture
def evidence(request: pytest.FixtureRequest) -> Iterator[dict[str, Any]]:
    measured: dict[str, Any] = {}
    yield measured
    out = os.environ.get(OUT_ENV)
    if out and measured:
        folder = Path(out) / "measurements"
        folder.mkdir(parents=True, exist_ok=True)
        name = request.node.name.replace("/", "_").replace("[", "_").rstrip("]")
        (folder / f"{name}.json").write_text(json.dumps(measured, indent=2, default=str))
