"""Logging ``extra`` keys must not collide with LogRecord attributes.

A collision raises KeyError inside the log call. That crash-looped the production
scheduler on 2026-10-26 (``extra={"created": ...}``).
"""

from __future__ import annotations

import ast
import logging
from pathlib import Path

SRC = Path(__file__).resolve().parents[2] / "src" / "marketagg"
RESERVED = set(vars(logging.makeLogRecord({}))) | {"message", "asctime"}


def test_no_log_call_uses_a_reserved_extra_key() -> None:
    offenders = []
    for path in SRC.rglob("*.py"):
        for node in ast.walk(ast.parse(path.read_text())):
            if not isinstance(node, ast.Call):
                continue
            for keyword in node.keywords:
                if keyword.arg == "extra" and isinstance(keyword.value, ast.Dict):
                    keys = {k.value for k in keyword.value.keys
                            if isinstance(k, ast.Constant) and isinstance(k.value, str)}
                    offenders += [f"{path.name}:{node.lineno}:{k}" for k in keys & RESERVED]
    assert offenders == []
