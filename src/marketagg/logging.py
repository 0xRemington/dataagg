"""Structured UTC JSON logs with secret redaction.

Every record passes through ``RedactingFilter`` before formatting, so the message,
structured fields and exception text are all scrubbed. Known secret values can be
registered explicitly; common token/password/URL-credential shapes are scrubbed
by pattern as a second line of defence.
"""

from __future__ import annotations

import json
import logging
import re
import sys
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from typing import Any, TextIO

REDACTED = "[REDACTED]"

_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"(?i)\b(bearer\s+)[A-Za-z0-9._~+/=-]+"), rf"\1{REDACTED}"),
    (
        re.compile(
            r"(?i)\b((?:access_|refresh_|api_|read_|export_)?token|password|passwd|secret"
            r"|api[_-]?key|authorization|cookie)(\s*[=:]\s*)(\"?)[^\s\"',&;]+"
        ),
        rf"\1\2\3{REDACTED}",
    ),
    (
        re.compile(r"([a-zA-Z][a-zA-Z0-9+.-]*://[^:/@\s]*):[^@/\s]*@"),
        rf"\1:{REDACTED}@",
    ),
)

_STANDARD_ATTRS = frozenset(
    logging.LogRecord("", 0, "", 0, "", None, None).__dict__.keys()
    | {"message", "asctime", "taskName"}
)


class Redactor:
    def __init__(self, secrets: Iterable[str] = ()) -> None:
        self._secrets: set[str] = set()
        self.register(secrets)

    def register(self, secrets: Iterable[str]) -> None:
        # Very short values would redact ordinary text; secrets are never that short.
        self._secrets.update(s for s in secrets if len(s) >= 6)

    def __call__(self, text: str) -> str:
        for secret in sorted(self._secrets, key=len, reverse=True):
            text = text.replace(secret, REDACTED)
        for pattern, replacement in _PATTERNS:
            text = pattern.sub(replacement, text)
        return text

    def scrub(self, value: Any) -> Any:
        if isinstance(value, str):
            return self(value)
        if isinstance(value, Mapping):
            return {k: self.scrub(v) for k, v in value.items()}
        if isinstance(value, list | tuple):
            return [self.scrub(v) for v in value]
        return value


class RedactingFilter(logging.Filter):
    def __init__(self, redactor: Redactor) -> None:
        super().__init__()
        self.redactor = redactor

    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = self.redactor(record.getMessage())
        record.args = None
        for key, value in list(record.__dict__.items()):
            if key not in _STANDARD_ATTRS:
                record.__dict__[key] = self.redactor.scrub(value)
        if record.exc_info:
            record.exc_text = self.redactor(logging.Formatter().formatException(record.exc_info))
            record.exc_info = None
        return True


class JsonFormatter(logging.Formatter):
    """One JSON object per line; ``ts`` is ISO 8601 UTC with ``Z``."""

    def format(self, record: logging.LogRecord) -> str:
        ts = datetime.fromtimestamp(record.created, tz=UTC)
        payload: dict[str, Any] = {
            "ts": ts.isoformat(timespec="microseconds").replace("+00:00", "Z"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _STANDARD_ATTRS and key not in payload:
                payload[key] = value
        if record.exc_text:
            payload["exc"] = record.exc_text
        return json.dumps(payload, default=str, sort_keys=False)


def configure_logging(
    level: str = "INFO",
    secrets: Iterable[str] = (),
    stream: TextIO | None = None,
) -> Redactor:
    """Install the JSON+redaction handler on the ``marketagg`` logger tree."""
    redactor = Redactor(secrets)
    handler = logging.StreamHandler(stream or sys.stderr)
    handler.addFilter(RedactingFilter(redactor))
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger("marketagg")
    for existing in list(root.handlers):
        root.removeHandler(existing)
    root.addHandler(handler)
    root.setLevel(level)
    root.propagate = False
    return redactor


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name if name.startswith("marketagg") else f"marketagg.{name}")
