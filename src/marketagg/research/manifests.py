"""Dataset manifests and deterministic serialisation helpers (T15)."""

from __future__ import annotations

import csv
import hashlib
import io
from collections.abc import Iterable, Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID

from marketagg.domain.primitives import canonical_dumps, format_decimal, format_utc

FORMULA_PREFIXES = ("=", "+", "-", "@", "\t", "\r")


def canonical_value(value: Any) -> Any:  # noqa: PLR0911 - one return per type
    if value is None or isinstance(value, bool | int | float | str):
        return value
    if isinstance(value, Decimal):
        return format_decimal(value)
    if isinstance(value, datetime):
        return format_utc(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, list | tuple):
        return [canonical_value(v) for v in value]
    return str(value)


def semantic_hash(rows: Iterable[Mapping[str, Any]], spec: Mapping[str, Any]) -> str:
    """Hash of meaning, not bytes: canonical JSON of the spec and every row in order."""
    digest = hashlib.sha256()
    digest.update(canonical_dumps(dict(spec)).encode())
    for row in rows:
        digest.update(b"\n")
        digest.update(canonical_dumps({k: canonical_value(v) for k, v in row.items()}).encode())
    return digest.hexdigest()


def safe_cell(value: Any, *, text_column: bool) -> str:
    """CSV text cells starting with formula characters are neutralised with a quote."""
    if value is None:
        return ""
    if isinstance(value, list | tuple):
        value = "|".join(str(v) for v in value)
    rendered = canonical_value(value)
    rendered = "" if rendered is None else str(rendered)
    if text_column and rendered.startswith(FORMULA_PREFIXES):
        return "'" + rendered
    return rendered


def write_csv(path: Path, columns: Sequence[str], rows: Iterable[Mapping[str, Any]],
              text_columns: set[str]) -> None:
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
    writer.writerow(columns)
    for row in rows:
        writer.writerow([safe_cell(row.get(c), text_column=c in text_columns) for c in columns])
    path.write_text(buffer.getvalue(), encoding="utf-8")


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()
