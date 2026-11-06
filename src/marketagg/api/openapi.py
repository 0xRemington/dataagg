"""Generate the checked-in OpenAPI document without a database connection."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

from marketagg.api.app import ApiServices, create_app


def openapi_document() -> dict[str, Any]:
    # Route/schema generation does not touch services; placeholders suffice.
    services = ApiServices(engine=MagicMock(), research=MagicMock(), calendar=MagicMock(),
                           policies=MagicMock(), read_tokens=(), export_tokens=())
    document: dict[str, Any] = create_app(services).openapi()
    return document


def write_openapi(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(openapi_document(), indent=2, sort_keys=True) + "\n")
