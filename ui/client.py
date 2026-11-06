"""HTTP client for the dashboard. The UI never touches PostgreSQL directly (ADR-011).

``MARKETAGG_UI_FIXTURE_DIR`` switches to a fixture client that serves recorded
API responses (used by tests and offline demos).
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Protocol

import httpx


class ApiUnavailable(RuntimeError):
    pass


class ApiClient(Protocol):
    def get(self, path: str, **params: Any) -> dict[str, Any]: ...


class HttpClient:
    def __init__(self, base_url: str, token: str, timeout: float = 15.0) -> None:
        self._client = httpx.Client(base_url=base_url, timeout=timeout,
                                    headers={"Authorization": f"Bearer {token}"})

    def get(self, path: str, **params: Any) -> dict[str, Any]:
        clean = {k: v for k, v in params.items() if v is not None}
        try:
            response = self._client.get(path, params=clean)
        except httpx.HTTPError as exc:
            raise ApiUnavailable(f"API unreachable: {type(exc).__name__}") from exc
        body: dict[str, Any] = response.json()
        if response.status_code >= 400:
            raise ApiUnavailable(f"{body.get('code', response.status_code)}: "
                                 f"{body.get('message', 'request failed')}")
        return body


class FixtureClient:
    """Serves ``<route>.json`` files, e.g. ``v1_bars.json`` for ``/v1/bars``."""

    def __init__(self, directory: Path) -> None:
        self.directory = directory

    def get(self, path: str, **params: Any) -> dict[str, Any]:
        name = path.strip("/").replace("/", "_") + ".json"
        target = self.directory / name
        if not target.is_file():
            raise ApiUnavailable(f"fixture missing for {path}")
        body: dict[str, Any] = json.loads(target.read_text())
        if "code" in body and "request_id" in body and "items" not in body:
            raise ApiUnavailable(f"{body['code']}: {body['message']}")
        return body


def make_client() -> ApiClient:
    fixtures = os.environ.get("MARKETAGG_UI_FIXTURE_DIR")
    if fixtures:
        return FixtureClient(Path(fixtures))
    token_file = os.environ.get("API_READ_TOKEN_FILE")
    if not token_file:
        raise ApiUnavailable("API_READ_TOKEN_FILE is not configured")
    token = Path(token_file).read_text(encoding="utf-8").strip()
    return HttpClient(os.environ.get("MARKETAGG_API_URL", "http://127.0.0.1:8000"), token)
