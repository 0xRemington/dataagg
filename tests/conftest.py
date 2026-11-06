"""Shared pytest configuration.

- ``live`` tests are skipped unless ``--run-live`` is given (explicit opt-in).
- Unit and contract tests are network-free: outbound socket connections fail
  loudly unless a test is marked ``integration`` or ``live``.
"""

from __future__ import annotations

import socket
from collections.abc import Iterator
from typing import Any

import pytest

pytest_plugins = ["dbfixtures"]


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--run-live",
        action="store_true",
        default=False,
        help="run tests marked 'live' (contacts real providers; requires approved review)",
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if config.getoption("--run-live"):
        return
    skip_live = pytest.mark.skip(reason="live test: opt in with --run-live")
    for item in items:
        if "live" in item.keywords:
            item.add_marker(skip_live)


class NetworkBlockedError(RuntimeError):
    pass


@pytest.fixture(autouse=True)
def _block_network(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch
) -> Iterator[None]:
    if request.node.get_closest_marker("integration") or request.node.get_closest_marker("live"):
        yield
        return

    def guarded_connect(self: socket.socket, address: Any) -> None:
        if self.family == socket.AF_UNIX:
            return original_connect(self, address)
        raise NetworkBlockedError(f"network access in an offline test: {address!r}")

    original_connect = socket.socket.connect
    monkeypatch.setattr(socket.socket, "connect", guarded_connect)
    yield
