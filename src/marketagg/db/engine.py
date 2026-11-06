"""Engine construction. DSNs come only from secret files (06)."""

from __future__ import annotations

from sqlalchemy import Engine, create_engine

from marketagg.settings import ConfigError, Settings, load_settings, read_secret


def database_url_from_settings(settings: Settings | None = None) -> str:
    resolved = settings or load_settings()
    if resolved.database_url_file is None:
        raise ConfigError(["DATABASE_URL_FILE is not configured"])
    return normalize_url(read_secret(resolved.database_url_file).get_secret_value())


def normalize_url(url: str) -> str:
    """Use the psycopg 3 driver for plain ``postgresql://`` URLs."""
    for prefix in ("postgresql://", "postgres://"):
        if url.startswith(prefix):
            return "postgresql+psycopg://" + url[len(prefix):]
    return url


def make_engine(
    url: str,
    *,
    application_name: str = "marketagg",
    pool_size: int = 5,
    statement_timeout_ms: int | None = 15_000,
) -> Engine:
    options = f"-c application_name={application_name} -c timezone=UTC"
    if statement_timeout_ms is not None:
        options += f" -c statement_timeout={statement_timeout_ms}"
    return create_engine(
        normalize_url(url),
        pool_size=pool_size,
        max_overflow=0,
        pool_pre_ping=True,
        connect_args={"options": options, "connect_timeout": 10},
    )
