"""Construct long-running services from typed settings (used by the CLI)."""

from __future__ import annotations

from pathlib import Path

from sqlalchemy import Engine

from marketagg.api.app import ApiServices
from marketagg.calendar import SessionCalendar
from marketagg.config import PoliciesConfig, UniverseConfig, load_policies, load_universe
from marketagg.db.engine import database_url_from_settings, make_engine
from marketagg.research.datasets import DatasetService
from marketagg.research.repository import ResearchRepository
from marketagg.runtime import Runtime, bootstrap, build_provider
from marketagg.settings import ConfigError, Settings, read_secret


def engine_for(settings: Settings, application: str, *, pool_size: int = 5,
               statement_timeout_ms: int | None = 15_000) -> Engine:
    return make_engine(database_url_from_settings(settings), application_name=application,
                       pool_size=pool_size, statement_timeout_ms=statement_timeout_ms)


def configs(settings: Settings) -> tuple[UniverseConfig, PoliciesConfig]:
    universe = load_universe(settings.universe_config)
    policies = load_policies(settings.policy_config)
    if universe.provider != settings.provider.value:
        raise ConfigError([f"{settings.universe_config} targets provider {universe.provider!r} "
                           f"but PROVIDER={settings.provider.value}"])
    return universe, policies


def runtime_for(settings: Settings, application: str, *,
                provision_partitions: bool = True) -> Runtime:
    universe, policies = configs(settings)
    calendar = SessionCalendar()
    engine = engine_for(settings, application)
    provider = build_provider(settings, calendar)
    return bootstrap(engine, provider, universe, policies,
                     requests_per_minute=settings.requests_per_minute,
                     payload_root=settings.payload_root,
                     provision_partitions=provision_partitions)


def _tokens(path: Path | None) -> tuple[str, ...]:
    return () if path is None else (read_secret(path).get_secret_value(),)


def api_services_for(settings: Settings) -> ApiServices:
    rt = runtime_for(settings, "marketagg-api", provision_partitions=False)
    research = ResearchRepository(rt.engine, rt.calendar, rt.policy_ids)
    export_tokens = _tokens(settings.api_export_token_file)
    signing_key = (export_tokens[0].encode() if export_tokens else b"")
    datasets = None
    if signing_key:
        datasets = DatasetService(rt.engine, research, settings.export_root,
                                  signing_key=signing_key, max_rows=settings.max_export_rows)
    return ApiServices(engine=rt.engine, research=research, calendar=rt.calendar,
                       policies=rt.policies, read_tokens=_tokens(settings.api_read_token_file),
                       export_tokens=export_tokens, datasets=datasets)
