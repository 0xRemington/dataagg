"""Versioned YAML configuration: universe scope and research/collection policies (06).

Configuration contains no secrets. Every loaded document becomes an immutable
``PolicyVersion`` (content-addressed), so a change always yields a new policy ID
and running/exported datasets keep the previous one.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from marketagg.domain.policies import PolicyVersion
from marketagg.settings import ConfigError


class _Strict(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class SymbolConfig(_Strict):
    symbol: str = Field(pattern=r"^[A-Z][A-Z0-9.\-]{0,15}$")
    asset_class: Literal["equity", "etf"]
    currency: str = "USD"
    calendar_id: str = "XNYS"


class OptionsScope(_Strict):
    priority_expiries: int = Field(default=4, ge=1, le=12)
    dte_targets: tuple[int, ...] = (30, 45, 60, 90)
    dte_min: int = Field(default=30, ge=0)
    dte_max: int = Field(default=90, ge=1)
    other_expiries_max: int = Field(default=6, ge=0, le=40)
    broad_expiries_max: int = Field(default=24, ge=0, le=80)
    retention: Literal["all_returned"] = "all_returned"
    old_last_trade_minutes: int = Field(default=60, gt=0)
    iv_plausible_max: float = Field(default=5.0, gt=0)

    @model_validator(mode="after")
    def _band(self) -> OptionsScope:
        if self.dte_min > self.dte_max:
            raise ValueError("dte_min must not exceed dte_max")
        if any(not self.dte_min <= t <= self.dte_max for t in self.dte_targets):
            raise ValueError("every DTE target must lie inside [dte_min, dte_max]")
        return self


class BarsScope(_Strict):
    minute_overlap_minutes: int = Field(default=20, ge=1, le=120)
    minute_safe_lag_seconds: int = Field(default=80, ge=0)
    publication_grace_minutes: int = Field(default=3, ge=0)
    daily_history_years: int = Field(default=5, ge=1, le=30)
    daily_chunk_overlap_days: int = Field(default=5, ge=0)
    minute_bootstrap_days: int = Field(default=7, ge=1)
    max_repair_cycles: int = Field(default=3, ge=1)


class UniverseConfig(_Strict):
    version: int = Field(ge=1)
    provider: str
    symbols: tuple[SymbolConfig, ...] = Field(min_length=1)
    options: OptionsScope = OptionsScope()
    bars: BarsScope = BarsScope()

    @model_validator(mode="after")
    def _unique(self) -> UniverseConfig:
        names = [s.symbol for s in self.symbols]
        if len(names) != len(set(names)):
            raise ValueError("duplicate symbols in universe")
        return self


class AgeLimit(_Strict):
    option_seconds: int = Field(gt=0)
    spot_seconds: int = Field(gt=0)


class RetryPolicy(_Strict):
    max_attempts: int = Field(default=3, ge=1, le=10)
    base_seconds: float = Field(default=2.0, gt=0)
    cap_seconds: float = Field(default=60.0, gt=0)
    rate_limit_cooldown_seconds: int = Field(default=900, gt=0)
    rate_limit_cooldown_max_seconds: int = Field(default=7200, gt=0)
    schema_change_cooldown_seconds: int = Field(default=3600, gt=0)
    unavailable_failures_to_open: int = Field(default=5, ge=1)
    unavailable_cooldown_seconds: int = Field(default=300, gt=0)


class SchedulePolicy(_Strict):
    lease_seconds: int = Field(default=180, gt=0)
    heartbeat_seconds: int = Field(default=20, gt=0)
    provider_timeout_seconds: int = Field(default=30, gt=0)
    priority_chain_deadline_seconds: int = Field(default=120, gt=0)
    backlog_shed_threshold: int = Field(default=50, ge=1)
    session_minutes: int = Field(default=390, gt=0)


class PoliciesConfig(_Strict):
    version: int = Field(ge=1)
    age_limits: dict[str, AgeLimit]
    retry: RetryPolicy = RetryPolicy()
    schedule: SchedulePolicy = SchedulePolicy()
    max_observation_skew_seconds: int = Field(default=120, gt=0)
    reconciliation_price_abs: str = "0.01"
    reconciliation_price_rel: float = 1e-5
    reconciliation_volume_rel: float = 0.01

    @model_validator(mode="after")
    def _required_ages(self) -> PoliciesConfig:
        missing = {"1m", "5m", "15m", "4h", "1d"} - set(self.age_limits)
        if missing:
            raise ValueError(f"age_limits missing timeframes: {sorted(missing)}")
        return self


def _load(path: Path) -> dict[str, Any]:
    try:
        loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise ConfigError([f"{path}: {exc}"]) from None
    if not isinstance(loaded, dict):
        raise ConfigError([f"{path}: expected a mapping at top level"])
    return loaded


def load_universe(path: Path) -> UniverseConfig:
    try:
        return UniverseConfig.model_validate(_load(path))
    except ValueError as exc:
        raise ConfigError([f"{path}: {exc}"]) from None


def load_policies(path: Path) -> PoliciesConfig:
    try:
        return PoliciesConfig.model_validate(_load(path))
    except ValueError as exc:
        raise ConfigError([f"{path}: {exc}"]) from None


def policy_version(kind: str, model: BaseModel, code_sha: str = "unversioned") -> PolicyVersion:
    return PolicyVersion.create(kind, model.model_dump(mode="json"), code_sha)
