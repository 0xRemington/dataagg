"""Typed configuration (docs/build-guide/06 "Configuration and secrets contract").

Precedence: profile defaults < ``--env-file`` < process environment. Secrets are
only ever referenced by ``*_FILE`` paths; inline secret variables are refused.
Validation is pure (``load_settings``); filesystem checks of secret files are
separate (``check_settings``) so tests and the CLI can report both.
"""

from __future__ import annotations

import hashlib
import ipaddress
import json
import os
import re
import stat
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, SecretStr, ValidationError, model_validator


class Profile(StrEnum):
    FIXTURE = "fixture"
    DEVELOPMENT = "development"
    PRODUCTION = "production"


class ProviderName(StrEnum):
    FAKE = "fake"
    YAHOO = "yahoo"
    LICENSED = "licensed"


LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR"]

# Environment variable -> Settings field. Only these names are read.
ENV_FIELDS: dict[str, str] = {
    "PROVIDER": "provider",
    "LIVE_DATA_ENABLED": "live_data_enabled",
    "SOURCE_REVIEW_ID": "source_review_id",
    "PROVIDER_CREDENTIALS_FILE": "provider_credentials_file",
    "DATABASE_URL_FILE": "database_url_file",
    "API_READ_TOKEN_FILE": "api_read_token_file",
    "API_EXPORT_TOKEN_FILE": "api_export_token_file",
    "API_BIND_HOST": "api_bind_host",
    "UNIVERSE_CONFIG": "universe_config",
    "POLICY_CONFIG": "policy_config",
    "REQUESTS_PER_MINUTE": "requests_per_minute",
    "PLANNED_UTILIZATION_LIMIT": "planned_utilization_limit",
    "PROVIDER_CONCURRENCY": "provider_concurrency",
    "HOT_RETENTION_DAYS": "hot_retention_days",
    "RAW_RETENTION_DAYS": "raw_retention_days",
    "MAX_LOCAL_DATA_GIB": "max_local_data_gib",
    "MAX_EXPORT_ROWS": "max_export_rows",
    "BACKUP_DESTINATION": "backup_destination",
    "BACKUP_KEY_FILE": "backup_key_file",
    "LOG_LEVEL": "log_level",
    "DATA_ROOT": "data_root",
    "ARCHIVE_OFFSITE": "archive_offsite",
    "API_PORT": "api_port",
}

# Inline secrets would end up in process listings, crash dumps and logs.
FORBIDDEN_INLINE_SECRETS = (
    "DATABASE_URL",
    "API_READ_TOKEN",
    "API_EXPORT_TOKEN",
    "BACKUP_KEY",
    "PROVIDER_CREDENTIALS",
)

_SECRET_FILE_FIELDS = (
    "provider_credentials_file",
    "database_url_file",
    "api_read_token_file",
    "api_export_token_file",
    "backup_key_file",
)

_PROFILE_DEFAULTS: dict[Profile, dict[str, Any]] = {
    Profile.FIXTURE: {},
    Profile.DEVELOPMENT: {},
    Profile.PRODUCTION: {
        "database_url_file": "/run/secrets/database_url",
        "api_read_token_file": "/run/secrets/api_read_token",
        "api_export_token_file": "/run/secrets/api_export_token",
        "backup_key_file": "/run/secrets/backup_key",
    },
}


class ConfigError(ValueError):
    """Configuration is invalid; ``problems`` lists every reason."""

    def __init__(self, problems: list[str]) -> None:
        super().__init__("; ".join(problems))
        self.problems = problems


class Settings(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    profile: Profile
    provider: ProviderName = ProviderName.FAKE
    live_data_enabled: bool = False
    source_review_id: str | None = None
    provider_credentials_file: Path | None = None

    database_url_file: Path | None = None
    api_read_token_file: Path | None = None
    api_export_token_file: Path | None = None
    api_bind_host: str = "127.0.0.1"

    universe_config: Path = Path("config/universe.yaml")
    policy_config: Path = Path("config/policies.yaml")

    # Self-imposed pacing (03 "Request accounting"), not a provider allowance.
    requests_per_minute: int = Field(default=10, gt=0, le=600)
    planned_utilization_limit: float = Field(default=0.70, gt=0, le=1)
    provider_concurrency: int = Field(default=1, ge=1, le=8)

    hot_retention_days: int = Field(default=90, gt=0)
    raw_retention_days: int = Field(default=30, ge=0)
    max_local_data_gib: int = Field(default=100, gt=0)
    max_export_rows: int = Field(default=5_000_000, gt=0)

    backup_destination: str | None = None
    backup_key_file: Path | None = None

    log_level: LogLevel = "INFO"

    # Local storage roots (payload spool, archives, exports) live under DATA_ROOT.
    data_root: Path = Path("data")
    # Off-host archive copies: a mounted remote directory; required before pruning.
    archive_offsite: Path | None = None
    api_port: int = Field(default=8000, ge=1, le=65535)

    @property
    def payload_root(self) -> Path:
        return self.data_root / "payloads"

    @property
    def archive_root(self) -> Path:
        return self.data_root / "archives"

    @property
    def export_root(self) -> Path:
        return self.data_root / "exports"

    @model_validator(mode="after")
    def _reject_contradictions(self) -> Settings:
        problems: list[str] = []
        if self.profile is Profile.FIXTURE and (
            self.provider is not ProviderName.FAKE or self.live_data_enabled
        ):
            problems.append("fixture profile requires PROVIDER=fake and LIVE_DATA_ENABLED=false")
        if self.live_data_enabled and self.provider is ProviderName.FAKE:
            problems.append("LIVE_DATA_ENABLED=true contradicts fake-only provider capabilities")
        if self.provider is not ProviderName.FAKE and not self.live_data_enabled:
            problems.append(
                f"PROVIDER={self.provider} contacts a live source; set LIVE_DATA_ENABLED=true "
                "only after a recorded source review"
            )
        if self.live_data_enabled and not self.source_review_id:
            problems.append("live data requires SOURCE_REVIEW_ID (a recorded rights review)")
        if (
            self.live_data_enabled
            and self.provider is ProviderName.LICENSED
            and self.provider_credentials_file is None
        ):
            problems.append("licensed provider requires PROVIDER_CREDENTIALS_FILE")
        if not _is_loopback(self.api_bind_host) and (
            self.api_read_token_file is None or self.api_export_token_file is None
        ):
            problems.append(
                f"API_BIND_HOST={self.api_bind_host} is not loopback; configure "
                "API_READ_TOKEN_FILE and API_EXPORT_TOKEN_FILE"
            )
        if problems:
            raise ValueError("; ".join(problems))
        return self

    def secret_files(self) -> dict[str, Path]:
        return {
            name: path for name in _SECRET_FILE_FIELDS if (path := getattr(self, name)) is not None
        }

    def redacted(self) -> dict[str, Any]:
        """JSON-safe view for logs/startup. Secret values are never loaded here."""
        data = self.model_dump(mode="json")
        if data["backup_destination"]:
            data["backup_destination"] = redact_url_credentials(data["backup_destination"])
        return data

    def config_hash(self) -> str:
        canonical = json.dumps(self.redacted(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode()).hexdigest()


def _is_loopback(host: str) -> bool:
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


_URL_USERINFO = re.compile(r"(?P<scheme>[a-zA-Z][a-zA-Z0-9+.-]*://)(?P<user>[^:/@\s]*):[^@/\s]*@")


def redact_url_credentials(value: str) -> str:
    return _URL_USERINFO.sub(r"\g<scheme>\g<user>:[REDACTED]@", value)


_TRUE = {"1", "true", "yes", "on"}
_FALSE = {"0", "false", "no", "off"}


def _coerce_env(field: str, raw: str) -> Any:
    value = raw.strip()
    if value == "":
        return None
    if field == "live_data_enabled":
        lowered = value.lower()
        if lowered in _TRUE:
            return True
        if lowered in _FALSE:
            return False
        raise ConfigError([f"LIVE_DATA_ENABLED: expected true/false, got {value!r}"])
    return value


def parse_env_file(path: Path) -> dict[str, str]:
    """Minimal ``KEY=VALUE`` reader for ``--env-file``; no interpolation."""
    values: dict[str, str] = {}
    for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        key, sep, value = stripped.partition("=")
        if not sep:
            raise ConfigError([f"{path}:{lineno}: expected KEY=VALUE"])
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def resolve_profile(profile: str | Profile | None, environ: Mapping[str, str]) -> Profile:
    name = profile or environ.get("MARKETAGG_ENV") or Profile.DEVELOPMENT.value
    try:
        return Profile(name)
    except ValueError:
        choices = ", ".join(p.value for p in Profile)
        raise ConfigError([f"unknown profile {name!r}; expected one of {choices}"]) from None


def load_settings(
    profile: str | Profile | None = None,
    environ: Mapping[str, str] | None = None,
) -> Settings:
    env = dict(os.environ) if environ is None else dict(environ)
    resolved = resolve_profile(profile, env)

    inline = [name for name in FORBIDDEN_INLINE_SECRETS if env.get(name)]
    if inline:
        raise ConfigError(
            [f"{name} must not be set inline; use {name}_FILE" for name in inline]
        )

    values: dict[str, Any] = {"profile": resolved, **_PROFILE_DEFAULTS[resolved]}
    for env_name, field in ENV_FIELDS.items():
        if env_name in env:
            coerced = _coerce_env(field, env[env_name])
            if coerced is None:
                values.pop(field, None)
            else:
                values[field] = coerced
    try:
        return Settings.model_validate(values)
    except ValidationError as exc:
        raise ConfigError([_format_error(err) for err in exc.errors()]) from None


def _format_error(err: Any) -> str:
    loc = ".".join(str(part) for part in err["loc"])
    env_name = next((k for k, v in ENV_FIELDS.items() if v == loc), loc)
    message = str(err["msg"]).removeprefix("Value error, ")
    return f"{env_name}: {message}" if env_name else message


@dataclass(frozen=True)
class Finding:
    severity: Literal["error", "warning"]
    message: str


def check_settings(settings: Settings) -> list[Finding]:
    """Filesystem checks: required secret files exist, are non-empty, are private."""
    required: set[str] = set()
    if settings.profile is Profile.PRODUCTION:
        required |= {"database_url_file", "api_read_token_file", "api_export_token_file"}
    if settings.live_data_enabled and settings.provider is ProviderName.LICENSED:
        required.add("provider_credentials_file")

    findings: list[Finding] = []
    for name in sorted(required):
        if getattr(settings, name) is None:
            findings.append(Finding("error", f"{name.upper()} is required for this profile"))
    for name, path in sorted(settings.secret_files().items()):
        severity: Literal["error", "warning"] = "error" if name in required else "warning"
        if not path.is_file():
            findings.append(Finding(severity, f"{name.upper()}: {path} is not a readable file"))
            continue
        if path.stat().st_size == 0:
            findings.append(Finding(severity, f"{name.upper()}: {path} is empty"))
        if os.name == "posix" and path.stat().st_mode & (stat.S_IRWXG | stat.S_IRWXO):
            findings.append(
                Finding("warning", f"{name.upper()}: {path} is group/world accessible")
            )
    if settings.profile is Profile.PRODUCTION and not settings.backup_destination:
        findings.append(Finding("warning", "BACKUP_DESTINATION is not configured"))
    return findings


def read_secret(path: Path) -> SecretStr:
    """Read one secret file; the value stays wrapped so repr/logging cannot leak it."""
    return SecretStr(path.read_text(encoding="utf-8").strip())
