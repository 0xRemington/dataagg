from __future__ import annotations

import io
import json
import logging
import os
import socket
from pathlib import Path

import pytest

from marketagg.cli import EXIT_INVALID_CONFIG, EXIT_OK, main
from marketagg.logging import REDACTED, configure_logging, get_logger
from marketagg.settings import (
    ENV_FIELDS,
    FORBIDDEN_INLINE_SECRETS,
    ConfigError,
    Profile,
    ProviderName,
    check_settings,
    load_settings,
    parse_env_file,
    read_secret,
)


def _write_secret(path: Path, value: str) -> Path:
    path.write_text(value + "\n", encoding="utf-8")
    path.chmod(0o600)
    return path


# -- Profiles and budget validation --------------------------------------------


def test_fixture_profile_defaults_to_fake_and_no_live_data() -> None:
    settings = load_settings("fixture", {})
    assert settings.profile is Profile.FIXTURE
    assert settings.provider is ProviderName.FAKE
    assert settings.live_data_enabled is False
    assert check_settings(settings) == []


def test_profile_falls_back_to_marketagg_env() -> None:
    assert load_settings(None, {"MARKETAGG_ENV": "fixture"}).profile is Profile.FIXTURE
    assert load_settings(None, {}).profile is Profile.DEVELOPMENT


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("REQUESTS_PER_MINUTE", "0"),
        ("REQUESTS_PER_MINUTE", "-3"),
        ("REQUESTS_PER_MINUTE", "ten"),
        ("PLANNED_UTILIZATION_LIMIT", "1.5"),
        ("PLANNED_UTILIZATION_LIMIT", "0"),
        ("PLANNED_UTILIZATION_LIMIT", "nan"),
        ("PROVIDER_CONCURRENCY", "0"),
        ("MAX_EXPORT_ROWS", "0"),
    ],
)
def test_invalid_budget_is_rejected_with_variable_name(name: str, value: str) -> None:
    with pytest.raises(ConfigError) as excinfo:
        load_settings("fixture", {name: value})
    assert any(problem.startswith(name) for problem in excinfo.value.problems)


def test_unknown_profile_is_rejected() -> None:
    with pytest.raises(ConfigError, match="unknown profile"):
        load_settings("staging", {})


# -- Live-source gating ------------------------------------------------------------


def test_live_flag_alone_cannot_enable_a_live_source() -> None:
    with pytest.raises(ConfigError, match="SOURCE_REVIEW_ID"):
        load_settings("development", {"PROVIDER": "yahoo", "LIVE_DATA_ENABLED": "true"})


def test_live_flag_contradicts_fake_provider() -> None:
    with pytest.raises(ConfigError, match="fake-only"):
        load_settings(
            "development", {"LIVE_DATA_ENABLED": "true", "SOURCE_REVIEW_ID": "SR-1"}
        )


def test_non_fake_provider_requires_explicit_live_enablement() -> None:
    with pytest.raises(ConfigError, match="LIVE_DATA_ENABLED=true"):
        load_settings("development", {"PROVIDER": "yahoo"})


def test_fixture_profile_never_allows_live_sources() -> None:
    env = {"PROVIDER": "yahoo", "LIVE_DATA_ENABLED": "true", "SOURCE_REVIEW_ID": "SR-1"}
    with pytest.raises(ConfigError, match="fixture profile"):
        load_settings("fixture", env)
    assert load_settings("development", env).live_data_enabled is True


def test_licensed_live_source_requires_existing_credentials(tmp_path: Path) -> None:
    env = {"PROVIDER": "licensed", "LIVE_DATA_ENABLED": "true", "SOURCE_REVIEW_ID": "SR-2"}
    with pytest.raises(ConfigError, match="PROVIDER_CREDENTIALS_FILE"):
        load_settings("development", env)

    missing = load_settings(
        "development", {**env, "PROVIDER_CREDENTIALS_FILE": str(tmp_path / "absent")}
    )
    assert [f.severity for f in check_settings(missing)] == ["error"]

    creds = _write_secret(tmp_path / "creds", "vendor-key-123456")
    present = load_settings("development", {**env, "PROVIDER_CREDENTIALS_FILE": str(creds)})
    assert check_settings(present) == []


def test_invalid_boolean_is_rejected() -> None:
    with pytest.raises(ConfigError, match="LIVE_DATA_ENABLED"):
        load_settings("development", {"LIVE_DATA_ENABLED": "maybe"})


# -- Secrets ---------------------------------------------------------------------


def test_inline_secrets_are_refused() -> None:
    with pytest.raises(ConfigError, match="DATABASE_URL must not be set inline"):
        load_settings("development", {"DATABASE_URL": "postgresql://u:p@db/x"})


def test_public_bind_requires_api_tokens() -> None:
    with pytest.raises(ConfigError, match="not loopback"):
        load_settings("development", {"API_BIND_HOST": "0.0.0.0"})  # noqa: S104
    assert load_settings("development", {"API_BIND_HOST": "::1"}).api_bind_host == "::1"


def test_production_requires_secret_files(tmp_path: Path) -> None:
    settings = load_settings(
        "production",
        {
            "DATABASE_URL_FILE": str(tmp_path / "db"),
            "API_READ_TOKEN_FILE": str(tmp_path / "read"),
            "API_EXPORT_TOKEN_FILE": str(tmp_path / "export"),
            "BACKUP_KEY_FILE": "",
        },
    )
    errors = [f.message for f in check_settings(settings) if f.severity == "error"]
    assert len(errors) == 3

    for name in ("db", "read", "export"):
        _write_secret(tmp_path / name, f"value-for-{name}-xyz")
    errors = [f.message for f in check_settings(settings) if f.severity == "error"]
    assert errors == []


@pytest.mark.skipif(os.name != "posix", reason="POSIX permission bits")
def test_world_readable_secret_file_warns(tmp_path: Path) -> None:
    path = _write_secret(tmp_path / "db", "postgresql://u:p@h/db")
    path.chmod(0o644)
    settings = load_settings("development", {"DATABASE_URL_FILE": str(path)})
    findings = check_settings(settings)
    assert [f.severity for f in findings] == ["warning"]
    assert "group/world" in findings[0].message


def test_read_secret_never_reveals_value_in_repr(tmp_path: Path) -> None:
    secret = read_secret(_write_secret(tmp_path / "token", "tok-abcdef-123456"))
    assert secret.get_secret_value() == "tok-abcdef-123456"
    assert "tok-abcdef" not in repr(secret) and "tok-abcdef" not in str(secret)


def test_redacted_config_hides_backup_credentials_and_hash_is_stable() -> None:
    env = {"BACKUP_DESTINATION": "s3://backup-user:hunter2secret@bucket/prefix"}
    first = load_settings("development", env)
    redacted = first.redacted()
    assert "hunter2secret" not in json.dumps(redacted)
    assert redacted["backup_destination"].startswith("s3://backup-user:[REDACTED]@")
    assert first.config_hash() == load_settings("development", env).config_hash()
    assert first.config_hash() != load_settings("development", {}).config_hash()


# -- Logging ---------------------------------------------------------------------


def _capture(secrets: tuple[str, ...] = ()) -> io.StringIO:
    stream = io.StringIO()
    configure_logging("DEBUG", secrets=secrets, stream=stream)
    return stream


def test_logs_are_json_utc_and_redact_tokens() -> None:
    stream = _capture(secrets=("registered-secret-value",))
    log = get_logger("test")
    log.info(
        "connecting with Authorization: Bearer abc.def.ghi to postgresql://app:pw123@db/x",
        extra={"api_token": "token=zzz-secret", "note": "registered-secret-value used"},
    )
    try:
        raise RuntimeError("failed password=opensesame")
    except RuntimeError:
        log.exception("boom")
    lines = [json.loads(line) for line in stream.getvalue().splitlines()]
    text = stream.getvalue()
    for leaked in ("abc.def.ghi", "pw123", "zzz-secret", "registered-secret-value",
                   "opensesame"):
        assert leaked not in text
    assert REDACTED in lines[0]["msg"]
    assert lines[0]["ts"].endswith("Z")
    assert lines[1]["level"] == "ERROR" and "RuntimeError" in lines[1]["exc"]


def test_logging_redaction_keeps_ordinary_text() -> None:
    stream = _capture()
    get_logger("test").info("fetched 24 rows for SYNA", extra={"rows": 24})
    record = json.loads(stream.getvalue())
    assert record["msg"] == "fetched 24 rows for SYNA"
    assert record["rows"] == 24
    assert record["logger"] == "marketagg.test"


@pytest.fixture(autouse=True)
def _reset_marketagg_logger() -> None:
    logger = logging.getLogger("marketagg")
    for handler in list(logger.handlers):
        logger.removeHandler(handler)


# -- CLI ---------------------------------------------------------------------------


@pytest.fixture
def clean_env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    for name in ("MARKETAGG_ENV", *ENV_FIELDS, *FORBIDDEN_INLINE_SECRETS):
        monkeypatch.delenv(name, raising=False)
    return monkeypatch


def test_cli_config_check_fixture_exits_zero(
    clean_env: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(["config", "check", "--profile", "fixture", "--json"]) == EXIT_OK
    result = json.loads(capsys.readouterr().out)
    assert result["ok"] is True
    assert result["config"]["provider"] == "fake"
    assert len(result["config_hash"]) == 64


def test_cli_invalid_budget_exits_nonzero(
    clean_env: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    clean_env.setenv("REQUESTS_PER_MINUTE", "0")
    assert main(["config", "check", "--profile", "fixture"]) == EXIT_INVALID_CONFIG
    assert "REQUESTS_PER_MINUTE" in capsys.readouterr().err


def test_cli_env_file_is_applied_beneath_process_env(
    clean_env: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    env_file = tmp_path / "test.env"
    env_file.write_text("# comment\nREQUESTS_PER_MINUTE=7\n", encoding="utf-8")
    assert main(["config", "check", "--profile", "fixture", "--env-file", str(env_file),
                 "--json"]) == EXIT_OK
    assert json.loads(capsys.readouterr().out)["config"]["requests_per_minute"] == 7

    clean_env.setenv("REQUESTS_PER_MINUTE", "9")
    assert main(["config", "check", "--profile", "fixture", "--env-file", str(env_file),
                 "--json"]) == EXIT_OK
    assert json.loads(capsys.readouterr().out)["config"]["requests_per_minute"] == 9


def test_env_example_is_a_valid_development_configuration() -> None:
    root = Path(__file__).resolve().parents[2]
    env = parse_env_file(root / ".env.example")
    settings = load_settings(env["MARKETAGG_ENV"], env)
    assert settings.provider is ProviderName.FAKE
    assert settings.live_data_enabled is False


def test_offline_tests_cannot_open_network_connections() -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock, pytest.raises(
        RuntimeError, match="network access in an offline test"
    ):
        sock.connect(("127.0.0.1", 9))
