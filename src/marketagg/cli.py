"""``marketagg`` command line. The foundation owner owns command names (04)."""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
from collections.abc import Callable, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, cast

from marketagg import __version__
from marketagg.logging import configure_logging, get_logger
from marketagg.settings import (
    ConfigError,
    Profile,
    Settings,
    check_settings,
    load_settings,
    parse_env_file,
    read_secret,
)

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_INVALID_CONFIG = 2

log = get_logger(__name__)


def _profile_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--profile", choices=[p.value for p in Profile],
                        help="configuration profile (default: $MARKETAGG_ENV or development)")
    parser.add_argument("--env-file", type=Path,
                        help="KEY=VALUE file applied beneath the process environment")


def _build_parser() -> argparse.ArgumentParser:  # noqa: PLR0915 - flat command table
    parser = argparse.ArgumentParser(
        prog="marketagg",
        description="Private options and underlying market-data aggregator (research use).")
    parser.add_argument("--version", action="version", version=f"marketagg {__version__}")
    commands = parser.add_subparsers(dest="command", required=True, metavar="COMMAND")

    config = commands.add_parser("config", help="inspect and validate configuration")
    config_commands = config.add_subparsers(dest="config_command", required=True)
    check = config_commands.add_parser(
        "check", help="validate settings and secret file references; exit 2 if invalid")
    _profile_args(check)
    check.add_argument("--json", action="store_true", help="print machine-readable result")
    check.set_defaults(handler=_config_check)

    migrate = commands.add_parser("migrate", help="apply database migrations (owner role)")
    _profile_args(migrate)
    migrate.add_argument("--bootstrap", action="store_true",
                         help="then register provider, policies, instruments and partitions")
    migrate.set_defaults(handler=_migrate)

    boot = commands.add_parser("bootstrap", help="register provider, policies, instruments, "
                                                 "partitions (idempotent)")
    _profile_args(boot)
    boot.set_defaults(handler=_bootstrap)

    crypto = commands.add_parser("binance", help="Binance BTCUSDT candle collector (UPDATE2)")
    _profile_args(crypto)
    crypto.add_argument("action", choices=["bootstrap", "stream"],
                        help="bootstrap: register (owner role); stream: run the collector")
    crypto.set_defaults(handler=_binance)

    for name, handler, help_text in (
        ("scheduler", _scheduler, "run the single authoritative planner"),
        ("worker", _worker, "run a bounded collection/aggregation worker"),
    ):
        service = commands.add_parser(name, help=help_text)
        _profile_args(service)
        service.add_argument("--once", action="store_true", help="one cycle then exit")
        service.add_argument("--id", default=f"{name}-{os.getpid()}")
        service.set_defaults(handler=handler)

    api = commands.add_parser("api", help="research HTTP API")
    api_commands = api.add_subparsers(dest="api_command", required=True)
    serve = api_commands.add_parser("serve", help="serve the private API")
    _profile_args(serve)
    serve.set_defaults(handler=_api_serve)
    openapi = api_commands.add_parser("openapi", help="write the OpenAPI document")
    openapi.add_argument("--output", type=Path, default=Path("docs/api/openapi.json"))
    openapi.set_defaults(handler=_api_openapi)

    schedule = commands.add_parser("schedule", help="dry-run schedule and request budget")
    schedule_commands = schedule.add_subparsers(dest="schedule_command", required=True)
    plan = schedule_commands.add_parser("plan", help="list a session's jobs and budget")
    _profile_args(plan)
    plan.add_argument("--date", type=date.fromisoformat, required=True)
    plan.add_argument("--json", action="store_true")
    plan.set_defaults(handler=_schedule_plan)

    probe = commands.add_parser("probe", help="feasibility probe (T00)", add_help=False)
    probe.set_defaults(handler=_probe)

    ops = commands.add_parser("ops", help="operations: status, backup, restore, archive")
    ops_commands = ops.add_subparsers(dest="ops_command", required=True)
    status = ops_commands.add_parser("status", help="operator status and alerts")
    _profile_args(status)
    status.add_argument("--json", action="store_true")
    status.set_defaults(handler=_ops_status)
    backup = ops_commands.add_parser("backup", help="encrypted catalog-consistent backup")
    _profile_args(backup)
    backup.add_argument("--verify", action="store_true", help="verify after writing")
    backup.add_argument("--destination", type=Path)
    backup.set_defaults(handler=_ops_backup)
    restore = ops_commands.add_parser("restore", help="restore a backup into a NEW database")
    _profile_args(restore)
    restore.add_argument("--backup", type=Path, required=True)
    restore.add_argument("--admin-url-file", type=Path, required=True)
    restore.add_argument("--target-database", required=True)
    restore.add_argument("--target-data-root", type=Path, required=True)
    restore.set_defaults(handler=_ops_restore)
    archive = ops_commands.add_parser("archive", help="archive and activate one month")
    _profile_args(archive)
    archive.add_argument("--month", required=True, help="YYYY-MM")
    archive.add_argument("--dataset", choices=["bar_observations", "option_snapshots"],
                         required=True)
    archive.set_defaults(handler=_ops_archive)

    datasets = commands.add_parser("datasets", help="export jobs")
    dataset_commands = datasets.add_subparsers(dest="datasets_command", required=True)
    run_pending = dataset_commands.add_parser("run-pending", help="build queued exports")
    _profile_args(run_pending)
    run_pending.set_defaults(handler=_datasets_run)

    release = commands.add_parser("release", help="MVP release gate (T19)")
    release_commands = release.add_subparsers(dest="release_command", required=True)
    release_check = release_commands.add_parser("check", help="evaluate release evidence")
    release_check.add_argument("--report", type=Path, required=True)
    release_check.set_defaults(handler=_release_check)
    return parser


def _settings(args: argparse.Namespace) -> Settings:
    environ: dict[str, str] = {}
    if getattr(args, "env_file", None) is not None:
        environ.update(parse_env_file(args.env_file))
    environ.update(os.environ)
    settings = load_settings(getattr(args, "profile", None), environ)
    configure_logging(settings.log_level)
    return settings


def _config_check(args: argparse.Namespace) -> int:
    try:
        settings = _settings(args)
    except (ConfigError, OSError) as exc:
        problems = exc.problems if isinstance(exc, ConfigError) else [str(exc)]
        _emit(args.json, ok=False, errors=problems, warnings=[])
        return EXIT_INVALID_CONFIG
    findings = check_settings(settings)
    errors = [f.message for f in findings if f.severity == "error"]
    warnings = [f.message for f in findings if f.severity == "warning"]
    log.info("config checked",
             extra={"profile": settings.profile.value, "config_hash": settings.config_hash()})
    _emit(args.json, ok=not errors, errors=errors, warnings=warnings,
          config=settings.redacted(), config_hash=settings.config_hash())
    return EXIT_INVALID_CONFIG if errors else EXIT_OK


def _emit(
    as_json: bool, *, ok: bool, errors: list[str], warnings: list[str], **extra: object
) -> None:
    if as_json:
        print(json.dumps({"ok": ok, "errors": errors, "warnings": warnings, **extra}, indent=2))
        return
    for message in errors:
        print(f"error: {message}", file=sys.stderr)
    for message in warnings:
        print(f"warning: {message}", file=sys.stderr)
    if ok:
        config = cast(dict[str, object], extra["config"])
        print(
            f"config ok: profile={config['profile']} provider={config['provider']} "
            f"live_data_enabled={str(config['live_data_enabled']).lower()} "
            f"config_hash={extra['config_hash']}"
        )


def _migrate(args: argparse.Namespace) -> int:
    from marketagg.db.migrate import upgrade  # noqa: PLC0415 - keep CLI import fast
    from marketagg.services import engine_for  # noqa: PLC0415

    settings = _settings(args)
    engine = engine_for(settings, "marketagg-migrate", statement_timeout_ms=None)
    upgrade(engine)
    print("migrations at head")
    if args.bootstrap:  # reference data is written by the owner, before services start
        from marketagg.runtime import enqueue_history_bootstrap  # noqa: PLC0415
        from marketagg.services import runtime_for  # noqa: PLC0415

        rt = runtime_for(settings, "marketagg-bootstrap")
        print(f"bootstrapped provider={rt.provider.provider_id} "
              f"instruments={sorted(rt.instruments)} "
              f"history_jobs_enqueued={enqueue_history_bootstrap(rt)}")
    return EXIT_OK


def _bootstrap(args: argparse.Namespace) -> int:
    from marketagg.services import runtime_for  # noqa: PLC0415

    rt = runtime_for(_settings(args), "marketagg-bootstrap")
    print(json.dumps({"instruments": {k: str(v) for k, v in rt.instruments.items()},
                      "policies": rt.policy_ids}, indent=2))
    return EXIT_OK


def _binance(args: argparse.Namespace) -> int:
    from marketagg.crypto import binance  # noqa: PLC0415
    from marketagg.db.ingest import IngestRepository  # noqa: PLC0415
    from marketagg.services import engine_for  # noqa: PLC0415

    engine = engine_for(_settings(args), "marketagg-binance")
    if args.action == "bootstrap":
        instrument = binance.bootstrap(engine, IngestRepository(engine), datetime.now(UTC))
        print(json.dumps({"symbol": binance.SYMBOL, "instrument": str(instrument),
                          "policy": binance.BARS_POLICY.id}))
        return EXIT_OK
    binance.run(engine)
    return EXIT_OK


def _loop(once: bool, step: Callable[[], Any], interval: float) -> None:
    stopping = False

    def stop(*_: object) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    while not stopping:
        step()
        if once:
            return
        time.sleep(interval)


def _scheduler(args: argparse.Namespace) -> int:
    from marketagg.runtime import make_scheduler  # noqa: PLC0415
    from marketagg.services import runtime_for  # noqa: PLC0415

    service = make_scheduler(runtime_for(_settings(args), "marketagg-scheduler"), args.id)

    def tick() -> None:
        summary = service.tick()
        if summary is None:
            log.warning("scheduler lock held elsewhere; not planning")
        else:
            log.info("planned", extra={"jobs_created": summary.created,
                                       "jobs_shed": summary.shed})

    try:
        _loop(args.once, tick, 5.0)
    finally:
        service.release()
    return EXIT_OK


def _worker(args: argparse.Namespace) -> int:
    from marketagg.db.ingest import IngestRepository  # noqa: PLC0415
    from marketagg.runtime import make_worker  # noqa: PLC0415
    from marketagg.services import runtime_for  # noqa: PLC0415

    settings = _settings(args)
    rt = runtime_for(settings, "marketagg-worker")
    recovered = IngestRepository(rt.engine).finalize_staged()
    if recovered:
        log.warning("published staged batches at recovery time", extra={"count": len(recovered)})
    worker = make_worker(rt, args.id)
    worker.disk_paths = (settings.data_root,)
    _loop(args.once, worker.run_until_idle, 1.0)
    return EXIT_OK


def _api_serve(args: argparse.Namespace) -> int:
    import uvicorn  # noqa: PLC0415

    from marketagg.api.app import create_app  # noqa: PLC0415
    from marketagg.services import api_services_for  # noqa: PLC0415

    settings = _settings(args)
    app = create_app(api_services_for(settings))
    uvicorn.run(app, host=settings.api_bind_host, port=settings.api_port, log_config=None,
                workers=1, server_header=False)
    return EXIT_OK


def _api_openapi(args: argparse.Namespace) -> int:
    from marketagg.api.openapi import write_openapi  # noqa: PLC0415

    write_openapi(args.output)
    print(f"wrote {args.output}")
    return EXIT_OK


def _schedule_plan(args: argparse.Namespace) -> int:
    from marketagg.calendar import SessionCalendar  # noqa: PLC0415
    from marketagg.config import policy_version  # noqa: PLC0415
    from marketagg.scheduler.budgets import BudgetInputs, plan_budget  # noqa: PLC0415
    from marketagg.scheduler.planner import SchedulePlanner  # noqa: PLC0415
    from marketagg.services import configs  # noqa: PLC0415

    settings = _settings(args)
    universe, policies = configs(settings)
    calendar = SessionCalendar()
    session = calendar.session(args.date)
    if session is None:
        print(f"{args.date} is not a session: no jobs planned")
        return EXIT_OK
    planner = SchedulePlanner(calendar, universe, policies,
                              policy_version("collection", universe).id)
    counts: dict[str, int] = {}
    for intent in planner.intents_for_session(session):
        counts[intent.kind] = counts.get(intent.kind, 0) + 1
    budget = plan_budget(BudgetInputs(
        symbols=len(universe.symbols), priority_expiries=universe.options.priority_expiries,
        other_expiries=universe.options.other_expiries_max,
        priority_polls=counts.get("chain_priority", 0) // max(len(universe.symbols), 1),
        hourly_polls=counts.get("chain_other", 0) // max(len(universe.symbols), 1),
        minute_polls=session.minutes_count, session_minutes=session.minutes_count,
        requests_per_minute=settings.requests_per_minute,
        planned_utilization=settings.planned_utilization_limit))
    report = {"session": args.date.isoformat(), "early_close": session.early_close,
              "jobs": counts, "budget": budget.as_dict()}
    print(json.dumps(report, indent=2, default=str))
    return EXIT_OK if budget.feasible else EXIT_FAILED


def _probe(args: argparse.Namespace, rest: Sequence[str]) -> int:
    from marketagg.feasibility import main as probe_main  # noqa: PLC0415

    return probe_main(list(rest))


def _ops_status(args: argparse.Namespace) -> int:
    from marketagg.calendar import SessionCalendar  # noqa: PLC0415
    from marketagg.ops.status import operator_status  # noqa: PLC0415
    from marketagg.services import engine_for  # noqa: PLC0415

    settings = _settings(args)
    report = operator_status(engine_for(settings, "marketagg-ops"), SessionCalendar(),
                             datetime.now(UTC), data_paths=(settings.data_root,))
    print(json.dumps({"severity": report.severity, **report.sections}, indent=2, default=str))
    return EXIT_OK if report.severity != "critical" else EXIT_FAILED


def _ops_backup(args: argparse.Namespace) -> int:
    from marketagg.db.engine import database_url_from_settings  # noqa: PLC0415
    from marketagg.ops.backup import create_backup, load_key, verify_backup  # noqa: PLC0415
    from marketagg.services import engine_for  # noqa: PLC0415

    settings = _settings(args)
    destination = args.destination or (Path(settings.backup_destination)
                                       if settings.backup_destination else None)
    if destination is None or settings.backup_key_file is None:
        print("error: BACKUP_DESTINATION and BACKUP_KEY_FILE are required", file=sys.stderr)
        return EXIT_INVALID_CONFIG
    key = load_key(settings.backup_key_file)
    result = create_backup(engine_for(settings, "marketagg-backup",
                                      statement_timeout_ms=None),
                           database_url_from_settings(settings), destination, key,
                           archive_root=settings.archive_root,
                           payload_root=settings.payload_root)
    if args.verify:
        verify_backup(result.path, key)
    print(json.dumps({"backup": str(result.path), "verified": True,
                      "entries": len(result.manifest["entries"]),
                      "rpo_known_at_max": result.manifest["rpo_known_at_max"]}, indent=2))
    return EXIT_OK


def _ops_restore(args: argparse.Namespace) -> int:
    from marketagg.db.engine import database_url_from_settings  # noqa: PLC0415
    from marketagg.ops.backup import load_key, restore_backup  # noqa: PLC0415

    settings = _settings(args)
    if settings.backup_key_file is None:
        print("error: BACKUP_KEY_FILE is required", file=sys.stderr)
        return EXIT_INVALID_CONFIG
    source_db = database_url_from_settings(settings).rsplit("/", 1)[-1].split("?")[0]
    report = restore_backup(args.backup, load_key(settings.backup_key_file),
                            admin_url=read_secret(args.admin_url_file).get_secret_value(),
                            target_database=args.target_database, source_database=source_db,
                            archive_root=args.target_data_root / "archives",
                            payload_root=args.target_data_root / "payloads")
    print(json.dumps(report.__dict__, indent=2, default=str))
    return EXIT_OK


def _ops_archive(args: argparse.Namespace) -> int:
    from marketagg.services import engine_for  # noqa: PLC0415
    from marketagg.storage.archive import ArchiveManager, DirectoryOffsite  # noqa: PLC0415

    settings = _settings(args)
    if settings.archive_offsite is None:
        print("error: ARCHIVE_OFFSITE is required to activate archives", file=sys.stderr)
        return EXIT_INVALID_CONFIG
    year, month = (int(p) for p in args.month.split("-"))
    manager = ArchiveManager(engine_for(settings, "marketagg-archive",
                                        statement_timeout_ms=None),
                             settings.archive_root, DirectoryOffsite(settings.archive_offsite))
    generation = manager.archive_and_activate(args.dataset, settings.provider.value,
                                              date(year, month, 1))
    print(json.dumps({"generation": generation.generation, "rows": generation.row_count,
                      "uri": generation.uri, "state": generation.state}, indent=2))
    return EXIT_OK


def _datasets_run(args: argparse.Namespace) -> int:
    from marketagg.services import api_services_for  # noqa: PLC0415

    services = api_services_for(_settings(args))
    if services.datasets is None:
        print("error: API_EXPORT_TOKEN_FILE (download signing) is required", file=sys.stderr)
        return EXIT_INVALID_CONFIG
    print(json.dumps({"recovered": services.datasets.recover(),
                      "ran": [str(j) for j in services.datasets.run_pending()]}, indent=2))
    return EXIT_OK


def _release_check(args: argparse.Namespace) -> int:
    from marketagg.release import evaluate_release  # noqa: PLC0415

    result = evaluate_release(args.report)
    print(json.dumps(result, indent=2, default=str))
    return EXIT_OK if result["release_allowed"] else EXIT_FAILED


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args, rest = parser.parse_known_args(argv)
    if args.command == "probe":
        return _probe(args, rest)
    if rest:
        parser.error(f"unrecognized arguments: {' '.join(rest)}")
    try:
        code: int = args.handler(args)
    except ConfigError as exc:
        for problem in exc.problems:
            print(f"error: {problem}", file=sys.stderr)
        return EXIT_INVALID_CONFIG
    return code


if __name__ == "__main__":
    sys.exit(main())
