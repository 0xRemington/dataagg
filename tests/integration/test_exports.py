from __future__ import annotations

import csv
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest
from sqlalchemy import Engine, text
from testkit import BAR_POLICY, World, aggregation_policy_id, ingest_minutes, m1_key, make_world

from marketagg.analytics.aggregate import derive_pending
from marketagg.calendar import SessionCalendar
from marketagg.collectors.recovery import scan_minute_gaps
from marketagg.domain import Timeframe
from marketagg.providers.fake import BarRevision, FakeScenario
from marketagg.research.datasets import DatasetRequest, DatasetService
from marketagg.research.manifests import safe_cell
from marketagg.research.repository import ResearchRepository

CAL = SessionCalendar()
OPEN = datetime(2025, 3, 7, 14, 30, tzinfo=UTC)
CLOSE = datetime(2025, 3, 7, 21, 0, tzinfo=UTC)


def _setup(engine: Engine, tmp_path: Path, scenario: FakeScenario | None = None,
           page_size: int = 50) -> tuple[World, DatasetService]:
    world = make_world(engine, scenario, start=CLOSE + timedelta(minutes=5))
    ingest_minutes(world, OPEN, CLOSE)
    policy = aggregation_policy_id(world)
    research = ResearchRepository(engine, CAL, {"bars_ingest": BAR_POLICY.id,
                                                "aggregation": policy})
    service = DatasetService(engine, research, tmp_path / "exports", signing_key=b"s" * 32,
                             clock=world.clock, page_size=page_size)
    return world, service


def _request(world: World, decision: datetime, **kw: Any) -> DatasetRequest:
    base: dict[str, Any] = dict(instrument_id=world.instruments["SYNA"], provider_id="fake",
                                timeframe=Timeframe.M1, start=OPEN, end=CLOSE,
                                decision_at=decision, formats=("csv", "parquet"))
    return DatasetRequest(**(base | kw))


def _run(service: DatasetService, request: DatasetRequest) -> Any:
    return service.run(service.submit(request))


def test_same_pinned_request_reproduces_semantic_hash(engine: Engine, tmp_path: Path) -> None:
    world, service = _setup(engine, tmp_path)
    decision = world.clock.now()
    first = _run(service, _request(world, decision))
    second = _run(service, _request(world, decision))
    assert first.state == second.state == "succeeded"
    assert first.manifest["output_sha256"] == second.manifest["output_sha256"]
    assert first.manifest["row_count"] == 390  # paginated in pages of 50
    files = first.manifest["eligibility_summary"]["files"]
    assert set(files) == {"bars.csv", "options.csv", "bars.parquet", "options.parquet",
                          "lineage.parquet"}
    table = pq.read_table(service.root / str(first.id) / "bars.parquet")
    assert table.num_rows == 390 and str(table.schema.field("open").type) == "decimal128(20, 8)"
    lineage = pq.read_table(service.root / str(first.id) / "lineage.parquet")
    assert lineage.num_rows == 390


def test_later_correction_does_not_change_old_export(engine: Engine, tmp_path: Path) -> None:
    target = datetime(2025, 3, 7, 15, 0, tzinfo=UTC)
    world, service = _setup(engine, tmp_path,
                            FakeScenario(bar_revisions=(BarRevision(target, 2, 1),)))
    decision = world.clock.now()
    before = _run(service, _request(world, decision))
    world.clock.advance(timedelta(hours=1))
    ingest_minutes(world, OPEN, CLOSE)  # correction arrives later
    pinned = _run(service, _request(world, decision))
    current = _run(service, _request(world, world.clock.now()))
    assert pinned.manifest["output_sha256"] == before.manifest["output_sha256"]
    assert current.manifest["output_sha256"] != before.manifest["output_sha256"]


def test_missing_data_and_invalidation_lineage_are_summarised(engine: Engine,
                                                              tmp_path: Path) -> None:
    gap = datetime(2025, 3, 7, 15, 7, tzinfo=UTC)
    world, service = _setup(engine, tmp_path, FakeScenario(omitted_minutes=frozenset({gap})))
    derive_pending(engine, world.repo, CAL, aggregation_policy_id=service.research.policy_ids[
        "aggregation"], now=world.clock.now(),
        instruments={v: k for k, v in world.instruments.items()})
    chain = world.provider.fetch_chain(world.chain_request())
    staged = world.repo.stage_chain(chain, instrument_id=world.instruments["SYNA"], scope={})
    world.repo.publish(staged.batch_id)
    world.repo.publish(world.repo.invalidate_snapshot(staged.written_ids[0],
                                                      chain.fetch_completed_at, reason="t"))
    with engine.connect() as conn:
        contract = conn.execute(text("SELECT contract_id FROM option_snapshots WHERE id=:i"),
                                {"i": staged.written_ids[0]}).scalar_one()
    scan_minute_gaps(engine, CAL, key=m1_key(world), start=OPEN, now=world.clock.now(),
                     grace=timedelta(minutes=3), horizon=timedelta(days=7), horizon_source="t")
    world.clock.advance(timedelta(minutes=1))
    # The chain was polled at ~21:05 (after the close), so widen the window past it.
    view = _run(service, _request(world, world.clock.now(), timeframe=Timeframe.M5,
                                  end=CLOSE + timedelta(minutes=30), contract_ids=(contract,)))
    summary = view.manifest["eligibility_summary"]
    assert summary["bars_excluded_incomplete"] == 1  # the bucket missing a minute
    assert summary["options_invalidated"] == 1 and summary["options"] == 1
    assert summary["known_gaps"] == [{"kind": "missing_minutes", "status": "open", "count": 1}]
    assert summary["options_unknown_quote_time"] == 1


def test_failed_and_cancelled_jobs_publish_nothing(engine: Engine, tmp_path: Path,
                                                   monkeypatch: pytest.MonkeyPatch) -> None:
    world, service = _setup(engine, tmp_path)
    job = service.submit(_request(world, world.clock.now()))

    def boom(*args: Any, **kwargs: Any) -> None:
        raise OSError("disk full")

    monkeypatch.setattr("marketagg.research.datasets._write_parquet", boom)
    view = service.run(job)
    assert view.state == "failed" and view.error["message"] == "disk full"
    assert not (service.root / str(job)).exists()
    assert list(service.root.iterdir()) == []  # staging removed
    cancelled = service.submit(_request(world, world.clock.now()))
    assert service.cancel(cancelled)
    assert service.run(cancelled).state == "cancelled"
    assert not (service.root / str(cancelled)).exists()


def test_restart_recovery_requeues_and_cleans_staging(engine: Engine, tmp_path: Path) -> None:
    world, service = _setup(engine, tmp_path)
    job = service.submit(_request(world, world.clock.now()))
    with engine.begin() as conn:
        conn.execute(text("UPDATE dataset_jobs SET state='running' WHERE id=:i"), {"i": job})
    (service.root / f".{job}-abc").mkdir(parents=True)
    assert service.recover() == {"requeued": 1, "staging_removed": 1}
    assert service.run_pending() == [job]
    assert service.view(job).state == "succeeded"


def test_csv_formula_cells_are_neutralised(engine: Engine, tmp_path: Path) -> None:
    assert safe_cell("=HYPERLINK(\"x\")", text_column=True) == "'=HYPERLINK(\"x\")"
    assert safe_cell("@SUM(A1)", text_column=True).startswith("'")
    assert safe_cell("-0.05", text_column=False) == "-0.05"  # numbers untouched
    world, service = _setup(engine, tmp_path)
    view = _run(service, _request(world, world.clock.now()))
    with (service.root / str(view.id) / "bars.csv").open() as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["interval_start"].endswith("Z") and "." in rows[0]["open"]


def test_signed_downloads_expire_and_reject_tampering(engine: Engine, tmp_path: Path) -> None:
    world, service = _setup(engine, tmp_path)
    view = _run(service, _request(world, world.clock.now()))
    expires = world.clock.now() + timedelta(minutes=5)
    sig = service.sign(view.id, "bars.csv", expires)
    assert service.resolve_download(view.id, "bars.csv", int(expires.timestamp()), sig).is_file()
    with pytest.raises(PermissionError):
        service.resolve_download(view.id, "options.csv", int(expires.timestamp()), sig)
    world.clock.advance(timedelta(minutes=6))
    with pytest.raises(PermissionError, match="expired"):
        service.resolve_download(view.id, "bars.csv", int(expires.timestamp()), sig)
