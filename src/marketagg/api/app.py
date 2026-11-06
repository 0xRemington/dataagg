"""Private research HTTP API (T13). Reads through ``ResearchRepository`` only.

- ``/health/live`` is dependency-free; ``/health/ready`` checks DB reachability,
  migration head and required partitions; collection state is reported separately.
- Data routes require a bearer token (read scope); dataset creation requires the
  export scope. Tokens are compared in constant time and never echoed.
- Errors use ``{code, message, details, request_id}``; 401/403 auth, 409
  incompatible basis/source, 422 invalid query, 429 local capacity, 503 dependency.
- When ``decision_at`` is omitted the cutoff is fixed at request time and embedded
  in pagination cursors, so pages stay consistent while ingestion continues.
"""

from __future__ import annotations

import hmac
import threading
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Annotated, Any, Literal
from uuid import UUID

from fastapi import BackgroundTasks, Depends, FastAPI, Query, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse
from sqlalchemy import Engine, text
from sqlalchemy.exc import OperationalError

from marketagg import __version__
from marketagg.analytics.asof import AgeLimits, BarRecord, OptionRecord, availability_label
from marketagg.analytics.quote_metrics import quote_metrics
from marketagg.api import schemas as s
from marketagg.calendar import SessionCalendar
from marketagg.clock import SYSTEM_CLOCK, Clock
from marketagg.config import PoliciesConfig
from marketagg.db.migrate import current_revision, head_revision
from marketagg.db.partitions import missing_partitions
from marketagg.domain import AvailabilityMode, PriceBasis, Timeframe
from marketagg.research.datasets import DatasetRequest, DatasetService
from marketagg.research.repository import IncompatibleQuery, QueryError, ResearchRepository


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str,
                 details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.status, self.code, self.message, self.details = status, code, message, details or {}


@dataclass
class ApiServices:
    engine: Engine
    research: ResearchRepository
    calendar: SessionCalendar
    policies: PoliciesConfig
    read_tokens: tuple[str, ...]
    export_tokens: tuple[str, ...]
    datasets: DatasetService | None = None
    clock: Clock = SYSTEM_CLOCK
    max_concurrent: int = 8
    run_exports_inline: bool = True


def _request_id(request: Request) -> str:
    return str(getattr(request.state, "request_id", "unknown"))


def _check(tokens: tuple[str, ...], presented: str | None) -> bool:
    if not presented or not tokens:
        return False
    return any(hmac.compare_digest(presented.encode(), t.encode()) for t in tokens)


def create_app(services: ApiServices) -> FastAPI:  # noqa: PLR0915 - route registration
    app = FastAPI(title="marketagg research API", version=__version__,
                  description="Private, read-only research API. Observations from delayed "
                              "sources carry unknown quote freshness.")
    capacity = threading.BoundedSemaphore(services.max_concurrent)

    @app.middleware("http")
    async def request_context(request: Request,
                              call_next: Callable[[Request], Awaitable[Response]]) -> Response:
        request.state.request_id = uuid.uuid4().hex
        if request.url.path.startswith("/v1/") and not capacity.acquire(blocking=False):
            return _error(request, 429, "local_capacity", "too many concurrent requests")
        try:
            response = await call_next(request)
        finally:
            if request.url.path.startswith("/v1/"):
                capacity.release()
        response.headers["X-Request-ID"] = request.state.request_id
        return response

    @app.exception_handler(ApiError)
    async def api_error(request: Request, exc: ApiError) -> JSONResponse:
        return _error(request, exc.status, exc.code, exc.message, exc.details)

    @app.exception_handler(RequestValidationError)
    async def validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
        details = {"errors": [{"loc": [str(p) for p in e["loc"]], "msg": e["msg"]}
                              for e in exc.errors()]}
        return _error(request, 422, "invalid_query", "request validation failed", details)

    @app.exception_handler(OperationalError)
    async def db_down(request: Request, exc: OperationalError) -> JSONResponse:
        return _error(request, 503, "dependency_unavailable", "database unavailable")

    def require_read(request: Request) -> None:
        token = _bearer(request)
        if token is None:
            raise ApiError(401, "unauthenticated", "bearer token required")
        if not (_check(services.read_tokens, token) or _check(services.export_tokens, token)):
            raise ApiError(403, "forbidden", "token not accepted")

    def require_export(request: Request) -> None:
        token = _bearer(request)
        if token is None:
            raise ApiError(401, "unauthenticated", "bearer token required")
        if not _check(services.export_tokens, token):
            raise ApiError(403, "forbidden", "export scope required")

    read = [Depends(require_read)]
    export = [Depends(require_export)]

    def cutoff(decision_at: datetime | None) -> datetime:
        now = services.clock.now()
        if decision_at is None:
            return now
        if decision_at.tzinfo is None:
            raise ApiError(422, "invalid_query", "decision_at must include a timezone")
        if decision_at > now:
            raise ApiError(422, "invalid_query", "decision_at is in the future")
        return decision_at

    def guarded(fn: Callable[[], Any]) -> Any:
        try:
            return fn()
        except IncompatibleQuery as exc:
            raise ApiError(409, "incompatible_query", str(exc)) from exc
        except QueryError as exc:
            raise ApiError(422, "invalid_query", str(exc)) from exc

    # -- Health -------------------------------------------------------------------

    @app.get("/health/live", tags=["health"])
    def live() -> dict[str, str]:
        return {"status": "alive"}

    @app.get("/health/ready", tags=["health"])
    def ready(request: Request) -> JSONResponse:
        checks: dict[str, Any] = {}
        try:
            with services.engine.connect() as conn:
                checks["database"] = "ok"
                current = current_revision(conn)
                checks["migration"] = "ok" if current == head_revision() else (
                    f"expected {head_revision()}, found {current}")
                last = conn.execute(text(
                    "SELECT max(known_at) FROM observation_batches WHERE status='published'")
                ).scalar_one()
            now = services.clock.now()
            missing = missing_partitions(services.engine, now, now + timedelta(days=31))
            checks["partitions"] = "ok" if not missing else {"missing": missing}
        except OperationalError:
            checks["database"] = "unavailable"
            last = None
        ok = all(v == "ok" for v in checks.values())
        body = {"status": "ready" if ok else "not_ready", "checks": checks,
                "collection": {"last_published_known_at": last.isoformat() if last else None},
                "request_id": _request_id(request)}
        return JSONResponse(body, status_code=200 if ok else 503)

    # -- Reference ------------------------------------------------------------------

    @app.get("/v1/instruments", response_model=s.InstrumentsResponse, tags=["reference"],
             dependencies=read)
    def instruments(request: Request) -> s.InstrumentsResponse:
        rows = services.research.instruments()
        return s.InstrumentsResponse(request_id=_request_id(request), items=[
            s.InstrumentOut(id=r["id"], symbol=r["symbol"], provider_id=r["provider_id"],
                            asset_class=r["asset_class"], currency=r["currency"],
                            calendar_id=r["calendar_id"], status=r["status"],
                            valid_from=r["valid_from"], valid_to=r["valid_to"],
                            identity_known_at=r["known_at"]) for r in rows])

    @app.get("/v1/contracts", response_model=s.ContractsResponse, tags=["reference"],
             dependencies=read)
    def contracts(
        request: Request, instrument_id: UUID, provider_id: str,
        expiration: date | None = None, option_type: Literal["C", "P"] | None = None,
        strike_min: Decimal | None = None, strike_max: Decimal | None = None,
        asof: datetime | None = None,
    ) -> s.ContractsResponse:
        decision = cutoff(asof)
        infos = services.research.contracts(instrument_id, provider_id=provider_id,
                                            decision_at=decision, expiration=expiration,
                                            option_type=option_type, strike_min=strike_min,
                                            strike_max=strike_max)
        return s.ContractsResponse(
            request_id=_request_id(request), source=s.Source(provider_id=provider_id),
            availability=_availability(AvailabilityMode.OBSERVED, decision),
            coverage={"contracts": len(infos)},
            warnings=[] if all(c.has_terms for c in infos) else [
                "contract terms unknown for some contracts: browse only, not model-eligible"],
            items=[s.ContractOut(id=c.id, provider_symbol=c.provider_symbol,
                                 option_type=c.option_type,  # type: ignore[arg-type]
                                 strike=c.strike, expiration_date=c.expiration_date,
                                 first_known_at=c.first_known_at, terms_known=c.has_terms,
                                 model_eligible=c.has_terms) for c in infos])

    # -- Bars, options, panel ----------------------------------------------------------

    @app.get("/v1/bars", response_model=s.BarsResponse, tags=["data"],
             dependencies=read)
    def bars(
        request: Request, instrument_id: UUID, provider_id: str,
        timeframe: Timeframe, start: datetime, end: datetime,
        decision_at: datetime | None = None,
        mode: AvailabilityMode = AvailabilityMode.OBSERVED,
        price_basis: PriceBasis = PriceBasis.PROVIDER_NATIVE,
        include_partial: bool = False,
        limit: Annotated[int, Query(ge=1, le=10_000)] = 10_000,
        cursor: str | None = None,
    ) -> s.BarsResponse:
        decision = cutoff(decision_at)
        page = guarded(lambda: services.research.bars(
            instrument_id=instrument_id, provider_id=provider_id, timeframe=timeframe,
            start=start, end=end, decision_at=decision, price_basis=price_basis, mode=mode,
            include_partial=include_partial, limit=limit, cursor=cursor))
        key = services.research.bar_key(instrument_id, provider_id, timeframe, price_basis)
        warnings = []
        if provider_id == "binance":
            warnings.append("24/7 market: buckets are UTC-anchored Binance klines")
        elif timeframe is Timeframe.H4:
            warnings.append("4h buckets are session-anchored: 240m then a shorter remainder")
        if any(not b.complete for b in page.items):
            warnings.append("partial (incomplete) bars included for display only")
        return s.BarsResponse(
            request_id=_request_id(request), timeframe=timeframe.value,
            source=s.Source(provider_id=provider_id, policy_id=key.policy_id,
                            price_basis=price_basis.value, session_kind="RTH"),
            availability=_availability(mode, page.decision_at),
            coverage={"returned": len(page.items),
                      "incomplete": sum(1 for b in page.items if not b.complete)},
            warnings=warnings, next_cursor=page.next_cursor,
            items=[_bar_out(b, services.calendar) for b in page.items])

    @app.get("/v1/options/history", response_model=s.OptionHistoryResponse, tags=["data"],
             dependencies=read)
    def option_history(
        request: Request, contract_id: UUID, start: datetime, end: datetime,
        provider_id: str | None = None, decision_at: datetime | None = None,
        limit: Annotated[int, Query(ge=1, le=10_000)] = 10_000, cursor: str | None = None,
    ) -> s.OptionHistoryResponse:
        decision = cutoff(decision_at)
        page = guarded(lambda: services.research.options(
            contract_id=contract_id, start=start, end=end, decision_at=decision,
            provider_id=provider_id, limit=limit, cursor=cursor))
        unknown = sum(1 for o in page.items if o.provider_event_at is None)
        return s.OptionHistoryResponse(
            request_id=_request_id(request), contract_id=contract_id,
            source=s.Source(provider_id=provider_id or "any"),
            availability=_availability(AvailabilityMode.OBSERVED, page.decision_at),
            coverage={"returned": len(page.items), "unknown_quote_time": unknown},
            warnings=["raw periodic observations; no option candles are implied"] + (
                ["quote event time unknown: freshness unknown"] if unknown else []),
            next_cursor=page.next_cursor, items=[_option_out(o) for o in page.items])

    @app.get("/v1/panel", response_model=s.PanelResponse, tags=["data"],
             dependencies=read)
    def panel(
        request: Request, instrument_id: UUID, provider_id: str,
        timeframe: Timeframe, start: datetime, end: datetime,
        contract_id: UUID | None = None, option_provider_id: str | None = None,
        decision_at: datetime | None = None,
        mode: AvailabilityMode = AvailabilityMode.OBSERVED,
        allow_mixed_sources: bool = False,
    ) -> s.PanelResponse:
        decision = cutoff(decision_at)
        limit = services.policies.age_limits[timeframe.value]
        result = guarded(lambda: services.research.panel(
            instrument_id=instrument_id, provider_id=provider_id, timeframe=timeframe,
            start=start, end=end, decision_at=decision, contract_id=contract_id,
            option_provider_id=option_provider_id,
            age_limits=AgeLimits(timedelta(seconds=limit.option_seconds),
                                 timedelta(seconds=limit.spot_seconds)),
            allow_mixed_sources=allow_mixed_sources, mode=mode))
        key = services.research.bar_key(instrument_id, provider_id, timeframe,
                                        PriceBasis.PROVIDER_NATIVE)
        return s.PanelResponse(
            request_id=_request_id(request), timeframe=timeframe.value, contract_id=contract_id,
            sampling="latest option observation known at each bar end, same session, "
                     f"within {limit.option_seconds}s; spot cutoff min(D, option known_at)",
            source=s.Source(provider_id=provider_id, policy_id=key.policy_id,
                            price_basis=PriceBasis.PROVIDER_NATIVE.value, session_kind="RTH"),
            availability=_availability(mode, result.decision_at),
            coverage={"points": len(result.points),
                      "with_option": sum(1 for p in result.points if p.option is not None)},
            warnings=result.warnings,
            items=[s.PanelPointOut(
                bar=_bar_out(p.bar, services.calendar), decision_at=p.decision_at,
                option=_option_out(p.option) if p.option is not None else None,
                spot_close=p.spot.close if p.spot is not None else None,
                spot_interval_end=p.spot.interval_end if p.spot is not None else None,
                option_observation_age_seconds=p.option_observation_age_s,
                quote_age_seconds=p.quote_age_s, spot_age_seconds=p.spot_age_s,
                freshness=p.freshness,
                execution_eligible=p.execution_eligible, exclusions=list(p.exclusions),
            ) for p in result.points])

    @app.get("/v1/surfaces", response_model=s.SurfaceResponse, tags=["data"],
             dependencies=read)
    def surfaces(request: Request, instrument_id: UUID, provider_id: str,
                 target_dte: int | None = None, target_delta: float | None = None,
                 asof: datetime | None = None) -> s.SurfaceResponse:
        decision = cutoff(asof)
        return s.SurfaceResponse(
            request_id=_request_id(request), source=s.Source(provider_id=provider_id),
            availability=_availability(AvailabilityMode.OBSERVED, decision), value=None,
            diagnostics={"failure_code": "unsupported_until_model_inputs_verified",
                         "detail": "constant-maturity and 25-delta surfaces require T20/T21",
                         "target_dte": target_dte, "target_delta": target_delta,
                         "extrapolation": False},
            warnings=["surface metrics are not available in the MVP"])

    @app.get("/v1/quality", response_model=s.QualityResponse, tags=["operations"],
             dependencies=read)
    def quality(request: Request, provider_id: str | None = None) -> s.QualityResponse:
        return s.QualityResponse(request_id=_request_id(request),
                                 report=_jsonable(services.research.quality(
                                     provider_id=provider_id)))

    # -- Datasets ------------------------------------------------------------------------

    @app.post("/v1/datasets", status_code=202, response_model=s.DatasetJobOut,
              tags=["datasets"], dependencies=export)
    def create_dataset(request: Request, body: s.DatasetRequestIn,
                       background: BackgroundTasks) -> s.DatasetJobOut:
        if services.datasets is None:
            raise ApiError(503, "dependency_unavailable", "export service not configured")
        if body.decision_at > services.clock.now():
            raise ApiError(422, "invalid_query", "decision_at is in the future")
        try:
            dataset = DatasetRequest(
                instrument_id=body.instrument_id, provider_id=body.provider_id,
                timeframe=Timeframe(body.timeframe), start=body.start, end=body.end,
                decision_at=body.decision_at, mode=AvailabilityMode(body.mode),
                contract_ids=tuple(body.contract_ids), formats=tuple(body.formats))
        except ValueError as exc:
            raise ApiError(422, "invalid_query", str(exc)) from exc
        if body.end - body.start > timedelta(days=5 * 366):
            raise ApiError(422, "invalid_query", "export window exceeds five years")
        job_id = services.datasets.submit(dataset)
        if services.run_exports_inline:
            background.add_task(services.datasets.run, job_id)
        return s.DatasetJobOut(request_id=_request_id(request), id=job_id, state="queued")

    @app.get("/v1/datasets/{job_id}", response_model=s.DatasetJobOut, tags=["datasets"],
             dependencies=read)
    def get_dataset(request: Request, job_id: UUID) -> s.DatasetJobOut:
        if services.datasets is None:
            raise ApiError(503, "dependency_unavailable", "export service not configured")
        try:
            view = services.datasets.view(job_id)
        except KeyError as exc:
            raise ApiError(404, "not_found", "dataset job not found") from exc
        downloads = {}
        if view.state == "succeeded":
            expires = services.clock.now() + timedelta(hours=1)
            for name in sorted(view.files):
                sig = services.datasets.sign(job_id, name, expires)
                downloads[name] = (f"/v1/datasets/{job_id}/files/{name}"
                                   f"?expires={int(expires.timestamp())}&signature={sig}")
        manifest = None
        if view.manifest is not None:
            manifest = _jsonable({k: view.manifest[k] for k in (
                "id", "created_at", "schema_version", "output_sha256", "row_count",
                "policy_versions", "eligibility_summary", "query_spec")})
        return s.DatasetJobOut(request_id=_request_id(request), id=job_id, state=view.state,
                               manifest=manifest, downloads=downloads,
                               error=view.error)

    @app.get("/v1/datasets/{job_id}/files/{filename}", tags=["datasets"])
    def download(job_id: UUID, filename: str, expires: int, signature: str) -> FileResponse:
        if services.datasets is None:
            raise ApiError(503, "dependency_unavailable", "export service not configured")
        try:
            path = services.datasets.resolve_download(job_id, filename, expires, signature)
        except PermissionError as exc:
            raise ApiError(403, "forbidden", str(exc)) from exc
        except (FileNotFoundError, KeyError) as exc:
            raise ApiError(404, "not_found", "file not found") from exc
        return FileResponse(path, filename=filename)

    return app


def _bearer(request: Request) -> str | None:
    header = request.headers.get("authorization", "")
    scheme, _, token = header.partition(" ")
    return token.strip() if scheme.lower() == "bearer" and token.strip() else None


def _error(request: Request, status: int, code: str, message: str,
           details: dict[str, Any] | None = None) -> JSONResponse:
    body = s.ErrorBody(code=code, message=message, details=details or {},
                       request_id=_request_id(request))
    return JSONResponse(body.model_dump(mode="json"), status_code=status)


def _availability(mode: AvailabilityMode, decision: datetime) -> s.Availability:
    return s.Availability(mode=mode.value, decision_at=decision,
                          label=availability_label(mode))


def _bar_out(b: BarRecord, calendar: SessionCalendar) -> s.BarOut:
    minutes = int((b.interval_end - b.interval_start).total_seconds() // 60)
    label = b.timeframe
    if b.timeframe == "4h" and b.provider_id != "binance":  # binance 4h is plain UTC
        bucket = calendar.bucket_for(b.interval_start, Timeframe.H4)
        label = bucket.label if bucket is not None else "4h session bucket"
    return s.BarOut(interval_start=b.interval_start, interval_end=b.interval_end,
                    session_date=b.session_date, duration_minutes=minutes, label=label,
                    open=b.open, high=b.high, low=b.low, close=b.close, volume=b.volume,
                    complete=b.complete, expected_minutes=b.expected_minutes,
                    observed_minutes=b.observed_minutes,
                    is_short_session_bucket=b.is_short_session_bucket,
                    quality_flags=list(b.quality_flags), known_at=b.known_at)


def _option_out(o: OptionRecord) -> s.OptionOut:
    metrics = quote_metrics(o).values
    return s.OptionOut(
        observed_at=o.observed_at, known_at=o.known_at, fetch_completed_at=o.fetch_completed_at,
        provider_event_at=o.provider_event_at,
        quote_time_basis=str(o.field_semantics.get("quote_time_basis", "unknown")),
        last_trade_at=o.last_trade_at, bid=o.bid, ask=o.ask, midpoint=metrics["midpoint"],
        last=o.last, bid_size=o.bid_size, ask_size=o.ask_size,
        reported_iv=metrics["reported_iv"], reported_iv_origin=metrics["reported_iv_origin"],
        reported_volume=o.reported_volume, reported_open_interest=o.reported_open_interest,
        quality_flags=list(o.quality_flags), record_revision=o.record_revision)


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [_jsonable(v) for v in value]
    if isinstance(value, datetime):
        return value.isoformat().replace("+00:00", "Z")
    if isinstance(value, date | UUID | Decimal):
        return str(value)
    return value
