import asyncio
import hmac
import json
import threading
import time
from collections import deque
from contextlib import asynccontextmanager, suppress

from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from .config import Settings
from .contracts import Batch, Summary
from .store import CursorExpired, EventConflict, StorageFull, Store


def create_app(settings=None, store=None):
    settings = settings or Settings.from_env()

    @asynccontextmanager
    async def lifespan(app):
        app.state.store = store or Store(settings)
        async def worker():
            while True:
                try:
                    await asyncio.to_thread(app.state.store.maintain)
                    while await asyncio.to_thread(app.state.store.aggregate):
                        await asyncio.sleep(0)
                    app.state.worker_failed = False
                except Exception:
                    # No request bodies, identifiers or detailed errors in service logs.
                    app.state.worker_failed = True
                await asyncio.sleep(settings.aggregate_interval)
        app.state.worker_failed = False
        task = asyncio.create_task(worker()) if settings.mode != "off" else None
        yield
        if task:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        if store is None:
            app.state.store.close()

    app = FastAPI(title="Snow Statistics", version="1", lifespan=lifespan, docs_url=None, redoc_url=None)
    app.add_middleware(CORSMiddleware, allow_origins=list(settings.origins), allow_methods=["GET", "POST"],
                       allow_headers=["Content-Type"], allow_credentials=False)
    admissions, admission_lock = deque(), threading.Lock()

    @app.exception_handler(RequestValidationError)
    async def invalid_request(request, exc):
        return JSONResponse({"detail": "invalid request"}, status_code=422)

    def authorize(value, expected):
        if not expected or not hmac.compare_digest(value or "", "Bearer " + expected):
            raise HTTPException(401, "unauthorized")

    @app.get("/healthz")
    def health():
        return JSONResponse({"status": "degraded" if app.state.worker_failed else "ok", "mode": settings.mode},
                            status_code=503 if app.state.worker_failed else 200)

    @app.post("/analytics/v1/events", status_code=202)
    async def ingest(request: Request, authorization: str | None = Header(default=None)):
        if settings.mode == "off":
            raise HTTPException(503, "collection disabled")
        with admission_lock:
            cutoff = time.monotonic() - 60
            while admissions and admissions[0] < cutoff:
                admissions.popleft()
            if len(admissions) >= 120:
                raise HTTPException(429, "ingest rate limit", headers={"Retry-After": "60"})
            admissions.append(time.monotonic())
        if request.headers.get("content-type", "").split(";")[0] != "application/json":
            raise HTTPException(415, "application/json required")
        body = bytearray()
        try:
            async with asyncio.timeout(5):
                async for chunk in request.stream():
                    body.extend(chunk)
                    if len(body) > settings.max_body_bytes:
                        raise HTTPException(413, "batch too large")
        except TimeoutError:
            raise HTTPException(408, "request body timeout") from None
        try:
            batch = Batch.model_validate_json(body)
        except (ValidationError, ValueError):
            raise HTTPException(422, "invalid event contract") from None
        if any(e.event_type == "request_complete" for e in batch.events):
            authorize(authorization, settings.server_token)
        elif request.headers.get("origin") not in settings.origins:
            # Browser events are untrusted activity estimates. Origin is an abuse
            # reduction measure, not authentication or proof of human activity.
            raise HTTPException(403, "origin not allowed")
        try:
            return await asyncio.to_thread(app.state.store.ingest, batch.events)
        except StorageFull:
            raise HTTPException(503, "storage capacity reached", headers={"Retry-After": "60"}) from None
        except EventConflict:
            raise HTTPException(409, "event ID conflict") from None
        except ValueError:
            raise HTTPException(422, "event outside configured allowlist or time window") from None

    @app.get("/analytics/public/v1/summary.json", response_model=Summary)
    def summary():
        result = app.state.store.summary(archived=settings.mode == "off")
        return JSONResponse(json.loads(result.model_dump_json()), headers={"Cache-Control": "public, max-age=60"})

    @app.get("/analytics/private/v1/events")
    def read(after: int = Query(0, ge=0), limit: int = Query(500, ge=1, le=500), authorization: str | None = Header(default=None)):
        authorize(authorization, settings.reader_token)
        try:
            return JSONResponse(app.state.store.read(after, limit), headers={"Cache-Control": "no-store"})
        except CursorExpired:
            raise HTTPException(410, "cursor expired; reconcile retained archive before resetting") from None

    return app
