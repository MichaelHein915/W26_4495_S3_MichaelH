"""FastAPI app for the cloud dashboard, backed by ClickHouse.

Usage (Docker CMD):
    uvicorn crypto_analytics.dashboard.api_cloud:create_cloud_app \\
        --factory --host 0.0.0.0 --port 8000 --workers 2

Environment variables (from .env.cloud):
    APP_ENV=cloud
    CLICKHOUSE_HOST=clickhouse   (service name inside docker compose network)
    CLICKHOUSE_PORT=8123
    CLICKHOUSE_DB=crypto
    CLICKHOUSE_USER=crypto_writer
    CLICKHOUSE_PASSWORD=<secret>
    DOMAIN=crypto.yourdomain.com  (used in CORS allow-origins)
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import traceback
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from crypto_analytics.clickhouse_query_service import ClickHouseDashboardQueryService
from crypto_analytics.insights.service import DeterministicInsightsService
from crypto_analytics.insights.chat import DashboardChatService
from crypto_analytics.settings import AppSettings

logger = logging.getLogger(__name__)


class ChatHistoryMessage(BaseModel):
    role: str
    content: str


class DashboardChatRequest(BaseModel):
    message: str
    symbol: str | None = None
    history: list[ChatHistoryMessage] = Field(default_factory=list)


def create_cloud_app(settings: AppSettings | None = None) -> FastAPI:
    settings = settings or AppSettings.from_env()

    service = ClickHouseDashboardQueryService(settings)
    insights_service = DeterministicInsightsService(
        model_backend=settings.insight_model_backend,
        model_name=settings.insight_model_name,
    )
    chat_service = DashboardChatService(
        model_backend=settings.chat_model_backend,
        model_name=settings.chat_model_name,
        model_source=settings.chat_model_source,
        max_tokens=settings.chat_max_tokens,
    )

    # Allow localhost for browser-based development, plus the deployed domain.
    domain = os.getenv("DOMAIN", "").strip()
    allow_origins = [
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ]
    if domain:
        allow_origins += [f"https://{domain}", f"http://{domain}"]

    app = FastAPI(
        title="Enterprise Crypto Analytics — Cloud API",
        version="0.1.0",
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allow_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ── Health ────────────────────────────────────────────────────────────────
    @app.get("/api/health")
    def healthcheck() -> dict:
        try:
            symbols = service.list_symbols()
            return {
                "status": "ok",
                "backend": "clickhouse",
                "clickhouse_host": settings.clickhouse_host,
                "tracked_symbols": len(symbols),
            }
        except Exception as exc:
            return {"status": "error", "detail": str(exc)}

    # ── Symbols ───────────────────────────────────────────────────────────────
    @app.get("/api/symbols")
    def list_symbols() -> dict:
        return {"symbols": service.list_symbols()}

    # ── Dashboard snapshot (REST poll) ────────────────────────────────────────
    @app.get("/api/dashboard")
    def dashboard(
        symbol: str | None = Query(default=None),
        candle_limit: int = Query(default=60, ge=1, le=240),
        alert_limit: int = Query(default=20, ge=1, le=100),
    ) -> dict:
        try:
            snapshot = service.get_dashboard_snapshot(
                symbol=symbol,
                candle_limit=candle_limit,
                alert_limit=alert_limit,
            )
            snapshot["insights"] = insights_service.build_from_snapshot(
                snapshot,
                selected_symbol=snapshot.get("selected_symbol"),
            )
            return snapshot
        except Exception as exc:
            traceback.print_exc()
            logger.exception("Unhandled error in /api/dashboard: %s", exc)
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    # ── Dashboard SSE stream (React live updates) ─────────────────────────────
    @app.get("/api/dashboard/stream")
    async def dashboard_stream(
        symbol: str | None = Query(default=None),
        candle_limit: int = Query(default=60, ge=1, le=240),
        alert_limit: int = Query(default=20, ge=1, le=100),
    ) -> StreamingResponse:
        """Server-Sent Events — push a new snapshot every time ClickHouse has
        fresh data (polls every 2 s) with 30-second heartbeats to keep the
        connection alive through Caddy.
        """
        async def _generator():
            last_snapshot_hash: int = -1
            heartbeat_counter = 0

            while True:
                try:
                    snapshot = service.get_dashboard_snapshot(
                        symbol=symbol,
                        candle_limit=candle_limit,
                        alert_limit=alert_limit,
                    )
                    snapshot["insights"] = insights_service.build_from_snapshot(
                        snapshot,
                        selected_symbol=snapshot.get("selected_symbol"),
                    )
                    # Only push when something actually changed
                    current_hash = hash(snapshot.get("summary", {}).get("last_updated_at"))
                    if current_hash != last_snapshot_hash:
                        last_snapshot_hash = current_hash
                        yield f"data: {json.dumps(snapshot)}\n\n"
                    else:
                        heartbeat_counter += 1
                        if heartbeat_counter % 15 == 0:   # ~30 s at 2-s poll
                            yield ": heartbeat\n\n"
                except Exception as exc:
                    logger.exception("SSE error: %s", exc)
                    yield f"data: {json.dumps({'sse_error': str(exc)})}\n\n"

                await asyncio.sleep(2.0)

        return StreamingResponse(
            _generator(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",   # tell Caddy/Nginx not to buffer SSE
                "Connection": "keep-alive",
            },
        )

    # ── Insights ──────────────────────────────────────────────────────────────
    @app.get("/api/insights")
    def insights(
        symbol: str | None = Query(default=None),
        candle_limit: int = Query(default=60, ge=1, le=240),
        alert_limit: int = Query(default=20, ge=1, le=100),
    ) -> dict:
        snapshot = service.get_dashboard_snapshot(
            symbol=symbol,
            candle_limit=candle_limit,
            alert_limit=alert_limit,
        )
        return insights_service.build_from_snapshot(
            snapshot,
            selected_symbol=snapshot.get("selected_symbol"),
        )

    # ── Chat ──────────────────────────────────────────────────────────────────
    @app.post("/api/chat")
    def chat(request: DashboardChatRequest) -> dict:
        try:
            snapshot = service.get_dashboard_snapshot(
                symbol=request.symbol,
                candle_limit=60,
                alert_limit=20,
            )
            return chat_service.answer_question(
                question=request.message,
                snapshot=snapshot,
                history=[m.model_dump() for m in request.history],
            )
        except Exception as exc:
            logger.exception("Chat error: %s", exc)
            return {
                "type": "chat",
                "backend": settings.chat_model_backend,
                "model_name": settings.chat_model_name,
                "answer": "The dashboard chat encountered an error. Please try again.",
                "refusal": False,
                "citations": [],
                "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "warning": str(exc),
            }

    # ── React SPA (static build) ──────────────────────────────────────────────
    frontend_dist   = Path(__file__).resolve().parents[3] / "apps/dashboard/frontend/dist"
    frontend_assets = frontend_dist / "assets"
    frontend_index  = frontend_dist / "index.html"

    if frontend_assets.exists():
        app.mount("/assets", StaticFiles(directory=frontend_assets), name="assets")

    @app.get("/", include_in_schema=False, response_model=None)
    def root():
        if frontend_index.exists():
            return FileResponse(frontend_index)
        return JSONResponse({"message": "Frontend not built. Run: npm install && npm run build"})

    @app.get("/{full_path:path}", include_in_schema=False, response_model=None)
    def spa_fallback(full_path: str):
        if frontend_index.exists():
            return FileResponse(frontend_index)
        raise HTTPException(status_code=404, detail=f"Not found: {full_path}")

    return app
