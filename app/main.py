from __future__ import annotations

import asyncio
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from .henrik import HenrikClient
from .storage import AppConfig, StatsStore, normalize_endpoint, normalize_group_code
from .tracker import SpectraWatchManager, StatsTracker

config = AppConfig.load()
store = StatsStore()
henrik = HenrikClient(
    config.henrik_api_base_url,
    config.henrik_api_key,
    config.request_timeout_seconds,
    config.henrik_max_requests_per_minute,
)
tracker = StatsTracker(store, henrik, config)
watches = SpectraWatchManager(tracker, config)

app = FastAPI(title="PCMT Stats", version="0.2.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


class WatchRequest(BaseModel):
    groupCode: str
    spectraEndpoint: str


@app.on_event("startup")
async def on_startup() -> None:
    store.cleanup_expired()
    tracker.resume_pending()


@app.get("/")
async def root() -> dict[str, Any]:
    return {
        "service": "PCMT Stats",
        "status": "UP",
        "version": "0.2.0",
        "henrikConfigured": henrik.configured,
        "statsLifetimeHours": 24,
        "henrikMaxRequestsPerMinute": config.henrik_max_requests_per_minute,
        "statsIdentity": "spectraEndpoint+groupCode",
    }


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/status")
async def status() -> dict[str, Any]:
    return {
        "status": "UP",
        "version": "0.2.0",
        "henrikConfigured": henrik.configured,
        "activeSpectraWatches": watches.active_count,
        "statsLifetimeHours": 24,
        "henrikMaxRequestsPerMinute": config.henrik_max_requests_per_minute,
        "statsIdentity": "spectraEndpoint+groupCode",
    }


@app.post("/api/watch")
async def watch(payload: WatchRequest) -> dict[str, Any]:
    try:
        created = watches.ensure_watch(payload.groupCode, payload.spectraEndpoint)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {
        "ok": True,
        "created": created,
        "groupCode": normalize_group_code(payload.groupCode),
        "spectraEndpoint": normalize_endpoint(payload.spectraEndpoint),
    }


def _resolve_lookup_row(group_code: str, spectra_endpoint: str | None) -> tuple[dict[str, Any] | None, bool]:
    """Return (row, ambiguous).

    spectraEndpoint is optional only for backwards compatibility with an older
    frontend. It is safe to omit when exactly one Spectra server currently has
    that group code. If multiple servers share the code, we deliberately refuse
    to guess so one production can never display another production's stats.
    """

    group_code = normalize_group_code(group_code)
    if spectra_endpoint:
        return store.get(group_code, normalize_endpoint(spectra_endpoint)), False

    rows = store.list_for_group(group_code)
    if len(rows) == 1:
        return rows[0], False
    if len(rows) > 1:
        return None, True
    return None, False


@app.get("/api/group/{group_code}")
async def group_status(
    group_code: str,
    spectra_endpoint: str | None = Query(None, alias="spectraEndpoint"),
) -> dict[str, Any]:
    row, ambiguous = _resolve_lookup_row(group_code, spectra_endpoint)
    if ambiguous:
        raise HTTPException(
            status_code=409,
            detail="Group code exists on multiple Spectra servers; spectraEndpoint is required",
        )
    if not row:
        raise HTTPException(status_code=404, detail="Server/group pair has no tracked match")
    # Do not expose player PUUIDs or the cached stats payload through diagnostics.
    return {key: value for key, value in row.items() if key not in {"stats", "playerPuuid"}}


@app.get("/getStats")
async def get_stats(
    code: str = Query(..., min_length=1),
    spectra_endpoint: str | None = Query(None, alias="spectraEndpoint"),
) -> JSONResponse:
    group_code = normalize_group_code(code)
    row, ambiguous = _resolve_lookup_row(group_code, spectra_endpoint)

    if ambiguous:
        # Preserve the legacy frontend's HTTP-200 polling behavior, but never
        # return a random server's result when the group code is ambiguous.
        return JSONResponse(
            {
                "status": 409,
                "error": "spectraEndpoint is required because this group code exists on multiple Spectra servers",
                "data": {"players": []},
            },
            headers={"Cache-Control": "no-store"},
        )

    if row:
        stats = store.get_published_stats(row["groupCode"], row["spectraEndpoint"])
        if stats:
            return JSONResponse(stats, headers={"Cache-Control": "no-store"})

    # Keep the legacy frontend's polling path quiet: it checks only whether
    # response.data.players has entries, so pending/missing data remains HTTP 200.
    body = {
        "status": 202 if row and row.get("state") == "pending" else 404,
        "data": {"players": []},
    }
    return JSONResponse(body, headers={"Cache-Control": "no-store"})


@app.get("/api/internal/debug/tasks")
async def task_debug() -> dict[str, int]:
    # Intentionally contains no tokens or stats payloads; useful during deployment.
    return {
        "activeSpectraWatches": watches.active_count,
        "pendingMatches": len(store.list_pending()),
        "asyncioTasks": len(asyncio.all_tasks()),
    }
