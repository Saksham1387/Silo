"""Control plane: create/list/kill sandboxes, proxy traffic to envd, reap expired sandboxes.

Two kinds of auth, mirroring E2B:
  X-API-KEY       -> management calls (create, list, kill, set timeout)
  X-Access-Token  -> per-sandbox token, required on every proxied call to envd

Run:  uvicorn control.main:app --port 8000
"""

import asyncio
import hmac
import os
import secrets
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from starlette.background import BackgroundTask

from .runtime import make_runtime

API_KEY = os.environ.get("SILO_API_KEY", "dev-key")
MAX_SANDBOXES = int(os.environ.get("SILO_MAX_SANDBOXES", "20"))
MAX_TIMEOUT = int(os.environ.get("SILO_MAX_TIMEOUT", "3600"))

runtime = None  # created in lifespan so importing this module has no side effects
http: httpx.AsyncClient = None


@dataclass
class SandboxRecord:
    id: str
    token: str
    port: int
    created_at: float
    expires_at: float
    metadata: dict = field(default_factory=dict)


sandboxes: dict[str, SandboxRecord] = {}


# ---------------------------------------------------------------- lifecycle
async def kill_sandbox(sandbox_id: str) -> bool:
    rec = sandboxes.pop(sandbox_id, None)
    if rec is None:
        return False
    await asyncio.to_thread(runtime.stop, sandbox_id)
    return True


async def reaper():
    """Every couple of seconds, kill sandboxes whose TTL has passed."""
    while True:
        await asyncio.sleep(2)
        now = time.time()
        for sid in [s.id for s in sandboxes.values() if s.expires_at <= now]:
            await kill_sandbox(sid)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global runtime, http
    runtime = await asyncio.to_thread(make_runtime)
    http = httpx.AsyncClient(timeout=httpx.Timeout(30.0, read=None))
    task = asyncio.create_task(reaper())
    yield
    task.cancel()
    for sid in list(sandboxes):
        await kill_sandbox(sid)
    await http.aclose()


app = FastAPI(title="silo control plane", lifespan=lifespan)


def require_api_key(x_api_key: str = Header(default="")) -> None:
    if not hmac.compare_digest(x_api_key, API_KEY):
        raise HTTPException(status_code=401, detail="invalid API key")


def get_live(sandbox_id: str) -> SandboxRecord:
    rec = sandboxes.get(sandbox_id)
    if rec is None or rec.expires_at <= time.time():
        raise HTTPException(status_code=404, detail="sandbox not found or expired")
    return rec


def public(rec: SandboxRecord) -> dict:
    return {
        "id": rec.id,
        "created_at": rec.created_at,
        "expires_at": rec.expires_at,
        "metadata": rec.metadata,
    }


# -------------------------------------------------------------- management API
class CreateRequest(BaseModel):
    timeout: int = Field(300, ge=1, description="seconds until the sandbox is killed")
    cpus: float = Field(1.0, gt=0, le=8)
    memory_mb: int = Field(512, ge=64, le=16384)
    metadata: dict = {}


@app.post("/sandboxes", dependencies=[Depends(require_api_key)])
async def create_sandbox(req: CreateRequest):
    if len(sandboxes) >= MAX_SANDBOXES:
        raise HTTPException(status_code=429, detail="sandbox limit reached")
    timeout = min(req.timeout, MAX_TIMEOUT)

    sandbox_id = "sbx_" + secrets.token_hex(6)
    token = secrets.token_urlsafe(24)
    try:
        port = await asyncio.to_thread(runtime.start, sandbox_id, token, req.cpus, req.memory_mb)
    except Exception as e:
        await asyncio.to_thread(runtime.stop, sandbox_id)
        raise HTTPException(status_code=500, detail=f"failed to start sandbox: {e}")

    # Don't hand the sandbox back until envd answers.
    for _ in range(100):
        try:
            r = await http.get(f"http://127.0.0.1:{port}/health", timeout=1.0)
            if r.status_code == 200:
                break
        except httpx.HTTPError:
            pass
        await asyncio.sleep(0.2)
    else:
        await asyncio.to_thread(runtime.stop, sandbox_id)
        raise HTTPException(status_code=500, detail="envd did not become healthy in time")

    now = time.time()
    rec = SandboxRecord(sandbox_id, token, port, now, now + timeout, req.metadata)
    sandboxes[sandbox_id] = rec
    return {**public(rec), "token": token}


@app.get("/sandboxes", dependencies=[Depends(require_api_key)])
async def list_sandboxes():
    return [public(r) for r in sandboxes.values()]


@app.delete("/sandboxes/{sandbox_id}", status_code=204, dependencies=[Depends(require_api_key)])
async def delete_sandbox(sandbox_id: str):
    if not await kill_sandbox(sandbox_id):
        raise HTTPException(status_code=404, detail="sandbox not found")


class TimeoutRequest(BaseModel):
    timeout: int = Field(..., ge=1)


@app.post("/sandboxes/{sandbox_id}/timeout", dependencies=[Depends(require_api_key)])
async def set_timeout(sandbox_id: str, req: TimeoutRequest):
    rec = get_live(sandbox_id)
    rec.expires_at = time.time() + min(req.timeout, MAX_TIMEOUT)
    return public(rec)


# ------------------------------------------------------------------ data plane
HOP_BY_HOP = {"host", "connection", "keep-alive", "transfer-encoding", "te", "upgrade",
              "proxy-authorization", "proxy-authenticate", "trailers", "content-length"}


@app.api_route("/sandboxes/{sandbox_id}/envd/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def proxy(sandbox_id: str, path: str, request: Request):
    """Authenticated, streaming reverse proxy: client -> control plane -> envd."""
    rec = get_live(sandbox_id)
    if not hmac.compare_digest(request.headers.get("x-access-token", ""), rec.token):
        raise HTTPException(status_code=401, detail="invalid sandbox access token")

    upstream = http.build_request(
        request.method,
        f"http://127.0.0.1:{rec.port}/{path}",
        params=request.query_params,
        headers={k: v for k, v in request.headers.items() if k.lower() not in HOP_BY_HOP},
        content=request.stream(),
    )
    try:
        resp = await http.send(upstream, stream=True)
    except httpx.HTTPError:
        raise HTTPException(status_code=502, detail="sandbox unreachable")

    return StreamingResponse(
        resp.aiter_raw(),
        status_code=resp.status_code,
        headers={k: v for k, v in resp.headers.items() if k.lower() not in HOP_BY_HOP},
        background=BackgroundTask(resp.aclose),
    )
