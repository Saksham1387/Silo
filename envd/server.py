"""envd: the daemon that runs INSIDE every sandbox.

The control plane never runs user code itself. It only starts the sandbox;
everything else (run a command, read/write a file) is a call to this server.

Endpoints
  GET  /health                 liveness probe (no auth)
  POST /exec                   run a shell command, stream NDJSON events
  PUT  /files?path=...         write raw request body to a file
  GET  /files?path=...         download a file
  GET  /ls?path=...            list a directory
"""
import asyncio
import codecs
import hmac
import json
import os
import signal
from pathlib import Path
from typing import Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

TOKEN = os.environ.get("ENVD_TOKEN", "")
WORKDIR = Path(os.environ.get("WORKDIR", str(Path.home()))).resolve()

app = FastAPI(title="envd")


def auth(x_access_token: str = Header(default="")) -> None:
    if TOKEN and not hmac.compare_digest(x_access_token, TOKEN):
        raise HTTPException(status_code=401, detail="invalid access token")


def resolve(path: str) -> Path:
    p = Path(path).expanduser()
    return p if p.is_absolute() else WORKDIR / p


@app.get("/health")
def health():
    return {"ok": True}


class ExecRequest(BaseModel):
    cmd: str
    cwd: Optional[str] = None
    env: dict[str, str] = {}
    stdin: Optional[str] = None
    timeout: Optional[float] = 60  # seconds; None = no limit


def _line(obj: dict) -> bytes:
    return (json.dumps(obj) + "\n").encode()


@app.post("/exec", dependencies=[Depends(auth)])
async def exec_command(req: ExecRequest):
    cwd = resolve(req.cwd) if req.cwd else WORKDIR
    if not cwd.is_dir():
        raise HTTPException(status_code=400, detail=f"cwd not found: {cwd}")

    async def events():
        proc = await asyncio.create_subprocess_shell(
            req.cmd,
            cwd=str(cwd),
            env={**os.environ, **req.env},
            stdin=asyncio.subprocess.PIPE if req.stdin is not None else asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,  # own process group so we can kill children too
        )
        queue: asyncio.Queue = asyncio.Queue()

        async def pump(stream, kind):
            dec = codecs.getincrementaldecoder("utf-8")(errors="replace")
            while chunk := await stream.read(4096):
                text = dec.decode(chunk)
                if text:
                    await queue.put({"type": kind, "data": text})
            await queue.put(None)  # this stream is finished

        pumps = [
            asyncio.create_task(pump(proc.stdout, "stdout")),
            asyncio.create_task(pump(proc.stderr, "stderr")),
        ]
        try:
            yield _line({"type": "start", "pid": proc.pid})
            if req.stdin is not None:
                proc.stdin.write(req.stdin.encode())
                await proc.stdin.drain()
                proc.stdin.close()

            loop = asyncio.get_running_loop()
            deadline = loop.time() + req.timeout if req.timeout else None
            finished = 0
            timed_out = False
            while finished < 2:
                wait = None if deadline is None else max(0.0, deadline - loop.time())
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=wait)
                except asyncio.TimeoutError:
                    timed_out = True
                    break
                if item is None:
                    finished += 1
                else:
                    yield _line(item)

            if timed_out:
                _kill_group(proc.pid)
                await proc.wait()
                yield _line({"type": "exit", "code": -1, "error": f"timeout after {req.timeout}s"})
            else:
                code = await proc.wait()
                yield _line({"type": "exit", "code": code})
        finally:
            # runs on normal end AND when the client disconnects mid-stream
            if proc.returncode is None:
                _kill_group(proc.pid)
            for t in pumps:
                t.cancel()

    return StreamingResponse(events(), media_type="application/x-ndjson")


def _kill_group(pid: int) -> None:
    try:
        os.killpg(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


@app.put("/files", dependencies=[Depends(auth)])
async def write_file(path: str, request: Request):
    p = resolve(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "wb") as f:
        async for chunk in request.stream():
            f.write(chunk)
    return {"path": str(p), "size": p.stat().st_size}


@app.get("/files", dependencies=[Depends(auth)])
def read_file(path: str):
    p = resolve(path)
    if not p.is_file():
        raise HTTPException(status_code=404, detail=f"not a file: {p}")
    return FileResponse(p)


@app.get("/ls", dependencies=[Depends(auth)])
def list_dir(path: str = "."):
    p = resolve(path)
    if not p.is_dir():
        raise HTTPException(status_code=404, detail=f"not a directory: {p}")
    entries = []
    for child in sorted(p.iterdir()):
        st = child.lstat()
        entries.append(
            {
                "name": child.name,
                "path": str(child),
                "type": "dir" if child.is_dir() else "file",
                "size": st.st_size,
                "modified": st.st_mtime,
            }
        )
    return entries
