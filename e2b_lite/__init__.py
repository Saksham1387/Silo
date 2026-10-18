"""e2b_lite: a tiny client SDK. Thin wrapper over the control plane + envd.

    from e2b_lite import Sandbox

    with Sandbox.create(timeout=120) as sbx:
        r = sbx.commands.run("echo hello")
        print(r.stdout)
        sbx.files.write("hi.txt", "hello")
        print(sbx.files.read("hi.txt"))
"""
import json
import os
import posixpath
import shlex
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional, Union

import httpx

DEFAULT_API_URL = "http://127.0.0.1:8000"


class SandboxError(Exception):
    pass


class CommandExitError(SandboxError):
    def __init__(self, result: "CommandResult"):
        super().__init__(f"command exited with code {result.exit_code}: {result.stderr.strip()[:500]}")
        self.result = result


@dataclass
class CommandResult:
    stdout: str
    stderr: str
    exit_code: int
    error: Optional[str] = None


@dataclass
class FileInfo:
    name: str
    path: str
    type: str
    size: int
    modified: float


def _raise_for_status(r: httpx.Response) -> None:
    if r.is_success:
        return
    try:
        detail = r.json().get("detail", r.text)
    except Exception:
        detail = r.text
    raise SandboxError(f"HTTP {r.status_code}: {detail}")


def _truncate(text: str, limit: int) -> str:
    if limit <= 0 or len(text) <= limit:
        return text
    return text[:limit] + f"\n...[truncated {len(text) - limit} chars]"


class Commands:
    def __init__(self, sandbox: "Sandbox"):
        self._sbx = sandbox

    def run(
        self,
        cmd: str,
        *,
        cwd: Optional[str] = None,
        envs: Optional[dict] = None,
        stdin: Optional[str] = None,
        timeout: Optional[float] = 60,
        on_stdout: Optional[Callable[[str], None]] = None,
        on_stderr: Optional[Callable[[str], None]] = None,
        check: bool = True,
    ) -> CommandResult:
        """Run a shell command. Output is streamed to the callbacks as it arrives.

        Raises CommandExitError on non-zero exit unless check=False.
        """
        body = {"cmd": cmd, "cwd": cwd, "env": envs or {}, "stdin": stdin, "timeout": timeout}
        out, err = [], []
        exit_code, error = None, None
        with self._sbx._http.stream(
            "POST",
            self._sbx._envd("exec"),
            json=body,
            headers=self._sbx._auth,
            timeout=httpx.Timeout(30.0, read=None),  # server enforces `timeout`
        ) as r:
            if not r.is_success:
                r.read()
                _raise_for_status(r)
            for line in r.iter_lines():
                if not line:
                    continue
                ev = json.loads(line)
                if ev["type"] == "stdout":
                    out.append(ev["data"])
                    if on_stdout:
                        on_stdout(ev["data"])
                elif ev["type"] == "stderr":
                    err.append(ev["data"])
                    if on_stderr:
                        on_stderr(ev["data"])
                elif ev["type"] == "exit":
                    exit_code, error = ev["code"], ev.get("error")
        if exit_code is None:
            raise SandboxError("connection closed before the command finished (sandbox killed?)")
        result = CommandResult("".join(out), "".join(err), exit_code, error)
        if check and exit_code != 0:
            raise CommandExitError(result)
        return result


class Files:
    def __init__(self, sandbox: "Sandbox"):
        self._sbx = sandbox

    def write(self, path: str, data: Union[str, bytes]) -> None:
        if isinstance(data, str):
            data = data.encode()
        r = self._sbx._http.put(
            self._sbx._envd("files"), params={"path": path}, content=data, headers=self._sbx._auth
        )
        _raise_for_status(r)

    def read_bytes(self, path: str) -> bytes:
        r = self._sbx._http.get(self._sbx._envd("files"), params={"path": path}, headers=self._sbx._auth)
        _raise_for_status(r)
        return r.content

    def read(self, path: str) -> str:
        return self.read_bytes(path).decode()

    def list(self, path: str = ".") -> list[FileInfo]:
        r = self._sbx._http.get(self._sbx._envd("ls"), params={"path": path}, headers=self._sbx._auth)
        _raise_for_status(r)
        return [FileInfo(**e) for e in r.json()]

    def exists(self, path: str) -> bool:
        """True if the path exists (file, directory, or symlink)."""
        r = self._sbx.commands.run(f"test -e {shlex.quote(path)}", check=False)
        return r.exit_code == 0

    def stat(self, path: str) -> FileInfo:
        """Metadata for a single path (name, path, type, size, modified)."""
        target = path.rstrip("/") or "/"
        directory = posixpath.dirname(target) or "."
        name = posixpath.basename(target)
        for entry in self.list(directory):
            if entry.name == name:
                return entry
        raise SandboxError(f"no such file or directory: {path}")

    def mkdir(self, path: str, *, parents: bool = True) -> None:
        """Create a directory. parents=True creates intermediate dirs and is idempotent."""
        flag = "-p " if parents else ""
        r = self._sbx.commands.run(f"mkdir {flag}-- {shlex.quote(path)}", check=False)
        if r.exit_code != 0:
            raise SandboxError(f"failed to create directory {path}: {r.stderr.strip() or 'unknown error'}")

    def remove(self, path: str, *, recursive: bool = False) -> None:
        """Delete a file, or a directory when recursive=True. Missing paths are ignored."""
        flag = "-rf" if recursive else "-f"
        r = self._sbx.commands.run(f"rm {flag} -- {shlex.quote(path)}", check=False)
        if r.exit_code != 0:
            raise SandboxError(f"failed to remove {path}: {r.stderr.strip() or 'unknown error'}")

    def move(self, src: str, dst: str) -> None:
        """Rename or move a file or directory."""
        r = self._sbx.commands.run(f"mv -- {shlex.quote(src)} {shlex.quote(dst)}", check=False)
        if r.exit_code != 0:
            raise SandboxError(f"failed to move {src} -> {dst}: {r.stderr.strip() or 'unknown error'}")

    def copy(self, src: str, dst: str) -> None:
        """Copy a file or directory tree to a new path."""
        r = self._sbx.commands.run(f"cp -r -- {shlex.quote(src)} {shlex.quote(dst)}", check=False)
        if r.exit_code != 0:
            raise SandboxError(f"failed to copy {src} -> {dst}: {r.stderr.strip() or 'unknown error'}")

    def append(self, path: str, data: Union[str, bytes]) -> None:
        """Append to a file, creating it if needed."""
        if isinstance(data, str):
            data = data.encode()
        existing = self.read_bytes(path) if self.exists(path) else b""
        self.write(path, existing + data)

    def upload(self, local_path: Union[str, Path], remote_path: Optional[str] = None) -> str:
        """Upload a local file into the sandbox. Defaults to the local filename. Returns the remote path."""
        local = Path(local_path)
        remote = remote_path or local.name
        self.write(remote, local.read_bytes())
        return remote

    def download(self, remote_path: str, local_path: Optional[Union[str, Path]] = None) -> Path:
        """Download a file from the sandbox to the local machine. Returns the local path."""
        local = Path(local_path) if local_path else Path(posixpath.basename(remote_path))
        local.parent.mkdir(parents=True, exist_ok=True)
        local.write_bytes(self.read_bytes(remote_path))
        return local


class Sandbox:
    def __init__(self, id: str, token: str, api_url: str, api_key: str, expires_at: float):
        self.id = id
        self.expires_at = expires_at
        self._token = token
        self._api_url = api_url.rstrip("/")
        self._api_key = api_key
        self._http = httpx.Client(timeout=30.0)
        self.commands = Commands(self)
        self.files = Files(self)

    # --- construction
    @classmethod
    def create(
        cls,
        *,
        timeout: int = 300,
        cpus: float = 1.0,
        memory_mb: int = 512,
        metadata: Optional[dict] = None,
        api_url: Optional[str] = None,
        api_key: Optional[str] = None,
    ) -> "Sandbox":
        api_url = api_url or os.environ.get("E2B_LITE_API_URL", DEFAULT_API_URL)
        api_key = api_key or os.environ.get("E2B_LITE_API_KEY", "dev-key")
        r = httpx.post(
            f"{api_url.rstrip('/')}/sandboxes",
            json={"timeout": timeout, "cpus": cpus, "memory_mb": memory_mb, "metadata": metadata or {}},
            headers={"X-API-KEY": api_key},
            timeout=60.0,  # container start + health wait
        )
        _raise_for_status(r)
        d = r.json()
        return cls(d["id"], d["token"], api_url, api_key, d["expires_at"])

    @classmethod
    def list(cls, *, api_url: Optional[str] = None, api_key: Optional[str] = None) -> list[dict]:
        api_url = api_url or os.environ.get("E2B_LITE_API_URL", DEFAULT_API_URL)
        api_key = api_key or os.environ.get("E2B_LITE_API_KEY", "dev-key")
        r = httpx.get(f"{api_url.rstrip('/')}/sandboxes", headers={"X-API-KEY": api_key})
        _raise_for_status(r)
        return r.json()

    # --- commands
    def run(
        self,
        cmd: str,
        *,
        cwd: Optional[str] = None,
        envs: Optional[dict] = None,
        stdin: Optional[str] = None,
        timeout: Optional[float] = 60,
        max_output_chars: int = 20000,
    ) -> dict:
        """Agent-friendly command runner. Never raises on a non-zero exit.

        Returns a JSON-serializable dict: {ok, exit_code, stdout, stderr, error}.
        Long output is truncated so it stays usable in an agent's context window.
        """
        result = self.commands.run(cmd, cwd=cwd, envs=envs, stdin=stdin, timeout=timeout, check=False)
        return {
            "ok": result.exit_code == 0,
            "exit_code": result.exit_code,
            "stdout": _truncate(result.stdout, max_output_chars),
            "stderr": _truncate(result.stderr, max_output_chars),
            "error": result.error,
        }

    # --- lifecycle
    def set_timeout(self, seconds: int) -> None:
        r = self._http.post(
            f"{self._api_url}/sandboxes/{self.id}/timeout",
            json={"timeout": seconds},
            headers={"X-API-KEY": self._api_key},
        )
        _raise_for_status(r)
        self.expires_at = r.json()["expires_at"]

    def kill(self) -> None:
        r = self._http.delete(f"{self._api_url}/sandboxes/{self.id}", headers={"X-API-KEY": self._api_key})
        if r.status_code != 404:  # already gone (expired) is fine
            _raise_for_status(r)
        self._http.close()

    def __enter__(self) -> "Sandbox":
        return self

    def __exit__(self, *exc) -> None:
        self.kill()

    # --- internals
    @property
    def _auth(self) -> dict:
        return {"X-Access-Token": self._token}

    def _envd(self, path: str) -> str:
        return f"{self._api_url}/sandboxes/{self.id}/envd/{path}"
