"""Runtimes: the "how do I actually start an isolated sandbox" layer.

The control plane only needs two things from a runtime:
    start(sandbox_id, token, cpus, memory_mb) -> host port where envd listens
    stop(sandbox_id)

DockerRuntime  - real isolation (cgroup limits, dropped caps, non-root user).
LocalRuntime   - DEV/TEST ONLY. Runs envd as a plain subprocess with NO isolation.
                 It exists so the whole stack can be tested without Docker.

Swapping DockerRuntime for a Firecracker runtime later means implementing
these same two methods; nothing else in the system changes.
"""
import os
import shutil
import socket
import subprocess
import sys
import tempfile
from pathlib import Path

ENVD_PORT = 49983
ROOT = Path(__file__).resolve().parent.parent


class DockerRuntime:
    def __init__(self, image: str = "silo-envd"):
        import docker  # imported lazily so LocalRuntime works without it

        self.client = docker.from_env()
        self.image = image
        self.cleanup_orphans()

    def start(self, sandbox_id: str, token: str, cpus: float, memory_mb: int) -> int:
        container = self.client.containers.run(
            self.image,
            detach=True,
            name=f"silo-{sandbox_id}",
            labels={"silo": "1", "silo.id": sandbox_id},
            environment={"ENVD_TOKEN": token},
            # publish envd on a random port, bound to loopback only
            ports={f"{ENVD_PORT}/tcp": ("127.0.0.1", None)},
            mem_limit=f"{memory_mb}m",
            memswap_limit=f"{memory_mb}m",  # no swap
            nano_cpus=int(cpus * 1_000_000_000),
            init=True,  # tiny init as PID 1 so killed/orphaned children get reaped (no zombies)
            pids_limit=256,  # fork-bomb guard
            cap_drop=["ALL"],
            security_opt=["no-new-privileges"],
        )
        container.reload()
        return int(container.attrs["NetworkSettings"]["Ports"][f"{ENVD_PORT}/tcp"][0]["HostPort"])

    def stop(self, sandbox_id: str) -> None:
        import docker

        try:
            self.client.containers.get(f"silo-{sandbox_id}").remove(force=True)
        except docker.errors.NotFound:
            pass

    def cleanup_orphans(self) -> None:
        """State is in memory, so after a control-plane restart old containers are unreachable. Remove them."""
        for c in self.client.containers.list(all=True, filters={"label": "silo=1"}):
            c.remove(force=True)


class LocalRuntime:
    """NO ISOLATION. Dev/test only."""

    def __init__(self):
        self.procs: dict[str, tuple[subprocess.Popen, str]] = {}

    def start(self, sandbox_id: str, token: str, cpus: float, memory_mb: int) -> int:
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]
        home = tempfile.mkdtemp(prefix=f"silo-{sandbox_id}-")
        proc = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "--app-dir", str(ROOT / "envd"), "server:app",
             "--host", "127.0.0.1", "--port", str(port), "--log-level", "warning"],
            env={**os.environ, "ENVD_TOKEN": token, "WORKDIR": home},
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self.procs[sandbox_id] = (proc, home)
        return port

    def stop(self, sandbox_id: str) -> None:
        entry = self.procs.pop(sandbox_id, None)
        if not entry:
            return
        proc, home = entry
        proc.kill()
        proc.wait()
        shutil.rmtree(home, ignore_errors=True)


def make_runtime():
    kind = os.environ.get("SILO_RUNTIME", "docker")
    if kind == "local":
        print("WARNING: using LocalRuntime - sandboxes are NOT isolated (dev only)")
        return LocalRuntime()
    return DockerRuntime(os.environ.get("SILO_IMAGE", "silo-envd"))
