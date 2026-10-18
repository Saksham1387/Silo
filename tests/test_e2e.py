"""End-to-end tests. Uses LocalRuntime (no Docker needed), so it tests the whole
stack: SDK -> control plane -> proxy -> envd. Run:  pytest -q
"""
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest

from e2b_lite import CommandExitError, Sandbox, SandboxError

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="module")
def api():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    url = f"http://127.0.0.1:{port}"
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "control.main:app", "--port", str(port), "--log-level", "warning"],
        cwd=ROOT,
        env={**os.environ, "E2B_LITE_RUNTIME": "local", "E2B_LITE_API_KEY": "test-key"},
    )
    for _ in range(50):
        try:
            httpx.get(url + "/sandboxes", headers={"X-API-KEY": "test-key"})
            break
        except httpx.HTTPError:
            time.sleep(0.2)
    else:
        proc.kill()
        pytest.fail("control plane did not start")
    yield {"api_url": url, "api_key": "test-key"}
    proc.terminate()
    proc.wait(timeout=10)


@pytest.fixture
def sbx(api):
    with Sandbox.create(timeout=60, **api) as s:
        yield s


def test_run_and_stream(sbx):
    chunks = []
    r = sbx.commands.run("echo out; echo err 1>&2", on_stdout=chunks.append)
    assert r.stdout == "out\n" and r.stderr == "err\n" and r.exit_code == 0
    assert chunks == ["out\n"]


def test_nonzero_exit(sbx):
    with pytest.raises(CommandExitError) as e:
        sbx.commands.run("exit 3")
    assert e.value.result.exit_code == 3
    assert sbx.commands.run("exit 3", check=False).exit_code == 3


def test_files_roundtrip(sbx):
    sbx.files.write("a/b/c.txt", "héllo")
    assert sbx.files.read("a/b/c.txt") == "héllo"
    sbx.files.write("bin.dat", bytes(range(256)) * 1000)
    assert sbx.files.read_bytes("bin.dat") == bytes(range(256)) * 1000
    assert "a" in [f.name for f in sbx.files.list(".")]
    with pytest.raises(SandboxError):
        sbx.files.read("missing.txt")


def test_files_visible_to_commands(sbx):
    sbx.files.write("x.sh", "echo from-file")
    assert sbx.commands.run("sh x.sh").stdout.strip() == "from-file"


def test_file_ops(sbx):
    sbx.files.mkdir("d/sub")
    assert not sbx.files.exists("d/sub/nope.txt")
    sbx.files.write("d/sub/a.txt", "one")
    assert sbx.files.exists("d/sub/a.txt")
    info = sbx.files.stat("d/sub/a.txt")
    assert info.name == "a.txt" and info.type == "file" and info.size == 3
    sbx.files.append("d/sub/a.txt", " two")
    assert sbx.files.read("d/sub/a.txt") == "one two"
    sbx.files.copy("d/sub/a.txt", "d/sub/b.txt")
    assert sbx.files.read("d/sub/b.txt") == "one two"
    sbx.files.move("d/sub/b.txt", "d/sub/c.txt")
    assert sbx.files.exists("d/sub/c.txt") and not sbx.files.exists("d/sub/b.txt")
    sbx.files.remove("d/sub/c.txt")
    assert not sbx.files.exists("d/sub/c.txt")
    sbx.files.remove("d", recursive=True)
    assert not sbx.files.exists("d")
    with pytest.raises(SandboxError):
        sbx.files.stat("d/sub/a.txt")


def test_upload_download(sbx, tmp_path):
    src = tmp_path / "local.txt"
    src.write_text("uploaded!")
    remote = sbx.files.upload(src, "pkg/remote.txt")
    assert remote == "pkg/remote.txt"
    assert sbx.files.read("pkg/remote.txt") == "uploaded!"
    out = sbx.files.download("pkg/remote.txt", tmp_path / "out.txt")
    assert out.read_text() == "uploaded!"


def test_run_helper(sbx):
    ok = sbx.run("echo hi")
    assert ok["ok"] and ok["exit_code"] == 0 and ok["stdout"] == "hi\n"
    bad = sbx.run("echo oops 1>&2; exit 2")
    assert not bad["ok"] and bad["exit_code"] == 2 and bad["stderr"] == "oops\n"
    long = sbx.run("python -c 'print(\"x\"*100000)'", max_output_chars=100)
    assert "truncated" in long["stdout"]


def test_cwd_env_stdin(sbx):
    sbx.files.write("d/f.txt", "1")
    assert sbx.commands.run("ls", cwd="d").stdout.strip() == "f.txt"
    assert sbx.commands.run("echo $FOO", envs={"FOO": "bar"}).stdout.strip() == "bar"
    assert sbx.commands.run("cat", stdin="piped").stdout == "piped"


def test_timeout_kills_process_tree(sbx):
    t = time.time()
    r = sbx.commands.run("sleep 30 & sleep 30", timeout=1, check=False)
    assert time.time() - t < 5
    assert "timeout" in r.error
    # No *live* `sleep` may remain. (Killed-but-unreaped zombies, state Z, are dead and ignored.)
    check = (
        r"""for p in /proc/[0-9]*; do [ "$(cat $p/comm 2>/dev/null)" = sleep ] """
        r"""&& ! grep -q '^State:.Z' $p/status 2>/dev/null && echo alive; done; echo done"""
    )
    assert sbx.commands.run(check).stdout.strip() == "done"


def test_streaming_is_incremental(sbx):
    stamps = []
    sbx.commands.run("echo a; sleep 1; echo b", on_stdout=lambda s: stamps.append(time.time()))
    assert len(stamps) == 2 and stamps[1] - stamps[0] > 0.7


def test_auth(api, sbx):
    base = f"{api['api_url']}/sandboxes"
    assert httpx.post(base, json={}, headers={"X-API-KEY": "wrong"}).status_code == 401
    url = f"{base}/{sbx.id}/envd/exec"
    body = {"cmd": "echo hi"}
    assert httpx.post(url, json=body).status_code == 401
    assert httpx.post(url, json=body, headers={"X-Access-Token": "nope"}).status_code == 401


def test_sandboxes_are_separate(api):
    with Sandbox.create(**api) as a, Sandbox.create(**api) as b:
        a.files.write("only-a.txt", "x")
        assert "only-a.txt" not in [f.name for f in b.files.list(".")]
        # a's token must not open b
        r = httpx.post(f"{api['api_url']}/sandboxes/{b.id}/envd/exec", json={"cmd": "id"}, headers=a._auth)
        assert r.status_code == 401


def test_kill_and_list(api):
    s = Sandbox.create(**api)
    assert s.id in [x["id"] for x in Sandbox.list(**api)]
    s.kill()
    assert s.id not in [x["id"] for x in Sandbox.list(**api)]


def test_ttl_reaper(api):
    s = Sandbox.create(timeout=1, **api)
    time.sleep(4.5)
    assert s.id not in [x["id"] for x in Sandbox.list(**api)]
    s2 = Sandbox.create(timeout=1, **api)
    s2.set_timeout(60)
    time.sleep(3)
    assert s2.commands.run("echo alive").stdout.strip() == "alive"
    s2.kill()
