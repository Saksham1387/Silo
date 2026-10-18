"""Demo. Start the control plane first:  uvicorn control.main:app --port 8000"""
import sys

from e2b_lite import CommandExitError, Sandbox

with Sandbox.create(timeout=120) as sbx:
    print("sandbox:", sbx.id)

    # 1. run a command, streaming output as it arrives
    sbx.commands.run(
        "for i in 1 2 3; do echo tick $i; sleep 0.3; done",
        on_stdout=lambda s: print("  [stdout]", s, end=""),
    )

    # 2. write a file, then run code that uses it
    sbx.files.write("hello.py", "import sys\nprint('hello from', sys.version.split()[0])\n")
    print(sbx.commands.run("python hello.py").stdout.strip())

    # 3. read it back / list the directory
    print(sbx.files.read("hello.py").splitlines()[0])
    print([f.name for f in sbx.files.list(".")])

    # 4. failures surface as exceptions
    try:
        sbx.commands.run("exit 3")
    except CommandExitError as e:
        print("exit code:", e.result.exit_code)

    # 5. timeouts kill the whole process tree
    r = sbx.commands.run("sleep 30", timeout=1, check=False)
    print("timed out:", r.error)

print("sandbox killed")
