"""Pipeline executor. Runs a validated Plan by wiring subprocess.PIPE
between aa-* invocations. The shell is never involved -- each stage gets
its argv list passed directly to subprocess.

stderr from each stage streams to the parent stderr in real time so the
user sees progress. stdout is consumed by the next stage (or printed by
the last stage).
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import threading

from .plan import Plan
from .safety import ValidationResult


def _stream_stderr(proc: subprocess.Popen, label: str) -> threading.Thread:
    """Pump proc.stderr to sys.stderr in a background thread, prefixed by tool name."""
    def pump():
        if proc.stderr is None:
            return
        for raw in iter(proc.stderr.readline, b""):
            try:
                line = raw.decode("utf-8", errors="replace")
            except Exception:
                continue
            sys.stderr.write(f"[{label}] {line}")
            sys.stderr.flush()
    t = threading.Thread(target=pump, daemon=True)
    t.start()
    return t


def execute(plan: Plan, validation: ValidationResult) -> int:
    """Run a validated pipeline. Returns the exit code of the LAST stage.

    Refuses to run unless validation.ok is True. This is the second line of
    defense -- the UI also checks, but executor refuses too in case anyone
    calls execute() programmatically.
    """
    if not validation.ok:
        sys.stderr.write("aa-help: refusing to execute -- plan is not valid:\n")
        for e in validation.errors:
            sys.stderr.write(f"  - {e}\n")
        return 2

    if plan.kind != "pipeline" or not plan.stages:
        sys.stderr.write("aa-help: nothing to execute (no pipeline in plan).\n")
        return 2

    # Verify each tool is on PATH before launching anything. This catches the
    # "you haven't pip-installed yet" case with a clear message.
    missing = [s.tool for s in plan.stages if shutil.which(s.tool) is None]
    if missing:
        sys.stderr.write(
            f"aa-help: these tools aren't on PATH: {', '.join(missing)}\n"
            "Did you run `pip install -e .` after adding them?\n"
        )
        return 127

    procs: list[subprocess.Popen] = []
    threads: list[threading.Thread] = []
    prev_stdout = None  # type: ignore[var-annotated]

    try:
        for i, stage in enumerate(plan.stages):
            is_last = i == len(plan.stages) - 1
            argv = [stage.tool] + list(stage.args)
            proc = subprocess.Popen(
                argv,
                stdin=prev_stdout,
                stdout=None if is_last else subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=os.environ.copy(),
                bufsize=1,
            )
            procs.append(proc)
            threads.append(_stream_stderr(proc, stage.tool))

            # Close our handle to the previous proc's stdout so it gets EOF
            # when the previous proc exits (per the Python docs idiom).
            if prev_stdout is not None:
                prev_stdout.close()
            prev_stdout = proc.stdout

        # Wait for everything in order.
        rcs = [p.wait() for p in procs]
        for t in threads:
            t.join(timeout=2.0)

        # Report any non-zero exits in earlier stages (the last one's code is
        # the headline, but earlier failures matter for path-piping suites).
        for i, rc in enumerate(rcs[:-1]):
            if rc != 0:
                sys.stderr.write(
                    f"aa-help: stage {i + 1} ({plan.stages[i].tool}) exited {rc}\n"
                )
        return rcs[-1]

    except KeyboardInterrupt:
        sys.stderr.write("\naa-help: interrupted; terminating pipeline...\n")
        for p in procs:
            try:
                p.terminate()
            except Exception:
                pass
        for p in procs:
            try:
                p.wait(timeout=3.0)
            except Exception:
                p.kill()
        return 130
