#!/usr/bin/env python3
"""
aa-test

Offline self-test of the aa-* chain. In a scratch directory it:

  1. writes a small synthetic EK60 .raw (aalibrary.utils.ek60_synth),
  2. runs  aa-nc --sonar_model EK60 | aa-sv | aa-clean | aa-graph  through
     real pipes (no shell), checking each stage's exit status, output file
     and file name (<base>.nc, <base>_<hash8>.nc, <product name>.png),
  3. checks the provenance of all four products with aa-metadata (--json and
     --verify): base name, chain length, hashes, the image's link to the
     product it shows,
  4. runs the chain again and checks every stage reused its output,
  5. checks that a stage fed an empty pipe fails (exit 1) instead of
     printing help.

Prints one PASS/FAIL line per check on stdout; exits 0 when all pass, 1
otherwise. Needs no network, credentials or real data.

    aa-test                 run in a temporary directory, then remove it
    aa-test --keep          keep the temporary directory
    aa-test --dir DIR       run in DIR (created if needed, never removed)
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import sysconfig
import tempfile
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

from aalibrary.console._core import Help, ToolSpec, render, show_help

RAW_NAME = "aatest-D20160703-T060000.raw"   # distinctive: never clobbers real data
N_PINGS = 60
CHAIN_TIMEOUT_S = 900
HASH8 = re.compile(r"^(?P<base>.+)_(?P<hash>[0-9a-f]{8})\.(?P<ext>[A-Za-z0-9]+)$")

SPEC = ToolSpec(name="aa-test", role="utility", engines=())

HELP = Help(
    summary="Offline self-test of the aa-* chain on synthetic EK60 data.",
    does=(
        "Writes a small synthetic EK60 .raw (2 channels, 60 pings, no network "
        "needed) in a scratch directory and runs the core chain through real "
        "pipes, the way you would in a shell. It checks each stage's exit "
        "status, output file and file name, then the provenance of all four "
        "products (aa-metadata --json and --verify), then runs the chain again "
        "and checks that every stage reused its output, and finally that a "
        "stage fed an empty pipe fails with exit 1 instead of printing help."
    ),
    stdin="Nothing.",
    stdout=("One line per check: PASS, FAIL, or SKIP (not run because an earlier "
            "stage failed), with the product name and time; under a failure, the "
            "last lines of that stage's stderr. Then 'aa-test: PASS' or "
            "'aa-test: FAIL'."),
    options=[
        ("--keep", "keep the temporary directory (it is also kept when a check "
                   "fails)"),
        ("--dir DIR", "run in DIR instead (created if needed, never removed); "
                      "products already there are reused"),
    ],
    files=(
        "Writes aatest-D20160703-T060000.raw and its products (.nc, .png) in a "
        "new temporary directory, with the download cache (AA_CACHE_DIR) inside "
        "it. Runs with AA_NAMING, AA_REUSE and AA_GCS_* unset so your settings "
        "cannot change the result. Tests the aa-* commands installed next to "
        "the Python that runs aa-test."
    ),
    pipeline="Not a pipeline stage. Run it on its own; exit status 0 means all passed.",
    examples=[
        "aa-test",
        "aa-test --keep",
        "aa-test --dir ./aa-selftest && aa-metadata ./aa-selftest/*.png",
    ],
    notes=["Takes 20-60 s, most of it the four tools starting up; the second, "
           "reusing run is quick."],
)


# ---------------------------------------------------------------------------
# Help.
# ---------------------------------------------------------------------------

def print_help():
    """Curated help (also used by the docs generator)."""
    sys.stdout.write(render(SPEC, HELP, _build_parser()))


def print_help_full():
    print(__doc__)


def _build_parser():
    p = argparse.ArgumentParser(prog="aa-test", add_help=False)
    p.add_argument("--keep", action="store_true",
                   help="Keep the temporary directory.")
    p.add_argument("--dir", default=None, metavar="DIR",
                   help="Run in DIR (created if needed, never removed).")
    return p


# ---------------------------------------------------------------------------
# Running tools.
# ---------------------------------------------------------------------------

def _exe(tool: str) -> str | None:
    """The installed command, preferring the one beside this interpreter."""
    for scripts in (sysconfig.get_path("scripts"), str(Path(sys.executable).parent)):
        if scripts:
            cand = Path(scripts) / tool
            if cand.is_file() and os.access(cand, os.X_OK):
                return str(cand)
    return shutil.which(tool)


def _env(workdir: Path) -> dict:
    env = dict(os.environ)
    for key in ("AA_NAMING", "AA_REUSE", "AA_GCS_FAKE_ROOT", "AA_GCS_MOUNTS"):
        env.pop(key, None)
    env["AA_CACHE_DIR"] = str(workdir / ".aa-cache")
    env["MPLBACKEND"] = "Agg"
    return env


@dataclass
class StageRun:
    tool: str
    argv: list[str]
    rc: int | None = None
    out: list[str] = field(default_factory=list)
    err: str = ""
    ended: float = 0.0
    seconds: float = 0.0

    @property
    def output(self) -> str | None:
        lines = [x.strip() for x in self.out if x.strip()]
        return lines[-1] if lines else None


def run_chain(argvs: list[list[str]], *, cwd: Path, env: dict,
              timeout: float = CHAIN_TIMEOUT_S) -> list[StageRun]:
    """Run argvs as one pipeline: stdout of each stage -> stdin of the next.

    The bytes pass through this process so that each stage's output line
    can be recorded, but every stage reads a real pipe, as in a | b | c.
    """
    runs = [StageRun(Path(a[0]).name, a) for a in argvs]
    procs: list[subprocess.Popen] = []
    t0 = time.monotonic()
    try:
        for i, argv in enumerate(argvs):
            procs.append(subprocess.Popen(
                argv, cwd=str(cwd), env=env,
                stdin=subprocess.DEVNULL if i == 0 else subprocess.PIPE,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE))
    except OSError as exc:
        for p in procs:
            p.kill()
        runs[len(procs)].rc = 127
        runs[len(procs)].err = f"cannot start {argvs[len(procs)][0]}: {exc}"
        return runs

    def relay(i: int) -> None:
        nxt = procs[i + 1].stdin if i + 1 < len(procs) else None
        for raw in iter(procs[i].stdout.readline, b""):
            runs[i].out.append(raw.decode("utf-8", "replace").rstrip("\n"))
            if nxt is not None:
                try:
                    nxt.write(raw)
                    nxt.flush()
                except OSError:
                    pass
        procs[i].stdout.close()
        if nxt is not None:
            try:
                nxt.close()
            except OSError:
                pass

    def drain(i: int) -> None:
        runs[i].err = procs[i].stderr.read().decode("utf-8", "replace")
        procs[i].stderr.close()

    def wait(i: int) -> None:
        runs[i].rc = procs[i].wait()
        runs[i].ended = time.monotonic() - t0

    threads = []
    for i in range(len(procs)):
        for fn in (relay, drain, wait):
            t = threading.Thread(target=fn, args=(i,), daemon=True)
            t.start()
            threads.append(t)
    deadline = time.monotonic() + timeout
    for t in threads:
        t.join(max(0.0, deadline - time.monotonic()))
    if any(t.is_alive() for t in threads):
        for p in procs:
            if p.poll() is None:
                p.kill()
        for r in runs:
            if r.rc is None:
                r.rc = -9
                r.err += f"\n(aa-test: killed after {timeout:.0f} s)"
    previous = 0.0
    for r in runs:
        r.seconds = max(0.0, r.ended - previous)
        previous = max(previous, r.ended)
    return runs


def run_one(argv: list[str], *, cwd: Path, env: dict, stdin: bytes | None = None,
            timeout: float = 300) -> subprocess.CompletedProcess:
    return subprocess.run(argv, cwd=str(cwd), env=env, input=stdin,
                          capture_output=True, timeout=timeout)


# ---------------------------------------------------------------------------
# Reporting.
# ---------------------------------------------------------------------------

class Report:
    def __init__(self) -> None:
        self.results: list[bool] = []

    def line(self, ok: bool | None, label: str, detail: str, seconds: float | None = None,
             tail: str = "") -> bool:
        status = {True: "PASS", False: "FAIL", None: "SKIP"}[ok]
        took = f"{seconds:6.1f}s" if seconds is not None else ""
        text = f"  {status}  {label:<11s} {detail}"
        if took:
            text = f"{text:<70s} {took}"
        print(text, flush=True)
        if tail and not ok:
            for t in _tail(tail):
                print(f"          | {t}", flush=True)
        self.results.append(bool(ok))
        return bool(ok)

    @property
    def passed(self) -> int:
        return sum(self.results)

    @property
    def ok(self) -> bool:
        return all(self.results) and bool(self.results)


def _tail(text: str, n: int = 8) -> list[str]:
    lines = [ln.rstrip() for ln in text.strip().splitlines() if ln.strip()]
    return lines[-n:]


# ---------------------------------------------------------------------------
# Checks.
# ---------------------------------------------------------------------------

def expected_name_problem(stage: int, name: str, base: str, prev_name: str | None) -> str | None:
    """Why ``name`` breaks the naming rule for chain stage ``stage``, or None."""
    if stage == 0:                                    # aa-nc: EchoData
        want = f"{base}.nc"
        return None if name == want else f"expected {want}"
    if stage in (1, 2):                               # aa-sv, aa-clean: products
        m = HASH8.match(name)
        if not m or m["base"] != base or m["ext"] != "nc":
            return f"expected {base}_<hash8>.nc"
        return None
    if stage == 3:                                    # aa-graph: named after what it shows
        want = f"{Path(prev_name).stem}.png" if prev_name else "<clean product>.png"
        return None if name == want else f"expected {want}"
    return None


def check_chain(report: Report, runs: list[StageRun], base: str, workdir: Path) -> list[Path] | None:
    paths: list[Path] = []
    prev_name = None
    failed_at = None
    for i, r in enumerate(runs):
        if failed_at is not None:
            report.line(None, r.tool, f"not checked (stage {failed_at + 1}, "
                                      f"{runs[failed_at].tool}, failed)")
            continue
        out = r.output
        problem = None
        if r.rc != 0:
            problem = f"exit status {r.rc}"
        elif not out:
            problem = "printed no output path"
        elif len([x for x in r.out if x.strip()]) != 1:
            problem = f"printed {len(r.out)} lines on stdout, expected 1 path"
        elif not Path(out).exists():
            problem = f"printed {out}, which does not exist"
        elif Path(out).resolve().parent != workdir.resolve():
            problem = f"wrote {out}, expected it beside its input in {workdir}"
        else:
            problem = expected_name_problem(i, Path(out).name, base, prev_name)
            if problem:
                problem = f"named {Path(out).name}: {problem}"
        if problem:
            report.line(False, r.tool, problem, r.seconds, tail=r.err)
            failed_at = i
            continue
        report.line(True, r.tool, Path(out).name, r.seconds)
        paths.append(Path(out))
        prev_name = Path(out).name
    return paths if failed_at is None else None


def check_metadata(report: Report, paths: list[Path], base: str, *, cwd: Path,
                   env: dict) -> None:
    exe = _exe("aa-metadata")
    if exe is None:
        report.line(False, "metadata", "aa-metadata is not installed")
        return
    t0 = time.monotonic()
    problems: list[str] = []
    res = run_one([exe, "--json", *map(str, paths)], cwd=cwd, env=env)
    docs = []
    if res.returncode != 0:
        problems.append(f"aa-metadata --json exited {res.returncode}")
    else:
        try:
            docs = [json.loads(ln) for ln in res.stdout.decode().splitlines() if ln.strip()]
        except ValueError as exc:
            problems.append(f"aa-metadata --json printed invalid JSON ({exc})")
    if docs and len(docs) != len(paths):
        problems.append(f"{len(docs)} provenance records for {len(paths)} files")
    elif docs:
        for i, (path, doc) in enumerate(zip(paths, docs)):
            prod = doc.get("product", {})
            steps = doc.get("pipeline", [])
            if doc.get("base") != base:
                problems.append(f"{path.name}: base {doc.get('base')!r}, expected {base!r}")
            if len(steps) != i + 1:
                problems.append(f"{path.name}: {len(steps)} pipeline steps, expected {i + 1}")
            m = HASH8.match(path.name)
            if i in (1, 2) and m and not str(prod.get("recipe", "")).startswith(m["hash"]):
                problems.append(f"{path.name}: name hash {m['hash']} != recipe "
                                f"{str(prod.get('recipe'))[:8]}")
        if len(docs) == 4:
            shown = docs[3].get("product", {}).get("scientific_hash")
            drawn = docs[2].get("product", {}).get("hash")
            if shown != drawn:
                problems.append("the PNG does not record the aa-clean product as what it shows")
    ver = run_one([exe, "--verify", *map(str, paths)], cwd=cwd, env=env)
    verified = ver.stdout.decode().count("hash verified")
    if ver.returncode != 0 or verified != len(paths):
        problems.append(f"aa-metadata --verify exited {ver.returncode}, "
                        f"{verified}/{len(paths)} hashes verified")
    seconds = time.monotonic() - t0
    if problems:
        report.line(False, "metadata", problems[0], seconds,
                    tail="\n".join(problems[1:]) + "\n" + ver.stderr.decode() + res.stderr.decode())
    else:
        report.line(True, "metadata", f"{len(paths)} products: base, chain, hashes agree; "
                                      "--verify ok", seconds)


def check_reuse(report: Report, argvs: list[list[str]], first: list[Path], *, cwd: Path,
                env: dict) -> None:
    runs = run_chain(argvs, cwd=cwd, env=env)
    total = sum(r.seconds for r in runs)
    not_reused = []
    for r, want in zip(runs, first):
        if r.rc != 0:
            report.line(False, "reuse", f"second run: {r.tool} exited {r.rc}", total, tail=r.err)
            return
        if "reusing" not in r.err:
            not_reused.append(r.tool)
        elif not r.output or Path(r.output).resolve() != want.resolve():
            report.line(False, "reuse", f"second run: {r.tool} printed {r.output}, "
                                        f"first run {want}", total)
            return
    if not_reused:
        report.line(False, "reuse", "second run recomputed instead of reusing: "
                                    + ", ".join(not_reused), total,
                    tail="\n".join(r.err for r in runs if r.tool in not_reused))
    else:
        report.line(True, "reuse", f"second run reused all {len(runs)} stages "
                                   "(stderr: reusing)", total)


def check_empty_pipe(report: Report, *, cwd: Path, env: dict) -> None:
    exe = _exe("aa-sv")
    if exe is None:
        report.line(False, "empty pipe", "aa-sv is not installed")
        return
    t0 = time.monotonic()
    res = run_one([exe], cwd=cwd, env=env, stdin=b"")
    out = res.stdout.decode().strip()
    seconds = time.monotonic() - t0
    if res.returncode == 1 and not out:
        report.line(True, "empty pipe", "aa-sv exits 1 on an empty pipe, prints nothing",
                    seconds)
    else:
        report.line(False, "empty pipe", f"aa-sv on an empty pipe: exit {res.returncode}, "
                                         f"{len(out.splitlines())} stdout lines "
                                         "(expected exit 1, no output)", seconds,
                    tail=res.stderr.decode())


# ---------------------------------------------------------------------------
# Main.
# ---------------------------------------------------------------------------

def self_test(workdir: Path) -> Report:
    report = Report()
    env = _env(workdir)
    raw = workdir / RAW_NAME
    base = raw.stem

    # 1. Synthetic data.
    t0 = time.monotonic()
    try:
        from aalibrary.utils.ek60_synth import write_ek60_raw

        write_ek60_raw(raw, n_pings=N_PINGS)
        report.line(True, "synth", f"{raw.name} (EK60, {N_PINGS} pings)",
                    time.monotonic() - t0)
    except Exception as exc:  # noqa: BLE001 - report, don't crash
        report.line(False, "synth", f"cannot write synthetic EK60 data: {exc}",
                    time.monotonic() - t0)
        return report

    # 2. The chain, first run.
    tools = ["aa-nc", "aa-sv", "aa-clean", "aa-graph"]
    exes = {t: _exe(t) for t in tools}
    missing = [t for t, e in exes.items() if e is None]
    if missing:
        report.line(False, "chain", "not installed: " + ", ".join(missing)
                    + " (run aa-refresh, or pip install -e . in a clone)")
        return report
    argvs = [[exes["aa-nc"], str(raw), "--sonar_model", "EK60"],
             [exes["aa-sv"]], [exes["aa-clean"]], [exes["aa-graph"]]]
    paths = check_chain(report, run_chain(argvs, cwd=workdir, env=env), base, workdir)

    # 3. Provenance.
    if paths:
        check_metadata(report, paths, base, cwd=workdir, env=env)
    else:
        report.line(None, "metadata", "not checked (the chain failed)")

    # 4. Reuse.
    if paths:
        check_reuse(report, argvs, paths, cwd=workdir, env=env)
    else:
        report.line(None, "reuse", "not checked (the chain failed)")

    # 5. Empty pipe.
    check_empty_pipe(report, cwd=workdir, env=env)
    return report


def main():
    parser = _build_parser()
    if show_help(SPEC, HELP, parser, full=print_help_full):
        sys.exit(0)
    args = parser.parse_args()

    if args.dir:
        workdir = Path(args.dir).expanduser().resolve()
        workdir.mkdir(parents=True, exist_ok=True)
        temporary = False
    else:
        workdir = Path(tempfile.mkdtemp(prefix="aa-test-")).resolve()
        temporary = True

    print(f"aa-test: offline self-test in {workdir}", flush=True)
    t0 = time.monotonic()
    try:
        report = self_test(workdir)
    except KeyboardInterrupt:
        print("aa-test: interrupted", flush=True)
        sys.exit(130)
    elapsed = time.monotonic() - t0

    verdict = "PASS" if report.ok else "FAIL"
    summary = f"aa-test: {verdict} ({report.passed}/{len(report.results)} checks, {elapsed:.0f} s)"
    if temporary and report.ok and not args.keep:
        shutil.rmtree(workdir, ignore_errors=True)
        summary += "; scratch directory removed (--keep keeps it)"
    else:
        summary += f"; files kept in {workdir}"
    print(summary, flush=True)
    sys.exit(0 if report.ok else 1)


if __name__ == "__main__":
    main()
