"""aa-refresh — clean reinstall of AA-SI development libraries with a pretty UI.

Requires: rich  (pip install rich)
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Callable

from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table
from rich.text import Text


LIBRARIES = [
    {
        "pip_name": "aalibrary",
        "repo_url": "git+https://github.com/nmfs-ost/AA-SI_aalibrary.git@main",
    },
    {
        "pip_name": "AA-SI-KMEANS",
        "repo_url": "git+https://github.com/nmfs-ost/AA-SI_KMeans.git@main",
    },
]

console = Console()


# --------------------------------------------------------------------------- #
# Models
# --------------------------------------------------------------------------- #

@dataclass
class Result:
    pip_name: str
    success: bool
    duration: float
    error_tail: list[str] = field(default_factory=list)


# --------------------------------------------------------------------------- #
# Subprocess streaming
# --------------------------------------------------------------------------- #

def stream_command(
    cmd: list[str],
    on_line: Callable[[str], None],
    tail_size: int = 30,
) -> tuple[int, list[str]]:
    """Run `cmd`, call `on_line` for each line of merged stdout/stderr.

    Returns (return_code, last_n_lines).  The captured tail is what we show
    inside an error panel if the install ends up failing.
    """
    tail: deque[str] = deque(maxlen=tail_size)
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        errors="replace",
    )
    assert proc.stdout is not None
    for raw in proc.stdout:
        line = raw.rstrip()
        if not line:
            continue
        tail.append(line)
        on_line(line)
    return proc.wait(), list(tail)


# --------------------------------------------------------------------------- #
# Refresh logic
# --------------------------------------------------------------------------- #

def refresh_one(
    lib: dict,
    progress: Progress,
    task_id: int,
    status_text: Text,
) -> Result:
    pip_name = lib["pip_name"]
    repo_url = lib["repo_url"]
    started = time.monotonic()

    def update(stage: str, line: str) -> None:
        # Keep the live status to a single line so the layout stays stable.
        trimmed = line if len(line) <= 90 else line[:87] + "..."
        status_text.plain = f"  [{stage}] {trimmed}"
        progress.update(
            task_id,
            description=f"[bold]{pip_name}[/]  [dim]· {stage}[/]",
        )

    # 1) Uninstall — non-zero is fine; the package may not have been installed.
    stage_remove = "removing old version"
    update(stage_remove, "starting...")
    stream_command(
        [sys.executable, "-m", "pip", "uninstall", "-y", pip_name],
        on_line=lambda line: update(stage_remove, line),
    )

    # 2) Reinstall from GitHub — --no-cache-dir + --force-reinstall so that
    #    setuptools re-discovers any new sub-packages.
    stage_install = "pulling latest from GitHub"
    update(stage_install, "starting...")
    rc, tail = stream_command(
        [
            sys.executable, "-m", "pip", "install",
            "--no-cache-dir", "--force-reinstall", repo_url,
        ],
        on_line=lambda line: update(stage_install, line),
    )

    duration = time.monotonic() - started
    if rc != 0:
        return Result(pip_name, success=False, duration=duration, error_tail=tail)
    return Result(pip_name, success=True, duration=duration)


# --------------------------------------------------------------------------- #
# UI rendering
# --------------------------------------------------------------------------- #

def render_header() -> Panel:
    body = Text.assemble(
        "Welcome! This tool keeps your local AA-SI libraries in sync with the "
        "latest code on GitHub. It removes your current copies and reinstalls "
        "them fresh from the ",
        ("main", "italic"),
        " branch — so any new features, fixes, or sub-modules the team has "
        "shipped show up on your machine.\n\n",
        ("Recommended every week or two", "bold"),
        ", and any time a teammate mentions a feature you don't seem to have "
        "yet.\n\n",
        ("interpreter: ", "dim"),
        (sys.executable, "dim"),
    )
    return Panel(
        body,
        border_style="cyan",
        title="[bold cyan]aa-refresh[/]",
        title_align="left",
        padding=(1, 2),
    )


def render_farewell() -> Panel:
    body = Text.assemble(
        "The latest versions of your AA-SI libraries are now installed. "
        "If you ever hit a missing function, a strange import error, or a "
        "sub-module that won't load, running ",
        ("aa-refresh", "bold"),
        " is usually the first thing to try.\n\n",
        ("Tip: ", "bold green"),
        ("come back and run this every week or two to stay current.", "dim"),
    )
    return Panel(
        body,
        border_style="green",
        title="[bold green]✔ You're all set[/]",
        title_align="left",
        padding=(1, 2),
    )


def render_summary(results: list[Result]) -> Table:
    table = Table(title="Refresh Summary", title_style="bold", show_lines=False)
    table.add_column("Library", style="bold")
    table.add_column("Status")
    table.add_column("Time", justify="right", style="dim")
    for r in results:
        status = (
            Text("✔ ok", style="green")
            if r.success
            else Text("✘ failed", style="red")
        )
        table.add_row(r.pip_name, status, f"{r.duration:.1f}s")
    return table


def render_error_panels(failed: list[Result]) -> list[Panel]:
    panels = []
    for r in failed:
        body = "\n".join(r.error_tail) or "(no output captured)"
        panels.append(
            Panel(
                body,
                title=f"[red]Last output from {r.pip_name}[/]",
                border_style="red",
            )
        )
    return panels


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #

def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="aa-refresh",
        description=(
            "Keep your AA-SI development libraries in sync with the latest code "
            "on GitHub. aa-refresh removes your current copies of aalibrary and "
            "AA-SI-KMEANS and reinstalls them fresh from main, so any new "
            "features, fixes, or sub-modules show up on your machine. "
            "Recommended every week or two."
        ),
        epilog="Run this from inside your active virtual environment.",
    )
    parser.add_argument(
        "--only",
        metavar="PIP_NAME",
        help="Refresh just one library instead of all of them. "
             "Example: --only aalibrary",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)

    targets = LIBRARIES
    if args.only:
        targets = [lib for lib in LIBRARIES if lib["pip_name"] == args.only]
        if not targets:
            known = ", ".join(lib["pip_name"] for lib in LIBRARIES)
            console.print(
                f"[red]ERROR:[/] unknown library [bold]{args.only}[/]. "
                f"Known: {known}"
            )
            return 1

    console.print(render_header())

    progress = Progress(
        SpinnerColumn(style="cyan", finished_text="[green]✓[/]"),
        TextColumn("{task.description}"),
        BarColumn(bar_width=None, pulse_style="cyan"),
        TimeElapsedColumn(),
        console=console,
        transient=False,
    )

    status_text = Text("  waiting...", style="dim italic")
    live_group = Group(progress, status_text)

    results: list[Result] = []
    with Live(live_group, console=console, refresh_per_second=12):
        for lib in targets:
            task_id = progress.add_task(
                f"[bold]{lib['pip_name']}[/]  [dim]· queued[/]",
                total=None,
            )
            result = refresh_one(lib, progress, task_id, status_text)
            results.append(result)
            # Mark task complete so the spinner becomes a check / cross.
            progress.update(
                task_id,
                description=(
                    f"[bold]{lib['pip_name']}[/]  "
                    + (
                        "[green]✔ done[/]"
                        if result.success
                        else "[red]✘ failed[/]"
                    )
                ),
                completed=1,
                total=1,
            )
        status_text.plain = ""  # clear the live status line at the end

    console.print()
    console.print(render_summary(results))

    failed = [r for r in results if not r.success]
    if failed:
        console.print()
        console.print(
            "[yellow]Something didn't go through.[/] Below is the tail end of "
            "pip's output for each failure — that's usually enough for someone "
            "on the team to figure out what happened."
        )
        console.print()
        for panel in render_error_panels(failed):
            console.print(panel)
        return 1

    console.print()
    console.print(render_farewell())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())