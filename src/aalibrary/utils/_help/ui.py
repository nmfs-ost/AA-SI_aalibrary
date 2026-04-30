"""Interactive UI for aa-help, dressed up with `rich`.

Design choices:
  - Width is CLAMPED to a sensible band (60-100 columns). On very wide
    terminals, output stays readable instead of spreading edge to edge. On
    very narrow ones, panels collapse padding to stay legible.
  - Emojis are USED SPARINGLY: one in the banner, one on success, one on
    failure, a wave on the execution divider. Stage rows and arg lines stay
    plain text -- emojis on every line crosses into noise.
  - Ctrl-C and Ctrl-D ALWAYS exit cleanly. The REPL catches at every level
    and prints a single goodbye line; no tracebacks.
"""
from __future__ import annotations

import shutil
import sys
from contextlib import contextmanager
from typing import Iterator

from .plan import Plan
from .safety import (
    ValidationResult,
    render_command,
    render_pipeline,
    validate,
)
from .executor import execute


# --- rich console ----------------------------------------------------------

try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.syntax import Syntax
    from rich.markdown import Markdown
    from rich.table import Table
    from rich.text import Text
    from rich.rule import Rule
    from rich.box import ROUNDED, HEAVY
    _HAVE_RICH = True
except ModuleNotFoundError:
    _HAVE_RICH = False


# Width clamp. Below MIN, terminals are too cramped for our panels and we
# tighten everything up. Above MAX, lines get hard to scan; we cap there.
WIDTH_MIN = 60
WIDTH_MAX = 100


def _term_width() -> int:
    cols = shutil.get_terminal_size((80, 24)).columns
    return max(WIDTH_MIN, min(cols, WIDTH_MAX))


def _is_narrow() -> bool:
    cols = shutil.get_terminal_size((80, 24)).columns
    return cols < 70


def _make_console(stderr: bool = False):
    if not _HAVE_RICH:
        return None
    return Console(
        stderr=stderr,
        width=_term_width(),
        # rich already auto-detects no-color terminals, log files, etc.
        # We don't override its TTY detection here.
    )


_console = _make_console(stderr=False)
_err_console = _make_console(stderr=True)


def _is_tty() -> bool:
    return sys.stdin.isatty() and sys.stdout.isatty()


# --- public helpers --------------------------------------------------------

def print_banner(mode: str) -> None:
    """REPL banner shown at startup. One fish anchors the title."""
    if _HAVE_RICH:
        body = Text()
        body.append("🐟 aa-help", style="bold cyan")
        body.append(" — active acoustics assistant\n", style="cyan")
        if not _is_narrow():
            body.append("Ask anything about aalibrary or the aa-* tools.\n",
                        style="dim")
        body.append("\nMode: ", style="dim")
        if mode == "execute":
            body.append("EXECUTE allowed", style="bold green")
        else:
            body.append("dry-run ", style="bold yellow")
            body.append("(--execute to enable)", style="dim")
        body.append("\nExit: ", style="dim")
        body.append("Ctrl-D, Ctrl-C, or /exit", style="dim italic")
        _console.print(Panel(
            body, border_style="cyan", box=ROUNDED,
            padding=(0, 2) if not _is_narrow() else (0, 1),
        ))
    else:
        print("🐟 aa-help — active acoustics assistant")
        print(f"Mode: {'EXECUTE allowed' if mode == 'execute' else 'dry-run'}")
        print("Exit: Ctrl-D, Ctrl-C, or /exit")


def print_goodbye() -> None:
    """Single-line goodbye on clean exit."""
    if _HAVE_RICH:
        _console.print()
        _console.print("[cyan]🌊 fair winds.[/cyan]")
    else:
        print("\nfair winds.")


def print_info(msg: str) -> None:
    if _HAVE_RICH:
        _console.print(f"[dim]{msg}[/dim]")
    else:
        print(msg)


def print_error(msg: str) -> None:
    if _HAVE_RICH:
        _err_console.print(f"[bold red]error:[/bold red] {msg}")
    else:
        print(f"error: {msg}", file=sys.stderr)


@contextmanager
def thinking(label: str = "thinking") -> Iterator[None]:
    """Spinner during Vertex calls. Ctrl-C cleanly aborts the spinner."""
    if _HAVE_RICH and _is_tty():
        try:
            with _console.status(f"[cyan]{label}...[/cyan]", spinner="dots"):
                yield
        except KeyboardInterrupt:
            # Re-raise so the REPL/main loop can decide what to do, but the
            # status context exits cleanly first.
            raise
    else:
        print(f"[{label}]", file=sys.stderr)
        yield


# --- plan rendering --------------------------------------------------------

def _render_pipeline_panel(plan: Plan) -> None:
    pipeline_text = render_pipeline(plan)
    narrow = _is_narrow()

    if plan.summary:
        _console.print()
        _console.print(Markdown(plan.summary))
        _console.print()

    syntax = Syntax(
        pipeline_text, "bash",
        theme="ansi_dark", line_numbers=False, word_wrap=True,
        background_color="default",
    )
    _console.print(Panel(
        syntax,
        title="[bold]proposed pipeline[/bold]",
        title_align="left",
        border_style="cyan",
        box=ROUNDED,
        padding=(1, 2) if not narrow else (0, 1),
    ))

    if plan.stages:
        stages_table = Table(show_header=False, box=None,
                             padding=(0, 1, 0, 1))
        stages_table.add_column(style="dim", justify="right", width=3)
        stages_table.add_column(style="bold cyan", no_wrap=True)
        stages_table.add_column(style="dim", overflow="fold")
        for i, s in enumerate(plan.stages, 1):
            stages_table.add_row(f"{i}.", s.tool, s.explanation or "")
        _console.print(Panel(
            stages_table,
            title="[bold]stages[/bold]",
            title_align="left",
            border_style="dim",
            box=ROUNDED,
            padding=(0, 1),
        ))

    meta = Table.grid(padding=(0, 1))
    meta.add_column(style="bold")
    meta.add_column(overflow="fold")
    if plan.expected_output:
        meta.add_row("output:", f"[green]{plan.expected_output}[/green]")
    if plan.risks:
        for r in plan.risks:
            meta.add_row("[yellow]risk:[/yellow]", f"[yellow]{r}[/yellow]")
    if plan.expected_output or plan.risks:
        _console.print(meta)


def _render_validation(v: ValidationResult) -> None:
    if v.errors:
        body = Text()
        for e in v.errors:
            body.append(f"  ✗ {e}\n", style="red")
        _console.print(Panel(
            body, title="[bold red]plan blocked[/bold red]",
            title_align="left", border_style="red", box=HEAVY,
        ))
    if v.warnings:
        body = Text()
        for w in v.warnings:
            body.append(f"  ! {w}\n", style="yellow")
        _console.print(Panel(
            body, title="[bold yellow]heads-up[/bold yellow]",
            title_align="left", border_style="yellow", box=ROUNDED,
        ))


def _render_answer(plan: Plan) -> None:
    if not plan.answer:
        _console.print("[dim](no answer)[/dim]")
        return
    _console.print(Panel(
        Markdown(plan.answer),
        border_style="cyan", box=ROUNDED,
        padding=(1, 2) if not _is_narrow() else (0, 1),
    ))


def _render_clarify(plan: Plan) -> None:
    _console.print(Panel(
        f"[bold]I need one thing first:[/bold]\n\n  {plan.question}",
        title="[yellow]question[/yellow]",
        title_align="left",
        border_style="yellow", box=ROUNDED,
        padding=(1, 2) if not _is_narrow() else (0, 1),
    ))


def _print_plan(plan: Plan) -> None:
    if not _HAVE_RICH:
        if plan.kind == "answer":
            print(plan.answer or "(no answer)")
        elif plan.kind == "clarify":
            print(f"I need one thing first:\n  {plan.question}\n")
        else:
            print("\n--- PROPOSED PIPELINE ---")
            print(plan.summary)
            print(render_pipeline(plan))
            for i, s in enumerate(plan.stages, 1):
                print(f"  {i}. {s.tool} -- {s.explanation}")
            if plan.expected_output:
                print(f"output: {plan.expected_output}")
            for r in plan.risks:
                print(f"risk: {r}")
        return

    if plan.kind == "answer":
        _render_answer(plan)
    elif plan.kind == "clarify":
        _render_clarify(plan)
    else:
        _render_pipeline_panel(plan)


# --- prompts ---------------------------------------------------------------

class UserExit(Exception):
    """Raised when the user hits Ctrl-C inside an InquirerPy prompt.
    Caller decides whether to abort the plan-handling or quit the REPL."""


def _confirm(message: str, default: bool = False) -> bool:
    if not _is_tty():
        return default
    try:
        from InquirerPy import inquirer
        return bool(inquirer.confirm(
            message=message, default=default,
            qmark="?", amark="✓",
        ).execute())
    except KeyboardInterrupt:
        raise UserExit()
    except Exception:
        try:
            ans = input(f"{message} [{'Y/n' if default else 'y/N'}]: ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            raise UserExit()
        if not ans:
            return default
        return ans.startswith("y")


def _menu(question: str, choices: list[tuple[str, str]]) -> str:
    if not _is_tty():
        return choices[0][1]
    try:
        from InquirerPy import inquirer
        from InquirerPy.base.control import Choice
        return inquirer.select(
            message=question,
            choices=[Choice(value=v, name=label) for label, v in choices],
            default=choices[0][1],
            qmark="?", amark="✓",
            pointer="▸",
        ).execute()
    except KeyboardInterrupt:
        raise UserExit()
    except Exception:
        for i, (label, _v) in enumerate(choices, 1):
            print(f"  {i}. {label}")
        while True:
            try:
                ans = input("Select (number): ").strip()
            except (EOFError, KeyboardInterrupt):
                raise UserExit()
            try:
                idx = int(ans) - 1
                if 0 <= idx < len(choices):
                    return choices[idx][1]
            except ValueError:
                pass


# --- main entry ------------------------------------------------------------

def handle_plan(plan: Plan, *, allow_execute: bool) -> int:
    """Render a plan and run the chosen action. Returns exit code.

    Raises UserExit if the user Ctrl-C's out of a menu/confirm. Caller
    (REPL or one-shot main) decides whether that means "skip this plan"
    or "quit the program".
    """
    _print_plan(plan)

    if plan.kind in ("answer", "clarify"):
        return 0

    validation = validate(plan)
    if _HAVE_RICH:
        _render_validation(validation)
    else:
        if validation.errors:
            print("\nplan blocked:")
            for e in validation.errors:
                print(f"  ✗ {e}")
        if validation.warnings:
            print("\nheads-up:")
            for w in validation.warnings:
                print(f"  ! {w}")

    choices: list[tuple[str, str]] = []
    if validation.ok and allow_execute:
        choices.append(("▶  run this pipeline", "run"))
    elif validation.ok and not allow_execute:
        choices.append(("◌  print only (--execute not set)", "print"))
    choices.extend([
        ("📋  copy pipeline to clipboard", "copy"),
        ("│   show as one-liner", "oneline"),
        ("✕   cancel", "cancel"),
    ])

    if _HAVE_RICH:
        _console.print()
    action = _menu("what would you like to do?", choices)

    if action == "cancel":
        if _HAVE_RICH:
            _console.print("[dim]cancelled.[/dim]")
        else:
            print("cancelled.")
        return 0

    if action == "oneline":
        parts = [render_command(s) for s in plan.stages]
        oneliner = " | ".join(parts)
        if _HAVE_RICH:
            _console.print()
            _console.print(Syntax(oneliner, "bash", theme="ansi_dark",
                                  background_color="default", word_wrap=True))
        else:
            print("\n" + oneliner)
        return 0

    if action == "copy":
        text = render_pipeline(plan)
        try:
            import pyperclip
            pyperclip.copy(text)
            if _HAVE_RICH:
                _console.print("[green]✓ copied to clipboard.[/green]")
            else:
                print("copied to clipboard.")
        except Exception:
            if _HAVE_RICH:
                _console.print("[yellow]clipboard unavailable. "
                               "pipeline:[/yellow]")
                _console.print(Syntax(text, "bash", theme="ansi_dark",
                                      background_color="default",
                                      word_wrap=True))
            else:
                print("clipboard unavailable:\n")
                print(text)
        return 0

    if action == "print":
        if _HAVE_RICH:
            _console.print("[dim]--execute not set; printing only:[/dim]")
            _console.print(Syntax(render_pipeline(plan), "bash",
                                  theme="ansi_dark",
                                  background_color="default",
                                  word_wrap=True))
        else:
            print("\n(--execute was not set; not running.)")
            print(render_pipeline(plan))
        return 0

    if action == "run":
        if not validation.ok:
            print_error("refusing to run -- validation failed.")
            return 2
        if validation.needs_network_confirm:
            if not _confirm(
                "this pipeline accesses network/cloud storage. continue?",
                default=False,
            ):
                if _HAVE_RICH:
                    _console.print("[dim]cancelled.[/dim]")
                else:
                    print("cancelled.")
                return 0
        if not _confirm("run it?", default=True):
            if _HAVE_RICH:
                _console.print("[dim]cancelled.[/dim]")
            else:
                print("cancelled.")
            return 0

        if _HAVE_RICH:
            _console.print(Rule("🌊", style="cyan"))
            _console.print("[bold cyan]executing...[/bold cyan]\n")
        else:
            print("\nrunning...\n")

        rc = execute(plan, validation)

        if _HAVE_RICH:
            _console.print(Rule(style="cyan"))
            if rc == 0:
                _console.print("[bold green]🎣 pipeline complete.[/bold green]")
            else:
                _console.print(f"[bold red]✗ pipeline failed "
                               f"(exit {rc}).[/bold red]")
        else:
            if rc == 0:
                print("\npipeline complete.")
            else:
                print(f"\npipeline failed (exit {rc}).")
        return rc

    return 0