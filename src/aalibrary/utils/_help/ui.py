"""Interactive UI for aa-help, dressed up with `rich`.

What this module provides:
  - handle_plan(plan, allow_execute) -- main entry; renders a Plan and runs
    the user's chosen action.
  - thinking() -- context manager spinner around long Vertex calls.
  - print_error(), print_info(), print_banner() -- helpers used by aa_help.py.

Design notes:
  - rich handles all output (panels, syntax highlighting, markdown).
  - InquirerPy handles all input (arrow-key menus, confirms).
  - When stdin/stdout isn't a TTY (piped, scripted), rich degrades to plain
    text and InquirerPy's menus fall back to numbered prompts.
"""
from __future__ import annotations

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


# --- rich console (single shared instance) ---------------------------------

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

_console = Console(stderr=False) if _HAVE_RICH else None
_err_console = Console(stderr=True) if _HAVE_RICH else None


def _is_tty() -> bool:
    return sys.stdin.isatty() and sys.stdout.isatty()


# --- public helpers used by aa_help.py -------------------------------------

def print_banner(mode: str) -> None:
    """REPL banner shown at startup."""
    if _HAVE_RICH:
        body = Text()
        body.append("aa-help interactive mode\n", style="bold cyan")
        body.append("Ask anything about aalibrary or active acoustics.\n\n",
                    style="dim")
        body.append("Mode: ", style="dim")
        if mode == "execute":
            body.append("EXECUTE allowed", style="bold green")
        else:
            body.append("dry-run ", style="bold yellow")
            body.append("(use --execute to enable)", style="dim")
        body.append("\nCommands: /reset  /exit  (Ctrl-D / Ctrl-C also exit)",
                    style="dim")
        _console.print(Panel(body, border_style="cyan", box=ROUNDED,
                             padding=(0, 2)))
    else:
        print("aa-help interactive mode. Ask anything.")
        print(f"Mode: {'EXECUTE allowed' if mode == 'execute' else 'dry-run'}")


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
    """Context manager: shows a spinner while Vertex is being called."""
    if _HAVE_RICH and _is_tty():
        with _console.status(f"[cyan]{label}...[/cyan]", spinner="dots"):
            yield
    else:
        print(f"[{label}]", file=sys.stderr)
        yield


# --- plan rendering --------------------------------------------------------

def _render_pipeline_panel(plan: Plan) -> None:
    """The hero panel for kind=pipeline plans."""
    pipeline_text = render_pipeline(plan)

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
        title="[bold]Proposed pipeline[/bold]",
        title_align="left",
        border_style="cyan",
        box=ROUNDED,
        padding=(1, 2),
    ))

    if plan.stages:
        stages_table = Table(show_header=False, box=None, padding=(0, 1, 0, 1))
        stages_table.add_column(style="dim", justify="right", width=3)
        stages_table.add_column(style="bold cyan", no_wrap=True)
        stages_table.add_column(style="dim")
        for i, s in enumerate(plan.stages, 1):
            stages_table.add_row(f"{i}.", s.tool, s.explanation or "")
        _console.print(Panel(
            stages_table,
            title="[bold]Stages[/bold]",
            title_align="left",
            border_style="dim",
            box=ROUNDED,
            padding=(0, 1),
        ))

    meta = Table.grid(padding=(0, 1))
    meta.add_column(style="bold")
    meta.add_column()
    if plan.expected_output:
        meta.add_row("Output:", f"[green]{plan.expected_output}[/green]")
    if plan.risks:
        for r in plan.risks:
            meta.add_row("[yellow]Risk:[/yellow]", f"[yellow]{r}[/yellow]")
    if plan.expected_output or plan.risks:
        _console.print(meta)


def _render_validation(v: ValidationResult) -> None:
    if v.errors:
        body = Text()
        for e in v.errors:
            body.append(f"  ✗ {e}\n", style="red")
        _console.print(Panel(
            body, title="[bold red]Plan blocked[/bold red]",
            title_align="left", border_style="red", box=HEAVY,
        ))
    if v.warnings:
        body = Text()
        for w in v.warnings:
            body.append(f"  ! {w}\n", style="yellow")
        _console.print(Panel(
            body, title="[bold yellow]Heads-up[/bold yellow]",
            title_align="left", border_style="yellow", box=ROUNDED,
        ))


def _render_answer(plan: Plan) -> None:
    """kind=answer -- a knowledge-question response, rendered as markdown."""
    if not plan.answer:
        _console.print("[dim](no answer)[/dim]")
        return
    _console.print(Panel(
        Markdown(plan.answer),
        border_style="cyan", box=ROUNDED, padding=(1, 2),
    ))


def _render_clarify(plan: Plan) -> None:
    """kind=clarify -- the planner needs one more thing."""
    _console.print(Panel(
        f"[bold]I need one thing first:[/bold]\n\n  {plan.question}",
        title="[yellow]Question[/yellow]",
        title_align="left",
        border_style="yellow", box=ROUNDED, padding=(1, 2),
    ))


def _print_plan(plan: Plan) -> None:
    if not _HAVE_RICH:
        # Plain fallback (matches the old ui.py).
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
                print(f"Output: {plan.expected_output}")
            for r in plan.risks:
                print(f"Risk: {r}")
        return

    if plan.kind == "answer":
        _render_answer(plan)
    elif plan.kind == "clarify":
        _render_clarify(plan)
    else:
        _render_pipeline_panel(plan)


# --- input prompts ---------------------------------------------------------

def _confirm(message: str, default: bool = False) -> bool:
    if not _is_tty():
        return default
    try:
        from InquirerPy import inquirer
        return bool(inquirer.confirm(
            message=message, default=default,
            qmark="?", amark="✓",
        ).execute())
    except Exception:
        ans = input(f"{message} [{'Y/n' if default else 'y/N'}]: ").strip().lower()
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
    except Exception:
        for i, (label, _v) in enumerate(choices, 1):
            print(f"  {i}. {label}")
        while True:
            ans = input("Select (number): ").strip()
            try:
                idx = int(ans) - 1
                if 0 <= idx < len(choices):
                    return choices[idx][1]
            except ValueError:
                pass


# --- main entry ------------------------------------------------------------

def handle_plan(plan: Plan, *, allow_execute: bool) -> int:
    _print_plan(plan)

    if plan.kind in ("answer", "clarify"):
        return 0

    validation = validate(plan)
    if _HAVE_RICH:
        _render_validation(validation)
    else:
        if validation.errors:
            print("\nPlan blocked:")
            for e in validation.errors:
                print(f"  ✗ {e}")
        if validation.warnings:
            print("\nHeads-up:")
            for w in validation.warnings:
                print(f"  ! {w}")

    choices: list[tuple[str, str]] = []
    if validation.ok and allow_execute:
        choices.append(("▶  Run this pipeline", "run"))
    elif validation.ok and not allow_execute:
        choices.append(("◌  Print only (--execute not set)", "print"))
    choices.extend([
        ("📋  Copy pipeline to clipboard", "copy"),
        ("│   Show as one-liner (for shell)", "oneline"),
        ("✕   Cancel", "cancel"),
    ])

    if _HAVE_RICH:
        _console.print()
    action = _menu("What would you like to do?", choices)

    if action == "cancel":
        if _HAVE_RICH:
            _console.print("[dim]Cancelled.[/dim]")
        else:
            print("Cancelled.")
        return 0

    if action == "oneline":
        parts = [render_command(s) for s in plan.stages]
        oneliner = " | ".join(parts)
        if _HAVE_RICH:
            _console.print()
            _console.print(Syntax(oneliner, "bash", theme="ansi_dark",
                                  background_color="default"))
        else:
            print("\n" + oneliner)
        return 0

    if action == "copy":
        text = render_pipeline(plan)
        try:
            import pyperclip
            pyperclip.copy(text)
            if _HAVE_RICH:
                _console.print("[green]✓ Copied to clipboard.[/green]")
            else:
                print("Copied to clipboard.")
        except Exception:
            if _HAVE_RICH:
                _console.print("[yellow]Clipboard unavailable. "
                               "Pipeline:[/yellow]")
                _console.print(Syntax(text, "bash", theme="ansi_dark",
                                      background_color="default"))
            else:
                print("Clipboard unavailable:\n")
                print(text)
        return 0

    if action == "print":
        if _HAVE_RICH:
            _console.print("[dim]--execute not set; printing only:[/dim]")
            _console.print(Syntax(render_pipeline(plan), "bash",
                                  theme="ansi_dark",
                                  background_color="default"))
        else:
            print("\n(--execute was not set; not running.)")
            print(render_pipeline(plan))
        return 0

    if action == "run":
        if not validation.ok:
            print_error("Refusing to run -- validation failed.")
            return 2
        if validation.needs_network_confirm:
            if not _confirm(
                "This pipeline accesses network/cloud storage. Continue?",
                default=False,
            ):
                if _HAVE_RICH:
                    _console.print("[dim]Cancelled.[/dim]")
                else:
                    print("Cancelled.")
                return 0
        if not _confirm("Run it?", default=True):
            if _HAVE_RICH:
                _console.print("[dim]Cancelled.[/dim]")
            else:
                print("Cancelled.")
            return 0

        if _HAVE_RICH:
            _console.print(Rule(style="cyan"))
            _console.print("[bold cyan]Executing...[/bold cyan]\n")
        else:
            print("\nRunning...\n")

        rc = execute(plan, validation)

        if _HAVE_RICH:
            _console.print(Rule(style="cyan"))
            if rc == 0:
                _console.print("[bold green]✓ Pipeline complete.[/bold green]")
            else:
                _console.print(f"[bold red]✗ Pipeline failed "
                               f"(exit {rc}).[/bold red]")
        else:
            if rc == 0:
                print("\nPipeline complete.")
            else:
                print(f"\nPipeline failed (exit {rc}).")
        return rc

    return 0