"""Interactive UI for aa-help, dressed up with `rich`.

Design choices:
  - Width is CLAMPED to a sensible band (60-100 columns) in real terminals.
    On very wide terminals output stays readable; on narrow ones panels
    collapse padding to stay legible. In Jupyter we let Rich pick its own
    width (HTML render, no column constraint).
  - Emojis are USED SPARINGLY and ONLY in standalone single-line messages
    (banner, success, failure). Inside menus and tables we use plain text
    because mixing single-width (│ ▶ ✕) and double-width (📋) characters
    causes alignment to break in terminals that render emoji at width 1
    -- common over SSH, in tmux, and with non-Nerd-Font setups.
  - Ctrl-C and Ctrl-D ALWAYS exit cleanly. The REPL catches at every level
    and prints a single goodbye line; no tracebacks.
  - JUPYTER: detected at import time. When inside a kernel we swap the
    InquirerPy menus (which need a real TTY) for numbered input() prompts
    -- `input()` works inside Jupyter cells. We also emit code blocks as
    HTML with a real "copy" button via navigator.clipboard.
  - CLIPBOARD: pyperclip is the primary path on local desktops. Over SSH
    it usually fails silently; OSC 52 escape sequences are the fallback
    that works in any modern terminal (iTerm2, kitty, alacritty, recent
    xterm, Windows Terminal, tmux with `set-clipboard on`).
"""
from __future__ import annotations

import base64
import os
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


# --- environment detection -------------------------------------------------

def _detect_jupyter() -> bool:
    """True when running inside an IPython kernel (notebook / lab / qtconsole).

    We deliberately check the shell class name rather than a generic
    `get_ipython() is not None`, because plain IPython terminal sessions
    DO have a get_ipython() but still have a working TTY -- there
    InquirerPy works fine and we want to use it.
    """
    try:
        from IPython import get_ipython  # type: ignore
    except ImportError:
        return False
    ipy = get_ipython()
    if ipy is None:
        return False
    # ZMQInteractiveShell = notebook/lab/qtconsole. TerminalInteractiveShell
    # = `ipython` in a real terminal -- there we want regular TTY behavior.
    return ipy.__class__.__name__ == "ZMQInteractiveShell"


_IN_JUPYTER = _detect_jupyter()


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


# Width clamp for terminals. In Jupyter, Rich renders to HTML and
# we let it choose width naturally.
WIDTH_MIN = 60
WIDTH_MAX = 100


def _term_width() -> int:
    cols = shutil.get_terminal_size((80, 24)).columns
    return max(WIDTH_MIN, min(cols, WIDTH_MAX))


def _is_narrow() -> bool:
    if _IN_JUPYTER:
        return False
    cols = shutil.get_terminal_size((80, 24)).columns
    return cols < 70


def _make_console(stderr: bool = False):
    if not _HAVE_RICH:
        return None
    if _IN_JUPYTER:
        # Force jupyter mode so Rich emits HTML even if its auto-detect
        # gets confused (e.g., when stderr is redirected).
        return Console(stderr=stderr, force_jupyter=True)
    return Console(stderr=stderr, width=_term_width())


_console = _make_console(stderr=False)
_err_console = _make_console(stderr=True)


def _is_tty() -> bool:
    if _IN_JUPYTER:
        # Jupyter has no real TTY but we DO have a usable input() and
        # display() -- treat as interactive.
        return True
    return sys.stdin.isatty() and sys.stdout.isatty()


# --- clipboard -------------------------------------------------------------

def _osc52_copy(text: str) -> bool:
    """Copy via OSC 52 escape sequence. Returns True on attempt.

    Works in modern terminals (iTerm2, kitty, alacritty, WezTerm, Windows
    Terminal, recent xterm, tmux >=3.2 with `set -g set-clipboard on`).
    Crucially, this works OVER SSH where pyperclip can't reach the
    user's local clipboard.

    No way to confirm the terminal actually accepted the sequence -- we
    just write it and hope. That's fine because we already tried pyperclip
    first and this is the fallback path.
    """
    if not sys.stdout.isatty():
        return False
    try:
        encoded = base64.b64encode(text.encode("utf-8")).decode("ascii")
    except Exception:
        return False
    seq = f"\033]52;c;{encoded}\a"
    # If we're inside tmux, wrap in passthrough so tmux forwards the
    # OSC 52 to the outer terminal instead of swallowing it.
    if os.environ.get("TMUX"):
        seq = f"\033Ptmux;\033{seq}\033\\"
    try:
        sys.stdout.write(seq)
        sys.stdout.flush()
        return True
    except Exception:
        return False


def _copy_to_clipboard(text: str) -> tuple[bool, str]:
    """Try every clipboard route we know about. Returns (success, method).

    Order matters: pyperclip first because when it works it's silent and
    reliable. OSC 52 second as the SSH/remote fallback. We don't combine
    the two -- writing OSC 52 *and* pyperclipping puts you at the mercy
    of whichever the terminal latched onto last.
    """
    # 1. pyperclip: local desktop clipboard. Raises on missing backend.
    try:
        import pyperclip
        pyperclip.copy(text)
        return True, "system clipboard"
    except Exception:
        pass
    # 2. OSC 52: works over SSH and in modern terminal emulators.
    if _osc52_copy(text):
        return True, "terminal clipboard (OSC 52)"
    return False, ""


# --- jupyter HTML helpers --------------------------------------------------

def _jupyter_display_code(code: str, *, lang: str = "bash",
                          title: str = "proposed pipeline") -> None:
    """Render a code block in Jupyter with a real `copy` button.

    Uses navigator.clipboard.writeText, which is the standard async
    clipboard API. It needs a secure context (https or localhost) -- both
    JupyterLab and classic notebook qualify.
    """
    try:
        from IPython.display import display, HTML
    except ImportError:
        # Should never happen if _IN_JUPYTER is True, but be safe.
        print(f"--- {title} ---")
        print(code)
        return
    import html as html_lib
    import json as json_lib

    code_html = html_lib.escape(code)
    code_js = json_lib.dumps(code)  # JS-safe string literal
    # Inline styles only -- no external CSS. Notebooks strip <style> in
    # some configurations.
    snippet = f"""
    <div style="position:relative;background:#1e1e1e;color:#d4d4d4;
                padding:12px 14px;border-radius:6px;font-family:
                ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace;
                font-size:13px;line-height:1.5;white-space:pre;
                overflow-x:auto;margin:8px 0;border:1px solid #333;">
      <div style="position:absolute;top:6px;left:12px;
                  font-family:system-ui,-apple-system,sans-serif;
                  font-size:11px;color:#888;letter-spacing:0.5px;
                  text-transform:uppercase;">{html_lib.escape(title)}</div>
      <button
        onclick="navigator.clipboard.writeText({code_js}).then(
                   ()=>{{const t=this;t.innerText='✓ copied';
                         t.style.background='#2d5a2d';
                         setTimeout(()=>{{t.innerText='copy';
                                          t.style.background='#333';}},1500);}},
                   ()=>{{this.innerText='✗ failed';}});"
        style="position:absolute;top:6px;right:8px;
               background:#333;color:#ddd;border:1px solid #555;
               border-radius:4px;padding:3px 10px;cursor:pointer;
               font-family:system-ui,-apple-system,sans-serif;
               font-size:11px;">copy</button>
      <div style="margin-top:18px;"><code>{code_html}</code></div>
    </div>
    """
    display(HTML(snippet))


# --- public helpers --------------------------------------------------------

def print_banner(mode: str) -> None:
    """REPL banner shown at startup.

    The fish lives OUTSIDE the panel: emoji width is a renderer-vs-font
    coin flip, and putting one inside a Panel lets the emoji-line's right
    border drift one column away from every other line. Outside the panel
    there's no border to misalign.
    """
    if _HAVE_RICH:
        _console.print()
        _console.print("[bold cyan]>>>[/bold cyan] [bold cyan]aa-help[/bold cyan] "
                       "[cyan]— active acoustics assistant[/cyan]  [dim]🐟[/dim]")
        body = Text()
        if not _is_narrow():
            body.append("Ask anything about aalibrary or the aa-* tools.\n",
                        style="dim")
            body.append("\n", style="dim")
        body.append("Mode: ", style="dim")
        if mode == "execute":
            body.append("EXECUTE allowed", style="bold green")
        else:
            body.append("dry-run ", style="bold yellow")
            body.append("(--no-execute to keep dry-run; --execute is now default)",
                        style="dim")
        body.append("\nExit: ", style="dim")
        body.append("Ctrl-D, Ctrl-C, or /exit", style="dim italic")
        _console.print(Panel(
            body, border_style="cyan", box=ROUNDED,
            padding=(0, 2) if not _is_narrow() else (0, 1),
        ))
    else:
        print(">>> aa-help — active acoustics assistant 🐟")
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
    """Spinner during Vertex calls.

    In Jupyter the Rich spinner doesn't animate naturally inside a cell
    (it works via Live but adds clutter), so we just print a static line
    and skip the spinner there.
    """
    if _IN_JUPYTER:
        if _HAVE_RICH:
            _console.print(f"[dim]({label}...)[/dim]")
        else:
            print(f"({label}...)")
        yield
        return
    if _HAVE_RICH and _is_tty():
        try:
            with _console.status(f"[cyan]{label}...[/cyan]", spinner="dots"):
                yield
        except KeyboardInterrupt:
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

    # In Jupyter, render the pipeline as an HTML block with a real
    # clickable copy button. In a terminal, fall back to Rich Syntax
    # inside a Panel (the menu's "copy" action is still available).
    if _IN_JUPYTER:
        _jupyter_display_code(pipeline_text, lang="bash",
                              title="proposed pipeline")
    else:
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
            body.append(f"  x {e}\n", style="red")
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
    """Render the question prompt for a clarify plan.

    When `options` are present, we skip the panel here -- the menu itself
    will be the visual element. Without options, fall back to a panel
    asking the user to type a free-form answer.
    """
    if plan.options:
        return  # menu in handle_plan is the entire UI
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


def _numbered_menu(question: str, choices: list[tuple[str, str]]) -> str:
    """Pure-input() numbered menu. Used as the Jupyter path AND as the
    fallback when InquirerPy fails for any reason in a real terminal.

    Works in Jupyter cells because input() pops up a notebook input box.
    """
    if _HAVE_RICH and _IN_JUPYTER:
        # In Jupyter, the question has already been shown if needed; print
        # the choices nicely with Rich.
        _console.print(f"\n[bold yellow]?[/bold yellow] [bold]{question}[/bold]")
        for i, (label, _v) in enumerate(choices, 1):
            _console.print(f"  [cyan]{i}.[/cyan] {label}")
    else:
        print(f"\n{question}")
        for i, (label, _v) in enumerate(choices, 1):
            print(f"  {i}. {label}")

    while True:
        try:
            ans = input("Select (number): ").strip()
        except (EOFError, KeyboardInterrupt):
            raise UserExit()
        if not ans:
            continue
        try:
            idx = int(ans) - 1
            if 0 <= idx < len(choices):
                return choices[idx][1]
        except ValueError:
            pass
        # Invalid input -- prompt again.
        print(f"please enter a number between 1 and {len(choices)}")


def _confirm(message: str, default: bool = False) -> bool:
    if _IN_JUPYTER:
        try:
            ans = input(f"{message} [{'Y/n' if default else 'y/N'}]: "
                        ).strip().lower()
        except (EOFError, KeyboardInterrupt):
            raise UserExit()
        if not ans:
            return default
        return ans.startswith("y")

    if not _is_tty():
        return default
    try:
        from InquirerPy import inquirer
        return bool(inquirer.confirm(
            message=message, default=default,
            qmark="?", amark="+",
        ).execute())
    except KeyboardInterrupt:
        raise UserExit()
    except Exception:
        try:
            ans = input(f"{message} [{'Y/n' if default else 'y/N'}]: "
                        ).strip().lower()
        except (EOFError, KeyboardInterrupt):
            raise UserExit()
        if not ans:
            return default
        return ans.startswith("y")


def _menu(question: str, choices: list[tuple[str, str]]) -> str:
    # Jupyter: InquirerPy can't drive prompt_toolkit without a real TTY.
    # Use the numbered-input menu, which works because input() in a
    # notebook pops up a text widget below the cell.
    if _IN_JUPYTER:
        return _numbered_menu(question, choices)

    if not _is_tty():
        # Non-interactive (piped, redirected). Pick the first choice
        # silently to keep scripted runs working.
        return choices[0][1]

    try:
        from InquirerPy import inquirer
        from InquirerPy.base.control import Choice
        return inquirer.select(
            message=question,
            choices=[Choice(value=v, name=label) for label, v in choices],
            default=choices[0][1],
            qmark="?", amark="+",
            pointer="> ",
        ).execute()
    except KeyboardInterrupt:
        raise UserExit()
    except Exception:
        # Last-ditch fallback if InquirerPy explodes (broken terminfo,
        # weird locale, etc.). Numbered prompt is dumb but reliable.
        return _numbered_menu(question, choices)


# --- main entry ------------------------------------------------------------

def handle_plan(plan: Plan, *, allow_execute: bool) -> tuple[int, str | None]:
    """Render a plan and run the chosen action.

    Returns (exit_code, follow_up_question).
      - exit_code is 0 in normal cases, non-zero on execution failure.
      - follow_up_question is a string when the user picked a clarify option;
        the caller should re-plan with that as the new prompt. Otherwise None.

    Raises UserExit if the user Ctrl-C's out of a menu/confirm. Caller
    (REPL or one-shot main) decides whether that means "skip this plan"
    or "quit the program".
    """
    _print_plan(plan)

    if plan.kind == "answer":
        return 0, None

    if plan.kind == "clarify":
        if plan.options:
            # Plain-text choices for alignment safety -- no leading icons.
            choices: list[tuple[str, str]] = [(opt, opt) for opt in plan.options]
            choices.append(("(cancel / type my own answer)", "__cancel__"))

            if _HAVE_RICH and not _IN_JUPYTER:
                _console.print()
                _console.print(f"[bold yellow]?[/bold yellow] [bold]"
                               f"{plan.question}[/bold]")
            picked = _menu("pick one:" if not _IN_JUPYTER else plan.question,
                           choices)

            if picked == "__cancel__":
                if _HAVE_RICH:
                    _console.print("[dim](cancelled; ask a new question)[/dim]")
                return 0, None
            follow_up = f"{plan.question} -> {picked}"
            return 0, follow_up
        return 0, None

    validation = validate(plan)
    if _HAVE_RICH:
        _render_validation(validation)
    else:
        if validation.errors:
            print("\nplan blocked:")
            for e in validation.errors:
                print(f"  x {e}")
        if validation.warnings:
            print("\nheads-up:")
            for w in validation.warnings:
                print(f"  ! {w}")

    # Plain-text menu choices. NO leading icons mixed with emoji -- that's
    # what was breaking alignment in some terminals. The pointer (`>`) on
    # the selected row is sufficient visual feedback.
    choices: list[tuple[str, str]] = []
    if validation.ok and allow_execute:
        choices.append(("run this pipeline", "run"))
    elif validation.ok and not allow_execute:
        choices.append(("print only (--execute not set)", "print"))
    choices.extend([
        ("copy pipeline to clipboard", "copy"),
        ("show as one-liner", "oneline"),
        ("cancel", "cancel"),
    ])

    if _HAVE_RICH and not _IN_JUPYTER:
        _console.print()
    action = _menu("what would you like to do?", choices)

    if action == "cancel":
        if _HAVE_RICH:
            _console.print("[dim]cancelled.[/dim]")
        else:
            print("cancelled.")
        return 0, None

    if action == "oneline":
        parts = [render_command(s) for s in plan.stages]
        oneliner = " | ".join(parts)
        if _IN_JUPYTER:
            _jupyter_display_code(oneliner, lang="bash", title="one-liner")
        elif _HAVE_RICH:
            _console.print()
            _console.print(Syntax(oneliner, "bash", theme="ansi_dark",
                                  background_color="default", word_wrap=True))
        else:
            print("\n" + oneliner)
        return 0, None

    if action == "copy":
        text = render_pipeline(plan)
        if _IN_JUPYTER:
            # In Jupyter the pipeline panel already has its own copy
            # button. Re-display it for emphasis and let the user click.
            _jupyter_display_code(text, lang="bash",
                                  title="copy this pipeline")
            _console.print("[dim](click the `copy` button above)[/dim]")
            return 0, None
        ok, method = _copy_to_clipboard(text)
        if ok:
            if _HAVE_RICH:
                _console.print(f"[green]+ copied to {method}.[/green]")
            else:
                print(f"copied to {method}.")
        else:
            if _HAVE_RICH:
                _console.print("[yellow]clipboard unavailable. "
                               "pipeline:[/yellow]")
                _console.print(Syntax(text, "bash", theme="ansi_dark",
                                      background_color="default",
                                      word_wrap=True))
            else:
                print("clipboard unavailable:\n")
                print(text)
        return 0, None

    if action == "print":
        if _IN_JUPYTER:
            _jupyter_display_code(render_pipeline(plan), lang="bash",
                                  title="--execute not set; printing only")
        elif _HAVE_RICH:
            _console.print("[dim]--execute not set; printing only:[/dim]")
            _console.print(Syntax(render_pipeline(plan), "bash",
                                  theme="ansi_dark",
                                  background_color="default",
                                  word_wrap=True))
        else:
            print("\n(--execute was not set; not running.)")
            print(render_pipeline(plan))
        return 0, None

    if action == "run":
        if not validation.ok:
            print_error("refusing to run -- validation failed.")
            return 2, None
        if validation.needs_network_confirm:
            if not _confirm(
                "this pipeline accesses network/cloud storage. continue?",
                default=False,
            ):
                if _HAVE_RICH:
                    _console.print("[dim]cancelled.[/dim]")
                else:
                    print("cancelled.")
                return 0, None
        if not _confirm("run it?", default=True):
            if _HAVE_RICH:
                _console.print("[dim]cancelled.[/dim]")
            else:
                print("cancelled.")
            return 0, None
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
        return rc, None
    return 0, None