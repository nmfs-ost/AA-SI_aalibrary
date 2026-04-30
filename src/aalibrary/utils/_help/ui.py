"""Interactive UI for aa-help using InquirerPy.

When stdin is a real TTY, presents arrow-key menus. When stdin is piped
(e.g., from `aa-get | aa-help "..."`), falls back to a plain text rendering
that won't try to draw a TUI.
"""
from __future__ import annotations

import os
import shutil
import sys

from .plan import Plan
from .safety import (
    ValidationResult,
    render_command,
    render_pipeline,
    validate,
)
from .executor import execute


def _is_tty() -> bool:
    return sys.stdin.isatty() and sys.stdout.isatty()


def _hr(char: str = "─") -> str:
    width = shutil.get_terminal_size((80, 24)).columns
    return char * max(20, min(width, 100))


def _print_plan(plan: Plan) -> None:
    if plan.kind == "answer":
        print(plan.answer or "(no answer)")
        return
    if plan.kind == "clarify":
        print(f"I need one thing first:\n  {plan.question}\n")
        return

    # pipeline
    print()
    print(_hr())
    print("PROPOSED PIPELINE")
    print(_hr())
    if plan.summary:
        print(plan.summary)
        print()
    print(render_pipeline(plan))
    print()
    if plan.stages:
        print("Stages:")
        for i, st in enumerate(plan.stages, 1):
            tail = f" -- {st.explanation}" if st.explanation else ""
            print(f"  {i}. {st.tool}{tail}")
        print()
    if plan.expected_output:
        print(f"Expected output: {plan.expected_output}")
    if plan.risks:
        print("Risks / notes:")
        for r in plan.risks:
            print(f"  ! {r}")
    print(_hr())


def _show_validation(v: ValidationResult) -> None:
    if v.errors:
        print("\nValidation BLOCKED this plan:")
        for e in v.errors:
            print(f"  ✗ {e}")
    if v.warnings:
        print("\nValidation warnings:")
        for w in v.warnings:
            print(f"  ! {w}")


def _confirm(message: str, default: bool = False) -> bool:
    if not _is_tty():
        return default
    try:
        from InquirerPy import inquirer
        return bool(inquirer.confirm(message=message, default=default).execute())
    except Exception:
        ans = input(f"{message} [{'Y/n' if default else 'y/N'}]: ").strip().lower()
        if not ans:
            return default
        return ans.startswith("y")


def _menu(question: str, choices: list[tuple[str, str]]) -> str:
    """Show a menu of (label, value) and return the chosen value."""
    if not _is_tty():
        # Non-interactive fallback: pick the first non-destructive choice.
        return choices[0][1]
    try:
        from InquirerPy import inquirer
        from InquirerPy.base.control import Choice
        return inquirer.select(
            message=question,
            choices=[Choice(value=v, name=label) for label, v in choices],
            default=choices[0][1],
        ).execute()
    except Exception:
        # Fallback to numeric prompt if InquirerPy isn't available or fails.
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


def handle_plan(plan: Plan, *, allow_execute: bool) -> int:
    """Render the plan and run the user's chosen action. Returns exit code."""
    _print_plan(plan)

    if plan.kind in ("answer", "clarify"):
        return 0

    validation = validate(plan)
    _show_validation(validation)

    # Build menu options based on what's safe / available.
    choices: list[tuple[str, str]] = []
    if validation.ok and allow_execute:
        choices.append(("Run this pipeline", "run"))
    elif validation.ok and not allow_execute:
        choices.append(("Print only (--execute not set)", "print"))
    choices.extend([
        ("Copy pipeline to clipboard", "copy"),
        ("Show as one-liner (for shell)", "oneline"),
        ("Cancel", "cancel"),
    ])

    action = _menu("What would you like to do?", choices)

    if action == "cancel":
        print("Cancelled.")
        return 0

    if action == "oneline":
        # Single-line version, ` | ` joined.
        parts = [render_command(s) for s in plan.stages]
        print("\n" + " | ".join(parts))
        return 0

    if action == "copy":
        text = render_pipeline(plan)
        try:
            import pyperclip
            pyperclip.copy(text)
            print("Copied to clipboard.")
        except Exception:
            print("Clipboard not available. Here's the pipeline:\n")
            print(text)
        return 0

    if action == "print":
        print("\n(--execute was not set; not running.)")
        print(render_pipeline(plan))
        return 0

    if action == "run":
        if not validation.ok:
            # Belt-and-suspenders.
            print("\nRefusing to run -- validation failed.")
            return 2
        if validation.needs_network_confirm:
            if not _confirm(
                "This pipeline accesses network/cloud storage. Continue?",
                default=False,
            ):
                print("Cancelled.")
                return 0
        if not _confirm("Run it?", default=True):
            print("Cancelled.")
            return 0
        print("\nRunning...\n")
        return execute(plan, validation)

    return 0
