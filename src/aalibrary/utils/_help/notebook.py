"""Jupyter-friendly entry point for aa-help.

Why this module exists:
  When you run `!aa-help "question"` from a notebook cell, that's a
  shell-magic SUBPROCESS. The subprocess has no IPython, so our Jupyter
  detection fails, the menus fall back to a non-interactive default, and
  the HTML copy button never reaches the notebook because subprocesses
  only have stdout -- no `display()` channel into the kernel.

  This module gives you a pure-Python entry point. Calling `ask("...")`
  from a cell stays inside the kernel, so:
    - the renderer detects Jupyter and emits HTML copy buttons
    - the menu uses input() (which Jupyter routes to a cell text widget)
    - confirm prompts work
    - everything is rendered with Rich's Jupyter HTML output

Usage:
    from aalibrary.utils._help.notebook import ask
    ask("how do I compute Sv from a raw file?")
    ask("download the cruise from azure", execute=False)  # dry-run

If you keep getting "not configured" errors, do the one-time setup in a
real terminal (`aa-help --setup`) -- the wizard wants stdin and isn't
worth porting to ipywidgets.
"""
from __future__ import annotations

from . import config as cfg
from .planner import Planner
from .ui import (
    UserExit,
    handle_plan,
    print_error,
    print_goodbye,
    thinking,
)


# Cache the planner across cells so we don't pay Vertex client init on
# every ask(). Tests/long sessions can call reset() to drop it.
_planner: Planner | None = None
_settings_signature: tuple | None = None


def _settings_sig(s: cfg.Settings) -> tuple:
    """Tuple of fields whose change should invalidate the cached planner."""
    return (s.project_id, s.location, s.model)


def _get_planner() -> Planner:
    global _planner, _settings_signature
    settings = cfg.load_config()
    if not settings.is_complete():
        raise RuntimeError(
            "aa-help is not configured. Run `aa-help --setup` in a real "
            "terminal first to set project_id and location, then come "
            "back to the notebook."
        )
    sig = _settings_sig(settings)
    if _planner is None or sig != _settings_signature:
        _planner = Planner(settings)
        _settings_signature = sig
    return _planner


def ask(question: str, *, execute: bool = True, max_followups: int = 3) -> int:
    """Run one aa-help round inside the notebook kernel.

    Args:
      question: free-form question or goal, same as the CLI.
      execute: if True (default), pipelines may run after confirmation.
        Set False for a pure dry-run that only proposes commands.
      max_followups: cap on clarify->re-plan rounds. The planner can ask
        for clarification; each "pick one" choice counts as a follow-up.

    Returns:
      Exit code from the final action: 0 for plain answers / cancelled
      pipelines / successful pipelines, non-zero for execution failure
      (matches the CLI's exit-code conventions).
    """
    planner = _get_planner()
    prompt = question
    last_rc = 0
    try:
        for _ in range(max_followups + 1):
            with thinking("planning"):
                plan = planner.plan(prompt)
            last_rc, follow_up = handle_plan(plan, allow_execute=execute)
            if not follow_up:
                return last_rc
            prompt = follow_up
        return last_rc
    except (UserExit, KeyboardInterrupt):
        print_goodbye()
        return 130
    except Exception as e:
        print_error(str(e))
        return 1


def reset() -> None:
    """Drop the cached planner. Call after editing config or switching models."""
    global _planner, _settings_signature
    _planner = None
    _settings_signature = None