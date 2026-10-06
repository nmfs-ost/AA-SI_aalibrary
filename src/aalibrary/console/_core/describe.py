"""``aa-<tool> --describe``: what a tool is, as JSON, for programs.

The same facts ``--help`` shows people, in a form a front end can build a
form from: the tool's role and product kind, which options are scientific
(enter the hash), every option argparse knows (flags, type, default, choices,
help), the curated help text, and how the tool chains (chaining.py).
"""

from __future__ import annotations

import argparse
import json
import re

from . import chaining

SCHEMA = "aa-describe/1"


def _action(action: argparse.Action) -> dict:
    default = action.default
    if isinstance(default, float) and default != default:
        default = None
    if not isinstance(default, (str, int, float, bool, list, type(None))):
        default = None
    return {
        "dest": action.dest,
        "flags": list(action.option_strings),
        "kind": type(action).__name__,
        "nargs": action.nargs if isinstance(action.nargs, (str, int)) else None,
        "type": getattr(action.type, "__name__", None) if action.type else None,
        "default": default,
        "choices": [str(c) for c in action.choices] if action.choices else [],
        "help": "" if action.help == argparse.SUPPRESS else (action.help or ""),
        "required": bool(action.required),
    }


def describe(spec, help_, parser) -> dict:
    actions = [_action(a) for a in (getattr(parser, "_actions", None) or [])
               if not isinstance(a, argparse._HelpAction) and a.help != argparse.SUPPRESS]
    options = {}
    for entry in getattr(help_, "options", None) or []:
        try:
            flags, text = entry
        except (TypeError, ValueError):
            continue
        for flag in re.findall(r"--?[A-Za-z][\w-]*", flags):
            options.setdefault(flag, text)
    try:
        from importlib.metadata import version

        aal = version("aalibrary")
    except Exception:  # noqa: BLE001
        aal = ""
    return {
        "schema": SCHEMA,
        "name": spec.name,
        "aalibrary": aal,
        "role": spec.role,
        "kind": spec.kind,
        "op": spec.op or spec.name,
        "opVersion": spec.op_version,
        "science": sorted((spec.params or {}).keys()),
        "scienceOptional": sorted(getattr(spec, "optional", ()) or ()),
        "summary": getattr(help_, "summary", "") or "",
        "does": getattr(help_, "does", "") or "",
        "reads": getattr(help_, "stdin", "") or "",
        "writes": getattr(help_, "stdout", "") or "",
        "chaining": getattr(help_, "pipeline", "") or "",
        "examples": list(getattr(help_, "examples", None) or []),
        "scienceText": dict(getattr(help_, "science", None) or {}),
        "options": options,
        "actions": actions,
        "traits": chaining.traits_of(spec.name),
    }


def dumps(spec, help_, parser) -> str:
    return json.dumps(describe(spec, help_, parser), sort_keys=True)


__all__ = ["SCHEMA", "describe", "dumps"]
