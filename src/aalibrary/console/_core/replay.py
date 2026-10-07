"""The commands that made a product, from its provenance, for anyone to rerun.

A product's provenance records every step that led to it: the tool, its
scientific settings (canonical, as hashed), and the identity of every input.
This turns that record back into console commands:

* ``command``: the one step that made this product, from its direct inputs
  (their gs:// URIs), e.g. ``aa-integrate gs://…/x_Sv.nc --bottom gs://…evl …``.
* ``script``: the whole chain from the raw files, as a Bash script with one
  variable per product, so it can be read, changed and run anywhere.

Nothing here names a workstation: no local paths, no user folders. Inputs in
the bucket keep their gs:// URIs (anyone with access can read them); raw files
and anything that was only ever local appear by name, under ``$RAW`` or as a
placeholder to fill in. Outputs go to ``$DEST`` (default: the current folder).
The same inputs with the same settings make the same product hash, which is
what makes the script a check as well as a recipe.

Steps that cannot be rerun from their settings alone (a line or region drawn
in a viewer, an ECS written from typed values) are used as the files they
made, by URI.
"""

from __future__ import annotations

import argparse
import importlib
import re
import shlex
from collections.abc import Callable

from . import chaining

SCHEMA = "aa-commands/1"

#: Tools whose product comes from content the record holds only as a hash
#: (shapes drawn in a viewer, values typed into a form): reused, not rerun.
FROM_CONTENT = {"aa-annotate", "aa-ecs"}

#: Tools that make a new asset from several inputs (and so its base name).
NAMES_ASSET = {"aa-combine"}

Actions = list[dict]


# --------------------------------------------------------------------------- #
# What each tool's options are
# --------------------------------------------------------------------------- #
def _action_doc(action: argparse.Action) -> dict:
    return {
        "dest": action.dest,
        "flags": list(action.option_strings),
        "kind": type(action).__name__,
        "nargs": action.nargs if isinstance(action.nargs, (str, int)) else None,
        "default": action.default,
        "const": getattr(action, "const", None),
    }


_actions_cache: dict[str, Actions | None] = {}


def tool_actions(tool: str) -> Actions | None:
    """The tool's argparse actions (dest, flags, kind, default), or None."""
    if tool in _actions_cache:
        return _actions_cache[tool]
    found = None
    try:
        module = importlib.import_module("aalibrary.console." + tool.replace("-", "_"))
        build = getattr(module, "_build_parser", None) or getattr(
            module, "build_parser", None
        )
        if build is not None:
            parser = build()
            found = [
                _action_doc(a)
                for a in parser._actions
                if not isinstance(a, argparse._HelpAction)
            ]
    except Exception:  # noqa: BLE001 - an unknown tool is rendered without flags
        found = None
    _actions_cache[tool] = found
    return found


# --------------------------------------------------------------------------- #
# Values to flags
# --------------------------------------------------------------------------- #
def _long(flags: list[str]) -> str:
    longs = [f for f in flags if f.startswith("--")]
    return longs[0] if longs else (flags[0] if flags else "")


def _literal(value) -> str:
    """A --param KEY=VALUE value as the tools parse it (Python literals)."""
    if isinstance(value, (list, tuple)):
        return "(" + ",".join(_literal(v) for v in value) + ")"
    if isinstance(value, bool) or value is None:
        return repr(value)
    if isinstance(value, float) and value.is_integer():
        return str(int(value)) if abs(value) < 1e15 else repr(value)
    return str(value)


def _text(value) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def _same(a, b) -> bool:
    if (
        isinstance(a, (int, float))
        and isinstance(b, (int, float))
        and not isinstance(a, bool)
        and not isinstance(b, bool)
    ):
        return float(a) == float(b)
    return a == b


def flags_for(
    actions: Actions | None, params: dict, *, skip: set[str] = frozenset()
) -> tuple[list[str], list[str]]:
    """argv for these canonical settings, and the settings that have no flag."""
    argv: list[str] = []
    missing: list[str] = []
    by_dest: dict[str, list[dict]] = {}
    for a in actions or []:
        if a["flags"]:
            by_dest.setdefault(a["dest"], []).append(a)
    for name in sorted(params):
        value = params[name]
        if name in skip or value is None or value == {} or value == []:
            continue
        options = by_dest.get(name)
        if not options:
            missing.append(name)
            continue
        if isinstance(value, bool):
            if options[0].get("default") is not None and _same(
                options[0]["default"], value
            ):
                continue  # the default: nothing to say
            want = "_StoreTrueAction" if value else "_StoreFalseAction"
            match = [a for a in options if a["kind"] == want]
            if match:
                argv.append(_long(match[0]["flags"]))
            elif not any(
                a["kind"] in ("_StoreTrueAction", "_StoreFalseAction") for a in options
            ):
                argv += [_long(options[0]["flags"]), _text(value)]
            continue
        action = options[0]
        if _same(action.get("default"), value):
            continue
        flag = _long(action["flags"])
        if isinstance(value, dict):
            items = [
                f"{k}={_literal(v)}" for k, v in sorted(value.items()) if v is not None
            ]
            if action["kind"] == "_AppendAction":
                for item in items:
                    argv += [flag, item]
            elif items:
                argv += [flag, *items]
        elif isinstance(value, (list, tuple)):
            if action["kind"] == "_AppendAction":
                for item in value:
                    argv += [flag, _text(item)]
            elif action.get("nargs") in ("+", "*") or isinstance(
                action.get("nargs"), int
            ):
                argv += [flag, *(_text(v) for v in value)]
            else:
                argv += [flag, ",".join(_text(v) for v in value)]
        else:
            argv += [flag, _text(value)]
    return argv, missing


class _Ref(str):
    """A shell word this module built and quoted itself ("$RAW/x", "$SV"):
    left live. Everything else, which comes from the record, is quoted."""


def _quote(arg: str) -> str:
    """Quote for the shell. Only references built here stay live: a value
    from the record (a setting, a name) is always a literal, whatever it
    starts with, so a record cannot put a command in the script."""
    if isinstance(arg, _Ref):
        return str(arg)
    return shlex.quote(str(arg))


def _raw_ref(name: str) -> _Ref:
    """A raw file under $RAW, quoted for the shell."""
    if re.fullmatch(r"[\w.+-]+", name):
        return _Ref(f'"$RAW/{name}"')
    return _Ref('"$RAW"/' + shlex.quote(name))


def _comment(text: object) -> str:
    """One line of a script comment: no newline (or other control character)
    from the record can end it and start a command."""
    return "".join(ch if ch.isprintable() else " " for ch in str(text))


# --------------------------------------------------------------------------- #
# The record's graph
# --------------------------------------------------------------------------- #
def _products(step: dict) -> list[str]:
    p = step.get("product")
    if isinstance(p, list):
        return [f"aa:{x}" for x in p]
    return [f"aa:{p}"] if p else []


def _roles(step: dict, steps: list[dict]) -> dict[str, str]:
    """input id -> role, from the step's recipe inputs."""
    by_recipe: dict[str, list[str]] = {}
    for other in steps:
        if other.get("recipe"):
            by_recipe.setdefault(other["recipe"], []).extend(_products(other))
    roles: dict[str, str] = {}
    for item in step.get("recipe_inputs") or []:
        recipe, role = str(item.get("recipe") or ""), str(item.get("role") or "source")
        if recipe.startswith("aa:"):
            roles[recipe] = role
        elif recipe.startswith("r:"):
            for pid in by_recipe.get(recipe[2:], []):
                roles.setdefault(pid, role)
    return roles


def _flag_for_role(
    tool: str, role: str, actions: Actions | None, used: set[str]
) -> str:
    """The option an input with this role is given with ('' for the positional)."""
    if role == "source":
        return ""
    dests = {a["dest"]: a for a in actions or [] if a["flags"]}
    if role == "echodata":
        return (
            _long(dests["echodata"]["flags"]) if "echodata" in dests else "--echodata"
        )
    kind, _, dest = role.partition(":")
    if dest and dest in dests:
        return _long(dests[dest]["flags"])
    traits = chaining.TRAITS.get(tool)
    candidates = [
        d
        for d, kinds in (traits.inputs.items() if traits else [])
        if kind in kinds and d in dests
    ]
    fresh = [d for d in candidates if d not in used] or candidates
    if fresh:
        used.add(fresh[0])
        return _long(dests[fresh[0]]["flags"])
    return f"--{kind}"


def _var_name(tool: str, taken: set[str]) -> str:
    """A shell variable for a step's product, from the tool or a drawn
    line's name: [A-Z_][A-Z0-9_]*, whatever the name holds."""
    base = re.sub(r"[^A-Z0-9_]", "_", tool.removeprefix("aa-").upper()).strip("_")
    base = base or "OUT"
    if not re.match(r"[A-Z_]", base):
        base = f"_{base}"
    name, n = base, 1
    while name in taken:
        n += 1
        name = f"{base}{n}"
    taken.add(name)
    return name


# --------------------------------------------------------------------------- #
# The commands
# --------------------------------------------------------------------------- #
def commands(
    doc: dict,
    *,
    resolve: Callable[[str], str] | None = None,
    actions: Callable[[str], Actions | None] = tool_actions,
) -> dict:
    """``{"schema", "product", "command", "script", "notes"}`` for one record.

    *resolve* maps an input id (``aa:<hash>``) to its gs:// URI when the record
    itself does not say (an input of an input); without it such inputs are
    left as placeholders.
    """
    steps = [s for s in doc.get("pipeline") or [] if isinstance(s, dict)]
    product = doc.get("product") or {}
    name = product.get("name") or doc.get("file", {}).get("name") or ""
    notes: list[str] = []
    if not steps:
        return {
            "schema": SCHEMA,
            "product": product.get("hash", ""),
            "command": "",
            "script": "",
            "notes": ["No steps recorded."],
        }

    known: dict[str, str] = {}
    for item in doc.get("inputs") or []:
        uri = str(item.get("uri") or "")
        if item.get("id") and uri.startswith(("gs://", "s3://", "http://", "https://")):
            known[str(item["id"])] = uri

    def uri_of(pid: str) -> str:
        if pid in known:
            return known[pid]
        if resolve is not None:
            try:
                got = resolve(pid) or ""
            except Exception:  # noqa: BLE001 - an unresolved input stays a placeholder
                got = ""
            if got.startswith(("gs://", "s3://")):
                known[pid] = got
                return got
        return ""

    sources = {
        str(s.get("id")): str(s.get("name") or "") for s in doc.get("sources") or []
    }
    made_by: dict[str, int] = {}
    for i, step in enumerate(steps):
        for pid in _products(step):
            made_by[pid] = i

    # -- the last step, from its direct inputs ---------------------------------
    last = steps[-1]
    acts = actions(last.get("tool", ""))
    roles = _roles(last, steps)
    used: set[str] = set()
    positional: list[str] = []
    side: list[str] = []
    for pid in last.get("inputs") or []:
        ref = uri_of(pid) or (
            f"<{sources[pid]}>" if pid in sources else f"<{_input_name(doc, pid)}>"
        )
        flag = _flag_for_role(
            last.get("tool", ""), roles.get(pid, "source"), acts, used
        )
        if flag:
            side += [flag, ref]
        else:
            positional.append(ref)
    argv, missing = flags_for(acts, last.get("params") or {})
    command = " ".join(
        _quote(a) for a in [last.get("tool", ""), *positional, *side, *argv]
    )
    if missing:
        notes.append(
            f"{last.get('tool')}: recorded settings without an option: {', '.join(missing)}."
        )

    # -- the chain ---------------------------------------------------------------
    taken: set[str] = {"RAW", "DEST"}
    var_of: dict[str, str] = {}
    lines = [
        "#!/usr/bin/env bash",
        (
            f"# Remake {_comment(name or 'this product')} "
            f"(product aa:{_comment(str(product.get('hash', ''))[:8])}) "
            "with the AA-SI console tools."
        ),
        "# Each command prints the product it made; the same inputs and settings make",
        "# the same product hash. Set RAW and DEST, then run it.",
        "set -euo pipefail",
        'RAW="${RAW:-./raw}"     # a folder holding the raw files named below',
        'DEST="${DEST:-.}"       # where the products go: a folder, or gs://bucket/prefix/',
        "",
    ]
    needed = _needed(steps, len(steps) - 1, made_by)
    for i, step in enumerate(steps):
        if i not in needed:
            continue
        tool = step.get("tool", "")
        pids = _products(step)
        if tool in FROM_CONTENT:
            for pid in pids:
                var = _var_name(
                    tool
                    if tool != "aa-annotate"
                    else str((step.get("params") or {}).get("name") or "annotation"),
                    taken,
                )
                var_of[pid] = var
                uri = uri_of(pid)
                label = (step.get("params") or {}).get("name") or tool
                how = (
                    "drawn in a viewer"
                    if tool == "aa-annotate"
                    else "written from calibration values"
                )
                lines.append(
                    f"# {_comment(label)}: {how} ({tool}); used as the file it made"
                )
                lines.append(
                    f"{var}=" + shlex.quote(uri or f"<{label} file {pid[:11]}>")
                )
            continue
        acts = actions(tool)
        roles = _roles(step, steps)
        argv, missing = flags_for(acts, step.get("params") or {})
        if missing:
            notes.append(
                f"{tool}: recorded settings without an option: {', '.join(missing)}."
            )
        has_dest = any(a["dest"] == "dest" and a["flags"] for a in acts or [])
        count = max(1, len(pids))
        inputs = list(step.get("inputs") or [])
        per = len(inputs) // count if count > 1 and len(inputs) % count == 0 else None
        lines.append(
            f"# {_comment(tool)}"
            + (f" x{count}" if count > 1 else "")
            + f" ({_comment(step.get('op', ''))})"
        )
        for k in range(count):
            these = inputs[k * per : (k + 1) * per] if per else inputs
            used = set()
            positional, side = [], []
            for pid in these:
                if pid in var_of:
                    ref = _Ref(f'"${var_of[pid]}"')
                elif pid in sources:
                    ref = _raw_ref(sources[pid])
                else:
                    uri = uri_of(pid)
                    ref = uri if uri else f"<{_input_name(doc, pid)}>"
                flag = _flag_for_role(tool, roles.get(pid, "source"), acts, used)
                if flag:
                    side += [flag, ref]
                else:
                    positional.append(ref)
            var = _var_name(tool, taken)
            if k < len(pids):
                var_of[pids[k]] = var
            # A merge names a new asset: give it the recorded base name, so the
            # files come out named as before (the name is not in the hash).
            base = doc.get("base") if tool in NAMES_ASSET else None
            has_base = any(a["dest"] == "base" and a["flags"] for a in acts or [])
            call = [tool, *positional, *side, *argv]
            if base and has_base:
                call += ["--base", str(base)]
            if has_dest:
                call += ["--dest", _Ref('"$DEST"')]
            lines.append(f"{var}=$(" + " ".join(_quote(a) for a in call) + ")")
        lines.append("")
    final = var_of.get(_products(last)[0]) if _products(last) else None
    if final:
        lines.append(f'echo "${final}"')
    return {
        "schema": SCHEMA,
        "product": product.get("hash", ""),
        "command": command,
        "script": "\n".join(lines).rstrip() + "\n",
        "notes": notes,
    }


def _input_name(doc: dict, pid: str) -> str:
    for item in doc.get("inputs") or []:
        if item.get("id") == pid:
            return str(item.get("name") or pid[:11])
    return pid[:11]


def _needed(steps: list[dict], last: int, made_by: dict[str, int]) -> set[int]:
    """The steps the last one depends on (a record can carry side branches)."""
    keep: set[int] = set()
    stack = [last]
    while stack:
        i = stack.pop()
        if i in keep:
            continue
        keep.add(i)
        for pid in steps[i].get("inputs") or []:
            j = made_by.get(pid)
            if j is not None and j != i:
                stack.append(j)
    return keep
