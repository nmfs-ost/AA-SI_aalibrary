"""Shared core of the aa-* console tools.

One implementation of the rules every tool follows:

    canon       canonical forms of scientific options (flag order and
                spelling never change a hash)
    identity    input identities and the product hash
    naming      the base-name and file-name rule
    provenance  file-level provenance: build, embed, read
    uris        gs:// URIs as first-class inputs and outputs
    stdio       the pipe contract (what a stage reads and prints)
    helptext    curated --help, same shape for every tool
    tool        ToolSpec + Run: the five calls a product tool makes
    chaining    what each tool reads, writes, adds and needs (front ends)
    describe    --describe: a tool as JSON

See docs/documentation/console_provenance.md for the design and
docs/documentation/console_tool_authoring.md for how to write a tool.
"""

from __future__ import annotations

import sys

from . import canon, chaining, describe, helptext, identity, naming, provenance, stdio, uris
from .helptext import Help, render
from .tool import Input, Output, Run, ToolSpec, add_common_flags, record_source


def help_mode(argv: list[str] | None = None) -> str | None:
    """'full' for --help-all, 'curated' for -h/--help, 'describe' for
    --describe, else None.

    Only flags before a bare '--' count, and only as whole arguments.
    """
    argv = sys.argv[1:] if argv is None else argv
    for arg in argv:
        if arg == "--":
            break
        if arg == "--help-all":
            return "full"
        if arg in ("-h", "--help"):
            return "curated"
        if arg == "--describe":
            return "describe"
    return None


def show_help(spec: ToolSpec, help_: Help, parser=None, *, full=None,
              argv: list[str] | None = None) -> bool:
    """Print curated help (or the full reference) if asked; True if printed."""
    mode = help_mode(argv)
    if mode is None:
        return False
    try:
        if mode == "describe":
            sys.stdout.write(describe.dumps(spec, help_, parser) + "\n")
        elif mode == "full" and full is not None:
            full()
        else:
            sys.stdout.write(render(spec, help_, parser))
        sys.stdout.flush()
    except BrokenPipeError:            # `aa-x --help-all | head`
        stdio.quiet_broken_pipe()
    return True


__all__ = [
    "canon", "chaining", "describe", "helptext", "identity", "naming", "provenance", "stdio",
    "uris",
    "Help", "render", "Input", "Output", "Run", "ToolSpec", "add_common_flags", "record_source",
    "help_mode", "show_help",
]
