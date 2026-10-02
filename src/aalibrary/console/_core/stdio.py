"""The pipe contract, in one place.

Input tokens (positional argument, or lines on stdin) may be:
    /abs/or/relative/path.nc
    file:///abs/path.nc
    gs://bucket/key.nc
    {"uri": "...", ...}     an aa/1 JSON handle (aa-combine --json, aa-store --json)
Blank lines and lines starting with '#' are ignored.

stdout carries only results (one path/URI per line); everything else goes
to stderr. An empty stdin from a pipe is an error, not a request for help:
it almost always means the previous stage failed, and printing help to
stdout would feed the help text to the next stage as if it were a path.
"""

from __future__ import annotations

import json
import os
import sys
from typing import NoReturn

EXIT_NO_INPUT = 1


class NoInput(SystemExit):
    pass


def _token(line: str) -> str | None:
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    if line.startswith("{"):
        try:
            handle = json.loads(line)
        except ValueError:
            print(f"warning: skipping an unreadable JSON line on stdin: {line[:80]}",
                  file=sys.stderr)
            return None
        if not isinstance(handle, dict):
            return None
        value = handle.get("uri") or handle.get("path") or handle.get("url")
        if not value:
            return None
        line = str(value)
    if line.startswith("file://"):
        from .uris import from_file_uri

        line = from_file_uri(line)
    return line


def normalize_token(value: str | None) -> str | None:
    """Apply the token rules to a positional argument too."""
    if value is None:
        return None
    return _token(str(value))


def stdin_is_piped() -> bool:
    try:
        return not sys.stdin.isatty()
    except (AttributeError, ValueError):
        return False


def read_tokens(limit: int | None = None) -> list[str]:
    """Tokens from stdin (all lines, or the first ``limit``)."""
    out: list[str] = []
    if not stdin_is_piped():
        return out
    for line in sys.stdin:
        tok = _token(line)
        if tok is None:
            continue
        out.append(tok)
        if limit is not None and len(out) >= limit:
            break
    return out


def fail(tool: str, message: str, code: int = 1) -> NoReturn:
    print(f"{tool}: {message}", file=sys.stderr)
    raise SystemExit(code)


def one_input(positional: str | None, tool: str) -> str:
    """The single input path/URI: the positional argument, else one stdin line."""
    tok = normalize_token(positional)
    if tok:
        return tok
    if not stdin_is_piped():
        fail(tool, "no input: pass a path or gs:// URI, or pipe one in (see --help).")
    toks = read_tokens(limit=1)
    if not toks:
        fail(tool, "received nothing on stdin; the previous stage in the pipe "
                   "probably failed (see its error above).", EXIT_NO_INPUT)
    return toks[0]


def many_inputs(positionals: list[str] | None, tool: str, *, allow_empty: bool = False) -> list[str]:
    """All inputs: the positional arguments, else every stdin line."""
    toks = [t for t in (normalize_token(p) for p in (positionals or [])) if t]
    if toks:
        return toks
    if not stdin_is_piped():
        if allow_empty:
            return []
        fail(tool, "no input: pass paths or gs:// URIs, or pipe them in (see --help).")
    toks = read_tokens()
    if not toks and not allow_empty:
        fail(tool, "received nothing on stdin; the previous stage in the pipe "
                   "probably failed (see its error above).", EXIT_NO_INPUT)
    return toks


def quiet_broken_pipe() -> None:
    """The reader went away (``| head``): send the rest of stdout nowhere.

    Without this Python prints a BrokenPipeError traceback at exit.
    """
    try:
        devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull, sys.stdout.fileno())
    except (OSError, ValueError, AttributeError):
        pass


def write(text: str) -> None:
    """Write to stdout, tolerating a closed pipe."""
    try:
        sys.stdout.write(text)
        sys.stdout.flush()
    except BrokenPipeError:
        quiet_broken_pipe()


def emit(value: object) -> None:
    """Print one result for the next stage."""
    write(str(value) + "\n")


__all__ = ["NoInput", "normalize_token", "stdin_is_piped", "read_tokens", "fail",
           "one_input", "many_inputs", "emit", "write", "quiet_broken_pipe"]
