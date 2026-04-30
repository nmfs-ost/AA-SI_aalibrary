"""Interactive REPL for aa-help."""
from __future__ import annotations

import sys

from .vertex_client import VertexHelper


_BANNER = """\
aa-help interactive mode. Ask anything about aalibrary or active acoustics.
Commands: /reset (clear history)   /exit (quit)   Ctrl-D / Ctrl-C also exit.
"""


def run_repl(helper: VertexHelper) -> int:
    print(_BANNER)
    while True:
        try:
            line = input("aa-help> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return 0
        if not line:
            continue
        if line in ("/exit", "/quit", ":q"):
            return 0
        if line == "/reset":
            helper.reset()
            print("[history cleared]")
            continue
        try:
            helper.ask(line, stream=True)
        except KeyboardInterrupt:
            print("\n[interrupted]")
        except Exception as e:  # noqa: BLE001
            print(f"[error] {e}", file=sys.stderr)