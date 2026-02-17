#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from contextlib import contextmanager
from pathlib import Path

from loguru import logger

from aalibrary.utils.raw_fetch_schedule_builder import default_output_path, main as builder_main


def _coerce_yaml_name(name: str) -> str:
    name = name.strip()
    if not name:
        raise ValueError("file_name cannot be empty.")
    p = Path(name)
    if p.suffix == "":
        p = p.with_suffix(".yaml")
    return p.name  # keep only filename; directory comes from output_dir


@contextmanager
def _tty_io_if_piped():
    """
    If stdout is piped (not a TTY), run the interactive UI on /dev/tty
    so InquirerPy still has a real terminal, while stdout remains a pipe.
    """
    if sys.stdout.isatty() and sys.stdin.isatty():
        # Normal interactive run
        yield
        return

    # If we're piped, try to attach UI to the controlling terminal.
    try:
        tty = open("/dev/tty", "r+", encoding="utf-8", buffering=1)
    except OSError as e:
        logger.error(
            "stdout is piped but no /dev/tty is available. "
            "Run interactively or allocate a TTY. "
            f"Underlying error: {e}"
        )
        raise

    old_stdin, old_stdout = sys.stdin, sys.stdout
    try:
        sys.stdin = tty
        sys.stdout = tty
        yield
    finally:
        sys.stdin = old_stdin
        sys.stdout = old_stdout
        try:
            tty.close()
        except Exception:
            pass


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Interactive raw fetch schedule builder; prints saved YAML path."
    )

    parser.add_argument(
        "output_dir_pos",
        type=Path,
        nargs="?",
        help="Optional directory to save into (defaults to CWD).",
    )
    parser.add_argument(
        "-d", "--output_dir",
        type=Path,
        default=None,
        help="Directory to save into (overrides positional).",
    )
    parser.add_argument(
        "-n", "--file_name",
        type=str,
        default=None,
        help="Output filename (default: timestamped fetch_request_*.yaml).",
    )

    args = parser.parse_args()

    out_dir = (args.output_dir or args.output_dir_pos or Path.cwd()).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    file_name = default_output_path().name if args.file_name is None else _coerce_yaml_name(args.file_name)
    out_path = (out_dir / file_name).resolve()

    try:
        # Run UI on a real TTY if stdout is piped
        with _tty_io_if_piped():
            saved_path = builder_main(output_path=out_path)

        # IMPORTANT: stdout is reserved for pipelines — print only the final path.
        print(Path(saved_path).resolve())

    except Exception as e:
        logger.exception(f"aa-get failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
