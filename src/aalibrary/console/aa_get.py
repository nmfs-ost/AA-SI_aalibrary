#!/usr/bin/env python3
"""
aa-get

Interactive terminal UI tool that builds a raw-fetch schedule YAML file.

Key behaviors:
- Runs an interactive InquirerPy UI (needs a TTY).
- Saves YAML to a user-chosen directory + filename.
- Prints ONLY the saved YAML path to stdout (pipeline-safe).
- When stdout is piped, UI output is automatically sent to stderr so piping works:
    aa-get -n test.yaml | cat

Usage:
  aa-get [OPTIONS] [OUTPUT_DIR]

Arguments:
  OUTPUT_DIR
      Optional directory to save into.
      Defaults to current working directory.

      Special value:
        -   Read OUTPUT_DIR from stdin (one line). Useful if you *want* to pipe
            an output dir into aa-get without hanging in normal use.
            Example:
              echo /tmp/schedules | aa-get -n test.yaml -

Options:
  -d, --output_dir PATH
      Directory to save into (overrides positional OUTPUT_DIR).

  -n, --file_name NAME
      Output filename. Adds .yaml if missing.
      Default: fetch_request_<timestamp>.yaml

  -h, --help
      Show help.

Examples:
  aa-get
  aa-get ./schedules
  aa-get -d ./schedules -n test.yaml
  aa-get -n test.yaml | cat
  echo ./schedules | aa-get -n test.yaml - | cat
"""
from __future__ import annotations

import argparse
import sys
from contextlib import contextmanager
from pathlib import Path

from loguru import logger

# Import your app's main + its default naming helper
from aalibrary.utils.raw_fetch_schedule_builder import default_output_path, main as builder_main


def print_help() -> None:
    print(__doc__.strip() + "\n")


def _coerce_yaml_file_name(name: str) -> str:
    """
    Ensure filename is non-empty and ends with .yaml.
    Returns filename only (no directory component).
    """
    name = (name or "").strip()
    if not name:
        raise ValueError("file_name cannot be empty.")

    p = Path(name)

    # If user passed something like "dir/name.yaml", keep only the name.
    # Directory is controlled by output_dir to avoid ambiguity.
    if p.name == "":
        raise ValueError("file_name must include a filename.")

    if p.suffix == "":
        p = p.with_suffix(".yaml")

    return p.name


def _read_output_dir_from_stdin() -> Path:
    """
    Read ONE line from stdin for OUTPUT_DIR, used only when positional OUTPUT_DIR is '-'.
    """
    if sys.stdin.isatty():
        raise RuntimeError("OUTPUT_DIR is '-' but stdin is a TTY (nothing to read).")

    line = sys.stdin.readline()
    if not line:
        raise RuntimeError("OUTPUT_DIR is '-' but stdin provided no data.")
    return Path(line.strip())


@contextmanager
def _ui_stdout_to_stderr_when_piped():
    """
    In interactive tools, stdout being piped makes stdout non-TTY.
    InquirerPy/prompt_toolkit expects a TTY for UI rendering.

    Solution:
    - If stdout is a TTY: do nothing.
    - If stdout is piped but stderr is a TTY: temporarily route sys.stdout -> sys.stderr
      so UI renders on the terminal, while stdout stays clean for pipeline output.
    - If neither stdout nor stderr is a TTY: cannot run interactive UI.
    """
    if sys.stdout.isatty():
        yield
        return

    if sys.stderr.isatty():
        old_stdout = sys.stdout
        try:
            sys.stdout = sys.stderr
            yield
        finally:
            sys.stdout = old_stdout
        return

    raise RuntimeError(
        "aa-get requires a TTY for the interactive UI, but neither stdout nor stderr "
        "is a TTY. Run interactively or allocate a TTY."
    )


def main() -> int:
    # Preserve a handle to the "real" stdout for the final pipeline-safe print.
    real_stdout = sys.stdout

    # aa-clean style help trigger if user literally passes no args and wants help:
    # For aa-get, running with no args is valid, so we don't auto-print help here.

    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("-h", "--help", action="store_true", help="Show help and exit.")

    parser.add_argument(
        "output_dir_pos",
        nargs="?",
        default=None,
        help="Optional directory to save into. Use '-' to read directory from stdin.",
    )

    parser.add_argument(
        "-d",
        "--output_dir",
        type=str,
        default=None,
        help="Directory to save into (overrides positional OUTPUT_DIR).",
    )

    parser.add_argument(
        "-n",
        "--file_name",
        type=str,
        default=None,
        help="Output filename (default: fetch_request_<timestamp>.yaml).",
    )

    args = parser.parse_args()

    if args.help:
        print_help()
        return 0

    # ---------------------------
    # Resolve output directory
    # ---------------------------
    try:
        if args.output_dir is not None:
            out_dir = Path(args.output_dir).expanduser()
        elif args.output_dir_pos is not None:
            if str(args.output_dir_pos).strip() == "-":
                out_dir = _read_output_dir_from_stdin().expanduser()
            else:
                out_dir = Path(str(args.output_dir_pos)).expanduser()
        else:
            out_dir = Path.cwd()

        out_dir = out_dir.resolve()
        out_dir.mkdir(parents=True, exist_ok=True)

    except Exception as e:
        logger.exception(f"Failed to resolve/create output directory: {e}")
        return 2

    # ---------------------------
    # Resolve filename
    # ---------------------------
    try:
        if args.file_name is None:
            file_name = default_output_path().name
        else:
            file_name = _coerce_yaml_file_name(args.file_name)
    except Exception as e:
        logger.exception(f"Invalid --file_name: {e}")
        return 2

    out_path = (out_dir / file_name).resolve()

    # ---------------------------
    # Run interactive builder
    # ---------------------------
    try:
        # If stdout is piped, UI output is routed to stderr (TTY) automatically.
        with _ui_stdout_to_stderr_when_piped():
            saved_path = builder_main(output_path=out_path)

        # Pipeline contract: ONLY the saved path goes to stdout.
        # Use the real stdout handle even if we temporarily redirected sys.stdout.
        print(Path(saved_path).resolve(), file=real_stdout)
        real_stdout.flush()
        return 0

    except Exception as e:
        logger.exception(f"aa-get failed: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
