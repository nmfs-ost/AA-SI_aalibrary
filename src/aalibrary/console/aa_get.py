#!/usr/bin/env python3
"""
aa-get

Interactive terminal UI tool that builds a raw-fetch schedule YAML file.

Pipeline contract:
- Runs an interactive InquirerPy UI (needs a TTY somewhere — stdout OR stderr).
- Saves YAML to a user-chosen directory + filename.
- Prints ONLY the saved YAML path to stdout (pipeline-safe).
- When stdout is piped, UI output is automatically routed to stderr so piping works:
      aa-get -n test.yaml | aa-fetch

Usage:
  aa-get [OPTIONS] [OUTPUT_DIR]
"""
from __future__ import annotations

# === Silence noisy library logs BEFORE any heavy imports ===
import logging
import sys
import warnings

logging.disable(logging.CRITICAL)
warnings.filterwarnings("ignore")

from loguru import logger
logger.remove()
logger.add(sys.stderr, level="WARNING")

import argparse
from contextlib import contextmanager
from pathlib import Path

# Heavy imports — UI builder
from aalibrary.utils.raw_fetch_schedule_builder import (
    default_output_path,
    main as builder_main,
)


def silence_all_logs():
    """Re-apply suppression in case a library re-enabled logging."""
    logging.disable(logging.CRITICAL)
    for name in [None] + list(logging.root.manager.loggerDict):
        lg = logging.getLogger(name)
        lg.handlers.clear()
        lg.propagate = True
    logger.remove()
    logger.add(sys.stderr, level="WARNING")


def print_help() -> None:
    """Verbose help. Prints to stderr so it never contaminates pipeline stdout."""
    help_text = r"""
aa-get — Interactive YAML schedule builder (prints saved YAML path)

WHAT THIS TOOL DOES
  • Launches an interactive terminal UI (InquirerPy) to build a fetch schedule YAML
  • Saves the YAML to disk in a consistent schema
  • Prints ONLY the saved YAML path to stdout as the final line (pipeline-friendly)

WHY IT PRINTS A PATH TO STDOUT
  aa-get is designed to feed downstream tools by emitting the YAML file path.
  The primary intended downstream consumer is:

      aa-fetch

  This enables a clean two-step pipeline:
      (1) aa-get builds the YAML
      (2) aa-fetch consumes the YAML and performs the download

HOW TO USE WITH aa-fetch (RECOMMENDED)
  Build the YAML interactively, then immediately execute it:

      aa-get -n request.yaml | aa-fetch -o ./downloads -n run_001
      aa-get | aa-fetch                  # defaults to CWD + timestamped filename

  Notes:
    • During the interactive UI, aa-get may render UI output to your terminal.
    • The FINAL line written to stdout is the YAML file path.
    • aa-fetch reads that single path from stdin and executes the job.

USAGE
  aa-get [OPTIONS] [OUTPUT_DIR]

ARGUMENTS
  OUTPUT_DIR
      Optional directory to save into.
      Defaults to the current working directory.

      Special value:
        -   Read OUTPUT_DIR from stdin (one line). Useful if you want to pipe an
            output directory into aa-get explicitly:
              echo /tmp/schedules | aa-get -n request.yaml -

OPTIONS
  -d, --output_dir PATH
      Directory to save into (overrides positional OUTPUT_DIR).
      Default: current working directory.

  -n, --file_name NAME
      Output filename. Adds .yaml if missing.
      Default: fetch_request_<YYYYMMDD_HHMMSS>.yaml

  -h, --help
      Show this help and exit.

EXAMPLES
  1) Save into CWD with a timestamped filename:
      aa-get

  2) Save into a directory via positional OUTPUT_DIR:
      aa-get ./schedules

  3) Save into a directory via flag, with an explicit filename:
      aa-get -d ./schedules -n test.yaml

  4) Recommended end-to-end pipeline (build → execute):
      aa-get -n request.yaml | aa-fetch -o ./downloads -n run_001
      aa-get | aa-fetch
"""
    print(help_text.strip() + "\n", file=sys.stderr)


def _coerce_yaml_file_name(name: str) -> str:
    """Ensure filename is non-empty and ends with .yaml. Returns filename only."""
    name = (name or "").strip()
    if not name:
        raise ValueError("file_name cannot be empty.")

    p = Path(name)

    # If user passed something like "dir/name.yaml", keep only the name —
    # directory is controlled by output_dir to avoid ambiguity.
    if p.name == "":
        raise ValueError("file_name must include a filename.")

    if p.suffix == "":
        p = p.with_suffix(".yaml")

    return p.name


def _read_output_dir_from_stdin() -> Path:
    """Read ONE line from stdin for OUTPUT_DIR (used only when positional is '-')."""
    if sys.stdin.isatty():
        raise RuntimeError("OUTPUT_DIR is '-' but stdin is a TTY (nothing to read).")

    line = sys.stdin.readline()
    if not line:
        raise RuntimeError("OUTPUT_DIR is '-' but stdin provided no data.")
    return Path(line.strip())


@contextmanager
def _ui_stdout_to_stderr_when_piped():
    """
    Make the InquirerPy UI render correctly when stdout is piped to another tool.

    - If stdout is a TTY: do nothing.
    - If stdout is piped but stderr is a TTY: temporarily route sys.stdout → sys.stderr
      so UI renders on the terminal, while the *real* stdout stays clean for the
      pipeline.
    - If neither stdout nor stderr is a TTY: cannot run an interactive UI.
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

    # Help short-circuit before argparse so -h and --help behave identically
    # and don't trigger argparse's auto-help (we use add_help=False below).
    if "--help" in sys.argv or "-h" in sys.argv:
        print_help()
        return 0

    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument(
        "output_dir_pos",
        nargs="?",
        default=None,
        help="Optional directory to save into. Use '-' to read directory from stdin.",
    )
    parser.add_argument(
        "-d", "--output_dir",
        type=str,
        default=None,
        help="Directory to save into (overrides positional OUTPUT_DIR).",
    )
    parser.add_argument(
        "-n", "--file_name",
        type=str,
        default=None,
        help="Output filename (default: fetch_request_<timestamp>.yaml).",
    )

    args = parser.parse_args()

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
        with _ui_stdout_to_stderr_when_piped():
            saved_path = builder_main(output_path=out_path)

        # If the builder returns None, the user cancelled — emit nothing
        # and exit non-zero so the downstream pipeline doesn't get a stray path.
        if saved_path is None:
            return 1

        # Pipeline contract: ONLY the saved path goes to the real stdout,
        # even if we temporarily redirected sys.stdout for the UI.
        print(Path(saved_path).resolve(), file=real_stdout)
        real_stdout.flush()
        return 0

    except KeyboardInterrupt:
        # Don't dump a traceback for an intentional Ctrl+C.
        print("\nCancelled.", file=sys.stderr)
        return 130

    except Exception as e:
        logger.exception(f"aa-get failed: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())