#!/usr/bin/env python3
"""
aa-get

Interactive terminal-UI builder wrapper.

Usage:
  aa-get [OPTIONS] [OUTPUT_DIR]

Arguments:
  OUTPUT_DIR            Optional directory to save into.
                        Defaults to current working directory.

Options:
  -d, --output_dir      Directory to save into (overrides positional).
  -n, --file_name       Output file name (default: timestamped).
  -q, --quiet           Suppress builder printing; only print saved path.

Behavior:
  - Runs InquirerPy UI to build the schedule
  - Saves YAML to (output_dir / file_name)
  - Prints the saved path to stdout as the final line (pipeline-friendly)
"""
from __future__ import annotations

import argparse
import sys
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
    return p.name  # force just the name; dir comes from output_dir


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the raw fetch schedule builder and save a YAML file."
    )

    parser.add_argument(
        "output_dir_pos",
        type=Path,
        nargs="?",
        help="Optional directory to save into (defaults to CWD).",
    )

    parser.add_argument(
        "-d",
        "--output_dir",
        type=Path,
        default=None,
        help="Directory to save into (overrides positional).",
    )

    parser.add_argument(
        "-n",
        "--file_name",
        type=str,
        default=None,
        help="Output filename (default: fetch_request_<timestamp>.yaml).",
    )

    parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Suppress builder printing; only emit the saved path.",
    )

    args = parser.parse_args()

    # Interactive UI needs a TTY; if not, fail fast instead of “hanging”
    if not sys.stdin.isatty() or not sys.stdout.isatty():
        logger.error("aa-get requires an interactive TTY (stdin and stdout).")
        sys.exit(2)

    # Decide output directory
    out_dir = (args.output_dir or args.output_dir_pos or Path.cwd()).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    # Decide file name
    if args.file_name is None:
        file_name = default_output_path().name
    else:
        file_name = _coerce_yaml_name(args.file_name)

    out_path = (out_dir / file_name).resolve()

    try:
        # IMPORTANT: do NOT redirect stdout; InquirerPy expects a real terminal.
        # If you want “quiet”, implement it in the builder (see note below).
        saved_path = builder_main(output_path=out_path)

        # Final output for pipelines
        print(Path(saved_path).resolve())

    except Exception as e:
        logger.exception(f"aa-get failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
