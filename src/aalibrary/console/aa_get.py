#!/usr/bin/env python3
"""
aa-get

Console tool wrapper for the Raw Fetch Schedule Builder (terminal UI).

Usage:
  aa-get [OPTIONS] [OUTPUT_DIR]

Behavior:
  - Launches the interactive builder UI (InquirerPy).
  - Writes the YAML to: (output_dir / file_name)
  - Prints ONLY the saved YAML path to stdout (pipeline-safe).

Positional:
  OUTPUT_DIR   Optional. Directory to save into.
               If omitted, defaults to current working directory.
               If not provided and stdin is piped, reads OUTPUT_DIR from stdin.

Options:
  -d, --output_dir   Directory to save into (overrides positional).
  -n, --file_name    Output filename (default: fetch_request_<timestamp>.yaml)

Example:
  aa-get -d ./schedules -n rl2107.yaml
  echo ./schedules | aa-get -n rl2107.yaml
"""
from __future__ import annotations

import argparse
import io
import sys
from contextlib import redirect_stdout
from pathlib import Path

from loguru import logger

# Import the real app main (separate file)
from aalibrary.utils.raw_fetch_schedule_builder import default_output_path, main as builder_main

def print_help() -> None:
    print(__doc__.strip() + "\n")


def _coerce_yaml_name(name: str) -> str:
    name = name.strip()
    if not name:
        raise ValueError("file_name cannot be empty.")
    p = Path(name)
    if p.suffix == "":
        return str(p.with_suffix(".yaml"))
    return name


def main() -> None:
    # aa-clean style: if no args and stdin is piped, treat it as positional path
    if len(sys.argv) == 1:
        if not sys.stdin.isatty():
            stdin_data = sys.stdin.readline().strip()
            if stdin_data:
                sys.argv.append(stdin_data)
        else:
            print_help()
            sys.exit(0)

    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("-h", "--help", action="store_true", help="Show help and exit.")

    # Positional OUTPUT_DIR (optional)
    parser.add_argument(
        "output_dir_pos",
        type=Path,
        nargs="?",
        help="Optional directory to save into (pipeline-friendly positional).",
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
        help="Output file name (default: fetch_request_<timestamp>.yaml).",
    )

    args = parser.parse_args()

    if args.help:
        print_help()
        sys.exit(0)

    # Decide output directory
    out_dir: Path
    if args.output_dir is not None:
        out_dir = args.output_dir
    elif args.output_dir_pos is not None:
        out_dir = args.output_dir_pos
    else:
        out_dir = Path.cwd()

    out_dir = out_dir.expanduser()

    # Decide file name
    if args.file_name is None:
        # reuse builder's default naming (timestamped)
        file_name = default_output_path().name
    else:
        file_name = _coerce_yaml_name(args.file_name)

    out_path = (out_dir / file_name).resolve()

    # Run the interactive builder, but keep stdout clean for pipelines
    buf = io.StringIO()
    try:
        with redirect_stdout(buf):
            saved_path = builder_main(output_path=out_path)

        # Send captured UI chatter to stderr (optional)
        # If you want it quieter, comment these lines out.
        captured = buf.getvalue().strip()
        if captured:
            logger.debug("\n" + captured)

        # Pipeline-safe: ONLY print the final path
        print(Path(saved_path).resolve())

    except Exception as e:
        logger.exception(f"aa-get failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
