#!/usr/bin/env python3
"""
aa-fetch

Execute a multi-fetch job defined by a YAML file.

Input (YAML path):
- Optional positional YAML_PATH
- OR a single-line YAML path via stdin (pipeline-friendly)
  Example:
    aa-get -n req.yaml | aa-fetch

Output:
- No stdout output. This tool performs an action only.
- Logs go to stderr via loguru.

Usage:
  aa-fetch [OPTIONS] [YAML_PATH]

Arguments:
  YAML_PATH
      Path to YAML file. Optional.
      If omitted, aa-fetch reads ONE line from stdin.
      Special: '-' forces reading YAML_PATH from stdin.

Options:
  -o, --output_root PATH
      Parent directory where the download directory will be created.
      Default: current working directory.

  -n, --download_dir_name NAME
      Name of the download directory under output_root.
      Default: aa_fetch_<timestamp>
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

from loguru import logger

import aalibrary.utils.multi_fetch_yaml_parser as mf


def _default_download_dir_name() -> str:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"aa_fetch_{stamp}"


def _read_one_line_from_stdin(label: str) -> str:
    line = sys.stdin.readline()
    if not line:
        raise RuntimeError(f"Expected {label} from stdin, but got EOF.")
    line = line.strip()
    if not line:
        raise RuntimeError(f"Expected {label} from stdin, but got empty line.")
    return line


def _resolve_yaml_path(positional: str | None) -> Path:
    """
    Resolve YAML path from positional or stdin.

    Rules:
    - If positional provided and not '-': use it
    - If positional is '-': read ONE line from stdin
    - If positional missing:
        - if stdin is piped (not a TTY): read ONE line from stdin
        - else: raise SystemExit(0) so caller can show help
    """
    if positional is not None:
        if positional.strip() == "-":
            if sys.stdin.isatty():
                raise RuntimeError("YAML_PATH is '-' but stdin is a TTY (nothing to read).")
            return Path(_read_one_line_from_stdin("YAML_PATH"))
        return Path(positional)

    # No positional; accept piped stdin
    if not sys.stdin.isatty():
        return Path(_read_one_line_from_stdin("YAML_PATH"))

    raise SystemExit(0)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Execute aa-fetch YAML job (no stdout output)."
    )

    parser.add_argument(
        "yaml_path",
        nargs="?",
        default=None,
        help="Path to YAML file. Optional. If omitted, reads one line from stdin. Use '-' to force stdin.",
    )

    parser.add_argument(
        "-o",
        "--output_root",
        type=Path,
        default=None,
        help="Parent directory where download directory will be created (default: CWD).",
    )

    parser.add_argument(
        "-n",
        "--download_dir_name",
        type=str,
        default=None,
        help="Download directory name under output_root (default: aa_fetch_<timestamp>).",
    )

    # Parse args
    args = parser.parse_args()

    # YAML path from positional or piped stdin
    try:
        yaml_path = _resolve_yaml_path(args.yaml_path).expanduser().resolve()
        print(yaml_path)
    except SystemExit:
        parser.print_help(sys.stderr)
        return 0
    except Exception as e:
        logger.exception(f"Failed to resolve YAML path: {e}")
        return 2

    if not yaml_path.exists():
        logger.error(f"YAML file does not exist: {yaml_path}")
        return 2
    if not yaml_path.is_file():
        logger.error(f"YAML path is not a file: {yaml_path}")
        return 2

    # Output directory handling (two flags you described)
    output_root = (args.output_root or Path.cwd()).expanduser().resolve()
    download_dir_name = (args.download_dir_name or _default_download_dir_name()).strip()

    if not download_dir_name:
        logger.error("--download_dir_name cannot be empty.")
        return 2

    try:
        output_root.mkdir(parents=True, exist_ok=True)
        download_dir = (output_root / download_dir_name).resolve()
        download_dir.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        logger.exception(f"Failed to create download directory under '{output_root}': {e}")
        return 2

    # Execute the exact logic you showed (but no stdout prints)
    try:
        yaml_test = mf.YAMLParser(yaml_file_path=str(yaml_path))

        # Log SQL query if present (stderr), but don't print to stdout
        if hasattr(yaml_test, "sql_query"):
            logger.info(f"SQL query built from YAML:\n{yaml_test.sql_query}")

        # Execute fetch. No BigQuery args here (per your requirement).
        # If your parser uses BigQuery internally, it can pull credentials from env/defaults.
        results = mf.parse_yaml_and_fetch_results(yaml_file_path=str(yaml_path))

        # Log summary (stderr)
        try:
            logger.info(f"Fetch results: {results}")
            logger.info(f"Result count: {len(results)}")
        except Exception:
            logger.info("Fetch completed (results not sizeable/loggable cleanly).")

        logger.info(f"Download directory: {download_dir}")
        return 0

    except Exception as e:
        logger.exception(f"aa-fetch failed: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())