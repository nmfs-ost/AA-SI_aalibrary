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


def main() -> int:


    if len(sys.argv) == 1:
        if not sys.stdin.isatty():
            stdin_data = sys.stdin.readline().strip()
            if stdin_data:
                sys.argv.append(stdin_data)
        else:
            #print_help()
            sys.exit(0)

    parser = argparse.ArgumentParser(
        description="Execute aa-fetch YAML job (no stdout output)."
    )

    parser.add_argument(
        "yaml_path",
        type=Path,
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



    if args.yaml_path is None:
        args.yaml_path = Path(sys.stdin.readline().strip())
        logger.error(f"YAML file does not exist: {args.yaml_path}")

    if not args.yaml_path.exists():
        logger.error(f"File '{args.input_path}' does not exist.")
        sys.exit(1)

    # Output directory handling (two flags you described)
    output_root = (args.output_root or Path.cwd()).expanduser().resolve()
    download_dir_name = (args.download_dir_name or _default_download_dir_name())

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
        yaml_test = mf.YAMLParser(yaml_file_path=str(args.yaml_path))

        # Log SQL query if present (stderr), but don't print to stdout
        if hasattr(yaml_test, "sql_query"):
            logger.info(f"SQL query built from YAML:\n{yaml_test.sql_query}")

        # Execute fetch. No BigQuery args here (per your requirement).
        # If your parser uses BigQuery internally, it can pull credentials from env/defaults.
        results = mf.parse_yaml_and_fetch_results(yaml_file_path=str(args.yaml_path))

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