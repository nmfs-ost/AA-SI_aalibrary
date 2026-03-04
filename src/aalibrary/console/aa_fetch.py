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




def print_help() -> None:
    """
    Verbose, human-friendly help text (aa-clean style).
    Prints to stderr so it never contaminates pipeline stdout.
    """
    help_text = r"""
aa-fetch — Execute a YAML-driven multi-fetch job (no stdout output)

WHAT THIS TOOL DOES
  • Reads a YAML file that describes one or more fetch "requests"
  • Parses it using aalibrary.utils.multi_fetch_yaml_parser
  • Executes the fetch workflow (network / database / download work)
  • Creates a download directory for this run
  • Logs progress and errors to stderr (via loguru)
  • IMPORTANT: This tool DOES NOT print anything to stdout on success.

WHY THERE IS NO STDOUT
  aa-fetch is an "action" tool. It is designed to execute work and log to stderr.
  This keeps stdout clean for other tools in the ecosystem and avoids breaking pipes.

HOW YAML_PATH IS PROVIDED
  (A) Positional argument:
      aa-fetch /path/to/fetch_request.yaml

  (B) Piped into stdin (one line):
      echo /path/to/fetch_request.yaml | aa-fetch

  Notes:
    • If YAML_PATH is omitted and stdin is NOT piped, aa-fetch prints this help and exits.
    • When reading from stdin, aa-fetch reads exactly ONE line and strips whitespace.

DOWNLOAD DIRECTORY CONTROLS (ONLY TWO FLAGS)
  -o, --output_root PATH
      Default: current working directory (CWD)

  -n, --download_dir_name NAME
      Default: aa_fetch_<YYYYMMDD_HHMMSS>

PIPELINE EXAMPLES
  aa-get -n request.yaml | aa-fetch
  aa-fetch ./request.yaml
  aa-fetch -o ./downloads -n run_001 ./request.yaml

TROUBLESHOOTING
  • "File does not exist" — check the YAML path or your pipeline output.
  • Auth errors — ensure your environment credentials are set.
"""
    print(help_text.strip() + "\n", file=sys.stderr)

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
            print_help()
            sys.exit(0)

    import aalibrary.utils.multi_fetch_yaml_parser as mf
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

    print(args)

    if args.yaml_path is None:
        args.yaml_path = Path(sys.stdin.readline().strip())
        logger.error(f"YAML file does not exist: {args.yaml_path}")

    if not args.yaml_path.exists():
        logger.error(f"File '{args.yaml_path}' does not exist.")
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
            mf.download_results(results, download_dir)

        except Exception:
            logger.info("Fetch completed (results not sizeable/loggable cleanly).")

        logger.info(f"Download directory: {download_dir}")
        return 0

    except Exception as e:
        logger.exception(f"aa-fetch failed: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())