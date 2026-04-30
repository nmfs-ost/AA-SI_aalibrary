#!/usr/bin/env python3
"""
aa-fetch

Execute a multi-fetch job defined by a YAML file.

Reads a YAML path from a positional argument or from stdin, parses it via
aalibrary.utils.multi_fetch_yaml_parser, runs the resulting SQL against the
metadata DB, and downloads matching files into a per-run directory under
--output_root.

Pipeline contract:
- stdin  : optional YAML path (one line)
- stdout : NOTHING — this is the terminus of the build → fetch pipeline
- stderr : all logs and errors

Typical usage:
    aa-get -n request.yaml | aa-fetch
    aa-fetch ./request.yaml -o ./downloads -n run_001
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
# INFO level so the user sees fetch progress / counts / destination on stderr.
# Stdout intentionally stays empty per the pipeline contract above.
logger.add(sys.stderr, level="INFO")

# Heavy imports
import argparse
from datetime import datetime
from pathlib import Path


def silence_all_logs():
    """Re-apply suppression in case a library re-enabled logging
    or added its own loguru sink during initialization."""
    logging.disable(logging.CRITICAL)
    for name in [None] + list(logging.root.manager.loggerDict):
        lg = logging.getLogger(name)
        lg.handlers.clear()
        lg.propagate = True
    logger.remove()
    logger.add(sys.stderr, level="INFO")


def print_help() -> None:
    """Verbose help. Prints to stderr so it never contaminates pipeline stdout."""
    help_text = r"""
aa-fetch — Execute a YAML-driven multi-fetch job (no stdout output)

WHAT THIS TOOL DOES
  • Reads a YAML file describing one or more fetch "requests"
  • Parses it via aalibrary.utils.multi_fetch_yaml_parser
  • Builds a SQL query and runs it against the metadata DB
  • Downloads matching files into a per-run directory
  • Logs progress and errors to stderr (loguru)
  • Prints NOTHING to stdout — this is the terminus of the pipeline

HOW YAML_PATH IS PROVIDED
  (A) Positional argument:
      aa-fetch /path/to/fetch_request.yaml

  (B) Piped via stdin (one line):
      aa-get -n req.yaml | aa-fetch

  Notes:
    • If YAML_PATH is omitted and stdin is a TTY, aa-fetch prints help and exits.
    • When reading from stdin, aa-fetch reads exactly ONE line and strips whitespace.
    • Flags can be combined with stdin input:
          cat path.txt | aa-fetch -o ./downloads -n run_001

DOWNLOAD DIRECTORY
  -o, --output_root PATH
      Parent directory for the per-run download directory.
      Default: current working directory.

  -n, --download_dir_name NAME
      Name of the per-run download directory under --output_root.
      Default: aa_fetch_<YYYYMMDD_HHMMSS>

EXIT CODES
  0  success
  1  YAML/import/runtime/download error
  2  invalid usage (missing input, bad output dir name)

PIPELINE EXAMPLES
  aa-get | aa-fetch
  aa-get -n request.yaml | aa-fetch -o ./downloads -n run_001
  aa-fetch ./request.yaml

TROUBLESHOOTING
  • "File does not exist" — check the YAML path or your pipeline output.
  • Auth / BigQuery errors — ensure your GCP credentials are configured.
"""
    print(help_text.strip() + "\n", file=sys.stderr)


def _default_download_dir_name() -> str:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"aa_fetch_{stamp}"


def _read_yaml_path_from_stdin() -> Path | None:
    """Return Path read from stdin (single line), or None if stdin is a TTY / empty."""
    if sys.stdin.isatty():
        return None
    line = sys.stdin.readline()
    if not line:
        return None
    line = line.strip()
    if not line:
        return None
    return Path(line)


def main() -> int:
    # Help short-circuit
    if "--help" in sys.argv or "-h" in sys.argv:
        print_help()
        return 0

    # No-args + interactive terminal → show help
    if len(sys.argv) == 1 and sys.stdin.isatty():
        print_help()
        return 0

    parser = argparse.ArgumentParser(
        description="Execute aa-fetch YAML job (no stdout output).",
        add_help=False,
    )
    parser.add_argument(
        "yaml_path",
        type=Path,
        nargs="?",
        help="Path to YAML file. Optional — falls back to stdin.",
    )
    parser.add_argument(
        "-o", "--output_root",
        type=Path,
        default=None,
        help="Parent directory where the download directory will be created (default: CWD).",
    )
    parser.add_argument(
        "-n", "--download_dir_name",
        type=str,
        default=None,
        help="Download directory name under output_root (default: aa_fetch_<timestamp>).",
    )

    args = parser.parse_args()

    # ---------------------------
    # Resolve YAML path: positional > stdin > fail
    # ---------------------------
    if args.yaml_path is None:
        stdin_path = _read_yaml_path_from_stdin()
        if stdin_path is None:
            logger.error("No YAML path provided and no stdin available.")
            print_help()
            return 2
        args.yaml_path = stdin_path
        # Note: the previous version logged "YAML file does not exist" here
        # unconditionally — a copy-paste bug. The path may well exist; we
        # check next.
        logger.info(f"Read YAML path from stdin: {args.yaml_path}")

    if not args.yaml_path.exists():
        logger.error(f"YAML file does not exist: {args.yaml_path}")
        return 1

    if not args.yaml_path.is_file():
        logger.error(f"YAML path is not a file: {args.yaml_path}")
        return 1

    # ---------------------------
    # Resolve & create download directory
    # ---------------------------
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

    logger.info(f"Download directory: {download_dir}")

    # ---------------------------
    # Heavy import (deferred so --help / arg validation is fast)
    # ---------------------------
    try:
        import aalibrary.utils.multi_fetch_yaml_parser as mf
    except Exception as e:
        logger.exception(f"Failed to import multi_fetch_yaml_parser: {e}")
        return 1

    # ---------------------------
    # Execute fetch
    # ---------------------------
    try:
        # YAMLParser always populates sql_query, so the previous hasattr()
        # guard was dead code — log it directly.
        yaml_test = mf.YAMLParser(yaml_file_path=str(args.yaml_path))
        logger.info(f"SQL query built from YAML:\n{yaml_test.sql_query}")

        results = mf.parse_yaml_and_fetch_results(yaml_file_path=str(args.yaml_path))

        try:
            n = len(results)
        except TypeError:
            n = "?"
        logger.info(f"Result count: {n}")

        if not results:
            logger.warning("No files matched the YAML criteria — nothing to download.")
            return 0

        # IMPORTANT: do NOT wrap download_results in a "could not log cleanly"
        # except. The previous version did, and it silently swallowed real
        # download failures behind a benign-sounding message. Let download
        # errors surface as their own logged exception.
        try:
            mf.download_results(results, str(download_dir))
        except Exception as e:
            logger.exception(f"Download failed: {e}")
            return 1

        logger.success(f"aa-fetch complete. Files in: {download_dir}")
        return 0

    except Exception as e:
        logger.exception(f"aa-fetch failed: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())