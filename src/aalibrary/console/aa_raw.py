#!/usr/bin/env python3
"""
aa-raw

Download a raw echosounder file from NCEI. Pipeline-friendly: prints the
absolute path of the downloaded file to stdout so the rest of the
aa-suite can pick it up.

Pipeline contract (mirrors the rest of the aa-suite):
    input  : flags only — no stdin (this tool is a SOURCE, not a consumer)
    output : .raw file on disk; absolute path printed to stdout
    logs   : stderr via loguru

Typical pipeline usage:
    aa-raw --file_name D20190804-T113723.raw \\
           --ship_name Henry_B._Bigelow \\
           --survey_name HB1907 \\
           --sonar_model EK60 \\
           --file_download_directory ./downloads \\
        | aa-nc --sonar_model EK60 \\
        | aa-sv \\
        | aa-clean \\
        | aa-graph
"""
from __future__ import annotations

# === Silence logs BEFORE any heavy imports ===
import logging
import sys
import warnings

logging.disable(logging.CRITICAL)
warnings.filterwarnings("ignore")

from loguru import logger
logger.remove()
# Default sink: WARNING+ to stderr so real errors aren't swallowed.
# _configure_logging() below replaces this once --quiet / --debug are parsed.
logger.add(sys.stderr, level="WARNING")

import argparse
import pprint
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
    logger.add(sys.stderr, level="WARNING")


def _configure_logging(quiet: bool, debug: bool) -> None:
    """Replace the suppression sink with one at the user's chosen level.
    --debug wins over --quiet (mutually-exclusive check happens in main)."""
    logger.remove()
    if debug:
        logger.add(sys.stderr, level="DEBUG", backtrace=True, diagnose=False)
    elif quiet:
        logger.add(sys.stderr, level="WARNING", backtrace=False, diagnose=False)
    else:
        logger.add(sys.stderr, level="INFO", backtrace=True, diagnose=False)


def print_help() -> None:
    help_text = """
    Usage: aa-raw [OPTIONS]

    Required:
      --file_name NAME            Name of the file to download
                                  (e.g. D20190804-T113723.raw).
      --ship_name NAME            Name of the ship (e.g. Henry_B._Bigelow).
      --survey_name NAME          Name of the survey (e.g. HB1907).
      --sonar_model NAME          Type of echosounder (e.g. EK60, EK80).

    Optional:
      --file_type TYPE            File type (default: raw).
      --data_source SRC           Data source identifier (default: NCEI).
                                  Currently only 'NCEI' is wired through; other
                                  values log a warning and proceed as NCEI.
      --file_download_directory PATH
                                  Where to download. Default: current directory.
                                  Created if it doesn't exist.
      --upload_to_gcp             Also upload the downloaded file to GCP.
      --debug                     Verbose logging (DEBUG level on stderr).
      --quiet                     Suppress INFO logs; final path still prints.
      -h, --help                  Show this help and exit.

    Description:
      Downloads a raw echosounder file from NCEI given (ship, survey,
      sonar_model, file_name). The absolute path of the downloaded file
      is printed on stdout, ready for piping into aa-nc and onward.

    Pipeline example:
      aa-raw --file_name D20190804-T113723.raw \\
             --ship_name Henry_B._Bigelow --survey_name HB1907 \\
             --sonar_model EK60 --file_download_directory ./downloads \\
        | aa-nc --sonar_model EK60 | aa-sv | aa-clean

    Direct example:
      aa-raw --file_name D20190804-T113723.raw \\
             --ship_name Henry_B._Bigelow --survey_name HB1907 \\
             --sonar_model EK60 \\
             --file_download_directory Henry_B._Bigelow_HB1907_EK60_NCEI
    """
    print(help_text)


def main() -> None:
    # No-args / explicit help short-circuit
    if len(sys.argv) == 1:
        print_help()
        sys.exit(0)

    if "--help" in sys.argv or "-h" in sys.argv:
        print_help()
        sys.exit(0)

    parser = argparse.ArgumentParser(
        description="Download a raw echosounder file from NCEI.",
        add_help=False,
    )

    parser.add_argument("--file_name", required=True,
                        help="Name of the file to download.")
    parser.add_argument("--file_type", default="raw",
                        help="Type of the file (default: raw).")
    parser.add_argument("--ship_name", required=True,
                        help="Name of the ship.")
    parser.add_argument("--survey_name", required=True,
                        help="Name of the survey.")
    parser.add_argument("--sonar_model", required=True,
                        help="Type of echosounder (e.g. EK60).")
    # The previous version documented --data_source in print_help() but
    # never added it to argparse, so passing the documented flag errored
    # with "unrecognized arguments". Wired up now; logs a warning if
    # the user requests anything other than the only supported source.
    parser.add_argument("--data_source", default="NCEI",
                        help="Data source (default: NCEI).")
    parser.add_argument("--file_download_directory", default=".",
                        help="Directory to download into (default: CWD).")
    parser.add_argument("--upload_to_gcp", action="store_true",
                        help="Also upload the downloaded file to GCP.")
    parser.add_argument("--debug", action="store_true",
                        help="Enable verbose DEBUG-level logging.")
    parser.add_argument("--quiet", action="store_true",
                        help="Suppress INFO logs.")

    args = parser.parse_args()

    if args.debug and args.quiet:
        logger.error("Use --debug OR --quiet, not both.")
        sys.exit(2)

    _configure_logging(args.quiet, args.debug)

    if args.data_source.upper() != "NCEI":
        logger.warning(
            f"--data_source='{args.data_source}' requested, but aa-raw currently "
            "only downloads from NCEI. Proceeding as NCEI."
        )

    # Resolve and create the destination directory. The previous version
    # delegated this to download_raw_file_from_ncei; if that function
    # didn't create the directory, the download silently failed and we
    # still printed a non-existent path to stdout, breaking the next
    # pipeline stage with a confusing FileNotFoundError.
    download_dir = Path(args.file_download_directory).expanduser().resolve()
    try:
        download_dir.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        logger.error(f"Could not create download directory '{download_dir}': {e}")
        sys.exit(2)

    # Heavy import deferred so --help is fast and a typo on a required
    # arg fails fast (in argparse) before paying the import cost.
    try:
        from aalibrary.ingestion import download_raw_file_from_ncei
    except Exception as e:
        logger.exception(f"Failed to import aalibrary.ingestion: {e}")
        sys.exit(1)

    args_summary = {
        "file_name": args.file_name,
        "file_type": args.file_type,
        "ship_name": args.ship_name,
        "survey_name": args.survey_name,
        "sonar_model": args.sonar_model,
        "data_source": args.data_source,
        "file_download_directory": str(download_dir),
        "upload_to_gcp": args.upload_to_gcp,
        "debug": args.debug,
    }
    logger.debug(
        f"Executing aa-raw configured with [OPTIONS]:\n"
        f"{pprint.pformat(args_summary)}"
    )

    try:
        logger.info(
            f"Downloading {args.file_name} "
            f"({args.ship_name} / {args.survey_name} / {args.sonar_model}) "
            f"from NCEI -> {download_dir}"
        )
        download_raw_file_from_ncei(
            file_name=args.file_name,
            file_type=args.file_type,
            ship_name=args.ship_name,
            survey_name=args.survey_name,
            echosounder=args.sonar_model,
            file_download_directory=str(download_dir),
            upload_to_gcp=args.upload_to_gcp,
            debug=args.debug,
        )
    except Exception as e:
        logger.exception(f"Download failed: {e}")
        sys.exit(1)

    downloaded = download_dir / args.file_name

    # Sanity check: the underlying function returns nothing useful, so
    # we only know the download succeeded by checking the file is on
    # disk. Without this, a silent failure would print a non-existent
    # path to stdout and break the next pipeline stage downstream.
    if not downloaded.exists():
        logger.error(
            f"Download appeared to succeed, but '{downloaded}' is not on disk. "
            "Rerun with --debug for details."
        )
        sys.exit(1)

    logger.success(
        f"Downloaded {downloaded.name} via aa-raw. "
        "Passing .raw path to stdout..."
    )
    # Pipeline contract: print the absolute path on stdout.
    print(downloaded.resolve())


if __name__ == "__main__":
    main()