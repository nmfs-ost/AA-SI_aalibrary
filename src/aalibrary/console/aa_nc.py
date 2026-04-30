#!/usr/bin/env python3
"""
aa-nc

Console tool for converting raw echosounder files (.raw) to NetCDF using
Echopype's open_raw / EchoData.to_netcdf. Produces a multi-group NetCDF
EchoData file suitable as input to aa-sv.

This tool does NOT compute Sv and does NOT remove background noise — it
is purely the RAW → NetCDF conversion stage of the pipeline.

Pipeline-friendly: writes the output path to stdout, all logs to stderr.

Typical pipeline usage:
    aa-nc --sonar_model EK60 input.raw | aa-sv | aa-clean
"""

# === Silence logs BEFORE any heavy imports ===
import logging
import sys
import warnings

logging.disable(logging.CRITICAL)
warnings.filterwarnings("ignore")

from loguru import logger
logger.remove()
# Default sink: WARNING+ to stderr so real errors aren't swallowed.
logger.add(sys.stderr, level="WARNING")

# Now the heavy imports — anything they log gets squashed
import argparse
import pprint
from pathlib import Path

import echopype as ep


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


def print_help():
    help_text = """
    Usage: aa-nc [OPTIONS] INPUT_PATH

    Arguments:
    INPUT_PATH                  Path to the input .raw file. (Required,
                                may also be supplied via stdin.)

    Options:
    -o, --output_path           Path to save the converted NetCDF output.
                                Default: same directory as input, with the
                                .raw stem and a .nc suffix.

    --sonar_model               Sonar model identifier (REQUIRED).
                                Examples: EK60, EK80, AZFP, EA640.

    Description:
    Converts a raw echosounder file (.raw) into a multi-group NetCDF
    EchoData file using echopype.open_raw. The output is the input to
    the next pipeline stage (aa-sv), which is what actually computes Sv.

    The input .raw file is never modified.

    Example:
    aa-nc /path/to/input.raw --sonar_model EK60 -o /path/to/output.nc
    """
    print(help_text)


def main():
    # No-args / explicit help
    if len(sys.argv) == 1:
        print_help()
        sys.exit(0)

    if "--help" in sys.argv or "-h" in sys.argv:
        print_help()
        sys.exit(0)

    parser = argparse.ArgumentParser(
        description="Convert .raw files to NetCDF EchoData with Echopype.",
        add_help=False,
    )

    parser.add_argument(
        "input_path",
        type=Path,
        nargs="?",
        help="Path to the .raw file.",
    )
    parser.add_argument(
        "-o", "--output_path",
        type=Path,
        help="Path to save processed output. Default: input stem with .nc suffix.",
    )
    parser.add_argument(
        "--sonar_model",
        type=str,
        required=True,
        help="Sonar model identifier (e.g., EK60, EK80, AZFP, EA640).",
    )

    args = parser.parse_args()

    # ---------------------------
    # Validate input
    # ---------------------------
    if args.input_path is None:
        if sys.stdin.isatty():
            logger.error("No input path provided and no stdin available.")
            sys.exit(1)
        args.input_path = Path(sys.stdin.readline().strip())
        logger.info(f"Read input path from stdin: {args.input_path}")

    if not args.input_path.exists():
        logger.error(f"File '{args.input_path}' does not exist.")
        sys.exit(1)

    allowed_extensions = {".raw"}
    ext = args.input_path.suffix.lower()
    if ext not in allowed_extensions:
        logger.error(
            f"'{args.input_path.name}' is not a supported file type. "
            f"Allowed: {', '.join(sorted(allowed_extensions))}"
        )
        sys.exit(1)

    # ---------------------------
    # Resolve output path
    # ---------------------------
    if args.output_path is None:
        args.output_path = args.input_path.with_suffix(".nc")
    else:
        args.output_path = args.output_path.with_suffix(".nc")

    # Guard against clobbering the input. With the .raw → .nc extension
    # split this is essentially impossible, but cheap insurance against
    # someone passing -o pointing at the source file.
    if args.output_path.resolve() == args.input_path.resolve():
        logger.error(f"Refusing to overwrite input file: {args.input_path.resolve()}")
        sys.exit(1)

    # ---------------------------
    # Process file
    # ---------------------------
    try:
        args_summary = {
            "input_path": args.input_path,
            "output_path": args.output_path,
            "sonar_model": args.sonar_model,
        }
        logger.debug(
            f"Executing aa-nc configured with [OPTIONS]:\n"
            f"{pprint.pformat(args_summary)}"
        )

        process_file(
            input_path=args.input_path,
            output_path=args.output_path,
            sonar_model=args.sonar_model,
        )

        logger.success(
            f"Generated {args.output_path.resolve()} with aa-nc. "
            "Passing .nc path to stdout..."
        )
        # Pipe the output path to stdout for the next tool
        print(args.output_path.resolve())

    except Exception as e:
        logger.exception(f"Error during processing: {e}")
        sys.exit(1)


def process_file(input_path: Path, output_path: Path, sonar_model: str):
    """Load a raw file as EchoData and save it as a multi-group NetCDF.

    No Sv computation, no noise removal — those happen downstream in
    aa-sv and aa-clean. This is just the conversion stage.
    """
    logger.info(f"Loading {input_path} into EchoData (sonar_model={sonar_model})")
    ed = ep.open_raw(raw_file=input_path, sonar_model=sonar_model)

    logger.info(f"Saving EchoData to {output_path}")
    ed.to_netcdf(save_path=output_path)
    logger.success(f"RAW → NetCDF conversion complete: {output_path.resolve()}")


if __name__ == "__main__":
    main()