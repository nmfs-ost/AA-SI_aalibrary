#!/usr/bin/env python3
"""
Console tool for converting RAW files to NetCDF using Echopype,
removing background noise, applying transformations, and saving back.
"""

# === Silence logs BEFORE any heavy imports ===
import logging
import sys
import warnings

logging.disable(logging.CRITICAL)
warnings.filterwarnings("ignore")

from loguru import logger
logger.remove()
# Keep WARNING+ visible on stderr so real errors aren't swallowed.
# Drop this line if you want truly silent output.
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
    INPUT_PATH                 Path to the input .raw file. (Required)

    Options:
    -o, --output_path           Path to save processed Sv output (.nc file).
                                Default: creates a new .nc from the input .raw.

    --sonar_model               Sonar model number (required).
                                Example: EK60, EK80, etc.

    Description:
    This tool calculates Sv (volume backscattering strength) from a
    .raw file using Echopype. The output is always a NetCDF (.nc) file
    containing the computed Sv values. A new .nc file is created for the
    output; the input .raw file is never overwritten.

    Example:
    aa-nc /path/to/input.raw --sonar_model EK60 -o /path/to/output.nc
    """
    print(help_text)


def main():
    # Display help if no arguments are provided or if --help is explicitly passed
    if len(sys.argv) == 1 or "--help" in sys.argv:
        print_help()
        sys.exit(0)

    parser = argparse.ArgumentParser(
        description="Process .raw or .netcdf4 files with Echopype and remove background noise."
    )

    parser.add_argument(
        "input_path",
        type=Path,
        help="Path to the .raw or .netcdf4 file.",
        nargs="?",
    )
    parser.add_argument(
        "-o", "--output_path",
        type=Path,
        help="Path to save processed output. Default creates a new .nc for RAW.",
    )
    parser.add_argument(
        "--sonar_model",
        type=str,
        required=True,
        help="Sonar model number (e.g., EK60, EK80, etc.).",
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
        logger.success(f"⟶ Receiving .raw path from stdin :\n\t{args.input_path}")

    if not args.input_path.exists():
        logger.error(f"File '{args.input_path}' does not exist.")
        sys.exit(1)

    allowed_extensions = {".raw": "raw"}
    ext = args.input_path.suffix.lower()
    if ext not in allowed_extensions:
        logger.error(
            f"'{args.input_path.name}' is not a supported file type. "
            f"Allowed: {', '.join(allowed_extensions.keys())}"
        )
        sys.exit(1)

    file_type = allowed_extensions[ext]

    # ---------------------------
    # Set default output path
    # ---------------------------
    if args.output_path is None:
        if file_type == "raw":
            args.output_path = args.input_path.with_suffix(".nc")

    # ---------------------------
    # Process file
    # ---------------------------
    try:
        pretty_args = pprint.pformat(vars(args))
        logger.debug(
            f"Executing aa-nc configured with [OPTIONS]:\n{pretty_args}\n"
            f"* ( Each aa-nc associated option_name may be overridden using --option_name value )"
        )
        process_file(
            input_path=args.input_path,
            output_path=args.output_path,
            sonar_model=args.sonar_model,
        )
        logger.success(f"Generated {args.output_path.resolve()} with aa-nc.")
        print(args.output_path.resolve())
    except Exception as e:
        logger.exception(f"Error during processing: {e}")
        sys.exit(1)


def clean_attrs(Sv):
    for k, v in Sv.attrs.items():
        if v is None:
            Sv.attrs[k] = "NA"
    for var in Sv.data_vars:
        for k, v in Sv[var].attrs.items():
            if v is None:
                Sv[var].attrs[k] = "NA"
    return Sv


def process_file(input_path: Path, output_path: Path, sonar_model: str):
    """
    Load EchoData from RAW or NetCDF, remove background noise, apply transformations, and save to NetCDF.
    """
    logger.info(f"Loading {input_path} into EchoData...")
    ed = ep.open_raw(raw_file=input_path, sonar_model=sonar_model)
    ed.to_netcdf(save_path=output_path.with_suffix(".nc"))


if __name__ == "__main__":
    main()