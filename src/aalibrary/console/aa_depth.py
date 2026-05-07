#!/usr/bin/env python3
"""
Console tool for adding a depth coordinate to an Echopype Sv NetCDF file.
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

import xarray as xr
import echopype as ep  # noqa: F401  (kept in case you re-enable .ep accessor use)
from echopype.consolidate import add_depth


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
    Usage: aa-depth [OPTIONS] [INPUT_PATH]

    Arguments:
    INPUT_PATH                 Path to the .nc / .netcdf4 file containing Sv.
                               Optional. Defaults to stdin if not provided.

    Options:
    -o, --output_path          Path to save processed output.
                               Default: same directory as input, with '_depth'
                               appended to the stem and a .nc suffix.

    --depth-offset             Offset along depth to account for transducer
                               position in water (default: 0.0).

    --tilt                     Transducer tilt angle in degrees (default: 0.0).

    --downward / --no-downward Whether transducers point downward.
                               Default: --downward (True).

    Description:
    Loads a NetCDF Sv dataset, adds a depth coordinate via
    echopype.consolidate.add_depth, and writes the result to a new
    .nc file. The output path is printed to stdout for piping.

    Example:
    aa-depth /path/to/input_Sv.nc --depth-offset 1.5 --tilt 5
    """
    print(help_text)


def main():
    # If no args and stdin has data, treat the first stdin line as the input path
    if len(sys.argv) == 1:
        if not sys.stdin.isatty():
            stdin_data = sys.stdin.readline().strip()
            if stdin_data:
                sys.argv.append(stdin_data)
            else:
                print_help()
                sys.exit(0)
        else:
            print_help()
            sys.exit(0)

    if "--help" in sys.argv or "-h" in sys.argv:
        print_help()
        sys.exit(0)

    parser = argparse.ArgumentParser(
        description="Add a depth coordinate to an Echopype Sv NetCDF file.",
        add_help=False,  # we handle help ourselves above
    )

    parser.add_argument(
        "input_path",
        type=Path,
        nargs="?",
        help="Path to the .nc or .netcdf4 file.",
    )
    parser.add_argument(
        "-o", "--output_path",
        type=Path,
        help="Path to save processed output. Default appends '_depth' to the input stem.",
    )
    parser.add_argument(
        "--depth-offset",
        type=float,
        default=0.0,
        help="Offset along depth to account for transducer position in water (default: 0.0).",
    )
    parser.add_argument(
        "--tilt",
        type=float,
        default=0.0,
        help="Transducer tilt angle in degrees (default: 0.0).",
    )
    parser.add_argument(
        "--downward",
        action="store_true",
        default=True,
        help="Transducers point downward (default: True). Use --no-downward to disable.",
    )
    parser.add_argument(
        "--no-downward",
        dest="downward",
        action="store_false",
        help=argparse.SUPPRESS,
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

    allowed_extensions = {".netcdf4": "netcdf", ".nc": "netcdf"}
    ext = args.input_path.suffix.lower()
    if ext not in allowed_extensions:
        logger.error(
            f"'{args.input_path.name}' is not a supported file type. "
            f"Allowed: {', '.join(allowed_extensions.keys())}"
        )
        sys.exit(1)

    # ---------------------------
    # Resolve output path
    # ---------------------------
    if args.output_path is None:
        args.output_path = args.input_path

    args.output_path = args.output_path.with_stem(args.output_path.stem + "_depth")
    args.output_path = args.output_path.with_suffix(".nc")

    # Guard against clobbering the input
    if args.output_path.resolve() == args.input_path.resolve():
        logger.error(
            f"Refusing to overwrite input file: {args.input_path.resolve()}"
        )
        sys.exit(1)

    # ---------------------------
    # Process file
    # ---------------------------
    try:
        pretty_args = pprint.pformat(vars(args))
        logger.debug(f"\naa-depth args:\n{pretty_args}")

        process_file(
            input_path=args.input_path,
            output_path=args.output_path,
            depth_offset=args.depth_offset,
            tilt=args.tilt,
            downward=args.downward,
        )

        logger.success(f"Desired data generated and saved to\n\t{args.output_path.resolve()}")
        logger.success("Piping saved .nc path to stdout ⟶")
        print(args.output_path.resolve())

    except Exception as e:
        logger.exception(f"Error during processing: {e}")
        sys.exit(1)


def process_file(
    input_path: Path,
    output_path: Path,
    depth_offset: float = 0.0,
    tilt: float = 0.0,
    downward: bool = True,
):
    """
    Load Sv from NetCDF, add a depth coordinate, and save back to NetCDF.
    """
    logger.info(f"Loading NetCDF file {input_path} into xarray dataset")

    # Open into memory then close the file handle so we can write to a path
    # in the same directory without xarray holding a read lock.
    with xr.open_dataset(input_path) as ds_in:
        ds_Sv = ds_in.load()

    ds_Sv = add_depth(
        ds_Sv,
        depth_offset=depth_offset,
        tilt=tilt,
        downward=downward,
    )

    ds_Sv.to_netcdf(output_path)


if __name__ == "__main__":
    main()