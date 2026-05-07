#!/usr/bin/env python3
"""
aa-mvbs-index

Console tool for computing MVBS (Mean Volume Backscattering Strength)
using *index binning* with Echopype, from a calibrated Sv NetCDF file.

This wraps:
    echopype.commongrid.compute_MVBS_index_binning(
        ds_Sv, range_sample_num=<int>, ping_num=<int>
    )

Pipeline-friendly: reads input path from positional arg or stdin, writes
output path to stdout, all logs to stderr.

Typical pipeline usage:
    aa-nc --sonar_model EK60 input.raw | aa-sv | aa-mvbs-index
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
# Without this, any exception in process_file disappears silently and
# the pipeline downstream gets no input — a confusing failure mode.
logger.add(sys.stderr, level="WARNING")

# Now the heavy imports — anything they log gets squashed
import argparse
import pprint
from pathlib import Path

import echopype as ep
import xarray as xr
from echopype.commongrid import compute_MVBS_index_binning


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
    Usage: aa-mvbs-index [OPTIONS] [INPUT_PATH]

    Arguments:
    INPUT_PATH                  Path to the calibrated Sv .nc / .netcdf4
                                file, or a converted Echopype file that
                                can be calibrated to Sv.
                                Optional. Defaults to stdin if not provided.

    Options:
    -o, --output_path           Path to save the MVBS dataset.
                                Default: same directory as input, with
                                '_mvbs_index' appended to the stem and a
                                .nc suffix.

    --range-sample-num INT      Number of samples per bin along
                                'range_sample'. Default: 100
    --ping-num INT              Number of pings per bin along the ping
                                axis. Default: 100

    Description:
    Computes MVBS by binning along the index-based axes (range_sample
    and ping number). This is distinct from physical-unit binning
    (meters/seconds), which is what compute_MVBS uses. The output path
    is printed to stdout for piping into the next stage of the pipeline.

    Example:
        aa-mvbs-index /path/to/input_Sv.nc --range-sample-num 30 \\
                      --ping-num 5 -o /path/to/mvbs.nc
    """
    print(help_text)


def main():
    # Stdin / no-args handling
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
        description="Compute MVBS via index binning from a calibrated Sv NetCDF.",
        add_help=False,
    )

    parser.add_argument(
        "input_path",
        type=Path,
        nargs="?",
        help="Path to the .nc / .netcdf4 file containing Sv.",
    )
    parser.add_argument(
        "-o", "--output_path",
        type=Path,
        help="Path to save processed output. Default appends '_mvbs_index' to the input stem.",
    )
    parser.add_argument(
        "--range-sample-num",
        dest="range_sample_num",
        type=int,
        default=100,
        help="Number of samples per bin along range_sample (default: 100).",
    )
    parser.add_argument(
        "--ping-num",
        dest="ping_num",
        type=int,
        default=100,
        help="Number of pings per bin (default: 100).",
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

    allowed_extensions = {".netcdf4", ".nc"}
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
        args.output_path = args.input_path.with_stem(
            args.input_path.stem + "_mvbs_index"
        ).with_suffix(".nc")
    else:
        args.output_path = args.output_path.with_suffix(".nc")

    # Guard against clobbering the input
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
            "range_sample_num": args.range_sample_num,
            "ping_num": args.ping_num,
        }
        logger.debug(
            f"Executing aa-mvbs-index configured with [OPTIONS]:\n"
            f"{pprint.pformat(args_summary)}"
        )

        process_file(
            input_path=args.input_path,
            output_path=args.output_path,
            range_sample_num=args.range_sample_num,
            ping_num=args.ping_num,
        )

        logger.success(
            f"Generated {args.output_path.resolve()} with aa-mvbs-index. "
            "Passing .nc path to stdout..."
        )
        # Pipe the output path to stdout for the next tool
        print(args.output_path.resolve())

    except Exception as e:
        logger.exception(f"Error during processing: {e}")
        sys.exit(1)


def clean_attrs(ds):
    """Replace None-valued attrs with 'NA' so the dataset is NetCDF-safe.
    NetCDF attrs cannot be None — to_netcdf will raise on serialization."""
    for k, v in ds.attrs.items():
        if v is None:
            ds.attrs[k] = "NA"
    for var in ds.data_vars:
        for k, v in ds[var].attrs.items():
            if v is None:
                ds[var].attrs[k] = "NA"
    return ds


def process_file(
    input_path: Path,
    output_path: Path,
    range_sample_num: int = 100,
    ping_num: int = 100,
):
    """Load Sv from NetCDF, compute MVBS via index binning, and save."""

    logger.info(f"Loading dataset from {input_path}")
    ds = xr.open_dataset(input_path)

    # If 'Sv' isn't present, fall back to calibrating from a converted
    # Echopype file. Mirrors aa-sv's contract so this tool can sit
    # anywhere downstream of aa-nc in a pipeline — e.g. directly after
    # aa-nc when the user wants a quick MVBS without a separate aa-sv step.
    if "Sv" not in ds.data_vars:
        logger.info("No 'Sv' variable found; calibrating to Sv via Echopype")
        ed = ep.open_converted(input_path)
        ds = ep.calibrate.compute_Sv(ed)

    logger.info(
        f"Computing MVBS with index binning "
        f"(range_sample_num={range_sample_num}, ping_num={ping_num})"
    )
    ds_mvbs = compute_MVBS_index_binning(
        ds_Sv=ds,
        range_sample_num=range_sample_num,
        ping_num=ping_num,
    )
    ds_mvbs = clean_attrs(ds_mvbs)

    output_path = output_path.with_suffix(".nc")
    logger.info(f"Saving MVBS (index binning) dataset to {output_path}")
    ds_mvbs.to_netcdf(output_path, mode="w", format="NETCDF4")
    logger.success(f"MVBS computation complete: {output_path.resolve()}")


if __name__ == "__main__":
    main()