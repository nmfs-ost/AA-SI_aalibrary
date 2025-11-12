#!/usr/bin/env python3
"""
Console tool for computing MVBS using *index binning* with Echopype.

This wraps:
  echopype.commongrid.compute_MVBS_index_binning(ds_Sv,
      range_sample_num=<int>, ping_num=<int>)

Pattern matches your existing tools:
- optional stdin piping for INPUT_PATH
- argparse-wrapped single function
- clear docstrings & human-readable inline comments
"""

import io
from contextlib import redirect_stdout
import argparse
import sys
from pathlib import Path
import xarray as xr
from loguru import logger
import echopype as ep  # ensure echopype is installed
from echopype.commongrid import compute_MVBS_index_binning
import pprint


def print_help():
    """Standalone help text (useful when invoked with no args and no stdin)."""
    help_text = """
    Usage: aa-mvbs-index [OPTIONS] [INPUT_PATH]

    Arguments:
      INPUT_PATH                   Path to the calibrated Sv NetCDF (.nc),
                                   or a converted Echopype file that can be calibrated.
                                   Optional. Defaults to stdin if not provided.

    Options:
      -o, --output_path PATH       Where to write the MVBS dataset (NetCDF).
                                   Default: <stem>_mvbs_index.nc

      --range-sample-num INT       Number of samples along 'range_sample' per bin.
                                   Default: 100
      --ping-num INT               Number of pings per bin along ping axis.
                                   Default: 100

      -h, --help                   Show this help message and exit.

    Description:
      Computes Mean Volume Backscattering Strength (MVBS) by binning along
      the index-based axes (range_sample and ping number). This differs from
      physical-unit binning (meters/seconds) done by compute_MVBS.

    Examples:
      aa-mvbs-index data.nc --range-sample-num 30 --ping-num 5
      aa-mvbs-index data.nc -o mvbs_idx.nc
    """
    print(help_text)


def _add_basic_attrs(ds: xr.Dataset) -> None:
    """Replace None attrs with strings to avoid NetCDF writer issues."""
    # Dataset-level attrs
    for k, v in list(ds.attrs.items()):
        if v is None:
            ds.attrs[k] = "NA"
    # Variable-level attrs
    for var in ds.data_vars:
        for k, v in list(ds[var].attrs.items()):
            if v is None:
                ds[var].attrs[k] = "NA"


def main():
    """Entry point for the aa-mvbs-index CLI."""
    # If no argv, try to read an INPUT_PATH token from stdin; otherwise print help and exit.
    if len(sys.argv) == 1:
        if not sys.stdin.isatty():
            stdin_data = sys.stdin.readline().strip()
            if stdin_data:
                sys.argv.append(stdin_data)
        else:
            print_help()
            sys.exit(0)

    parser = argparse.ArgumentParser(
        description="Compute MVBS using index binning (range_sample, ping_num) from calibrated Sv."
    )

    # ---------------------------
    # Positional/IO args
    # ---------------------------
    parser.add_argument(
        "input_path",
        type=Path,
        nargs="?",
        help="Path to a NetCDF file containing Sv (preferred) or a converted file that can be calibrated to Sv.",
    )
    parser.add_argument(
        "-o", "--output_path",
        type=Path,
        help="Output path for the MVBS NetCDF (default: <stem>_mvbs_index.nc).",
    )

    # ---------------------------
    # compute_MVBS_index_binning parameters
    # ---------------------------
    parser.add_argument("--range-sample-num", dest="range_sample_num", type=int, default=100,
                        help="Number of samples per bin along range_sample (default: 100).")
    parser.add_argument("--ping-num", dest="ping_num", type=int, default=100,
                        help="Number of pings per bin (default: 100).")

    args = parser.parse_args()

    # ---------------------------
    # Resolve/validate input
    # ---------------------------
    if args.input_path is None:
        # If not provided on CLI, try to read a path token from stdin (same behavior as your other tools).
        args.input_path = Path(sys.stdin.readline().strip())
        logger.info(f"Read input path from stdin: {args.input_path}")

    if not args.input_path.exists():
        logger.error(f"File '{args.input_path}' does not exist.")
        sys.exit(1)

    # Default output path if not provided
    if args.output_path is None:
        args.output_path = args.input_path.with_stem(args.input_path.stem + "_mvbs_index").with_suffix(".nc")

    try:
        # ---------------------------
        # Load dataset quietly
        # ---------------------------
        # Suppress any library chatter to stdout so pipelines remain clean.
        f = io.StringIO()
        with redirect_stdout(f):
            ds = xr.open_dataset(args.input_path)

        # ---------------------------
        # Ensure we have calibrated Sv
        # ---------------------------
        # If 'Sv' isn't present, try to calibrate from a converted Echopype file.
        if "Sv" not in ds.data_vars:
            logger.info("No 'Sv' variable found; attempting to calibrate to Sv via Echopype...")
            ed = ep.open_converted(args.input_path)
            ds = ep.calibrate.compute_Sv(ed)

        # ---------------------------
        # Compute MVBS (index binning)
        # ---------------------------
        logger.info("Computing MVBS with index binning...")
        ds_mvbs = compute_MVBS_index_binning(
            ds_Sv=ds,
            range_sample_num=args.range_sample_num,
            ping_num=args.ping_num,
        )

        # Clean attributes to avoid None in NetCDF
        _add_basic_attrs(ds_mvbs)

        # Save to NetCDF
        logger.info(f"Saving MVBS (index binning) to {args.output_path} ...")
        ds_mvbs.to_netcdf(args.output_path, mode="w", format="NETCDF4")

        # Pretty-print args for logs and echo the primary output path to stdout for piping
        pretty_args = pprint.pformat(vars(args))
        logger.debug(f"\naa-mvbs-index args:\n{pretty_args}")
        print(args.output_path.resolve())

        logger.info("MVBS (index binning) computation complete.")

    except Exception as e:
        logger.exception(f"Error during MVBS (index binning) computation: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
