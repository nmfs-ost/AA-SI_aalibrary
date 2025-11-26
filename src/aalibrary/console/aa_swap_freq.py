#!/usr/bin/env python3
"""
Console tool to swap the 'channel' dimension with 'frequency_nominal' using Echopype,
so frequency becomes the primary dimension/coordinate.

Pattern matches your existing tools:
- optional stdin piping for INPUT_PATH
- argparse-wrapped single function
- human-readable docstrings & inline comments
"""

import io
from contextlib import redirect_stdout
import argparse
import sys
from pathlib import Path
import numpy as np
import xarray as xr
from loguru import logger
from echopype.consolidate import swap_dims_channel_frequency
import pprint


def print_help():
    """Standalone help text (useful when invoked with no args and no stdin)."""
    help_text = """
    Usage: aa-swap-freq [OPTIONS] [INPUT_PATH]

    Arguments:
      INPUT_PATH                   Path to a NetCDF file (.nc) with a 'channel' dimension
                                   and a 'frequency_nominal' variable/coordinate.
                                   Optional. Defaults to stdin if not provided.

    Options:
      -o, --output_path PATH       Where to write the swapped dataset (NetCDF).
                                   Default: <stem>_freqswap.nc
      --check-unique               Fail early if duplicate frequency_nominal values exist.
      --no-overwrite               Do not overwrite an existing output file.

      -h, --help                   Show this help message and exit.

    Description:
      Replaces the 'channel' dimension with the 'frequency_nominal' coordinate so that
      data are indexed by nominal transducer frequency (e.g., 18000., 38000., 120000.).
      Operation requires unique frequencies.
    """
    print(help_text)


def _add_basic_attrs(ds: xr.Dataset) -> None:
    """Replace None attrs with strings to avoid NetCDF writer issues."""
    for k, v in list(ds.attrs.items()):
        if v is None:
            ds.attrs[k] = "NA"
    for var in ds.data_vars:
        for k, v in list(ds[var].attrs.items()):
            if v is None:
                ds[var].attrs[k] = "NA"


def _assert_unique_frequencies(ds: xr.Dataset) -> None:
    """
    If requested, verify that 'frequency_nominal' exists and is unique.
    Raises SystemExit(1) with an error message if not.
    """
    if "frequency_nominal" not in ds and "frequency_nominal" not in ds.coords:
        logger.error("Dataset lacks 'frequency_nominal' variable/coord required for swapping.")
        sys.exit(1)

    # frequency_nominal could be data var or coord; access safely:
    freq = ds["frequency_nominal"]
    vals = np.asarray(freq.values).ravel()
    # Remove NaNs before uniqueness check
    vals = vals[~np.isnan(vals)]
    if len(vals) != len(np.unique(vals)):
        logger.error("Duplicate values found in 'frequency_nominal'; cannot swap dims.")
        sys.exit(1)


def main():
    """Entry point for the aa-swap-freq CLI."""
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
        description="Swap 'channel' dimension with 'frequency_nominal' so frequency becomes the primary dimension."
    )

    # ---------------------------
    # Positional/IO args
    # ---------------------------
    parser.add_argument(
        "input_path",
        type=Path,
        nargs="?",
        help="Path to a NetCDF file containing 'channel' and 'frequency_nominal'.",
    )
    parser.add_argument(
        "-o", "--output_path",
        type=Path,
        help="Output path for the swapped NetCDF (default: <stem>_freqswap.nc).",
    )
    parser.add_argument(
        "--check-unique",
        action="store_true",
        help="Fail early if duplicate frequency_nominal values exist.",
    )
    parser.add_argument(
        "--no-overwrite",
        action="store_true",
        help="Do not overwrite an existing output file.",
    )

    args = parser.parse_args()

    # ---------------------------
    # Resolve/validate input
    # ---------------------------
    if args.input_path is None:
        args.input_path = Path(sys.stdin.readline().strip())
        logger.info(f"Read input path from stdin: {args.input_path}")

    if not args.input_path.exists():
        logger.error(f"File '{args.input_path}' does not exist.")
        sys.exit(1)

    if args.output_path is None:
        args.output_path = args.input_path.with_stem(args.input_path.stem + "_freqswap").with_suffix(".nc")

    if args.output_path.exists() and args.no_overwrite:
        logger.error(f"Output file '{args.output_path}' exists and --no-overwrite was set.")
        sys.exit(1)

    try:
        # ---------------------------
        # Load dataset quietly
        # ---------------------------
        # Suppress any library chatter to stdout so pipelines remain clean.
        f = io.StringIO()
        with redirect_stdout(f):
            ds = xr.open_dataset(args.input_path)

        # Optional: early uniqueness check (the swap itself will fail if duplicates exist)
        if args.check_unique:
            _assert_unique_frequencies(ds)

        # ---------------------------
        # Swap dimensions
        # ---------------------------
        logger.info("Swapping 'channel' dimension with 'frequency_nominal' ...")
        ds_swapped = swap_dims_channel_frequency(ds)

        # Clean attributes to avoid None in NetCDF
        _add_basic_attrs(ds_swapped)

        # Save to NetCDF
        logger.info(f"Saving swapped dataset to {args.output_path} ...")
        ds_swapped.to_netcdf(args.output_path, mode="w", format="NETCDF4")

        # Pretty-print args for logs and echo the primary output path to stdout for piping
        pretty_args = pprint.pformat(vars(args))
        logger.debug(f"\naa-swap-freq args:\n{pretty_args}")
        print(args.output_path.resolve())

        logger.info("Frequency/channel dimension swap complete.")

    except Exception as e:
        logger.exception(f"Error during frequency/channel swap: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
