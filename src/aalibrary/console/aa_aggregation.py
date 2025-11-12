#!/usr/bin/env python3
"""
Console tool to compute Echopype metrics.aggregation for backscatter.

Pattern matches your suite:
- optional stdin piping for INPUT_PATH
- argparse-wrapped single function
- clear, human-readable comments

Wraps:
  echopype.metrics.aggregation(ds: xarray.Dataset, range_label: str = "echo_range") -> xarray.DataArray

Notes:
- `aggregation` expects a calibrated Dataset with an `echo_range` (or equivalent).
- If missing, consider generating a file that contains `echo_range` (e.g., from compute_Sv).
"""

import io
from contextlib import redirect_stdout
import argparse
import sys
from pathlib import Path
import xarray as xr
from loguru import logger
from echopype.metrics import aggregation
import pprint


def print_help():
    """Standalone help text (handy when invoked with no args and no stdin)."""
    help_text = """
    Usage: aa-aggregation [OPTIONS] [INPUT_PATH]

    Arguments:
      INPUT_PATH                   Path to a NetCDF file (.nc) containing a calibrated
                                   Dataset with 'echo_range'. Optional; defaults to
                                   reading one token from stdin.

    Options:
      -o, --output_path PATH       Output NetCDF path (default: <stem>_aggregation.nc).
      --range-label STR            Name of the DataArray holding range (default: echo_range).
      --no-overwrite               Do not overwrite an existing output file.
      --quiet                      Print only the output path (or suppress extras).
      -h, --help                   Show this help message and exit.

    Description:
      Computes the Echopype aggregation metric of backscatter along the range axis.
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


def main():
    """Entry point for the aa-aggregation CLI."""
    # If no argv, try to read a token path from stdin; else print help and exit.
    if len(sys.argv) == 1:
        if not sys.stdin.isatty():
            token = sys.stdin.readline().strip()
            if token:
                sys.argv.append(token)
        else:
            print_help()
            sys.exit(0)

    parser = argparse.ArgumentParser(
        description="Compute Echopype metrics.aggregation."
    )

    # IO args
    parser.add_argument(
        "input_path",
        type=Path,
        nargs="?",
        help="Path to a NetCDF Dataset containing 'echo_range' (typically from calibrated Sv).",
    )
    parser.add_argument(
        "-o", "--output_path",
        type=Path,
        help="Output NetCDF path (default: <stem>_aggregation.nc).",
    )

    # aggregation parameters
    parser.add_argument("--range-label", dest="range_label", default="echo_range",
                        help="Name of the range DataArray (default: echo_range).")

    # behavior flags
    parser.add_argument("--no-overwrite", action="store_true",
                        help="Do not overwrite an existing output file.")
    parser.add_argument("--quiet", action="store_true",
                        help="Reduce logs; print only final path.")

    args = parser.parse_args()

    # Resolve / validate
    if args.input_path is None:
        args.input_path = Path(sys.stdin.readline().strip())
        if not args.quiet:
            logger.info(f"Read input path from stdin: {args.input_path}")

    if not args.input_path.exists():
        logger.error(f"File '{args.input_path}' does not exist.")
        sys.exit(1)

    if args.output_path is None:
        args.output_path = args.input_path.with_stem(args.input_path.stem + "_aggregation").with_suffix(".nc")

    if args.output_path.exists() and args.no_overwrite:
        logger.error(f"Output file '{args.output_path}' exists and --no-overwrite was set.")
        sys.exit(1)

    try:
        # Load quietly to keep stdout clean for piping
        f = io.StringIO()
        with redirect_stdout(f):
            ds = xr.open_dataset(args.input_path)

        # Ensure we have a range variable
        have_range = args.range_label in ds.variables or args.range_label in ds.coords
        if not have_range:
            logger.error(
                f"Required range label '{args.range_label}' not found in Dataset."
            )
            sys.exit(1)

        # Compute aggregation
        if not args.quiet:
            logger.info("Computing aggregation metric...")
        da_aggr = aggregation(ds=ds, range_label=args.range_label)

        # Package into a Dataset for output
        out_ds = da_aggr.to_dataset(name="aggregation")
        out_ds["aggregation"].attrs.setdefault("long_name", "Aggregation metric")
        out_ds.attrs.setdefault("source_tool", "aa-aggregation")
        out_ds.attrs.setdefault("range_label", args.range_label)

        _add_basic_attrs(out_ds)

        # Save
        if not args.quiet:
            logger.info(f"Saving aggregation to {args.output_path} ...")
        out_ds.to_netcdf(args.output_path, mode="w", format="NETCDF4")

        # Log/debug + print path for piping
        if not args.quiet:
            logger.debug(f"\naa-aggregation args:\n{pprint.pformat(vars(args))}")
        print(args.output_path.resolve())

        if not args.quiet:
            logger.info("Aggregation computation complete.")

    except Exception as e:
        logger.exception(f"Error during aggregation computation: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
