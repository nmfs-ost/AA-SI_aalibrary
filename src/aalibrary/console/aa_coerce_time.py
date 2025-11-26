#!/usr/bin/env python3
"""
Console tool to coerce a time coordinate so it always increases, using
Echopype’s qc function `coerce_increasing_time`.

Pattern matches your existing tools:
- optional stdin piping for INPUT_PATH
- argparse-wrapped simple function
- human-readable docstrings & inline comments

This wraps:
  echopype.qc.coerce_increasing_time(ds, time_name='ping_time', win_len=100)

Notes:
- The function **edits the Dataset in place** to fix any local time reversals.
- We optionally report whether time reversals existed before and after the fix.
"""

import io
from contextlib import redirect_stdout
import argparse
import sys
from pathlib import Path
import xarray as xr
from loguru import logger
from echopype.qc import coerce_increasing_time, exist_reversed_time
import pprint


def print_help():
    """Standalone help text (useful when invoked with no args and no stdin)."""
    help_text = """
    Usage: aa-coerce-time [OPTIONS] [INPUT_PATH]

    Arguments:
      INPUT_PATH                   Path to a NetCDF file (.nc) whose time coordinate
                                   may contain local reversals. Optional; defaults to
                                   reading a single token from stdin.

    Options:
      -o, --output_path PATH       Output NetCDF path (default: <stem>_timefix.nc).
      --time-name STR              Name of the time coordinate to coerce (default: ping_time).
      --win-len INT                Local window length used to infer the next ping time
                                   when a reversal is detected (default: 100).
      --report                     Print a short report on time reversals before/after.
      --no-overwrite               Do not overwrite an existing output file.
      -h, --help                   Show this help message and exit.

    Description:
      Detects and fixes local backward jumps in a datetime coordinate by enforcing
      a monotonically increasing series (forward-only time).

    Example:
      aa-coerce-time pingdata.nc --time-name ping_time --win-len 120 --report -o pingdata_timefix.nc
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
    """Entry point for the aa-coerce-time CLI."""
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
        description="Coerce a time coordinate to be strictly increasing."
    )

    # ---------------------------
    # Positional / IO args
    # ---------------------------
    parser.add_argument(
        "input_path",
        type=Path,
        nargs="?",
        help="Path to a NetCDF file containing the time coordinate to fix.",
    )
    parser.add_argument(
        "-o", "--output_path",
        type=Path,
        help="Output NetCDF path (default: <stem>_timefix.nc).",
    )

    # ---------------------------
    # coerce_increasing_time params
    # ---------------------------
    parser.add_argument("--time-name", dest="time_name", default="ping_time",
                        help="Name of the time coordinate to coerce (default: ping_time).")
    parser.add_argument("--win-len", dest="win_len", type=int, default=100,
                        help="Local window length for inferring next ping time (default: 100).")

    # Behavior flags
    parser.add_argument("--report", action="store_true",
                        help="Print whether time reversals exist before/after.")
    parser.add_argument("--no-overwrite", action="store_true",
                        help="Do not overwrite an existing output file.")

    args = parser.parse_args()

    # ---------------------------
    # Resolve / validate input
    # ---------------------------
    if args.input_path is None:
        args.input_path = Path(sys.stdin.readline().strip())
        logger.info(f"Read input path from stdin: {args.input_path}")

    if not args.input_path.exists():
        logger.error(f"File '{args.input_path}' does not exist.")
        sys.exit(1)

    if args.output_path is None:
        args.output_path = args.input_path.with_stem(args.input_path.stem + "_timefix").with_suffix(".nc")

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

        # Optional pre-check report
        had_reversal = None
        if args.report:
            try:
                had_reversal = exist_reversed_time(ds, args.time_name)
                logger.info(f"Time reversal present before fix? {had_reversal}")
            except Exception as e:
                logger.warning(f"Could not check for reversed time before fix: {e}")

        # ---------------------------
        # Coerce time to increase
        # ---------------------------
        logger.info(f"Coercing '{args.time_name}' to increasing with win_len={args.win_len} ...")
        # Function edits ds in place; return value may be None.
        coerce_increasing_time(ds=ds, time_name=args.time_name, win_len=args.win_len)

        # Optional post-check report
        if args.report:
            try:
                has_reversal_after = exist_reversed_time(ds, args.time_name)
                logger.info(f"Time reversal present after fix?  {has_reversal_after}")
            except Exception as e:
                logger.warning(f"Could not check for reversed time after fix: {e}")

        # Clean attributes to avoid None in NetCDF
        _add_basic_attrs(ds)

        # Save to NetCDF
        logger.info(f"Saving coerced dataset to {args.output_path} ...")
        ds.to_netcdf(args.output_path, mode="w", format="NETCDF4")

        # Pretty-print args for logs and echo the primary output to stdout for piping
        pretty_args = pprint.pformat(vars(args))
        logger.debug(f"\naa-coerce-time args:\n{pretty_args}")
        print(args.output_path.resolve())

        logger.info("Time coercion complete.")

    except Exception as e:
        logger.exception(f"Error during time coercion: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
