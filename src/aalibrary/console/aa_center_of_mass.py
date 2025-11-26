#!/usr/bin/env python3
"""
Console tool to compute Echopype metrics.center_of_mass (COM) for backscatter.

Pattern matches your suite:
- optional stdin piping for INPUT_PATH
- argparse-wrapped single function
- clear, human-readable comments

Wraps:
  echopype.metrics.center_of_mass(ds: xarray.Dataset, range_label: str = "echo_range") -> xarray.DataArray

Notes:
- `center_of_mass` expects a calibrated Dataset with an `echo_range` (or equivalent).
- If missing, you can try `--try-calibrate` to open as converted EchoData and compute Sv.
"""

import io
from contextlib import redirect_stdout
import argparse
import sys
from pathlib import Path
import xarray as xr
from loguru import logger
import echopype as ep  # only used if --try-calibrate to compute Sv (for echo_range)
from echopype.metrics import center_of_mass
import pprint


def print_help():
    """Standalone help text (handy when invoked with no args and no stdin)."""
    help_text = """
    Usage: aa-center-of-mass [OPTIONS] [INPUT_PATH]

    Arguments:
      INPUT_PATH                   Path to a NetCDF file (.nc) containing a calibrated
                                   Dataset with 'echo_range'. Optional; defaults to
                                   reading one token from stdin.

    Options:
      -o, --output_path PATH       Output NetCDF path (default: <stem>_com.nc).
      --range-label STR            Name of the DataArray holding range (default: echo_range).
      --try-calibrate              If 'echo_range' is missing, try to open as converted
                                   EchoData and compute Sv to obtain it.
      --no-overwrite               Do not overwrite an existing output file.
      --quiet                      Print only the output path (or suppress extras).
      -h, --help                   Show this help message and exit.

    Description:
      Computes the center of mass (depth-weighted mean) of backscatter along range.
      Units: meters (same units as the provided range axis).
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
    """Entry point for the aa-center-of-mass CLI."""
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
        description="Compute Echopype metrics.center_of_mass (COM)."
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
        help="Output NetCDF path (default: <stem>_com.nc).",
    )

    # center_of_mass parameters
    parser.add_argument("--range-label", dest="range_label", default="echo_range",
                        help="Name of the range DataArray (default: echo_range).")

    # behavior flags
    parser.add_argument("--try-calibrate", action="store_true",
                        help="If 'echo_range' missing, attempt to compute Sv to obtain it.")
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
        args.output_path = args.input_path.with_stem(args.input_path.stem + "_com").with_suffix(".nc")

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

        if not have_range and args.try_calibrate:
            if not args.quiet:
                logger.info(f"'{args.range_label}' not found; attempting to compute Sv to obtain it...")
            # Try opening as converted and computing Sv; this typically adds 'echo_range'
            ed = ep.open_converted(args.input_path)
            ds = ep.calibrate.compute_Sv(ed)
            have_range = args.range_label in ds.variables or args.range_label in ds.coords

        if not have_range:
            logger.error(
                f"Required range label '{args.range_label}' not found in Dataset. "
                f"Consider using --try-calibrate if the file is an Echopype-converted product."
            )
            sys.exit(1)

        # Compute center of mass (COM)
        if not args.quiet:
            logger.info("Computing center of mass (COM)...")
        da_com = center_of_mass(ds=ds, range_label=args.range_label)

        # Package into a Dataset for output
        out_ds = da_com.to_dataset(name="center_of_mass")
        out_ds["center_of_mass"].attrs.setdefault("long_name", "Center of Mass of Backscatter")
        out_ds["center_of_mass"].attrs.setdefault("units", "m")
        out_ds.attrs.setdefault("source_tool", "aa-center-of-mass")
        out_ds.attrs.setdefault("range_label", args.range_label)

        _add_basic_attrs(out_ds)

        # Save
        if not args.quiet:
            logger.info(f"Saving center of mass to {args.output_path} ...")
        out_ds.to_netcdf(args.output_path, mode="w", format="NETCDF4")

        # Log/debug + print path for piping
        if not args.quiet:
            logger.debug(f"\naa-center-of-mass args:\n{pprint.pformat(vars(args))}")
        print(args.output_path.resolve())

        if not args.quiet:
            logger.info("Center of mass computation complete.")

    except Exception as e:
        logger.exception(f"Error during center of mass computation: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
