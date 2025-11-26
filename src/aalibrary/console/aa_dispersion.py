#!/usr/bin/env python3
"""
Console tool to compute dispersion (inertia) of backscatter using Echopype.

Wraps:
  echopype.metrics.dispersion(ds: xarray.Dataset, range_label='echo_range')
which returns a DataArray representing inertia (unit m^-2). :contentReference[oaicite:1]{index=1}
"""

import io
from contextlib import redirect_stdout
import argparse
import sys
from pathlib import Path
import xarray as xr
from loguru import logger
from echopype.metrics import dispersion
import pprint


def print_help():
    help_text = """
    Usage: aa-dispersion [OPTIONS] [INPUT_PATH]

    Arguments:
      INPUT_PATH                  Path to a NetCDF file (.nc) containing a calibrated
                                  Dataset with an `echo_range` (or similar) coordinate.
                                  Optional; defaults to stdin if not provided.

    Options:
      -o, --output_path          Path to write the resulting dispersion (NetCDF).
                                  Default: <stem>_dispersion.nc
      --range-label STR          Name of the range variable/coordinate (default: echo_range).
      --no-overwrite             Do not overwrite an existing output file.
      --quiet                    Print only the output path (suppress logs).

    Description:
      Computes the inertia of the backscatter distribution (i.e., dispersion/spread)
      using Echopype’s metrics.dispersion. The returned quantity has units m⁻².
    """
    print(help_text)


def _add_basic_attrs(ds: xr.Dataset) -> None:
    """Replace None attrs with 'NA' to avoid NetCDF writer issues."""
    for k, v in list(ds.attrs.items()):
        if v is None:
            ds.attrs[k] = "NA"
    for var in ds.data_vars:
        for kk, vv in list(ds[var].attrs.items()):
            if vv is None:
                ds[var].attrs[kk] = "NA"


def main():
    if len(sys.argv) == 1:
        if not sys.stdin.isatty():
            token = sys.stdin.readline().strip()
            if token:
                sys.argv.append(token)
        else:
            print_help()
            sys.exit(0)

    parser = argparse.ArgumentParser(
        description="Compute dispersion (inertia) of backscatter using Echopype."
    )
    parser.add_argument(
        "input_path",
        type=Path,
        nargs="?",
        help="Path to a NetCDF file (.nc) dataset with Sv and echo_range."
    )
    parser.add_argument(
        "-o", "--output_path",
        type=Path,
        help="Output NetCDF path (default: <stem>_dispersion.nc)."
    )
    parser.add_argument(
        "--range-label", dest="range_label", default="echo_range",
        help="Name of the range coordinate/variable (default: echo_range)."
    )
    parser.add_argument(
        "--no-overwrite", action="store_true",
        help="Do not overwrite an existing output file."
    )
    parser.add_argument(
        "--quiet", action="store_true",
        help="Suppress logs; print only output path."
    )

    args = parser.parse_args()

    if args.input_path is None:
        args.input_path = Path(sys.stdin.readline().strip())
        if not args.quiet:
            logger.info(f"Read input path from stdin: {args.input_path}")

    if not args.input_path.exists():
        logger.error(f"File '{args.input_path}' does not exist.")
        sys.exit(1)

    if args.output_path is None:
        args.output_path = args.input_path.with_stem(args.input_path.stem + "_dispersion").with_suffix(".nc")

    if args.output_path.exists() and args.no-overwrite:
        logger.error(f"Output file '{args.output_path}' exists and --no-overwrite was set.")
        sys.exit(1)

    try:
        # load dataset quietly
        f = io.StringIO()
        with redirect_stdout(f):
            ds = xr.open_dataset(args.input_path)

        # compute dispersion
        if not args.quiet:
            logger.info("Computing dispersion (inertia)...")
        da_disp = dispersion(ds=ds, range_label=args.range_label)

        # wrap into Dataset
        ds_out = da_disp.to_dataset(name="dispersion")
        ds_out["dispersion"].attrs.setdefault("long_name", "Dispersion (inertia)")
        ds_out["dispersion"].attrs.setdefault("units", "m^-2")
        ds_out.attrs.setdefault("source_tool", "aa-dispersion")
        ds_out.attrs.setdefault("range_label", args.range_label)

        _add_basic_attrs(ds_out)

        if not args.quiet:
            logger.info(f"Saving dispersion to {args.output_path} ...")
        ds_out.to_netcdf(args.output_path, mode="w", format="NETCDF4")

        if not args.quiet:
            logger.debug(f"\naa-dispersion args:\n{pprint.pformat(vars(args))}")
        print(args.output_path.resolve())

        if not args.quiet:
            logger.info("Dispersion computation complete.")

    except Exception as e:
        logger.exception(f"Error during dispersion computation: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
