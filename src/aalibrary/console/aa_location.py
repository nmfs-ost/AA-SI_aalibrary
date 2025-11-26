#!/usr/bin/env python3
"""
Console tool for adding geographical location (latitude/longitude) to an Sv dataset
using Echopype’s consolidate.add_location.

Pattern matches your existing tools:
- optional stdin piping for INPUT_PATH
- argparse-wrapped single function
- human-readable docstrings & inline comments

This wraps:
  echopype.consolidate.add_location(
      ds, echodata, datagram_type=None, nmea_sentence=None
  )

It interpolates platform location (lat/lon) from the original data file’s
Platform/NMEA records to the acoustic ping_time of the Sv dataset.
"""

import io
from contextlib import redirect_stdout
import argparse
import sys
from pathlib import Path
import xarray as xr
from loguru import logger
import echopype as ep  # ensure echopype is installed
from echopype.consolidate import add_location
import pprint


def print_help():
    """Standalone help text when invoked with no args and no stdin."""
    help_text = """
    Usage: aa-location [OPTIONS] [INPUT_PATH]

    Arguments:
      INPUT_PATH                   Path to an Sv NetCDF (.nc), or another Dataset
                                   that has ping_time and can accept location.
                                   Optional. Defaults to stdin if not provided.

    Options:
      -o, --output_path PATH       Where to write the output NetCDF with lat/lon.
                                   Default: <stem>_loc.nc
      --echodata PATH              Path to an EchoData source (raw/converted file or
                                   Zarr/NetCDF) that contains Platform/NMEA groups
                                   for interpolation. (Required if INPUT lacks these.)
      --datagram-type STR          (Optional) Instrument/datagram type hint used by
                                   add_location to select nav source.
      --nmea-sentence STR          (Optional) Specific NMEA sentence to use (e.g. 'GGA').

      -h, --help                   Show this help message and exit.

    Description:
      Interpolates geographic location (latitude, longitude) from the platform
      navigation stream in the original file to the acoustic ping_time of the
      Sv dataset, and writes the result to NetCDF.

    Examples:
      aa-location sv.nc --echodata rawfile.raw
      aa-location sv.nc --echodata cruise.zarr --nmea-sentence GGA -o sv_loc.nc
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
    """Entry point for the aa-location CLI."""
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
        description="Add geographic location (lat/lon) to an Sv dataset using Echopype."
    )

    # ---------------------------
    # Positional/IO args
    # ---------------------------
    parser.add_argument(
        "input_path",
        type=Path,
        nargs="?",
        help="Path to an Sv NetCDF (.nc) or compatible Dataset file.",
    )
    parser.add_argument(
        "-o", "--output_path",
        type=Path,
        help="Output NetCDF path (default: <stem>_loc.nc).",
    )

    # ---------------------------
    # add_location parameters
    # ---------------------------
    parser.add_argument(
        "--echodata",
        type=Path,
        help="Path to EchoData source (raw/converted NetCDF/Zarr) containing Platform/NMEA.",
    )
    parser.add_argument(
        "--datagram-type",
        dest="datagram_type",
        help="Optional datagram type hint for selecting nav records.",
    )
    parser.add_argument(
        "--nmea-sentence",
        dest="nmea_sentence",
        help="Optional NMEA sentence (e.g., 'GGA').",
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
        args.output_path = args.input_path.with_stem(args.input_path.stem + "_loc").with_suffix(".nc")

    # EchoData source may be optional if INPUT already contains necessary Platform info,
    # but in general you’ll provide --echodata to be explicit.
    if args.echodata is not None and not args.echodata.exists():
        logger.error(f"EchoData source '{args.echodata}' does not exist.")
        sys.exit(1)

    try:
        # ---------------------------
        # Load dataset quietly
        # ---------------------------
        f = io.StringIO()
        with redirect_stdout(f):
            ds = xr.open_dataset(args.input_path)

        # ---------------------------
        # Prepare echodata argument
        # ---------------------------
        # add_location accepts either an EchoData object or a path. We pass the path
        # directly if provided; otherwise we try to infer by reusing the input path.
        echodata_arg = args.echodata if args.echodata is not None else args.input_path

        # ---------------------------
        # Add location
        # ---------------------------
        logger.info("Adding geographic location (lat/lon) to Sv dataset ...")
        ds_with_loc = add_location(
            ds=ds,
            echodata=echodata_arg,
            datagram_type=args.datagram_type,
            nmea_sentence=args.nmea_sentence,
        )

        # Clean attributes to avoid None in NetCDF
        _add_basic_attrs(ds_with_loc)

        # Save to NetCDF
        logger.info(f"Saving dataset with location to {args.output_path} ...")
        ds_with_loc.to_netcdf(args.output_path, mode="w", format="NETCDF4")

        # Pretty-print args for logs and echo the primary output path to stdout for piping
        pretty_args = pprint.pformat(vars(args))
        logger.debug(f"\naa-location args:\n{pretty_args}")
        print(args.output_path.resolve())

        logger.info("Location interpolation complete.")

    except Exception as e:
        logger.exception(f"Error during add_location: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
