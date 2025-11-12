#!/usr/bin/env python3
"""
Console tool for estimating background noise from calibrated Sv using Echopype.

Pattern matches your existing tools:
- optional stdin piping for INPUT_PATH
- argparse-wrapped simple function
- human-readable docstrings & inline comments

This wraps `echopype.clean.estimate_background_noise(ds_Sv, ping_num, range_sample_num, background_noise_max=None)`
and writes the resulting noise estimate (DataArray) to NetCDF as a Dataset named "Sv_noise".
"""

import io
from contextlib import redirect_stdout
import argparse
import sys
from pathlib import Path
import xarray as xr
from loguru import logger
import echopype as ep  # ensure echopype is installed
from echopype.clean import estimate_background_noise
import pprint


def print_help():
    """Standalone help text (helpful when invoked with no args and no stdin)."""
    help_text = """
    Usage: aa-noise-est [OPTIONS] [INPUT_PATH]

    Arguments:
      INPUT_PATH                   Path to the calibrated .nc (NetCDF) file
                                   containing Sv (preferred), or a converted
                                   Echopype file that can be calibrated to Sv.
                                   Optional. Defaults to stdin if not provided.

    Options:
      -o, --output_path PATH       Where to write the background-noise estimate (NetCDF).
                                   Default: <stem>_noise.nc

      --ping-num INT               Number of pings used to obtain noise estimates.
                                   Default: 20
      --range-sample-num INT       Number of samples along the range axis for each estimate.
                                   Default: 20
      --background-noise-max STR   Upper limit for background noise (dB), e.g. '−125.0dB'.
                                   Default: None

      -h, --help                   Show this help message and exit.

    Description:
      Estimates background noise by computing mean calibrated power from
      windows of pings and range samples. Writes a NetCDF containing a single
      variable "Sv_noise".

    Examples:
      aa-noise-est data.nc --ping-num 50 --range-sample-num 200 --background-noise-max -120.0dB
      aa-noise-est data.nc -o cruise01_legA_noise.nc
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
    """Entry point for the aa-noise-est CLI."""
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
        description="Estimate background noise (Sv_noise) from Sv using Echopype."
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
        help="Output path for the noise NetCDF (default: <stem>_noise.nc).",
    )

    # ---------------------------
    # estimate_background_noise parameters
    # ---------------------------
    parser.add_argument(
        "--ping-num",
        dest="ping_num",
        type=int,
        default=20,
        help="Number of pings to obtain noise estimates (default: 20).",
    )
    parser.add_argument(
        "--range-sample-num",
        dest="range_sample_num",
        type=int,
        default=20,
        help="Number of range samples per estimate window (default: 20).",
    )
    parser.add_argument(
        "--background-noise-max",
        dest="background_noise_max",
        default=None,
        help="Upper limit for background noise in dB, e.g. '-120.0dB' (default: None).",
    )

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
        args.output_path = args.input_path.with_stem(args.input_path.stem + "_noise").with_suffix(".nc")

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
        if "Sv" not in ds.data_vars:
            logger.info("No 'Sv' variable found; attempting to calibrate to Sv via Echopype...")
            ed = ep.open_converted(args.input_path)
            ds = ep.calibrate.compute_Sv(ed)

        # ---------------------------
        # Estimate background noise (returns DataArray)
        # ---------------------------
        logger.info("Estimating background noise (Sv_noise)...")
        da_noise = estimate_background_noise(
            ds_Sv=ds,
            ping_num=args.ping_num,
            range_sample_num=args.range_sample_num,
            background_noise_max=args.background_noise_max,
        )

        # Wrap DataArray into a Dataset for clearer NetCDF structure and naming
        ds_out = da_noise.to_dataset(name="Sv_noise")
        # Basic attrs sanitization to avoid None in NetCDF
        _add_basic_attrs(ds_out)

        # Save to NetCDF
        logger.info(f"Saving noise estimate to {args.output_path} ...")
        ds_out.to_netcdf(args.output_path, mode="w", format="NETCDF4")

        # Pretty-print args for logs and echo the primary output path to stdout for piping
        pretty_args = pprint.pformat(vars(args))
        logger.debug(f"\naa-noise-est args:\n{pretty_args}")
        print(args.output_path.resolve())

        logger.info("Background noise estimation complete.")

    except Exception as e:
        logger.exception(f"Error during background noise estimation: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
