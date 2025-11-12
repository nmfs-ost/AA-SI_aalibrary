#!/usr/bin/env python3
"""
Console tool for adding split-beam (alongship/athwartship) angles to an Sv dataset
using Echopype’s `consolidate.add_splitbeam_angle`.

Pattern matches your existing tools:
- optional stdin piping for INPUT_PATH
- argparse-wrapped single function
- human-readable docstrings & inline comments

This wraps:
  echopype.consolidate.add_splitbeam_angle(
      source_Sv, echodata, waveform_mode, encode_mode,
      pulse_compression=False, storage_options={}, to_disk=True
  )

We call it with `to_disk=False` to return an xarray.Dataset and then write to <output>.nc.
"""

import io
from contextlib import redirect_stdout
import argparse
import sys
from pathlib import Path
import xarray as xr
from loguru import logger
import echopype as ep  # ensure echopype is installed
from echopype.consolidate import add_splitbeam_angle
import pprint


def print_help():
    """Standalone help text when invoked with no args and no stdin."""
    help_text = """
    Usage: aa-splitbeam-angle [OPTIONS] [INPUT_PATH]

    Arguments:
      INPUT_PATH                   Path to an Sv NetCDF (.nc). Optional; if omitted,
                                   a path token may be read from stdin.

    Options:
      -o, --output_path PATH       Output NetCDF path (default: <stem>_splitbeam_angle.nc).
      --echodata PATH              Path to EchoData source (raw/converted) that holds
                                   Sonar/Beam_group* data required for angle computation.
                                   If not provided, defaults to INPUT_PATH.
      --waveform-mode {CW,BB}      Transmit waveform mode: CW (narrowband) or BB (broadband).
                                   Required.
      --encode-mode {complex,power}  Return echo encoding type: 'complex' or 'power'.
                                   Required. ('power' only valid with CW.)
      --pulse-compression          Use pulse compression (valid only for BB + complex).
      --no-overwrite               Do not overwrite an existing output file.

      -h, --help                   Show this help message and exit.

    Description:
      Computes alongship and athwartship split-beam angles and adds them to the Sv dataset.
      Requires the associated raw or converted file containing beam group and transducer data.
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
    """Entry point for the aa-splitbeam-angle CLI."""
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
        description="Add split-beam angles (alongship/athwartship) to an Sv dataset."
    )

    # ---------------------------
    # Positional/IO args
    # ---------------------------
    parser.add_argument(
        "input_path",
        type=Path,
        nargs="?",
        help="Path to an Sv NetCDF (.nc).",
    )
    parser.add_argument(
        "-o", "--output_path",
        type=Path,
        help="Output NetCDF path (default: <stem>_splitbeam_angle.nc).",
    )
    parser.add_argument(
        "--echodata",
        type=Path,
        help="Path to EchoData source (raw/converted) containing Sonar/Beam_group*.",
    )

    # ---------------------------
    # add_splitbeam_angle parameters
    # ---------------------------
    parser.add_argument("--waveform-mode", dest="waveform_mode",
                        choices=["CW", "BB"], required=True,
                        help="Transmit waveform mode: CW (narrowband) or BB (broadband).")
    parser.add_argument("--encode-mode", dest="encode_mode",
                        choices=["complex", "power"], required=True,
                        help="Return echo encoding type: complex or power.")
    parser.add_argument("--pulse-compression", dest="pulse_compression",
                        action="store_true",
                        help="Use pulse compression (valid only for BB + complex).")

    parser.add_argument("--no-overwrite", action="store_true",
                        help="Do not overwrite an existing output file.")

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
        args.output_path = args.input_path.with_stem(args.input_path.stem + "_splitbeam_angle").with_suffix(".nc")

    if args.output_path.exists() and args.no_overwrite:
        logger.error(f"Output file '{args.output_path}' exists and --no-overwrite was set.")
        sys.exit(1)

    if args.echodata is not None and not args.echodata.exists():
        logger.error(f"EchoData source '{args.echodata}' does not exist.")
        sys.exit(1)

    echodata_arg = args.echodata if args.echodata is not None else args.input_path

    try:
        # ---------------------------
        # Load dataset quietly
        # ---------------------------
        f = io.StringIO()
        with redirect_stdout(f):
            ds = xr.open_dataset(args.input_path)

        # ---------------------------
        # Compute split-beam angles and add to Sv
        # ---------------------------
        logger.info("Adding split-beam angles to Sv ...")
        ds_with_angle = add_splitbeam_angle(
            source_Sv=ds,
            echodata=echodata_arg,
            waveform_mode=args.waveform_mode,
            encode_mode=args.encode_mode,
            pulse_compression=args.pulse_compression,
            to_disk=False,  # return Dataset for manual saving
        )

        # Clean attributes to avoid None in NetCDF
        _add_basic_attrs(ds_with_angle)

        # Save to NetCDF
        logger.info(f"Saving Sv + angle dataset to {args.output_path} ...")
        ds_with_angle.to_netcdf(args.output_path, mode="w", format="NETCDF4")

        # Pretty-print args for logs and echo the primary output path to stdout for piping
        pretty_args = pprint.pformat(vars(args))
        logger.debug(f"\naa-splitbeam-angle args:\n{pretty_args}")
        print(args.output_path.resolve())

        logger.info("Split-beam angle computation complete.")

    except Exception as e:
        logger.exception(f"Error during add_splitbeam_angle: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
