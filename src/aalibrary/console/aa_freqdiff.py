#!/usr/bin/env python3
"""
Console tool to compute a frequency-differencing mask using Echopype.

This wraps:
  echopype.mask.frequency_differencing(
      source_Sv, storage_options={}, freqABEq=None, chanABEq=None
  )

The mask marks regions where the difference between Sv at one frequency (or channel)
and another meets a “dB difference” criterion (or channel selection criterion).
"""

import io
from contextlib import redirect_stdout
import argparse
import sys
from pathlib import Path
import xarray as xr
from loguru import logger
from echopype.mask import frequency_differencing
import pprint


def print_help():
    """Print help text if invoked without arguments or with no stdin."""
    help_text = """
    Usage: aa-freqdiff [OPTIONS] [INPUT_PATH]

    Arguments:
      INPUT_PATH                Path to a NetCDF/Zarr file (or dataset) containing
                               Sv with a `channel` dimension and `frequency_nominal`, or
                               a conversion output. Optional — defaults to stdin if not provided.

    Options:
      -o, --output_path PATH    Where to write the mask NetCDF (default: <stem>_freqdiff.nc).
      --freqABEq STR            Frequency differencing expression, e.g. '"38.0kHz" - "120.0kHz">=10.0dB'.
      --chanABEq STR            Channel-based differencing expression, e.g. '"chan1" - "chan2"<-5dB'.
      --quiet                   Suppress logger info, only print output path.
      -h, --help                Show this help message and exit.

    Description:
      Computes a boolean mask of Sv data where one frequency minus another
      meets a user-specified threshold/difference. Useful for identifying
      scatterers with different frequency responses (for example krill).
    
    Examples:
      aa-freqdiff data.nc --freqABEq '"38.0kHz" - "120.0kHz">=12.0dB' -o out_mask.nc
      aa-freqdiff data.nc --chanABEq '"chan1" - "chan2"<-5dB'
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
    """Entry point for the aa-freqdiff CLI."""
    if len(sys.argv) == 1:
        if not sys.stdin.isatty():
            stdin_data = sys.stdin.readline().strip()
            if stdin_data:
                sys.argv.append(stdin_data)
        else:
            print_help()
            sys.exit(0)

    parser = argparse.ArgumentParser(
        description="Compute a frequency-differencing mask (Sv differences) using Echopype."
    )
    parser.add_argument(
        "input_path",
        type=Path,
        nargs="?",
        help="Path to dataset (NetCDF or Zarr) containing Sv with channel/frequency_nominal.",
    )
    parser.add_argument(
        "-o", "--output_path",
        type=Path,
        help="Output path for mask NetCDF (default: <stem>_freqdiff.nc).",
    )
    parser.add_argument(
        "--freqABEq",
        dest="freqABEq",
        default=None,
        help="Expression for differencing by frequency, e.g. '\"38.0kHz\" - \"120.0kHz\">=10.0dB'."
    )
    parser.add_argument(
        "--chanABEq",
        dest="chanABEq",
        default=None,
        help="Expression for differencing by channel names, e.g. '\"chan1\" - \"chan2\"<-5dB'."
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress informational logging; only print output path."
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
        args.output_path = args.input_path.with_stem(args.input_path.stem + "_freqdiff").with_suffix(".nc")

    if args.freqABEq is None and args.chanABEq is None:
        logger.error("Either --freqABEq or --chanABEq must be provided (not both).")
        sys.exit(1)
    if args.freqABEq is not None and args.chanABEq is not None:
        logger.error("Only one of --freqABEq or --chanABEq may be provided.")
        sys.exit(1)

    try:
        # Load dataset quietly (suppress chatter)
        f = io.StringIO()
        with redirect_stdout(f):
            ds = xr.open_dataset(args.input_path)

        logger.info("Computing frequency-differencing mask...")
        mask = frequency_differencing(
            source_Sv=ds,
            storage_options={},   # no special options here
            freqABEq=args.freqABEq,
            chanABEq=args.chanABEq
        )

        # Wrap DataArray into Dataset for writing
        mask_ds = mask.to_dataset(name="freqdiff_mask")
        _add_basic_attrs(mask_ds)

        logger.info(f"Saving mask to {args.output_path} ...")
        mask_ds.to_netcdf(args.output_path, mode="w", format="NETCDF4")

        if not args.quiet:
            logger.debug(f"\naa-freqdiff args:\n{pprint.pformat(vars(args))}")
        print(args.output_path.resolve())

        if not args.quiet:
            logger.info("Frequency differencing mask complete.")

    except Exception as e:
        logger.exception(f"Error during frequency differencing: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
