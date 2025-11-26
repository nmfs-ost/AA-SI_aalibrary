#!/usr/bin/env python3
"""
Console tool for locating attenuated signal in calibrated Sv data with Echopype
and saving an attenuated-signal mask (and optionally an Sv-cleaned file).

Pattern matches your existing tools:
- optional stdin piping for INPUT_PATH
- argparse-wrapped simple function
- human-readable docstrings & inline comments
"""

import io
from contextlib import redirect_stdout
import argparse
import sys
from pathlib import Path
import xarray as xr
from loguru import logger
import echopype as ep  # ensure echopype is installed
from echopype.clean import mask_attenuated_signal
import pprint


def print_help():
    """Standalone help text (useful when invoked with no args and no stdin)."""
    help_text = """
    Usage: aa-attenuated [OPTIONS] [INPUT_PATH]

    Arguments:
      INPUT_PATH                   Path to the calibrated .nc (NetCDF) file
                                   containing Sv (preferred), or a converted
                                   Echopype file that can be calibrated to Sv.
                                   Optional. Defaults to stdin if not provided.

    Options:
      -o, --output_path PATH       Where to write the attenuated-signal mask (NetCDF).
                                   Default: <stem>_attenuated_mask.nc
      --apply                      Also apply the mask to Sv and write a cleaned
                                   Sv file (suffix: _attenuated_cleaned.nc).

      # mask_attenuated_signal parameters
      --upper-limit-sl STR         Upper limit of deep scattering layer line, e.g. '400.0m'.
                                   Default: 400.0m
      --lower-limit-sl STR         Lower limit of deep scattering layer line, e.g. '500.0m'.
                                   Default: 500.0m
      --num-side-pings INT         Pings on each side defining the comparison block.
                                   Default: 15
      --attenuation-threshold STR  Threshold above local context, e.g. '8.0dB'.
                                   Default: 8.0dB
      --range-var STR              Name of the range/depth coordinate (e.g., 'depth').
                                   Default: depth

      -h, --help                   Show this help message and exit.

    Description:
      Creates a boolean mask marking likely attenuated-signal pings based on
      comparisons across neighboring ping blocks between two depth limits.
      Optionally applies the mask to Sv to produce a cleaned Sv dataset.

    Examples:
      aa-attenuated data.nc --upper-limit-sl 350m --lower-limit-sl 480m --num-side-pings 17
      aa-attenuated data.nc --apply -o out_mask.nc
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
    """Entry point for the aa-attenuated CLI."""
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
        description="Create an attenuated-signal mask from Sv and (optionally) write Sv cleaned with that mask."
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
        help="Output path for the mask NetCDF (default: <stem>_attenuated_mask.nc).",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Also write Sv cleaned by the attenuated-signal mask to <stem>_attenuated_cleaned.nc.",
    )

    # ---------------------------
    # mask_attenuated_signal parameters
    # ---------------------------
    parser.add_argument("--upper-limit-sl", dest="upper_limit_sl", default="400.0m",
                        help="Upper limit of deep scattering layer line, e.g., '400.0m' (default: 400.0m).")
    parser.add_argument("--lower-limit-sl", dest="lower_limit_sl", default="500.0m",
                        help="Lower limit of deep scattering layer line, e.g., '500.0m' (default: 500.0m).")
    parser.add_argument("--num-side-pings", dest="num_side_pings", type=int, default=15,
                        help="Number of side pings for comparison block (default: 15).")
    parser.add_argument("--attenuation-threshold", dest="attenuation_signal_threshold", default="8.0dB",
                        help="Attenuation threshold above local context, e.g., '8.0dB' (default: 8.0dB).")
    parser.add_argument("--range-var", dest="range_var", default="depth",
                        help="Range/depth variable name (default: depth).")

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

    # Default mask output path if not provided
    if args.output_path is None:
        args.output_path = args.input_path.with_stem(args.input_path.stem + "_attenuated_mask").with_suffix(".nc")

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
        # Compute attenuated-signal mask
        # ---------------------------
        logger.info("Computing attenuated-signal mask...")
        mask = mask_attenuated_signal(
            ds_Sv=ds,
            upper_limit_sl=args.upper_limit_sl,
            lower_limit_sl=args.lower_limit_sl,
            num_side_pings=args.num_side_pings,
            attenuation_signal_threshold=args.attenuation_signal_threshold,
            range_var=args.range_var,
        )

        # Save mask to its own NetCDF
        logger.info(f"Saving attenuated-signal mask to {args.output_path} ...")
        # Wrap DataArray into a Dataset for clearer NetCDF structure
        mask_ds = mask.to_dataset(name="attenuated_mask")
        _add_basic_attrs(mask_ds)
        mask_ds.to_netcdf(args.output_path, mode="w", format="NETCDF4")

        # Optionally write a cleaned Sv file with attenuated-signal samples set to NaN
        if args.apply:
            cleaned_path = args.input_path.with_stem(args.input_path.stem + "_attenuated_cleaned").with_suffix(".nc")
            logger.info(f"Applying mask to Sv and writing cleaned Sv to {cleaned_path} ...")
            ds_clean = ds.copy()
            # Keep values where NOT attenuated signal
            ds_clean["Sv"] = ds_clean["Sv"].where(~mask, other=float("nan"))
            _add_basic_attrs(ds_clean)
            ds_clean.to_netcdf(cleaned_path, mode="w", format="NETCDF4")

        # Pretty-print args for logs and echo the primary output (mask path) to stdout for piping
        pretty_args = pprint.pformat(vars(args))
        logger.debug(f"\naa-attenuated args:\n{pretty_args}")
        print(args.output_path.resolve())

        logger.info("Attenuated-signal masking complete.")

    except Exception as e:
        logger.exception(f"Error during attenuated-signal masking: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
