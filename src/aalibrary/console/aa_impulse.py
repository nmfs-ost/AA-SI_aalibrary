#!/usr/bin/env python3
"""
Console tool for locating impulse noise in calibrated Sv data with Echopype
and saving an impulse-noise mask (and optionally an Sv-cleaned file).

This follows the same general pattern as your aa-clean tool:
- optional stdin piping for INPUT_PATH
- argparse-wrapped simple function
- verbose, human-readable inline comments and docstrings
"""

import io
from contextlib import redirect_stdout
import argparse
import sys
from pathlib import Path
import xarray as xr
from loguru import logger
import echopype as ep  # ensure echopype is installed
from echopype.clean import mask_impulse_noise
import pprint


def print_help():
    """Standalone help text (useful when invoked with no args and no stdin)."""
    help_text = """
    Usage: aa-impulse [OPTIONS] [INPUT_PATH]

    Arguments:
      INPUT_PATH                 Path to the calibrated .nc (NetCDF) file
                                 containing Sv (preferred), or a converted
                                 Echopype file that can be calibrated to Sv.
                                 Optional. Defaults to stdin if not provided.

    Options:
      -o, --output_path PATH     Where to write the impulse-noise mask (NetCDF).
                                 Default: <stem>_impulse_mask.nc
      --apply                    Also apply the mask to Sv and write a cleaned
                                 Sv file (suffix: _impulse_cleaned.nc).
      --depth-bin STR            Vertical bin size for comparison, e.g. '5m'.
                                 Default: 5m
      --num-side-pings INT       Pings on each side for two-sided comparison.
                                 Default: 2
      --impulse-threshold STR    Threshold in dB above local context, e.g. '10.0dB'.
                                 Default: 10.0dB
      --range-var STR            Name of the range/depth coordinate (e.g., 'depth').
                                 Default: depth
      --use-index-binning        Use index-based binning instead of physical units.
      -h, --help                 Show this help message and exit.

    Description:
      Creates a boolean mask marking likely impulse-noise “flecks” using a
      ping-wise two-sided comparison in depth-binned windows.
      Optionally applies the mask to Sv to produce a cleaned Sv dataset.

    Examples:
      aa-impulse data.nc --depth-bin 5m --num-side-pings 3 --impulse-threshold 12dB
      aa-impulse data.nc --apply -o out_mask.nc
    """
    print(help_text)


def main():
    """Entry point for the aa-impulse CLI."""
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
        description="Create an impulse-noise mask from Sv and (optionally) write Sv cleaned with that mask."
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
        help="Output path for the mask NetCDF (default: <stem>_impulse_mask.nc).",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Also write Sv cleaned by the impulse mask to <stem>_impulse_cleaned.nc.",
    )

    # ---------------------------
    # mask_impulse_noise parameters
    # ---------------------------
    parser.add_argument(
        "--depth-bin",
        dest="depth_bin",
        default="5m",
        help="Depth bin size, e.g., '5m' (default: 5m).",
    )
    parser.add_argument(
        "--num-side-pings",
        dest="num_side_pings",
        type=int,
        default=2,
        help="Number of side pings for two-sided comparison (default: 2).",
    )
    parser.add_argument(
        "--impulse-threshold",
        dest="impulse_noise_threshold",
        default="10.0dB",
        help="Impulse threshold above local context, e.g. '10.0dB' (default: 10.0dB).",
    )
    parser.add_argument(
        "--range-var",
        dest="range_var",
        default="depth",
        help="Range/depth variable name (default: depth).",
    )
    parser.add_argument(
        "--use-index-binning",
        dest="use_index_binning",
        action="store_true",
        help="Use index-based binning rather than physical bin sizes.",
    )

    args = parser.parse_args()

    # ---------------------------
    # Resolve/validate input
    # ---------------------------
    if args.input_path is None:
        # If not provided on CLI, try to read a path token from stdin (same behavior as aa-clean).
        args.input_path = Path(sys.stdin.readline().strip())
        logger.info(f"Read input path from stdin: {args.input_path}")

    if not args.input_path.exists():
        logger.error(f"File '{args.input_path}' does not exist.")
        sys.exit(1)

    # Default mask output path if not provided
    if args.output_path is None:
        args.output_path = args.input_path.with_stem(args.input_path.stem + "_impulse_mask").with_suffix(".nc")

    try:
        # ---------------------------
        # Load dataset quietly
        # ---------------------------
        # Like your aa-clean pattern, suppress any library chatter to stdout so pipelines remain clean.
        f = io.StringIO()
        with redirect_stdout(f):
            ds = xr.open_dataset(args.input_path)

        # ---------------------------
        # Ensure we have calibrated Sv
        # ---------------------------
        # If Sv is missing, attempt to calibrate from an Echopype-converted file.
        if "Sv" not in ds.data_vars:
            logger.info("No 'Sv' variable found; attempting to calibrate to Sv via Echopype...")
            # Try opening as converted EchoData and calibrate
            ed = ep.open_converted(args.input_path)
            ds = ep.calibrate.compute_Sv(ed)

        # ---------------------------
        # Compute impulse-noise mask
        # ---------------------------
        logger.info("Computing impulse-noise mask...")
        mask = mask_impulse_noise(
            ds_Sv=ds,
            depth_bin=args.depth_bin,
            num_side_pings=args.num_side_pings,
            impulse_noise_threshold=args.impulse_noise_threshold,
            range_var=args.range_var,
            use_index_binning=args.use_index_binning,
        )

        # Save mask to its own NetCDF
        logger.info(f"Saving impulse-noise mask to {args.output_path} ...")
        # Wrap DataArray into a Dataset for clearer NetCDF structure
        mask_ds = mask.to_dataset(name="impulse_mask")
        _add_basic_attrs(mask_ds)
        mask_ds.to_netcdf(args.output_path, mode="w", format="NETCDF4")

        # Optionally write a cleaned Sv file with impulse-noise samples set to NaN
        if args.apply:
            cleaned_path = args.input_path.with_stem(args.input_path.stem + "_impulse_cleaned").with_suffix(".nc")
            logger.info(f"Applying mask to Sv and writing cleaned Sv to {cleaned_path} ...")

            # Align mask to Sv dims and set masked samples to NaN
            ds_clean = ds.copy()
            # DataArray.where keeps values where condition is True; here we want to keep when NOT impulse noise.
            ds_clean["Sv"] = ds_clean["Sv"].where(~mask, other=float("nan"))
            _add_basic_attrs(ds_clean)
            ds_clean.to_netcdf(cleaned_path, mode="w", format="NETCDF4")

        # Pretty-print args for logs and echo the primary output (mask path) to stdout for piping
        pretty_args = pprint.pformat(vars(args))
        logger.debug(f"\naa-impulse args:\n{pretty_args}")
        print(args.output_path.resolve())

        logger.info("Impulse-noise masking complete.")

    except Exception as e:
        logger.exception(f"Error during impulse-noise masking: {e}")
        sys.exit(1)


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


if __name__ == "__main__":
    main()
