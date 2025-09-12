#!/usr/bin/env python3
"""
Console tool for creating an impulse-noise mask using echopype.clean.mask_impulse_noise
and saving the mask (attached to the original dataset) back to NetCDF.

Usage examples:
  aa-min /path/to/input.nc --depth_bin 5m --num_side_pings 2 -o /path/to/output.nc
  cat /path/to/input.nc | aa-min --impulse_noise_threshold 12.0dB

This mirrors the style of your aa-mvbs wrapper (stdin support, pretty args, logging).
"""

import argparse
import sys
from pathlib import Path
from loguru import logger
import pprint
import xarray as xr
import echopype as ep
from echopype.clean import mask_impulse_noise
import math

def print_help():
    help_text = """
    Usage: aa-min [OPTIONS] [INPUT_PATH]

    Arguments:
    INPUT_PATH                  Path to the .netcdf4 file.
                                Optional. Defaults to stdin if not provided.

    Options:
    -o, --output_path           Path to save processed output (NetCDF).
                                Default: input file with "_mask" appended to stem.

    --depth_bin                 Downsampling vertical bin size (default: 5m)
    --num_side_pings            Number of side pings for two-sided comparison (default: 2)
    --impulse_noise_threshold   Threshold (dB) for impulse detection (default: "10.0dB")
    --range_var                 Range coordinate: "depth" or "echo_range" (default: depth)
    --use_index_binning         Use index-based binning for speed (default: False)

    Example:
    aa-min /path/to/input.nc --depth_bin 5m --num_side_pings 3 \
        --impulse_noise_threshold "12.0dB" -o /path/to/output_mask.nc
    """
    print(help_text)



def main():
    # Allow passing input path via stdin if no args provided
    if len(sys.argv) == 1:
        if not sys.stdin.isatty():
            stdin_data = sys.stdin.readline().strip()
            if stdin_data:
                sys.argv.append(stdin_data)
        else:
            print_help()
            sys.exit(0)

    parser = argparse.ArgumentParser(
        description="Create an impulse-noise mask using echopype.clean.mask_impulse_noise."
    )

    parser.add_argument("input_path", type=Path, help="Path to the .netcdf4 file.", nargs="?")
    parser.add_argument("-o", "--output_path", type=Path, help="Path to save processed output.")

    parser.add_argument("--depth_bin", type=str, default="5m",
                        help="Downsampling bin size along vertical range variable (default: 5m).")
    parser.add_argument("--num_side_pings", type=int, default=2,
                        help="Number of side pings for two-sided comparison (default: 2).")
    parser.add_argument("--impulse_noise_threshold", type=str, default="10.0dB",
                        help='Impulse noise threshold, as a string with units (default: "10.0dB").')
    parser.add_argument("--range_var", type=str, choices=["depth", "echo_range"], default="depth",
                        help='Vertical axis range variable: "depth" or "echo_range" (default: depth).')
    parser.add_argument("--use_index_binning", action="store_true",
                        help="Use index-based binning for speed (default: False).")

    args = parser.parse_args()

    # Handle stdin fallback for input_path
    if args.input_path is None:
        args.input_path = Path(sys.stdin.readline().strip())
        logger.info(f"Read input path from stdin: {args.input_path}")

    if not args.input_path.exists():
        logger.debug(vars(args))
        logger.error(f"File '{args.input_path}' does not exist.")
        sys.exit(1)

    allowed_extensions = {".netcdf4": "netcdf", ".nc": "netcdf"}
    ext = args.input_path.suffix.lower()
    if ext not in allowed_extensions:
        logger.error(
            f"'{args.input_path.name}' is not a supported file type. "
            f"Allowed: {', '.join(allowed_extensions.keys())}"
        )
        sys.exit(1)

    # Default output: append _mask to stem (mirrors aa-mvbs pattern)
    if args.output_path is None:
        args.output_path = args.input_path
    args.output_path = args.output_path.with_stem(args.output_path.stem + "_maskimpulsenoise")
    args.output_path = args.output_path.with_suffix(".nc")
    logger.info(f"Output path set to: {args.output_path}")

    # Call processor
    try:
        process_file(
            input_path=args.input_path,
            output_path=args.output_path,
            depth_bin=args.depth_bin,
            num_side_pings=args.num_side_pings,
            impulse_noise_threshold=args.impulse_noise_threshold,
            range_var=args.range_var,
            use_index_binning=args.use_index_binning,
        )
        # Pretty-print args
        pretty_args = pprint.pformat(vars(args))
        logger.debug(f"\naa-min args:\n{pretty_args}")
        # Emit the output path to stdout for piping
        print(args.output_path.resolve())

    except Exception as e:
        logger.exception("Error during processing: %s", e)
        sys.exit(1)


def process_file(
    input_path: Path,
    output_path: Path,
    depth_bin: str = "5m",
    num_side_pings: int = 2,
    impulse_noise_threshold: str = "10.0dB",
    range_var: str = "depth",
    use_index_binning: bool = False
):
    """
    Load dataset, locate Sv dataset/variable, compute impulse-noise mask,
    attach the mask back to the original dataset, and save to NetCDF.
    """
   # Step 1: Load file into EchoData object
    logger.info(f"Loading NetCDF file {input_path} into EchoData...")
    ds_Sv = xr.open_dataset(input_path)


    da_mask = mask_impulse_noise(
        ds_Sv,
        depth_bin=depth_bin,
        num_side_pings=num_side_pings,
        impulse_noise_threshold=impulse_noise_threshold,
        range_var=range_var,
        use_index_binning=use_index_binning,
    )




    # Save to NetCDF
    logger.info("Saving dataset (with the attached impulse noise mask) to %s ...", output_path)
    da_mask.to_netcdf(output_path, mode="w", format="NETCDF4")
    logger.info("Save complete.")


if __name__ == "__main__":
    main()
