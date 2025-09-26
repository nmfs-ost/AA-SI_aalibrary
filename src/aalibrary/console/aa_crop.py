#!/usr/bin/env python3
"""
Template console tool for processing .raw or .netcdf4 files with Echopype using Loguru.
"""

import argparse
import sys
from pathlib import Path

from loguru import logger
import echopype as ep  # make sure echopype is installed


def main():
    parser = argparse.ArgumentParser(
        description="Convert RAW files to NetCDF using Echopype, apply transformations, and save back."
    )

    parser = argparse.ArgumentParser(
        description="Convert RAW files to NetCDF using Echopype, remove background noise, and save back."
    )

    parser.add_argument(
        "input_path", type=Path, help="Path to the .raw or .netcdf4 file."
    )

    parser.add_argument(
        "-o",
        "--output_path",
        type=Path,
        help="Path to save processed output. Defaults to input_path with '_processed.nc' suffix.",
    )

    # remove_background_noise arguments (excluding ds_Sv)
    parser.add_argument(
        "--ping_num",
        type=int,
        required=True,
        help="Number of pings to use for background noise removal.",
    )

    parser.add_argument(
        "--range_sample_num",
        type=int,
        required=True,
        help="Number of range samples to use for background noise removal.",
    )

    parser.add_argument(
        "--background_noise_max",
        type=str,
        default=None,
        help="Optional maximum background noise value.",
    )

    parser.add_argument(
        "--snr_threshold",
        type=float,
        default=3.0,
        help="SNR threshold in dB (default: 3.0).",
    )

    args = parser.parse_args()

    # ---------------------------
    # Validate input
    # ---------------------------
    if not args.input_path.exists():
        logger.error(f"File '{args.input_path}' does not exist.")
        sys.exit(1)

    allowed_extensions = {".raw": "raw", ".netcdf4": "netcdf", ".nc": "netcdf"}

    ext = args.input_path.suffix.lower()
    if ext not in allowed_extensions:
        logger.error(
            f"'{args.input_path.name}' is not a supported file type. "
            f"Allowed: {', '.join(allowed_extensions.keys())}"
        )
        sys.exit(1)

    file_type = allowed_extensions[ext]

    # Set default output path if not provided
    if args.output_path is None:
        args.output_path = args.input_path.with_name(
            args.input_path.stem + "_processed.nc"
        )

    # ---------------------------
    # Process file
    # ---------------------------
    try:
        process_file(args.input_path, args.output_path, file_type)
        # Print to stdout for piping
        print(args.output_path)
    except Exception as e:
        logger.exception(f"Error during processing: {e}")
        sys.exit(1)


def process_file(input_path: Path, output_path: Path, file_type: str):
    """
    Process a RAW or NetCDF file with Echopype, apply transformation, and save to NetCDF.
    """
    # Step 1: Load file into EchoData object
    if file_type == "raw":
        logger.info(f"Loading RAW file {input_path} into EchoData...")
        ed = ep.open_raw(input_path)  # use appropriate sonar_type if needed
    elif file_type == "netcdf":
        logger.info(f"Loading NetCDF file {input_path} into EchoData...")
        ed = ep.open_converted(input_path)

    # Step 2: Apply transformation (placeholder)
    logger.info("Applying transformations to EchoData...")
    ed = transform_echo_data(ed)  # replace with actual logic

    # Step 3: Save back to NetCDF
    logger.info(f"Saving processed EchoData to {output_path} ...")
    ed.to_netcdf(output_path)
    logger.info("Processing complete.")


def transform_echo_data(ed):
    """
    Placeholder function to apply any transformation to EchoData.
    """
    # TODO: add your transformation logic here
    return ed


if __name__ == "__main__":
    main()
