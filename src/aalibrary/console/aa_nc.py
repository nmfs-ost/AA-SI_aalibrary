#!/usr/bin/env python3
"""
Console tool for converting RAW files to NetCDF using Echopype,
removing background noise, applying transformations, and saving back.
"""

import argparse
import sys
from pathlib import Path
from loguru import logger
import echopype as ep  # make sure echopype is installed

def print_help():
    help_text = """
    Usage: aa-nc [OPTIONS] INPUT_PATH

    Options:
    INPUT_PATH                  Path to the .raw or .netcdf4 file. (Required)
    -o, --output_path           Path to save processed output.
                                Default: overwrites .nc files or creates a new .nc for RAW.
    --sonar_model               Sonar model number (e.g., EK60, EK80). (Required)

    Description:
    This script processes .raw and produces a NetCDF file using echopype.

    Example:
    aa-nc /path/to/input.raw --sonar_model EK80 -o /path/to/output.nc
    """
    print(help_text)


def main():
    
    
    # Display help if no arguments are provided or if --help is explicitly passed
    if len(sys.argv) == 1 or "--help" in sys.argv:
        print_help()
        sys.exit(0)
    
    parser = argparse.ArgumentParser(
        description="Process .raw or .netcdf4 files with Echopype and remove background noise."
    )

    # ---------------------------
    # Required file arguments
    # ---------------------------
    parser.add_argument(
        "input_path",
        type=Path,
        help="Path to the .raw or .netcdf4 file."
    )

    parser.add_argument(
        "-o", "--output_path",
        type=Path,
        help="Path to save processed output. Default behavior overwrites .nc files or creates a new .nc for RAW."
    )

    # ---------------------------
    # remove_background_noise arguments
    # ---------------------------
    parser.add_argument(
        "--sonar_model",
        type=str,
        required=True,
        help="Sonar model number (e.g., EK60, EK80, etc.)."
    )

    args = parser.parse_args()

    # ---------------------------
    # Validate input
    # ---------------------------
    if not args.input_path.exists():
        logger.error(f"File '{args.input_path}' does not exist.")
        sys.exit(1)

    allowed_extensions = {
        ".raw": "raw"
    }

    ext = args.input_path.suffix.lower()
    if ext not in allowed_extensions:
        logger.error(
            f"'{args.input_path.name}' is not a supported file type. "
            f"Allowed: {', '.join(allowed_extensions.keys())}"
        )
        sys.exit(1)

    file_type = allowed_extensions[ext]

    # ---------------------------
    # Set default output path
    # ---------------------------
    if args.output_path is None:
        if file_type == "raw":
            # Overwrite the existing NetCDF
            args.output_path = args.input_path.with_suffix(".nc")

    # ---------------------------
    # Process file
    # ---------------------------
    try:
        process_file(
            input_path=args.input_path,
            output_path=args.output_path,
            sonar_model=args.sonar_model
        )
        # Print output path to stdout for piping
        print(args.output_path.resolve())
    except Exception as e:
        logger.exception(f"Error during processing: {e}")
        sys.exit(1)

def clean_attrs(Sv):
    # Dataset-level attrs
    for k, v in Sv.attrs.items():
        if v is None:
            Sv.attrs[k] = "NA"  # or float('nan') if numeric

    # Variable-level attrs
    for var in Sv.data_vars:
        for k, v in Sv[var].attrs.items():
            if v is None:
                Sv[var].attrs[k] = "NA"  # or float('nan') if numeric
    return Sv

def process_file(input_path: Path, output_path: Path, sonar_model: str):
    """
    Load EchoData from RAW or NetCDF, remove background noise, apply transformations, and save to NetCDF.
    """

    logger.info(f"Loading RAW file {input_path} into EchoData...")
    ed = ep.open_raw(raw_file = input_path, sonar_model = sonar_model)  # add sonar_type if needed


    ed.to_netcdf(save_path = output_path.with_suffix(".nc"))


    # Step 4: Save back to NetCDF
    logger.info(f"Saving processed EchoData to {output_path} ...")


    logger.info("Processing complete.")



if __name__ == "__main__":
    main()
