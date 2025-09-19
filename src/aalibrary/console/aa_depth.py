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
import pprint
import xarray as xr
from echopype.consolidate import add_depth

def print_help():
    help_text = """


    Options:
    INPUT_PATH                  Path to the .raw or .netcdf4 file. (Required)
    -o, --output_path           Path to save processed output.
                                Default: overwrites .nc files or creates a new .nc for RAW.
    --variable                  Variable to add to the output dataset (e.g., depth, location, splitbeam_angle).

    Description:


    Example:

    """
    print(help_text)


def main():

    if len(sys.argv) == 1:
        if not sys.stdin.isatty():
            stdin_data = sys.stdin.readline().strip()
            if stdin_data:
                sys.argv.append(stdin_data)
        else:
            print_help()
            sys.exit(0)

    parser = argparse.ArgumentParser(
        description="Process .raw or .netcdf4 files with Echopype and remove background noise."
    )

    # ---------------------------
    # Required file arguments
    # ---------------------------
    parser.add_argument(
        "input_path", type=Path, help="Path to the .raw or .netcdf4 file."
    )

    parser.add_argument(
        "-o",
        "--output_path",
        type=Path,
        help="Path to save processed output. Default behavior overwrites .nc files or creates a new .nc for RAW.",
    )

    parser.add_argument(
        "--depth-offset",
        type=float,
        default=0.0,
        help="Offset along depth to account for transducer position in water (default: 0)."
    )

    parser.add_argument(
        "--tilt",
        type=float,
        default=0.0,
        help="Transducer tilt angle in degrees (default: 0)."
    )

    parser.add_argument(
        "--downward",
        action="store_true",
        default=True,
        help="Flag: if provided, transducers point downward (default: True). "
             "Use --no-downward to set False."
    )

    parser.add_argument(
        "--no-downward",
        dest="downward",
        action="store_false",
        help=argparse.SUPPRESS  # hidden, just to allow toggling
    )
    
    args = parser.parse_args()

       # ---------------------------
    # Validate input
    # ---------------------------

    if args.input_path is None:
        # Read from stdin
        args.input_path = Path(sys.stdin.readline().strip())
        logger.info(f"Read input path from stdin: {args.input_path}")

    if not args.input_path.exists():
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

    file_type = allowed_extensions[ext]

    # ---------------------------
    # Set default output path
    # ---------------------------
    if args.output_path is None:
        if file_type == "netcdf":
            # Overwrite the existing NetCDF
            args.output_path = args.input_path



    # ---------------------------
    # Process file
    # ---------------------------
    try:
        
        args.output_path = args.output_path.with_stem(args.output_path.stem + "_depth")
        args.output_path = args.output_path.with_suffix(".nc")

        # Pretty-print args to logger
        args_dict = vars(args)
        pretty_args = pprint.pformat(args_dict)
        logger.debug(f"\naa-depth args:\n{pretty_args}")
        
        process_file(
            input_path=args.input_path,
            output_path=args.output_path,
            depth_offset=args.depth_offset,
            tilt=args.tilt,
            downward=args.downward        )
        # Print output path to stdout for piping
        
        logger.success(f"Desired data generated and saved to\n\t{args.output_path.resolve()}")
        logger.success(f"Piping saved .nc path to stdout ⟶")        

        print(args.output_path.resolve())
    except Exception as e:
        logger.exception(f"Error during processing: {e}")
        sys.exit(1)



def process_file(input_path: Path, output_path: Path, depth_offset: float = 0.0, tilt: float = 0.0, downward: bool = True, variable: str = "depth"):
    """
    Load EchoData from RAW or NetCDF, remove background noise, apply transformations, and save to NetCDF.
    """

    logger.info(f"Loading NetCDF file {input_path} into xarray dataset")
    ds_Sv = xr.open_dataset(input_path)

    ds_Sv = add_depth(
        ds_Sv,
        depth_offset=depth_offset,
        tilt=tilt,
        downward=downward    )

        
    ds_Sv.to_netcdf(output_path)    



if __name__ == "__main__":
    main()
