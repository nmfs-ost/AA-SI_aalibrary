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
from echopype.clean import remove_background_noise


def print_help():
    help_text = """
    Usage: aa-ts [OPTIONS] [INPUT_PATH]

    Arguments:
    INPUT_PATH                 Path to the .raw or .netcdf4 file. (Optional, defaults to stdin)

    Options:
    -o, --output_path           Path to save processed output.
                                Default: overwrites .nc files or creates a new .nc for RAW.

    Description:
    This tool processes .raw or .netcdf4 files with Echopype and removes
    background noise using ping-based and range-based thresholds.

    Example:
    aa-clean /path/to/input.raw --ping_num 50 --range_sample_num 200 \\
            --snr_threshold 5.0 -o /path/to/output.nc
    """
    print(help_text)


def parse_env_params(pair_list):
    """Parses key=value pairs into dict for env_params."""
    env = {}
    for pair in pair_list or []:
        if '=' not in pair:
            raise argparse.ArgumentTypeError(f"Invalid env param: {pair}")
        key, value = pair.split('=', 1)
        env[key.strip()] = float(value)
    return env

def parse_cal_params(pair_list):
    """Parses key=value pairs into dict for cal_params."""
    cal = {}
    for pair in pair_list or []:
        if '=' not in pair:
            raise argparse.ArgumentTypeError(f"Invalid cal param: {pair}")
        key, value = pair.split('=', 1)
        cal[key.strip()] = float(value)
    return cal

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
        "input_path",
        type=Path,
        help="Path to the .raw or .netcdf4 file.",
        nargs="?",  # makes it optional
    )

    parser.add_argument(
        "-o",
        "--output_path",
        type=Path,
        help="Path to save processed output. Default behavior overwrites .nc files or creates a new .nc for RAW.",
    )

    parser.add_argument("--env-param", dest="env_params", nargs='*',
                        type=parse_env_params,
                        help="Environmental parameter(s) as key=value (e.g. sound_speed=1500)")
    parser.add_argument("--cal-param", dest="cal_params", nargs='*',
                        type=parse_cal_params,
                        help="Calibration parameter(s) as key=value (e.g. gain_correction=1.0)")
    args = parser.parse_args()

    # ---------------------------
    # remove_background_noise arguments
    # ---------------------------


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
        
        args.output_path = args.output_path.with_stem(args.output_path.stem + "_ts")
        args.output_path = args.output_path.with_suffix(".nc")
        process_file(
            input_path=args.input_path,
            output_path=args.output_path
        )

        # Print output path to stdout for piping
        print(args.output_path.resolve())

    except Exception as e:
        logger.exception(f"Error during processing: {e}")
        sys.exit(1)


def clean_attrs(TS):
    # Dataset-level attrs
    for k, v in TS.attrs.items():
        if v is None:
            TS.attrs[k] = "NA"  # or float('nan') if numeric

    # Variable-level attrs
    for var in TS.data_vars:
        for k, v in TS[var].attrs.items():
            if v is None:
                TS[var].attrs[k] = "NA"  # or float('nan') if numeric
    return TS


def process_file(
    input_path: Path,
    output_path: Path
):
    """
    Load EchoData from RAW or NetCDF, compute TS and save to NetCDF.
    """


    logger.info(f"Loading NetCDF file {input_path} into EchoData...")
    ed = ep.open_converted(input_path)

    logger.info(f"Computing TS from EchoData...")
    ds_TS = ep.calibrate.compute_TS(ed)

    # Step 4: Save back to NetCDF
    logger.info(f"Saving processed EchoData to {output_path} ...")

    ds_TS_clean_copy = clean_attrs(ds_TS)
    ds_TS = ds_TS_clean_copy  # Ensure we use the cleaned version
    # .to_netcdf(output_path, overwrite=True)
    # ed.ds_TS_clean = TS_clean  # Update EchoData with cleaned TS

    output_path = output_path.with_suffix(".nc")
    ds_TS.to_netcdf(output_path)
    logger.info("TS computation complete.")



if __name__ == "__main__":
    main()
