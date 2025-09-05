#!/usr/bin/env python3
"""
Console tool for converting RAW files to NetCDF using Echopype,
removing background noise, applying transformations, and saving back.
"""
import io
from contextlib import redirect_stdout
import argparse
import sys
from pathlib import Path
import xarray as xr

from loguru import logger
import echopype as ep  # make sure echopype is installed
from echopype.clean import remove_background_noise


def print_help():
    help_text = """
    Usage: aa-clean [OPTIONS] [INPUT_PATH]

    Arguments:
    INPUT_PATH                  Path to the .netcdf4 file.
                                Optional. Defaults to stdin if not provided.

    Options:
    -o, --output_path           Path to save processed output.
                                Default: overwrites .nc files or creates a new .nc for RAW.
    --ping_num                  Number of pings to use for background noise removal.
                                Default: 20
    --range_sample_num          Number of range samples to use for background noise removal.
                                Default: 20
    --background_noise_max      Optional maximum background noise value.
                                Default: None
    --snr_threshold             SNR threshold in dB.
                                Default: 3.0

    Description:
    This tool processes .netcdf4 files with Echopype and removes
    background noise using ping-based and range-based thresholds.

    Example:
    aa-clean /path/to/input.nc --ping_num 50 --range_sample_num 200 \\
            --snr_threshold 5.0 -o /path/to/output.nc
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

    # ---------------------------
    # remove_background_noise arguments
    # ---------------------------
    parser.add_argument(
        "--ping_num",
        type=int,
        default=20,
        help="Number of pings to use for background noise removal.",
    )

    parser.add_argument(
        "--range_sample_num",
        type=int,
        default=20,
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

        else:
            # RAW file → produce NetCDF with same stem
            args.output_path = args.input_path.with_suffix(".nc")

    # ---------------------------
    # Process file
    # ---------------------------
    try:
        
        args.output_path = args.output_path.with_stem(args.output_path.stem + "_clean")
        args.output_path = args.output_path.with_suffix(".nc")
        process_file(
            input_path=args.input_path,
            output_path=args.output_path,
            file_type=file_type,
            ping_num=args.ping_num,
            range_sample_num=args.range_sample_num,
            background_noise_max=args.background_noise_max,
            snr_threshold=args.snr_threshold,
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


def process_file(
    input_path: Path,
    output_path: Path,
    file_type: str,
    ping_num: int,
    range_sample_num: int,
    background_noise_max: str = None,
    snr_threshold: float = 3.0,
):
    """
    Load EchoData from RAW or NetCDF, remove background noise, apply transformations, and save to NetCDF.
    """
    logger.info(f"Loading NetCDF file {input_path} into EchoData...")
    
    


    f = io.StringIO()
    with redirect_stdout(f):
        ed = xr.open_dataset(input_path)

    # Step 3: Apply any additional transformation
    logger.info("Applying transformations to EchoData...")
    # Sv = ep.calibrate.compute_Sv(ed)
    Sv_clean = transform_echo_data(
        ed, ping_num, range_sample_num, background_noise_max, snr_threshold
    )

    # Step 4: Save back to NetCDF
    logger.info(f"Saving processed EchoData to {output_path} ...")

    Sv_clean = clean_attrs(Sv_clean)

    # .to_netcdf(output_path, overwrite=True)
    # ed.ds_Sv_clean = Sv_clean  # Update EchoData with cleaned Sv

    Sv_clean.to_netcdf(output_path.with_suffix(".nc"))
    logger.info("Processing complete.")


def transform_echo_data(
    ed: ep.echodata,
    ping_num: int,
    range_sample_num: int,
    background_noise_max: str = None,
    snr_threshold: float = 3.0,
):

    # Step 2: Remove background noise
    logger.info("Removing background noise...")
    # ds_Sv comes from the EchoData object internally



    #Sv = ep.calibrate.compute_Sv(ed)
    Sv_clean = remove_background_noise(
        ed,
        ping_num=ping_num,
        range_sample_num=range_sample_num,
        background_noise_max=background_noise_max,
        SNR_threshold=f"{snr_threshold}dB",
    )
    return Sv_clean


if __name__ == "__main__":
    main()
