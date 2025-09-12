#!/usr/bin/env python3
"""
Console tool for converting RAW files to NetCDF using Echopype,
removing background noise, applying transformations, and saving back.
"""

import argparse
import math
import sys
from pathlib import Path
from loguru import logger
import echopype as ep  # make sure echopype is installed
from echopype.clean import remove_background_noise
import sys
import signal
import pprint

def print_help():
    help_text = """
    Usage: aa-nasc [OPTIONS] [INPUT_PATH]

    Arguments:
    INPUT_PATH                  Path to the .netcdf4 file.
                                Optional. Defaults to stdin if not provided.

    Options:
    -o, --output_path           Path to save processed NASC output.
                                Default: overwrites input .nc with NASC group
                                or creates a new .nc.

    --range-bin                 Depth bin size in meters.
                                Default: 10m
    --dist-bin                  Horizontal distance bin size in nautical miles.
                                Default: 0.5nmi
    --method                    Flox reduction strategy for binning.
                                Default: map-reduce
    --skipna                    Skip NaN values when averaging.
                                Default: enabled
    --no-skipna                 Include NaN values in mean calculations.
    --closed                    Which side of the bin interval is closed.
                                Choices: left, right
                                Default: left
    --flox-kwargs               Additional flox arguments as key=value pairs.
                                Example: --flox-kwargs min_count=5

    Description:
    This tool computes NASC (Nautical Area Scattering Coefficient) from
    .raw or .netcdf4 files with Echopype. NASC integrates Sv (volume
    backscattering strength) across range and distance bins, producing
    standardized measures for biomass estimation and comparison.

    Example:
    aa-nasc /path/to/input.nc --range-bin 20m --dist-bin 1nmi \\
            --method map-reduce -o /path/to/output.nc
    """
    print(help_text)


def parse_flox_kwargs(pair_list):
    """Parse flox keyword arguments from key=value strings."""
    flox = {}
    for pair in pair_list or []:
        if '=' not in pair:
            raise argparse.ArgumentTypeError(f"Invalid flox-kv pair: {pair}")
        key, value = pair.split('=', 1)
        flox[key.strip()] = eval(value)  # or safer parsing if needed
    return flox

def main():

    signal.signal(signal.SIGPIPE, signal.SIG_DFL)


    if len(sys.argv) == 1:
        if not sys.stdin.isatty():
            stdin_data = sys.stdin.readline().strip()
            if stdin_data:
                sys.argv.append(stdin_data)
        else:
            print_help()
            sys.exit(0)

    
    parser = argparse.ArgumentParser(
        description="Compute Mean Volume Backscattering Strength (nasc) from an Sv dataset using Echopype."
    )

    # ---------------------------
    # Required file arguments
    # ---------------------------
    parser.add_argument(
        "input_path",
        type=Path,
        help="Path to the .raw or .netcdf4 file.",
        nargs="?",                # makes it optional
    )

    parser.add_argument(
        "-o", "--output_path",
        type=Path,
        help="Path to save processed nasc output. Default overwrites input .nc with nasc group or creates a new .nc."
    )

    # ---------------------------
    # compute_nasc arguments
    # ---------------------------
    parser.add_argument("--range-bin", default="10m",
                        help="Depth bin size in meters (default: 10m)")
    parser.add_argument("--dist-bin", default="0.5nmi",
                        help="Horizontal distance bin size in nautical miles (default: 0.5nmi)")
    parser.add_argument("--method", default="map-reduce",
                        help="Flox reduction strategy (default: map-reduce)")
    parser.add_argument("--skipna", action="store_true", default=True,
                        help="Skip NaN values in mean (default: enabled)")
    parser.add_argument("--no-skipna", dest="skipna", action="store_false",
                        help="Include NaN values in mean calculations")
    parser.add_argument("--closed", choices=["left", "right"], default="left",
                        help="Which side of the bin interval is closed (default: left)")
    parser.add_argument("--flox-kwargs", nargs="*", type=parse_flox_kwargs,
                        help="Additional flox kwargs as key=value pairs")

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

    allowed_extensions = {
        ".netcdf4": "netcdf",
        ".nc": "netcdf"
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
        if file_type == "netcdf":
            # Overwrite the existing NetCDF
            args.output_path = args.input_path
            logger.info(f"No output path provided. Overwriting input NetCDF: {args.output_path}")
            
        else:
            # RAW file → produce NetCDF with same stem
            args.output_path = args.input_path.with_suffix(".nc")
            logger.info(f"No output path provided. Saving to: {args.output_path}")

    # ---------------------------
    # Process file
    # ---------------------------
    
    args.output_path = args.output_path.with_stem(args.output_path.stem + "_nasc")
    args.output_path = args.output_path.with_suffix(".nc")
    logger.trace(f"Output path set to: {args.output_path}")
    
    try:
        process_file(
            input_path=args.input_path,
            output_path=args.output_path,
            range_bin=args.range_bin,
            dist_bin=args.dist_bin,
            method=args.method,
            skipna=args.skipna,
            closed=args.closed,
            flox_kwargs=parse_flox_kwargs(args.flox_kwargs)
        )
        # Pretty-print args
        pretty_args = pprint.pformat(vars(args))
        logger.debug(f"\naa-nasc args:\n{pretty_args}")
        print(args.output_path.resolve())
    
    except Exception as e:
        logger.exception(f"Error during processing: {e}")
        sys.exit(1)
        

def clean_attrs(Sv):
    """
    Clean and sanitize metadata attributes in an xarray Dataset by replacing None values.
    
    This function addresses data quality issues where None values in metadata attributes
    can cause problems during data processing, serialization to NetCDF, or when interfacing
    with systems that don't handle None values gracefully.
    
    Parameters
    ----------
    Sv : xarray.Dataset
        An xarray Dataset object (typically containing acoustic backscatter data)
        with potentially problematic None values in its metadata attributes.
        
    Returns
    -------
    xarray.Dataset
        The same Dataset with None values in attributes replaced by "NA" strings.
        The dataset is modified in-place but also returned to support method chaining.
        
    Notes
    -----
    The function operates on two levels of attributes:
    
    1. Dataset-level attributes (Sv.attrs): Global metadata for the entire dataset
    2. Variable-level attributes (Sv[var].attrs): Metadata specific to each data variable
    
    All None values are replaced with the string "NA". For numeric attributes where
    a numeric representation of missing values is preferred, consider using 
    float('nan') instead of "NA".
    
    """
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
    output_path: Path = None,
    range_bin: str = "10m",
    dist_bin: str = "0.5nmi",
    method: str = "map-reduce",
    skipna: bool = True,
    closed: str = "left",
    flox_kwargs: dict = None
):
    
    
    
    
    """
    Load EchoData from RAW or NetCDF, remove background noise, apply transformations, and save to NetCDF.
    """
    # Step 1: Load file into EchoData object


    logger.info(f"Loading NetCDF file {input_path} into EchoData...")
    ed = ep.open_converted(input_path)


    # Step 3: Apply any additional transformation
    logger.info("Applying mean volume backscattering strength (nasc) transformations to EchoData...")
    #Sv = ep.calibrate.compute_Sv(ed)
    logger.info("Calibrating EchoData to Sv...")
    ds_Sv = ep.calibrate.compute_Sv(ed)

    logger.info("Computing NASC...")
    ds_Sv_nasc = ep.commongrid.compute_NASC(
        ds_Sv,
        range_bin=range_bin,
        dist_bin=dist_bin,
        method=method,
        skipna=skipna,
        closed=closed,
        **(flox_kwargs if flox_kwargs else {})
    )


    # Step 4: Save back to NetCDF
    logger.info(f"Saving processed EchoData to {output_path} ...")


    ds_Sv_nasc_copy = clean_attrs(ds_Sv_nasc)
    ds_Sv_nasc = ds_Sv_nasc_copy  # Ensure we use the cleaned version
    #.to_netcdf(output_path, overwrite=True)
    #ed.ds_Sv_clean = Sv_clean  # Update EchoData with cleaned Sv
    ds_Sv_nasc.to_netcdf(output_path, mode="w", format="NETCDF4")
    logger.info("Processing complete.")



if __name__ == "__main__":
    
    main()
