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


def print_help():
    help_text = ""



def main():

    signal.signal(signal.SIGPIPE, signal.SIG_DFL)

    # Display help if no arguments are provided or if --help is explicitly passed
    if len(sys.argv) == 1 or "--help" in sys.argv:
        print_help()
        sys.exit(0)
    
    parser = argparse.ArgumentParser(
        description="Compute Mean Volume Backscattering Strength (MVBS) from an Sv dataset using Echopype."
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
        help="Path to save processed MVBS output. Default overwrites input .nc with MVBS group or creates a new .nc."
    )

    # ---------------------------
    # compute_MVBS arguments
    # ---------------------------
    parser.add_argument(
        "--range_var",
        type=str,
        choices=["echo_range", "depth"],
        default="echo_range",
        help="Range coordinate to bin over (default: echo_range)."
    )

    parser.add_argument(
        "--range_bin",
        type=str,
        default="20m",
        help="Bin size along range dimension (default: 20m)."
    )

    parser.add_argument(
        "--ping_time_bin",
        type=str,
        default="20s",
        help="Bin size along ping_time dimension (default: 20s)."
    )

    parser.add_argument(
        "--method",
        type=str,
        choices=["map-reduce", "coarsen", "block"],
        default="map-reduce",
        help="Computation method for binning (default: map-reduce)."
    )

    parser.add_argument(
        "--reindex",
        action="store_true",
        help="If set, reindex the result to match uniform bin edges (default: False)."
    )

    parser.add_argument(
        "--skipna",
        action="store_true",
        help="Skip NaN values when averaging (default: True)."
    )

    parser.add_argument(
        "--fill_value",
        type=float,
        default=math.nan,
        help="Fill value for empty bins (default: NaN)."
    )

    parser.add_argument(
        "--closed",
        type=str,
        choices=["left", "right"],
        default="left",
        help="Which side of bins are closed (default: left)."
    )

    parser.add_argument(
        "--range_var_max",
        type=str,
        default=None,
        help="Optional maximum value for range_var (default: None)."
    )

    # flox_kwargs could be passed as key=value pairs, but often users don’t need it.
    parser.add_argument(
        "--flox_kwargs",
        nargs="*",
        help="Optional advanced arguments for flox (format: key=value)."
    )

    args = parser.parse_args()

    # ---------------------------
    # Validate input
    # ---------------------------

    if args.input_path is None:
        # Read from stdin
        
        args.input_path = Path(sys.stdin.read().strip())
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
    try:
        process_file(
            input_path=args.input_path,
            output_path=args.output_path,
            file_type=file_type,
            range_var=args.range_var,
            range_bin=args.range_bin,
            ping_time_bin=args.ping_time_bin,
            method=args.method,
            reindex=args.reindex,
            skipna=args.skipna,
            fill_value=args.fill_value,
            closed=args.closed,
            range_var_max=args.range_var_max,
            flox_kwargs=(
                dict(kv.split("=", 1) for kv in args.flox_kwargs)
                if args.flox_kwargs else {}
            ),
        )
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
    file_type: str = "netcdf",
    range_var: str = "echo_range",
    range_bin: str = "20m",
    ping_time_bin: str = "20s",
    method: str = "map-reduce",
    reindex: bool = False,
    skipna: bool = True,
    fill_value: float = float("nan"),
    closed: str = "left",
    range_var_max: str = None,
    flox_kwargs: dict = None,
):
    
    
    
    
    """
    Load EchoData from RAW or NetCDF, remove background noise, apply transformations, and save to NetCDF.
    """
    # Step 1: Load file into EchoData object
    if file_type == "raw":
        logger.info(f"Loading RAW file {input_path} into EchoData...")
        ed = ep.open_raw(input_path)  # add sonar_type if needed
    elif file_type == "netcdf":
        logger.info(f"Loading NetCDF file {input_path} into EchoData...")
        ed = ep.open_converted(input_path)
        #print(ed)


    # Step 3: Apply any additional transformation
    logger.info("Applying mean volume backscattering strength (MVBS) transformations to EchoData...")
    #Sv = ep.calibrate.compute_Sv(ed)
    Sv_mvbs = transform_to_mvbs(
        ed=ed,
        range_var=range_var,
        range_bin=range_bin,
        ping_time_bin=ping_time_bin,
        method=method,
        reindex=reindex,
        skipna=skipna,
        fill_value=fill_value,
        closed=closed,
        range_var_max=range_var_max,
        **(flox_kwargs if flox_kwargs else {})
    )


    # Step 4: Save back to NetCDF
    logger.info(f"Saving processed EchoData to {output_path} ...")


    Sv_mvbs_copy = clean_attrs(Sv_mvbs)
    Sv_mvbs = Sv_mvbs_copy  # Ensure we use the cleaned version
    #.to_netcdf(output_path, overwrite=True)
    #ed.ds_Sv_clean = Sv_clean  # Update EchoData with cleaned Sv
    
    tmp_path = output_path.with_suffix(".tmp.nc")
    Sv_mvbs.to_netcdf(tmp_path)
    logger.info("Processing complete.")


def transform_to_mvbs(
    ed: ep.echodata,
    range_var: str = "echo_range",
    range_bin: str = "20m",
    ping_time_bin: str = "20s",
    method: str = "map-reduce",
    reindex: bool = False,
    skipna: bool = True,
    fill_value: float = math.nan,
    closed: str = "left",
    range_var_max: str = None,
    **flox_kwargs
):
    """
    Compute MVBS from an EchoData object.

    Parameters
    ----------
    ed : ep.echodata
        EchoData object containing calibrated Sv data.
    range_var : {"echo_range", "depth"}, default "echo_range"
        Range coordinate to bin over.
    range_bin : str, default "20m"
        Bin size along range dimension.
    ping_time_bin : str, default "20s"
        Bin size along ping_time dimension.
    method : {"map-reduce", "coarsen", "block"}, default "map-reduce"
        Method for binning.
    reindex : bool, default False
        Reindex the result to uniform bin edges.
    skipna : bool, default True
        Skip NaN values during averaging.
    fill_value : float, default NaN
        Fill value for empty bins.
    closed : {"left", "right"}, default "left"
        Which side of the interval is closed.
    range_var_max : str, optional
        Maximum value for the range variable.
    **flox_kwargs
        Additional keyword arguments passed to flox.

    Returns
    -------
    xarray.Dataset
        Dataset containing MVBS.
    """
    logger.info("Calibrating EchoData to Sv...")
    ds_Sv = ep.calibrate.compute_Sv(ed)

    logger.info("Computing MVBS...")
    Sv_mvbs = ep.commongrid.compute_MVBS(
        ds_Sv,
        range_var=range_var,
        range_bin=range_bin,
        ping_time_bin=ping_time_bin,
        method=method,
        reindex=reindex,
        skipna=skipna,
        fill_value=fill_value,
        closed=closed,
        range_var_max=range_var_max,
        **flox_kwargs
    )

    return Sv_mvbs


if __name__ == "__main__":
    main()
