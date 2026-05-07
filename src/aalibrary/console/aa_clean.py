#!/usr/bin/env python3
"""
aa-clean

Console tool for removing background noise from a Sv (volume backscattering
strength) NetCDF dataset using echopype.clean.remove_background_noise, and
saving the cleaned result back to NetCDF.

Pipeline-friendly: reads input path from positional arg or stdin, writes
output path to stdout, all logs to stderr.

Typical pipeline usage:
    aa-nc --sonar_model EK60 input.raw | aa-sv | aa-clean
"""

# === Silence logs BEFORE any heavy imports ===
import logging
import sys
import warnings

logging.disable(logging.CRITICAL)
warnings.filterwarnings("ignore")

from loguru import logger
logger.remove()
# Default sink: WARNING+ to stderr so real errors aren't swallowed.
# Without this, any exception in process_file disappears silently and
# the pipeline downstream gets no input — a confusing failure mode.
logger.add(sys.stderr, level="WARNING")

# Now the heavy imports — anything they log gets squashed
import argparse
import pprint
from pathlib import Path

import xarray as xr
from echopype.clean import remove_background_noise


def silence_all_logs():
    """Re-apply suppression in case a library re-enabled logging
    or added its own loguru sink during initialization."""
    logging.disable(logging.CRITICAL)
    for name in [None] + list(logging.root.manager.loggerDict):
        lg = logging.getLogger(name)
        lg.handlers.clear()
        lg.propagate = True
    logger.remove()
    logger.add(sys.stderr, level="WARNING")


def print_help():
    help_text = """
    Usage: aa-clean [OPTIONS] [INPUT_PATH]

    Arguments:
    INPUT_PATH                  Path to a Sv .nc / .netcdf4 file
                                (typically the output of aa-sv).
                                Optional. Defaults to stdin if not provided.

    Options:
    -o, --output_path           Path to save processed output.
                                Default: same directory as input, with
                                '_clean' appended to the stem and a
                                .nc suffix. Note: '_clean' is ALWAYS
                                appended, even when -o is given, so the
                                input file is never silently overwritten.

    --ping_num                  Number of pings to use for background
                                noise estimation.
                                Default: 20

    --range_sample_num          Number of range samples to use for background
                                noise estimation.
                                Default: 20

    --background_noise_max      Optional upper bound on the estimated
                                background noise, e.g. "-125dB". Pass with
                                the dB unit suffix.
                                Default: None (no cap).

    --snr_threshold             SNR threshold as a number in dB. The 'dB'
                                unit suffix is appended automatically before
                                handing off to echopype.
                                Default: 3.0

    Description:
    Removes background noise from a Sv NetCDF using
    echopype.clean.remove_background_noise. The expected input is the
    output of aa-sv (a flat NetCDF Sv dataset, NOT a multi-group EchoData
    file from aa-nc).

    Pipeline example:
        aa-nc --sonar_model EK60 input.raw | aa-sv | aa-clean

    Direct example:
        aa-clean /path/to/input_Sv.nc \\
                 --ping_num 50 --range_sample_num 200 \\
                 --snr_threshold 5.0 -o /path/to/output.nc
    """
    print(help_text)


def main():
    # Stdin / no-args handling
    if len(sys.argv) == 1:
        if not sys.stdin.isatty():
            stdin_data = sys.stdin.readline().strip()
            if stdin_data:
                sys.argv.append(stdin_data)
            else:
                # Stdin was piped but empty — bail with help instead of
                # falling through and crashing on Path("").exists().
                print_help()
                sys.exit(0)
        else:
            print_help()
            sys.exit(0)

    if "--help" in sys.argv or "-h" in sys.argv:
        print_help()
        sys.exit(0)

    parser = argparse.ArgumentParser(
        description="Remove background noise from a Sv NetCDF file with Echopype.",
        add_help=False,
    )

    parser.add_argument(
        "input_path",
        type=Path,
        nargs="?",
        help="Path to the Sv .nc / .netcdf4 file.",
    )
    parser.add_argument(
        "-o", "--output_path",
        type=Path,
        help="Path to save processed output. '_clean' is appended to the stem.",
    )
    parser.add_argument(
        "--ping_num",
        type=int,
        default=20,
        help="Number of pings to use for background noise estimation.",
    )
    parser.add_argument(
        "--range_sample_num",
        type=int,
        default=20,
        help="Number of range samples to use for background noise estimation.",
    )
    parser.add_argument(
        "--background_noise_max",
        type=str,
        default=None,
        help='Optional upper bound for background noise (e.g. "-125dB").',
    )
    parser.add_argument(
        "--snr_threshold",
        type=float,
        default=3.0,
        help="SNR threshold in dB (default: 3.0). 'dB' suffix added automatically.",
    )

    args = parser.parse_args()

    # ---------------------------
    # Validate input
    # ---------------------------
    if args.input_path is None:
        if sys.stdin.isatty():
            logger.error("No input path provided and no stdin available.")
            sys.exit(1)
        args.input_path = Path(sys.stdin.readline().strip())
        logger.info(f"Read input path from stdin: {args.input_path}")

    if not args.input_path.exists():
        logger.error(f"File '{args.input_path}' does not exist.")
        sys.exit(1)

    allowed_extensions = {".netcdf4", ".nc"}
    ext = args.input_path.suffix.lower()
    if ext not in allowed_extensions:
        logger.error(
            f"'{args.input_path.name}' is not a supported file type. "
            f"Allowed: {', '.join(sorted(allowed_extensions))}"
        )
        sys.exit(1)

    # ---------------------------
    # Resolve output path
    # ---------------------------
    if args.output_path is None:
        args.output_path = args.input_path

    args.output_path = args.output_path.with_stem(args.output_path.stem + "_clean")
    args.output_path = args.output_path.with_suffix(".nc")

    # Guard against clobbering the input — refuse rather than silently
    # corrupt the source file.
    if args.output_path.resolve() == args.input_path.resolve():
        logger.error(f"Refusing to overwrite input file: {args.input_path.resolve()}")
        sys.exit(1)

    # ---------------------------
    # Process file
    # ---------------------------
    try:
        args_summary = {
            "input_path": args.input_path,
            "output_path": args.output_path,
            "ping_num": args.ping_num,
            "range_sample_num": args.range_sample_num,
            "background_noise_max": args.background_noise_max,
            "snr_threshold": args.snr_threshold,
        }
        logger.debug(
            f"Executing aa-clean configured with [OPTIONS]:\n"
            f"{pprint.pformat(args_summary)}"
        )

        process_file(
            input_path=args.input_path,
            output_path=args.output_path,
            ping_num=args.ping_num,
            range_sample_num=args.range_sample_num,
            background_noise_max=args.background_noise_max,
            snr_threshold=args.snr_threshold,
        )

        logger.success(
            f"Generated {args.output_path.resolve()} with aa-clean. "
            "Passing .nc path to stdout..."
        )
        # Pipe the output path to stdout for the next tool
        print(args.output_path.resolve())

    except Exception as e:
        logger.exception(f"Error during processing: {e}")
        sys.exit(1)


def clean_attrs(ds):
    """Replace None-valued attrs with 'NA' so the dataset is NetCDF-safe.
    NetCDF attrs cannot be None — to_netcdf will raise on serialization.
    Using 'NA' (matching aa-sv) rather than 'NaN' to avoid implying the
    attribute was a missing numeric value."""
    for k, v in ds.attrs.items():
        if v is None:
            ds.attrs[k] = "NA"
    for var in ds.data_vars:
        for k, v in ds[var].attrs.items():
            if v is None:
                ds[var].attrs[k] = "NA"
    return ds


def process_file(
    input_path: Path,
    output_path: Path,
    ping_num: int,
    range_sample_num: int,
    background_noise_max: str = None,
    snr_threshold: float = 3.0,
):
    """Load a Sv NetCDF, remove background noise, and save the result."""

    logger.info(f"Loading Sv dataset from {input_path}")
    # Note: this expects a flat Sv dataset (output of aa-sv via
    # ds_Sv.to_netcdf), NOT a multi-group EchoData file (output of
    # aa-nc via ed.to_netcdf). xr.open_dataset would only see the root
    # group of the latter and remove_background_noise would fail.
    ds_Sv = xr.open_dataset(input_path)

    try:
        logger.info(
            f"Removing background noise "
            f"(ping_num={ping_num}, range_sample_num={range_sample_num}, "
            f"background_noise_max={background_noise_max}, "
            f"SNR_threshold={snr_threshold}dB)"
        )
        ds_Sv_clean = remove_background_noise(
            ds_Sv,
            ping_num=ping_num,
            range_sample_num=range_sample_num,
            background_noise_max=background_noise_max,
            SNR_threshold=f"{snr_threshold}dB",
        )

        ds_Sv_clean = clean_attrs(ds_Sv_clean)

        # Force-load before writing so we don't depend on the source
        # file handle still being open during to_netcdf's lazy compute.
        ds_Sv_clean.load()
    finally:
        ds_Sv.close()

    output_path = output_path.with_suffix(".nc")
    logger.info(f"Saving cleaned Sv dataset to {output_path}")
    ds_Sv_clean.to_netcdf(output_path)
    logger.success(f"Background noise removal complete: {output_path.resolve()}")


if __name__ == "__main__":
    main()