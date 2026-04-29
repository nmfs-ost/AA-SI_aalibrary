#!/usr/bin/env python3
"""
aa-sv

Console tool for computing Sv (volume backscattering strength) from a .nc
EchoData file (typically the output of aa-nc) using Echopype, and saving
back to NetCDF.

Pipeline-friendly: reads input path from positional arg or stdin, writes
output path to stdout, all logs to stderr.
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

# Now the heavy imports - anything they log gets squashed
import argparse
import pprint
from pathlib import Path

import echopype as ep


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
    Usage: aa-sv [OPTIONS] [INPUT_PATH]

    Arguments:
    INPUT_PATH                  Path to the .nc / .netcdf4 EchoData file.
                                Optional. Defaults to stdin if not provided.

    Options:
    -o, --output_path           Path to save processed output.
                                Default: same directory as input, with '_Sv'
                                appended to the stem and a .nc suffix.

    --waveform_mode             For EK80 echosounders ONLY: waveform mode.
                                Choices: CW, BB, FM
                                Default: not passed (echopype picks per-sonar).

    --encode_mode               For EK80 echosounders ONLY: encoding mode.
                                Choices: complex, power
                                Default: not passed (echopype picks per-sonar).

    Description:
    This tool computes Sv (volume backscattering strength) from a previously-
    converted NetCDF EchoData file using echopype.calibrate.compute_Sv, and
    saves the result to a new .nc file. The output path is printed to stdout
    for piping into the next stage of the pipeline.

    For visualization, pipe the output into aa-plot:
        aa-nc --sonar_model EK60 input.raw | aa-sv | aa-plot

    Example:
        aa-sv /path/to/input.nc --waveform_mode FM --encode_mode power \\
              -o /path/to/output.nc
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
                print_help()
                sys.exit(0)
        else:
            print_help()
            sys.exit(0)

    if "--help" in sys.argv or "-h" in sys.argv:
        print_help()
        sys.exit(0)

    parser = argparse.ArgumentParser(
        description="Compute Sv from a NetCDF EchoData file with Echopype.",
        add_help=False,
    )

    parser.add_argument(
        "input_path",
        type=Path,
        nargs="?",
        help="Path to the .nc / .netcdf4 EchoData file.",
    )
    parser.add_argument(
        "-o", "--output_path",
        type=Path,
        help="Path to save processed output. Default appends '_Sv' to the input stem.",
    )
    parser.add_argument(
        "--waveform_mode",
        type=str,
        default=None,
        choices=["CW", "BB", "FM"],
        help="For EK80 Echosounders ONLY: waveform mode. Omit for EK60.",
    )
    parser.add_argument(
        "--encode_mode",
        type=str,
        default=None,
        choices=["complex", "power"],
        help="For EK80 Echosounders ONLY: encoding mode. Omit for EK60.",
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

    args.output_path = args.output_path.with_stem(args.output_path.stem + "_Sv")
    args.output_path = args.output_path.with_suffix(".nc")

    # Guard against clobbering the input
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
            "waveform_mode": args.waveform_mode,
            "encode_mode": args.encode_mode,
        }
        logger.debug(
            f"Executing aa-sv configured with [OPTIONS]:\n"
            f"{pprint.pformat(args_summary)}"
        )

        process_file(
            input_path=args.input_path,
            output_path=args.output_path,
            waveform_mode=args.waveform_mode,
            encode_mode=args.encode_mode,
        )

        logger.success(
            f"Generated {args.output_path.resolve()} with aa-sv. "
            "Passing .nc path to stdout..."
        )
        # Pipe the output path to stdout for the next tool
        print(args.output_path.resolve())

    except Exception as e:
        logger.exception(f"Error during processing: {e}")
        sys.exit(1)


def clean_attrs(ds):
    """Replace None-valued attrs with 'NA' so the dataset is NetCDF-safe.
    NetCDF attrs cannot be None — to_netcdf will raise on serialization."""
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
    waveform_mode=None,
    encode_mode=None,
):
    """Load EchoData from NetCDF, compute Sv, and save to NetCDF."""

    logger.info(f"Loading EchoData from {input_path}")
    ed = ep.open_converted(input_path)

    # Build kwargs lazily — only pass waveform_mode / encode_mode when the
    # user explicitly provided them. echopype's compute_Sv treats these as
    # EK80-only; passing CW/complex unconditionally to an EK60 dataset
    # raises an error. The previous version of this script always passed
    # them, which is why EK60 pipelines silently failed.
    compute_kwargs = {}
    if waveform_mode is not None:
        compute_kwargs["waveform_mode"] = waveform_mode
    if encode_mode is not None:
        compute_kwargs["encode_mode"] = encode_mode

    if compute_kwargs:
        logger.info(f"Computing Sv (EK80 mode: {compute_kwargs})")
    else:
        logger.info("Computing Sv (using echopype defaults for this sonar)")

    ds_Sv = ep.calibrate.compute_Sv(ed, **compute_kwargs)
    ds_Sv = clean_attrs(ds_Sv)

    output_path = output_path.with_suffix(".nc")
    logger.info(f"Saving Sv dataset to {output_path}")
    ds_Sv.to_netcdf(output_path)
    logger.success(f"Sv computation complete: {output_path.resolve()}")


if __name__ == "__main__":
    main()