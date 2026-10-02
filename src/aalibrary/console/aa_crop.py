#!/usr/bin/env python3
"""
aa-crop: PLACEHOLDER. Not installed as a command (no entry point in
pyproject.toml); run as ``python -m aalibrary.console.aa_crop``.

Meant to become: crop an echogram / EchoData to a (ping, range) window.

What it does today: loads a .raw or converted .nc/.netcdf4 file with echopype,
applies transform_echo_data(), which returns the EchoData unchanged, and writes
it to NetCDF (default <input stem>_processed.nc). The required --ping_num and
--range_sample_num, and --background_noise_max / --snr_threshold, are parsed
but not used: they were copied from the background-noise template this file
started from. It records no provenance and is not part of the shared console
core (no product hash, no reuse, no gs://).
"""

import argparse
import sys
from pathlib import Path

from loguru import logger
import echopype as ep  # make sure echopype is installed

from aalibrary.console._core import Help, ToolSpec, help_mode, render, stdio

# Help only: aa-crop is not wired into the core (no Run, no provenance).
SPEC = ToolSpec(name="aa-crop", role="utility", engines=())

HELP = Help(
    summary="PLACEHOLDER: copies EchoData unchanged. Not installed as a command.",
    does=(
        "Nothing scientific yet. It is meant to crop an echogram / EchoData to a "
        "(ping, range) window. Today it loads the input with echopype, applies "
        "transform_echo_data(), which returns the EchoData unchanged, and writes "
        "it to NetCDF. --ping_num, --range_sample_num, --background_noise_max and "
        "--snr_threshold are parsed but not used (left over from the "
        "background-noise template the file was copied from)."
    ),
    stdin="Does not read stdin. One local input path as the argument (.raw, .nc or .netcdf4).",
    stdout="The output path.",
    metadata="Records no provenance and computes no product hash.",
    options=[
        ("INPUT_PATH", "a converted EchoData .nc/.netcdf4 (or .raw; see NOTE)"),
        ("-o, --output_path PATH", "output file (default <input stem>_processed.nc)"),
        ("--ping_num N", "required, unused"),
        ("--range_sample_num N", "required, unused"),
        ("--background_noise_max X", "unused"),
        ("--snr_threshold DB", "unused (default 3.0)"),
    ],
    files="Local files only. Writes beside the input unless -o is given.",
    pipeline=(
        "Not installed as a command (there is no aa-crop entry point in "
        "pyproject.toml), so it is not part of any pipeline. Run it as "
        "python -m aalibrary.console.aa_crop."
    ),
    examples=[
        "python -m aalibrary.console.aa_crop x.nc --ping_num 1 --range_sample_num 1",
    ],
    notes=[
        ".raw input fails: echopype.open_raw is called without a sonar model. "
        "Re-saving a converted .nc can also fail with recent xarray versions "
        "(\"unexpected encoding parameters for 'netCDF4' backend\").",
    ],
)


def print_help():
    """Curated help (also used by the docs generator)."""
    sys.stdout.write(render(SPEC, HELP, _build_parser()))


def print_help_full():
    """The complete argparse reference (--help-all)."""
    _build_parser().print_help()


def _build_parser():
    parser = argparse.ArgumentParser(
        prog="aa-crop",
        description=(
            "PLACEHOLDER (not installed as a command): loads EchoData and writes "
            "it back unchanged. Meant to become a (ping, range) crop."
        ),
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
        help="Unused. (Number of pings to use for background noise removal.)",
    )

    parser.add_argument(
        "--range_sample_num",
        type=int,
        required=True,
        help="Unused. (Number of range samples to use for background noise removal.)",
    )

    parser.add_argument(
        "--background_noise_max",
        type=str,
        default=None,
        help="Unused. (Optional maximum background noise value.)",
    )

    parser.add_argument(
        "--snr_threshold",
        type=float,
        default=3.0,
        help="Unused. (SNR threshold in dB, default: 3.0.)",
    )
    return parser


def main():
    mode = help_mode()
    bare = len(sys.argv) == 1 and not stdio.stdin_is_piped()   # bare command on a terminal
    if bare or mode == "curated":
        print_help()
        sys.exit(0)
    if mode == "full":
        print_help_full()
        sys.exit(0)

    parser = _build_parser()
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
