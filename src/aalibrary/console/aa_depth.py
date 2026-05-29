#!/usr/bin/env python3
"""
Console tool for adding a depth coordinate to an Echopype Sv NetCDF file.
"""

# === Silence logs BEFORE any heavy imports ===
import logging
import sys
import warnings

logging.disable(logging.CRITICAL)
warnings.filterwarnings("ignore")

from loguru import logger
logger.remove()
# Keep WARNING+ visible on stderr so real errors aren't swallowed.
# Drop this line if you want truly silent output.
logger.add(sys.stderr, level="WARNING")

# Now the heavy imports — anything they log gets squashed
import argparse
import pprint
from pathlib import Path
from typing import Optional

import xarray as xr
import echopype as ep  # used for ep.open_converted when --echodata is supplied
from echopype.consolidate import add_depth


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
    Usage: aa-depth [OPTIONS] [INPUT_PATH]

    Arguments:
    INPUT_PATH                 Path to the .nc / .netcdf4 file containing Sv.
                               Optional. Defaults to stdin if not provided.

    Options:
    -o, --output_path          Path to save processed output. If provided, it is
                               used as-is (a .nc suffix is added only when missing).
                               If omitted, defaults to the input path with '_depth'
                               appended to the stem and a .nc suffix.

    --depth-offset             Offset (meters) along depth to account for transducer
                               position in water. Default: None (transducer at the
                               surface). If set, Platform vertical offsets are ignored.

    --tilt                     Transducer tilt angle in degrees (0 = vertical).
                               Default: None. If set, Platform/Beam angles are ignored.

    --downward / --no-downward Whether transducers point downward.
                               Default: --downward (True).

    --echodata                 Path to the converted EchoData file (.nc/.netcdf4/.zarr)
                               that the Sv dataset originated from. Required when using
                               any of the --use-* options below.

    --use-platform-vertical-offsets
                               Use the EchoData Platform group vertical offsets to
                               compute transducer depth (EK60/EK80 only). Ignored if
                               --depth-offset is given.

    --use-platform-angles      Use the EchoData Platform group angles to scale
                               echo_range (EK60/EK80 only). Ignored if --tilt is given.
                               Cannot be combined with --use-beam-angles.

    --use-beam-angles          Use the EchoData Beam group angles to scale echo_range
                               (EK60/EK80 only). Ignored if --tilt is given.
                               Cannot be combined with --use-platform-angles.

    Description:
    Loads a NetCDF Sv dataset, adds a depth coordinate via
    echopype.consolidate.add_depth, and writes the result to a new
    .nc file. The output path is printed to stdout for piping.

    Example:
    aa-depth /path/to/input_Sv.nc --depth-offset 1.5 --tilt 5
    aa-depth /path/to/input_Sv.nc --echodata /path/to/converted.zarr \\
             --use-platform-vertical-offsets --use-beam-angles
    """
    print(help_text)


def main():
    # If no args and stdin has data, treat the first stdin line as the input path
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
        description="Add a depth coordinate to an Echopype Sv NetCDF file.",
        add_help=False,  # we handle help ourselves above
    )

    parser.add_argument(
        "input_path",
        type=Path,
        nargs="?",
        help="Path to the .nc or .netcdf4 file.",
    )
    parser.add_argument(
        "-o", "--output_path",
        type=Path,
        help=(
            "Path to save processed output. Used as-is if provided; "
            "otherwise defaults to the input stem with '_depth' appended."
        ),
    )
    parser.add_argument(
        "--depth-offset",
        type=float,
        default=None,
        help=(
            "Offset (m) along depth for transducer position in water "
            "(default: None = surface). Overrides Platform vertical offsets if set."
        ),
    )
    parser.add_argument(
        "--tilt",
        type=float,
        default=None,
        help=(
            "Transducer tilt angle in degrees, 0 = vertical (default: None). "
            "Overrides Platform/Beam angles if set."
        ),
    )
    parser.add_argument(
        "--downward",
        action="store_true",
        default=True,
        help="Transducers point downward (default: True). Use --no-downward to disable.",
    )
    parser.add_argument(
        "--no-downward",
        dest="downward",
        action="store_false",
        help=argparse.SUPPRESS,
    )

    # --- New parameters mirroring the current echopype.consolidate.add_depth API ---
    parser.add_argument(
        "--echodata",
        type=Path,
        default=None,
        help=(
            "Path to the converted EchoData file (.nc/.netcdf4/.zarr) the Sv "
            "originated from. Required for the --use-* options."
        ),
    )
    parser.add_argument(
        "--use-platform-vertical-offsets",
        action="store_true",
        default=False,
        help=(
            "Use EchoData Platform vertical offsets to compute transducer depth "
            "(EK60/EK80 only). Ignored if --depth-offset is given."
        ),
    )
    parser.add_argument(
        "--use-platform-angles",
        action="store_true",
        default=False,
        help=(
            "Use EchoData Platform angles to scale echo_range (EK60/EK80 only). "
            "Ignored if --tilt is given. Cannot be combined with --use-beam-angles."
        ),
    )
    parser.add_argument(
        "--use-beam-angles",
        action="store_true",
        default=False,
        help=(
            "Use EchoData Beam angles to scale echo_range (EK60/EK80 only). "
            "Ignored if --tilt is given. Cannot be combined with --use-platform-angles."
        ),
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

    allowed_extensions = {".netcdf4": "netcdf", ".nc": "netcdf"}
    ext = args.input_path.suffix.lower()
    if ext not in allowed_extensions:
        logger.error(
            f"'{args.input_path.name}' is not a supported file type. "
            f"Allowed: {', '.join(allowed_extensions.keys())}"
        )
        sys.exit(1)

    # ---------------------------
    # Validate EchoData + option combinations
    # ---------------------------
    if args.echodata is not None:
        if not args.echodata.exists():
            logger.error(f"EchoData file '{args.echodata}' does not exist.")
            sys.exit(1)
        ed_ext = args.echodata.suffix.lower()
        allowed_ed = {".nc", ".netcdf4", ".zarr"}
        if ed_ext not in allowed_ed:
            logger.error(
                f"'{args.echodata.name}' is not a supported EchoData type. "
                f"Allowed: {', '.join(sorted(allowed_ed))}"
            )
            sys.exit(1)

    needs_echodata = (
        args.use_platform_vertical_offsets
        or args.use_platform_angles
        or args.use_beam_angles
    )
    if needs_echodata and args.echodata is None:
        logger.error(
            "--use-platform-vertical-offsets, --use-platform-angles, and "
            "--use-beam-angles require --echodata to be provided."
        )
        sys.exit(1)

    # Per echopype: platform and beam angles cannot be used in tandem.
    if args.use_platform_angles and args.use_beam_angles:
        logger.error(
            "--use-platform-angles and --use-beam-angles cannot be used together."
        )
        sys.exit(1)

    # Soft warnings: explicit values override the corresponding Platform/Beam data.
    if args.depth_offset is not None and args.use_platform_vertical_offsets:
        logger.warning(
            "Both --depth-offset and --use-platform-vertical-offsets given; "
            "the explicit --depth-offset takes precedence."
        )
    if args.tilt is not None and (args.use_platform_angles or args.use_beam_angles):
        logger.warning(
            "Both --tilt and platform/beam angles given; "
            "the explicit --tilt takes precedence."
        )

    # ---------------------------
    # Resolve output path
    # ---------------------------
    if args.output_path is None:
        # Derive from input: append '_depth' to the stem and force a .nc suffix.
        args.output_path = args.input_path.with_stem(
            args.input_path.stem + "_depth"
        ).with_suffix(".nc")
    elif args.output_path.suffix == "":
        # Respect an explicit path; only default the suffix when one is missing.
        args.output_path = args.output_path.with_suffix(".nc")

    # Guard against clobbering files we read from.
    out_resolved = args.output_path.resolve()
    if out_resolved == args.input_path.resolve():
        logger.error(f"Refusing to overwrite input file: {args.input_path.resolve()}")
        sys.exit(1)
    if args.echodata is not None and out_resolved == args.echodata.resolve():
        logger.error(f"Refusing to overwrite EchoData file: {args.echodata.resolve()}")
        sys.exit(1)

    # ---------------------------
    # Process file
    # ---------------------------
    try:
        pretty_args = pprint.pformat(vars(args))
        logger.debug(f"\naa-depth args:\n{pretty_args}")

        process_file(
            input_path=args.input_path,
            output_path=args.output_path,
            depth_offset=args.depth_offset,
            tilt=args.tilt,
            downward=args.downward,
            echodata_path=args.echodata,
            use_platform_vertical_offsets=args.use_platform_vertical_offsets,
            use_platform_angles=args.use_platform_angles,
            use_beam_angles=args.use_beam_angles,
        )

        logger.success(f"Desired data generated and saved to\n\t{args.output_path.resolve()}")
        logger.success("Piping saved .nc path to stdout ⟶")
        print(args.output_path.resolve())

    except Exception as e:
        logger.exception(f"Error during processing: {e}")
        sys.exit(1)


def process_file(
    input_path: Path,
    output_path: Path,
    depth_offset: Optional[float] = None,
    tilt: Optional[float] = None,
    downward: bool = True,
    echodata_path: Optional[Path] = None,
    use_platform_vertical_offsets: bool = False,
    use_platform_angles: bool = False,
    use_beam_angles: bool = False,
):
    """
    Load Sv from NetCDF, add a depth coordinate, and save back to NetCDF.
    """
    logger.info(f"Loading NetCDF file {input_path} into xarray dataset")

    # Open into memory then close the file handle so we can write to a path
    # in the same directory without xarray holding a read lock.
    with xr.open_dataset(input_path) as ds_in:
        ds_Sv = ds_in.load()

    # Open the source EchoData object only when requested. Kept in scope through
    # to_netcdf so any lazily-read Platform/Beam values resolve during the write.
    echodata = None
    if echodata_path is not None:
        logger.info(f"Opening EchoData file {echodata_path}")
        echodata = ep.open_converted(echodata_path)

    ds_Sv = add_depth(
        ds_Sv,
        echodata=echodata,
        depth_offset=depth_offset,
        tilt=tilt,
        downward=downward,
        use_platform_vertical_offsets=use_platform_vertical_offsets,
        use_platform_angles=use_platform_angles,
        use_beam_angles=use_beam_angles,
    )

    ds_Sv.to_netcdf(output_path)


if __name__ == "__main__":
    main()