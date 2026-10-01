#!/usr/bin/env python3
"""
aa-depth

Console tool for adding a depth coordinate to an Echopype Sv NetCDF file
(echopype.consolidate.add_depth).

Pipeline-friendly: reads the input path (or gs:// URI) from the positional
argument or stdin, writes the output path to stdout, all logs to stderr.
The output carries the input's provenance plus this step; an --echodata
file is recorded as a second input (role "echodata").
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
logger.add(sys.stderr, level="WARNING")

import argparse
import pprint
from pathlib import Path
from typing import Optional

from aalibrary.console._core import (
    Help, Run, ToolSpec, add_common_flags, canon, naming, render, show_help, stdio, uris,
)

# xarray / echopype are imported inside process_file so --help stays fast.

SPEC = ToolSpec(
    name="aa-depth",
    role="transform",
    kind="sv",
    op="echopype.consolidate.add_depth",
    op_version=1,
    params={
        "depth_offset": canon.number,
        "tilt": canon.number,
        "downward": canon.boolean,
        "use_platform_vertical_offsets": canon.boolean,
        "use_platform_angles": canon.boolean,
        "use_beam_angles": canon.boolean,
    },
)

HELP = Help(
    summary="Add a depth variable to an Sv dataset (echopype.consolidate.add_depth).",
    does=(
        "Adds 'depth' (m; channel x ping_time x range_sample) to the Sv dataset: "
        "depth = transducer depth + echo_range x cos(tilt); with --no-downward "
        "the echo_range term is subtracted instead (transducer depth - "
        "echo_range x cos(tilt)). Transducer depth is --depth-offset, else the "
        "Platform vertical offsets of --echodata (with "
        "--use-platform-vertical-offsets), else 0. The tilt is --tilt, else the "
        "Platform or Beam angles of --echodata (--use-platform-angles / "
        "--use-beam-angles), else 0. An explicit --depth-offset or --tilt always "
        "wins over the corresponding --use-* flag. Every input variable is kept "
        "unchanged; the product kind is the input's (sv, mvbs, ...)."
    ),
    stdin="One Sv .nc/.netcdf4 path or gs:// URI (aa-sv output, or anything with echo_range).",
    stdout="The output file's absolute path (or gs:// URI).",
    options=[
        ("-o, --output_path PATH", "Explicit output, used as given; '.nc' is added only "
                                   "when it has no suffix. Local path or gs:// URI."),
        ("--depth-offset M", "transducer depth below the surface, in metres"),
        ("--tilt DEG", "transducer tilt from vertical, in degrees"),
        ("--no-downward", "upward-looking transducers (default: downward)"),
        ("--echodata ED.nc", "the EchoData (aa-nc output) the Sv came from; needed by "
                             "the --use-* options. Its content enters the product hash."),
        ("--use-platform-vertical-offsets", "transducer depth from Platform (EK60/EK80)"),
        ("--use-platform-angles | --use-beam-angles",
         "tilt from Platform or Beam angles (EK60/EK80; not both)"),
    ],
    science={
        "depth_offset": "Transducer depth (m). Overrides the Platform vertical offsets.",
        "tilt": "Tilt from vertical (degrees). Overrides Platform/Beam angles.",
        "downward": ("Downward-looking by default; --no-downward (upward-looking) "
                     "subtracts the echo_range term from the transducer depth."),
        "use_platform_vertical_offsets": "Transducer depth from the EchoData Platform group.",
        "use_platform_angles": "Tilt from the EchoData Platform group angles.",
        "use_beam_angles": "Tilt from the EchoData Beam group angles.",
        "echodata": "EchoData file: its content identity (not its path) enters the hash.",
    },
    files=(
        "Reads Sv .nc/.netcdf4 and an optional EchoData .nc/.netcdf4/.zarr, local "
        "or gs://. Writes <base>_<hash>.nc beside the input (current directory "
        "for gs:// input), or -o, or --dest. An identical earlier result is "
        "reused. AA_NAMING=legacy: <stem>_depth.nc. Refuses to overwrite the "
        "input or the --echodata file."
    ),
    pipeline=(
        "After aa-sv, before tools that need depth (aa-detect-seafloor): "
        "aa-nc | aa-sv | aa-depth | ..."
    ),
    examples=[
        "aa-nc D20160703-T060000.raw --sonar_model EK60 | aa-sv | aa-depth --depth-offset 5",
        "aa-depth Sv.nc --echodata D20160703-T060000.nc --use-platform-vertical-offsets",
    ],
)


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
    """Curated help (also used by the docs generator)."""
    sys.stdout.write(render(SPEC, HELP, _build_parser()))


def print_help_full():
    help_text = """
    Usage: aa-depth [OPTIONS] [INPUT_PATH]

    Arguments:
    INPUT_PATH                 Path (or gs:// URI) to the .nc / .netcdf4 file
                               containing Sv. Optional. Defaults to stdin if not
                               provided.

    Options:
    -o, --output_path          Path (or gs:// URI) to save processed output. If
                               provided, it is used as-is (a .nc suffix is added
                               only when missing). If omitted, defaults to
                               <base>_<hash>.nc beside the input
                               (AA_NAMING=legacy: the input stem with '_depth'
                               appended and a .nc suffix).

    --depth-offset             Offset (meters) along depth to account for transducer
                               position in water. Default: None (transducer at the
                               surface). If set, Platform vertical offsets are ignored.

    --tilt                     Transducer tilt angle in degrees (0 = vertical).
                               Default: None. If set, Platform/Beam angles are ignored.

    --downward / --no-downward Whether transducers point downward.
                               Default: --downward (True). With --no-downward
                               depth = transducer depth - echo_range*cos(tilt)
                               (only the echo_range term changes sign).

    --echodata                 Path (or gs:// URI) to the converted EchoData file
                               (.nc/.netcdf4/.zarr) that the Sv dataset originated
                               from. Required when using any of the --use-* options
                               below. Its identity is recorded as a second input
                               and enters the product hash.

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

    --base NAME                Base name for the output.
    --dest DIR|gs://PREFIX     Write the default-named output there.
    --force                    Recompute even if an identical product exists.

    Description:
    Loads a NetCDF Sv dataset, adds a depth coordinate via
    echopype.consolidate.add_depth, and writes the result to a new
    .nc file. The output path is printed to stdout for piping.
    Provenance (the input's chain plus this step) is embedded in the
    output; see aa-metadata.

    Example:
    aa-depth /path/to/input_Sv.nc --depth-offset 1.5 --tilt 5
    aa-depth /path/to/input_Sv.nc --echodata /path/to/converted.zarr \\
             --use-platform-vertical-offsets --use-beam-angles
    """
    print(help_text)


def _build_parser():
    parser = argparse.ArgumentParser(
        description="Add a depth coordinate to an Echopype Sv NetCDF file.",
        add_help=False,  # help is handled by show_help()
    )

    parser.add_argument(
        "input_path",
        type=str,
        nargs="?",
        help="Path or gs:// URI of the .nc or .netcdf4 file.",
    )
    parser.add_argument(
        "-o", "--output_path",
        type=str,
        help=(
            "Path to save processed output. Used as-is if provided (.nc added "
            "only when there is no suffix)."
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

    # --- Parameters mirroring the current echopype.consolidate.add_depth API ---
    parser.add_argument(
        "--echodata",
        type=str,
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
    add_common_flags(parser)
    return parser


def _inherited_kind(src) -> str:
    """The input's product kind (sv, mvbs, mask ...): this step keeps what the data are.

    Falls back to SPEC.kind ("sv") for inputs without provenance, and for
    EchoData or fetched source files, whose kinds describe a different
    representation.
    """
    kind = (((src.prov or {}).get("product") or {}).get("kind") or "").strip()
    return kind if kind and kind not in {"echodata", "source"} else SPEC.kind


def _explicit_output(value: Optional[str]) -> Optional[str]:
    """-o as given; '.nc' only when the name has no suffix (the tool's old rule)."""
    if not value:
        return None
    name = value.rstrip("/").rsplit("/", 1)[-1]
    return value if Path(name).suffix else naming.with_ext(value, ".nc")


def main():
    # No args on a terminal: help. (An empty pipe is an error: see stdio.)
    if len(sys.argv) == 1 and not stdio.stdin_is_piped():
        print_help()
        sys.exit(0)

    parser = _build_parser()
    if show_help(SPEC, HELP, parser, full=print_help_full):
        sys.exit(0)

    args = parser.parse_args()

    # ---------------------------
    # Validate input
    # ---------------------------
    token = stdio.one_input(args.input_path, SPEC.name)
    run = Run(SPEC, args)
    src = run.input(token)

    allowed_extensions = {".netcdf4": "netcdf", ".nc": "netcdf"}
    ext = src.local.suffix.lower()
    if ext not in allowed_extensions:
        logger.error(
            f"'{src.name}' is not a supported file type. "
            f"Allowed: {', '.join(allowed_extensions.keys())}"
        )
        sys.exit(1)

    # ---------------------------
    # Validate EchoData + option combinations
    # ---------------------------
    ed = None
    if args.echodata is not None:
        missing = f"EchoData file '{args.echodata}' does not exist."
        if not uris.is_gcs(args.echodata) and not Path(args.echodata).expanduser().exists():
            logger.error(missing)
            sys.exit(1)
        # A file-valued scientific option: its identity enters the hash.
        try:
            ed = run.input(args.echodata, role="echodata")
        except FileNotFoundError:          # a gs:// object that is not there
            logger.error(missing)
            sys.exit(1)
        ed_ext = ed.local.suffix.lower()
        allowed_ed = {".nc", ".netcdf4", ".zarr"}
        if ed_ext not in allowed_ed:
            logger.error(
                f"'{ed.name}' is not a supported EchoData type. "
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
    out = run.plan(
        ext=".nc",
        kind=_inherited_kind(src),
        explicit=_explicit_output(args.output_path),
        legacy=lambda: src.local.with_stem(src.local.stem + "_depth").with_suffix(".nc"),
    )

    # Guard against clobbering files we read from.
    if not out.remote:
        out_resolved = Path(out.target).resolve()
        if out_resolved == src.local.resolve():
            logger.error(f"Refusing to overwrite input file: {src.local.resolve()}")
            sys.exit(1)
        if ed is not None and out_resolved == ed.local.resolve():
            logger.error(f"Refusing to overwrite EchoData file: {ed.local.resolve()}")
            sys.exit(1)

    if run.reusable(out):
        run.finish(out)
        return

    # ---------------------------
    # Process file
    # ---------------------------
    try:
        args_summary = dict(vars(args), output=out.target, product=out.hash)
        logger.debug(f"\naa-depth args:\n{pprint.pformat(args_summary)}")

        process_file(
            input_path=src.local,
            output_path=out.local,
            depth_offset=args.depth_offset,
            tilt=args.tilt,
            downward=args.downward,
            echodata_path=ed.local if ed is not None else None,
            use_platform_vertical_offsets=args.use_platform_vertical_offsets,
            use_platform_angles=args.use_platform_angles,
            use_beam_angles=args.use_beam_angles,
        )

        logger.success(f"Desired data generated and saved to\n\t{out.target}")
        run.finish(out)

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
    import xarray as xr
    import echopype as ep
    from echopype.consolidate import add_depth

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
        echodata = ep.open_converted(str(echodata_path))

    # An explicit --depth-offset / --tilt takes precedence over the EchoData
    # Platform/Beam values, as documented. echopype 0.11 add_depth does not
    # enforce that itself: it sets the explicit value and then still runs the
    # Platform/Beam branch, which overwrites it (an explicit offset was
    # ignored; --tilt with --use-beam-angles gave all-NaN depth on EK60).
    # So the overridden --use-* flags are simply not passed on.
    ds_Sv = add_depth(
        ds_Sv,
        echodata=echodata,
        depth_offset=depth_offset,
        tilt=tilt,
        downward=downward,
        use_platform_vertical_offsets=use_platform_vertical_offsets and depth_offset is None,
        use_platform_angles=use_platform_angles and tilt is None,
        use_beam_angles=use_beam_angles and tilt is None,
    )

    # Written anyway (as before), but say so: e.g. EK60 files carry no Beam
    # angles, so --use-beam-angles yields NaN everywhere.
    if bool(ds_Sv["depth"].isnull().all()):
        hint = ""
        if (use_platform_vertical_offsets and depth_offset is None) or (
                (use_platform_angles or use_beam_angles) and tilt is None):
            hint = (" The EchoData Platform/Beam values used by the --use-* options are "
                    "missing (NaN) in this file; EK60 files often record a zero beam "
                    "direction, which echopype stores as NaN. Consider --depth-offset / "
                    "--tilt instead.")
        logger.warning(f"every 'depth' value is NaN.{hint}")

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    ds_Sv.to_netcdf(output_path)


if __name__ == "__main__":
    main()
