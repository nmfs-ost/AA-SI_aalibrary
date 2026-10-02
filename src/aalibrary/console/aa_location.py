#!/usr/bin/env python3
"""
aa-location

Console tool for adding geographical location (latitude/longitude) to an Sv
dataset using Echopype's consolidate.add_location.

This wraps:
  echopype.consolidate.add_location(
      ds, echodata, datagram_type=None, nmea_sentence=None
  )

It interpolates platform location (lat/lon) from the EchoData Platform/NMEA
records to the acoustic ping_time of the Sv dataset.

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
# Default sink: WARNING+ to stderr so real errors aren't swallowed.
logger.add(sys.stderr, level="WARNING")

import argparse
import io
import pprint
from contextlib import redirect_stdout
from pathlib import Path

from aalibrary.console._core import (
    Help, Run, ToolSpec, add_common_flags, canon, render, show_help, stdio, uris,
)

# xarray / echopype are imported inside process_file so --help stays fast.

SPEC = ToolSpec(
    name="aa-location",
    role="transform",
    kind="sv",
    op="echopype.consolidate.add_location",
    op_version=1,
    params={
        # --datagram-type is upper-cased by argparse (echopype only knows the
        # exact "MRU1"/"IDX"); the NMEA sentence is matched exactly: no folding.
        "datagram_type": canon.choice("upper"),
        "nmea_sentence": canon.text,
    },
)

HELP = Help(
    summary="Add latitude/longitude to an Sv dataset (echopype.consolidate.add_location).",
    does=(
        "Interpolates the platform position from the EchoData Platform group "
        "(NMEA fixes by default) to each ping_time of the Sv dataset and adds "
        "'latitude' and 'longitude' (ping_time). Every input variable is kept "
        "unchanged; the product kind is the input's (sv, mvbs, ...)."
    ),
    stdin="One Sv .nc path or gs:// URI (aa-sv output, or aa-depth etc. downstream).",
    stdout="The output file's absolute path (or gs:// URI).",
    options=[
        ("--echodata ED.nc", "the EchoData (aa-nc output) the Sv came from. Needed in "
                             "practice: without it the input itself is opened as "
                             "EchoData, which an Sv file is not. Its content enters "
                             "the product hash."),
        ("--nmea-sentence GGA", "use only this NMEA sentence type"),
        ("--datagram-type MRU1|IDX", "EK only: take position from MRU1 or IDX "
                                     "datagrams instead of NMEA (any case; cannot be "
                                     "combined with --nmea-sentence)"),
        ("-o, --output_path PATH", "Explicit output, used exactly as given. Local path or "
                                   "gs:// URI."),
    ],
    science={
        "datagram_type": "Position source: MRU1 or IDX datagrams (EK only); default NMEA.",
        "nmea_sentence": "NMEA sentence type to use (e.g. GGA); default all.",
        "echodata": "EchoData file: its content identity (not its path) enters the hash.",
    },
    files=(
        "Reads Sv .nc and EchoData .nc/.zarr, local or gs://. Writes "
        "<base>_<hash>.nc beside the input (current directory for gs:// input), "
        "or -o, or --dest. An identical earlier result is reused. "
        "AA_NAMING=legacy: <stem>_loc.nc. Refuses to overwrite the input or the "
        "--echodata file."
    ),
    pipeline="After aa-sv: ED=$(aa-nc x.raw --sonar_model EK60); aa-sv $ED | aa-location --echodata $ED",
    examples=[
        "ED=$(aa-nc D20160703-T060000.raw --sonar_model EK60)",
        "aa-sv \"$ED\" | aa-location --echodata \"$ED\"",
        "aa-location Sv.nc --echodata ED.nc --nmea-sentence GGA -o Sv_loc.nc",
    ],
)


class EchoDataError(Exception):
    """The EchoData source could not be opened (message is user-facing)."""


def print_help():
    """Curated help (also used by the docs generator)."""
    sys.stdout.write(render(SPEC, HELP, _build_parser()))


def print_help_full():
    help_text = """
    Usage: aa-location [OPTIONS] [INPUT_PATH]

    Arguments:
      INPUT_PATH                   Path (or gs:// URI) to an Sv NetCDF (.nc), or
                                   another Dataset that has ping_time and can accept
                                   location. Optional. Defaults to stdin if not provided.

    Options:
      -o, --output_path PATH       Where to write the output NetCDF with lat/lon,
                                   used exactly as given (local path or gs:// URI).
                                   Default: <base>_<hash>.nc beside the input
                                   (AA_NAMING=legacy: <stem>_loc.nc).
      --echodata PATH              Path (or gs:// URI) to the converted EchoData
                                   (.nc/.zarr, the aa-nc output) that contains the
                                   Platform/NMEA groups for interpolation. Defaults
                                   to INPUT_PATH, which only works if INPUT_PATH is
                                   itself EchoData; an Sv file is not, so pass this.
                                   Its identity is recorded as a second input and
                                   enters the product hash. A .raw is not accepted:
                                   convert it with aa-nc first.
      --datagram-type {MRU1,IDX}   (Optional) EK only: 'MRU1' or 'IDX' (any case) to
                                   take the position from those datagrams instead of
                                   NMEA. Other values are refused (they used to fall
                                   back to NMEA silently).
      --nmea-sentence STR          (Optional) Specific NMEA sentence to use (e.g. 'GGA').

      --base NAME                  Base name for the output.
      --dest DIR|gs://PREFIX       Write the default-named output there.
      --force                      Recompute even if an identical product exists.
      -h, --help                   Show the short help; --help-all shows this text.

    Description:
      Interpolates geographic location (latitude, longitude) from the platform
      navigation stream in the EchoData file to the acoustic ping_time of the
      Sv dataset, and writes the result to NetCDF. Provenance (the input's
      chain plus this step) is embedded in the output; see aa-metadata.

    Examples:
      aa-location sv.nc --echodata converted.nc
      aa-location sv.nc --echodata cruise.zarr --nmea-sentence GGA -o sv_loc.nc
    """
    print(help_text)


def _build_parser():
    parser = argparse.ArgumentParser(
        description="Add geographic location (lat/lon) to an Sv dataset using Echopype.",
        add_help=False,  # help is handled by show_help()
    )

    # ---------------------------
    # Positional/IO args
    # ---------------------------
    parser.add_argument(
        "input_path",
        type=str,
        nargs="?",
        help="Path or gs:// URI of an Sv NetCDF (.nc) or compatible Dataset file.",
    )
    parser.add_argument(
        "-o", "--output_path",
        type=str,
        help="Output NetCDF path, used as given.",
    )

    # ---------------------------
    # add_location parameters
    # ---------------------------
    parser.add_argument(
        "--echodata",
        type=str,
        help="Path to the converted EchoData (NetCDF/Zarr) containing Platform/NMEA.",
    )
    parser.add_argument(
        "--datagram-type",
        dest="datagram_type",
        # echopype only recognizes exactly "MRU1" / "IDX"; anything else used
        # to fall back to NMEA silently while being recorded as given.
        type=lambda value: value.strip().upper(),
        choices=["MRU1", "IDX"],
        help="Optional datagram type ('MRU1' or 'IDX', EK only) for selecting nav records.",
    )
    parser.add_argument(
        "--nmea-sentence",
        dest="nmea_sentence",
        help="Optional NMEA sentence (e.g., 'GGA').",
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


def _add_basic_attrs(ds) -> None:
    """Replace None attrs with strings to avoid NetCDF writer issues."""
    for k, v in list(ds.attrs.items()):
        if v is None:
            ds.attrs[k] = "NA"
    for var in ds.data_vars:
        for k, v in list(ds[var].attrs.items()):
            if v is None:
                ds[var].attrs[k] = "NA"


def main():
    """Entry point for the aa-location CLI."""
    # No args on a terminal: help. (An empty pipe is an error: see stdio.)
    if len(sys.argv) == 1 and not stdio.stdin_is_piped():
        print_help()
        sys.exit(0)

    parser = _build_parser()
    if show_help(SPEC, HELP, parser, full=print_help_full):
        sys.exit(0)

    args = parser.parse_args()
    # echopype compares these exactly; use them stripped, as they are hashed.
    for name in ("datagram_type", "nmea_sentence"):
        if getattr(args, name) is not None:
            setattr(args, name, getattr(args, name).strip())

    # ---------------------------
    # Resolve/validate input
    # ---------------------------
    token = stdio.one_input(args.input_path, SPEC.name)
    run = Run(SPEC, args)
    src = run.input(token)

    # The EchoData source defaults to the input itself (kept for compatibility).
    # Only an explicit --echodata is a second input; the default is the same
    # file, already identified above.
    ed = None
    if args.echodata is not None:
        missing = f"EchoData source '{args.echodata}' does not exist."
        if not uris.is_gcs(args.echodata) and not Path(args.echodata).expanduser().exists():
            logger.error(missing)
            sys.exit(1)
        try:
            ed = run.input(args.echodata, role="echodata")
        except FileNotFoundError:          # a gs:// object that is not there
            logger.error(missing)
            sys.exit(1)
    echodata_path = ed.local if ed is not None else src.local

    # ---------------------------
    # Resolve output path
    # ---------------------------
    out = run.plan(
        ext=".nc",
        kind=_inherited_kind(src),
        explicit=args.output_path or None,
        legacy=lambda: src.local.with_stem(src.local.stem + "_loc").with_suffix(".nc"),
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

    try:
        args_summary = dict(vars(args), output=out.target, product=out.hash)
        logger.debug(f"\naa-location args:\n{pprint.pformat(args_summary)}")

        process_file(
            input_path=src.local,
            output_path=out.local,
            echodata_path=echodata_path,
            echodata_defaulted=ed is None,
            datagram_type=args.datagram_type,
            nmea_sentence=args.nmea_sentence,
        )

        logger.info("Location interpolation complete.")
        run.finish(out)

    except EchoDataError as e:
        logger.error(str(e))
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Error during add_location: {e}")
        sys.exit(1)


def _open_echodata(path: Path, defaulted: bool):
    """Open the EchoData source; say plainly what to do when that fails.

    echopype 0.11's add_location cannot open a path itself (it passes
    'engine' to xarray twice), so the tool always hands it an EchoData
    object.
    """
    import echopype as ep

    try:
        return ep.open_converted(str(path))
    except Exception as e:
        if defaulted:
            raise EchoDataError(
                f"'{path.name}' is not an EchoData file, and --echodata was not given "
                "(it defaults to INPUT_PATH). Pass the EchoData this Sv came from: "
                "--echodata ECHODATA.nc (the aa-nc output)."
            ) from e
        raise EchoDataError(
            f"cannot open EchoData '{path}' ({type(e).__name__}: {e}). --echodata must be "
            "a converted EchoData .nc/.zarr (the aa-nc output)."
        ) from e


def process_file(input_path: Path, output_path: Path, echodata_path: Path,
                 echodata_defaulted: bool = False, datagram_type=None, nmea_sentence=None):
    """Load Sv, add latitude/longitude from EchoData, and save to NetCDF."""
    import xarray as xr
    from echopype.consolidate import add_location

    # EchoData first: the most likely failure, and a cheap one.
    echodata = _open_echodata(Path(echodata_path), echodata_defaulted)

    # Load dataset quietly
    f = io.StringIO()
    with redirect_stdout(f):
        ds = xr.open_dataset(input_path)

    logger.info("Adding geographic location (lat/lon) to Sv dataset ...")
    ds_with_loc = add_location(
        ds=ds,
        echodata=echodata,
        datagram_type=datagram_type,
        nmea_sentence=nmea_sentence,
    )

    # Still written (as before), but an all-NaN position is almost always a
    # selection that matched nothing (e.g. --nmea-sentence GLL in a GGA-only file).
    if bool(ds_with_loc["latitude"].isnull().all()):
        hint = (f" No usable '{nmea_sentence}' fixes in the EchoData? Check --nmea-sentence."
                if nmea_sentence else
                f" No usable {datagram_type} position records in the EchoData?"
                if datagram_type else "")
        logger.warning(f"every interpolated latitude/longitude is NaN.{hint}")

    # Clean attributes to avoid None in NetCDF
    _add_basic_attrs(ds_with_loc)

    logger.info("Saving dataset with location ...")
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    ds_with_loc.to_netcdf(output_path, mode="w", format="NETCDF4")


if __name__ == "__main__":
    main()
