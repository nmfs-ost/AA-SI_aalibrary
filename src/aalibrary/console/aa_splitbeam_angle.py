#!/usr/bin/env python3
"""
aa-splitbeam-angle

Console tool for adding split-beam (alongship/athwartship) angles to an Sv
dataset using Echopype's `consolidate.add_splitbeam_angle`.

This wraps:
  echopype.consolidate.add_splitbeam_angle(
      source_Sv, echodata, waveform_mode, encode_mode,
      pulse_compression=False, storage_options={}, to_disk=True
  )

We call it with `to_disk=False` to return an xarray.Dataset and then write
it to the output .nc.

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
    name="aa-splitbeam-angle",
    role="transform",
    kind="sv",
    op="echopype.consolidate.add_splitbeam_angle",
    op_version=1,
    params={
        "waveform_mode": canon.choice(),
        "encode_mode": canon.choice(),
        "pulse_compression": canon.boolean,
    },
)

HELP = Help(
    summary="Add split-beam angles to an Sv dataset (echopype.consolidate.add_splitbeam_angle).",
    does=(
        "Computes the alongship and athwartship split-beam angles (degrees) from "
        "the EchoData Beam group and adds 'angle_alongship' and "
        "'angle_athwartship' (channel x ping_time x range_sample) to the Sv "
        "dataset. Every input variable is kept unchanged."
    ),
    stdin="One Sv .nc path or gs:// URI (aa-sv output). Not a .raw: convert with aa-nc first.",
    stdout="The output file's absolute path (or gs:// URI).",
    options=[
        ("--echodata ED.nc", "the EchoData (aa-nc output) the Sv came from. Needed in "
                             "practice: without it the input itself is opened as "
                             "EchoData, which an Sv file is not. Its content enters "
                             "the product hash."),
        ("--waveform-mode CW|BB", "REQUIRED. CW narrowband (EK60 is always CW) or BB broadband"),
        ("--encode-mode power|complex", "REQUIRED. EK60: power. 'power' needs CW."),
        ("--pulse-compression", "BB + complex only"),
        ("--no-overwrite", "exit 1 instead of replacing a different existing output"),
        ("-o, --output_path PATH", "Explicit output, used exactly as given. Local path or "
                                   "gs:// URI."),
    ],
    science={
        "waveform_mode": "Transmit waveform: CW (narrowband) or BB (broadband).",
        "encode_mode": "Recorded echo encoding: power or complex.",
        "pulse_compression": "Apply pulse compression (BB + complex only).",
        "echodata": "EchoData file: its content identity (not its path) enters the hash.",
    },
    files=(
        "Reads Sv .nc and EchoData .nc/.zarr, local or gs://. Writes "
        "<base>_<hash>.nc beside the input (current directory for gs:// input), "
        "or -o, or --dest. An identical earlier result is reused (even with "
        "--no-overwrite). AA_NAMING=legacy: <stem>_splitbeam_angle.nc. Refuses "
        "to overwrite the input or the --echodata file."
    ),
    pipeline=(
        "After aa-sv, e.g. before aa-detect-seafloor --method blackwell, which "
        "needs the angles: aa-sv $ED | aa-depth | aa-splitbeam-angle --echodata $ED ..."
    ),
    examples=[
        "ED=$(aa-nc D20160703-T060000.raw --sonar_model EK60)",
        "aa-sv \"$ED\" | aa-splitbeam-angle --echodata \"$ED\" --waveform-mode CW --encode-mode power",
        "aa-splitbeam-angle Sv.nc --echodata ED.nc --waveform-mode BB --encode-mode complex "
        "--pulse-compression   # EK80",
    ],
)


class EchoDataError(Exception):
    """The EchoData source could not be opened (message is user-facing)."""


def print_help():
    """Curated help (also used by the docs generator)."""
    sys.stdout.write(render(SPEC, HELP, _build_parser()))


def print_help_full():
    help_text = """
    Usage: aa-splitbeam-angle [OPTIONS] [INPUT_PATH]

    Arguments:
      INPUT_PATH                   Path (or gs:// URI) to an Sv NetCDF (.nc).
                                   Optional; if omitted, a path token is read
                                   from stdin.

    Options:
      -o, --output_path PATH       Output NetCDF path, used exactly as given (local
                                   path or gs:// URI). Default: <base>_<hash>.nc
                                   beside the input (AA_NAMING=legacy:
                                   <stem>_splitbeam_angle.nc).
      --echodata PATH              Path (or gs:// URI) to the converted EchoData
                                   (.nc/.zarr, the aa-nc output) that holds the
                                   Sonar/Beam_group* data required for angle
                                   computation. If not provided, defaults to
                                   INPUT_PATH, which only works if INPUT_PATH is
                                   itself EchoData; an Sv file is not, so pass
                                   this. A .raw is not accepted: convert it with
                                   aa-nc first. Its identity is recorded as a
                                   second input and enters the product hash.
      --waveform-mode {CW,BB}      Transmit waveform mode: CW (narrowband) or BB (broadband).
                                   Required.
      --encode-mode {complex,power}  Return echo encoding type: 'complex' or 'power'.
                                   Required. ('power' only valid with CW.)
      --pulse-compression          Use pulse compression (valid only for BB + complex).
      --no-overwrite               Do not overwrite an existing, different output
                                   file (exit 1). An identical product (same input,
                                   same options) is reused instead.

      --base NAME                  Base name for the output.
      --dest DIR|gs://PREFIX       Write the default-named output there.
      --force                      Recompute even if an identical product exists.
      -h, --help                   Show the short help; --help-all shows this text.

    Description:
      Computes alongship and athwartship split-beam angles and adds them to the Sv dataset.
      Requires the associated converted EchoData file containing beam group and
      transducer data. Provenance (the input's chain plus this step) is embedded
      in the output; see aa-metadata.
    """
    print(help_text)


def _build_parser():
    parser = argparse.ArgumentParser(
        description="Add split-beam angles (alongship/athwartship) to an Sv dataset.",
        add_help=False,  # help is handled by show_help()
    )

    # ---------------------------
    # Positional/IO args
    # ---------------------------
    parser.add_argument(
        "input_path",
        type=str,
        nargs="?",
        help="Path or gs:// URI of an Sv NetCDF (.nc).",
    )
    parser.add_argument(
        "-o", "--output_path",
        type=str,
        help="Output NetCDF path, used as given.",
    )
    parser.add_argument(
        "--echodata",
        type=str,
        help="Path to the converted EchoData (NetCDF/Zarr) containing Sonar/Beam_group*.",
    )

    # ---------------------------
    # add_splitbeam_angle parameters
    # ---------------------------
    parser.add_argument("--waveform-mode", dest="waveform_mode",
                        choices=["CW", "BB"], required=True,
                        help="Transmit waveform mode: CW (narrowband) or BB (broadband).")
    parser.add_argument("--encode-mode", dest="encode_mode",
                        choices=["complex", "power"], required=True,
                        help="Return echo encoding type: complex or power.")
    parser.add_argument("--pulse-compression", dest="pulse_compression",
                        action="store_true",
                        help="Use pulse compression (valid only for BB + complex).")

    parser.add_argument("--no-overwrite", action="store_true",
                        help="Do not overwrite an existing output file.")
    add_common_flags(parser)
    return parser


def _add_basic_attrs(ds) -> None:
    """Replace None attrs with strings to avoid NetCDF writer issues."""
    for k, v in list(ds.attrs.items()):
        if v is None:
            ds.attrs[k] = "NA"
    for var in ds.data_vars:
        for k, v in list(ds[var].attrs.items()):
            if v is None:
                ds[var].attrs[k] = "NA"


def _exists(target: str) -> bool:
    return uris.stat(target) is not None if uris.is_gcs(target) else Path(target).exists()


def main():
    """Entry point for the aa-splitbeam-angle CLI."""
    # No args on a terminal: help. (An empty pipe is an error: see stdio.)
    if len(sys.argv) == 1 and not stdio.stdin_is_piped():
        print_help()
        sys.exit(0)

    parser = _build_parser()
    if show_help(SPEC, HELP, parser, full=print_help_full):
        sys.exit(0)

    args = parser.parse_args()

    # ---------------------------
    # Resolve/validate input
    # ---------------------------
    token = stdio.one_input(args.input_path, SPEC.name)
    run = Run(SPEC, args)
    src = run.input(token)

    # The EchoData source defaults to the input itself (kept for compatibility).
    # Only an explicit --echodata is a second input.
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
        explicit=args.output_path or None,
        legacy=lambda: src.local.with_stem(
            src.local.stem + "_splitbeam_angle").with_suffix(".nc"),
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

    # An identical product is never an "overwrite": reuse it first.
    if run.reusable(out):
        run.finish(out)
        return

    if args.no_overwrite and _exists(out.target):
        logger.error(f"Output file '{out.target}' exists and --no-overwrite was set.")
        sys.exit(1)

    try:
        args_summary = dict(vars(args), output=out.target, product=out.hash)
        logger.debug(f"\naa-splitbeam-angle args:\n{pprint.pformat(args_summary)}")

        process_file(
            input_path=src.local,
            output_path=out.local,
            echodata_path=echodata_path,
            echodata_defaulted=ed is None,
            waveform_mode=args.waveform_mode,
            encode_mode=args.encode_mode,
            pulse_compression=args.pulse_compression,
        )

        logger.info("Split-beam angle computation complete.")
        run.finish(out)

    except EchoDataError as e:
        logger.error(str(e))
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Error during add_splitbeam_angle: {e}")
        sys.exit(1)


def _open_echodata(path: Path, defaulted: bool):
    """Open the EchoData source; say plainly what to do when that fails.

    echopype 0.11's add_splitbeam_angle cannot open a path itself (it passes
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
                 echodata_defaulted: bool, waveform_mode: str, encode_mode: str,
                 pulse_compression: bool = False):
    """Load Sv, add split-beam angles from EchoData, and save to NetCDF."""
    import xarray as xr
    from echopype.consolidate import add_splitbeam_angle

    # EchoData first: the most likely failure, and a cheap one.
    echodata = _open_echodata(Path(echodata_path), echodata_defaulted)

    # Load dataset quietly
    f = io.StringIO()
    with redirect_stdout(f):
        ds = xr.open_dataset(input_path)

    logger.info("Adding split-beam angles to Sv ...")
    ds_with_angle = add_splitbeam_angle(
        source_Sv=ds,
        echodata=echodata,
        waveform_mode=waveform_mode,
        encode_mode=encode_mode,
        pulse_compression=pulse_compression,
        to_disk=False,  # return Dataset for manual saving
    )

    # Clean attributes to avoid None in NetCDF
    _add_basic_attrs(ds_with_angle)

    logger.info("Saving Sv + angle dataset ...")
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    ds_with_angle.to_netcdf(output_path, mode="w", format="NETCDF4")


if __name__ == "__main__":
    main()
