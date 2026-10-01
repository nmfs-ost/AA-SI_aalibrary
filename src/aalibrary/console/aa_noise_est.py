#!/usr/bin/env python3
"""
aa-noise-est

Console tool for estimating background noise from calibrated Sv using Echopype.

This wraps `echopype.clean.estimate_background_noise(ds_Sv, ping_num, range_sample_num, background_noise_max=None)`
and writes the resulting noise estimate (DataArray) to NetCDF as a Dataset named "Sv_noise".

Pipeline-friendly: reads input path (or gs:// URI) from positional arg or
stdin, writes output path to stdout, all logs to stderr. The output
carries the input's provenance plus this estimation step.
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
    Help, Run, ToolSpec, add_common_flags, canon, naming, render, show_help, stdio,
)

# Recorded in the provenance when the input had no Sv and this tool
# calibrated it itself.
IMPLICIT_SV_STEP = "echopype.calibrate.compute_Sv (defaults)"

SPEC = ToolSpec(
    name="aa-noise-est",
    role="transform",
    kind="noise",
    op="echopype.clean.estimate_background_noise",
    op_version=1,
    params={
        "ping_num": canon.integer,
        "range_sample_num": canon.integer,
        "background_noise_max": canon.quantity("dB"),
    },
)

HELP = Help(
    summary="Estimate background noise (Sv_noise) from Sv.",
    does=(
        "Runs echopype.clean.estimate_background_noise: the noise level of each "
        "block of --ping-num pings is the lowest mean calibrated power over "
        "blocks of --range-sample-num samples, optionally capped at "
        "--background-noise-max, then expressed as Sv (spreading and absorption "
        "loss added back). Writes a NetCDF with one variable, Sv_noise, on "
        "channel x ping_time x range_sample. This is the noise estimate that "
        "aa-clean subtracts (De Robertis & Higginbottom 2007); the input is not "
        "changed."
    ),
    stdin=(
        "One .nc path or gs:// URI: a flat Sv file from aa-sv (needs Sv, echo_range, "
        "sound_absorption), or an EchoData file from aa-nc (then Sv is first "
        "computed with echopype.calibrate.compute_Sv defaults, EK60/AZFP only; this "
        "is recorded in the provenance)."
    ),
    stdout="The noise file's absolute path (or gs:// URI).",
    options=[
        ("-o, --output_path PATH", "Explicit output, used exactly as given (no suffix, "
                                   "no extension change). Local path or gs:// URI."),
        ("--ping-num N", "pings per noise-estimation block (default: 20)"),
        ("--range-sample-num N", "range samples per block (default: 20)"),
        ("--background-noise-max=VALdB", "cap on the noise estimate, e.g. "
                                         "--background-noise-max=-125dB (write '=' before "
                                         "a negative value); default: no cap"),
    ],
    science={
        "ping_num": "Pings per noise-estimation block.",
        "range_sample_num": "Range samples per noise-estimation block.",
        "background_noise_max": "Upper limit on the noise estimate; '-125dB' and "
                                "'-125.0dB' are the same value.",
    },
    files=(
        "Reads NetCDF, local or gs://. Writes <base>_<hash8>.nc beside the input "
        "(current directory for gs:// input), or in --dest DIR|gs://PREFIX, or at -o. "
        "AA_NAMING=legacy restores the old default <input stem>_noise.nc. An "
        "identical earlier result is reused."
    ),
    pipeline=(
        "A side branch for inspecting noise: aa-nc | aa-sv | aa-noise-est | aa-graph. "
        "Its output holds only Sv_noise, so it does not feed aa-clean or aa-mvbs "
        "(aa-clean estimates the same noise itself)."
    ),
    examples=[
        "aa-nc D20160703-T060000.raw --sonar_model EK60 | aa-sv | aa-noise-est",
        "aa-noise-est sv.nc --ping-num 50 --range-sample-num 200 \\",
        "    --background-noise-max=-120.0dB",
    ],
)


def print_help():
    """Curated help (also used by the docs generator)."""
    sys.stdout.write(render(SPEC, HELP, _build_parser()))


def print_help_full():
    """The complete reference (--help-all)."""
    help_text = """
    Usage: aa-noise-est [OPTIONS] [INPUT_PATH]

    Arguments:
      INPUT_PATH                   Path (or gs:// URI) to the calibrated .nc
                                   (NetCDF) file containing Sv (preferred), or a
                                   converted Echopype file that can be calibrated
                                   to Sv.
                                   Optional. Defaults to stdin if not provided.

    Options:
      -o, --output_path PATH       Where to write the background-noise estimate
                                   (NetCDF). Used exactly as given.
                                   Default: <base>_<hash>.nc beside the input
                                   (AA_NAMING=legacy: <stem>_noise.nc).

      --ping-num INT               Number of pings used to obtain noise estimates.
                                   Default: 20
      --range-sample-num INT       Number of samples along the range axis for each estimate.
                                   Default: 20
      --background-noise-max STR   Upper limit for background noise (dB), e.g. '-125.0dB'.
                                   Write it with '=' because the value starts
                                   with '-': --background-noise-max=-125.0dB
                                   Default: None

      --base NAME                  Base name for the output.
      --dest DIR|gs://PREFIX       Write the default-named output there.
      --force                      Recompute even if an identical product exists.

      -h, --help                   Show the short help and exit.
      --help-all                   Show this message and exit.

    Description:
      Estimates background noise by computing mean calibrated power from
      windows of pings and range samples. Writes a NetCDF containing a single
      variable "Sv_noise".

      If the input has no 'Sv' variable (e.g. the EchoData file from aa-nc),
      Sv is computed first with echopype.calibrate.compute_Sv defaults; the
      provenance records this as an implicit step. Provenance (the input's
      chain plus this step) is embedded in the output; see aa-metadata.

    Examples:
      aa-noise-est data.nc --ping-num 50 --range-sample-num 200 --background-noise-max=-120.0dB
      aa-noise-est data.nc -o cruise01_legA_noise.nc
    """
    print(help_text)


def _add_basic_attrs(ds) -> None:
    """Replace None attrs with strings to avoid NetCDF writer issues."""
    # Dataset-level attrs
    for k, v in list(ds.attrs.items()):
        if v is None:
            ds.attrs[k] = "NA"
    # Variable-level attrs
    for var in ds.data_vars:
        for k, v in list(ds[var].attrs.items()):
            if v is None:
                ds[var].attrs[k] = "NA"


def _build_parser():
    parser = argparse.ArgumentParser(
        description="Estimate background noise (Sv_noise) from Sv using Echopype.",
        add_help=False,  # -h/--help and --help-all are handled by show_help
    )

    # ---------------------------
    # Positional/IO args
    # ---------------------------
    parser.add_argument(
        "input_path",
        type=str,
        nargs="?",
        help="Path or gs:// URI of a NetCDF file containing Sv (preferred) or a converted file that can be calibrated to Sv.",
    )
    parser.add_argument(
        "-o", "--output_path",
        type=str,
        help="Output path for the noise NetCDF, used as given.",
    )

    # ---------------------------
    # estimate_background_noise parameters
    # ---------------------------
    parser.add_argument(
        "--ping-num",
        dest="ping_num",
        type=int,
        default=20,
        help="Number of pings to obtain noise estimates (default: 20).",
    )
    parser.add_argument(
        "--range-sample-num",
        dest="range_sample_num",
        type=int,
        default=20,
        help="Number of range samples per estimate window (default: 20).",
    )
    parser.add_argument(
        "--background-noise-max",
        dest="background_noise_max",
        default=None,
        help="Upper limit for background noise in dB, e.g. '-120.0dB' (default: None).",
    )
    add_common_flags(parser)
    return parser


def main():
    """Entry point for the aa-noise-est CLI."""
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

    # ---------------------------
    # Resolve output path
    # ---------------------------
    # '-o' keeps its old rule: used verbatim.
    out = run.plan(
        ext=".nc",
        explicit=args.output_path or None,
        legacy=lambda: naming.with_stem_suffix(src.local, "_noise", ".nc"),
    )

    # Guard against clobbering the input
    if not out.remote and Path(out.target).resolve() == src.local.resolve():
        logger.error(f"Refusing to overwrite input file: {src.local.resolve()}")
        sys.exit(1)

    if run.reusable(out):
        run.finish(out)
        return

    try:
        import echopype as ep  # deferred so --help stays fast
        import xarray as xr
        from echopype.clean import estimate_background_noise

        # ---------------------------
        # Load dataset quietly
        # ---------------------------
        # Suppress any library chatter to stdout so pipelines remain clean.
        f = io.StringIO()
        with redirect_stdout(f):
            ds = xr.open_dataset(src.local)

        # ---------------------------
        # Ensure we have calibrated Sv
        # ---------------------------
        calibrated_here = False
        if "Sv" not in ds.data_vars:
            logger.info("No 'Sv' variable found; attempting to calibrate to Sv via Echopype...")
            ds.close()
            ed = ep.open_converted(str(src.local))
            ds = ep.calibrate.compute_Sv(ed)
            calibrated_here = True

        # ---------------------------
        # Estimate background noise (returns DataArray)
        # ---------------------------
        logger.info("Estimating background noise (Sv_noise)...")
        da_noise = estimate_background_noise(
            ds_Sv=ds,
            ping_num=args.ping_num,
            range_sample_num=args.range_sample_num,
            background_noise_max=args.background_noise_max,
        )

        # Wrap DataArray into a Dataset for clearer NetCDF structure and naming
        ds_out = da_noise.to_dataset(name="Sv_noise")
        # Basic attrs sanitization to avoid None in NetCDF
        _add_basic_attrs(ds_out)

        # Save to NetCDF
        out.local.parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"Saving noise estimate to {out.local} ...")
        ds_out.to_netcdf(out.local, mode="w", format="NETCDF4")

        # Pretty-print args for logs and echo the primary output path to stdout for piping
        pretty_args = pprint.pformat(vars(args) | {"output": out.target, "product": out.hash})
        logger.debug(f"\naa-noise-est args:\n{pretty_args}")
        run.finish(out, extra={"implicit_step": IMPLICIT_SV_STEP} if calibrated_here else None)

        logger.info("Background noise estimation complete.")

    except Exception as e:
        logger.exception(f"Error during background noise estimation: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
