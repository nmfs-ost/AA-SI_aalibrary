#!/usr/bin/env python3
"""
aa-dispersion

Console tool computing echopype.metrics.dispersion, the inertia (I) of the
backscatter along range: its spread about the center of mass, per channel
and ping.

Wraps:
  echopype.metrics.dispersion(ds: xarray.Dataset, range_label='echo_range')

    I = sum((r - CM)^2 * sv * dz) / sum(sv * dz)   over range_sample, sv = 10^(Sv/10)

I is a sv-weighted variance of range, so its unit is m^2 (echopype 0.11's
docstring says m^-2; the formula gives m^2).

Pipeline-friendly: reads the input path (or gs:// URI) from the positional
argument or stdin, writes the output path to stdout, logs to stderr. The
output carries the input's provenance plus this step.
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
# _configure_logging() below replaces this once --quiet is parsed.
logger.add(sys.stderr, level="WARNING")

import argparse
import io
import pprint
from contextlib import redirect_stdout
from pathlib import Path

from aalibrary.console._core import (
    Help, Run, ToolSpec, add_common_flags, canon, naming, render, show_help, stdio,
)

# xarray and echopype are imported inside compute() so --help stays fast.

SPEC = ToolSpec(
    name="aa-dispersion",
    role="transform",
    kind="echometric",
    op="echopype.metrics.dispersion",
    op_version=1,
    params={"range_label": canon.text},
)

HELP = Help(
    summary="Inertia (I, m^2): spread of backscatter about its center of mass.",
    does=(
        "Runs echopype.metrics.dispersion on calibrated Sv: I = sum((r - CM)^2 * sv*dz) "
        "/ sum(sv*dz) over range_sample, with sv = 10^(Sv/10), r the range variable, dz "
        "its spacing and CM the center of mass (Urmy et al. 2012). I is a sv-weighted "
        "variance of range. Writes one variable, 'dispersion' (units m2), on "
        "channel x ping_time."
    ),
    stdin=(
        "One Sv NetCDF path or gs:// URI (argument, or one line on stdin), e.g. from "
        "aa-sv or aa-clean. It must contain 'Sv' (dB) on a range_sample dimension and "
        "the range variable named by --range-label (default echo_range)."
    ),
    stdout="The output file's absolute path (or gs:// URI).",
    options=[
        ("-o, --output_path PATH", "Explicit output, used exactly as given (no suffix "
                                   "added). Local path or gs:// URI."),
        ("--range-label NAME", "variable holding range in metres (default: echo_range)"),
        ("--no-overwrite", "exit 1 if the output exists and is a different product "
                           "(an identical one is reused)"),
        ("--quiet", "warnings and errors only on stderr"),
    ],
    science={
        "range_label": "Variable used as range (m) for dz and the integral.",
    },
    files=(
        "Reads a flat Sv NetCDF, local or gs:// (through a gcsfuse mount when one "
        "covers it, otherwise downloaded once to the cache). Writes <base>_<hash8>.nc "
        "beside the input (current directory for gs:// input), or -o, or --dest. "
        "AA_NAMING=legacy restores <input stem>_dispersion.nc. An identical earlier "
        "result is reused."
    ),
    pipeline=(
        "After calibration: aa-nc | aa-sv | aa-dispersion. The output is a 2-D metric "
        "(channel x ping_time), not Sv, so it ends the Sv chain."
    ),
    examples=[
        "aa-nc D20160703-T060000.raw --sonar_model EK60 | aa-sv | aa-dispersion",
        "aa-dispersion sv.nc -o inertia.nc --no-overwrite",
    ],
    notes=[
        "echopype 0.11 computes the center of mass inside dispersion from 'echo_range' "
        "whatever --range-label says. With another --range-label the deviations and "
        "the center of mass come from different variables, which adds "
        "(CM of label - CM of echo_range)^2 to every value (+25 m2 for a depth that is "
        "echo_range + 5 m), and it fails when the file has no echo_range. Keep the "
        "default; the tool warns on stderr otherwise."
    ],
)


def _configure_logging(quiet: bool) -> None:
    """Replace the default suppression sink with a user-visible one."""
    logger.remove()
    logger.add(sys.stderr, level="WARNING" if quiet else "INFO")


def print_help():
    """Curated help (also used by the docs generator)."""
    sys.stdout.write(render(SPEC, HELP, _build_parser()))


def print_help_full():
    help_text = """
    Usage: aa-dispersion [OPTIONS] [INPUT_PATH]

    Arguments:
      INPUT_PATH                  Path (or gs:// URI) to a NetCDF file (.nc) containing a
                                  calibrated Dataset with 'Sv' and an `echo_range` (or
                                  similar) variable. Optional; defaults to stdin if not
                                  provided.

    Options:
      -o, --output_path          Path (or gs:// URI) to write the resulting dispersion
                                  (NetCDF), used as given.
                                  Default: <base>_<hash8>.nc beside the input
                                  (AA_NAMING=legacy: <stem>_dispersion.nc).
      --range-label STR          Name of the range variable/coordinate (default: echo_range).
      --no-overwrite             Do not overwrite an existing, different output file
                                  (exit 1). An identical product is reused.
      --quiet                    Warnings and errors only on stderr (stdout is always
                                  just the output path).
      --base NAME                Base name for the output.
      --dest DIR|gs://PREFIX     Write the default-named output there.
      --force                    Recompute even if an identical product exists.
      -h, --help                 Curated help. --help-all: this text.

    Description:
      Computes the inertia of the backscatter distribution (i.e., dispersion/spread
      about the center of mass) using Echopype's metrics.dispersion. The returned
      quantity is a sv-weighted variance of range and has units m^2 (written as
      units="m2"; echopype's docstring says m^-2, which the formula does not give).
      Provenance (the input's chain plus this step) is embedded in the output;
      see aa-metadata.
    """
    print(help_text)


def _build_parser():
    parser = argparse.ArgumentParser(
        description="Compute dispersion (inertia) of backscatter using Echopype.",
        add_help=False,
    )
    parser.add_argument(
        "input_path",
        type=str,
        nargs="?",
        help="Path or gs:// URI of a NetCDF file (.nc) dataset with Sv and echo_range.",
    )
    parser.add_argument(
        "-o", "--output_path",
        type=str,
        help="Output NetCDF path, used as given (default: <base>_<hash8>.nc).",
    )
    parser.add_argument(
        "--range-label", dest="range_label", default="echo_range",
        help="Name of the range coordinate/variable (default: echo_range).",
    )
    parser.add_argument(
        "--no-overwrite", action="store_true",
        help="Do not overwrite an existing, different output file.",
    )
    parser.add_argument(
        "--quiet", action="store_true",
        help="Warnings and errors only on stderr.",
    )
    add_common_flags(parser)
    return parser


def _add_basic_attrs(ds) -> None:
    """Replace None attrs with 'NA' to avoid NetCDF writer issues."""
    for k, v in list(ds.attrs.items()):
        if v is None:
            ds.attrs[k] = "NA"
    for var in ds.data_vars:
        for kk, vv in list(ds[var].attrs.items()):
            if vv is None:
                ds[var].attrs[kk] = "NA"


def _warn_range_label(label: str) -> None:
    """echopype 0.11's dispersion() calls center_of_mass(ds) without passing
    range_label, so the center of mass always comes from echo_range. The
    result is sum(w*(r - CM_echo_range)^2)/sum(w) = I + (CM_r - CM_echo_range)^2:
    the true inertia plus a constant bias. Kept as is (same numbers as
    before); the user is told on every run."""
    print(
        f"aa-dispersion: warning: --range-label {label!r}: echopype.metrics.dispersion "
        f"takes the center of mass from 'echo_range', not from {label!r}, so every value "
        f"is the inertia about the wrong center: it includes an added "
        f"(CM of {label} - CM of echo_range)^2 (e.g. +25 m2 when {label} = echo_range "
        f"+ 5 m), and it fails if the file has no echo_range. Only the default "
        f"--range-label echo_range gives the true inertia.",
        file=sys.stderr,
    )


def main():
    # No args on a terminal: help. (An empty pipe is an error: see stdio.)
    if len(sys.argv) == 1 and not stdio.stdin_is_piped():
        print_help()
        sys.exit(0)

    parser = _build_parser()
    if show_help(SPEC, HELP, parser, full=print_help_full):
        sys.exit(0)

    args = parser.parse_args()
    _configure_logging(args.quiet)
    # The hash canonicalizes the name with strip(); use the same name.
    args.range_label = args.range_label.strip()
    if args.range_label != "echo_range":
        _warn_range_label(args.range_label)

    # Resolve / validate (positional > stdin; local path or gs:// URI)
    token = stdio.one_input(args.input_path, SPEC.name)
    run = Run(SPEC, args)
    src = run.input(token)

    out = run.plan(
        ext=".nc",
        explicit=args.output_path,  # -o is used verbatim, as always
        legacy=lambda: naming.with_stem_suffix(src.local, "_dispersion", ".nc"),
    )

    if not out.remote and Path(out.target).resolve() == src.local.resolve():
        logger.error(f"Refusing to overwrite input file: {src.local.resolve()}")
        sys.exit(1)

    if run.reusable(out):
        run.finish(out)
        return

    # (This used to read `args.no-overwrite`, i.e. `args.no - overwrite`,
    # which raised AttributeError whenever the output already existed.)
    if args.no_overwrite and run.exists(out):
        logger.error(f"Output file '{out.target}' exists and --no-overwrite was set.")
        sys.exit(1)

    try:
        compute(src.local, out.local, range_label=args.range_label, shown=out.target)

        logger.debug(f"\naa-dispersion args:\n{pprint.pformat(vars(args))}")
        # Print path (or URI) for piping
        run.finish(out)
        logger.info("Dispersion computation complete.")

    except Exception as e:
        logger.exception(f"Error during dispersion computation: {e}")
        sys.exit(1)


def compute(input_path: Path, output_path: Path, *, range_label: str = "echo_range",
            shown: str | None = None) -> None:
    """Compute inertia from ``input_path`` and write it to ``output_path``.

    ``shown`` is the name logged for the output (the gs:// URI when
    ``output_path`` is a staging file).
    """
    import xarray as xr
    from echopype.metrics import dispersion

    # load dataset quietly
    f = io.StringIO()
    with redirect_stdout(f):
        ds = xr.open_dataset(input_path)

    logger.info("Computing dispersion (inertia)...")
    da_disp = dispersion(ds=ds, range_label=range_label)

    # Package into a Dataset for output.
    ds_out = da_disp.to_dataset(name="dispersion")
    # Fresh attrs: the result can inherit the range variable's attrs
    # (long_name, units, history) and those don't describe this metric.
    ds_out["dispersion"].attrs = {"long_name": "Dispersion (inertia)", "units": "m2"}
    ds_out.attrs.setdefault("source_tool", "aa-dispersion")
    ds_out.attrs.setdefault("range_label", range_label)

    _add_basic_attrs(ds_out)

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Saving dispersion to {shown or output_path} ...")
    ds_out.to_netcdf(output_path, mode="w", format="NETCDF4")
    ds.close()


if __name__ == "__main__":
    main()
