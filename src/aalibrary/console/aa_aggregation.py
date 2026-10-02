#!/usr/bin/env python3
"""
aa-aggregation

Console tool computing echopype.metrics.aggregation, the index of
aggregation (IA, unit: m^-1) of the backscatter along range, per channel
and ping.

Wraps:
  echopype.metrics.aggregation(ds: xarray.Dataset, range_label: str = "echo_range") -> xarray.DataArray

    IA = 1 / EA = sum(sv^2 * dz) / (sum(sv * dz))^2   over range_sample, sv = 10^(Sv/10)

Pipeline-friendly: reads the input path (or gs:// URI) from the positional
argument or stdin, writes the output path to stdout, logs to stderr. The
output carries the input's provenance plus this step.

Notes:
- `aggregation` expects a calibrated Dataset with `Sv` and an `echo_range`
  (or equivalent).
- If missing, consider generating a file that contains `echo_range` (e.g., with aa-sv).
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
    name="aa-aggregation",
    role="transform",
    kind="echometric",
    op="echopype.metrics.aggregation",
    op_version=1,
    params={"range_label": canon.text},
)

HELP = Help(
    summary="Index of aggregation (IA, m^-1) of backscatter along range, per ping.",
    does=(
        "Runs echopype.metrics.aggregation on calibrated Sv: IA = 1/EA = sum(sv^2*dz) / "
        "(sum sv*dz)^2 over range_sample, with sv = 10^(Sv/10) and dz the spacing of "
        "the range variable (Urmy et al. 2012). IA is high when a small part of the "
        "water column is much denser than the rest. Writes one variable, "
        "'aggregation' (units m-1), on channel x ping_time."
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
        "AA_NAMING=legacy restores <input stem>_aggregation.nc. An identical earlier "
        "result is reused."
    ),
    pipeline=(
        "After calibration: aa-nc | aa-sv | aa-aggregation. The output is a 2-D metric "
        "(channel x ping_time), not Sv, so it ends the Sv chain."
    ),
    examples=[
        "aa-nc D20160703-T060000.raw --sonar_model EK60 | aa-sv | aa-aggregation",
        "aa-aggregation sv.nc -o ia.nc --no-overwrite",
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
    Usage: aa-aggregation [OPTIONS] [INPUT_PATH]

    Arguments:
      INPUT_PATH                   Path (or gs:// URI) to a NetCDF file (.nc) containing a
                                   calibrated Dataset with 'Sv' and 'echo_range'. Optional;
                                   defaults to reading one token from stdin.

    Options:
      -o, --output_path PATH       Output NetCDF path or gs:// URI, used as given.
                                   Default: <base>_<hash8>.nc beside the input
                                   (AA_NAMING=legacy: <stem>_aggregation.nc).
      --range-label STR            Name of the DataArray holding range (default: echo_range).
      --no-overwrite               Do not overwrite an existing, different output file
                                   (exit 1). An identical product is reused.
      --quiet                      Warnings and errors only on stderr (stdout is always
                                   just the output path).
      --base NAME                  Base name for the output.
      --dest DIR|gs://PREFIX       Write the default-named output there.
      --force                      Recompute even if an identical product exists.
      -h, --help                   Curated help. --help-all: this text.

    Description:
      Computes the Echopype aggregation metric (index of aggregation, IA = 1/EA,
      units m^-1, written as units="m-1") of backscatter along the range axis.
      Provenance (the input's chain plus this step) is embedded in the output;
      see aa-metadata.
    """
    print(help_text)


def _build_parser():
    parser = argparse.ArgumentParser(
        description="Compute Echopype metrics.aggregation.",
        add_help=False,
    )

    # IO args
    parser.add_argument(
        "input_path",
        type=str,
        nargs="?",
        help="Path or gs:// URI of a NetCDF Dataset containing 'Sv' and 'echo_range'.",
    )
    parser.add_argument(
        "-o", "--output_path",
        type=str,
        help="Output NetCDF path, used as given (default: <base>_<hash8>.nc).",
    )

    # aggregation parameters
    parser.add_argument("--range-label", dest="range_label", default="echo_range",
                        help="Name of the range DataArray (default: echo_range).")

    # behavior flags
    parser.add_argument("--no-overwrite", action="store_true",
                        help="Do not overwrite an existing, different output file.")
    parser.add_argument("--quiet", action="store_true",
                        help="Warnings and errors only on stderr.")
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


def main():
    """Entry point for the aa-aggregation CLI."""
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

    # Resolve / validate (positional > stdin; local path or gs:// URI)
    token = stdio.one_input(args.input_path, SPEC.name)
    run = Run(SPEC, args)
    src = run.input(token)

    out = run.plan(
        ext=".nc",
        explicit=args.output_path,  # -o is used verbatim, as always
        legacy=lambda: naming.with_stem_suffix(src.local, "_aggregation", ".nc"),
    )

    if not out.remote and Path(out.target).resolve() == src.local.resolve():
        logger.error(f"Refusing to overwrite input file: {src.local.resolve()}")
        sys.exit(1)

    if run.reusable(out):
        run.finish(out)
        return

    if args.no_overwrite and run.exists(out):
        logger.error(f"Output file '{out.target}' exists and --no-overwrite was set.")
        sys.exit(1)

    try:
        compute(src.local, out.local, range_label=args.range_label, shown=out.target)

        logger.debug(f"\naa-aggregation args:\n{pprint.pformat(vars(args))}")
        # Print path (or URI) for piping
        run.finish(out)
        logger.info("Aggregation computation complete.")

    except Exception as e:
        logger.exception(f"Error during aggregation computation: {e}")
        sys.exit(1)


def compute(input_path: Path, output_path: Path, *, range_label: str = "echo_range",
            shown: str | None = None) -> None:
    """Compute IA from ``input_path`` and write it to ``output_path``.

    ``shown`` is the name logged for the output (the gs:// URI when
    ``output_path`` is a staging file).
    """
    import xarray as xr
    from echopype.metrics import aggregation

    # Load quietly to keep stdout clean for piping
    f = io.StringIO()
    with redirect_stdout(f):
        ds = xr.open_dataset(input_path)

    # Ensure we have a range variable
    have_range = range_label in ds.variables or range_label in ds.coords
    if not have_range:
        logger.error(
            f"Required range label '{range_label}' not found in Dataset."
        )
        sys.exit(1)

    logger.info("Computing aggregation metric...")
    da_aggr = aggregation(ds=ds, range_label=range_label)

    # Package into a Dataset for output.
    out_ds = da_aggr.to_dataset(name="aggregation")
    # Fresh attrs: the result can inherit the range variable's attrs
    # (long_name, units, history) and those don't describe this metric.
    out_ds["aggregation"].attrs = {"long_name": "Aggregation metric", "units": "m-1"}
    out_ds.attrs.setdefault("source_tool", "aa-aggregation")
    out_ds.attrs.setdefault("range_label", range_label)

    _add_basic_attrs(out_ds)

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Saving aggregation to {shown or output_path} ...")
    out_ds.to_netcdf(output_path, mode="w", format="NETCDF4")
    ds.close()


if __name__ == "__main__":
    main()
