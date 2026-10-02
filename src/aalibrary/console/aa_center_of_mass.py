#!/usr/bin/env python3
"""
aa-center-of-mass

Console tool computing echopype.metrics.center_of_mass (CM, unit: m), the
sv-weighted mean range of the backscatter, per channel and ping.

Wraps:
  echopype.metrics.center_of_mass(ds: xarray.Dataset, range_label: str = "echo_range") -> xarray.DataArray

    CM = sum(r * sv * dz) / sum(sv * dz)   over range_sample, sv = 10^(Sv/10)

Pipeline-friendly: reads the input path (or gs:// URI) from the positional
argument or stdin, writes the output path to stdout, logs to stderr. The
output carries the input's provenance plus this step.

Notes:
- `center_of_mass` expects a calibrated Dataset with `Sv` and an `echo_range`
  (or equivalent).
- If missing, you can try `--try-calibrate` to open as converted EchoData and compute Sv.
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
    name="aa-center-of-mass",
    role="transform",
    kind="echometric",
    op="echopype.metrics.center_of_mass",
    op_version=1,
    params={"range_label": canon.text, "try_calibrate": canon.boolean},
)

HELP = Help(
    summary="Center of mass (CM, m): the sv-weighted mean range of backscatter.",
    does=(
        "Runs echopype.metrics.center_of_mass on calibrated Sv: CM = sum(r * sv*dz) / "
        "sum(sv*dz) over range_sample, with sv = 10^(Sv/10), r the range variable and "
        "dz its spacing (Urmy et al. 2012). Writes one variable, 'center_of_mass' "
        "(units m, the range variable's reference: transducer range for echo_range), "
        "on channel x ping_time."
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
        ("--try-calibrate", "if that variable is missing, open the input as EchoData and "
                            "compute Sv first (echopype defaults; no EK80 modes)"),
        ("--no-overwrite", "exit 1 if the output exists and is a different product "
                           "(an identical one is reused)"),
        ("--quiet", "warnings and errors only on stderr"),
    ],
    science={
        "range_label": "Variable used as range (m): the weighted values, dz and the integral.",
        "try_calibrate": "Compute Sv from EchoData first when the range variable is "
                         "missing; changes what is analysed.",
    },
    files=(
        "Reads a flat Sv NetCDF, local or gs:// (through a gcsfuse mount when one "
        "covers it, otherwise downloaded once to the cache). Writes <base>_<hash8>.nc "
        "beside the input (current directory for gs:// input), or -o, or --dest. "
        "AA_NAMING=legacy restores <input stem>_com.nc. An identical earlier result "
        "is reused."
    ),
    pipeline=(
        "After calibration: aa-nc | aa-sv | aa-center-of-mass. The output is a 2-D "
        "metric (channel x ping_time), not Sv, so it ends the Sv chain."
    ),
    examples=[
        "aa-nc D20160703-T060000.raw --sonar_model EK60 | aa-sv | aa-center-of-mass",
        "aa-center-of-mass sv.nc -o cm.nc --no-overwrite",
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
    Usage: aa-center-of-mass [OPTIONS] [INPUT_PATH]

    Arguments:
      INPUT_PATH                   Path (or gs:// URI) to a NetCDF file (.nc) containing a
                                   calibrated Dataset with 'Sv' and 'echo_range'. Optional;
                                   defaults to reading one token from stdin.

    Options:
      -o, --output_path PATH       Output NetCDF path or gs:// URI, used as given.
                                   Default: <base>_<hash8>.nc beside the input
                                   (AA_NAMING=legacy: <stem>_com.nc).
      --range-label STR            Name of the DataArray holding range (default: echo_range).
      --try-calibrate              If 'echo_range' is missing, try to open as converted
                                   EchoData and compute Sv to obtain it.
      --no-overwrite               Do not overwrite an existing, different output file
                                   (exit 1). An identical product is reused.
      --quiet                      Warnings and errors only on stderr (stdout is always
                                   just the output path).
      --base NAME                  Base name for the output.
      --dest DIR|gs://PREFIX       Write the default-named output there.
      --force                      Recompute even if an identical product exists.
      -h, --help                   Curated help. --help-all: this text.

    Description:
      Computes the center of mass (sv-weighted mean range) of backscatter along range.
      Units: meters (same units as the provided range axis).
      Provenance (the input's chain plus this step) is embedded in the output;
      see aa-metadata.
    """
    print(help_text)


def _build_parser():
    parser = argparse.ArgumentParser(
        description="Compute Echopype metrics.center_of_mass (COM).",
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

    # center_of_mass parameters
    parser.add_argument("--range-label", dest="range_label", default="echo_range",
                        help="Name of the range DataArray (default: echo_range).")

    # behavior flags
    parser.add_argument("--try-calibrate", action="store_true",
                        help="If 'echo_range' missing, attempt to compute Sv to obtain it.")
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
    """Entry point for the aa-center-of-mass CLI."""
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
        legacy=lambda: naming.with_stem_suffix(src.local, "_com", ".nc"),
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
        compute(src.local, out.local, range_label=args.range_label,
                try_calibrate=args.try_calibrate, shown=out.target)

        logger.debug(f"\naa-center-of-mass args:\n{pprint.pformat(vars(args))}")
        # Print path (or URI) for piping
        run.finish(out)
        logger.info("Center of mass computation complete.")

    except Exception as e:
        logger.exception(f"Error during center of mass computation: {e}")
        sys.exit(1)


def compute(input_path: Path, output_path: Path, *, range_label: str = "echo_range",
            try_calibrate: bool = False, shown: str | None = None) -> None:
    """Compute CM from ``input_path`` and write it to ``output_path``.

    ``shown`` is the name logged for the output (the gs:// URI when
    ``output_path`` is a staging file).
    """
    import xarray as xr
    from echopype.metrics import center_of_mass

    # Load quietly to keep stdout clean for piping
    f = io.StringIO()
    with redirect_stdout(f):
        ds = xr.open_dataset(input_path)

    # Ensure we have a range variable
    have_range = range_label in ds.variables or range_label in ds.coords

    if not have_range and try_calibrate:
        logger.info(f"'{range_label}' not found; attempting to compute Sv to obtain it...")
        import echopype as ep

        # Try opening as converted and computing Sv; this typically adds 'echo_range'
        ed = ep.open_converted(str(input_path))
        ds = ep.calibrate.compute_Sv(ed)
        have_range = range_label in ds.variables or range_label in ds.coords

    if not have_range:
        logger.error(
            f"Required range label '{range_label}' not found in Dataset. "
            f"Consider using --try-calibrate if the file is an Echopype-converted product."
        )
        sys.exit(1)

    logger.info("Computing center of mass (COM)...")
    da_com = center_of_mass(ds=ds, range_label=range_label)

    # Package into a Dataset for output.
    out_ds = da_com.to_dataset(name="center_of_mass")
    # Fresh attrs: the result can inherit the range variable's attrs
    # (long_name, units, history) and those don't describe this metric.
    out_ds["center_of_mass"].attrs = {"long_name": "Center of Mass of Backscatter", "units": "m"}
    out_ds.attrs.setdefault("source_tool", "aa-center-of-mass")
    out_ds.attrs.setdefault("range_label", range_label)

    _add_basic_attrs(out_ds)

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Saving center of mass to {shown or output_path} ...")
    out_ds.to_netcdf(output_path, mode="w", format="NETCDF4")
    ds.close()


if __name__ == "__main__":
    main()
