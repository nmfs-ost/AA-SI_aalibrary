#!/usr/bin/env python3
"""
aa-coerce-time

Console tool to coerce a time coordinate so it always increases, using
Echopype's qc function `coerce_increasing_time`.

This wraps:
  echopype.qc.coerce_increasing_time(ds, time_name='ping_time', win_len=100)

Notes:
- echopype edits the time coordinate in place. Under pandas 3 the array
  behind an indexed coordinate is read-only, so the tool runs echopype on a
  writable copy of the coordinate and puts the corrected values back.
- A dataset without reversed timestamps is written unchanged (echopype
  itself raises IndexError in that case).
- --report prints whether time reversals existed before and after the fix.

Pipeline-friendly: reads the input path (or gs:// URI) from the positional
argument or stdin, writes the output path to stdout, all logs to stderr.
The output carries the input's provenance plus this step.
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

# numpy / xarray / echopype are imported inside the functions that use them
# so --help stays fast.

SPEC = ToolSpec(
    name="aa-coerce-time",
    role="transform",
    kind="sv",
    op="echopype.qc.coerce_increasing_time",
    op_version=1,
    params={"time_name": canon.text, "win_len": canon.integer},
)

HELP = Help(
    summary="Make a time coordinate strictly increasing (echopype.qc.coerce_increasing_time).",
    does=(
        "Finds pings whose timestamp jumps backwards and replaces each backward "
        "step with the median ping interval of the preceding --win-len pings; "
        "later intervals are kept, so the times after a reversal shift forward "
        "by the same amount. Only the time coordinate changes; no data values "
        "move. A file without reversals is written unchanged. The product kind is "
        "the input's (sv, mvbs, ...)."
    ),
    stdin="One NetCDF path or gs:// URI with the time coordinate (Sv, MVBS, ...).",
    stdout="The output file's absolute path (or gs:// URI).",
    options=[
        ("--time-name NAME", "the time coordinate to fix (default ping_time)"),
        ("--win-len N", "pings before a reversal used for the median interval (default 100)"),
        ("--report", "say on stderr whether reversals existed before/after"),
        ("--no-overwrite", "exit 1 instead of replacing a different existing output "
                           "(replacing is the default)"),
        ("-o, --output_path PATH", "Explicit output, used exactly as given. Local path or "
                                   "gs:// URI."),
    ],
    science={
        "time_name": "Time coordinate to coerce.",
        "win_len": "Window (pings) for the median ping interval.",
    },
    files=(
        "Reads NetCDF, local or gs://. Writes <base>_<hash>.nc beside the input "
        "(current directory for gs:// input), or -o, or --dest. An identical "
        "earlier result is reused. AA_NAMING=legacy: <stem>_timefix.nc."
    ),
    pipeline="Anywhere after aa-sv, typically before aa-mvbs: ... | aa-sv | aa-coerce-time | aa-mvbs ...",
    examples=[
        "aa-nc D20160703-T060000.raw --sonar_model EK60 | aa-sv | aa-coerce-time --report",
        "aa-coerce-time Sv.nc --time-name ping_time --win-len 120 -o Sv_timefix.nc",
    ],
)


def print_help():
    """Curated help (also used by the docs generator)."""
    sys.stdout.write(render(SPEC, HELP, _build_parser()))


def print_help_full():
    help_text = """
    Usage: aa-coerce-time [OPTIONS] [INPUT_PATH]

    Arguments:
      INPUT_PATH                   Path (or gs:// URI) to a NetCDF file (.nc) whose
                                   time coordinate may contain local reversals.
                                   Optional; defaults to reading a single token
                                   from stdin.

    Options:
      -o, --output_path PATH       Output NetCDF path, used exactly as given (local
                                   path or gs:// URI). Default: <base>_<hash>.nc
                                   beside the input (AA_NAMING=legacy:
                                   <stem>_timefix.nc).
      --time-name STR              Name of the time coordinate to coerce (default: ping_time).
      --win-len INT                Local window length used to infer the next ping time
                                   when a reversal is detected (default: 100).
      --report                     Print a short report on time reversals before/after
                                   (to stderr).
      --no-overwrite               Do not overwrite an existing, different output file
                                   (exit 1). An identical product is reused.
      --base NAME                  Base name for the output.
      --dest DIR|gs://PREFIX       Write the default-named output there.
      --force                      Recompute even if an identical product exists.
      -h, --help                   Show the short help; --help-all shows this text.

    Description:
      Detects and fixes local backward jumps in a datetime coordinate by enforcing
      a monotonically increasing series (forward-only time). Each backward step is
      replaced by the median interval of the preceding --win-len pings; the later
      intervals are preserved. A file without reversals is written unchanged.
      Provenance (the input's chain plus this step) is embedded in the output;
      see aa-metadata.

    Example:
      aa-coerce-time pingdata.nc --time-name ping_time --win-len 120 --report -o pingdata_timefix.nc
    """
    print(help_text)


def _build_parser():
    parser = argparse.ArgumentParser(
        description="Coerce a time coordinate to be strictly increasing.",
        add_help=False,  # help is handled by show_help()
    )

    # ---------------------------
    # Positional / IO args
    # ---------------------------
    parser.add_argument(
        "input_path",
        type=str,
        nargs="?",
        help="Path or gs:// URI of a NetCDF file containing the time coordinate to fix.",
    )
    parser.add_argument(
        "-o", "--output_path",
        type=str,
        help="Output NetCDF path, used as given.",
    )

    # ---------------------------
    # coerce_increasing_time params
    # ---------------------------
    parser.add_argument("--time-name", dest="time_name", default="ping_time",
                        help="Name of the time coordinate to coerce (default: ping_time).")
    parser.add_argument("--win-len", dest="win_len", type=int, default=100,
                        help="Local window length for inferring next ping time (default: 100).")

    # Behavior flags
    parser.add_argument("--report", action="store_true",
                        help="Print whether time reversals exist before/after.")
    parser.add_argument("--no-overwrite", action="store_true",
                        help="Do not overwrite an existing output file.")
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


def _exists(target: str) -> bool:
    return uris.stat(target) is not None if uris.is_gcs(target) else Path(target).exists()


def _note(message: str) -> None:
    """A line for the user on stderr (independent of the log level)."""
    print(f"{SPEC.name}: {message}", file=sys.stderr)


def main():
    """Entry point for the aa-coerce-time CLI."""
    # No args on a terminal: help. (An empty pipe is an error: see stdio.)
    if len(sys.argv) == 1 and not stdio.stdin_is_piped():
        print_help()
        sys.exit(0)

    parser = _build_parser()
    if show_help(SPEC, HELP, parser, full=print_help_full):
        sys.exit(0)

    args = parser.parse_args()
    args.time_name = args.time_name.strip()   # used exactly as hashed

    # ---------------------------
    # Resolve / validate input
    # ---------------------------
    token = stdio.one_input(args.input_path, SPEC.name)
    run = Run(SPEC, args)
    src = run.input(token)

    out = run.plan(
        ext=".nc",
        kind=_inherited_kind(src),
        explicit=args.output_path or None,
        legacy=lambda: src.local.with_stem(src.local.stem + "_timefix").with_suffix(".nc"),
    )

    # Guard against clobbering the input.
    if not out.remote and Path(out.target).resolve() == src.local.resolve():
        logger.error(f"Refusing to overwrite input file: {src.local.resolve()}")
        sys.exit(1)

    # An identical product is never an "overwrite": reuse it first.
    if run.reusable(out):
        run.finish(out)
        return

    if args.no_overwrite and _exists(out.target):
        logger.error(f"Output file '{out.target}' exists and --no-overwrite was set.")
        sys.exit(1)

    try:
        logger.debug(f"\naa-coerce-time args:\n"
                     f"{pprint.pformat(dict(vars(args), output=out.target, product=out.hash))}")

        process_file(
            input_path=src.local,
            output_path=out.local,
            time_name=args.time_name,
            win_len=args.win_len,
            report=args.report,
        )

        logger.info("Time coercion complete.")
        run.finish(out)

    except Exception as e:
        logger.exception(f"Error during time coercion: {e}")
        sys.exit(1)


def coerce_time(ds, time_name: str, win_len: int):
    """``ds`` with ``time_name`` made increasing by echopype; ``ds`` is not modified.

    echopype.qc.coerce_increasing_time writes the corrected times back into
    the dataset in place (echopype 0.10: ``ds[time_name].data[:] = ...``).
    Under pandas 3 the array behind an indexed coordinate such as ping_time
    is read-only, so that assignment fails. Run echopype on a scratch
    dataset holding a writable copy of the values as a plain (unindexed)
    variable, then put the result back as the coordinate, keeping its
    attributes and encoding. Works the same with echopype versions that
    replace the variable instead of writing into it.
    """
    import numpy as np
    import xarray as xr
    from echopype.qc import coerce_increasing_time

    da = ds[time_name]
    if da.ndim != 1:
        raise ValueError(f"'{time_name}' must be one-dimensional (has dims {da.dims}).")
    values = np.array(da.values, copy=True)          # writable, owned
    scratch = xr.Dataset({time_name: (("_aa_time",), values)})
    coerce_increasing_time(ds=scratch, time_name=time_name, win_len=win_len)
    fixed = np.asarray(scratch[time_name].values)
    if fixed.shape != values.shape:
        raise RuntimeError("echopype returned a time series of a different length.")
    new = xr.Variable(da.dims, fixed, attrs=dict(da.attrs), encoding=dict(da.encoding))
    if time_name in ds.coords:
        return ds.assign_coords({time_name: new})
    return ds.assign({time_name: new})


def process_file(input_path: Path, output_path: Path, time_name: str = "ping_time",
                 win_len: int = 100, report: bool = False):
    """Load, coerce the time coordinate to increase, and save to NetCDF."""
    import xarray as xr
    from echopype.qc import exist_reversed_time

    # Suppress any library chatter to stdout so pipelines remain clean.
    f = io.StringIO()
    with redirect_stdout(f):
        ds = xr.open_dataset(input_path)

    if time_name not in ds.variables:
        raise KeyError(f"time coordinate '{time_name}' not found in {Path(input_path).name}")

    # echopype raises IndexError when there is nothing to fix, so check first.
    had_reversal = bool(exist_reversed_time(ds, time_name))
    if report:
        _note(f"Time reversal present before fix? {had_reversal}")

    if had_reversal:
        logger.info(f"Coercing '{time_name}' to increasing with win_len={win_len} ...")
        ds = coerce_time(ds, time_name, win_len)
    else:
        _note(f"no reversed timestamps in '{time_name}'; values unchanged.")

    if report:
        try:
            has_reversal_after = exist_reversed_time(ds, time_name)
            _note(f"Time reversal present after fix?  {has_reversal_after}")
        except Exception as e:
            logger.warning(f"Could not check for reversed time after fix: {e}")

    # Clean attributes to avoid None in NetCDF
    _add_basic_attrs(ds)

    logger.info("Saving coerced dataset ...")
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    ds.to_netcdf(output_path, mode="w", format="NETCDF4")


if __name__ == "__main__":
    main()
