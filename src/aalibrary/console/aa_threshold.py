#!/usr/bin/env python3
"""
aa-threshold

Apply minimum and maximum Sv thresholds: the Echoview "Threshold" operator.

    --min -70   samples weaker than -70 dB become empty water (-999 dB,
                linear zero: they still count in averages as nothing), or
                no data (NaN) with --below nodata
    --max -30   samples stronger than -30 dB become no data (NaN), or empty
                water with --above empty, or -30 dB with --above clip

Empty water and no data differ in every average taken later (aa-mvbs,
aa-nasc, aa-integrate): empty water is a zero in the mean, no data is left
out of it. That is the same distinction Echoview draws.
"""

from __future__ import annotations

import logging
import sys
import warnings

logging.disable(logging.CRITICAL)
warnings.filterwarnings("ignore")

from loguru import logger  # noqa: E402

logger.remove()
logger.add(sys.stderr, level="WARNING")

import argparse  # noqa: E402

from aalibrary.console._core import (  # noqa: E402
    Help, Run, ToolSpec, add_common_flags, canon, render, show_help, stdio,
)

EMPTY_DB = -999.0

SPEC = ToolSpec(
    name="aa-threshold",
    role="transform",
    kind="sv",
    op="aa_threshold.apply",
    op_version=1,
    params={
        "min": canon.number,
        "max": canon.number,
        "below": canon.choice("lower"),
        "above": canon.choice("lower"),
        "var": canon.text,
    },
)

HELP = Help(
    summary="Minimum / maximum Sv thresholds (Echoview's Threshold operator).",
    does=(
        "Applies the thresholds to the values variable (Sv by default, or --var) "
        "and leaves every other variable as it is.\n\n"
        "Below --min: empty water (-999 dB, a linear zero that still counts in "
        "averages) or, with --below nodata, NaN (left out of averages). Above "
        "--max: NaN (default), empty water (--above empty), or the --max value "
        "itself (--above clip). NaN in the input stays NaN."
    ),
    stdin="One Sv / MVBS / TS product (.nc or .zarr) path or gs:// URI.",
    stdout="The thresholded product's path (or gs:// URI).",
    options=[
        ("--min DB", "minimum threshold, dB"),
        ("--max DB", "maximum threshold, dB"),
        ("--below empty|nodata", "what a sample below --min becomes (default empty)"),
        ("--above nodata|empty|clip", "what a sample above --max becomes (default nodata)"),
        ("--var NAME", "the values variable (default Sv, Sv_corrected, MVBS, TS...)"),
        ("-o, --output_path PATH", "exact output; local or gs://"),
    ],
    science={
        "min": "Minimum threshold, dB.",
        "max": "Maximum threshold, dB.",
        "below": "What samples below --min become.",
        "above": "What samples above --max become.",
        "var": "The variable thresholded (as resolved).",
    },
    files=(
        "Reads NetCDF or Zarr, local or gs://. Writes <base>_<hash8>.nc beside the "
        "input (current directory for gs:// input), or -o, or --dest."
    ),
    pipeline="... | aa-sv | aa-clean | aa-threshold --min -70 | aa-mvbs",
    examples=[
        "aa-threshold sv.nc --min -70",
        "aa-threshold sv.nc --min -80 --max -30 --above clip",
    ],
)


def print_help():
    sys.stdout.write(render(SPEC, HELP, _build_parser()))


def _build_parser():
    p = argparse.ArgumentParser(prog="aa-threshold", add_help=False,
                                description="Minimum and maximum Sv thresholds.")
    p.add_argument("input_path", type=str, nargs="?")
    p.add_argument("-o", "--output_path", type=str, default=None)
    p.add_argument("--min", type=float, default=None, metavar="DB")
    p.add_argument("--max", type=float, default=None, metavar="DB")
    p.add_argument("--below", choices=["empty", "nodata"], default="empty")
    p.add_argument("--above", choices=["nodata", "empty", "clip"], default="nodata")
    p.add_argument("--var", type=str, default=None)
    p.add_argument("--quiet", action="store_true")
    add_common_flags(p)
    return p


def threshold(ds, var: str, *, vmin=None, vmax=None, below="empty", above="nodata"):
    import numpy as np

    da = ds[var]
    out = da
    if vmin is not None:
        replacement = EMPTY_DB if below == "empty" else np.nan
        out = out.where(~(out < vmin), replacement)
    if vmax is not None:
        replacement = {"nodata": np.nan, "empty": EMPTY_DB, "clip": vmax}[above]
        out = out.where(~(out > vmax), replacement)
    out.attrs = dict(da.attrs)
    if vmin is not None:
        out.attrs["aa_threshold_min"] = float(vmin)
    if vmax is not None:
        out.attrs["aa_threshold_max"] = float(vmax)
    result = ds.copy()
    result[var] = out
    return result


def main() -> None:
    if len(sys.argv) == 1 and not stdio.stdin_is_piped():
        print_help()
        sys.exit(0)
    parser = _build_parser()
    if show_help(SPEC, HELP, parser):
        sys.exit(0)
    args = parser.parse_args()
    if args.min is None and args.max is None:
        print("aa-threshold: give --min and/or --max", file=sys.stderr)
        sys.exit(2)
    if args.min is not None and args.max is not None and args.min >= args.max:
        print("aa-threshold: --min must be below --max", file=sys.stderr)
        sys.exit(2)

    from aalibrary.console import _flat

    token = stdio.one_input(args.input_path, SPEC.name)
    probe = Run(SPEC, args)
    src = probe.input(token)
    try:
        with _flat.open_flat(src.local) as head:
            var = _flat.value_var(head, args.var)
    except (ValueError, OSError) as exc:
        print(f"aa-threshold: {exc}", file=sys.stderr)
        sys.exit(1)
    # The thresholds that do nothing are not hashed as if they did.
    params = {"var": var,
              "below": args.below if args.min is not None else None,
              "above": args.above if args.max is not None else None}
    run = Run(SPEC, args, params=params)
    run.input(token)
    out = run.plan(ext=".nc", explicit=args.output_path or None, kind=_flat.kind_of(src))
    if run.reusable(out):
        run.finish(out)
        return
    try:
        ds = _flat.open_flat(src.local, chunks={})
        result = threshold(ds, var, vmin=args.min, vmax=args.max,
                           below=args.below, above=args.above)
        _flat.write_netcdf(result, out.local)
    except Exception as exc:  # noqa: BLE001
        run.discard(out)
        logger.exception(f"aa-threshold: {exc}")
        sys.exit(1)
    run.finish(out)


if __name__ == "__main__":
    main()
