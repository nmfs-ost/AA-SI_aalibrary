#!/usr/bin/env python3
"""
aa-mvbs

Console tool for computing MVBS (Mean Volume Backscattering Strength) from
a Sv (volume backscattering strength) NetCDF dataset using
echopype.commongrid.compute_MVBS, and saving the result back to NetCDF.

Pipeline-friendly: reads input path from positional arg or stdin, writes
output path to stdout, all logs to stderr.

Typical pipeline usage:
    aa-nc --sonar_model EK60 input.raw | aa-sv | aa-mvbs
    aa-nc --sonar_model EK60 input.raw | aa-sv | aa-clean | aa-mvbs
"""

# === Silence logs BEFORE any heavy imports ===
import logging
import sys
import warnings

logging.disable(logging.CRITICAL)
warnings.filterwarnings("ignore")

from loguru import logger
logger.remove()
logger.add(sys.stderr, level="WARNING")

# Now the heavy imports — anything they log gets squashed
import argparse
import ast
import math
import pprint
import signal
from pathlib import Path

import xarray as xr
import echopype as ep


# Pipeline tools should die cleanly when the downstream end of the pipe
# closes early (`... | head -n 1`), not throw BrokenPipeError. Guarded
# with hasattr because SIGPIPE doesn't exist on Windows.
if hasattr(signal, "SIGPIPE"):
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)


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
    Usage: aa-mvbs [OPTIONS] [INPUT_PATH]

    Arguments:
    INPUT_PATH                  Path to a Sv .nc / .netcdf4 file
                                (typically the output of aa-sv or aa-clean).
                                Optional. Defaults to stdin if not provided.

    Options:
    -o, --output_path           Path to save processed output.
                                Default: same directory as input, with
                                '_mvbs' appended to the stem and a .nc
                                suffix. '_mvbs' is ALWAYS appended, so the
                                input file is never silently overwritten.

    --range_var                 Range coordinate to bin over.
                                Choices: echo_range, depth
                                Default: echo_range

    --range_bin                 Bin size along the range dimension.
                                Default: 20m

    --ping_time_bin             Bin size along the ping_time dimension.
                                Default: 20s

    --method                    Computation method for binning.
                                Choices: map-reduce, coarsen, block
                                Default: map-reduce

    --reindex                   Reindex the result to match uniform bin edges.
                                Default: False (omit the flag).

    --skipna                    Skip NaN values when averaging (default).
    --no_skipna                 Include NaN values in mean calculations.

    --fill_value                Fill value for empty bins.
                                Default: NaN

    --closed                    Which side of the bin interval is closed.
                                Choices: left, right
                                Default: left

    --range_var_max             Optional maximum value for range_var.
                                Default: None

    --flox_kwargs               Extra flox kwargs as KEY=VALUE pairs.
                                Values are parsed safely via ast.literal_eval.
                                Example: --flox_kwargs min_count=5

    Description:
    Computes MVBS (Mean Volume Backscattering Strength) from a Sv NetCDF
    using echopype.commongrid.compute_MVBS. Data are binned along range
    and ping_time dimensions with a configurable reduction method.

    The expected input is a flat Sv NetCDF (the output of aa-sv, optionally
    after aa-clean). It is NOT the multi-group EchoData NetCDF produced by
    aa-nc.

    Pipeline example:
        aa-nc --sonar_model EK60 input.raw | aa-sv | aa-mvbs

    Direct example:
        aa-mvbs /path/to/input_Sv.nc --range_var depth --range_bin 50m \\
                --ping_time_bin 60s --method coarsen -o /path/to/output.nc
    """
    print(help_text)


def _parse_flox_kwargs(pair_list):
    """Parse a list of 'key=value' strings into a dict.

    Values are parsed via ast.literal_eval (safe — no exec/eval of
    expressions), with a fallback to plain string when the value isn't a
    Python literal. Without this, every value arrived at flox as a
    string, silently breaking any numeric kwarg like 'min_count=5'.
    """
    if not pair_list:
        return {}

    out = {}
    for pair in pair_list:
        if "=" not in pair:
            raise argparse.ArgumentTypeError(
                f"Invalid --flox_kwargs entry '{pair}'. Expected KEY=VALUE."
            )
        k, v = pair.split("=", 1)
        k = k.strip()
        v = v.strip()
        if not k:
            raise argparse.ArgumentTypeError(
                f"Invalid --flox_kwargs entry '{pair}'. Empty key."
            )
        try:
            out[k] = ast.literal_eval(v)
        except (ValueError, SyntaxError):
            out[k] = v
    return out


def main():
    # Stdin / no-args handling
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
        description="Compute MVBS (Mean Volume Backscattering Strength) from a Sv NetCDF using Echopype.",
        add_help=False,
    )

    parser.add_argument(
        "input_path",
        type=Path,
        nargs="?",
        help="Path to a Sv .nc / .netcdf4 file.",
    )
    parser.add_argument(
        "-o", "--output_path",
        type=Path,
        help="Path to save processed output. '_mvbs' is appended to the stem.",
    )
    parser.add_argument(
        "--range_var",
        type=str,
        choices=["echo_range", "depth"],
        default="echo_range",
        help="Range coordinate to bin over (default: echo_range).",
    )
    parser.add_argument(
        "--range_bin",
        type=str,
        default="20m",
        help="Bin size along range dimension (default: 20m).",
    )
    parser.add_argument(
        "--ping_time_bin",
        type=str,
        default="20s",
        help="Bin size along ping_time dimension (default: 20s).",
    )
    parser.add_argument(
        "--method",
        type=str,
        choices=["map-reduce", "coarsen", "block"],
        default="map-reduce",
        help="Computation method for binning (default: map-reduce).",
    )
    parser.add_argument(
        "--reindex",
        action="store_true",
        default=False,
        help="If set, reindex the result to match uniform bin edges (default: False).",
    )
    # The previous version declared --skipna with action='store_true' and no
    # default=, so its actual default was False — directly contradicting
    # the help text which claimed "Default: True". Fix: default really is
    # True, and --no_skipna is provided to flip it.
    parser.add_argument(
        "--skipna",
        dest="skipna",
        action="store_true",
        default=True,
        help="Skip NaN values when averaging (default).",
    )
    parser.add_argument(
        "--no_skipna", "--no-skipna",
        dest="skipna",
        action="store_false",
        help="Include NaN values in mean calculations.",
    )
    parser.add_argument(
        "--fill_value",
        type=float,
        default=math.nan,
        help="Fill value for empty bins (default: NaN).",
    )
    parser.add_argument(
        "--closed",
        type=str,
        choices=["left", "right"],
        default="left",
        help="Which side of the bin interval is closed (default: left).",
    )
    parser.add_argument(
        "--range_var_max",
        type=str,
        default=None,
        help="Optional maximum value for range_var (default: None).",
    )
    parser.add_argument(
        "--flox_kwargs", "--flox-kwargs",
        dest="flox_kwargs",
        nargs="*",
        default=[],
        metavar="KEY=VALUE",
        help="Extra flox kwargs as KEY=VALUE pairs. Example: --flox_kwargs min_count=5",
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

    allowed_extensions = {".netcdf4", ".nc"}
    ext = args.input_path.suffix.lower()
    if ext not in allowed_extensions:
        logger.error(
            f"'{args.input_path.name}' is not a supported file type. "
            f"Allowed: {', '.join(sorted(allowed_extensions))}"
        )
        sys.exit(1)

    # ---------------------------
    # Resolve output path
    # ---------------------------
    if args.output_path is None:
        args.output_path = args.input_path

    args.output_path = args.output_path.with_stem(args.output_path.stem + "_mvbs")
    args.output_path = args.output_path.with_suffix(".nc")

    if args.output_path.resolve() == args.input_path.resolve():
        logger.error(f"Refusing to overwrite input file: {args.input_path.resolve()}")
        sys.exit(1)

    # ---------------------------
    # Parse flox kwargs (safe)
    # ---------------------------
    try:
        flox_kwargs = _parse_flox_kwargs(args.flox_kwargs)
    except argparse.ArgumentTypeError as e:
        logger.error(str(e))
        sys.exit(1)

    # ---------------------------
    # Process file
    # ---------------------------
    try:
        args_summary = {
            "input_path": args.input_path,
            "output_path": args.output_path,
            "range_var": args.range_var,
            "range_bin": args.range_bin,
            "ping_time_bin": args.ping_time_bin,
            "method": args.method,
            "reindex": args.reindex,
            "skipna": args.skipna,
            "fill_value": args.fill_value,
            "closed": args.closed,
            "range_var_max": args.range_var_max,
            "flox_kwargs": flox_kwargs,
        }
        logger.debug(
            f"Executing aa-mvbs configured with [OPTIONS]:\n"
            f"{pprint.pformat(args_summary)}"
        )

        process_file(
            input_path=args.input_path,
            output_path=args.output_path,
            range_var=args.range_var,
            range_bin=args.range_bin,
            ping_time_bin=args.ping_time_bin,
            method=args.method,
            reindex=args.reindex,
            skipna=args.skipna,
            fill_value=args.fill_value,
            closed=args.closed,
            range_var_max=args.range_var_max,
            flox_kwargs=flox_kwargs,
        )

        logger.success(
            f"Generated {args.output_path.resolve()} with aa-mvbs. "
            "Passing .nc path to stdout..."
        )
        print(args.output_path.resolve())

    except Exception as e:
        logger.exception(f"Error during processing: {e}")
        sys.exit(1)


def clean_attrs(ds):
    """Replace None-valued attrs with 'NA' so the dataset is NetCDF-safe.
    NetCDF attrs cannot be None — to_netcdf will raise on serialization."""
    for k, v in ds.attrs.items():
        if v is None:
            ds.attrs[k] = "NA"
    for var in ds.data_vars:
        for k, v in ds[var].attrs.items():
            if v is None:
                ds[var].attrs[k] = "NA"
    return ds


def process_file(
    input_path: Path,
    output_path: Path,
    range_var: str = "echo_range",
    range_bin: str = "20m",
    ping_time_bin: str = "20s",
    method: str = "map-reduce",
    reindex: bool = False,
    skipna: bool = True,
    fill_value: float = math.nan,
    closed: str = "left",
    range_var_max: str = None,
    flox_kwargs: dict = None,
):
    """Load a Sv NetCDF, compute MVBS, and save the result.

    The expected input is a flat Sv dataset (output of aa-sv, optionally
    cleaned via aa-clean), NOT a multi-group EchoData file from aa-nc.
    """

    logger.info(f"Loading Sv dataset from {input_path}")
    ds_Sv = xr.open_dataset(input_path)

    try:
        logger.info(
            f"Computing MVBS (range_var={range_var}, range_bin={range_bin}, "
            f"ping_time_bin={ping_time_bin}, method={method}, "
            f"reindex={reindex}, skipna={skipna}, fill_value={fill_value}, "
            f"closed={closed}, range_var_max={range_var_max}, "
            f"flox_kwargs={flox_kwargs or {}})"
        )
        ds_mvbs = ep.commongrid.compute_MVBS(
            ds_Sv,
            range_var=range_var,
            range_bin=range_bin,
            ping_time_bin=ping_time_bin,
            method=method,
            reindex=reindex,
            skipna=skipna,
            fill_value=fill_value,
            closed=closed,
            range_var_max=range_var_max,
            **(flox_kwargs or {}),
        )

        # The previous version skipped clean_attrs entirely on the MVBS
        # output, despite defining the helper. None-valued attrs would
        # then blow up to_netcdf at serialization time.
        ds_mvbs = clean_attrs(ds_mvbs)

        ds_mvbs.load()
    finally:
        ds_Sv.close()

    output_path = output_path.with_suffix(".nc")
    logger.info(f"Saving MVBS dataset to {output_path}")
    ds_mvbs.to_netcdf(output_path, mode="w", format="NETCDF4")
    logger.success(f"MVBS computation complete: {output_path.resolve()}")


if __name__ == "__main__":
    main()