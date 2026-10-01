#!/usr/bin/env python3
"""
aa-mvbs

Console tool for computing MVBS (Mean Volume Backscattering Strength) from
a Sv (volume backscattering strength) NetCDF dataset using
echopype.commongrid.compute_MVBS, and saving the result back to NetCDF.

Pipeline-friendly: reads input path (or gs:// URI) from positional arg or
stdin, writes output path to stdout, all logs to stderr. The output
carries the input's provenance plus this binning step.

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

import argparse
import ast
import math
import pprint
import signal
from pathlib import Path

from aalibrary.console._core import (
    Help, Run, ToolSpec, add_common_flags, canon, naming, render, show_help, stdio,
)


# Pipeline tools should die cleanly when the downstream end of the pipe
# closes early (`... | head -n 1`), not throw BrokenPipeError. Guarded
# with hasattr because SIGPIPE doesn't exist on Windows.
if hasattr(signal, "SIGPIPE"):
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)


# flox options that pick an algorithm or backend, not the result. They are
# passed through to flox but kept out of the product hash.
_FLOX_PERFORMANCE_KEYS = {"engine", "method", "reindex"}


def _flox_science(value):
    """Canonical flox kwargs for the hash: every key except engine/method/reindex."""
    if value is None:
        return None
    parsed = value if isinstance(value, dict) else _parse_flox_kwargs(value)
    kept = {k: v for k, v in parsed.items() if k not in _FLOX_PERFORMANCE_KEYS}
    return canon.normalize(kept) or None


SPEC = ToolSpec(
    name="aa-mvbs",
    role="transform",
    kind="mvbs",
    op="echopype.commongrid.compute_MVBS",
    op_version=1,
    params={
        "range_var": canon.choice(),
        "range_bin": canon.quantity("m"),
        "ping_time_bin": canon.quantity("s"),
        "skipna": canon.boolean,
        "fill_value": canon.number,
        "closed": canon.choice(),
        "range_var_max": canon.quantity(),
        "flox_kwargs": _flox_science,
        # Not hashed: --method and --reindex choose how flox computes the
        # same averages (performance), not what they are.
    },
    # flox does the binned reduction, so its major.minor is part of the
    # computation (like echopype's).
    engines=("echopype", "flox"),
)

HELP = Help(
    summary="Average Sv onto a regular range x time grid (MVBS).",
    does=(
        "Runs echopype.commongrid.compute_MVBS: averages Sv in the linear domain "
        "over bins of --range_bin metres of echo_range (or depth) and "
        "--ping_time_bin of ping_time. Output variable: Sv (the bin means, in dB) "
        "on channel x ping_time x echo_range (or depth), each coordinate being the "
        "bin's start."
    ),
    stdin=(
        "One flat Sv .nc/.netcdf4 path or gs:// URI, from aa-sv or aa-clean (not "
        "the EchoData file from aa-nc). --range_var depth needs a depth variable: "
        "run aa-depth first."
    ),
    stdout="The MVBS file's absolute path (or gs:// URI).",
    options=[
        ("-o, --output_path PATH", "Explicit output; '_mvbs' is ALWAYS appended to its "
                                   "stem and .nc forced (-o out.nc writes out_mvbs.nc). "
                                   "Local path or gs:// URI."),
        ("--range_var echo_range|depth", "range coordinate to bin (default: echo_range)"),
        ("--range_bin 20m", "range bin size, metres (default: 20m)"),
        ("--ping_time_bin 20s", "time bin size, a pandas frequency such as 20s or "
                                "1min (default: 20s)"),
        ("--skipna / --no_skipna", "ignore NaN samples in the means (default: skip)"),
        ("--fill_value X", "value for empty bins, in LINEAR sv: echopype converts it "
                           "to dB (1e-12 -> -120 dB, 0 -> -inf; default: NaN)"),
        ("--closed left|right", "closed side of each bin (default: left)"),
        ("--range_var_max 150m", "bin only up to this range (default: data maximum)"),
        ("--flox_kwargs K=V ...", "extra flox options, e.g. min_count=5"),
        ("--method map-reduce|coarsen|block", "flox strategy; performance only, not "
                                              "hashed (default: map-reduce)"),
        ("--reindex", "flox reindexing; performance only, not hashed; map-reduce only"),
    ],
    science={
        "range_var": "Range coordinate binned: echo_range or depth.",
        "range_bin": "Range bin size; '20m' and '20.0 m' are the same value.",
        "ping_time_bin": "Time bin size; '20s' and '20.0s' are the same value.",
        "skipna": "Skip NaN samples in the bin means (--skipna / --no_skipna).",
        "fill_value": "Value of empty bins, in linear sv (converted to dB with the "
                      "means); default NaN, recorded as \"NaN\".",
        "closed": "Closed side of each bin interval.",
        "range_var_max": "Upper end of the range bins.",
        "flox_kwargs": "Extra flox options, except engine, method and reindex, which "
                       "only change speed.",
    },
    files=(
        "Reads a flat Sv NetCDF, local or gs://. Writes <base>_<hash8>.nc beside "
        "the input (current directory for gs:// input), or in --dest DIR|gs://PREFIX, "
        "or at -o (+'_mvbs'). AA_NAMING=legacy restores the old default "
        "<input stem>_mvbs.nc. An identical earlier result is reused."
    ),
    pipeline=(
        "aa-nc | aa-sv [| aa-clean] | aa-mvbs | aa-graph. Averages the variable named "
        "Sv: after aa-clean that is still the uncorrected Sv (aa-clean puts the "
        "cleaned values in Sv_corrected)."
    ),
    examples=[
        "aa-nc D20160703-T060000.raw --sonar_model EK60 | aa-sv | aa-mvbs",
        "aa-mvbs sv.nc --range_bin 5m --ping_time_bin 1min",
        "aa-sv ed.nc | aa-depth | aa-mvbs --range_var depth --range_bin 10m",
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
    Usage: aa-mvbs [OPTIONS] [INPUT_PATH]

    Arguments:
    INPUT_PATH                  Path (or gs:// URI) to a Sv .nc / .netcdf4 file
                                (typically the output of aa-sv or aa-clean).
                                Optional. Defaults to stdin if not provided.

    Options:
    -o, --output_path           Path (or gs:// URI) to save processed output.
                                '_mvbs' is ALWAYS appended to its stem and a
                                .nc suffix forced (-o out.nc writes
                                out_mvbs.nc), so the input file is never
                                silently overwritten.
                                Default: <base>_<hash>.nc beside the input
                                (AA_NAMING=legacy: <stem>_mvbs.nc).

    --range_var                 Range coordinate to bin over.
                                Choices: echo_range, depth
                                Default: echo_range

    --range_bin                 Bin size along the range dimension.
                                Default: 20m

    --ping_time_bin             Bin size along the ping_time dimension.
                                Default: 20s

    --method                    Computation method for binning (flox
                                strategy; changes speed, not values).
                                Choices: map-reduce, coarsen, block
                                Default: map-reduce

    --reindex                   Reindex the result to match uniform bin edges.
                                Only valid with --method map-reduce.
                                Default: False (omit the flag).

    --skipna                    Skip NaN values when averaging (default).
    --no_skipna                 Include NaN values in mean calculations.

    --fill_value                Fill value for empty bins, in linear sv
                                units: it is converted to dB with the bin
                                means (1e-12 gives -120 dB, 0 gives -inf,
                                a negative value gives NaN).
                                Default: NaN

    --closed                    Which side of the bin interval is closed.
                                Choices: left, right
                                Default: left

    --range_var_max             Optional maximum value for range_var.
                                Default: None

    --flox_kwargs               Extra flox kwargs as KEY=VALUE pairs.
                                Values are parsed safely via ast.literal_eval.
                                Example: --flox_kwargs min_count=5

    --base NAME                 Base name for the output.
    --dest DIR|gs://PREFIX      Write the default-named output there.
    --force                     Recompute even if an identical product exists.

    Description:
    Computes MVBS (Mean Volume Backscattering Strength) from a Sv NetCDF
    using echopype.commongrid.compute_MVBS. Data are binned along range
    and ping_time dimensions with a configurable reduction method.

    The expected input is a flat Sv NetCDF (the output of aa-sv, optionally
    after aa-clean). It is NOT the multi-group EchoData NetCDF produced by
    aa-nc. The variable averaged is Sv; after aa-clean that is still the
    uncorrected Sv (the cleaned values are in Sv_corrected).

    Provenance (the input's chain plus this step) is embedded in the
    output; see aa-metadata.

    Pipeline example:
        aa-nc --sonar_model EK60 input.raw | aa-sv | aa-mvbs

    Direct example (writes /path/to/output_mvbs.nc; --range_var depth
    needs a depth variable, e.g. from aa-depth):
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


def _build_parser():
    parser = argparse.ArgumentParser(
        description="Compute MVBS (Mean Volume Backscattering Strength) from a Sv NetCDF using Echopype.",
        add_help=False,
    )
    parser.add_argument(
        "input_path",
        type=str,
        nargs="?",
        help="Path or gs:// URI of a Sv .nc / .netcdf4 file.",
    )
    parser.add_argument(
        "-o", "--output_path",
        type=str,
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
    # --no_skipna is declared first only so the generated --help lists this
    # option as --skipna; parsing is the same (both default to True, and the
    # last of the two flags on the command line wins).
    parser.add_argument(
        "--no_skipna", "--no-skipna",
        dest="skipna",
        action="store_false",
        help="Include NaN values in mean calculations.",
    )
    parser.add_argument(
        "--skipna",
        dest="skipna",
        action="store_true",
        default=True,
        help="Skip NaN values when averaging (default).",
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
    add_common_flags(parser)
    return parser


def main():
    # No args on a terminal: help. (An empty pipe is an error: see stdio.)
    if len(sys.argv) == 1 and not stdio.stdin_is_piped():
        print_help()
        sys.exit(0)

    parser = _build_parser()
    if show_help(SPEC, HELP, parser, full=print_help_full):
        sys.exit(0)

    args = parser.parse_args()

    # echopype.commongrid.compute_MVBS refuses any reindex value other than
    # None unless method == 'map-reduce'. Say so up front.
    if args.reindex and args.method != "map-reduce":
        logger.error(f"--reindex can only be used with --method map-reduce "
                     f"(got --method {args.method}).")
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
    # Validate input
    # ---------------------------
    token = stdio.one_input(args.input_path, SPEC.name)
    run = Run(SPEC, args, params={"flox_kwargs": _flox_science(flox_kwargs)})
    src = run.input(token)

    allowed_extensions = {".netcdf4", ".nc"}
    ext = src.local.suffix.lower()
    if ext not in allowed_extensions:
        logger.error(
            f"'{src.name}' is not a supported file type. "
            f"Allowed: {', '.join(sorted(allowed_extensions))}"
        )
        sys.exit(1)

    # ---------------------------
    # Resolve output path
    # ---------------------------
    # '-o' keeps its old rule: '_mvbs' is always appended and .nc forced.
    explicit = (naming.with_stem_suffix(args.output_path, "_mvbs", ".nc")
                if args.output_path else None)
    out = run.plan(
        ext=".nc",
        explicit=explicit,
        legacy=lambda: naming.with_stem_suffix(src.local, "_mvbs", ".nc"),
    )

    if not out.remote and Path(out.target).resolve() == src.local.resolve():
        logger.error(f"Refusing to overwrite input file: {src.local.resolve()}")
        sys.exit(1)

    if run.reusable(out):
        run.finish(out)
        return

    # ---------------------------
    # Process file
    # ---------------------------
    try:
        args_summary = {
            "input": token,
            "output": out.target,
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
            "product": out.hash,
        }
        logger.debug(
            f"Executing aa-mvbs configured with [OPTIONS]:\n"
            f"{pprint.pformat(args_summary)}"
        )

        process_file(
            input_path=src.local,
            output_path=out.local,
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
            f"Generated {out.target} with aa-mvbs. "
            "Passing .nc path to stdout..."
        )
        run.finish(out)

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
    import echopype as ep  # deferred so --help stays fast
    import xarray as xr

    logger.info(f"Loading Sv dataset from {input_path}")
    ds_Sv = xr.open_dataset(input_path)

    # compute_MVBS raises for any reindex other than None when method is
    # not 'map-reduce' (its default, False, included). The previous version
    # always passed reindex=False, so --method coarsen|block always failed.
    reindex_arg = reindex if method == "map-reduce" else None

    try:
        logger.info(
            f"Computing MVBS (range_var={range_var}, range_bin={range_bin}, "
            f"ping_time_bin={ping_time_bin}, method={method}, "
            f"reindex={reindex_arg}, skipna={skipna}, fill_value={fill_value}, "
            f"closed={closed}, range_var_max={range_var_max}, "
            f"flox_kwargs={flox_kwargs or {}})"
        )
        ds_mvbs = ep.commongrid.compute_MVBS(
            ds_Sv,
            range_var=range_var,
            range_bin=range_bin,
            ping_time_bin=ping_time_bin,
            method=method,
            reindex=reindex_arg,
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

    output_path = Path(output_path).with_suffix(".nc")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Saving MVBS dataset to {output_path}")
    ds_mvbs.to_netcdf(output_path, mode="w", format="NETCDF4")
    logger.success(f"MVBS computation complete: {output_path.resolve()}")


if __name__ == "__main__":
    main()
