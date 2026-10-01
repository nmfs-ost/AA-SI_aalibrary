#!/usr/bin/env python3
"""
aa-nasc

Console tool for computing NASC (Nautical Area Scattering Coefficient) from
a Sv (volume backscattering strength) NetCDF dataset using
echopype.commongrid.compute_NASC, and saving the result back to NetCDF.

compute_NASC needs depth, latitude and longitude in the Sv dataset, which
aa-sv does not write: add them with aa-depth and aa-location first.

Pipeline-friendly: reads input path (or gs:// URI) from positional arg or
stdin, writes output path to stdout, all logs to stderr. The output
carries the input's provenance plus this integration step.

Typical pipeline usage:
    aa-nc --sonar_model EK60 input.raw          # -> input.nc (EchoData)
    aa-sv input.nc | aa-depth | aa-location --echodata input.nc | aa-nasc
"""

# === Silence logs BEFORE any heavy imports ===
import logging
import sys
import warnings

logging.disable(logging.CRITICAL)
warnings.filterwarnings("ignore")

from loguru import logger
logger.remove()
# Default sink: WARNING+ to stderr so real errors aren't swallowed but
# the pipeline stdout stays clean for the next tool's input.
logger.add(sys.stderr, level="WARNING")

import argparse
import ast
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
    name="aa-nasc",
    role="transform",
    kind="nasc",
    op="echopype.commongrid.compute_NASC",
    op_version=1,
    params={
        "range_bin": canon.quantity("m"),
        "dist_bin": canon.quantity(),
        "skipna": canon.boolean,
        "closed": canon.choice(),
        "flox_kwargs": _flox_science,
        # Not hashed: --method chooses how flox computes the same sums
        # (performance), not what they are.
    },
    # flox does the binned reduction, so its major.minor is part of the
    # computation (like echopype's).
    engines=("echopype", "flox"),
)

HELP = Help(
    summary="Integrate Sv into NASC (m2 nmi-2) on depth x distance cells.",
    does=(
        "Runs echopype.commongrid.compute_NASC: bins Sv by --range_bin metres of "
        "depth and --dist_bin of along-track distance (from latitude/longitude), "
        "then NASC = mean sv x mean cell height x 4 pi 1852^2 per cell. "
        "Output: NASC on channel x distance x depth (bin starts; distance in nmi), "
        "plus mean ping_time, latitude and longitude per distance bin."
    ),
    stdin=(
        "One flat Sv .nc/.netcdf4 path or gs:// URI that has depth, latitude and "
        "longitude. aa-sv's output has none of these: run aa-depth and aa-location "
        "first (see IN A PIPELINE)."
    ),
    stdout="The NASC file's absolute path (or gs:// URI).",
    options=[
        ("-o, --output_path PATH", "Explicit output; '_nasc' is ALWAYS appended to its "
                                   "stem and .nc forced (-o out.nc writes out_nasc.nc). "
                                   "Local path or gs:// URI."),
        ("--range_bin 10m", "depth bin size, metres (default: 10m)"),
        ("--dist_bin 0.5nmi", "distance bin size, nautical miles (default: 0.5nmi)"),
        ("--skipna / --no_skipna", "ignore NaN samples in the means (default: skip)"),
        ("--closed left|right", "closed side of each bin (default: left)"),
        ("--flox_kwargs K=V ...", "extra flox options, e.g. min_count=5"),
        ("--method NAME", "flox strategy; performance only, not hashed "
                          "(default: map-reduce)"),
    ],
    science={
        "range_bin": "Depth bin size; '10m' and '10.0 m' are the same value.",
        "dist_bin": "Distance bin size; '0.5nmi' and '.5 nmi' are the same value.",
        "skipna": "Skip NaN samples in the bin means (--skipna / --no_skipna).",
        "closed": "Closed side of each bin interval.",
        "flox_kwargs": "Extra flox options, except engine, method and reindex, which "
                       "only change speed.",
    },
    files=(
        "Reads a flat Sv NetCDF, local or gs://. Writes <base>_<hash8>.nc beside "
        "the input (current directory for gs:// input), or in --dest DIR|gs://PREFIX, "
        "or at -o (+'_nasc'). AA_NAMING=legacy restores the old default "
        "<input stem>_nasc.nc. An identical earlier result is reused."
    ),
    pipeline=(
        "aa-sv ED.nc | aa-depth | aa-location --echodata ED.nc | aa-nasc, where ED.nc "
        "is the EchoData file from aa-nc (aa-location reads the GPS from it). "
        "Integrates the variable named Sv: after aa-clean that is still the "
        "uncorrected Sv."
    ),
    examples=[
        "aa-nc D20160703-T060000.raw --sonar_model EK60     # writes D20160703-T060000.nc",
        "aa-sv D20160703-T060000.nc | aa-depth \\",
        "    | aa-location --echodata D20160703-T060000.nc | aa-nasc",
        "aa-nasc sv_depth_loc.nc --range_bin 20m --dist_bin 1nmi",
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
    Usage: aa-nasc [OPTIONS] [INPUT_PATH]

    Arguments:
    INPUT_PATH                  Path (or gs:// URI) to a Sv .nc / .netcdf4 file
                                that contains depth, latitude and longitude
                                (aa-sv output after aa-depth and aa-location).
                                Optional. Defaults to stdin if not provided.

    Options:
    -o, --output_path           Path (or gs:// URI) to save processed output.
                                '_nasc' is ALWAYS appended to its stem and a
                                .nc suffix forced (-o out.nc writes
                                out_nasc.nc), so the input file is never
                                silently overwritten.
                                Default: <base>_<hash>.nc beside the input
                                (AA_NAMING=legacy: <stem>_nasc.nc).

    --range_bin                 Depth bin size, e.g. "10m".
                                Default: 10m

    --dist_bin                  Horizontal distance bin size, e.g. "0.5nmi".
                                Default: 0.5nmi

    --method                    Flox reduction strategy (changes speed, not
                                values).
                                Default: map-reduce

    --skipna                    Skip NaN values when averaging. (Default.)
    --no_skipna                 Include NaN values in mean calculations.

    --closed                    Which side of the bin interval is closed.
                                Choices: left, right
                                Default: left

    --flox_kwargs               Extra flox kwargs as KEY=VALUE pairs.
                                Values are parsed safely via ast.literal_eval,
                                so '5' becomes int, 'true' is treated as
                                a string (use 'True' for the bool), and
                                anything that doesn't parse as a literal
                                is kept as a plain string.
                                Example: --flox_kwargs min_count=5 engine=numpy

    --base NAME                 Base name for the output.
    --dest DIR|gs://PREFIX      Write the default-named output there.
    --force                     Recompute even if an identical product exists.

    Description:
    Computes NASC (Nautical Area Scattering Coefficient) from a Sv NetCDF
    using echopype.commongrid.compute_NASC. NASC integrates Sv across
    range and distance bins, producing a standardized measure for biomass
    estimation.

    The expected input is a flat Sv NetCDF (the output of aa-sv, optionally
    after aa-clean) with depth, latitude and longitude added by aa-depth
    and aa-location. It is NOT the multi-group EchoData NetCDF produced by
    aa-nc. Provenance (the input's chain plus this step) is embedded in the
    output; see aa-metadata.

    Pipeline example (ED.nc is the EchoData file written by aa-nc):
        aa-nc --sonar_model EK60 input.raw
        aa-sv ED.nc | aa-depth | aa-location --echodata ED.nc | aa-nasc

    Direct example (writes /path/to/output_nasc.nc):
        aa-nasc /path/to/input_Sv.nc --range_bin 20m --dist_bin 1nmi \\
                --method map-reduce -o /path/to/output.nc
    """
    print(help_text)


def _parse_flox_kwargs(pair_list):
    """Parse a list of 'key=value' strings into a dict.

    Values are parsed via ast.literal_eval (safe — no exec/eval of
    expressions), with a fallback to plain string when the value isn't a
    Python literal. The previous version used eval(), which was a
    code-execution hole on user input.
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
        description="Compute NASC (Nautical Area Scattering Coefficient) from a Sv NetCDF using Echopype.",
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
        help="Path to save processed output. '_nasc' is appended to the stem.",
    )
    parser.add_argument(
        "--range_bin", "--range-bin",
        dest="range_bin",
        type=str,
        default="10m",
        help="Depth bin size in meters (default: 10m).",
    )
    parser.add_argument(
        "--dist_bin", "--dist-bin",
        dest="dist_bin",
        type=str,
        default="0.5nmi",
        help="Horizontal distance bin size in nautical miles (default: 0.5nmi).",
    )
    parser.add_argument(
        "--method",
        type=str,
        default="map-reduce",
        help="Flox reduction strategy (default: map-reduce).",
    )
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
        "--closed",
        type=str,
        choices=["left", "right"],
        default="left",
        help="Which side of the bin interval is closed (default: left).",
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

    # ---------------------------
    # Parse flox kwargs (single pass, safe)
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
    # '-o' keeps its old rule: '_nasc' is always appended and .nc forced.
    explicit = (naming.with_stem_suffix(args.output_path, "_nasc", ".nc")
                if args.output_path else None)
    out = run.plan(
        ext=".nc",
        explicit=explicit,
        legacy=lambda: naming.with_stem_suffix(src.local, "_nasc", ".nc"),
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
            "range_bin": args.range_bin,
            "dist_bin": args.dist_bin,
            "method": args.method,
            "skipna": args.skipna,
            "closed": args.closed,
            "flox_kwargs": flox_kwargs,
            "product": out.hash,
        }
        logger.debug(
            f"Executing aa-nasc configured with [OPTIONS]:\n"
            f"{pprint.pformat(args_summary)}"
        )

        process_file(
            input_path=src.local,
            output_path=out.local,
            range_bin=args.range_bin,
            dist_bin=args.dist_bin,
            method=args.method,
            skipna=args.skipna,
            closed=args.closed,
            flox_kwargs=flox_kwargs,
        )

        logger.success(
            f"Generated {out.target} with aa-nasc. "
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
    range_bin: str = "10m",
    dist_bin: str = "0.5nmi",
    method: str = "map-reduce",
    skipna: bool = True,
    closed: str = "left",
    flox_kwargs: dict = None,
):
    """Load a Sv NetCDF, compute NASC, and save the result.

    The expected input is a flat Sv dataset (output of aa-sv, optionally
    cleaned via aa-clean) with depth, latitude and longitude added, NOT a
    multi-group EchoData file from aa-nc. The previous version of this
    script computed Sv internally via ep.calibrate.compute_Sv, which
    duplicated work that aa-sv already performs in the standard pipeline.
    """
    import echopype as ep  # deferred so --help stays fast
    import xarray as xr

    logger.info(f"Loading Sv dataset from {input_path}")
    ds_Sv = xr.open_dataset(input_path)

    try:
        logger.info(
            f"Computing NASC (range_bin={range_bin}, dist_bin={dist_bin}, "
            f"method={method}, skipna={skipna}, closed={closed}, "
            f"flox_kwargs={flox_kwargs or {}})"
        )
        ds_nasc = ep.commongrid.compute_NASC(
            ds_Sv,
            range_bin=range_bin,
            dist_bin=dist_bin,
            method=method,
            skipna=skipna,
            closed=closed,
            **(flox_kwargs or {}),
        )

        ds_nasc = clean_attrs(ds_nasc)

        # Materialize before the source handle closes — to_netcdf is
        # otherwise lazy via the dask graph rooted at ds_Sv.
        ds_nasc.load()
    finally:
        ds_Sv.close()

    output_path = Path(output_path).with_suffix(".nc")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Saving NASC dataset to {output_path}")
    ds_nasc.to_netcdf(output_path, mode="w", format="NETCDF4")
    logger.success(f"NASC computation complete: {output_path.resolve()}")


if __name__ == "__main__":
    main()
