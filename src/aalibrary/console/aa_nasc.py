#!/usr/bin/env python3
"""
aa-nasc

Console tool for computing NASC (Nautical Area Scattering Coefficient) from
a Sv (volume backscattering strength) NetCDF dataset using
echopype.commongrid.compute_NASC, and saving the result back to NetCDF.

Pipeline-friendly: reads input path from positional arg or stdin, writes
output path to stdout, all logs to stderr.

Typical pipeline usage:
    aa-nc --sonar_model EK60 input.raw | aa-sv | aa-nasc
    aa-nc --sonar_model EK60 input.raw | aa-sv | aa-clean | aa-nasc
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

# Now the heavy imports — anything they log gets squashed
import argparse
import ast
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
    Usage: aa-nasc [OPTIONS] [INPUT_PATH]

    Arguments:
    INPUT_PATH                  Path to a Sv .nc / .netcdf4 file
                                (typically the output of aa-sv or aa-clean).
                                Optional. Defaults to stdin if not provided.

    Options:
    -o, --output_path           Path to save processed output.
                                Default: same directory as input, with
                                '_nasc' appended to the stem and a .nc
                                suffix. '_nasc' is ALWAYS appended, so the
                                input file is never silently overwritten.

    --range_bin                 Depth bin size, e.g. "10m".
                                Default: 10m

    --dist_bin                  Horizontal distance bin size, e.g. "0.5nmi".
                                Default: 0.5nmi

    --method                    Flox reduction strategy.
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

    Description:
    Computes NASC (Nautical Area Scattering Coefficient) from a Sv NetCDF
    using echopype.commongrid.compute_NASC. NASC integrates Sv across
    range and distance bins, producing a standardized measure for biomass
    estimation.

    The expected input is a flat Sv NetCDF (the output of aa-sv, optionally
    after aa-clean). It is NOT the multi-group EchoData NetCDF produced by
    aa-nc.

    Pipeline example:
        aa-nc --sonar_model EK60 input.raw | aa-sv | aa-nasc

    Direct example:
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


def main():
    # Stdin / no-args handling
    if len(sys.argv) == 1:
        if not sys.stdin.isatty():
            stdin_data = sys.stdin.readline().strip()
            if stdin_data:
                sys.argv.append(stdin_data)
            else:
                # Stdin was piped but empty — bail with help instead of
                # falling through and crashing on Path("").exists().
                print_help()
                sys.exit(0)
        else:
            print_help()
            sys.exit(0)

    if "--help" in sys.argv or "-h" in sys.argv:
        print_help()
        sys.exit(0)

    parser = argparse.ArgumentParser(
        description="Compute NASC (Nautical Area Scattering Coefficient) from a Sv NetCDF using Echopype.",
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

    args.output_path = args.output_path.with_stem(args.output_path.stem + "_nasc")
    args.output_path = args.output_path.with_suffix(".nc")

    if args.output_path.resolve() == args.input_path.resolve():
        logger.error(f"Refusing to overwrite input file: {args.input_path.resolve()}")
        sys.exit(1)

    # ---------------------------
    # Parse flox kwargs (single pass, safe)
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
            "range_bin": args.range_bin,
            "dist_bin": args.dist_bin,
            "method": args.method,
            "skipna": args.skipna,
            "closed": args.closed,
            "flox_kwargs": flox_kwargs,
        }
        logger.debug(
            f"Executing aa-nasc configured with [OPTIONS]:\n"
            f"{pprint.pformat(args_summary)}"
        )

        process_file(
            input_path=args.input_path,
            output_path=args.output_path,
            range_bin=args.range_bin,
            dist_bin=args.dist_bin,
            method=args.method,
            skipna=args.skipna,
            closed=args.closed,
            flox_kwargs=flox_kwargs,
        )

        logger.success(
            f"Generated {args.output_path.resolve()} with aa-nasc. "
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
    range_bin: str = "10m",
    dist_bin: str = "0.5nmi",
    method: str = "map-reduce",
    skipna: bool = True,
    closed: str = "left",
    flox_kwargs: dict = None,
):
    """Load a Sv NetCDF, compute NASC, and save the result.

    The expected input is a flat Sv dataset (output of aa-sv, optionally
    cleaned via aa-clean), NOT a multi-group EchoData file from aa-nc.
    The previous version of this script computed Sv internally via
    ep.calibrate.compute_Sv, which duplicated work that aa-sv already
    performs in the standard pipeline.
    """

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

    output_path = output_path.with_suffix(".nc")
    logger.info(f"Saving NASC dataset to {output_path}")
    ds_nasc.to_netcdf(output_path, mode="w", format="NETCDF4")
    logger.success(f"NASC computation complete: {output_path.resolve()}")


if __name__ == "__main__":
    main()