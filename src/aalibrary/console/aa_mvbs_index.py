#!/usr/bin/env python3
"""
aa-mvbs-index

Console tool for computing MVBS (Mean Volume Backscattering Strength)
using *index binning* with Echopype, from a calibrated Sv NetCDF file.

This wraps:
    echopype.commongrid.compute_MVBS_index_binning(
        ds_Sv, range_sample_num=<int>, ping_num=<int>
    )

Pipeline-friendly: reads input path (or gs:// URI) from positional arg or
stdin, writes output path to stdout, all logs to stderr. The output
carries the input's provenance plus this binning step.

Typical pipeline usage:
    aa-nc --sonar_model EK60 input.raw | aa-sv | aa-mvbs-index
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
# Without this, any exception in process_file disappears silently and
# the pipeline downstream gets no input — a confusing failure mode.
logger.add(sys.stderr, level="WARNING")

import argparse
import pprint
from pathlib import Path

from aalibrary.console._core import (
    Help, Run, ToolSpec, add_common_flags, canon, naming, render, show_help, stdio,
)

# Recorded in the provenance when the input had no Sv and this tool
# calibrated it itself.
IMPLICIT_SV_STEP = "echopype.calibrate.compute_Sv (defaults)"

SPEC = ToolSpec(
    name="aa-mvbs-index",
    role="transform",
    kind="mvbs",
    op="echopype.commongrid.compute_MVBS_index_binning",
    op_version=1,
    params={
        "range_sample_num": canon.integer,
        "ping_num": canon.integer,
    },
)

HELP = Help(
    summary="MVBS binned by ping and sample counts (not seconds, metres).",
    does=(
        "Runs echopype.commongrid.compute_MVBS_index_binning: averages Sv in the "
        "linear domain over blocks of --ping-num pings x --range-sample-num range "
        "samples (the last block of each axis may be shorter). Output: Sv (block "
        "means, dB) and echo_range (each block's smallest range) on channel x "
        "ping_time x range_sample. Unlike aa-mvbs, bins are counts, not metres "
        "and seconds."
    ),
    stdin=(
        "One .nc/.netcdf4 path or gs:// URI: a flat Sv file from aa-sv or aa-clean, "
        "or an EchoData file from aa-nc (then Sv is first computed with "
        "echopype.calibrate.compute_Sv defaults, EK60/AZFP only; this is recorded "
        "in the provenance)."
    ),
    stdout="The MVBS file's absolute path (or gs:// URI).",
    options=[
        ("-o, --output_path PATH", "Explicit output, used as given with the extension "
                                   "forced to .nc. Local path or gs:// URI."),
        ("--range-sample-num N", "range samples per bin (default: 100)"),
        ("--ping-num N", "pings per bin (default: 100)"),
    ],
    science={
        "range_sample_num": "Range samples per bin.",
        "ping_num": "Pings per bin.",
    },
    files=(
        "Reads NetCDF, local or gs://. Writes <base>_<hash8>.nc beside the input "
        "(current directory for gs:// input), or in --dest DIR|gs://PREFIX, or at -o. "
        "AA_NAMING=legacy restores the old default <input stem>_mvbs_index.nc. An "
        "identical earlier result is reused."
    ),
    pipeline=(
        "aa-nc | aa-sv [| aa-clean] | aa-mvbs-index | aa-graph. Averages the "
        "variable named Sv: after aa-clean that is still the uncorrected Sv."
    ),
    examples=[
        "aa-nc D20160703-T060000.raw --sonar_model EK60 | aa-sv | aa-mvbs-index",
        "aa-mvbs-index sv.nc --range-sample-num 30 --ping-num 5",
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
    Usage: aa-mvbs-index [OPTIONS] [INPUT_PATH]

    Arguments:
    INPUT_PATH                  Path (or gs:// URI) to the calibrated Sv .nc /
                                .netcdf4 file, or a converted Echopype file
                                that can be calibrated to Sv.
                                Optional. Defaults to stdin if not provided.

    Options:
    -o, --output_path           Path (or gs:// URI) to save the MVBS dataset,
                                used as given with a .nc suffix forced.
                                Default: <base>_<hash>.nc beside the input
                                (AA_NAMING=legacy: <stem>_mvbs_index.nc).

    --range-sample-num INT      Number of samples per bin along
                                'range_sample'. Default: 100
    --ping-num INT              Number of pings per bin along the ping
                                axis. Default: 100

    --base NAME                 Base name for the output.
    --dest DIR|gs://PREFIX      Write the default-named output there.
    --force                     Recompute even if an identical product exists.

    Description:
    Computes MVBS by binning along the index-based axes (range_sample
    and ping number). This is distinct from physical-unit binning
    (meters/seconds), which is what compute_MVBS uses. The output path
    is printed to stdout for piping into the next stage of the pipeline.

    If the input has no 'Sv' variable (e.g. the EchoData file from aa-nc),
    Sv is computed first with echopype.calibrate.compute_Sv defaults; the
    provenance records this as an implicit step. Provenance (the input's
    chain plus this step) is embedded in the output; see aa-metadata.

    Example:
        aa-mvbs-index /path/to/input_Sv.nc --range-sample-num 30 \\
                      --ping-num 5 -o /path/to/mvbs.nc
    """
    print(help_text)


def _build_parser():
    parser = argparse.ArgumentParser(
        description="Compute MVBS via index binning from a calibrated Sv NetCDF.",
        add_help=False,
    )
    parser.add_argument(
        "input_path",
        type=str,
        nargs="?",
        help="Path or gs:// URI of the .nc / .netcdf4 file containing Sv.",
    )
    parser.add_argument(
        "-o", "--output_path",
        type=str,
        help="Path to save processed output (extension forced to .nc).",
    )
    parser.add_argument(
        "--range-sample-num",
        dest="range_sample_num",
        type=int,
        default=100,
        help="Number of samples per bin along range_sample (default: 100).",
    )
    parser.add_argument(
        "--ping-num",
        dest="ping_num",
        type=int,
        default=100,
        help="Number of pings per bin (default: 100).",
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
    # Validate input
    # ---------------------------
    token = stdio.one_input(args.input_path, SPEC.name)
    run = Run(SPEC, args)
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
    # '-o' keeps its old rule: used as given, extension forced to .nc.
    explicit = naming.with_ext(args.output_path, ".nc") if args.output_path else None
    out = run.plan(
        ext=".nc",
        explicit=explicit,
        legacy=lambda: naming.with_stem_suffix(src.local, "_mvbs_index", ".nc"),
    )

    # Guard against clobbering the input
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
            "range_sample_num": args.range_sample_num,
            "ping_num": args.ping_num,
            "product": out.hash,
        }
        logger.debug(
            f"Executing aa-mvbs-index configured with [OPTIONS]:\n"
            f"{pprint.pformat(args_summary)}"
        )

        calibrated_here = process_file(
            input_path=src.local,
            output_path=out.local,
            range_sample_num=args.range_sample_num,
            ping_num=args.ping_num,
        )

        logger.success(
            f"Generated {out.target} with aa-mvbs-index. "
            "Passing .nc path to stdout..."
        )
        # Pipe the output path (or URI) to stdout for the next tool
        run.finish(out, extra={"implicit_step": IMPLICIT_SV_STEP} if calibrated_here else None)

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
    range_sample_num: int = 100,
    ping_num: int = 100,
) -> bool:
    """Load Sv from NetCDF, compute MVBS via index binning, and save.

    Returns True when the input had no Sv and it was computed here.
    """
    import echopype as ep  # deferred so --help stays fast
    import xarray as xr
    from echopype.commongrid import compute_MVBS_index_binning

    logger.info(f"Loading dataset from {input_path}")
    ds = xr.open_dataset(input_path)

    # If 'Sv' isn't present, fall back to calibrating from a converted
    # Echopype file. Mirrors aa-sv's contract so this tool can sit
    # anywhere downstream of aa-nc in a pipeline — e.g. directly after
    # aa-nc when the user wants a quick MVBS without a separate aa-sv step.
    calibrated_here = False
    if "Sv" not in ds.data_vars:
        logger.info("No 'Sv' variable found; calibrating to Sv via Echopype")
        ds.close()
        ed = ep.open_converted(str(input_path))
        ds = ep.calibrate.compute_Sv(ed)
        calibrated_here = True

    logger.info(
        f"Computing MVBS with index binning "
        f"(range_sample_num={range_sample_num}, ping_num={ping_num})"
    )
    ds_mvbs = compute_MVBS_index_binning(
        ds_Sv=ds,
        range_sample_num=range_sample_num,
        ping_num=ping_num,
    )
    ds_mvbs = clean_attrs(ds_mvbs)

    output_path = Path(output_path).with_suffix(".nc")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Saving MVBS (index binning) dataset to {output_path}")
    ds_mvbs.to_netcdf(output_path, mode="w", format="NETCDF4")
    logger.success(f"MVBS computation complete: {output_path.resolve()}")
    return calibrated_here


if __name__ == "__main__":
    main()
