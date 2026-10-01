#!/usr/bin/env python3
"""
aa-impulse

Console tool for locating impulse noise in calibrated Sv data with Echopype
(echopype.clean.mask_impulse_noise) and saving an impulse-noise mask (and,
with --apply, a copy of the Sv data with the flagged samples set to NaN).

Pipeline-friendly: reads the input path (or gs:// URI) from a positional
arg or stdin, writes the MASK's path to stdout, all logs to stderr. Both
outputs carry the input's provenance plus this step.

Typical pipeline usage:
    aa-nc --sonar_model EK60 input.raw | aa-sv | aa-depth | aa-impulse --apply
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
# Without this, any exception in processing disappears silently and
# the pipeline downstream gets no input — a confusing failure mode.
logger.add(sys.stderr, level="WARNING")

import argparse
import pprint
from pathlib import Path

from aalibrary.console._core import (
    Help, Run, ToolSpec, add_common_flags, canon, naming, render, show_help, stdio,
)

SPEC = ToolSpec(
    name="aa-impulse",
    role="transform",
    kind="mask",
    op="echopype.clean.mask_impulse_noise",
    op_version=1,
    params={
        "depth_bin": canon.quantity("m"),
        "num_side_pings": canon.integer,
        "impulse_noise_threshold": canon.quantity("dB"),
        "range_var": canon.choice(),
        "use_index_binning": canon.boolean,
    },
)

IMPLICIT_SV = {"implicit_step": "echopype.calibrate.compute_Sv (defaults)"}

HELP = Help(
    summary="Impulse-noise mask for Sv; --apply also writes cleaned Sv.",
    does=(
        "Runs echopype.clean.mask_impulse_noise (Ryan et al. 2015). Sv is first "
        "averaged in --depth-bin bins; a sample is impulse noise when it is more "
        "than --impulse-threshold above BOTH the ping --num-side-pings before it "
        "and the ping --num-side-pings after it.\n\n"
        "The mask file holds one variable, impulse_mask: float64, 1 = impulse "
        "noise, 0 = keep, dims (channel, range_sample, ping_time). Samples whose "
        "binned Sv is NaN (e.g. beyond a channel's range) also come out as 1. "
        "echopype copies Sv's long_name/units attributes onto the mask; they do "
        "not describe it."
    ),
    stdin=(
        "One Sv NetCDF (.nc/.netcdf4) path or gs:// URI that has the --range-var "
        "variable: depth (add it with aa-depth) or echo_range. An EchoData file "
        "without Sv is calibrated first with compute_Sv defaults (recorded in the "
        "provenance as an implicit step)."
    ),
    stdout=(
        "The MASK's absolute path (or gs:// URI), one line. With --apply the "
        "cleaned Sv's path goes to stderr instead, as "
        "'aa-impulse: cleaned Sv: PATH' (also when it is reused)."
    ),
    metadata=(
        "Reads the input's provenance, appends this step with its canonical "
        "scientific options, computes the product hash, and embeds it all in "
        "the mask (NetCDF attributes aa_provenance, aa_product_hash, aa_base, "
        "aa_tool, history). The --apply file is its own product: same step, "
        "variant 'apply', kind sv, its own hash. Inspect with: aa-metadata FILE"
    ),
    options=[
        ("-o, --output_path PATH", "The mask file, used as given with the extension "
                                   "forced to .nc. Local path or gs:// URI. Does not "
                                   "move the --apply file."),
        ("--apply", "Also write the input's Sv with impulse samples set to NaN "
                    "(all other variables copied)."),
    ],
    science={
        "depth_bin": "Vertical averaging bin before the comparison, e.g. 5m.",
        "num_side_pings": "Compare each ping with the ping this many pings before "
                          "and after it.",
        "impulse_noise_threshold": "dB above both neighbours that counts as impulse "
                                   "noise, e.g. 10dB.",
        "range_var": "Vertical variable: depth or echo_range.",
        "use_index_binning": "Bin by range_sample index (assumes uniform sample "
                             "spacing per channel). Faster.",
    },
    files=(
        "Reads NetCDF, local or gs://. Writes the mask to <base>_<hash>.nc beside "
        "the input (current directory for gs:// input), or -o, or --dest. With "
        "--apply a second <base>_<hash>.nc (a different hash) goes beside the "
        "input or into --dest, never to -o. AA_NAMING=legacy: "
        "<stem>_impulse_mask.nc and <stem>_impulse_cleaned.nc beside the input. "
        "Identical earlier results are reused."
    ),
    pipeline=(
        "After aa-sv and aa-depth: ... | aa-sv | aa-depth | aa-impulse | aa-graph "
        "(draws the mask). Only the mask travels down the pipe."
    ),
    examples=[
        "aa-nc x.raw --sonar_model EK60 | aa-sv | aa-depth | aa-impulse --apply "
        "--use-index-binning",
        "aa-impulse sv_depth.nc --impulse-threshold 12dB --use-index-binning",
    ],
    notes=[
        "--use-index-binning is needed when the channels cover different depth "
        "ranges (e.g. EK60 38 + 120 kHz): echopype 0.11.1's default depth "
        "binning then fails with \"conflicting sizes for dimension 'depth_bins'\".",
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
    Usage: aa-impulse [OPTIONS] [INPUT_PATH]

    Arguments:
    INPUT_PATH                  Path (or gs:// URI) to the calibrated .nc /
                                .netcdf4 file containing Sv (preferred), or a
                                converted Echopype file that can be calibrated
                                to Sv. Optional. Defaults to stdin if not
                                provided (an empty stdin is an error, exit 1).

    Options:
    -o, --output_path           Path (or gs:// URI) to save the impulse-noise
                                mask (NetCDF); used as given, with the suffix
                                forced to .nc. Default: <base>_<hash>.nc beside
                                the input (AA_NAMING=legacy:
                                <stem>_impulse_mask.nc).

    --apply                     Also apply the mask to Sv (impulse samples set
                                to NaN) and write the result as a second file:
                                <base>_<hash>.nc beside the input or in --dest
                                (AA_NAMING=legacy: <stem>_impulse_cleaned.nc
                                beside the input). -o never moves this file.
                                stdout still gets only the mask path; this
                                file's path goes to stderr ('aa-impulse: cleaned Sv: PATH').

    --depth-bin                 Vertical bin size for comparison, e.g. '5m'.
                                Default: 5m
    --num-side-pings            Compare each ping with the ping this many pings
                                before and after it. Default: 2
    --impulse-threshold         Threshold in dB above both neighbours, e.g.
                                '10.0dB'. Default: 10.0dB
    --range-var                 Name of the range/depth variable: depth or
                                echo_range. Default: depth
    --use-index-binning         Use index-based binning instead of physical
                                units.

    --base NAME                 Base name for the outputs.
    --dest DIR|gs://PREFIX      Write the default-named outputs there.
    --force                     Recompute even if identical products exist.

    Description:
    Creates a mask marking likely impulse-noise "flecks" using a ping-wise
    two-sided comparison in depth-binned windows. The mask variable is
    'impulse_mask', float64 with 1 = impulse noise and 0 = keep, dims
    (channel, range_sample, ping_time). Optionally applies the mask to Sv to
    produce a cleaned Sv dataset. The mask path is printed to stdout for
    piping into the next stage of the pipeline. Provenance (the input's chain
    plus this step) is embedded in both files; see aa-metadata.

    If the input has no 'Sv' variable it is calibrated with
    echopype.calibrate.compute_Sv (defaults) first.

    When channels cover different depth ranges (e.g. EK60 38 + 120 kHz),
    echopype 0.11.1's default depth binning fails ("conflicting sizes for
    dimension 'depth_bins'"); use --use-index-binning.

    Example:
        aa-sv input.nc | aa-depth | aa-impulse --apply --depth-bin 5m \\
              --impulse-threshold 12dB --use-index-binning
    """
    print(help_text)


def _build_parser():
    parser = argparse.ArgumentParser(
        description="Create an impulse-noise mask from Sv with Echopype.",
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
        help="Path to save the mask (suffix forced to .nc). Default: <base>_<hash>.nc.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Also write Sv cleaned by the mask as a second product.",
    )

    # mask_impulse_noise parameters
    parser.add_argument("--depth-bin", dest="depth_bin", default="5m",
                        help="Depth bin size, e.g., '5m' (default: 5m).")
    parser.add_argument("--num-side-pings", dest="num_side_pings", type=int, default=2,
                        help="Number of side pings for two-sided comparison (default: 2).")
    parser.add_argument("--impulse-threshold", dest="impulse_noise_threshold",
                        default="10.0dB",
                        help="Threshold above local context, e.g. '10.0dB' (default: 10.0dB).")
    parser.add_argument("--range-var", dest="range_var", default="depth",
                        help="Range/depth variable name (default: depth).")
    parser.add_argument("--use-index-binning", dest="use_index_binning",
                        action="store_true",
                        help="Use index-based binning rather than physical bin sizes.")
    add_common_flags(parser)
    return parser


def _report_side(side):
    """The --apply file is not on stdout (the mask is); say where it is."""
    print(f"{SPEC.name}: cleaned Sv: {side.target}", file=sys.stderr)


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
    # Resolve output paths
    # ---------------------------
    # The mask: -o used as given with .nc forced, as always.
    explicit = naming.with_ext(args.output_path, ".nc") if args.output_path else None
    out = run.plan(
        ext=".nc",
        explicit=explicit,
        legacy=lambda: naming.with_stem_suffix(src.local, "_impulse_mask", ".nc"),
    )
    # The --apply side output: never moved by -o (legacy: beside the input).
    side = None
    if args.apply:
        side = run.plan(
            ext=".nc",
            variant="apply",
            kind="sv",
            legacy=lambda: naming.with_stem_suffix(src.local, "_impulse_cleaned", ".nc"),
        )

    # Guard against clobbering the input (or one output with the other)
    for o in (out, side):
        if o is not None and not o.remote and Path(o.target).resolve() == src.local.resolve():
            logger.error(f"Refusing to overwrite input file: {src.local.resolve()}")
            sys.exit(1)
    if side is not None and side.target == out.target:
        logger.error(f"-o names the same file as the --apply output: {out.target}")
        sys.exit(1)

    mask_done = run.reusable(out)
    side_done = side is None or run.reusable(side)
    if mask_done and side_done:
        if side is not None:
            run.finish(side, emit=False)
            _report_side(side)
        run.finish(out)
        return

    # ---------------------------
    # Process file
    # ---------------------------
    try:
        args_summary = {
            "input": token,
            "mask": out.target,
            "cleaned": side.target if side is not None else None,
            "depth_bin": args.depth_bin,
            "num_side_pings": args.num_side_pings,
            "impulse_noise_threshold": args.impulse_noise_threshold,
            "range_var": args.range_var,
            "use_index_binning": args.use_index_binning,
            "product": out.hash,
        }
        logger.debug(
            f"Executing aa-impulse configured with [OPTIONS]:\n"
            f"{pprint.pformat(args_summary)}"
        )

        ds, mask, calibrated = compute_mask(
            input_path=src.local,
            depth_bin=args.depth_bin,
            num_side_pings=args.num_side_pings,
            impulse_noise_threshold=args.impulse_noise_threshold,
            range_var=args.range_var,
            use_index_binning=args.use_index_binning,
        )
        extra = IMPLICIT_SV if calibrated else None

        if not mask_done:
            write_mask(mask, out.local)
        run.finish(out, extra=extra, emit=False)

        # The cleaned Sv is a side output, not the pipeline value.
        if side is not None:
            if not side_done:
                write_cleaned(ds, mask, side.local)
            run.finish(side, extra=extra, emit=False)
            _report_side(side)

        logger.success(f"Generated {out.target} with aa-impulse. Passing it to stdout...")
        stdio.emit(out.target)

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


def _noise(mask):
    """Boolean view of the mask, True = impulse noise.

    echopype returns this mask as float64 1.0/0.0, and ``~`` on a float
    array raises TypeError, so it must be made boolean before inverting.
    """
    if mask.dtype == bool:
        return mask
    return mask.fillna(0) != 0


def compute_mask(
    input_path: Path,
    depth_bin: str = "5m",
    num_side_pings: int = 2,
    impulse_noise_threshold: str = "10.0dB",
    range_var: str = "depth",
    use_index_binning: bool = False,
):
    """Load Sv (calibrating if needed) and compute the impulse-noise mask.

    Returns (ds_Sv, mask, calibrated) where ``calibrated`` says whether the
    input had no Sv and compute_Sv was run on it.
    """
    import echopype as ep  # deferred so --help stays fast
    import xarray as xr
    from echopype.clean import mask_impulse_noise

    logger.info(f"Loading dataset from {input_path}")
    ds = xr.open_dataset(input_path)

    # Ensure we have calibrated Sv. If the input is a converted EchoData
    # file rather than an already-calibrated Sv file, fall through to
    # echopype.calibrate.compute_Sv. Mirrors aa-sv's contract so this
    # tool can sit anywhere downstream of aa-nc in a pipeline.
    calibrated = False
    if "Sv" not in ds.data_vars:
        logger.info("No 'Sv' variable found; calibrating to Sv via Echopype")
        ed = ep.open_converted(str(input_path))
        ds = ep.calibrate.compute_Sv(ed)
        calibrated = True

    logger.info("Computing impulse-noise mask")
    mask = mask_impulse_noise(
        ds_Sv=ds,
        depth_bin=depth_bin,
        num_side_pings=num_side_pings,
        impulse_noise_threshold=impulse_noise_threshold,
        range_var=range_var,
        use_index_binning=use_index_binning,
    )
    return ds, mask, calibrated


def write_mask(mask, output_path: Path):
    """Save the mask as variable 'impulse_mask' (polarity as echopype emits it)."""
    # Wrap DataArray into a Dataset for clearer NetCDF structure
    mask_ds = mask.to_dataset(name="impulse_mask")
    mask_ds = clean_attrs(mask_ds)

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Saving impulse-noise mask to {output_path}")
    mask_ds.to_netcdf(output_path, mode="w", format="NETCDF4")


def write_cleaned(ds, mask, output_path: Path):
    """Write Sv with impulse samples set to NaN; every other variable as is."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Applying mask to Sv and writing cleaned Sv to {output_path}")
    ds_clean = ds.copy()
    # Keep values where NOT impulse noise
    ds_clean["Sv"] = (
        ds_clean["Sv"].where(~_noise(mask), other=float("nan"))
        .transpose(*ds["Sv"].dims)
    )
    ds_clean = clean_attrs(ds_clean)
    ds_clean.to_netcdf(output_path, mode="w", format="NETCDF4")


if __name__ == "__main__":
    main()
