#!/usr/bin/env python3
"""
aa-transient

Console tool for locating transient noise in calibrated Sv data with Echopype
(echopype.clean.mask_transient_noise) and saving a transient-noise mask (and,
with --apply, a copy of the Sv data with the flagged samples set to NaN).

Pipeline-friendly: reads the input path (or gs:// URI) from a positional
arg or stdin, writes the MASK's path to stdout, all logs to stderr. Both
outputs carry the input's provenance plus this step.

Typical pipeline usage:
    aa-nc --sonar_model EK60 input.raw | aa-sv | aa-depth | aa-transient --apply
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
    name="aa-transient",
    role="transform",
    kind="mask",
    op="echopype.clean.mask_transient_noise",
    op_version=1,
    # --chunk is dask chunking (performance only) and is deliberately absent.
    params={
        "func": canon.choice(),
        "depth_bin": canon.quantity("m"),
        "num_side_pings": canon.integer,
        "exclude_above": canon.quantity("m"),
        "transient_noise_threshold": canon.quantity("dB"),
        "range_var": canon.choice(),
        "use_index_binning": canon.boolean,
    },
)

IMPLICIT_SV = {"implicit_step": "echopype.calibrate.compute_Sv (defaults)"}

HELP = Help(
    summary="Transient-noise mask for Sv; --apply also writes cleaned Sv.",
    does=(
        "Runs echopype.clean.mask_transient_noise (Ryan et al. 2015). Each sample "
        "is compared with the pooled Sv (--func, in linear units) of its "
        "neighbourhood: +/- --depth-bin vertically and +/- --num-side-pings "
        "pings. A sample more than --transient-threshold above that pool is "
        "transient noise. Samples shallower than --exclude-above (plus "
        "--depth-bin without index binning) are never flagged.\n\n"
        "The mask file holds one variable, transient_mask: boolean, True = "
        "transient noise, False = keep, dims (channel, ping_time, range_sample). "
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
        "'aa-transient: cleaned Sv: PATH' (also when it is reused)."
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
        ("--apply", "Also write the input's Sv with transient samples set to NaN "
                    "(all other variables copied)."),
        ("--chunk KEY=VAL ...", "Dask chunk sizes for the index-binning pooling, "
                                "e.g. ping_time=256 range_sample=512 (dims of Sv). "
                                "Only used with --use-index-binning; performance "
                                "only, not hashed. Put INPUT_PATH before --chunk "
                                "(or end the list with --)."),
    ],
    science={
        "func": "Pooling function: nanmean or nanmedian (the only two echopype "
                "0.11.1 accepts; nanmedian is much slower).",
        "depth_bin": "Vertical half-height of the pooling window, e.g. 10m.",
        "num_side_pings": "Pings on each side in the pooling window.",
        "exclude_above": "Never flag samples shallower than this, e.g. 250m.",
        "transient_noise_threshold": "dB above the pooled Sv that counts as "
                                     "transient noise, e.g. 12dB.",
        "range_var": "Vertical variable: depth or echo_range.",
        "use_index_binning": "Pool by range_sample index (assumes uniform sample "
                             "spacing per channel). Much faster.",
    },
    files=(
        "Reads NetCDF, local or gs://. Writes the mask to <base>_<hash>.nc beside "
        "the input (current directory for gs:// input), or -o, or --dest. With "
        "--apply a second <base>_<hash>.nc (a different hash) goes beside the "
        "input or into --dest, never to -o. AA_NAMING=legacy: "
        "<stem>_transient_mask.nc and <stem>_transient_cleaned.nc beside the "
        "input. Identical earlier results are reused."
    ),
    pipeline=(
        "After aa-sv and aa-depth: ... | aa-sv | aa-depth | aa-transient | aa-graph "
        "(draws the mask). Only the mask travels down the pipe."
    ),
    examples=[
        "aa-nc x.raw --sonar_model EK60 | aa-sv | aa-depth | aa-transient "
        "--use-index-binning --exclude-above 20m",
        "aa-transient sv_depth.nc --apply --exclude-above 20m --use-index-binning",
    ],
    notes=[
        "Without --use-index-binning echopype 0.11.1 pools sample by sample in "
        "Python: minutes even for a small file.",
        "Keep --exclude-above (default 250m) inside the data's depth range, e.g. "
        "--exclude-above 20m. With --use-index-binning, echopype 0.11.1 takes the "
        "cut from the first ping of the first channel: if no depth at all is "
        "deeper it excludes nothing (shallow samples can be flagged); if that "
        "ping is too short but other data go deeper, it fails with "
        "\"overlapping depth ... larger than your array\". The tool warns before "
        "either happens.",
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
    Usage: aa-transient [OPTIONS] [INPUT_PATH]

    Arguments:
    INPUT_PATH                  Path (or gs:// URI) to the calibrated .nc /
                                .netcdf4 file containing Sv (preferred), or a
                                converted Echopype file that can be calibrated
                                to Sv. Optional. Defaults to stdin if not
                                provided (an empty stdin is an error, exit 1).

    Options:
    -o, --output_path           Path (or gs:// URI) to save the transient-noise
                                mask (NetCDF); used as given, with the suffix
                                forced to .nc. Default: <base>_<hash>.nc beside
                                the input (AA_NAMING=legacy:
                                <stem>_transient_mask.nc).

    --apply                     Also apply the mask to Sv (transient samples set
                                to NaN) and write the result as a second file:
                                <base>_<hash>.nc beside the input or in --dest
                                (AA_NAMING=legacy: <stem>_transient_cleaned.nc
                                beside the input). -o never moves this file.
                                stdout still gets only the mask path; this
                                file's path goes to stderr ('aa-transient: cleaned Sv: PATH').

    --func                      Pooling function: 'nanmean' or 'nanmedian'
                                (the only two echopype accepts).
                                Default: nanmean
    --depth-bin                 Vertical half-height of the pooling window,
                                e.g. '10m'. Default: 10m
    --num-side-pings            Pings on each side for the pooling window.
                                Default: 25
    --exclude-above             Exclude depths shallower than this, e.g.
                                '250.0m'. Default: 250.0m
    --transient-threshold       Threshold in dB above the pooled Sv, e.g.
                                '12.0dB'. Default: 12.0dB
    --range-var                 Name of the range/depth variable: depth or
                                echo_range. Default: depth
    --use-index-binning         Use index-based binning instead of physical
                                units (much faster).
    --chunk KEY=VAL [...]       Optional dask chunk sizes as key=value pairs
                                over Sv's dims (e.g., ping_time=256
                                range_sample=512). Only used with
                                --use-index-binning. Not part of the product
                                hash. It takes every following word, so give
                                INPUT_PATH before --chunk or end the list with
                                '--'; a trailing word without '=' is taken as
                                INPUT_PATH when none was given.

    --base NAME                 Base name for the outputs.
    --dest DIR|gs://PREFIX      Write the default-named outputs there.
    --force                     Recompute even if identical products exist.

    Description:
    Creates a boolean mask marking likely transient-noise events using a
    pooling comparison in depth-binned windows. The mask variable is
    'transient_mask', True = transient noise, dims (channel, ping_time,
    range_sample). Optionally applies the mask to Sv to produce a cleaned Sv
    dataset. The mask path is printed to stdout for piping into the next
    stage of the pipeline. Provenance (the input's chain plus this step) is
    embedded in both files; see aa-metadata.

    If the input has no 'Sv' variable it is calibrated with
    echopype.calibrate.compute_Sv (defaults) first.

    Example:
        aa-sv input.nc | aa-depth | aa-transient --apply --depth-bin 10m \\
              --transient-threshold 14.0dB --exclude-above 20m --use-index-binning
    """
    print(help_text)


def _build_parser():
    parser = argparse.ArgumentParser(
        description="Create a transient-noise mask from Sv with Echopype.",
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

    # mask_transient_noise parameters
    parser.add_argument("--func", default="nanmean",
                        help="Pooling function: nanmean or nanmedian (default: nanmean).")
    parser.add_argument("--depth-bin", dest="depth_bin", default="10m",
                        help="Depth bin size, e.g., '10m' (default: 10m).")
    parser.add_argument("--num-side-pings", dest="num_side_pings", type=int, default=25,
                        help="Number of side pings for pooling window (default: 25).")
    parser.add_argument("--exclude-above", dest="exclude_above", default="250.0m",
                        help="Exclude depths shallower than this (default: 250.0m).")
    parser.add_argument("--transient-threshold", dest="transient_noise_threshold",
                        default="12.0dB",
                        help="Threshold above local context, e.g. '12.0dB' (default: 12.0dB).")
    parser.add_argument("--range-var", dest="range_var", default="depth",
                        help="Range/depth variable name (default: depth).")
    parser.add_argument("--use-index-binning", dest="use_index_binning",
                        action="store_true",
                        help="Use index-based binning rather than physical bin sizes.")
    parser.add_argument("--chunk", nargs="*", type=str, default=None,
                        help="Optional dask chunk sizes as key=value pairs.")
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

    # --chunk takes every following word, so `--chunk ping_time=256 in.nc`
    # swallows the input. Give a trailing word without '=' back to
    # INPUT_PATH when no input was given otherwise.
    if args.chunk and args.input_path is None and "=" not in args.chunk[-1]:
        args.input_path = args.chunk.pop()

    # ---------------------------
    # Parse chunk dict (CLI parsing, fail before doing any heavy work)
    # ---------------------------
    try:
        chunk_dict = _parse_chunk_kv(args.chunk)
    except argparse.ArgumentTypeError as e:
        logger.error(str(e))
        sys.exit(1)

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
        legacy=lambda: naming.with_stem_suffix(src.local, "_transient_mask", ".nc"),
    )
    # The --apply side output: never moved by -o (legacy: beside the input).
    side = None
    if args.apply:
        side = run.plan(
            ext=".nc",
            variant="apply",
            kind="sv",
            legacy=lambda: naming.with_stem_suffix(src.local, "_transient_cleaned", ".nc"),
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
            "func": args.func,
            "depth_bin": args.depth_bin,
            "num_side_pings": args.num_side_pings,
            "exclude_above": args.exclude_above,
            "transient_noise_threshold": args.transient_noise_threshold,
            "range_var": args.range_var,
            "use_index_binning": args.use_index_binning,
            "chunk_dict": chunk_dict,
            "product": out.hash,
        }
        logger.debug(
            f"Executing aa-transient configured with [OPTIONS]:\n"
            f"{pprint.pformat(args_summary)}"
        )

        ds, mask, calibrated = compute_mask(
            input_path=src.local,
            func=args.func,
            depth_bin=args.depth_bin,
            num_side_pings=args.num_side_pings,
            exclude_above=args.exclude_above,
            transient_noise_threshold=args.transient_noise_threshold,
            range_var=args.range_var,
            use_index_binning=args.use_index_binning,
            chunk_dict=chunk_dict,
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

        logger.success(f"Generated {out.target} with aa-transient. Passing it to stdout...")
        stdio.emit(out.target)

    except Exception as e:
        logger.exception(f"Error during processing: {e}")
        sys.exit(1)


def _parse_chunk_kv(pairs):
    """Parse KEY=VAL pairs into a dict for chunking; cast integers when sensible."""
    out = {}
    for p in pairs or []:
        if "=" not in p:
            raise argparse.ArgumentTypeError(
                f"Invalid chunk pair (expected key=val): {p}"
            )
        k, v = p.split("=", 1)
        k = k.strip()
        v = v.strip()
        # Best-effort casting to int, else leave as string
        try:
            out[k] = int(v)
        except ValueError:
            out[k] = v
    return out


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
    """Boolean view of the mask, True = transient noise (echopype emits bool)."""
    if mask.dtype == bool:
        return mask
    return mask.fillna(0) != 0


def _warn_ignored_exclude_above(ds, exclude_above, range_var, use_index_binning):
    """Warn when echopype's index binning mishandles --exclude-above.

    echopype 0.11.1 (index_binning_pool_Sv) takes the cut as
    ``argmin((depth <= exclude_above).data)`` over the whole
    (channel, ping_time, range_sample) array, i.e. the first sample deeper
    than the limit counted from the first ping of the first channel:
      * no sample deeper anywhere: argmin is 0, nothing is excluded;
      * none in that first ping: the flat index lies past the range axis,
        the pooled slice is empty and echopype fails.
    The same computation is done here; only a warning, the result is echopype's.
    """
    import re

    import numpy as np

    if not use_index_binning or range_var not in ds:
        return
    m = re.match(r"^\s*([\d.]+)\s*m\s*$", str(exclude_above), flags=re.IGNORECASE)
    if not m:
        return
    dims = ("channel", "ping_time", "range_sample")
    try:
        limit = float(m.group(1))
        depth = ds[range_var]
        if set(depth.dims) != set(dims):
            return
        depth = depth.transpose(*dims)
        shallow = (depth <= limit).values          # NaN counts as not shallow
        cut = int(np.argmin(shallow))
        n_range = depth.sizes["range_sample"]
        deepest = float(np.nanmax(depth.values))
        first_ping = float(np.nanmax(depth.values[0, 0, :]))
    except (TypeError, ValueError, IndexError):
        return
    if shallow.all():
        logger.warning(
            f"--exclude-above {exclude_above} is deeper than all data ({range_var} max "
            f"{deepest:.1f} m): with --use-index-binning echopype 0.11.1 then excludes "
            "nothing, so shallow samples can be flagged. Use a value inside the data."
        )
    elif cut >= n_range:
        logger.warning(
            f"--exclude-above {exclude_above} is deeper than the first ping of the first "
            f"channel ({range_var} max {first_ping:.1f} m there): echopype 0.11.1's index "
            "binning takes its cut from that ping and will fail (\"overlapping depth ... "
            "larger than your array\"). Use a value inside the data."
        )


def compute_mask(
    input_path: Path,
    func: str = "nanmean",
    depth_bin: str = "10m",
    num_side_pings: int = 25,
    exclude_above: str = "250.0m",
    transient_noise_threshold: str = "12.0dB",
    range_var: str = "depth",
    use_index_binning: bool = False,
    chunk_dict=None,
):
    """Load Sv (calibrating if needed) and compute the transient-noise mask.

    Returns (ds_Sv, mask, calibrated) where ``calibrated`` says whether the
    input had no Sv and compute_Sv was run on it.
    """
    import echopype as ep  # deferred so --help stays fast
    import xarray as xr
    from echopype.clean import mask_transient_noise

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

    _warn_ignored_exclude_above(ds, exclude_above, range_var, use_index_binning)

    logger.info("Computing transient-noise mask")
    mask = mask_transient_noise(
        ds_Sv=ds,
        func=func,
        depth_bin=depth_bin,
        num_side_pings=num_side_pings,
        exclude_above=exclude_above,
        transient_noise_threshold=transient_noise_threshold,
        range_var=range_var,
        use_index_binning=use_index_binning,
        chunk_dict=chunk_dict or {},
    )
    return ds, mask, calibrated


def write_mask(mask, output_path: Path):
    """Save the mask as variable 'transient_mask' (polarity as echopype emits it)."""
    # Wrap DataArray into a Dataset for clearer NetCDF structure
    mask_ds = mask.to_dataset(name="transient_mask")
    mask_ds = clean_attrs(mask_ds)

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Saving transient-noise mask to {output_path}")
    mask_ds.to_netcdf(output_path, mode="w", format="NETCDF4")


def write_cleaned(ds, mask, output_path: Path):
    """Write Sv with transient samples set to NaN; every other variable as is."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Applying mask to Sv and writing cleaned Sv to {output_path}")
    ds_clean = ds.copy()
    # Keep values where NOT transient noise
    ds_clean["Sv"] = (
        ds_clean["Sv"].where(~_noise(mask), other=float("nan"))
        .transpose(*ds["Sv"].dims)
    )
    ds_clean = clean_attrs(ds_clean)
    ds_clean.to_netcdf(output_path, mode="w", format="NETCDF4")


if __name__ == "__main__":
    main()
