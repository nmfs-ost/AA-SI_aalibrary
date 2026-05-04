#!/usr/bin/env python3
"""
aa-transient

Console tool for locating transient noise in calibrated Sv data with Echopype
and saving a transient-noise mask (and optionally an Sv-cleaned file).

Pipeline-friendly: reads input path from positional arg or stdin, writes
output path (the mask) to stdout, all logs to stderr.

Typical pipeline usage:
    aa-nc --sonar_model EK60 input.raw | aa-sv | aa-transient --apply
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

# Now the heavy imports — anything they log gets squashed
import argparse
import pprint
from pathlib import Path

import echopype as ep
import xarray as xr
from echopype.clean import mask_transient_noise


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
    Usage: aa-transient [OPTIONS] [INPUT_PATH]

    Arguments:
    INPUT_PATH                  Path to the calibrated .nc / .netcdf4 file
                                containing Sv (preferred), or a converted
                                Echopype file that can be calibrated to Sv.
                                Optional. Defaults to stdin if not provided.

    Options:
    -o, --output_path           Path to save the transient-noise mask (NetCDF).
                                Default: same directory as input, with
                                '_transient_mask' appended to the stem
                                and a .nc suffix.

    --apply                     Also apply the mask to Sv and write a cleaned
                                Sv file alongside the mask, suffix
                                '_transient_cleaned'.

    --func                      Pooling function ('nanmean', 'nanmedian', etc.).
                                Default: nanmean
    --depth-bin                 Vertical bin size, e.g. '10m'. Default: 10m
    --num-side-pings            Pings on each side for the pooling window.
                                Default: 25
    --exclude-above             Exclude depths shallower than this, e.g.
                                '250.0m'. Default: 250.0m
    --transient-threshold       Threshold in dB above local context, e.g.
                                '12.0dB'. Default: 12.0dB
    --range-var                 Name of the range/depth coordinate.
                                Default: depth
    --use-index-binning         Use index-based binning instead of physical
                                units.
    --chunk KEY=VAL [...]       Optional chunk sizes as key=value pairs
                                (e.g., ping_time=256 depth=512).

    Description:
    Creates a boolean mask marking likely transient-noise events using a
    pooling comparison in depth-binned windows. Optionally applies the mask
    to Sv to produce a cleaned Sv dataset. The mask path is printed to
    stdout for piping into the next stage of the pipeline.

    Example:
        aa-sv input.nc | aa-transient --apply --depth-bin 10m \\
              --transient-threshold 14.0dB
    """
    print(help_text)


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
        description="Create a transient-noise mask from Sv with Echopype.",
        add_help=False,
    )

    parser.add_argument(
        "input_path",
        type=Path,
        nargs="?",
        help="Path to the .nc / .netcdf4 file containing Sv.",
    )
    parser.add_argument(
        "-o", "--output_path",
        type=Path,
        help="Path to save the mask. Default appends '_transient_mask' to the input stem.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Also write Sv cleaned by the mask to <stem>_transient_cleaned.nc.",
    )

    # mask_transient_noise parameters
    parser.add_argument("--func", default="nanmean",
                        help="Pooling function (default: nanmean).")
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
                        help="Optional chunk sizes as key=value pairs.")

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
        args.output_path = args.input_path.with_stem(
            args.input_path.stem + "_transient_mask"
        ).with_suffix(".nc")
    else:
        args.output_path = args.output_path.with_suffix(".nc")

    # Guard against clobbering the input
    if args.output_path.resolve() == args.input_path.resolve():
        logger.error(f"Refusing to overwrite input file: {args.input_path.resolve()}")
        sys.exit(1)

    # ---------------------------
    # Parse chunk dict (CLI parsing, fail before doing any heavy work)
    # ---------------------------
    try:
        chunk_dict = _parse_chunk_kv(args.chunk)
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
            "apply": args.apply,
            "func": args.func,
            "depth_bin": args.depth_bin,
            "num_side_pings": args.num_side_pings,
            "exclude_above": args.exclude_above,
            "transient_noise_threshold": args.transient_noise_threshold,
            "range_var": args.range_var,
            "use_index_binning": args.use_index_binning,
            "chunk_dict": chunk_dict,
        }
        logger.debug(
            f"Executing aa-transient configured with [OPTIONS]:\n"
            f"{pprint.pformat(args_summary)}"
        )

        process_file(
            input_path=args.input_path,
            output_path=args.output_path,
            apply_mask=args.apply,
            func=args.func,
            depth_bin=args.depth_bin,
            num_side_pings=args.num_side_pings,
            exclude_above=args.exclude_above,
            transient_noise_threshold=args.transient_noise_threshold,
            range_var=args.range_var,
            use_index_binning=args.use_index_binning,
            chunk_dict=chunk_dict,
        )

        logger.success(
            f"Generated {args.output_path.resolve()} with aa-transient. "
            "Passing .nc path to stdout..."
        )
        # Pipe the mask path to stdout for the next tool
        print(args.output_path.resolve())

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


def process_file(
    input_path: Path,
    output_path: Path,
    apply_mask: bool = False,
    func: str = "nanmean",
    depth_bin: str = "10m",
    num_side_pings: int = 25,
    exclude_above: str = "250.0m",
    transient_noise_threshold: str = "12.0dB",
    range_var: str = "depth",
    use_index_binning: bool = False,
    chunk_dict=None,
):
    """Load Sv from NetCDF, compute the transient-noise mask, save the mask,
    and optionally write a cleaned Sv file with mask applied."""

    logger.info(f"Loading dataset from {input_path}")
    ds = xr.open_dataset(input_path)

    # Ensure we have calibrated Sv. If the input is a converted EchoData
    # file rather than an already-calibrated Sv file, fall through to
    # echopype.calibrate.compute_Sv. Mirrors aa-sv's contract so this
    # tool can sit anywhere downstream of aa-nc in a pipeline.
    if "Sv" not in ds.data_vars:
        logger.info("No 'Sv' variable found; calibrating to Sv via Echopype")
        ed = ep.open_converted(input_path)
        ds = ep.calibrate.compute_Sv(ed)

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

    # Wrap DataArray into a Dataset for clearer NetCDF structure
    mask_ds = mask.to_dataset(name="transient_mask")
    mask_ds = clean_attrs(mask_ds)

    output_path = output_path.with_suffix(".nc")
    logger.info(f"Saving transient-noise mask to {output_path}")
    mask_ds.to_netcdf(output_path, mode="w", format="NETCDF4")

    # Optionally write a cleaned Sv file with transient samples set to NaN.
    # Note: stdout still receives the mask path — the cleaned file is a
    # side-output, not the pipeline value.
    if apply_mask:
        cleaned_path = input_path.with_stem(
            input_path.stem + "_transient_cleaned"
        ).with_suffix(".nc")
        logger.info(f"Applying mask to Sv and writing cleaned Sv to {cleaned_path}")
        ds_clean = ds.copy()
        # Keep values where NOT transient noise
        ds_clean["Sv"] = ds_clean["Sv"].where(~mask, other=float("nan"))
        ds_clean = clean_attrs(ds_clean)
        ds_clean.to_netcdf(cleaned_path, mode="w", format="NETCDF4")

    logger.success(f"Transient-noise masking complete: {output_path.resolve()}")


if __name__ == "__main__":
    main()