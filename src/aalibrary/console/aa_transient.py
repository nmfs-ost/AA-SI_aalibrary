#!/usr/bin/env python3
"""
Console tool for locating transient noise in calibrated Sv data with Echopype
and saving a transient-noise mask (and optionally an Sv-cleaned file).

Pattern matches your existing tools:
- optional stdin piping for INPUT_PATH
- argparse-wrapped simple function
- human-readable docstrings & inline comments
"""

import io
from contextlib import redirect_stdout
import argparse
import sys
from pathlib import Path
import xarray as xr
from loguru import logger
import echopype as ep  # ensure echopype is installed
from echopype.clean import mask_transient_noise
import pprint


def print_help():
    """Standalone help text (useful when invoked with no args and no stdin)."""
    help_text = """
    Usage: aa-transient [OPTIONS] [INPUT_PATH]

    Arguments:
      INPUT_PATH                 Path to the calibrated .nc (NetCDF) file
                                 containing Sv (preferred), or a converted
                                 Echopype file that can be calibrated to Sv.
                                 Optional. Defaults to stdin if not provided.

    Options:
      -o, --output_path PATH     Where to write the transient-noise mask (NetCDF).
                                 Default: <stem>_transient_mask.nc
      --apply                    Also apply the mask to Sv and write a cleaned
                                 Sv file (suffix: _transient_cleaned.nc).

      # mask_transient_noise parameters
      --func STR                 Pooling function ('nanmean', 'nanmedian', etc.).
                                 Default: nanmean
      --depth-bin STR            Vertical bin size, e.g. '10m'. Default: 10m
      --num-side-pings INT       Pings on each side for pooling window.
                                 Default: 25
      --exclude-above STR        Exclude depths shallower than this (e.g. '250.0m').
                                 Default: 250.0m
      --transient-threshold STR  Threshold in dB above local context, e.g. '12.0dB'.
                                 Default: 12.0dB
      --range-var STR            Name of the range/depth coordinate (e.g., 'depth').
                                 Default: depth
      --use-index-binning        Use index-based binning instead of physical units.
      --chunk KEY=VAL [...]      Optional chunk sizes as key=value pairs (e.g., ping_time=256 depth=512).

      -h, --help                 Show this help message and exit.

    Description:
      Creates a boolean mask marking likely transient-noise events using a pooling
      comparison in depth-binned windows. Optionally applies the mask to Sv to
      produce a cleaned Sv dataset.

    Examples:
      aa-transient data.nc --depth-bin 10m --num-side-pings 21 --transient-threshold 14.0dB
      aa-transient data.nc --apply -o out_mask.nc
    """
    print(help_text)


def _parse_chunk_kv(pairs):
    """Parse KEY=VAL pairs into a dict for chunking; cast integers when sensible."""
    out = {}
    for p in pairs or []:
        if "=" not in p:
            raise argparse.ArgumentTypeError(f"Invalid chunk pair (expected key=val): {p}")
        k, v = p.split("=", 1)
        k = k.strip()
        v = v.strip()
        # Best-effort casting to int, else leave as string
        try:
            out[k] = int(v)
        except ValueError:
            out[k] = v
    return out


def _add_basic_attrs(ds: xr.Dataset) -> None:
    """Replace None attrs with strings to avoid NetCDF writer issues."""
    # Dataset-level attrs
    for k, v in list(ds.attrs.items()):
        if v is None:
            ds.attrs[k] = "NA"
    # Variable-level attrs
    for var in ds.data_vars:
        for k, v in list(ds[var].attrs.items()):
            if v is None:
                ds[var].attrs[k] = "NA"


def main():
    """Entry point for the aa-transient CLI."""
    # If no argv, try to read an INPUT_PATH token from stdin; otherwise print help and exit.
    if len(sys.argv) == 1:
        if not sys.stdin.isatty():
            stdin_data = sys.stdin.readline().strip()
            if stdin_data:
                sys.argv.append(stdin_data)
        else:
            print_help()
            sys.exit(0)

    parser = argparse.ArgumentParser(
        description="Create a transient-noise mask from Sv and (optionally) write Sv cleaned with that mask."
    )

    # ---------------------------
    # Positional/IO args
    # ---------------------------
    parser.add_argument(
        "input_path",
        type=Path,
        nargs="?",
        help="Path to a NetCDF file containing Sv (preferred) or a converted file that can be calibrated to Sv.",
    )
    parser.add_argument(
        "-o", "--output_path",
        type=Path,
        help="Output path for the mask NetCDF (default: <stem>_transient_mask.nc).",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Also write Sv cleaned by the transient mask to <stem>_transient_cleaned.nc.",
    )

    # ---------------------------
    # mask_transient_noise parameters
    # ---------------------------
    parser.add_argument("--func", default="nanmean",
                        help="Pooling function (default: nanmean).")
    parser.add_argument("--depth-bin", dest="depth_bin", default="10m",
                        help="Depth bin size, e.g., '10m' (default: 10m).")
    parser.add_argument("--num-side-pings", dest="num_side_pings", type=int, default=25,
                        help="Number of side pings for pooling window (default: 25).")
    parser.add_argument("--exclude-above", dest="exclude_above", default="250.0m",
                        help="Exclude depths shallower than this (default: 250.0m).")
    parser.add_argument("--transient-threshold", dest="transient_noise_threshold", default="12.0dB",
                        help="Transient threshold above local context, e.g., '12.0dB' (default: 12.0dB).")
    parser.add_argument("--range-var", dest="range_var", default="depth",
                        help="Range/depth variable name (default: depth).")
    parser.add_argument("--use-index-binning", dest="use_index_binning", action="store_true",
                        help="Use index-based binning rather than physical bin sizes.")
    parser.add_argument("--chunk", nargs="*", type=str,
                        help="Optional chunk sizes as key=value pairs (e.g., ping_time=256 depth=512).")

    args = parser.parse_args()

    # ---------------------------
    # Resolve/validate input
    # ---------------------------
    if args.input_path is None:
        # If not provided on CLI, try to read a path token from stdin (same behavior as your other tools).
        args.input_path = Path(sys.stdin.readline().strip())
        logger.info(f"Read input path from stdin: {args.input_path}")

    if not args.input_path.exists():
        logger.error(f"File '{args.input_path}' does not exist.")
        sys.exit(1)

    # Default mask output path if not provided
    if args.output_path is None:
        args.output_path = args.input_path.with_stem(args.input_path.stem + "_transient_mask").with_suffix(".nc")

    try:
        # ---------------------------
        # Load dataset quietly
        # ---------------------------
        # Suppress any library chatter to stdout so pipelines remain clean.
        f = io.StringIO()
        with redirect_stdout(f):
            ds = xr.open_dataset(args.input_path)

        # ---------------------------
        # Ensure we have calibrated Sv
        # ---------------------------
        if "Sv" not in ds.data_vars:
            logger.info("No 'Sv' variable found; attempting to calibrate to Sv via Echopype...")
            ed = ep.open_converted(args.input_path)
            ds = ep.calibrate.compute_Sv(ed)

        # ---------------------------
        # Parse chunk dict if provided
        # ---------------------------
        chunk_dict = _parse_chunk_kv(args.chunk)

        # ---------------------------
        # Compute transient-noise mask
        # ---------------------------
        logger.info("Computing transient-noise mask...")
        mask = mask_transient_noise(
            ds_Sv=ds,
            func=args.func,
            depth_bin=args.depth_bin,
            num_side_pings=args.num_side_pings,
            exclude_above=args.exclude_above,
            transient_noise_threshold=args.transient_noise_threshold,
            range_var=args.range_var,
            use_index_binning=args.use_index_binning,
            chunk_dict=chunk_dict,
        )

        # Save mask to its own NetCDF
        logger.info(f"Saving transient-noise mask to {args.output_path} ...")
        # Wrap DataArray into a Dataset for clearer NetCDF structure
        mask_ds = mask.to_dataset(name="transient_mask")
        _add_basic_attrs(mask_ds)
        mask_ds.to_netcdf(args.output_path, mode="w", format="NETCDF4")

        # Optionally write a cleaned Sv file with transient-noise samples set to NaN
        if args.apply:
            cleaned_path = args.input_path.with_stem(args.input_path.stem + "_transient_cleaned").with_suffix(".nc")
            logger.info(f"Applying mask to Sv and writing cleaned Sv to {cleaned_path} ...")
            ds_clean = ds.copy()
            # Keep values where NOT transient noise
            ds_clean["Sv"] = ds_clean["Sv"].where(~mask, other=float("nan"))
            _add_basic_attrs(ds_clean)
            ds_clean.to_netcdf(cleaned_path, mode="w", format="NETCDF4")

        # Pretty-print args for logs and echo the primary output (mask path) to stdout for piping
        pretty_args = pprint.pformat(vars(args))
        logger.debug(f"\naa-transient args:\n{pretty_args}")
        print(args.output_path.resolve())

        logger.info("Transient-noise masking complete.")

    except Exception as e:
        logger.exception(f"Error during transient-noise masking: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
