#!/usr/bin/env python3
"""
Console tool for detecting transient noise in Sv using Echopype’s dispatcher
`echopype.clean.detect_transient(ds, method, params)` and saving a boolean
mask (and optionally an Sv-cleaned file).

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
import ast
import xarray as xr
from loguru import logger
import echopype as ep  # ensure echopype is installed
from echopype.clean import detect_transient
import pprint


def print_help():
    """Standalone help text (useful when invoked with no args and no stdin)."""
    help_text = """
    Usage: aa-detect-transient [OPTIONS] [INPUT_PATH]

    Arguments:
      INPUT_PATH                 Path to a calibrated Sv NetCDF (.nc), or a
                                 converted Echopype file that can be calibrated.
                                 Optional. Defaults to stdin if not provided.

    Options:
      -o, --output_path PATH     Where to write the transient-noise mask (NetCDF).
                                 Default: <stem>_detect_transient_mask.nc
      --apply                    Also apply the mask to Sv and write a cleaned
                                 Sv file (suffix: _detect_transient_cleaned.nc).

      # detect_transient parameters
      --method STR               Transient detection method name (dispatcher key).
                                 (e.g., 'pooling', 'percentile', etc.—see docs)
      --param KEY=VAL [...]      Parameters for the chosen method as key=value pairs.
                                 Values are safely parsed (int/float/bool/str).

      --range-var STR            Name of the range/depth coordinate (if your method
                                 expects it in params, you can also pass via --param).
                                 Default: depth

      -h, --help                 Show this help message and exit.

    Description:
      Dispatches transient-noise detection to a selected method via Echopype’s
      `detect_transient(ds, method, params)` and returns a boolean mask. Optionally
      applies the mask to Sv to produce a cleaned Sv dataset.

    Examples:
      aa-detect-transient data.nc --method pooling --param depth_bin=10m num_side_pings=25 transient_noise_threshold=12.0dB
      aa-detect-transient data.nc --apply -o out_mask.nc --method percentile --param percentile=99.5 window=21
    """
    print(help_text)


def _parse_kv_pairs(pairs):
    """
    Convert a list of KEY=VAL strings into a dict with safe literal parsing.
    - Tries int/float/bool/None via ast.literal_eval when possible.
    - Leaves plain strings as-is (so '10m' or '12.0dB' remain strings).
    """
    out = {}
    for p in pairs or []:
        if "=" not in p:
            raise argparse.ArgumentTypeError(f"Invalid key=value pair: {p}")
        k, v = p.split("=", 1)
        k = k.strip()
        v = v.strip()
        # Try safe literal -> if it fails, keep original string (good for '10m', '12.0dB')
        try:
            out[k] = ast.literal_eval(v)
        except Exception:
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
    """Entry point for the aa-detect-transient CLI."""
    # Read a token path from stdin when invoked with no args, else print help.
    if len(sys.argv) == 1:
        if not sys.stdin.isatty():
            stdin_data = sys.stdin.readline().strip()
            if stdin_data:
                sys.argv.append(stdin_data)
        else:
            print_help()
            sys.exit(0)

    parser = argparse.ArgumentParser(
        description="Detect transient noise in Sv using Echopype’s detect_transient dispatcher."
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
        help="Output path for the mask NetCDF (default: <stem>_detect_transient_mask.nc).",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Also write Sv cleaned by the transient mask to <stem>_detect_transient_cleaned.nc.",
    )

    # ---------------------------
    # detect_transient parameters
    # ---------------------------
    parser.add_argument(
        "--method",
        required=True,
        help="Transient detection method name (dispatcher key).",
    )
    parser.add_argument(
        "--param",
        nargs="*",
        help="Additional method parameters as key=value pairs (e.g., depth_bin=10m transient_noise_threshold=12.0dB).",
    )

    # Optional convenience if a method expects a range var name
    parser.add_argument("--range-var", dest="range_var", default="depth",
                        help="Range/depth variable name (default: depth).")

    args = parser.parse_args()

    # ---------------------------
    # Resolve/validate input
    # ---------------------------
    if args.input_path is None:
        args.input_path = Path(sys.stdin.readline().strip())
        logger.info(f"Read input path from stdin: {args.input_path}")

    if not args.input_path.exists():
        logger.error(f"File '{args.input_path}' does not exist.")
        sys.exit(1)

    # Default mask output path if not provided
    if args.output_path is None:
        args.output_path = args.input_path.with_stem(args.input_path.stem + "_detect_transient_mask").with_suffix(".nc")

    try:
        # ---------------------------
        # Load dataset quietly
        # ---------------------------
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
        # Build params for dispatcher
        # ---------------------------
        params = _parse_kv_pairs(args.param)
        # Provide range_var default via params only if user didn’t already pass it.
        params.setdefault("range_var", args.range_var)

        # ---------------------------
        # Run dispatcher
        # ---------------------------
        logger.info(f"Detecting transient noise with method='{args.method}' and params={params} ...")
        mask = detect_transient(
            ds=ds,
            method=args.method,
            params=params,
        )

        # Save mask to its own NetCDF
        logger.info(f"Saving transient-detection mask to {args.output_path} ...")
        mask_ds = mask.to_dataset(name="transient_detect_mask")
        _add_basic_attrs(mask_ds)
        mask_ds.to_netcdf(args.output_path, mode="w", format="NETCDF4")

        # Optionally apply mask to Sv and save a cleaned file
        if args.apply:
            cleaned_path = args.input_path.with_stem(args.input_path.stem + "_detect_transient_cleaned").with_suffix(".nc")
            logger.info(f"Applying mask to Sv and writing cleaned Sv to {cleaned_path} ...")
            ds_clean = ds.copy()
            ds_clean["Sv"] = ds_clean["Sv"].where(~mask, other=float("nan"))
            _add_basic_attrs(ds_clean)
            ds_clean.to_netcdf(cleaned_path, mode="w", format="NETCDF4")

        # Pretty-print args for logs and echo the primary output (mask path) to stdout for piping
        pretty_args = pprint.pformat(vars(args))
        logger.debug(f"\naa-detect-transient args:\n{pretty_args}")
        print(args.output_path.resolve())

        logger.info("Transient detection complete.")

    except Exception as e:
        logger.exception(f"Error during transient detection: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
