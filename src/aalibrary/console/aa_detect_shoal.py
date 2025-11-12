#!/usr/bin/env python3
"""
Console tool to detect shoals in Sv using Echopype’s dispatcher
`echopype.mask.detect_shoal(ds, method, params)` and save a 2D boolean mask
(optionally also write an Sv-cleaned file with the mask applied).

Pattern matches your suite:
- optional stdin piping for INPUT_PATH
- argparse-wrapped single function
- plain-English comments for readability
"""

import io
from contextlib import redirect_stdout
import argparse
import sys
from pathlib import Path
import ast
import xarray as xr
from loguru import logger
import echopype as ep  # for Sv calibration fallback
from echopype.mask import detect_shoal, apply_mask
import pprint


def print_help():
    """Standalone help text for zero-arg TTY invocation."""
    help_text = """
    Usage: aa-detect-shoal [OPTIONS] [INPUT_PATH]

    Arguments:
      INPUT_PATH                 Path to a calibrated Sv NetCDF (.nc), or a converted
                                 Echopype file that can be calibrated. Optional; defaults
                                 to stdin if not provided.

    Options:
      -o, --output_path PATH     Where to write the shoal mask (NetCDF).
                                 Default: <stem>_detect_shoal_mask.nc
      --apply                    Also apply the mask to Sv and write cleaned Sv to
                                 <stem>_detect_shoal_cleaned.nc

      # detect_shoal parameters
      --method STR               Shoal detection method (dispatcher key), e.g. 'echoview' or 'weill'. (required)
      --param KEY=VAL [...]      Parameters for the chosen method as key=value pairs.
                                 Values are safely parsed (int/float/bool/None) when possible;
                                 strings like '10m' or '12.0dB' remain strings.

      --no-overwrite             Do not overwrite an existing output file.
      --quiet                    Suppress logs; print only the final output path.
      -h, --help                 Show this help message and exit.

    Description:
      Dispatches shoal detection to the chosen method via Echopype’s
      `detect_shoal(ds, method, params)` and returns a 2D boolean mask
      (True = inside shoal). Optionally applies the mask to Sv and writes a
      cleaned Sv file.
    """
    print(help_text)


def _parse_kv_pairs(pairs):
    """
    Convert KEY=VAL strings into a dict with safe literal parsing.
    Leaves units-bearing strings (e.g., '10m', '12.0dB') as-is.
    """
    out = {}
    for p in pairs or []:
        if "=" not in p:
            raise argparse.ArgumentTypeError(f"Invalid key=value pair: {p}")
        k, v = p.split("=", 1)
        k = k.strip()
        v = v.strip()
        try:
            out[k] = ast.literal_eval(v)
        except Exception:
            out[k] = v
    return out


def _add_basic_attrs(ds: xr.Dataset) -> None:
    """Replace None attrs with strings to avoid NetCDF writer issues."""
    for k, v in list(ds.attrs.items()):
        if v is None:
            ds.attrs[k] = "NA"
    for var in ds.data_vars:
        for k, v in list(ds[var].attrs.items()):
            if v is None:
                ds[var].attrs[k] = "NA"


def main():
    """Entry point for the aa-detect-shoal CLI."""
    # stdin token behavior when no CLI args
    if len(sys.argv) == 1:
        if not sys.stdin.isatty():
            token = sys.stdin.readline().strip()
            if token:
                sys.argv.append(token)
        else:
            print_help()
            sys.exit(0)

    parser = argparse.ArgumentParser(
        description="Detect shoals in Sv using Echopype’s detect_shoal dispatcher."
    )

    # IO
    parser.add_argument(
        "input_path",
        type=Path,
        nargs="?",
        help="Path to a NetCDF file containing Sv (preferred) or a converted file to calibrate.",
    )
    parser.add_argument(
        "-o", "--output_path",
        type=Path,
        help="Output path for the shoal mask NetCDF (default: <stem>_detect_shoal_mask.nc).",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Also write Sv cleaned by the shoal mask to <stem>_detect_shoal_cleaned.nc.",
    )
    parser.add_argument(
        "--no-overwrite",
        action="store_true",
        help="Do not overwrite an existing output file.",
    )
    parser.add_argument("--quiet", action="store_true", help="Suppress logs; print only output path.")

    # detect_shoal params
    parser.add_argument("--method", required=True,
                        help="Shoal detection method name (dispatcher key), e.g., 'echoview', 'weill'.")
    parser.add_argument("--param", nargs="*",
                        help="Additional method parameters as key=value pairs.")

    args = parser.parse_args()

    # Resolve/validate
    if args.input_path is None:
        args.input_path = Path(sys.stdin.readline().strip())
        if not args.quiet:
            logger.info(f"Read input path from stdin: {args.input_path}")

    if not args.input_path.exists():
        logger.error(f"File '{args.input_path}' does not exist.")
        sys.exit(1)

    if args.output_path is None:
        args.output_path = args.input_path.with_stem(args.input_path.stem + "_detect_shoal_mask").with_suffix(".nc")

    if args.output_path.exists() and args.no_overwrite:
        logger.error(f"Output file '{args.output_path}' exists and --no-overwrite was set.")
        sys.exit(1)

    try:
        # Load dataset quietly (keep stdout clean for piping)
        f = io.StringIO()
        with redirect_stdout(f):
            ds = xr.open_dataset(args.input_path)

        # Ensure we have calibrated Sv; some files may be converted but not calibrated.
        if "Sv" not in ds.data_vars:
            if not args.quiet:
                logger.info("No 'Sv' variable found; attempting to calibrate to Sv via Echopype...")
            ed = ep.open_converted(args.input_path)
            ds = ep.calibrate.compute_Sv(ed)

        # Build dispatcher params
        params = _parse_kv_pairs(args.param)

        # Run shoal detection
        if not args.quiet:
            logger.info(f"Detecting shoals with method='{args.method}' and params={params} ...")
        mask = detect_shoal(ds=ds, method=args.method, params=params)

        # Save mask (wrap DA -> DS for NetCDF structure)
        mask_ds = mask.to_dataset(name="shoal_mask")
        _add_basic_attrs(mask_ds)
        if not args.quiet:
            logger.info(f"Saving shoal mask to {args.output_path} ...")
        mask_ds.to_netcdf(args.output_path, mode="w", format="NETCDF4")

        # Optionally apply the mask to Sv and save cleaned Sv
        if args.apply:
            cleaned_path = args.input_path.with_stem(args.input_path.stem + "_detect_shoal_cleaned").with_suffix(".nc")
            if cleaned_path.exists() and args.no_overwrite:
                logger.error(f"Cleaned output '{cleaned_path}' exists and --no-overwrite was set.")
                sys.exit(1)
            if not args.quiet:
                logger.info(f"Applying shoal mask to Sv and saving to {cleaned_path} ...")
            ds_clean = apply_mask(source_ds=ds, mask=mask, var_name="Sv")
            _add_basic_attrs(ds_clean)
            ds_clean.to_netcdf(cleaned_path, mode="w", format="NETCDF4")

        # Debug + print primary output path for piping
        if not args.quiet:
            logger.debug(f"\naa-detect-shoal args:\n{pprint.pformat(vars(args))}")
        print(args.output_path.resolve())

        if not args.quiet:
            logger.info("Shoal detection complete.")

    except Exception as e:
        logger.exception(f"Error during shoal detection: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
