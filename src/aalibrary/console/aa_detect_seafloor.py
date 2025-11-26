#!/usr/bin/env python3
"""
Console tool to detect the seafloor (bottom line) using Echopype’s dispatcher
`echopype.mask.detect_seafloor(ds, method, params)` and save the result.
Optionally, emit a 2D bottom mask and/or apply it to Sv.

Pattern matches your suite:
- optional stdin piping for INPUT_PATH
- argparse-wrapped single function
- key=value param parsing with safe literal eval (units strings kept)
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
from echopype.mask import detect_seafloor, apply_mask
import numpy as np
import pprint


def print_help():
    help_text = """
    Usage: aa-detect-seafloor [OPTIONS] [INPUT_PATH]

    Arguments:
      INPUT_PATH                   Path to a calibrated Sv NetCDF (.nc), or a converted
                                   Echopype file that can be calibrated. Optional; defaults
                                   to stdin if not provided.

    Options:
      -o, --output_path PATH       Where to write the bottom line (NetCDF).
                                   Default: <stem>_seafloor.nc

      --method STR                 Seafloor detection method (dispatcher key), e.g. 'basic', 'blackwell'. (required)
      --param KEY=VAL [...]        Parameters for the chosen method as key=value pairs.
                                   Values are safely parsed (int/float/bool/None) when possible;
                                   strings like '10m' remain strings.

      --emit-mask                  Also compute and save a 2D boolean mask of samples **below** the bottom line
                                   to <stem>_seafloor_mask.nc (True = below bottom).
      --range-label STR            Name of range/depth variable used to build the mask (default: echo_range).
      --apply                      Apply the bottom mask to Sv and write cleaned Sv to <stem>_seafloor_cleaned.nc.
      --no-overwrite               Do not overwrite existing outputs.
      --quiet                      Suppress logs; print only primary output path.
      -h, --help                   Show this help message and exit.

    Description:
      Dispatches to `detect_seafloor(ds, method, params)` and returns a 1-D bottom line (per ping).
      With --emit-mask, builds a 2D mask by comparing range to the bottom line (True below bottom).
      With --apply, applies the mask to Sv using echopype.mask.apply_mask.
    """
    print(help_text)


def _parse_kv_pairs(pairs):
    """Convert KEY=VAL strings to dict; keep units strings ('10m','12dB') as strings."""
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
    """Entry point for aa-detect-seafloor."""
    # stdin token when no args
    if len(sys.argv) == 1:
        if not sys.stdin.isatty():
            token = sys.stdin.readline().strip()
            if token:
                sys.argv.append(token)
        else:
            print_help()
            sys.exit(0)

    p = argparse.ArgumentParser(
        description="Detect the seafloor (bottom line) using Echopype’s detect_seafloor dispatcher."
    )
    # IO
    p.add_argument("input_path", type=Path, nargs="?",
                   help="Path to NetCDF containing Sv (preferred) or a converted file to calibrate.")
    p.add_argument("-o", "--output_path", type=Path,
                   help="Output path for bottom line NetCDF (default: <stem>_seafloor.nc).")
    p.add_argument("--no-overwrite", action="store_true",
                   help="Do not overwrite existing outputs.")
    p.add_argument("--quiet", action="store_true",
                   help="Suppress logs; print only primary output path.")

    # detect_seafloor params
    p.add_argument("--method", required=True,
                   help="Seafloor detection method key (e.g., 'basic', 'blackwell').")
    p.add_argument("--param", nargs="*",
                   help="Additional method parameters as key=value pairs.")

    # optional mask/apply
    p.add_argument("--emit-mask", action="store_true",
                   help="Also compute/save a 2D mask (True = below bottom).")
    p.add_argument("--range-label", dest="range_label", default="echo_range",
                   help="Range/depth variable name used to build mask (default: echo_range).")
    p.add_argument("--apply", action="store_true",
                   help="Apply the bottom mask to Sv and write cleaned Sv.")

    args = p.parse_args()

    # Resolve / validate input
    if args.input_path is None:
        args.input_path = Path(sys.stdin.readline().strip())
        if not args.quiet:
            logger.info(f"Read input path from stdin: {args.input_path}")

    if not args.input_path.exists():
        logger.error(f"File '{args.input_path}' does not exist.")
        sys.exit(1)

    # Default outputs
    if args.output_path is None:
        args.output_path = args.input_path.with_stem(args.input_path.stem + "_seafloor").with_suffix(".nc")

    # Additional output paths
    mask_path = args.input_path.with_stem(args.input_path.stem + "_seafloor_mask").with_suffix(".nc")
    cleaned_path = args.input_path.with_stem(args.input_path.stem + "_seafloor_cleaned").with_suffix(".nc")

    # Overwrite guards
    if args.no_overwrite:
        if args.output_path.exists():
            logger.error(f"Output '{args.output_path}' exists and --no-overwrite was set.")
            sys.exit(1)
        if args.emit_mask and mask_path.exists():
            logger.error(f"Mask output '{mask_path}' exists and --no-overwrite was set.")
            sys.exit(1)
        if args.apply and cleaned_path.exists():
            logger.error(f"Cleaned output '{cleaned_path}' exists and --no-overwrite was set.")
            sys.exit(1)

    try:
        # Load quietly
        f = io.StringIO()
        with redirect_stdout(f):
            ds = xr.open_dataset(args.input_path)

        # Ensure calibrated Sv exists for downstream apply/mask use cases
        if "Sv" not in ds.data_vars:
            if not args.quiet:
                logger.info("No 'Sv' found; attempting to calibrate via Echopype...")
            ed = ep.open_converted(args.input_path)
            ds = ep.calibrate.compute_Sv(ed)

        # Build dispatcher params
        params = _parse_kv_pairs(args.param)

        # Detect seafloor (returns a 1-D bottom line DataArray)
        if not args.quiet:
            logger.info(f"Detecting seafloor with method='{args.method}' and params={params} ...")
        bottom = detect_seafloor(ds=ds, method=args.method, params=params)

        # Save bottom line
        bottom_ds = bottom.to_dataset(name="seafloor")
        bottom_ds["seafloor"].attrs.setdefault("long_name", "Seafloor bottom line")
        bottom_ds["seafloor"].attrs.setdefault("units", "m")
        _add_basic_attrs(bottom_ds)
        if not args.quiet:
            logger.info(f"Saving seafloor bottom line to {args.output_path} ...")
        bottom_ds.to_netcdf(args.output_path, mode="w", format="NETCDF4")

        # Optionally build and save a 2D mask: True where range > bottom (i.e., below bottom)
        if args.emit_mask or args.apply:
            if args.range_label not in ds and args.range_label not in ds.coords and args.range_label not in ds.data_vars:
                logger.error(f"Range variable '{args.range_label}' not found; cannot build bottom mask.")
                sys.exit(1)

            # echo_range is typically 2D (ping x range); bottom is 1D over ping/time.
            # Broadcast comparison to create mask: True where sample is deeper than bottom.
            rng = ds[args.range_label]
            try:
                bottom_b = bottom.broadcast_like(rng) if set(bottom.dims) <= set(rng.dims) else bottom
                mask2d = rng > bottom_b
            except Exception:
                # Fallback: align along the first shared dimension
                shared = [d for d in bottom.dims if d in rng.dims]
                if not shared:
                    logger.error("Cannot align bottom line with range variable to form a mask.")
                    sys.exit(1)
                bottom_b = bottom.transpose(*shared).broadcast_like(rng)
                mask2d = rng > bottom_b

            mask_ds = mask2d.to_dataset(name="seafloor_mask")
            _add_basic_attrs(mask_ds)

            if args.emit_mask:
                if not args.quiet:
                    logger.info(f"Saving bottom mask to {mask_path} ...")
                mask_ds.to_netcdf(mask_path, mode="w", format="NETCDF4")

            if args.apply:
                if not args.quiet:
                    logger.info(f"Applying bottom mask to Sv and saving to {cleaned_path} ...")
                ds_clean = apply_mask(source_ds=ds, mask=mask2d, var_name="Sv")
                _add_basic_attrs(ds_clean)
                ds_clean.to_netcdf(cleaned_path, mode="w", format="NETCDF4")

        # Debug + print primary output for piping
        if not args.quiet:
            logger.debug(f"\naa-detect-seafloor args:\n{pprint.pformat(vars(args))}")
        print(args.output_path.resolve())
        if not args.quiet:
            logger.info("Seafloor detection complete.")

    except Exception as e:
        logger.exception(f"Error during seafloor detection: {e}")
        sys.exit(1)


# argparse adds attribute access; define a property to avoid AttributeError if not set
setattr(argparse.Namespace, "emit_mask", property(lambda self: getattr(self, "emit_mask", False)))

if __name__ == "__main__":
    main()
