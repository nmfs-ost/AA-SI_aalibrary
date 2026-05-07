#!/usr/bin/env python3
"""
aa-detect-seafloor

Console tool to detect the seafloor (bottom line) using Echopype's
dispatcher echopype.mask.detect_seafloor(ds, method, params) and save
the result. Optionally emits a 2D bottom mask and/or applies it to Sv.

Pipeline-friendly: reads input path from positional arg or stdin, writes
output path (the bottom-line file) to stdout, all logs to stderr.

Typical pipeline usage:
    aa-nc --sonar_model EK60 input.raw | aa-sv | \\
        aa-detect-seafloor --method blackwell --apply
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
import ast
import pprint
from pathlib import Path

import echopype as ep
import xarray as xr
from echopype.mask import detect_seafloor, apply_mask as ep_apply_mask


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
    Usage: aa-detect-seafloor [OPTIONS] [INPUT_PATH]

    Arguments:
    INPUT_PATH                  Path to the calibrated Sv .nc / .netcdf4
                                file, or a converted Echopype file that
                                can be calibrated to Sv.
                                Optional. Defaults to stdin if not provided.

    Options:
    -o, --output_path           Path to save the bottom-line dataset.
                                Default: same directory as input, with
                                '_seafloor' appended to the stem and a
                                .nc suffix.

    --method                    Seafloor detection method (dispatcher key),
                                e.g. 'basic', 'blackwell'. (REQUIRED)
    --param KEY=VAL [...]       Parameters for the chosen method as
                                key=value pairs. Values are safely parsed
                                (int / float / bool / None) when possible;
                                strings like '10m' remain strings.

    --emit-mask                 Also compute and save a 2D boolean mask of
                                samples below the bottom line, suffix
                                '_seafloor_mask' (True = below bottom).
    --range-label               Range/depth variable name used to build the
                                mask. Default: echo_range
    --apply                     Apply the bottom mask to Sv and write a
                                cleaned Sv file, suffix '_seafloor_cleaned'.
                                Implies mask construction.

    --no-overwrite              Do not overwrite existing output files.

    Description:
    Dispatches to detect_seafloor(ds, method, params) and returns a 1-D
    bottom line (per ping). With --emit-mask, builds a 2D mask by
    comparing range to the bottom line (True below bottom). With
    --apply, applies the mask to Sv via echopype.mask.apply_mask. The
    bottom-line path is printed to stdout for piping into the next stage
    of the pipeline.

    Examples:
        aa-detect-seafloor input_Sv.nc --method blackwell --emit-mask
        aa-sv input.nc | aa-detect-seafloor --method basic \\
              --param threshold=-40.0 --apply
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
        description="Detect the seafloor with Echopype's detect_seafloor dispatcher.",
        add_help=False,
    )

    # I/O
    parser.add_argument(
        "input_path",
        type=Path,
        nargs="?",
        help="Path to the .nc / .netcdf4 file containing Sv.",
    )
    parser.add_argument(
        "-o", "--output_path",
        type=Path,
        help="Path to save the bottom line. Default appends '_seafloor' to the input stem.",
    )
    parser.add_argument(
        "--no-overwrite",
        dest="no_overwrite",
        action="store_true",
        help="Do not overwrite existing outputs.",
    )

    # detect_seafloor params
    parser.add_argument(
        "--method",
        required=True,
        help="Seafloor detection method key (e.g., 'basic', 'blackwell').",
    )
    parser.add_argument(
        "--param",
        nargs="*",
        default=None,
        help="Method parameters as key=value pairs.",
    )

    # mask / apply
    parser.add_argument(
        "--emit-mask",
        dest="emit_mask",
        action="store_true",
        help="Also compute/save a 2D mask (True = below bottom).",
    )
    parser.add_argument(
        "--range-label",
        dest="range_label",
        default="echo_range",
        help="Range/depth variable name used to build mask (default: echo_range).",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply the bottom mask to Sv and write cleaned Sv.",
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
    # Resolve output paths
    # ---------------------------
    if args.output_path is None:
        args.output_path = args.input_path.with_stem(
            args.input_path.stem + "_seafloor"
        ).with_suffix(".nc")
    else:
        args.output_path = args.output_path.with_suffix(".nc")

    mask_path = args.input_path.with_stem(
        args.input_path.stem + "_seafloor_mask"
    ).with_suffix(".nc")
    cleaned_path = args.input_path.with_stem(
        args.input_path.stem + "_seafloor_cleaned"
    ).with_suffix(".nc")

    # Guard against clobbering the input. This applies to ALL outputs
    # because all three side-output paths are derived from the input
    # stem — pathological flag combinations could collide.
    input_resolved = args.input_path.resolve()
    for out in (args.output_path, mask_path, cleaned_path):
        if out.resolve() == input_resolved:
            logger.error(f"Refusing to overwrite input file: {input_resolved}")
            sys.exit(1)

    # Optional: refuse to overwrite existing outputs
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

    # Parse --param key=value pairs early so a bad pair fails before
    # we touch the dataset.
    try:
        params = _parse_kv_pairs(args.param)
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
            "method": args.method,
            "params": params,
            "emit_mask": args.emit_mask,
            "apply": args.apply,
            "range_label": args.range_label,
            "no_overwrite": args.no_overwrite,
        }
        logger.debug(
            f"Executing aa-detect-seafloor configured with [OPTIONS]:\n"
            f"{pprint.pformat(args_summary)}"
        )

        process_file(
            input_path=args.input_path,
            output_path=args.output_path,
            method=args.method,
            params=params,
            emit_mask=args.emit_mask,
            apply_mask=args.apply,
            range_label=args.range_label,
            mask_path=mask_path,
            cleaned_path=cleaned_path,
        )

        logger.success(
            f"Generated {args.output_path.resolve()} with aa-detect-seafloor. "
            "Passing .nc path to stdout..."
        )
        # Pipe the bottom-line path to stdout for the next tool
        print(args.output_path.resolve())

    except Exception as e:
        logger.exception(f"Error during processing: {e}")
        sys.exit(1)


def _parse_kv_pairs(pairs):
    """Convert KEY=VAL strings to dict; keep units strings ('10m','12dB')
    as strings, but coerce ints/floats/bools/None via ast.literal_eval."""
    out = {}
    for p in pairs or []:
        if "=" not in p:
            raise argparse.ArgumentTypeError(f"Invalid key=value pair: {p}")
        k, v = p.split("=", 1)
        k = k.strip()
        v = v.strip()
        try:
            out[k] = ast.literal_eval(v)
        except (ValueError, SyntaxError):
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
    method: str,
    params: dict,
    emit_mask: bool = False,
    apply_mask: bool = False,
    range_label: str = "echo_range",
    mask_path: Path = None,
    cleaned_path: Path = None,
):
    """Load Sv from NetCDF, dispatch detect_seafloor, and save the bottom
    line. Optionally compute and save a 2D below-bottom mask, and
    optionally apply that mask to Sv to produce a cleaned file."""

    logger.info(f"Loading dataset from {input_path}")
    ds = xr.open_dataset(input_path)

    # If 'Sv' isn't present, fall back to calibrating from a converted
    # Echopype file. Mirrors aa-sv's contract so this tool can sit
    # anywhere downstream of aa-nc in a pipeline.
    if "Sv" not in ds.data_vars:
        logger.info("No 'Sv' variable found; calibrating to Sv via Echopype")
        ed = ep.open_converted(input_path)
        ds = ep.calibrate.compute_Sv(ed)

    logger.info(f"Detecting seafloor with method='{method}' and params={params}")
    bottom = detect_seafloor(ds=ds, method=method, params=params)

    # Save the 1-D bottom line
    bottom_ds = bottom.to_dataset(name="seafloor")
    bottom_ds["seafloor"].attrs.setdefault("long_name", "Seafloor bottom line")
    bottom_ds["seafloor"].attrs.setdefault("units", "m")
    bottom_ds = clean_attrs(bottom_ds)

    output_path = output_path.with_suffix(".nc")
    logger.info(f"Saving seafloor bottom line to {output_path}")
    bottom_ds.to_netcdf(output_path, mode="w", format="NETCDF4")

    # The 2D mask is needed for either --emit-mask or --apply, so build
    # once and reuse.
    if emit_mask or apply_mask:
        # Pull the range/depth variable that the mask is defined against
        if (range_label not in ds
                and range_label not in ds.coords
                and range_label not in ds.data_vars):
            raise KeyError(
                f"Range variable '{range_label}' not found; cannot build bottom mask."
            )

        rng = ds[range_label]
        # echo_range is typically 2D (ping x range_sample); bottom is 1-D
        # over the ping/time axis. Broadcast bottom across the range
        # dimension so the comparison produces a 2D mask.
        try:
            bottom_b = (
                bottom.broadcast_like(rng)
                if set(bottom.dims) <= set(rng.dims)
                else bottom
            )
            mask2d = rng > bottom_b
        except Exception:
            # Fallback: align along the first shared dimension. This path
            # rarely fires but is cheap insurance for unusual coord names.
            shared = [d for d in bottom.dims if d in rng.dims]
            if not shared:
                raise RuntimeError(
                    "Cannot align bottom line with range variable to form a mask."
                )
            bottom_b = bottom.transpose(*shared).broadcast_like(rng)
            mask2d = rng > bottom_b

        if emit_mask:
            mask_ds = mask2d.to_dataset(name="seafloor_mask")
            mask_ds = clean_attrs(mask_ds)
            logger.info(f"Saving bottom mask to {mask_path}")
            mask_ds.to_netcdf(mask_path, mode="w", format="NETCDF4")

        if apply_mask:
            logger.info(f"Applying bottom mask to Sv and saving to {cleaned_path}")
            ds_clean = ep_apply_mask(source_ds=ds, mask=mask2d, var_name="Sv")
            ds_clean = clean_attrs(ds_clean)
            ds_clean.to_netcdf(cleaned_path, mode="w", format="NETCDF4")

    logger.success(f"Seafloor detection complete: {output_path.resolve()}")


if __name__ == "__main__":
    main()