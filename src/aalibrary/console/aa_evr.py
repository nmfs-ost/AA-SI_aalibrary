#!/usr/bin/env python3
"""
aa-evr

Console tool that masks an echogram NetCDF using one or more Echoview region files (.evr).

Pipeline behavior (AA-style):
- Reads input NetCDF (.nc/.netcdf4) file path(s) from stdin (newline-delimited) OR accepts them as positional args.
- Produces a new NetCDF output (original unchanged by default).
- Emits output .nc path(s) to stdout (one per line) so it can be piped downstream.

Core behavior:
- Load all supplied .evr files
- Compute a UNION mask (inside any region from any .evr)
- Apply mask to dataset variables that share the echogram (time, depth) dims
  (values outside regions set to NaN)
"""

import argparse
import sys
import pprint
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

import numpy as np
import xarray as xr
from loguru import logger
import echoregions as er


def print_help() -> None:
    help_text = """

aa-evr — apply Echoview region(s) (.evr) to an echogram NetCDF (.nc)

USAGE
  # pipeline style
  aa-sv input.raw | aa-evr --evr regions1.evr regions2.evr | aa-mvbs

  # direct style
  aa-evr input.nc --evr regions.evr
  aa-evr input1.nc input2.nc --evr a.evr b.evr --out-dir masked/

INPUT
  - If no positional input is provided and stdin is piped, reads newline-delimited .nc paths from stdin.
  - Otherwise, accepts one or more positional input paths.

OUTPUT
  - Writes a new NetCDF per input.
  - Prints each output path to stdout (one per line) for downstream piping.

OPTIONS
  --evr EVR [EVR ...]      One or more .evr files after a SINGLE --evr (required).
  -o, --output_path PATH   Output file path (only valid when exactly 1 input is processed).
  --out-dir DIR            Output directory for per-input outputs (recommended for pipelines).
  --suffix TEXT            Suffix appended to output stem (default: _evr).
  --overwrite              Overwrite existing outputs.
  --var NAME               Data variable used to build/apply mask (default: Sv).
  --time-dim NAME          Time dimension name (default: auto: ping_time, else time).
  --depth-dim NAME         Depth dimension name (default: auto: depth, else range_bin).
  --channel-index INT      If var has 'channel' dim, choose which channel to build mask from (default: 0).
  --write-mask             Write union region mask to output as int8 variable 'region_mask'.
  --debug                  Verbose logging to stderr.

NOTES
  - This tool masks values outside the region(s) by setting them to NaN.
  - Logging goes to stderr so stdout remains clean for piping.

"""
    print(help_text)


def configure_logging(debug: bool) -> None:
    logger.remove()
    logger.add(sys.stderr, level=("DEBUG" if debug else "INFO"))


def iter_input_paths(positional_inputs: List[Path]) -> Iterable[Path]:
    """
    AA-style input resolution:
    - If positional inputs are provided, use them.
    - Else, if stdin is piped, read newline-delimited paths.
    - Else, show help and exit.
    """
    if positional_inputs:
        for p in positional_inputs:
            yield p
        return

    if not sys.stdin.isatty():
        for line in sys.stdin:
            s = line.strip()
            if s:
                yield Path(s)
        return

    print_help()
    sys.exit(0)


def validate_inputs(input_paths: List[Path], evr_paths: List[Path]) -> None:
    allowed_ext = {".nc", ".netcdf4"}

    if not evr_paths:
        logger.error("At least one --evr file is required.")
        sys.exit(2)

    for evr in evr_paths:
        if evr.suffix.lower() != ".evr":
            logger.warning(f"EVR file does not end with .evr: {evr}")
        if not evr.exists():
            logger.error(f"EVR file not found: {evr}")
            sys.exit(2)

    for p in input_paths:
        if not p.exists():
            logger.error(f"Input file not found: {p}")
            sys.exit(1)
        if p.suffix.lower() not in allowed_ext:
            logger.error(f"Unsupported input extension for {p.name}. Allowed: {', '.join(sorted(allowed_ext))}")
            sys.exit(1)


def infer_dims(
    ds: xr.Dataset,
    var: str,
    time_dim: Optional[str],
    depth_dim: Optional[str],
) -> Tuple[str, str]:
    if var not in ds.data_vars:
        raise ValueError(
            f"Variable '{var}' not found in dataset. Available: {list(ds.data_vars.keys())}"
        )

    da = ds[var]

    if time_dim:
        tdim = time_dim
    else:
        if "ping_time" in da.dims:
            tdim = "ping_time"
        elif "time" in da.dims:
            tdim = "time"
        else:
            raise ValueError(
                f"Could not infer time dim for '{var}'. Provide --time-dim."
            )

    if depth_dim:
        ddim = depth_dim
    else:
        if "depth" in da.dims:
            ddim = "depth"
        elif "range_bin" in da.dims:
            ddim = "range_bin"
        else:
            raise ValueError(
                f"Could not infer depth dim for '{var}'. Provide --depth-dim."
            )

    if tdim not in da.dims:
        raise ValueError(f"--time-dim '{tdim}' not found in {var}.dims={da.dims}")
    if ddim not in da.dims:
        raise ValueError(f"--depth-dim '{ddim}' not found in {var}.dims={da.dims}")

    return tdim, ddim


def build_union_region_mask(
    ds: xr.Dataset,
    evr_files: List[Path],
    var: str,
    time_dim: str,
    depth_dim: str,
    channel_index: int = 0,
) -> xr.DataArray:
    """
    Returns a boolean DataArray mask aligned to ds[var] on (time_dim, depth_dim):
      True  => inside ANY region across ALL evr files
      False => outside all regions
    """
    da = ds[var]

    # If channel exists, pick one channel to build the mask from (matches echoregions examples)
    if "channel" in da.dims:
        if channel_index < 0 or channel_index >= da.sizes["channel"]:
            raise ValueError(
                f"--channel-index {channel_index} out of range for channel size {da.sizes['channel']}"
            )
        da2 = da.isel(channel=channel_index)
        # echoregions examples often drop 'channel' coord/var when operating on a single channel
        if "channel" in da2.coords:
            try:
                da2 = da2.drop_vars("channel")
            except Exception:
                pass
    else:
        da2 = da

    # echoregions expects a single DataArray; its docs use dims named ping_time and depth
    rename_map = {}
    if time_dim != "ping_time":
        rename_map[time_dim] = "ping_time"
    if depth_dim != "depth":
        rename_map[depth_dim] = "depth"
    if rename_map:
        da2 = da2.rename(rename_map)

    # Ensure order is consistent (ping_time, depth) for masking
    if set(["ping_time", "depth"]).issubset(set(da2.dims)):
        da2 = da2.transpose("ping_time", "depth", ...)

    # If dask-backed, compute to avoid regionmask surprises (docs call .compute())
    try:
        da2c = da2.compute()
    except Exception:
        da2c = da2

    union_mask: Optional[xr.DataArray] = None

    for evr_path in evr_files:
        regions2d = er.read_evr(str(evr_path))

        # Build 3D mask (region_id, depth, ping_time) by default when collapse_to_2d=False
        region_mask_ds, _region_points = regions2d.region_mask(
            da2c,
            collapse_to_2d=False,
        )

        if "mask_3d" not in region_mask_ds:
            raise RuntimeError(f"Expected 'mask_3d' in region_mask output for {evr_path}")

        # Union within this EVR: any region_id == 1 at each (ping_time, depth)
        # mask_3d dims are typically (region_id, depth, ping_time), so max over region_id -> (depth, ping_time)
        file_mask = (region_mask_ds["mask_3d"].max("region_id") == 1)

        # Make sure dims are (ping_time, depth) for stable broadcasting later
        if set(["ping_time", "depth"]).issubset(set(file_mask.dims)):
            file_mask = file_mask.transpose("ping_time", "depth")

        union_mask = file_mask if union_mask is None else (union_mask | file_mask)

    if union_mask is None:
        raise RuntimeError("No region mask produced (no EVR files processed?).")

    # Rename mask dims back to dataset dims
    inv_rename = {v: k for k, v in rename_map.items()}
    if inv_rename:
        union_mask = union_mask.rename(inv_rename)

    # Ensure final mask dims order is (time_dim, depth_dim)
    if set([time_dim, depth_dim]).issubset(set(union_mask.dims)):
        union_mask = union_mask.transpose(time_dim, depth_dim)

    return union_mask


def apply_mask_to_dataset(
    ds: xr.Dataset,
    mask: xr.DataArray,
    time_dim: str,
    depth_dim: str,
    write_mask: bool,
) -> xr.Dataset:
    """
    Apply mask to all data variables that contain (time_dim, depth_dim).
    Values outside mask become NaN.
    """
    ds_out = ds.copy(deep=False)

    for name, da in list(ds_out.data_vars.items()):
        if (time_dim in da.dims) and (depth_dim in da.dims):
            ds_out[name] = xr.where(mask, da, np.nan)

    if write_mask:
        ds_out["region_mask"] = mask.astype("int8")
        ds_out["region_mask"].attrs.update(
            long_name="Union region mask from EVR files (1=inside, 0=outside)"
        )

    return ds_out


def resolve_output_path(
    input_path: Path,
    output_path: Optional[Path],
    out_dir: Optional[Path],
    suffix: str,
) -> Path:
    if output_path is not None:
        return output_path

    out_base = input_path.with_suffix("").name + suffix + ".nc"
    return (out_dir or input_path.parent) / out_base


def process_file(
    input_path: Path,
    evr_files: List[Path],
    output_path: Path,
    overwrite: bool,
    var: str,
    time_dim: Optional[str],
    depth_dim: Optional[str],
    channel_index: int,
    write_mask: bool,
) -> Optional[Path]:
    if output_path.exists() and not overwrite:
        logger.error(f"Output exists (use --overwrite): {output_path}")
        return None

    logger.info(f"Loading NetCDF: {input_path}")
    ds = xr.open_dataset(input_path)

    tdim, ddim = infer_dims(ds, var=var, time_dim=time_dim, depth_dim=depth_dim)

    logger.debug(f"Inferred dims for var='{var}': time_dim='{tdim}', depth_dim='{ddim}'")

    mask = build_union_region_mask(
        ds=ds,
        evr_files=evr_files,
        var=var,
        time_dim=tdim,
        depth_dim=ddim,
        channel_index=channel_index,
    )

    inside = int(mask.sum().item()) if hasattr(mask.sum(), "item") else int(mask.sum())
    total = int(mask.size)
    if inside == 0:
        logger.warning("Union mask contains 0 'inside' cells; output variables will be all-NaN where masked.")
    else:
        logger.info(f"Union mask coverage: {inside}/{total} cells inside regions")

    ds_out = apply_mask_to_dataset(ds, mask=mask, time_dim=tdim, depth_dim=ddim, write_mask=write_mask)

    # Add minimal provenance
    ds_out.attrs["aa_tool"] = "aa-evr"
    ds_out.attrs["aa_evr_files"] = ",".join(str(p) for p in evr_files)

    logger.info(f"Writing output NetCDF: {output_path}")
    ds_out.to_netcdf(output_path)

    try:
        ds.close()
    except Exception:
        pass

    return output_path


def main() -> None:
    # If invoked with no args and stdin is not a tty, we still want argparse to run,
    # so we do NOT mutate sys.argv here. We handle stdin inputs after parsing.
    parser = argparse.ArgumentParser(
        description="Mask echogram NetCDF (.nc) using Echoview region file(s) (.evr)."
    )

    # Positional input(s) are optional; if omitted, we read from stdin (pipeline style)
    parser.add_argument(
        "input_paths",
        nargs="*",
        type=Path,
        help="Input .nc/.netcdf4 file path(s). If omitted, read from stdin.",
    )

    parser.add_argument(
        "--evr",
        required=True,
        nargs="+",      # <-- single --evr, multiple arguments
        type=Path,
        help="One or more Echoview .evr region files (provide multiple paths after a single --evr).",
    )

    parser.add_argument(
        "-o",
        "--output_path",
        type=Path,
        help="Output file path (only valid when exactly 1 input is processed).",
    )

    parser.add_argument(
        "--out-dir",
        type=Path,
        help="Output directory for per-input outputs (recommended for pipelines).",
    )

    parser.add_argument(
        "--suffix",
        type=str,
        default="_evr",
        help="Suffix appended to output stem (default: _evr).",
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing outputs.",
    )

    parser.add_argument(
        "--var",
        type=str,
        default="Sv",
        help="Data variable used to build/apply region mask (default: Sv).",
    )

    parser.add_argument(
        "--time-dim",
        type=str,
        default=None,
        help="Time dimension name (default: infer ping_time, else time).",
    )

    parser.add_argument(
        "--depth-dim",
        type=str,
        default=None,
        help="Depth dimension name (default: infer depth, else range_bin).",
    )

    parser.add_argument(
        "--channel-index",
        type=int,
        default=0,
        help="If var has 'channel' dim, choose which channel to build mask from (default: 0).",
    )

    parser.add_argument(
        "--write-mask",
        action="store_true",
        help="Write union region mask to output as int8 variable 'region_mask'.",
    )

    parser.add_argument(
        "--debug",
        action="store_true",
        help="Verbose logging to stderr.",
    )

    args = parser.parse_args()
    configure_logging(args.debug)

    # Resolve inputs (positional or stdin)
    input_paths = [p for p in iter_input_paths(args.input_paths)]
    if not input_paths:
        logger.error("No input paths provided (positional or via stdin).")
        sys.exit(1)

    # Expand/resolve paths
    input_paths = [p.expanduser().resolve() for p in input_paths]
    evr_files = [p.expanduser().resolve() for p in args.evr]

    # Validate
    validate_inputs(input_paths, evr_files)

    # Output path rules
    if args.output_path is not None and len(input_paths) != 1:
        logger.error("--output_path is only valid when processing exactly 1 input. Use --out-dir instead.")
        sys.exit(2)

    if args.out_dir is not None:
        args.out_dir = args.out_dir.expanduser().resolve()
        args.out_dir.mkdir(parents=True, exist_ok=True)

    # Pretty-print args (debug)
    if args.debug:
        logger.debug(f"\naa-evr args:\n{pprint.pformat(vars(args))}")

    # Process each input; print each output path to stdout
    any_fail = False
    for in_path in input_paths:
        out_path = resolve_output_path(
            input_path=in_path,
            output_path=(args.output_path.expanduser().resolve() if args.output_path else None),
            out_dir=args.out_dir,
            suffix=args.suffix,
        )

        try:
            produced = process_file(
                input_path=in_path,
                evr_files=evr_files,
                output_path=out_path,
                overwrite=args.overwrite,
                var=args.var,
                time_dim=args.time_dim,
                depth_dim=args.depth_dim,
                channel_index=args.channel_index,
                write_mask=args.write_mask,
            )
            if produced is not None:
                logger.success(f"Saved masked NetCDF:\n\t{produced}")
                logger.success("Piping saved .nc path to stdout ⟶")
                print(str(produced))
            else:
                any_fail = True
        except Exception as e:
            any_fail = True
            logger.exception(f"Error processing {in_path}: {e}")

    sys.exit(1 if any_fail else 0)


if __name__ == "__main__":
    main()