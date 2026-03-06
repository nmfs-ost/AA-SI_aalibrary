#!/usr/bin/env python3
"""
aa-evr

Apply Echoview region files (.evr) to an echogram NetCDF (.nc/.netcdf4) using echoregions Regions2D.

Pipeline behavior (AA-style):
- Accepts input NetCDF path(s) as positional arguments OR reads newline-delimited paths from stdin.
- Produces a NEW NetCDF output per input (original unchanged).
- Prints the output .nc path(s) to stdout (one per line) for downstream piping.
- Logs go to stderr (keeps stdout clean).

Core behavior:
- --evr is provided ONCE and accepts one or more .evr file paths after it.
- Loads all EVRs, unions all regions across all EVRs into one mask.
- Applies the union mask to all dataset variables that include (time_dim, depth_dim) by setting outside-region values to NaN.

Important nuance:
- EVR regions are defined in depth/range units (typically meters).
- Many echopype-derived Sv datasets have vertical dimension like range_sample/range_bin (index),
  plus a meters-valued coordinate/variable like 'depth' (often added by aa-depth) or 'echo_range'.
  aa-evr will prefer a meters-valued depth coordinate when available.
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


# ---------------------------
# Help / UX
# ---------------------------

def print_help() -> None:
    help_text = """

aa-evr — mask echogram NetCDF using Echoview region file(s) (.evr)

USAGE
  # pipeline style (stdin->stdout)
  aa-sv input.raw | aa-depth | aa-evr --evr regions/*.evr | aa-plot --all

  # direct style
  aa-evr input.nc --evr a.evr b.evr

  # many inputs
  aa-evr a.nc b.nc --evr regions/*.evr --out-dir masked/

INPUT
  INPUT_PATH [INPUT_PATH ...]
    Optional positional input .nc/.netcdf4 path(s). If omitted, aa-evr reads from stdin
    (newline-delimited paths).

REQUIRED
  --evr EVR [EVR ...]
    One or more .evr file paths after a SINGLE --evr flag.

OUTPUT
  -o, --output_path PATH
    Output file path (ONLY valid when processing exactly 1 input).
  --out-dir DIR
    Output directory for per-input outputs (recommended for pipelines / multiple inputs).
  --suffix TEXT
    Suffix appended to output stem (default: _evr)
  --overwrite
    Overwrite existing outputs

MASKING
  --var NAME
    Variable used to build/apply region mask (default: Sv)
  --time-dim NAME
    Time dimension name (default: infer ping_time, else time)
  --depth-dim NAME
    Depth dimension name (default: infer depth, else range_sample, else range_bin)

DEPTH COORDINATE (IMPORTANT)
  EVR depths are typically in meters. If your depth dimension is an index (e.g., range_sample),
  aa-evr will try to attach a meters-valued depth coordinate automatically:
    - prefers dataset variable/coord named 'depth' (often from aa-depth),
    - falls back to 'echo_range' if present.

OTHER
  --channel-index INT   If --var has a 'channel' dim, which channel to build the mask from (default: 0)
  --write-mask          Write union mask to output as int8 variable 'region_mask'
  --fail-empty          Exit non-zero if the union mask has zero inside cells
  --debug               Verbose logging to stderr

NOTES
  - Logging goes to stderr so stdout stays clean for piping.
  - Output values outside the region union are set to NaN (structure preserved).

"""
    print(help_text)


def _configure_logging(debug: bool) -> None:
    logger.remove()
    logger.add(sys.stderr, level=("DEBUG" if debug else "INFO"))


# ---------------------------
# Input handling
# ---------------------------

def _iter_input_paths(positional_inputs: List[Path]) -> Iterable[Path]:
    """
    AA-style:
      - If positional inputs exist, use them.
      - Else, if stdin is piped, read newline-delimited paths.
      - Else, show help and exit 0.
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


def _validate_inputs(input_paths: List[Path], evr_paths: List[Path]) -> None:
    allowed_ext = {".nc", ".netcdf4"}

    if not evr_paths:
        logger.error("At least one --evr file is required.")
        sys.exit(2)

    for evr in evr_paths:
        if not evr.exists():
            logger.error(f"EVR file not found: {evr}")
            sys.exit(2)

    for p in input_paths:
        if not p.exists():
            logger.error(f"Input file not found: {p}")
            sys.exit(1)
        if p.suffix.lower() not in allowed_ext:
            logger.error(
                f"Unsupported input extension for {p.name}. Allowed: {', '.join(sorted(allowed_ext))}"
            )
            sys.exit(1)


# ---------------------------
# Mask building utilities
# ---------------------------

def _infer_dims(
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

    # time dim
    if time_dim:
        tdim = time_dim
    else:
        if "ping_time" in da.dims:
            tdim = "ping_time"
        elif "time" in da.dims:
            tdim = "time"
        else:
            raise ValueError(f"Could not infer time dim for '{var}'. Provide --time-dim.")

    # depth dim
    if depth_dim:
        ddim = depth_dim
    else:
        if "depth" in da.dims:
            ddim = "depth"
        elif "range_sample" in da.dims:
            ddim = "range_sample"
        elif "range_bin" in da.dims:
            ddim = "range_bin"
        else:
            raise ValueError(f"Could not infer depth dim for '{var}'. Provide --depth-dim.")

    if tdim not in da.dims:
        raise ValueError(f"--time-dim '{tdim}' not found in {var}.dims={da.dims}")
    if ddim not in da.dims:
        raise ValueError(f"--depth-dim '{ddim}' not found in {var}.dims={da.dims}")

    return tdim, ddim


def _select_depth_coordinate_meters(
    ds: xr.Dataset,
    da: xr.DataArray,
    time_dim: str,
    depth_dim: str,
    channel_index: int,
) -> Optional[xr.DataArray]:
    """
    Try to find a meters-valued depth/range coordinate/variable that matches (time_dim, depth_dim).

    Preference:
      1) ds['depth'] (aa-depth commonly adds this)
      2) ds['echo_range'] (echopype often provides this)
      3) da.coords['depth'] or da.coords['echo_range'] if present

    Returns an array that can be attached as coordinate named 'depth' for region masking,
    after dims are renamed to ('ping_time','depth').
    """
    candidates = []

    # dataset vars
    for name in ("depth", "echo_range"):
        if name in ds:
            candidates.append(ds[name])

    # variable coords
    for name in ("depth", "echo_range"):
        if name in da.coords:
            candidates.append(da.coords[name])

    for c in candidates:
        if c is None:
            continue

        # select channel if needed
        if "channel" in c.dims and "channel" in da.dims:
            try:
                c = c.isel(channel=channel_index)
                if "channel" in c.coords:
                    try:
                        c = c.drop_vars("channel")
                    except Exception:
                        pass
            except Exception:
                continue

        # require it to be indexable/broadcastable over (time_dim, depth_dim)
        if time_dim in c.dims and depth_dim in c.dims:
            return c

        # Sometimes it's (depth_dim,) only (e.g., a 1D range in meters)
        if depth_dim in c.dims and time_dim not in c.dims:
            return c

    return None


def _build_union_region_mask(
    ds: xr.Dataset,
    evr_files: List[Path],
    var: str,
    time_dim: str,
    depth_dim: str,
    channel_index: int,
) -> xr.DataArray:
    """
    Build a boolean union mask aligned to ds[var] on (time_dim, depth_dim):
      True  => inside any region across all EVR files
      False => outside all regions
    """
    da = ds[var]

    # Choose one channel for mask construction if present
    if "channel" in da.dims:
        if channel_index < 0 or channel_index >= da.sizes["channel"]:
            raise ValueError(
                f"--channel-index {channel_index} out of range for channel size {da.sizes['channel']}"
            )
        da2 = da.isel(channel=channel_index)
        if "channel" in da2.coords:
            try:
                da2 = da2.drop_vars("channel")
            except Exception:
                pass
    else:
        da2 = da

    # Try to attach meters-valued depth coordinate
    depth_coord_m = _select_depth_coordinate_meters(
        ds=ds, da=da, time_dim=time_dim, depth_dim=depth_dim, channel_index=channel_index
    )
    if depth_coord_m is not None:
        logger.debug(
            f"Using meters-valued depth coordinate for masking: dims={depth_coord_m.dims} "
            f"name={'depth' if 'depth' in ds else ('echo_range' if 'echo_range' in ds else 'coord')}"
        )
    else:
        logger.warning(
            "No meters-valued depth/echo_range coordinate found that matches "
            f"({time_dim}, {depth_dim}). Masking will use the '{depth_dim}' coordinate values directly "
            "(this may produce an empty mask if depth_dim is an index like range_sample)."
        )

    # Rename dims to echoregions example names
    rename_map = {time_dim: "ping_time", depth_dim: "depth"}
    da2m = da2.rename(rename_map)

    if depth_coord_m is not None:
        depth_coord_m = depth_coord_m.rename(rename_map)

        # Attach as the coordinate named 'depth' (meters)
        # If depth_coord_m is 1D on 'depth', this still works.
        da2m = da2m.assign_coords(depth=depth_coord_m)

    # Ensure ordering
    da2m = da2m.transpose("ping_time", "depth", ...)

    # Compute (dask safety)
    try:
        da2m_c = da2m.compute()
    except Exception:
        da2m_c = da2m

    union_mask: Optional[xr.DataArray] = None

    for evr_path in evr_files:
        regions2d = er.read_evr(str(evr_path))

        region_mask_ds, _region_points = regions2d.region_mask(
            da2m_c,
            collapse_to_2d=False,
        )

        if "mask_3d" not in region_mask_ds:
            raise RuntimeError(f"Expected 'mask_3d' in region_mask output for {evr_path}")

        # Union within this EVR over region_id
        file_mask = (region_mask_ds["mask_3d"].max("region_id") == 1)

        # Standardize dims to (ping_time, depth)
        if set(["ping_time", "depth"]).issubset(set(file_mask.dims)):
            file_mask = file_mask.transpose("ping_time", "depth")

        union_mask = file_mask if union_mask is None else (union_mask | file_mask)

    if union_mask is None:
        raise RuntimeError("No region mask produced (no EVR files processed?).")

    # Rename dims back to dataset dims
    inv_rename = {"ping_time": time_dim, "depth": depth_dim}
    union_mask = union_mask.rename(inv_rename)

    # Ensure final order for stable broadcasting
    if set([time_dim, depth_dim]).issubset(set(union_mask.dims)):
        union_mask = union_mask.transpose(time_dim, depth_dim)

    return union_mask


def _apply_mask_to_dataset(
    ds: xr.Dataset,
    mask: xr.DataArray,
    time_dim: str,
    depth_dim: str,
    write_mask: bool,
) -> xr.Dataset:
    """
    Apply mask to all data variables containing (time_dim, depth_dim).
    Outside-region values become NaN.
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


# ---------------------------
# Output path utilities
# ---------------------------

def _resolve_output_path(
    input_path: Path,
    output_path: Optional[Path],
    out_dir: Optional[Path],
    suffix: str,
) -> Path:
    if output_path is not None:
        return output_path

    out_name = input_path.with_suffix("").name + suffix + ".nc"
    return (out_dir or input_path.parent) / out_name


# ---------------------------
# Main processing
# ---------------------------

def _process_file(
    input_path: Path,
    evr_files: List[Path],
    output_path: Path,
    overwrite: bool,
    var: str,
    time_dim: Optional[str],
    depth_dim: Optional[str],
    channel_index: int,
    write_mask: bool,
    fail_empty: bool,
) -> Optional[Path]:
    if output_path.exists() and not overwrite:
        logger.error(f"Output exists (use --overwrite): {output_path}")
        return None

    logger.info(f"Loading NetCDF: {input_path}")
    ds = xr.open_dataset(input_path)

    tdim, ddim = _infer_dims(ds, var=var, time_dim=time_dim, depth_dim=depth_dim)
    logger.debug(f"Using dims for var='{var}': time_dim='{tdim}', depth_dim='{ddim}'")

    mask = _build_union_region_mask(
        ds=ds,
        evr_files=evr_files,
        var=var,
        time_dim=tdim,
        depth_dim=ddim,
        channel_index=channel_index,
    )

    inside = int(mask.sum().item()) if hasattr(mask.sum(), "item") else int(mask.sum())
    total = int(mask.size)
    logger.info(f"Union mask coverage: {inside}/{total} cells inside regions")

    if inside == 0 and fail_empty:
        logger.error("Union mask is empty (0 inside cells). Failing due to --fail-empty.")
        try:
            ds.close()
        except Exception:
            pass
        return None

    ds_out = _apply_mask_to_dataset(ds, mask=mask, time_dim=tdim, depth_dim=ddim, write_mask=write_mask)

    # Minimal provenance
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
    if len(sys.argv) == 1 and sys.stdin.isatty():
        print_help()
        sys.exit(0)

    parser = argparse.ArgumentParser(
        description="Mask echogram NetCDF (.nc) using Echoview region file(s) (.evr)."
    )

    parser.add_argument(
        "input_paths",
        nargs="*",
        type=Path,
        help="Input .nc/.netcdf4 path(s). If omitted, read from stdin.",
    )

    parser.add_argument(
        "--evr",
        required=True,
        nargs="+",  # single --evr, multiple paths
        type=Path,
        help="One or more .evr files (provide multiple paths after a single --evr).",
    )

    parser.add_argument(
        "-o",
        "--output_path",
        type=Path,
        help="Output file path (ONLY valid when processing exactly 1 input).",
    )

    parser.add_argument(
        "--out-dir",
        type=Path,
        help="Output directory for per-input outputs (recommended for pipelines / multiple inputs).",
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
        help="Overwrite existing output files.",
    )

    parser.add_argument(
        "--var",
        type=str,
        default="Sv",
        help="Variable used to build/apply the region mask (default: Sv).",
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
        help="Depth dimension name (default: infer depth, else range_sample, else range_bin).",
    )

    parser.add_argument(
        "--channel-index",
        type=int,
        default=0,
        help="If --var has a 'channel' dim, which channel index to build mask from (default: 0).",
    )

    parser.add_argument(
        "--write-mask",
        action="store_true",
        help="Write union mask to output as int8 variable 'region_mask'.",
    )

    parser.add_argument(
        "--fail-empty",
        action="store_true",
        help="Exit non-zero if the union mask has zero inside cells.",
    )

    parser.add_argument(
        "--debug",
        action="store_true",
        help="Verbose logging to stderr.",
    )

    args = parser.parse_args()
    _configure_logging(args.debug)

    input_paths = [p for p in _iter_input_paths(args.input_paths)]
    if not input_paths:
        logger.error("No input paths provided (positional or via stdin).")
        sys.exit(1)

    input_paths = [p.expanduser().resolve() for p in input_paths]
    evr_files = [p.expanduser().resolve() for p in args.evr]

    _validate_inputs(input_paths, evr_files)

    # output path rules
    if args.output_path is not None and len(input_paths) != 1:
        logger.error("--output_path is only valid when processing exactly 1 input. Use --out-dir instead.")
        sys.exit(2)

    out_dir = args.out_dir.expanduser().resolve() if args.out_dir else None
    if out_dir is not None:
        out_dir.mkdir(parents=True, exist_ok=True)

    if args.debug:
        logger.debug(f"\naa-evr args:\n{pprint.pformat(vars(args))}")

    any_fail = False
    for in_path in input_paths:
        out_path = _resolve_output_path(
            input_path=in_path,
            output_path=(args.output_path.expanduser().resolve() if args.output_path else None),
            out_dir=out_dir,
            suffix=args.suffix,
        )

        try:
            produced = _process_file(
                input_path=in_path,
                evr_files=evr_files,
                output_path=out_path,
                overwrite=args.overwrite,
                var=args.var,
                time_dim=args.time_dim,
                depth_dim=args.depth_dim,
                channel_index=args.channel_index,
                write_mask=args.write_mask,
                fail_empty=args.fail_empty,
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