#!/usr/bin/env python3
"""
aa-evr

Mask echogram NetCDF (.nc/.netcdf4) using Echoview region files (.evr) via echoregions Regions2D.

AA-style pipeline behavior:
- Reads input NetCDF paths from stdin (newline-delimited) when piped OR accepts positional input(s).
- Produces a NEW NetCDF output per input (original unchanged unless you explicitly point output_path to same file).
- Emits output path(s) to stdout (one per line) for downstream piping.
- Logs go to stderr.

Core behavior:
- --evr is provided ONCE and accepts one or more .evr paths after it (argparse nargs="+").
- Loads all EVRs, unions all regions across them, creates a union mask.
- Applies mask to all dataset variables that include (time_dim, depth_dim) by setting outside-region values to NaN.

Version compatibility:
- echoregions.read_evr has changed across versions. This tool inspects the installed signature and only passes
  supported kwargs (e.g., raw_range, min_depth/max_depth, convert_time, convert_range_edges).
- If convert_time isn't supported, we coerce region times to numpy datetime64 manually.
- If raw_range isn't supported, we still do robust depth “sentinel” replacement after parsing.

Common pitfall addressed:
- EVR depths are usually in meters, while Sv vertical dimension may be an index (e.g., range_sample).
  If the dataset contains meters-valued depth/range (echo_range or depth from aa-depth), this tool will use it
  as the depth coordinate for region masking (critical for non-empty masks).
"""

import argparse
import inspect
import sys
import pprint
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd
import xarray as xr
from loguru import logger

import echoregions as er


# ---------------------------
# Help / logging
# ---------------------------

def print_help() -> None:
    print(
        """
aa-evr — apply Echoview region(s) (.evr) to an echogram NetCDF (.nc/.netcdf4)

USAGE
  # pipeline style
  aa-sv input.raw | aa-depth | aa-evr --evr regions/*.evr --depth-dim range_sample | aa-plot --all

  # direct style
  aa-evr input.nc --evr a.evr b.evr

REQUIRED
  --evr EVR [EVR ...]
    One or more .evr paths after a single --evr flag.

INPUT
  INPUT_PATH [INPUT_PATH ...]
    Optional positional .nc paths. If omitted, reads newline-delimited .nc paths from stdin.

OUTPUT
  -o, --output_path PATH   Only valid when processing exactly 1 input
  --out-dir DIR            Output directory for pipelines/multiple inputs
  --suffix TEXT            Output suffix appended to input stem (default: _evr)
  --overwrite              Overwrite output files if they already exist

MASKING
  --var NAME               Variable to mask (default: Sv)
  --time-dim NAME          Default: infer ping_time else time
  --depth-dim NAME         Default: infer depth else range_sample else range_bin
  --channel-index INT      Channel used for building mask when var has 'channel' dim (default: 0)
  --write-mask             Write union mask to output as int8 variable 'region_mask'
  --fail-empty             Exit non-zero if union mask has zero inside cells (prevents silent blank plots)
  --debug                  Verbose diagnostics to stderr

NOTES
  - Logs go to stderr so stdout stays clean for piping.
  - Values outside the union of all regions are set to NaN.
"""
    )


def _configure_logging(debug: bool) -> None:
    logger.remove()
    logger.add(sys.stderr, level=("DEBUG" if debug else "INFO"))


# ---------------------------
# Input handling
# ---------------------------

def _iter_input_paths(positional: List[Path]) -> Iterable[Path]:
    if positional:
        yield from positional
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
            logger.error(f"Unsupported input extension: {p.name} (allowed: {', '.join(sorted(allowed_ext))})")
            sys.exit(1)


# ---------------------------
# Dataset helpers
# ---------------------------

def _infer_dims(ds: xr.Dataset, var: str, time_dim: Optional[str], depth_dim: Optional[str]) -> Tuple[str, str]:
    if var not in ds.data_vars:
        raise ValueError(f"Variable '{var}' not found. Available: {list(ds.data_vars.keys())}")

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
        raise ValueError(f"time dim '{tdim}' not in {var}.dims={da.dims}")
    if ddim not in da.dims:
        raise ValueError(f"depth dim '{ddim}' not in {var}.dims={da.dims}")

    return tdim, ddim


def _select_channel(da: xr.DataArray, channel_index: int) -> xr.DataArray:
    if "channel" not in da.dims:
        return da
    if channel_index < 0 or channel_index >= da.sizes["channel"]:
        raise ValueError(f"--channel-index {channel_index} out of range (size={da.sizes['channel']})")
    da2 = da.isel(channel=channel_index)
    # avoid carrying channel coord/var into masking surface
    if "channel" in da2.coords:
        try:
            da2 = da2.drop_vars("channel")
        except Exception:
            pass
    return da2


def _maybe_select_channel_for_aux(x: xr.DataArray, var_da: xr.DataArray, channel_index: int) -> xr.DataArray:
    if ("channel" in x.dims) and ("channel" in var_da.dims):
        x = x.isel(channel=channel_index)
        if "channel" in x.coords:
            try:
                x = x.drop_vars("channel")
            except Exception:
                pass
    return x


def _get_depth_vals_meters_1d(
    ds: xr.Dataset,
    var_da: xr.DataArray,
    time_dim: str,
    depth_dim: str,
    channel_index: int,
) -> Optional[np.ndarray]:
    """
    Return a *plain* 1D numpy vector in meters aligned to depth_dim.
    Using numpy avoids importing scalar coords (like ping_time) that can break xarray dimension construction.
    Preference:
      1) ds['echo_range']
      2) ds['depth'] (often created by aa-depth)
      3) var_da.coords['echo_range'/'depth']
    """
    # echo_range first
    if "echo_range" in ds:
        erng = _maybe_select_channel_for_aux(ds["echo_range"], var_da, channel_index)
        if depth_dim in erng.dims and time_dim not in erng.dims:
            return np.asarray(erng.values)
        if (time_dim in erng.dims) and (depth_dim in erng.dims):
            return np.asarray(erng.isel({time_dim: 0}).values)

    # depth next
    if "depth" in ds:
        dep = _maybe_select_channel_for_aux(ds["depth"], var_da, channel_index)
        if depth_dim in dep.dims and time_dim not in dep.dims:
            return np.asarray(dep.values)
        if (time_dim in dep.dims) and (depth_dim in dep.dims):
            return np.asarray(dep.isel({time_dim: 0}).values)

    # coords on var
    for name in ("echo_range", "depth"):
        if name in var_da.coords:
            c = _maybe_select_channel_for_aux(var_da.coords[name], var_da, channel_index)
            if depth_dim in c.dims and time_dim not in c.dims:
                return np.asarray(c.values)
            if (time_dim in c.dims) and (depth_dim in c.dims):
                return np.asarray(c.isel({time_dim: 0}).values)

    return None


# ---------------------------
# echoregions compatibility + normalization
# ---------------------------

def _read_evr_compat(
    evr_path: Path,
    *,
    depth_vals: Optional[np.ndarray],
    min_depth: Optional[float],
    max_depth: Optional[float],
    debug: bool,
):
    """
    Call echoregions.read_evr with only kwargs supported by the installed echoregions version.
    """
    sig = inspect.signature(er.read_evr)
    params = sig.parameters

    kwargs = {}

    # newer versions
    if "raw_range" in params and depth_vals is not None:
        kwargs["raw_range"] = np.asarray(depth_vals)

    if "min_depth" in params and min_depth is not None:
        kwargs["min_depth"] = float(min_depth)
    if "max_depth" in params and max_depth is not None:
        kwargs["max_depth"] = float(max_depth)

    if "convert_time" in params:
        kwargs["convert_time"] = True

    if "convert_range_edges" in params:
        # generally safe; if unsupported it won't be here
        kwargs["convert_range_edges"] = True

    if debug:
        logger.debug(f"read_evr signature: {sig}")
        logger.debug(f"read_evr kwargs used: {kwargs}")

    return er.read_evr(str(evr_path), **kwargs)


def _coerce_regions_time_to_datetime64(regions2d) -> None:
    """
    If regions2d stores time lists as strings/objects, coerce to numpy datetime64.
    Safe no-op if structure differs.
    """
    try:
        df = regions2d.to_dataframe()
    except Exception:
        return

    if "time" not in df.columns:
        return

    coerced = []
    for t_list in df["time"]:
        try:
            coerced.append([np.datetime64(pd.to_datetime(t)) for t in t_list])
        except Exception:
            coerced.append(t_list)

    try:
        df["time"] = coerced
        regions2d.data = df
    except Exception:
        pass


def _replace_depth_sentinels(regions2d, min_depth: float, max_depth: float) -> None:
    """
    Some EVR files use sentinel edges like -9999.99 / 9999.99 for polygon bounds.
    Replace extreme values with min_depth/max_depth derived from echogram depth range.
    """
    try:
        df = regions2d.to_dataframe()
    except Exception:
        return

    if "depth" not in df.columns:
        return

    def fix_list(d_list):
        out = []
        for d in d_list:
            try:
                x = float(d)
                if x <= -9000:
                    out.append(float(min_depth))
                elif x >= 9000:
                    out.append(float(max_depth))
                else:
                    out.append(x)
            except Exception:
                out.append(d)
        return out

    try:
        df["depth"] = [fix_list(d_list) for d_list in df["depth"]]
        regions2d.data = df
    except Exception:
        pass


def _close_regions_polygons(regions2d) -> None:
    """
    Close each region polygon if possible, so region_mask isn't confused by open shapes.
    Uses Regions2D.close_region(points) when available; safe no-op otherwise.
    """
    if not hasattr(regions2d, "close_region"):
        return

    try:
        df = regions2d.to_dataframe()
    except Exception:
        return

    if not {"time", "depth"}.issubset(df.columns):
        return

    new_times = []
    new_depths = []

    for t_list, d_list in zip(df["time"], df["depth"]):
        try:
            pts = list(zip(t_list, d_list))
            pts2 = regions2d.close_region(pts)
            t2, d2 = zip(*pts2)
            new_times.append(list(t2))
            new_depths.append(list(d2))
        except Exception:
            new_times.append(t_list)
            new_depths.append(d_list)

    try:
        df["time"] = new_times
        df["depth"] = new_depths
        regions2d.data = df
    except Exception:
        pass


# ---------------------------
# Mask building + application
# ---------------------------

def _build_union_region_mask(
    ds: xr.Dataset,
    evr_files: List[Path],
    var: str,
    time_dim: str,
    depth_dim: str,
    channel_index: int,
    debug: bool,
) -> xr.DataArray:
    """
    Build a boolean union mask aligned to ds[var] on (time_dim, depth_dim).
    """
    var_da = ds[var]
    da = _select_channel(var_da, channel_index)

    # Build the surface echoregions expects: dims ("ping_time","depth") + depth coord in meters if available
    da_for_mask = da.rename({time_dim: "ping_time", depth_dim: "depth"}).transpose("ping_time", "depth", ...)

    # Ensure ping_time is a clean 1D coordinate vector
    pt_vals = None
    if time_dim in ds:
        pt_vals = np.asarray(ds[time_dim].values)
    elif time_dim in da.coords:
        pt_vals = np.asarray(da.coords[time_dim].values)
    else:
        pt_vals = np.asarray(da_for_mask["ping_time"].values)
    da_for_mask = da_for_mask.assign_coords(ping_time=("ping_time", pt_vals))

    # Attach meters-valued depth coordinate vector if available (critical)
    depth_vals = _get_depth_vals_meters_1d(ds, var_da, time_dim=time_dim, depth_dim=depth_dim, channel_index=channel_index)
    if depth_vals is not None:
        da_for_mask = da_for_mask.assign_coords(depth=("depth", np.asarray(depth_vals)))
    else:
        # fallback: use whatever xarray has; may be indices -> empty masks
        if debug:
            logger.warning("No meters depth vector found (echo_range/depth). Mask may end up empty.")

    # compute if dask-backed (echoregions often computes in docs)
    try:
        da_for_mask = da_for_mask.compute()
    except Exception:
        pass

    # derive min/max for sentinel replacement + optional read_evr kwargs
    if depth_vals is not None:
        min_d = float(np.nanmin(depth_vals))
        max_d = float(np.nanmax(depth_vals))
    else:
        # still provide something reasonable
        min_d, max_d = 0.0, 10000.0

    if debug:
        logger.debug(f"Mask depth range (m): {min_d} → {max_d}")
        logger.debug(f"Mask time range: {pt_vals.min()} → {pt_vals.max()}")

    union_mask: Optional[xr.DataArray] = None

    for evr_path in evr_files:
        regions2d = _read_evr_compat(
            evr_path,
            depth_vals=depth_vals,
            min_depth=min_d,
            max_depth=max_d,
            debug=debug,
        )

        # normalize regions dataframe for older echoregions builds
        _coerce_regions_time_to_datetime64(regions2d)
        _replace_depth_sentinels(regions2d, min_depth=min_d, max_depth=max_d)
        _close_regions_polygons(regions2d)

        # compute region mask
        region_mask_ds, _ = regions2d.region_mask(da_for_mask, collapse_to_2d=False)

        if "mask_3d" not in region_mask_ds:
            raise RuntimeError(f"Expected 'mask_3d' in region_mask output for {evr_path}")

        # union within this EVR over region_id; use >0 robustly
        file_union = (region_mask_ds["mask_3d"].max("region_id") > 0)
        if set(["ping_time", "depth"]).issubset(set(file_union.dims)):
            file_union = file_union.transpose("ping_time", "depth")

        union_mask = file_union if union_mask is None else (union_mask | file_union)

    if union_mask is None:
        raise RuntimeError("No region mask produced.")

    # rename back to dataset dims and return
    union_mask = union_mask.rename({"ping_time": time_dim, "depth": depth_dim}).transpose(time_dim, depth_dim)
    return union_mask


def _apply_mask(
    ds: xr.Dataset,
    mask: xr.DataArray,
    time_dim: str,
    depth_dim: str,
    write_mask: bool,
) -> xr.Dataset:
    ds_out = ds.copy(deep=False)

    for name, da in list(ds_out.data_vars.items()):
        if (time_dim in da.dims) and (depth_dim in da.dims):
            ds_out[name] = xr.where(mask, da, np.nan)

    if write_mask:
        ds_out["region_mask"] = mask.astype("int8")
        ds_out["region_mask"].attrs["long_name"] = "Union region mask from EVR files (1=inside, 0=outside)"

    return ds_out


# ---------------------------
# Output paths + processing
# ---------------------------

def _resolve_output_path(input_path: Path, output_path: Optional[Path], out_dir: Optional[Path], suffix: str) -> Path:
    if output_path is not None:
        return output_path
    out_name = input_path.with_suffix("").name + suffix + ".nc"
    return (out_dir or input_path.parent) / out_name


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
    debug: bool,
) -> Optional[Path]:
    if output_path.exists() and not overwrite:
        logger.error(f"Output exists (use --overwrite): {output_path}")
        return None

    ds = xr.open_dataset(input_path)

    tdim, ddim = _infer_dims(ds, var=var, time_dim=time_dim, depth_dim=depth_dim)
    if debug:
        logger.debug(f"Using dims: time_dim='{tdim}', depth_dim='{ddim}', {var}.dims={ds[var].dims}")

    mask = _build_union_region_mask(
        ds=ds,
        evr_files=evr_files,
        var=var,
        time_dim=tdim,
        depth_dim=ddim,
        channel_index=channel_index,
        debug=debug,
    )

    inside = int(mask.sum().item()) if hasattr(mask.sum(), "item") else int(mask.sum())
    total = int(mask.size)
    logger.info(f"Union mask coverage: {inside}/{total} cells inside regions")

    if inside == 0:
        logger.error(
            "Union mask is EMPTY (0 inside cells). Output will plot blank.\n"
            "Most common causes: EVR time mismatch, or EVR depths in meters but dataset has no meters depth vector."
        )
        if fail_empty:
            try:
                ds.close()
            except Exception:
                pass
            return None

    ds_out = _apply_mask(ds, mask=mask, time_dim=tdim, depth_dim=ddim, write_mask=write_mask)

    ds_out.attrs["aa_tool"] = "aa-evr"
    ds_out.attrs["aa_evr_files"] = ",".join(str(p) for p in evr_files)

    ds_out.to_netcdf(output_path)

    try:
        ds.close()
    except Exception:
        pass

    return output_path


def main() -> int:
    if len(sys.argv) == 1 and sys.stdin.isatty():
        print_help()
        return 0

    parser = argparse.ArgumentParser(
        description="Mask echogram NetCDF (.nc) using Echoview region file(s) (.evr)."
    )

    parser.add_argument(
        "input_paths",
        nargs="*",
        type=Path,
        help="Input .nc/.netcdf4 paths. If omitted, read from stdin.",
    )

    parser.add_argument(
        "--evr",
        required=True,
        nargs="+",
        type=Path,
        help="One or more .evr paths after a single --evr.",
    )

    parser.add_argument(
        "-o",
        "--output_path",
        type=Path,
        help="Output path (ONLY valid when processing exactly 1 input).",
    )

    parser.add_argument(
        "--out-dir",
        type=Path,
        help="Output directory for pipelines/multiple inputs.",
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
        help="Overwrite output files if they exist.",
    )

    parser.add_argument(
        "--var",
        type=str,
        default="Sv",
        help="Variable to mask (default: Sv).",
    )

    parser.add_argument(
        "--time-dim",
        type=str,
        default=None,
        help="Time dimension name (default: infer ping_time else time).",
    )

    parser.add_argument(
        "--depth-dim",
        type=str,
        default=None,
        help="Depth dimension name (default: infer depth else range_sample else range_bin).",
    )

    parser.add_argument(
        "--channel-index",
        type=int,
        default=0,
        help="Channel used to build mask when var has 'channel' dim (default: 0).",
    )

    parser.add_argument(
        "--write-mask",
        action="store_true",
        help="Write union mask to output as int8 variable 'region_mask'.",
    )

    parser.add_argument(
        "--fail-empty",
        action="store_true",
        help="Exit non-zero if the union mask is empty (0 inside cells).",
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
        return 1

    input_paths = [p.expanduser().resolve() for p in input_paths]
    evr_files = [p.expanduser().resolve() for p in args.evr]
    _validate_inputs(input_paths, evr_files)

    if args.output_path is not None and len(input_paths) != 1:
        logger.error("--output_path is only valid when processing exactly 1 input. Use --out-dir instead.")
        return 2

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
                debug=args.debug,
            )
            if produced:
                logger.success(f"Saved masked NetCDF:\n\t{produced}")
                logger.success("Piping saved .nc path to stdout ⟶")
                print(str(produced))
            else:
                any_fail = True
        except Exception as e:
            any_fail = True
            logger.exception(f"Error processing {in_path}: {e}")

    return 1 if any_fail else 0


if __name__ == "__main__":
    raise SystemExit(main())