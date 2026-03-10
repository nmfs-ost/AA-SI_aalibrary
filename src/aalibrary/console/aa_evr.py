#!/usr/bin/env python3
"""
aa-evr

Mask echogram NetCDF (.nc/.netcdf4) using Echoview region files (.evr) via echoregions Regions2D.

AA-style pipeline behavior:
- Reads input NetCDF paths from stdin (newline-delimited) when piped OR accepts positional inputs.
- Produces a NEW NetCDF output per input.
- Emits output path(s) to stdout (one per line) for downstream piping.
- Logs go to stderr.

Core behavior:
- --evr accepts one or more .evr paths (argparse nargs="+").
- Loads all EVRs and unions all regions across them.
- Applies union mask to all variables containing (time_dim, depth_dim): outside -> NaN.

Drawing mode (--evr omitted):
- Launched when --evr is NOT provided.
- Reads one NC path from stdin (or positional arg).
- Opens an interactive Bokeh browser app showing the echogram.
- User draws freehand ROI polygons directly on the echogram.
- On save, produces:
    1. An .evr file (--name, default: <stem>_regions.evr).
    2. A masked NetCDF (_evr.nc) with only drawn regions kept.
- Output NC path is emitted to stdout for downstream piping.

  Example:
    cat D20191001-T003423_Sv_depth.nc | aa-evr --name my_school.evr
    cat D20191001-T003423_Sv_depth.nc | aa-evr --name school.evr | aa-plot --all

Known fixes in this version:
  1. Zero-region crash: when echoregions returns mask_3d with region_id dimension of
     size 0 (no polygons overlap the echogram time window), .max("region_id") previously
     raised "zero-size array to reduction operation maximum which has no identity".
     Now detected and handled as an all-False mask with a diagnostic warning.
     This manifests when running aa-evr on output that has already been masked by
     aa-evl (the surviving time window may no longer overlap all EVR polygons).

  2. Coordinate label mismatch: _build_union_region_mask assigned metre-based depth
     coordinate labels to the mask (needed for geometry), but _apply_mask then tried to
     reindex those metre labels against the original range_sample integer indices.
     No labels matched -> full NaN fill -> bool(NaN)==True -> nothing became NaN ->
     echogram looked completely untouched with zero errors raised.
     Fixed by (a) rebuilding the returned mask with original dataset coordinate labels,
     and (b) using positional (coordinate-stripped) masking in _apply_mask.

  3. Robust region_mask return parsing: handles variable name variations (mask_3d,
     mask) and dimension name variations (region_id, region) across echoregions versions.
"""

import argparse
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
  # Apply existing EVR file(s):
  aa-sv input.raw | aa-depth | aa-evr --evr regions/*.evr | aa-plot --all
  aa-sv input.raw | aa-depth | aa-evl --evl bottom.evl | aa-evr --evr school.evr | aa-plot --all
  aa-evr input.nc --evr a.evr b.evr

  # Draw new regions interactively (--evr omitted):
  cat input.nc | aa-evr --name my_school.evr
  cat input.nc | aa-evr --name my_school.evr | aa-plot --all

MODES
  EVR mode (--evr provided)   Apply existing .evr file(s) to echogram.
  Draw mode (--evr omitted)   Open an interactive Bokeh browser app to draw
                              freehand ROI regions directly on the echogram.
                              Saves both a new .evr and a masked .nc.

REQUIRED (EVR mode only)
  --evr EVR [EVR ...]     One or more .evr paths.

INPUT
  INPUT_PATH [INPUT_PATH ...]
    Optional positional .nc paths. If omitted, reads newline-delimited .nc paths from stdin.

OUTPUT
  -o, --output-path PATH  Only valid when processing exactly 1 input.
  --out-dir DIR           Output directory for pipelines / multiple inputs.
  --suffix TEXT           Output suffix appended to input stem (default: _evr).
  --overwrite             Overwrite output files.

DRAWING MODE OPTIONS
  --name TEXT             EVR output filename (default: <input_stem>_regions.evr).
                          May include or omit the .evr extension.
  --port INT              Port for the Bokeh drawing server (default: 5006;
                          auto-increments if occupied).

MASKING (both modes)
  --var NAME              Variable to mask (default: Sv).
  --time-dim NAME         Time dimension name (default: infer ping_time else time).
  --depth-dim NAME        Depth dimension name (default: infer depth, range_sample,
                          or range_bin).
  --channel-index INT     Channel used to build mask when var has 'channel' dim
                          (default: 0).
  --write-mask            Write union mask as int8 variable 'region_mask' in output.
  --fail-empty            Exit non-zero if union mask is empty (0 cells inside).
  --debug                 Verbose diagnostics to stderr.

EXAMPLES
  # Mask to EVR regions only:
  aa-evr input.nc --evr school.evr --overwrite

  # Pipeline: EVL bottom line first, then EVR school regions:
  echo D20090916-T132105.raw | aa-nc --sonar_model EK60 | aa-sv | aa-depth \\
    | aa-evl --evl seafloor.evl --depth-offset -5.0 \\
    | aa-evr --evr d20090916_t124739-t132105.evr --overwrite \\
    | aa-plot --all

  # Draw new regions interactively, then plot:
  cat D20191001-T003423_Sv_depth.nc | aa-evr --name school_regions.evr | aa-plot --all

NOTE
  If the EVR polygon time range does not fully overlap the echogram ping_time range
  (e.g. because an upstream aa-evl has already masked the data), echoregions may
  return zero matching regions for some files. This is treated as an all-False mask
  for that file (nothing is kept from it) rather than a crash, and a warning is
  emitted. Run with --debug to compare time ranges.
"""
    )


def _configure_logging(debug: bool) -> None:
    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if debug else "INFO")


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
            logger.error(
                f"Unsupported input extension: {p.name} "
                f"(allowed: {', '.join(sorted(allowed_ext))})"
            )
            sys.exit(1)


# ---------------------------
# Dataset helpers
# ---------------------------

def _infer_dims(
    ds: xr.Dataset,
    var: str,
    time_dim: Optional[str],
    depth_dim: Optional[str],
) -> Tuple[str, str]:
    if var not in ds.data_vars:
        raise ValueError(
            f"Variable '{var}' not found. Available: {list(ds.data_vars.keys())}"
        )
    da = ds[var]

    if time_dim:
        tdim = time_dim
    else:
        tdim = (
            "ping_time" if "ping_time" in da.dims
            else ("time" if "time" in da.dims else None)
        )
        if tdim is None:
            raise ValueError(
                f"Could not infer time dim for '{var}'. Provide --time-dim."
            )

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
            raise ValueError(
                f"Could not infer depth dim for '{var}'. Provide --depth-dim."
            )

    if tdim not in da.dims:
        raise ValueError(f"time dim '{tdim}' not in {var}.dims={da.dims}")
    if ddim not in da.dims:
        raise ValueError(f"depth dim '{ddim}' not in {var}.dims={da.dims}")

    return tdim, ddim


# ---------------------------
# Mask building + application
# ---------------------------

def _extract_region_mask_union(
    region_mask_result,
    ping_time_size: int,
    depth_size: int,
    debug: bool,
) -> xr.DataArray:
    """
    Robustly extract a 2-D boolean union mask (ping_time x depth) from the
    return value of Regions2D.region_mask(), regardless of echoregions version.

    Handles:
      - Tuple return  -> (dataset_or_da, region_ids)
      - Dataset       -> look for 'mask_3d', then any variable
      - DataArray     -> use directly
      - ZERO regions  -> region_id dimension of size 0; returns all-False mask
                         instead of crashing on numpy max() of empty array.
        This happens when an EVR's polygon time range does not overlap the
        echogram after an upstream step (e.g. aa-evl) has trimmed the data.
    """
    # --- unwrap tuple if needed ---
    if isinstance(region_mask_result, tuple):
        raw = region_mask_result[0]
    else:
        raw = region_mask_result

    # --- extract DataArray from Dataset ---
    if isinstance(raw, xr.Dataset):
        for name in ("mask_3d", "mask", "Mask"):
            if name in raw:
                da = raw[name]
                break
        else:
            varnames = list(raw.data_vars)
            if not varnames:
                raise RuntimeError("region_mask returned an empty Dataset with no variables")
            da = raw[varnames[0]]
            if debug:
                logger.debug(
                    f"region_mask Dataset did not contain 'mask_3d'; "
                    f"using variable '{varnames[0]}' instead"
                )
    elif isinstance(raw, xr.DataArray):
        da = raw
    else:
        raise RuntimeError(
            f"region_mask returned unexpected type {type(raw)}; "
            "expected xr.Dataset or xr.DataArray"
        )

    # --- collapse region dimension ---
    for rdim in ("region_id", "region", "regions"):
        if rdim in da.dims:
            if da.sizes[rdim] == 0:
                # FIX: zero-region crash.
                # echoregions returned no matching polygons for this time window.
                # This is valid — it means no EVR polygons overlap the echogram,
                # commonly because aa-evl upstream has already trimmed the time range.
                # Return all-False (nothing inside any region) instead of crashing.
                logger.warning(
                    f"EVR returned 0 matching regions for this echogram "
                    f"(region_id dimension is empty). This typically means the EVR "
                    f"polygon time range does not overlap the current echogram ping_time "
                    f"range — which can happen when aa-evl has already masked the data. "
                    f"Treating as all-False (nothing inside any region). "
                    f"Run with --debug to compare time ranges."
                )
                other_dims = [d for d in da.dims if d != rdim]
                return xr.DataArray(
                    np.zeros([da.sizes[d] for d in other_dims], dtype=bool),
                    dims=other_dims,
                )
            else:
                da = (da.max(rdim) > 0)
            break

    return da.astype(bool)


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
    True  = inside a region  (values are KEPT).
    False = outside all regions (values become NaN).

    Two modes (auto-detected per EVR file):
      1) Normal echogram EVR (depth+time polygons): echoregions Regions2D.region_mask()
      2) GPS/alongtrack EVR (no usable depth): TIME-ONLY fallback — keep all depths
         for pings whose ping_time falls within any region time span.

    KEY: The returned mask uses the original dataset coordinate labels (not the
    metre-based labels used internally for geometry), so _apply_mask cannot
    produce a silent coordinate mismatch.
    """

    # --- channel stripping ---
    var_da = ds[var]
    if "channel" in var_da.dims:
        if channel_index < 0 or channel_index >= var_da.sizes["channel"]:
            raise ValueError(
                f"--channel-index {channel_index} out of range "
                f"(size={var_da.sizes['channel']})"
            )
        da = var_da.isel(channel=channel_index)
        if "channel" in da.coords:
            try:
                da = da.drop_vars("channel")
            except Exception:
                pass
    else:
        da = var_da

    # --- original coordinate labels (used to rebuild the mask at the end) ---
    orig_time_vals = np.asarray(
        ds[time_dim].values if time_dim in ds.coords else da.coords[time_dim].values
    )
    orig_depth_vals = (
        np.asarray(da.coords[depth_dim].values)
        if depth_dim in da.coords
        else np.arange(da.sizes[depth_dim])
    )

    # --- build surface for echoregions: rename dims to ("ping_time", "depth") ---
    da_for_mask = (
        da.rename({time_dim: "ping_time", depth_dim: "depth"})
        .transpose("ping_time", "depth", ...)
    )

    # Force ping_time to a clean 1-D coordinate vector
    da_for_mask = da_for_mask.assign_coords(ping_time=("ping_time", orig_time_vals))

    # --- resolve metre-valued depth coordinate for geometry ---
    depth_vals_m = None

    def _pick_channel(x):
        if x is None:
            return None
        if "channel" in x.dims and "channel" in var_da.dims:
            try:
                x = x.isel(channel=channel_index)
                if "channel" in x.coords:
                    try:
                        x = x.drop_vars("channel")
                    except Exception:
                        pass
            except Exception:
                return None
        return x

    for cname in ("echo_range", "depth"):
        x = _pick_channel(ds[cname]) if cname in ds else None
        if x is None and cname in var_da.coords:
            x = _pick_channel(var_da.coords[cname])
        if x is None:
            continue
        if depth_dim in x.dims and time_dim not in x.dims:
            depth_vals_m = np.asarray(x.values)
            break
        if time_dim in x.dims and depth_dim in x.dims:
            depth_vals_m = np.asarray(x.isel({time_dim: 0}).values)
            break

    if depth_vals_m is not None:
        da_for_mask = da_for_mask.assign_coords(depth=("depth", depth_vals_m))
        ech_depth_min = float(np.nanmin(depth_vals_m))
        ech_depth_max = float(np.nanmax(depth_vals_m))
    else:
        dtmp = np.asarray(da_for_mask["depth"].values)
        ech_depth_min = float(np.nanmin(dtmp))
        ech_depth_max = float(np.nanmax(dtmp))

    if debug:
        logger.debug(f"Echogram depth range: {ech_depth_min:.2f} -> {ech_depth_max:.2f} m")
        logger.debug(f"Echogram time range:  {orig_time_vals.min()} -> {orig_time_vals.max()}")
        logger.debug(f"da_for_mask shape: {da_for_mask.shape}, dims: {da_for_mask.dims}")

    # dask safety
    try:
        da_for_mask = da_for_mask.compute()
    except Exception:
        pass

    union_mask = None

    for evr_path in evr_files:
        # Use huge depth bounds to avoid old-API depth-filtering of sentinel values
        regions2d = er.read_evr(str(evr_path), min_depth=-1.0e9, max_depth=1.0e9)

        # --- get underlying dataframe ---
        df = None
        if hasattr(regions2d, "to_dataframe"):
            try:
                df = regions2d.to_dataframe()
            except Exception:
                pass
        if df is None and hasattr(regions2d, "data"):
            try:
                df = regions2d.data
            except Exception:
                pass
        if df is None:
            raise RuntimeError(
                f"Could not access Regions2D dataframe for {evr_path}"
            )

        df = df.copy()

        if debug:
            logger.debug(
                f"{evr_path.name}: {len(df)} regions, columns={list(df.columns)}"
            )

        # --- normalise TIME lists to datetime64 ---
        if "time" in df.columns:
            new_time = []
            for t_list in df["time"]:
                try:
                    t = pd.to_datetime(list(t_list), errors="coerce").dropna()
                    new_time.append([np.datetime64(x) for x in t])
                except Exception:
                    new_time.append([])
            df["time"] = new_time

        # --- normalise DEPTH lists: replace sentinels, clip to echogram range ---
        if "depth" in df.columns:
            new_depth = []
            for d_list in df["depth"]:
                fixed = []
                for d in list(d_list):
                    try:
                        x = float(d)
                    except Exception:
                        fixed.append(np.nan)
                        continue
                    if np.isfinite(x):
                        if x <= -9000:
                            x = ech_depth_min
                        elif x >= 9000:
                            x = ech_depth_max
                        x = min(max(x, ech_depth_min), ech_depth_max)
                    fixed.append(x)
                new_depth.append(fixed)
            df["depth"] = new_depth

        # --- close polygons ---
        if "time" in df.columns and "depth" in df.columns:
            closed_t, closed_d = [], []
            for t_list, d_list in zip(df["time"], df["depth"]):
                try:
                    if len(t_list) and len(d_list) and (
                        t_list[0] != t_list[-1] or d_list[0] != d_list[-1]
                    ):
                        t_list = list(t_list) + [t_list[0]]
                        d_list = list(d_list) + [d_list[0]]
                except Exception:
                    pass
                closed_t.append(t_list)
                closed_d.append(d_list)
            df["time"] = closed_t
            df["depth"] = closed_d

        try:
            regions2d.data = df
        except Exception:
            pass

        # --- detect whether EVR has any usable depth points ---
        has_depth = False
        if "depth" in df.columns:
            for d_list in df["depth"]:
                if any(
                    isinstance(x, (int, float, np.floating)) and np.isfinite(x)
                    for x in d_list
                ):
                    has_depth = True
                    break

        # -------------------------------------------------------
        # GPS / alongtrack fallback: TIME-ONLY mask
        # -------------------------------------------------------
        if not has_depth and "time" in df.columns:
            mask_time = np.zeros(orig_time_vals.shape, dtype=bool)
            for t_list in df["time"]:
                if not t_list:
                    continue
                try:
                    tmin = np.min(t_list)
                    tmax = np.max(t_list)
                    mask_time |= (orig_time_vals >= tmin) & (orig_time_vals <= tmax)
                except Exception:
                    continue

            file_union = xr.DataArray(
                np.broadcast_to(
                    mask_time[:, np.newaxis],
                    (len(orig_time_vals), da.sizes[depth_dim]),
                ).copy(),
                dims=("ping_time", "depth"),
            )
            if debug:
                logger.debug(
                    f"{evr_path.name}: time-only mask — "
                    f"inside pings={int(mask_time.sum())}/{mask_time.size}"
                )

        # -------------------------------------------------------
        # Normal echogram EVR: depth-time polygon mask
        # -------------------------------------------------------
        else:
            # Pad region endpoint times slightly to avoid boundary misses
            pad = pd.Timedelta(seconds=1)
            new_times = []
            for t_list in df["time"]:
                t = pd.to_datetime(list(t_list), errors="coerce").dropna()
                if len(t) == 0:
                    new_times.append(list(t_list))
                    continue
                tmin, tmax = t.min(), t.max()
                out = []
                for ti in t:
                    if ti == tmin:
                        out.append(np.datetime64(ti - pad))
                    elif ti == tmax:
                        out.append(np.datetime64(ti + pad))
                    else:
                        out.append(np.datetime64(ti))
                new_times.append(out)
            df["time"] = new_times
            try:
                regions2d.data = df
            except Exception:
                pass

            if debug:
                all_evr_times = []
                for t_list in df["time"]:
                    all_evr_times.extend(t_list)
                if all_evr_times:
                    logger.debug(
                        f"{evr_path.name}: EVR time range  "
                        f"{np.min(all_evr_times)} -> {np.max(all_evr_times)}"
                    )

            region_mask_result = regions2d.region_mask(da_for_mask, collapse_to_2d=False)

            file_union = _extract_region_mask_union(
                region_mask_result,
                ping_time_size=len(orig_time_vals),
                depth_size=da.sizes[depth_dim],
                debug=debug,
            )

            if set(file_union.dims) >= {"ping_time", "depth"}:
                file_union = file_union.transpose("ping_time", "depth")

            if debug:
                n_in = int(file_union.values.sum())
                logger.debug(
                    f"{evr_path.name}: polygon mask — "
                    f"inside={n_in}/{file_union.size} cells "
                    f"({100 * n_in / max(file_union.size, 1):.1f}%)"
                )

        union_mask = file_union if union_mask is None else (union_mask | file_union)

    if union_mask is None:
        raise RuntimeError("No region mask produced.")

    # --- rebuild mask with original dataset coordinate labels ---
    # Prevents the silent coordinate-mismatch bug where metre-based depth labels
    # on the mask fail to reindex against range_sample integer indices, producing
    # a full-NaN mask that bool-evaluates to all-True and keeps everything.
    mask_values = np.asarray(union_mask.values, dtype=bool)
    if mask_values.shape != (len(orig_time_vals), da.sizes[depth_dim]):
        mask_values = mask_values.reshape(len(orig_time_vals), da.sizes[depth_dim])

    return xr.DataArray(
        mask_values,
        dims=(time_dim, depth_dim),
        coords={
            time_dim: orig_time_vals,
            depth_dim: orig_depth_vals,
        },
    )


def _apply_mask(
    ds: xr.Dataset,
    mask: xr.DataArray,
    time_dim: str,
    depth_dim: str,
    write_mask: bool,
) -> xr.Dataset:
    """
    Apply boolean mask to all data variables containing (time_dim, depth_dim).

    Uses positional (coordinate-stripped) masking as the sole strategy.
    By construction the mask has the same shape as the data along (time_dim,
    depth_dim), so stripping coordinate labels before broadcasting guarantees
    no reindex mismatch can silently fill the mask with NaN — which in numpy
    evaluates as True and causes xr.where to keep all original values unchanged.
    """
    ds_out = ds.copy(deep=False)

    mask_np = np.asarray(mask.transpose(time_dim, depth_dim).values, dtype=bool)

    for name, da in list(ds_out.data_vars.items()):
        if (time_dim in da.dims) and (depth_dim in da.dims):
            m_bare = xr.DataArray(mask_np, dims=(time_dim, depth_dim))
            m_broadcast = m_bare.broadcast_like(da)
            ds_out[name] = xr.where(m_broadcast, da, np.nan)

    if write_mask:
        ds_out["region_mask"] = mask.astype("int8")
        ds_out["region_mask"].attrs["long_name"] = (
            "Union region mask from EVR files (1=inside, 0=outside)"
        )

    return ds_out


# ---------------------------
# Output path resolution
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
# Per-file processing
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
    debug: bool,
) -> Optional[Path]:
    if output_path.exists() and not overwrite:
        logger.error(f"Output exists (use --overwrite): {output_path}")
        return None

    ds = xr.open_dataset(input_path)

    tdim, ddim = _infer_dims(ds, var=var, time_dim=time_dim, depth_dim=depth_dim)
    if debug:
        logger.debug(
            f"Using dims: time_dim='{tdim}', depth_dim='{ddim}', "
            f"{var}.dims={ds[var].dims}, shape={ds[var].shape}"
        )

    mask = _build_union_region_mask(
        ds=ds,
        evr_files=evr_files,
        var=var,
        time_dim=tdim,
        depth_dim=ddim,
        channel_index=channel_index,
        debug=debug,
    )

    inside = int(mask.values.sum())
    total = int(mask.size)
    pct = 100.0 * inside / max(total, 1)
    logger.info(
        f"Union mask coverage: {inside}/{total} cells inside regions ({pct:.1f}%)"
    )

    if inside == 0:
        logger.error(
            "Union mask is EMPTY (0 cells inside). Output will be all-NaN.\n"
            "Possible causes:\n"
            "  - EVR time range does not overlap the NetCDF ping_time range\n"
            "  - EVR depth polygons are outside the echogram depth range\n"
            "  - The EVR is a GPS/track file, not an echogram region export\n"
            "Run with --debug to see time/depth ranges for diagnosis."
        )
        if fail_empty:
            try:
                ds.close()
            except Exception:
                pass
            return None

    if inside == total:
        logger.warning(
            "Union mask is FULL (all cells inside). Output will be identical to "
            "input — no data will be masked out. Check EVR polygon boundaries."
        )

    ds_out = _apply_mask(
        ds, mask=mask, time_dim=tdim, depth_dim=ddim, write_mask=write_mask
    )
    ds_out.attrs["aa_tool"] = "aa-evr"
    ds_out.attrs["aa_evr_files"] = ",".join(str(p) for p in evr_files)

    ds_out.to_netcdf(output_path)

    try:
        ds.close()
    except Exception:
        pass

    return output_path


# ---------------------------
# Entry point
# ---------------------------

def main() -> int:
    if len(sys.argv) == 1 and sys.stdin.isatty():
        print_help()
        return 0

    parser = argparse.ArgumentParser(
        description=(
            "Mask echogram NetCDF (.nc) using Echoview EVR region files, "
            "or draw new regions interactively (omit --evr)."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "input_paths", nargs="*", type=Path,
        help="Input .nc/.netcdf4 paths (or read from stdin).",
    )

    # ── EVR mode ──
    parser.add_argument(
        "--evr", required=False, default=None, nargs="+", type=Path, metavar="EVR",
        help="One or more .evr paths (omit to enter interactive drawing mode).",
    )

    # ── Drawing mode ──
    parser.add_argument(
        "--name", type=str, default=None, dest="name",
        help=(
            "Drawing mode: EVR output filename "
            "(default: <input_stem>_regions.evr).  "
            "May include or omit the .evr extension."
        ),
    )
    parser.add_argument(
        "--port", type=int, default=5006,
        help="Drawing mode: Bokeh server port (default: 5006; auto-increments if busy).",
    )

    # ── Shared output options ──
    parser.add_argument(
        "-o", "--output-path", dest="output_path", type=Path,
        help="Output path (only valid for a single input file, EVR mode only).",
    )
    parser.add_argument(
        "--out-dir", type=Path,
        help="Output directory (for pipelines / multiple inputs).",
    )
    parser.add_argument(
        "--suffix", type=str, default="_evr",
        help="Suffix appended to output stem (default: _evr, EVR mode only).",
    )
    parser.add_argument(
        "--overwrite", action="store_true",
        help="Overwrite existing output files.",
    )

    # ── Masking options (shared) ──
    parser.add_argument(
        "--var", type=str, default="Sv",
        help="Variable to mask (default: Sv).",
    )
    parser.add_argument(
        "--time-dim", type=str, default=None,
        help="Time dimension name (default: auto-detect).",
    )
    parser.add_argument(
        "--depth-dim", type=str, default=None,
        help="Depth dimension name (default: auto-detect).",
    )
    parser.add_argument(
        "--channel-index", type=int, default=0,
        help="Channel index for mask building (default: 0).",
    )
    parser.add_argument(
        "--write-mask", action="store_true",
        help="Write 'region_mask' variable to output NetCDF (EVR mode only).",
    )
    parser.add_argument(
        "--fail-empty", action="store_true",
        help="Exit non-zero if union mask is empty (EVR mode only).",
    )
    parser.add_argument(
        "--debug", action="store_true",
        help="Verbose diagnostics to stderr.",
    )
    args = parser.parse_args()

    _configure_logging(args.debug)

    # ══════════════════════════════════════════════════════════════
    # Route: DRAWING MODE  (--evr not provided)
    # ══════════════════════════════════════════════════════════════
    if not args.evr:
        try:
            from aalibrary.utils.region_draw import run_drawing_mode
        except ImportError as exc:
            logger.error(
                f"Could not import region_draw (drawing mode): {exc}\n"
                "Ensure region_draw.py is in the same directory as aa_evr.py, "
                "or on PYTHONPATH."
            )
            return 2

        input_paths = list(_iter_input_paths(args.input_paths))
        if not input_paths:
            logger.error(
                "Drawing mode requires one input NC path "
                "(positional argument or piped via stdin)."
            )
            return 1
        if len(input_paths) > 1:
            logger.warning(
                f"Drawing mode processes one file at a time; "
                f"using first input: {input_paths[0]}"
            )

        nc_path = input_paths[0].expanduser().resolve()
        if not nc_path.exists():
            logger.error(f"Input file not found: {nc_path}")
            return 1

        evr_name = args.name or (nc_path.with_suffix("").name + "_regions.evr")
        out_dir = args.out_dir.expanduser().resolve() if args.out_dir else None

        if args.debug:
            logger.debug(
                f"\naa-evr drawing mode args:\n"
                f"  nc_path={nc_path}\n"
                f"  evr_name={evr_name}\n"
                f"  out_dir={out_dir}\n"
                f"  var={args.var}, port={args.port}"
            )

        result = run_drawing_mode(
            nc_path=nc_path,
            evr_name=evr_name,
            out_dir=out_dir,
            var=args.var,
            time_dim=args.time_dim,
            depth_dim=args.depth_dim,
            channel_index=args.channel_index,
            overwrite=args.overwrite,
            port=args.port,
            debug=args.debug,
        )

        if result:
            logger.success("Piping saved .nc path to stdout ⟶")
            print(str(result))
            return 0
        else:
            return 1

    # ══════════════════════════════════════════════════════════════
    # Route: EVR MODE  (--evr provided)
    # ══════════════════════════════════════════════════════════════
    input_paths = list(_iter_input_paths(args.input_paths))
    if not input_paths:
        logger.error("No input paths provided (positional or via stdin).")
        return 1

    input_paths = [p.expanduser().resolve() for p in input_paths]
    evr_files = [p.expanduser().resolve() for p in args.evr]
    _validate_inputs(input_paths, evr_files)

    if args.output_path is not None and len(input_paths) != 1:
        logger.error(
            "--output-path is only valid when processing exactly 1 input. "
            "Use --out-dir for multiple files."
        )
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
            output_path=(
                args.output_path.expanduser().resolve() if args.output_path else None
            ),
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