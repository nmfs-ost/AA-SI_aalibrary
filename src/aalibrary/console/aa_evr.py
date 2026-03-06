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
- --evr is provided once and accepts one or more .evr paths after it (argparse nargs="+").
- Loads all EVRs and unions all regions across them.
- Applies union mask to all variables containing (time_dim, depth_dim): outside -> NaN.

BUG FIX NOTES (2025):
- Root cause of "untouched echogram" bug was a silent coordinate mismatch.
  _build_union_region_mask assigned meter-based depth coordinates to the mask
  (needed for echoregions geometry), but _apply_mask then tried to reindex
  those meter-valued labels against the original range_sample integer indices.
  No labels matched → full NaN fill → xr.where(NaN, da, NaN) → bool(NaN)==True
  → every cell kept → echogram looked untouched with no errors raised.

  Fix: _apply_mask now uses purely positional masking (coordinate-stripped),
  which is safe by construction because the mask has the same shape as the data.
  _build_union_region_mask now also re-attaches the original dataset coordinates
  to the returned mask as a belt-and-suspenders fix.

Compatibility note:
- Older echoregions.read_evr only supports (input_file, min_depth, max_depth).
  We call read_evr with a VERY WIDE depth window to avoid depth-filtering.
- region_mask return-value parsing is version-resilient: tries "mask_3d" first,
  then any variable in the returned Dataset, and collapses any region dimension.
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
  aa-sv input.raw | aa-depth | aa-evr --evr regions/*.evr --depth-dim range_sample | aa-plot --all
  aa-evr input.nc --evr a.evr b.evr

REQUIRED
  --evr EVR [EVR ...]     One or more .evr paths after a single --evr.

INPUT
  INPUT_PATH [INPUT_PATH ...]
    Optional positional .nc paths. If omitted, reads newline-delimited .nc paths from stdin.

OUTPUT
  -o, --output_path PATH  Only valid when processing exactly 1 input
  --out-dir DIR           Output directory for pipelines/multiple inputs
  --suffix TEXT           Output suffix appended to input stem (default: _evr)
  --overwrite             Overwrite output files

MASKING
  --var NAME              Variable to mask (default: Sv)
  --time-dim NAME         Default: infer ping_time else time
  --depth-dim NAME        Default: infer depth else range_sample else range_bin
  --channel-index INT     Channel used to build mask when var has 'channel' dim (default: 0)
  --write-mask            Write union mask as int8 variable 'region_mask'
  --fail-empty            Exit non-zero if union mask is empty (prevents silent blank plots)
  --debug                 Verbose diagnostics to stderr

NOTE
  EVR region files contain polygon boundaries defined by depth and date/time coordinates.
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
            logger.error(
                f"Unsupported input extension: {p.name} (allowed: {', '.join(sorted(allowed_ext))})"
            )
            sys.exit(1)


# ---------------------------
# Dataset helpers
# ---------------------------

def _infer_dims(
    ds: xr.Dataset, var: str, time_dim: Optional[str], depth_dim: Optional[str]
) -> Tuple[str, str]:
    if var not in ds.data_vars:
        raise ValueError(f"Variable '{var}' not found. Available: {list(ds.data_vars.keys())}")

    da = ds[var]

    if time_dim:
        tdim = time_dim
    else:
        tdim = (
            "ping_time" if "ping_time" in da.dims
            else ("time" if "time" in da.dims else None)
        )
        if tdim is None:
            raise ValueError(f"Could not infer time dim for '{var}'. Provide --time-dim.")

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


# ---------------------------
# Mask building + application
# ---------------------------

def _extract_region_mask_union(region_mask_result, debug: bool) -> xr.DataArray:
    """
    Robustly extract a 2-D boolean union mask (ping_time x depth) from the
    return value of Regions2D.region_mask(), regardless of echoregions version.

    The function tries, in order:
      1. Tuple return  → (dataset_or_da, region_ids)
      2. Dataset       → look for 'mask_3d', then any variable
      3. DataArray     → use directly
    Then collapses any region/region_id dimension via max().
    """
    # --- unwrap tuple if needed ---
    if isinstance(region_mask_result, tuple):
        raw = region_mask_result[0]
    else:
        raw = region_mask_result

    # --- extract DataArray from Dataset ---
    if isinstance(raw, xr.Dataset):
        # prefer mask_3d; fall back to first variable
        for name in ("mask_3d", "mask", "Mask"):
            if name in raw:
                da = raw[name]
                break
        else:
            varnames = list(raw.data_vars)
            if not varnames:
                raise RuntimeError("region_mask returned an empty Dataset")
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
            da = da.max(rdim)
            break

    return (da > 0).astype(bool)


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
    True  = inside a region  (these values are KEPT).
    False = outside all regions (these values become NaN).

    Two modes (auto-detected):
      1) Normal echogram EVR (depth+time polygons): echoregions Regions2D.region_mask()
      2) GPS/alongtrack EVR (no usable depth polygons): TIME-ONLY fallback
         → keep ALL depths for pings whose ping_time falls within any region time span.

    KEY FIX: The returned mask uses the **original dataset coordinate labels**
    (not the metre-based labels used internally for geometry), so _apply_mask
    can align it without any reindex mismatch.
    """

    # --- select var and drop channel if needed ---
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

    # --- capture original coordinate labels (used to rebuild mask at the end) ---
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

    # --- resolve a 1-D metre-valued depth coordinate for the geometry step ---
    depth_vals_m = None  # metres

    def _pick_channel(x):
        """Strip channel dimension if present."""
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
        # want 1-D over depth_dim
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
        # fallback: whatever "depth" coord exists (may be integer indices)
        dtmp = np.asarray(da_for_mask["depth"].values)
        ech_depth_min = float(np.nanmin(dtmp))
        ech_depth_max = float(np.nanmax(dtmp))

    if debug:
        logger.debug(f"Mask depth range (m): {ech_depth_min:.2f} → {ech_depth_max:.2f}")
        logger.debug(f"Mask time range: {orig_time_vals.min()} → {orig_time_vals.max()}")
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

        # --- get dataframe ---
        df = None
        for attr in ("to_dataframe", None):
            if attr and hasattr(regions2d, "to_dataframe"):
                try:
                    df = regions2d.to_dataframe()
                    break
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

        # --- detect whether EVR has usable depth ---
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
        if (not has_depth) and ("time" in df.columns):
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

            # Broadcast time-only mask to full (ping_time, depth) shape
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
        # Normal echogram EVR: depth–time polygon mask
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

            region_mask_result = regions2d.region_mask(da_for_mask, collapse_to_2d=False)
            file_union = _extract_region_mask_union(region_mask_result, debug=debug)

            # Ensure (ping_time, depth) order
            if set(file_union.dims) >= {"ping_time", "depth"}:
                file_union = file_union.transpose("ping_time", "depth")

            if debug:
                n_in = int(file_union.values.sum())
                logger.debug(
                    f"{evr_path.name}: polygon mask — "
                    f"inside={n_in}/{file_union.size} cells "
                    f"({100*n_in/max(file_union.size,1):.1f}%)"
                )

        union_mask = file_union if union_mask is None else (union_mask | file_union)

    if union_mask is None:
        raise RuntimeError("No region mask produced.")

    # ---------------------------------------------------------------
    # KEY FIX: Rebuild the mask using the *original* dataset coordinate
    # labels (not the metre-based labels from da_for_mask).
    #
    # Why this matters:
    #   da_for_mask used metre values as the "depth" coordinate so that
    #   echoregions can do correct polygon geometry.  But the returned
    #   mask therefore carries those metre labels.  When _apply_mask
    #   then tries  m.reindex({depth_dim: da[depth_dim]})  it is
    #   reindexing metres → integer range_sample indices → no match →
    #   full NaN fill → bool(NaN)==True → nothing ever becomes NaN →
    #   echogram looks completely untouched.
    #
    #   Solution: discard the metre coordinate labels and replace them
    #   with the original coordinate values from ds[var].  The shape is
    #   identical, so this is a pure label swap, not a data resample.
    # ---------------------------------------------------------------
    mask_values = np.asarray(union_mask.values, dtype=bool)
    # union_mask might have extra leading dims if broadcast; squeeze to 2D
    # (time, depth)
    if mask_values.ndim > 2:
        # Should not happen, but guard anyway
        mask_values = mask_values.reshape(
            len(orig_time_vals), da.sizes[depth_dim]
        )

    union_mask_out = xr.DataArray(
        mask_values,
        dims=(time_dim, depth_dim),
        coords={
            time_dim: orig_time_vals,
            depth_dim: orig_depth_vals,
        },
    )
    return union_mask_out


def _apply_mask(
    ds: xr.Dataset,
    mask: xr.DataArray,
    time_dim: str,
    depth_dim: str,
    write_mask: bool,
) -> xr.Dataset:
    """
    Apply boolean mask to all data variables containing (time_dim, depth_dim).

    KEY FIX: Use purely *positional* masking (coordinate-stripped) as the
    primary strategy.

    Why positional, not label-based?
      By construction the mask has exactly the same shape as the data along
      (time_dim, depth_dim).  Stripping coordinate labels before broadcasting
      guarantees no reindex mismatch can silently fill the mask with NaN —
      which in numpy evaluates as True (non-zero float), causing xr.where to
      keep original values everywhere and produce an untouched echogram.
    """
    ds_out = ds.copy(deep=False)

    # Pre-compute positional numpy array: shape (n_time, n_depth)
    mask_np = np.asarray(mask.transpose(time_dim, depth_dim).values, dtype=bool)

    for name, da in list(ds_out.data_vars.items()):
        if (time_dim in da.dims) and (depth_dim in da.dims):
            # Build a coordinate-free DataArray of the correct shape,
            # then broadcast to da's full shape (handles channel, etc.)
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
# Output paths + processing
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
            f"{var}.dims={ds[var].dims}, {var}.shape={ds[var].shape}"
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
            "Union mask is EMPTY (0 inside cells). Output will be all-NaN.\n"
            "Possible causes:\n"
            "  • EVR time range does not overlap the NetCDF file's ping_time range\n"
            "  • EVR depth polygons are outside the echogram's depth range\n"
            "  • The EVR is a GPS/track file, not an echogram region export\n"
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
            "Union mask is FULL (all cells inside). The output will be identical "
            "to the input — no data will be masked out.\n"
            "Check that the EVR polygons are correctly bounded."
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


def main() -> int:
    if len(sys.argv) == 1 and sys.stdin.isatty():
        print_help()
        return 0

    parser = argparse.ArgumentParser(
        description="Mask echogram NetCDF (.nc) using Echoview EVR region files."
    )
    parser.add_argument(
        "input_paths", nargs="*", type=Path,
        help="Input .nc/.netcdf4 paths (or stdin)."
    )
    parser.add_argument(
        "--evr", required=True, nargs="+", type=Path,
        help="One or more .evr paths after a single --evr."
    )
    parser.add_argument(
        "-o", "--output_path", type=Path,
        help="Output path (ONLY valid when processing exactly 1 input)."
    )
    parser.add_argument(
        "--out-dir", type=Path,
        help="Output directory for pipelines/multiple inputs."
    )
    parser.add_argument(
        "--suffix", type=str, default="_evr",
        help="Suffix appended to output stem (default: _evr)."
    )
    parser.add_argument(
        "--overwrite", action="store_true",
        help="Overwrite output files if they exist."
    )
    parser.add_argument(
        "--var", type=str, default="Sv",
        help="Variable to mask (default: Sv)."
    )
    parser.add_argument(
        "--time-dim", type=str, default=None,
        help="Time dimension name."
    )
    parser.add_argument(
        "--depth-dim", type=str, default=None,
        help="Depth dimension name (e.g., range_sample)."
    )
    parser.add_argument(
        "--channel-index", type=int, default=0,
        help="Channel index for mask building."
    )
    parser.add_argument(
        "--write-mask", action="store_true",
        help="Write 'region_mask' to output."
    )
    parser.add_argument(
        "--fail-empty", action="store_true",
        help="Fail if union mask is empty."
    )
    parser.add_argument(
        "--debug", action="store_true",
        help="Verbose logging to stderr."
    )
    args = parser.parse_args()

    _configure_logging(args.debug)

    input_paths = list(_iter_input_paths(args.input_paths))
    if not input_paths:
        logger.error("No input paths provided (positional or via stdin).")
        return 1

    input_paths = [p.expanduser().resolve() for p in input_paths]
    evr_files = [p.expanduser().resolve() for p in args.evr]
    _validate_inputs(input_paths, evr_files)

    if args.output_path is not None and len(input_paths) != 1:
        logger.error(
            "--output_path is only valid when processing exactly 1 input. "
            "Use --out-dir instead."
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