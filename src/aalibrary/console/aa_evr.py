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

Compatibility note:
- Older echoregions.read_evr only supports (input_file, min_depth, max_depth) and may FILTER points
  outside that range (turning them into NaN). Many EVR files use sentinel depths like ±9999.99
  for region edges; newer echoregions can convert these (docs), but older versions cannot. :contentReference[oaicite:2]{index=2}
- This tool therefore reads EVR with a VERY WIDE depth window, then replaces sentinel depths itself
  using the echogram’s min/max depth, and clips to that range.
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
  EVR region files (true echogram EVRs) contain polygon boundaries defined by depth and date/time coordinates. :contentReference[oaicite:3]{index=3}
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

    if time_dim:
        tdim = time_dim
    else:
        tdim = "ping_time" if "ping_time" in da.dims else ("time" if "time" in da.dims else None)
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


def _select_channel(da: xr.DataArray, channel_index: int) -> xr.DataArray:
    if "channel" not in da.dims:
        return da
    if channel_index < 0 or channel_index >= da.sizes["channel"]:
        raise ValueError(f"--channel-index {channel_index} out of range (size={da.sizes['channel']})")
    da2 = da.isel(channel=channel_index)
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
    Return a *plain* 1D numpy depth vector in meters aligned to depth_dim.
    Prefer echo_range; fallback to depth (often created by aa-depth); fallback to coords.
    """
    if "echo_range" in ds:
        erng = _maybe_select_channel_for_aux(ds["echo_range"], var_da, channel_index)
        if depth_dim in erng.dims and time_dim not in erng.dims:
            return np.asarray(erng.values)
        if (time_dim in erng.dims) and (depth_dim in erng.dims):
            return np.asarray(erng.isel({time_dim: 0}).values)

    if "depth" in ds:
        dep = _maybe_select_channel_for_aux(ds["depth"], var_da, channel_index)
        if depth_dim in dep.dims and time_dim not in dep.dims:
            return np.asarray(dep.values)
        if (time_dim in dep.dims) and (depth_dim in dep.dims):
            return np.asarray(dep.isel({time_dim: 0}).values)

    for name in ("echo_range", "depth"):
        if name in var_da.coords:
            c = _maybe_select_channel_for_aux(var_da.coords[name], var_da, channel_index)
            if depth_dim in c.dims and time_dim not in c.dims:
                return np.asarray(c.values)
            if (time_dim in c.dims) and (depth_dim in c.dims):
                return np.asarray(c.isel({time_dim: 0}).values)

    return None


# ---------------------------
# echoregions compatibility + EVR normalization
# ---------------------------

def _read_evr_compat(evr_path: Path, debug: bool):
    """
    Call echoregions.read_evr with only kwargs supported by the installed version.

    Your version shows: read_evr(input_file, min_depth=..., max_depth=...) only.
    We use a VERY WIDE window to avoid old-version filtering of sentinel depths (e.g., 9999.99).
    """
    sig = inspect.signature(er.read_evr)
    params = sig.parameters

    kwargs = {}
    if "min_depth" in params:
        kwargs["min_depth"] = -1.0e9
    if "max_depth" in params:
        kwargs["max_depth"] = 1.0e9

    if debug:
        logger.debug(f"read_evr signature: {sig}")
        logger.debug(f"read_evr kwargs used: {kwargs}")

    return er.read_evr(str(evr_path), **kwargs)


def _regions_dataframe(regions2d):
    """
    Return a pandas DataFrame of regions if possible.
    Tries .to_dataframe(), else .data, else None.
    """
    if hasattr(regions2d, "to_dataframe"):
        try:
            return regions2d.to_dataframe()
        except Exception:
            pass
    if hasattr(regions2d, "data"):
        try:
            df = regions2d.data
            return df if isinstance(df, pd.DataFrame) else None
        except Exception:
            pass
    return None


def _set_regions_dataframe(regions2d, df: pd.DataFrame) -> None:
    if hasattr(regions2d, "data"):
        try:
            regions2d.data = df
        except Exception:
            pass


def _coerce_region_times(df: pd.DataFrame) -> pd.DataFrame:
    if "time" not in df.columns:
        return df
    out = df.copy()
    coerced = []
    for t_list in out["time"]:
        try:
            coerced.append([np.datetime64(pd.to_datetime(t)) for t in t_list])
        except Exception:
            coerced.append(t_list)
    out["time"] = coerced
    return out


def _fix_and_clip_region_depths(
    df: pd.DataFrame,
    ech_depth_min: float,
    ech_depth_max: float,
) -> pd.DataFrame:
    """
    Replace sentinel depths (<=-9000 or >=9000) with echogram min/max,
    convert to floats where possible, and clip to [ech_depth_min, ech_depth_max].
    """
    if "depth" not in df.columns:
        return df

    out = df.copy()

    def fix_list(d_list):
        fixed = []
        for d in d_list:
            try:
                x = float(d)
            except Exception:
                # keep as-is; will become NaN later
                fixed.append(np.nan)
                continue

            if x <= -9000:
                x = float(ech_depth_min)
            elif x >= 9000:
                x = float(ech_depth_max)

            # clip
            if x < ech_depth_min:
                x = ech_depth_min
            elif x > ech_depth_max:
                x = ech_depth_max

            fixed.append(x)
        return fixed

    out["depth"] = [fix_list(d_list) for d_list in out["depth"]]
    return out


def _validate_regions_have_depth(df: pd.DataFrame, evr_path: Path) -> None:
    """
    If all region depth values are NaN, this EVR can't be applied to echogram masking.
    (Often indicates the EVR isn't an echogram depth/time polygon export.)
    """
    if "depth" not in df.columns:
        raise ValueError(
            f"EVR appears not to contain a 'depth' column usable for masking: {evr_path}"
        )

    # flatten and check numeric
    all_depths = []
    for d_list in df["depth"]:
        try:
            all_depths.extend(list(d_list))
        except Exception:
            continue

    if not all_depths:
        raise ValueError(f"EVR contains no depth points: {evr_path}")

    finite = [d for d in all_depths if isinstance(d, (int, float, np.floating)) and np.isfinite(d)]
    if len(finite) == 0:
        raise ValueError(
            "EVR depth values are all missing/NaN after parsing. "
            f"This EVR likely is not an echogram 2D region polygon export usable for Sv masking: {evr_path}\n"
            "Echogram EVR files contain region boundaries as points with depth and date/time coordinates. :contentReference[oaicite:4]{index=4}"
        )


def _maybe_close_polygons(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure each polygon is closed by repeating the first point at the end if needed.
    (Works regardless of echoregions having close_region()).
    """
    if not {"time", "depth"}.issubset(df.columns):
        return df

    out = df.copy()

    new_time = []
    new_depth = []

    for t_list, d_list in zip(out["time"], out["depth"]):
        try:
            if len(t_list) >= 2 and len(d_list) >= 2:
                if (t_list[0] != t_list[-1]) or (d_list[0] != d_list[-1]):
                    t_list = list(t_list) + [t_list[0]]
                    d_list = list(d_list) + [d_list[0]]
        except Exception:
            pass

        new_time.append(t_list)
        new_depth.append(d_list)

    out["time"] = new_time
    out["depth"] = new_depth
    return out


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

    Behavior:
      - Normal case (echogram EVR): use echoregions Regions2D.region_mask to get a depth–time mask.
      - GPS/alongtrack EVR fallback: if the EVR has no usable depth points (all NaN),
        perform TIME-ONLY masking: keep all depths for ping_times that fall within the union
        of region time ranges in the EVR.

    Returns:
      xr.DataArray[bool] with dims (time_dim, depth_dim)
    """
    var_da = ds[var]
    da = _select_channel(var_da, channel_index)

    # Build surface echoregions expects: dims ("ping_time","depth")
    da_for_mask = da.rename({time_dim: "ping_time", depth_dim: "depth"}).transpose("ping_time", "depth", ...)

    # Ensure 1D ping_time coordinate
    pt_vals = np.asarray(ds[time_dim].values if time_dim in ds else da.coords[time_dim].values)
    da_for_mask = da_for_mask.assign_coords(ping_time=("ping_time", pt_vals))

    # Attach meters-valued depth coordinate if available (best for echogram EVRs)
    depth_vals = _get_depth_vals_meters_1d(ds, var_da, time_dim=time_dim, depth_dim=depth_dim, channel_index=channel_index)
    if depth_vals is not None:
        da_for_mask = da_for_mask.assign_coords(depth=("depth", np.asarray(depth_vals)))
        ech_depth_min = float(np.nanmin(depth_vals))
        ech_depth_max = float(np.nanmax(depth_vals))
    else:
        # fallback: whatever depth coordinate exists (may be indices)
        ech_depth_min = float(np.nanmin(da_for_mask["depth"].values))
        ech_depth_max = float(np.nanmax(da_for_mask["depth"].values))

    if debug:
        logger.debug(f"Mask depth range (m): {ech_depth_min} → {ech_depth_max}")
        logger.debug(f"Mask time range: {pt_vals.min()} → {pt_vals.max()}")

    # compute if dask-backed
    try:
        da_for_mask = da_for_mask.compute()
    except Exception:
        pass

    union_mask: Optional[xr.DataArray] = None

    for evr_path in evr_files:
        regions2d = _read_evr_compat(evr_path, debug=debug)

        df = _regions_dataframe(regions2d)
        if df is None:
            raise RuntimeError(f"Could not access Regions2D dataframe for {evr_path}")

        # Normalize EVR content
        df = _coerce_region_times(df)
        df = _fix_and_clip_region_depths(df, ech_depth_min=ech_depth_min, ech_depth_max=ech_depth_max)
        df = _maybe_close_polygons(df)
        _set_regions_dataframe(regions2d, df)

        # Detect whether this EVR provides usable depth (echogram) polygons
        depth_finite = []
        if "depth" in df.columns:
            for d_list in df["depth"]:
                try:
                    depth_finite.extend([d for d in d_list if isinstance(d, (int, float, np.floating)) and np.isfinite(d)])
                except Exception:
                    pass

        has_depth = len(depth_finite) > 0

        if not has_depth and "time" in df.columns:
            # ---------------------------
            # GPS/alongtrack fallback: TIME-ONLY mask
            # ---------------------------
            pt = pd.to_datetime(da_for_mask["ping_time"].values)

            mask_time = np.zeros(pt.shape, dtype=bool)
            for t_list in df["time"]:
                try:
                    t = pd.to_datetime(t_list)
                    if len(t) == 0:
                        continue
                    mask_time |= (pt >= t.min()) & (pt <= t.max())
                except Exception:
                    continue

            file_union = xr.DataArray(
                mask_time,
                dims=("ping_time",),
                coords={"ping_time": da_for_mask["ping_time"]},
            ).broadcast_like(da_for_mask)

            if debug:
                inside = int(mask_time.sum())
                logger.debug(f"{evr_path.name}: time-only mask inside pings={inside}/{mask_time.size}")

        else:
            # ---------------------------
            # Normal: depth–time region mask
            # ---------------------------
            region_mask_ds, _ = regions2d.region_mask(da_for_mask, collapse_to_2d=False)
            if "mask_3d" not in region_mask_ds:
                raise RuntimeError(f"Expected 'mask_3d' in region_mask output for {evr_path}")

            file_union = (region_mask_ds["mask_3d"].max("region_id") > 0)
            if set(["ping_time", "depth"]).issubset(set(file_union.dims)):
                file_union = file_union.transpose("ping_time", "depth")

        union_mask = file_union if union_mask is None else (union_mask | file_union)

    if union_mask is None:
        raise RuntimeError("No region mask produced.")

    # Rename back to dataset dims and return
    union_mask = union_mask.rename({"ping_time": time_dim, "depth": depth_dim}).transpose(time_dim, depth_dim)
    return union_mask


def _apply_mask(ds: xr.Dataset, mask: xr.DataArray, time_dim: str, depth_dim: str, write_mask: bool) -> xr.Dataset:
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
            "If your EVR is an echogram region export, check time overlap; if it’s a GPS/alongtrack region file, it may not be usable for Sv masking. :contentReference[oaicite:5]{index=5}"
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

    parser = argparse.ArgumentParser(description="Mask echogram NetCDF (.nc) using Echoview EVR region files.")
    parser.add_argument("input_paths", nargs="*", type=Path, help="Input .nc/.netcdf4 paths (or stdin).")
    parser.add_argument("--evr", required=True, nargs="+", type=Path, help="One or more .evr paths after a single --evr.")
    parser.add_argument("-o", "--output_path", type=Path, help="Output path (ONLY valid when processing exactly 1 input).")
    parser.add_argument("--out-dir", type=Path, help="Output directory for pipelines/multiple inputs.")
    parser.add_argument("--suffix", type=str, default="_evr", help="Suffix appended to output stem (default: _evr).")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite output files if they exist.")
    parser.add_argument("--var", type=str, default="Sv", help="Variable to mask (default: Sv).")
    parser.add_argument("--time-dim", type=str, default=None, help="Time dimension name.")
    parser.add_argument("--depth-dim", type=str, default=None, help="Depth dimension name (e.g., range_sample).")
    parser.add_argument("--channel-index", type=int, default=0, help="Channel index for mask building.")
    parser.add_argument("--write-mask", action="store_true", help="Write 'region_mask' to output.")
    parser.add_argument("--fail-empty", action="store_true", help="Fail if union mask is empty.")
    parser.add_argument("--debug", action="store_true", help="Verbose logging to stderr.")
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