#!/usr/bin/env python3
"""
aa-evr

Mask echogram NetCDF using Echoview .evr region files via echoregions Regions2D.

Pipeline behavior (AA-style):
- Input .nc paths: positional OR stdin (newline-delimited)
- Output: new .nc per input
- Stdout: output path(s) (one per line)
- Stderr: logs

Core behavior:
- --evr is provided once and accepts multiple paths after it
- Union all regions across all EVRs
- Mask all vars that share (time_dim, depth_dim): outside -> NaN
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
    print(
        """
aa-evr — apply Echoview region(s) (.evr) to an echogram NetCDF (.nc)

USAGE
  aa-sv in.raw | aa-depth | aa-evr --evr regions/*.evr --depth-dim range_sample | aa-plot --all
  aa-evr input.nc --evr a.evr b.evr

REQUIRED
  --evr EVR [EVR ...]   One or more EVR paths after a single --evr

INPUT
  INPUT_PATH [INPUT_PATH ...]
    Optional positional .nc paths. If omitted, read newline-delimited paths from stdin.

OUTPUT
  -o, --output_path PATH   Only valid when processing exactly 1 input
  --out-dir DIR            Output directory (recommended for pipelines / multiple inputs)
  --suffix TEXT            Output suffix (default: _evr)
  --overwrite              Overwrite outputs

MASKING CONTROLS
  --var NAME               Variable to mask (default: Sv)
  --time-dim NAME          Default: infer ping_time else time
  --depth-dim NAME         Default: infer depth else range_sample else range_bin
  --channel-index INT      Channel used for mask building (default: 0)
  --write-mask             Write union mask to output as int8 variable 'region_mask'
  --fail-empty             Exit non-zero if union mask is empty
  --debug                  Verbose logging to stderr
"""
    )


def _configure_logging(debug: bool) -> None:
    logger.remove()
    logger.add(sys.stderr, level=("DEBUG" if debug else "INFO"))


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
            logger.error(f"Unsupported input extension: {p.name}")
            sys.exit(1)


def _infer_dims(ds: xr.Dataset, var: str, time_dim: Optional[str], depth_dim: Optional[str]) -> Tuple[str, str]:
    if var not in ds.data_vars:
        raise ValueError(f"Variable '{var}' not found. Available: {list(ds.data_vars.keys())}")

    da = ds[var]

    # time
    if time_dim:
        tdim = time_dim
    else:
        tdim = "ping_time" if "ping_time" in da.dims else ("time" if "time" in da.dims else None)
        if tdim is None:
            raise ValueError(f"Could not infer time dim for '{var}'. Provide --time-dim.")

    # depth
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


def _get_depth_vals_meters_1d(
    ds: xr.Dataset,
    da_var: xr.DataArray,
    time_dim: str,
    depth_dim: str,
    channel_index: int,
) -> Optional[np.ndarray]:
    """
    Return a *plain* 1D numpy vector of meters values aligned to depth_dim.
    IMPORTANT: returning numpy avoids dragging scalar coords (like ping_time) into the mask array.
    """
    def _maybe_select_channel(x: xr.DataArray) -> xr.DataArray:
        if "channel" in x.dims and "channel" in da_var.dims:
            x = x.isel(channel=channel_index)
            if "channel" in x.coords:
                try:
                    x = x.drop_vars("channel")
                except Exception:
                    pass
        return x

    # Prefer echo_range if available
    if "echo_range" in ds:
        erng = _maybe_select_channel(ds["echo_range"])
        if depth_dim in erng.dims and time_dim not in erng.dims:
            return np.asarray(erng.values)
        if (time_dim in erng.dims) and (depth_dim in erng.dims):
            return np.asarray(erng.isel({time_dim: 0}).values)

    # Next prefer depth (often from aa-depth)
    if "depth" in ds:
        dep = _maybe_select_channel(ds["depth"])
        if depth_dim in dep.dims and time_dim not in dep.dims:
            return np.asarray(dep.values)
        if (time_dim in dep.dims) and (depth_dim in dep.dims):
            return np.asarray(dep.isel({time_dim: 0}).values)

    # Or coords attached to Sv
    for name in ("echo_range", "depth"):
        if name in da_var.coords:
            c = _maybe_select_channel(da_var.coords[name])
            if depth_dim in c.dims and time_dim not in c.dims:
                return np.asarray(c.values)
            if (time_dim in c.dims) and (depth_dim in c.dims):
                return np.asarray(c.isel({time_dim: 0}).values)

    return None


def _close_regions_in_dataframe(regions2d) -> None:
    """
    Regions2D.close_region(points) closes a *single* polygon. Apply per region row if possible.
    Safe no-op if the expected structure isn't present.
    """
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
            points = list(zip(t_list, d_list))
            points = regions2d.close_region(points)  # closes one region polygon
            t_closed, d_closed = zip(*points)
            new_times.append(list(t_closed))
            new_depths.append(list(d_closed))
        except Exception:
            new_times.append(t_list)
            new_depths.append(d_list)

    try:
        df["time"] = new_times
        df["depth"] = new_depths
        # many echoregions internals use .data as the working dataframe
        regions2d.data = df
    except Exception:
        pass


def _build_union_region_mask(
    ds: xr.Dataset,
    evr_files: List[Path],
    var: str,
    time_dim: str,
    depth_dim: str,
    channel_index: int,
    debug: bool,
) -> xr.DataArray:
    da = ds[var]
    da_ch = _select_channel(da, channel_index)

    # Build the array echoregions wants: dims ("ping_time","depth")
    da_for_mask = da_ch.rename({time_dim: "ping_time", depth_dim: "depth"}).transpose("ping_time", "depth", ...)

    # FORCE ping_time to be a 1D coordinate (prevents scalar ping_time coord issues)
    pt_vals = np.asarray(ds[time_dim].values if time_dim in ds else da_ch[time_dim].values)
    da_for_mask = da_for_mask.assign_coords(ping_time=("ping_time", pt_vals))

    # Attach meters-valued 1D depth coordinate (as numpy to avoid importing scalar coords)
    depth_vals = _get_depth_vals_meters_1d(ds, da, time_dim=time_dim, depth_dim=depth_dim, channel_index=channel_index)
    if depth_vals is None:
        logger.warning(
            "No meters-valued depth vector found (echo_range/depth). "
            "EVR masking will likely be empty if your depth_dim is an index."
        )
        # fall back to existing depth coordinate if present; otherwise it will be indices
        if "depth" in da_for_mask.coords and da_for_mask["depth"].ndim == 1:
            depth_vals = np.asarray(da_for_mask["depth"].values)

    if depth_vals is not None:
        da_for_mask = da_for_mask.assign_coords(depth=("depth", np.asarray(depth_vals)))

    if debug and depth_vals is not None:
        logger.debug(f"Mask depth range (m): {float(np.nanmin(depth_vals))} → {float(np.nanmax(depth_vals))}")
        logger.debug(f"Mask time range: {pt_vals.min()} → {pt_vals.max()}")

    # Compute if dask-backed
    try:
        da_for_mask = da_for_mask.compute()
    except Exception:
        pass

    union_mask: Optional[xr.DataArray] = None

    for evr_path in evr_files:
        # Use raw_range to convert EVR range edges robustly; convert_time aligns with datetime64 ping_time.
        regions2d = er.read_evr(
            str(evr_path),
            convert_time=True,
            raw_range=(None if depth_vals is None else np.asarray(depth_vals)),
        )

        # Optional: close polygons row-by-row (safe no-op if structure differs)
        _close_regions_in_dataframe(regions2d)

        region_mask_ds, _ = regions2d.region_mask(da_for_mask, collapse_to_2d=False)
        if "mask_3d" not in region_mask_ds:
            raise RuntimeError(f"Expected mask_3d in region_mask output for {evr_path}")

        file_union = (region_mask_ds["mask_3d"].max("region_id") > 0)
        if set(["ping_time", "depth"]).issubset(file_union.dims):
            file_union = file_union.transpose("ping_time", "depth")

        union_mask = file_union if union_mask is None else (union_mask | file_union)

    if union_mask is None:
        raise RuntimeError("No region mask produced.")

    # Rename back to dataset dims for application
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
        logger.error("Union mask is EMPTY (0 inside cells). Output will plot blank.")
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

    parser = argparse.ArgumentParser(description="Mask echogram NetCDF using Echoview EVR region files.")
    parser.add_argument("input_paths", nargs="*", type=Path, help="Input .nc/.netcdf4 paths (or stdin).")
    parser.add_argument("--evr", required=True, nargs="+", type=Path, help="One or more .evr paths after a single --evr.")
    parser.add_argument("-o", "--output_path", type=Path, help="Output path (only when exactly 1 input).")
    parser.add_argument("--out-dir", type=Path, help="Output directory for pipelines/multiple inputs.")
    parser.add_argument("--suffix", type=str, default="_evr", help="Suffix appended to output stem.")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite outputs.")
    parser.add_argument("--var", type=str, default="Sv", help="Variable to mask (default Sv).")
    parser.add_argument("--time-dim", type=str, default=None, help="Time dim name.")
    parser.add_argument("--depth-dim", type=str, default=None, help="Depth dim name (e.g., range_sample).")
    parser.add_argument("--channel-index", type=int, default=0, help="Channel index for mask building.")
    parser.add_argument("--write-mask", action="store_true", help="Write 'region_mask' to output.")
    parser.add_argument("--fail-empty", action="store_true", help="Fail if union mask is empty.")
    parser.add_argument("--debug", action="store_true", help="Verbose logging.")
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
                print(str(produced))
            else:
                any_fail = True
        except Exception as e:
            any_fail = True
            logger.exception(f"Error processing {in_path}: {e}")

    return 1 if any_fail else 0


if __name__ == "__main__":
    raise SystemExit(main())