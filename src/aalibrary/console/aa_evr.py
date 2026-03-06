#!/usr/bin/env python3
"""
aa-evr

Mask echogram NetCDF (.nc/.netcdf4) using Echoview region files (.evr) via echoregions Regions2D.

Pipeline behavior (AA-style):
- Reads input NetCDF path(s) from stdin (newline-delimited) when piped OR accepts positional input(s).
- Produces a new NetCDF output per input (original unchanged).
- Emits output path(s) to stdout (one per line) for downstream piping.
- Logs go to stderr.

Key correctness detail:
- echoregions Regions2D.region_mask expects a DataArray with dims (ping_time, depth),
  where 'depth' coordinate values are in meters.
- Many echopype Sv datasets use vertical dim like range_sample/range_bin (index),
  plus meters-valued variables/coords like echo_range or depth (often added by aa-depth).
  This tool will prefer echo_range (1D meters), else derive a 1D meters vector from depth.
"""

import argparse
import sys
import pprint
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

import numpy as np
import xarray as xr
from loguru import logger

import pandas as pd
import echoregions as er


# ---------------------------
# Help
# ---------------------------

def print_help() -> None:
    help_text = """

aa-evr — apply Echoview region(s) (.evr) to an echogram NetCDF (.nc)

USAGE
  # pipeline
  aa-sv input.raw | aa-depth | aa-evr --evr regions/*.evr | aa-plot --all

  # direct
  aa-evr input.nc --evr a.evr b.evr

REQUIRED
  --evr EVR [EVR ...]
    One or more EVR paths after a single --evr (required).

INPUT
  INPUT_PATH [INPUT_PATH ...]
    Optional positional .nc paths.
    If omitted, aa-evr reads newline-delimited .nc paths from stdin.

OUTPUT
  -o, --output_path PATH   Only valid when processing exactly 1 input.
  --out-dir DIR            Output directory for multi-input or pipelines.
  --suffix TEXT            Output suffix (default: _evr)
  --overwrite              Overwrite outputs

MASKING CONTROLS
  --var NAME               Variable to mask (default: Sv)
  --time-dim NAME          Default: infer ping_time, else time
  --depth-dim NAME         Default: infer depth, else range_sample, else range_bin
  --channel-index INT      Channel for mask building (default: 0)
  --write-mask             Write union mask as 'region_mask' (int8)
  --fail-empty             Exit non-zero if union mask is empty (0 inside)
  --debug                  Verbose diagnostics to stderr

NOTE
  EVR depth values are typically meters. If your depth dim is an index (range_sample),
  this tool will attach a meters-valued 1D depth coordinate (prefers echo_range; else depth).

"""
    print(help_text)


def _configure_logging(debug: bool) -> None:
    logger.remove()
    logger.add(sys.stderr, level=("DEBUG" if debug else "INFO"))


# ---------------------------
# Input handling
# ---------------------------

def _iter_input_paths(positional_inputs: List[Path]) -> Iterable[Path]:
    if positional_inputs:
        yield from positional_inputs
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
# Echogram / EVR helpers
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

    if time_dim:
        tdim = time_dim
    else:
        if "ping_time" in da.dims:
            tdim = "ping_time"
        elif "time" in da.dims:
            tdim = "time"
        else:
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
        raise ValueError(f"--time-dim '{tdim}' not found in {var}.dims={da.dims}")
    if ddim not in da.dims:
        raise ValueError(f"--depth-dim '{ddim}' not found in {var}.dims={da.dims}")

    return tdim, ddim


def _select_channel(da: xr.DataArray, channel_index: int) -> xr.DataArray:
    if "channel" not in da.dims:
        return da
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
    return da2


def _meters_depth_vector_1d(
    ds: xr.Dataset,
    da_var: xr.DataArray,
    time_dim: str,
    depth_dim: str,
    channel_index: int,
) -> Optional[xr.DataArray]:
    """
    Return a 1D meters-valued vector aligned to depth_dim.

    Preference:
      1) echo_range (often 1D meters over depth_dim; may include channel)
      2) depth (often meters; may be 2D time x depth_dim; may include channel)
      3) coords of da_var with names echo_range/depth
    """
    # Candidate getter
    def _get(name: str) -> Optional[xr.DataArray]:
        if name in ds:
            return ds[name]
        if name in da_var.coords:
            return da_var.coords[name]
        return None

    # 1) echo_range
    echo_range = _get("echo_range")
    if echo_range is not None:
        if "channel" in echo_range.dims and "channel" in da_var.dims:
            try:
                echo_range = echo_range.isel(channel=channel_index)
                if "channel" in echo_range.coords:
                    try:
                        echo_range = echo_range.drop_vars("channel")
                    except Exception:
                        pass
            except Exception:
                echo_range = None

        if echo_range is not None and depth_dim in echo_range.dims and time_dim not in echo_range.dims:
            # 1D already
            return echo_range

        if echo_range is not None and (time_dim in echo_range.dims) and (depth_dim in echo_range.dims):
            # reduce to 1D
            return echo_range.isel({time_dim: 0})

    # 2) depth
    depth = _get("depth")
    if depth is not None:
        if "channel" in depth.dims and "channel" in da_var.dims:
            try:
                depth = depth.isel(channel=channel_index)
                if "channel" in depth.coords:
                    try:
                        depth = depth.drop_vars("channel")
                    except Exception:
                        pass
            except Exception:
                depth = None

        if depth is not None and depth_dim in depth.dims and time_dim not in depth.dims:
            return depth

        if depth is not None and (time_dim in depth.dims) and (depth_dim in depth.dims):
            # pick representative ping; could also use median(time_dim)
            return depth.isel({time_dim: 0})

    return None


def _evr_global_ranges(regions2d: "er.regions2d.Regions2D") -> Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp], Optional[float], Optional[float]]:
    """
    Compute overall min/max time and min/max depth from Regions2D.data time/depth columns.
    """
    df = regions2d.data
    if "time" not in df.columns or "depth" not in df.columns:
        return None, None, None, None

    all_times = []
    all_depths = []

    for tlist in df["time"]:
        try:
            all_times.extend(pd.to_datetime(tlist))
        except Exception:
            pass

    for dlist in df["depth"]:
        try:
            all_depths.extend([float(x) for x in dlist])
        except Exception:
            pass

    tmin = min(all_times) if all_times else None
    tmax = max(all_times) if all_times else None
    dmin = float(np.nanmin(all_depths)) if all_depths else None
    dmax = float(np.nanmax(all_depths)) if all_depths else None
    return tmin, tmax, dmin, dmax


# ---------------------------
# Mask creation / application
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
    Returns a boolean union mask aligned to ds[var] on (time_dim, depth_dim).
    """
    da = ds[var]
    da_ch = _select_channel(da, channel_index)

    # Build a (ping_time, depth) DataArray for echoregions
    # Rename dims to canonical names expected by echoregions: ping_time and depth
    da_for_mask = da_ch.rename({time_dim: "ping_time", depth_dim: "depth"})

    # Attach a meters-valued 1D depth coordinate (critical)
    depth_m_1d = _meters_depth_vector_1d(ds, da, time_dim=time_dim, depth_dim=depth_dim, channel_index=channel_index)

    if depth_m_1d is not None:
        # Rename depth dim to 'depth' if needed
        depth_m_1d = depth_m_1d.rename({depth_dim: "depth"}) if depth_dim in depth_m_1d.dims else depth_m_1d
        # Ensure it's 1D on 'depth'
        if depth_m_1d.ndim != 1 or "depth" not in depth_m_1d.dims:
            # last resort: try squeezing any leftover dims
            depth_m_1d = depth_m_1d.squeeze()
        da_for_mask = da_for_mask.assign_coords(depth=depth_m_1d)
        if debug:
            logger.debug(f"Using meters depth coord: name={depth_m_1d.name}, dims={depth_m_1d.dims}, "
                         f"min={float(depth_m_1d.min())}, max={float(depth_m_1d.max())}")
    else:
        logger.warning(
            f"No meters-valued depth coordinate found (echo_range/depth). "
            f"Masking will use the raw '{depth_dim}' coordinate values; "
            f"this often yields an empty mask if '{depth_dim}' is an index."
        )

    # Standard ordering
    da_for_mask = da_for_mask.transpose("ping_time", "depth", ...)

    # Compute if dask-backed
    try:
        da_for_mask = da_for_mask.compute()
    except Exception:
        pass

    # Echogram ranges for diagnostics + EVR min/max depth replacement
    ping_time_vals = da_for_mask["ping_time"].values
    ech_tmin = pd.to_datetime(ping_time_vals.min()) if ping_time_vals.size else None
    ech_tmax = pd.to_datetime(ping_time_vals.max()) if ping_time_vals.size else None

    if "depth" in da_for_mask.coords:
        dcoord = da_for_mask["depth"].values
        ech_dmin = float(np.nanmin(dcoord)) if dcoord.size else 0.0
        ech_dmax = float(np.nanmax(dcoord)) if dcoord.size else 1000.0
    else:
        ech_dmin, ech_dmax = 0.0, 1000.0

    if debug:
        logger.debug(f"Echogram ranges: time {ech_tmin} → {ech_tmax}, depth {ech_dmin} → {ech_dmax}")

    union_mask: Optional[xr.DataArray] = None

    for evr_path in evr_files:
        # Use echogram depth range for sentinel replacement in EVR parsing
        regions2d = er.read_evr(str(evr_path), min_depth=float(ech_dmin), max_depth=float(ech_dmax))

        # Force closed polygons by replacing internal dataframe with closed version
        try:
            closed_df = regions2d.close_region()
            regions2d.data = closed_df
        except Exception as e:
            if debug:
                logger.debug(f"close_region() failed or not applied for {evr_path}: {e}")

        if debug:
            tmin, tmax, dmin, dmax = _evr_global_ranges(regions2d)
            logger.debug(f"EVR '{evr_path.name}' ranges: time {tmin} → {tmax}, depth {dmin} → {dmax}")

        region_mask_ds, _region_points = regions2d.region_mask(
            da_for_mask,
            collapse_to_2d=False,
        )

        if "mask_3d" not in region_mask_ds:
            raise RuntimeError(f"Expected 'mask_3d' in region_mask output for {evr_path}")

        # Union within this EVR: any region_id marks inside
        # Use > 0 (robust) rather than == 1
        file_union = (region_mask_ds["mask_3d"].max("region_id") > 0)

        # Standardize dims to (ping_time, depth)
        if set(["ping_time", "depth"]).issubset(set(file_union.dims)):
            file_union = file_union.transpose("ping_time", "depth")

        union_mask = file_union if union_mask is None else (union_mask | file_union)

    if union_mask is None:
        raise RuntimeError("No region mask produced (no EVR files processed?).")

    # Rename mask dims back to dataset dims for application
    union_mask = union_mask.rename({"ping_time": time_dim, "depth": depth_dim})
    union_mask = union_mask.transpose(time_dim, depth_dim)

    return union_mask


def _apply_mask_to_dataset(
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
        ds_out["region_mask"].attrs.update(
            long_name="Union region mask from EVR files (1=inside, 0=outside)"
        )

    return ds_out


# ---------------------------
# Output paths
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
# Process
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

    logger.info(f"Loading NetCDF: {input_path}")
    ds = xr.open_dataset(input_path)

    tdim, ddim = _infer_dims(ds, var=var, time_dim=time_dim, depth_dim=depth_dim)
    if debug:
        logger.debug(f"Using dims for {var}: time_dim='{tdim}', depth_dim='{ddim}', Sv.dims={ds[var].dims}")

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
            "Common causes: EVR time/depth do not overlap echogram, or echogram depth coordinate is not meters.\n"
            "Run with --debug --write-mask to see overlap diagnostics."
        )
        if fail_empty:
            try:
                ds.close()
            except Exception:
                pass
            return None

    ds_out = _apply_mask_to_dataset(ds, mask=mask, time_dim=tdim, depth_dim=ddim, write_mask=write_mask)

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
        nargs="+",
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
        help="Variable used to build/apply region mask (default: Sv).",
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
        help="If --var has 'channel' dim, which channel to build mask from (default: 0).",
    )

    parser.add_argument(
        "--write-mask",
        action="store_true",
        help="Write union mask to output as int8 variable 'region_mask'.",
    )

    parser.add_argument(
        "--fail-empty",
        action="store_true",
        help="Exit non-zero if union mask has zero inside cells.",
    )

    parser.add_argument(
        "--debug",
        action="store_true",
        help="Verbose logging to stderr (prints overlap diagnostics).",
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
                debug=args.debug,
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