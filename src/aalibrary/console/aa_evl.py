#!/usr/bin/env python3
"""
aa-evl

Mask echogram NetCDF (.nc/.netcdf4) using Echoview line files (.evl) via echoregions Lines2D.

AA-style pipeline behavior:
- Reads input NetCDF paths from stdin (newline-delimited) when piped OR accepts positional inputs.
- Produces a NEW NetCDF output per input.
- Emits output path(s) to stdout (one per line) for downstream piping.
- Logs go to stderr.

Core behavior:
- --evl accepts one or more .evl paths (argparse nargs="+").
- Loads all EVLs and builds a per-ping depth threshold for each, then unions them
  into a single composite line (the shallowest or deepest, depending on --keep).
- Applies the line mask to all variables containing (time_dim, depth_dim): the
  side that is masked becomes NaN.

EVL semantics:
  An EVL file is a time-series of (datetime, depth_metres) points defining a
  boundary line across the echogram (e.g. seafloor, surface, bottom exclusion
  zone).  Unlike EVR (closed polygons), an EVL is an open line; there is no
  "inside" - only above vs. below.

  --keep above   Keep data ABOVE the union line (default); mask everything below.
                 Typical use: mask out seafloor / bottom noise.
  --keep below   Keep data BELOW the union line; mask everything above.
                 Typical use: mask out near-surface noise, keep deep water data.
  --keep between Requires exactly two EVL files; keeps data between the two lines.

  --depth-offset METRES
                 Shift the line up (negative) or down (positive) before masking.
                 Useful for adding a safety buffer above the seafloor, e.g. -5.0
                 to keep data more than 5 m above the detected bottom.

Design notes:
- Union line for "above" mode:  per-ping MINIMUM depth across all EVLs
  (the shallowest of all lines; anything above all of them is safe).
- Union line for "below" mode:  per-ping MAXIMUM depth across all EVLs
  (the deepest of all lines).
- "Between" mode requires exactly 2 EVLs: upper_line <= depth <= lower_line.
- Lines are interpolated to every ping_time in the echogram using linear
  interpolation; pings outside the EVL time range use the nearest boundary
  value (forward/back fill).
- Sentinel depth values (e.g. +/-9999.99) are replaced with the echogram
  depth min/max before interpolation.
- Coordinate-label mismatch is avoided by positional masking (same fix as aa-evr).
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
# _configure_logging() below replaces this once --debug is parsed.
logger.add(sys.stderr, level="WARNING")

# Now the heavy imports - anything they log gets squashed
import argparse
import pprint
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd
import xarray as xr

import echoregions as er


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


# ---------------------------------------------------------------------------
# Workaround: echoregions.lines.lines_parser.parse_evl crashes on pandas 2.x
# when string[pyarrow] is the default string dtype.
#
# Upstream code does:
#     df = pd.DataFrame([{"time": "20090916 1247393820", ...}])  # str column
#     df.loc[:, "time"] = df.loc[:, "time"].apply(parse_time)    # <-- DatetimeArray
#
# The .apply returns datetime objects but the column is dtyped `string[pyarrow]`,
# which refuses datetime values:
#     TypeError: Invalid value '<DatetimeArray>...' for dtype 'str'
#
# Old pandas silently upgraded the column dtype; new pandas does not.
# We patch parse_evl to convert the string column to datetime BEFORE
# writing it back, sidestepping the dtype clash entirely.
#
# Re-applies upstream's own logic; only the assignment mechanic changes.
# ---------------------------------------------------------------------------
def _patch_echoregions_parse_evl() -> None:
    try:
        import os as _os
        from echoregions.lines import lines_parser as _lp
        from echoregions.utils.io import check_file as _check_file
        from echoregions.utils.time import parse_time as _parse_time
    except Exception as _exc:
        logger.debug(f"Could not patch echoregions.parse_evl: {_exc}")
        return

    def parse_evl(input_file: str):
        _check_file(input_file, "EVL")
        with open(input_file, encoding="utf-8-sig") as fid:
            file_lines = fid.readlines()
        file_type, file_format_number, ev_version = file_lines[0].strip().split()
        file_metadata = {
            "file_name": (
                _os.path.splitext(_os.path.basename(input_file))[0]
                + _os.path.splitext(_os.path.basename(input_file))[1]
            ),
            "file_type": file_type,
            "evl_file_format_version": file_format_number,
            "echoview_version": ev_version,
        }
        n_points = int(file_lines[1].strip())
        if (len(file_lines) - 2) != n_points:
            raise ValueError(
                "There exists a mismatch between the expected number of lines "
                f"in the file and the actual number of points. There should be "
                f"2 less lines in the file than the number of points, however "
                f"we have {len(file_lines)} number of lines in the file and "
                f"{n_points} number of points."
            )
        points = []
        for i in range(n_points):
            date, time, depth, status = file_lines[i + 2].strip().split()
            points.append(
                {
                    # Pre-parse to datetime so the column is born as the right
                    # dtype — no string-to-datetime reassignment needed.
                    "time": _parse_time(f"{date} {time}"),
                    "depth": float(depth),
                    "status": status,
                }
            )
        df = pd.DataFrame(points)
        df = df.assign(**file_metadata)
        order = list(file_metadata.keys()) + ["time", "depth", "status"]
        return df[order]

    _lp.parse_evl = parse_evl
    # The Lines class imports parse_evl by name into its own module
    # namespace, so we have to rebind the reference there too — patching
    # only lines_parser leaves Lines.__init__ calling the unpatched copy.
    try:
        from echoregions.lines import lines as _lines_mod
        _lines_mod.parse_evl = parse_evl
    except Exception as _exc:
        logger.debug(f"Could not rebind parse_evl in echoregions.lines.lines: {_exc}")
    logger.debug("Applied echoregions.parse_evl PyArrow-string-dtype patch.")


_patch_echoregions_parse_evl()


# ---------------------------
# Help / logging
# ---------------------------

def print_help() -> None:
    print(
        """
aa-evl - apply Echoview line(s) (.evl) to an echogram NetCDF (.nc/.netcdf4)

USAGE
  echo input.nc | aa-evl --evl seafloor.evl | aa-plot --all
  aa-evl input.nc --evl top.evl bottom.evl --keep between
  echo input.nc | aa-evl --evl seafloor.evl --depth-offset -5.0 --overwrite

REQUIRED
  --evl EVL [EVL ...]     One or more .evl paths (accepts wildcards via shell).

INPUT
  INPUT_PATH [INPUT_PATH ...]
    Optional positional .nc paths. If omitted, reads newline-delimited .nc paths
    from stdin.

OUTPUT
  -o, --output-path PATH  Only valid when processing exactly 1 input.
  --out-dir DIR           Output directory for pipelines / multiple inputs.
  --suffix TEXT           Suffix appended to output stem (default: _evl).
  --overwrite             Overwrite output files if they exist.

MASKING
  --keep {above,below,between}
                          Which side of the line(s) to KEEP (default: above).
                            above   : keep data shallower than the line; mask
                                      deeper data.  Union = per-ping MIN depth.
                                      Typical use: exclude seafloor / bottom.
                            below   : keep data deeper than the line; mask
                                      shallower data.  Union = per-ping MAX depth.
                                      Typical use: exclude near-surface noise.
                            between : keep data BETWEEN two lines.  Requires
                                      exactly 2 EVL files (upper then lower).
  --depth-offset METRES   Shift the composite line by this many metres before
                          masking.  Negative = shift up (shallower); positive =
                          shift down (deeper).  Default: 0.0.
                          Example: --depth-offset -5  removes 5 m above seafloor.
  --var NAME              Variable to mask (default: Sv).
  --time-dim NAME         Time dimension name (default: infer ping_time or time).
  --depth-dim NAME        Depth dimension name (default: infer depth, range_sample,
                          or range_bin).
  --channel-index INT     Channel used to resolve metre-depth coordinates when the
                          variable has a 'channel' dim (default: 0).
  --write-line            Write the interpolated composite line as a variable
                          'evl_line_depth' in the output NetCDF.
  --fail-empty            Exit non-zero if the composite line has no valid points.
  --debug                 Verbose diagnostics to stderr.

EXAMPLES
  # Mask everything below the seafloor line, with a 5 m safety buffer above it:
  aa-evl input.nc --evl seafloor.evl --keep above --depth-offset -5.0

  # Mask near-surface noise (everything above the surface exclusion line):
  aa-evl input.nc --evl surface.evl --keep below

  # Keep only data between two operator-drawn lines:
  aa-evl input.nc --evl upper.evl lower.evl --keep between

  # Full pipeline:
  echo D20090916-T132105.raw | aa-nc --sonar_model EK60 | aa-sv | aa-depth \\
    | aa-evl --evl seafloor.evl --depth-offset -5.0 --overwrite | aa-plot --all

NOTE
  EVL files are Echoview line export files containing (datetime, depth_metres)
  pairs that define a boundary line across the echogram.  They differ from EVR
  (region) files, which contain closed polygons.
"""
    )


def _configure_logging(debug: bool) -> None:
    """Replace the default suppression sink with a user-visible one.
    Keeps standard logging fully disabled."""
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


def _validate_inputs(input_paths: List[Path], evl_paths: List[Path], keep: str) -> None:
    allowed_ext = {".nc", ".netcdf4"}

    if not evl_paths:
        logger.error("At least one --evl file is required.")
        sys.exit(2)

    if keep == "between" and len(evl_paths) != 2:
        logger.error(
            f"--keep between requires exactly 2 EVL files; got {len(evl_paths)}."
        )
        sys.exit(2)

    for evl in evl_paths:
        if not evl.exists():
            logger.error(f"EVL file not found: {evl}")
            sys.exit(2)
        if evl.suffix.lower() != ".evl":
            logger.warning(f"Unexpected extension for EVL file: {evl.name}")

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
# Dimension helpers
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


def _get_depth_coord_metres(
    ds: xr.Dataset,
    var_da: xr.DataArray,
    time_dim: str,
    depth_dim: str,
    channel_index: int,
) -> Optional[np.ndarray]:
    """
    Return a 1-D numpy array of depth values in metres aligned to depth_dim,
    or None if no usable metres coordinate is found.
    Tries echo_range then depth; strips channel if present; takes first ping
    if 2-D.
    """
    def _pick_ch(x):
        if x is None:
            return None
        if "channel" in x.dims:
            try:
                x = x.isel(channel=channel_index)
                if "channel" in x.coords:
                    x = x.drop_vars("channel")
            except Exception:
                return None
        return x

    for cname in ("echo_range", "depth"):
        x = _pick_ch(ds[cname]) if cname in ds else None
        if x is None and cname in var_da.coords:
            x = _pick_ch(var_da.coords[cname])
        if x is None:
            continue
        if depth_dim in x.dims and time_dim not in x.dims:
            return np.asarray(x.values, dtype=float)
        if time_dim in x.dims and depth_dim in x.dims:
            return np.asarray(x.isel({time_dim: 0}).values, dtype=float)

    return None


# ---------------------------
# EVL reading + line building
# ---------------------------

def _read_evl_to_series(
    evl_path: Path,
    ech_depth_min: float,
    ech_depth_max: float,
    debug: bool,
) -> pd.Series:
    """
    Read a single EVL file and return a pd.Series indexed by datetime64[ns]
    with depth values in metres.

    Handles:
    - Sentinel depth values (|depth| >= 9000) -> clipped to echogram range.
    - NaN / bad depth values -> dropped.
    - The echoregions Lines2D dataframe layout (columns depend on version).
    """
    lines2d = er.read_evl(str(evl_path))

    # --- get underlying dataframe (version-resilient) ---
    df = None
    if hasattr(lines2d, "to_dataframe"):
        try:
            df = lines2d.to_dataframe()
        except Exception:
            pass
    if df is None and hasattr(lines2d, "data"):
        try:
            df = lines2d.data
        except Exception:
            pass
    if df is None:
        raise RuntimeError(
            f"Could not access Lines2D dataframe for {evl_path}"
        )

    df = df.copy()

    if debug:
        logger.debug(
            f"{evl_path.name}: Lines2D dataframe columns={list(df.columns)}, "
            f"shape={df.shape}"
        )

    # --- locate time and depth columns ---
    # Common column name variations across echoregions versions:
    #   time / ping_time / datetime
    #   depth / depth_meters / Depth
    time_col = None
    for candidate in ("ping_time", "time", "datetime", "Time"):
        if candidate in df.columns:
            time_col = candidate
            break

    depth_col = None
    for candidate in ("depth", "depth_meters", "Depth", "range"):
        if candidate in df.columns:
            depth_col = candidate
            break

    # If the dataframe has a DatetimeIndex, use that for time
    if time_col is None and isinstance(df.index, pd.DatetimeIndex):
        df = df.reset_index().rename(columns={"index": "ping_time"})
        time_col = "ping_time"

    if time_col is None or depth_col is None:
        raise RuntimeError(
            f"{evl_path.name}: Cannot locate time/depth columns in EVL dataframe. "
            f"Available columns: {list(df.columns)}"
        )

    # --- coerce time to datetime64[ns] ---
    t_raw = pd.to_datetime(df[time_col], errors="coerce", utc=False)
    # Strip timezone so we can compare with naive NetCDF timestamps
    if hasattr(t_raw, "dt") and t_raw.dt.tz is not None:
        t_raw = t_raw.dt.tz_localize(None)

    # --- coerce depth to float, replace sentinels, clip ---
    d_raw = pd.to_numeric(df[depth_col], errors="coerce").astype(float)
    d_raw = d_raw.where(d_raw.abs() < 9000, other=np.nan)  # drop sentinels
    d_raw = d_raw.clip(lower=ech_depth_min, upper=ech_depth_max)

    # --- build series, drop NaT / NaN rows, sort by time ---
    s = pd.Series(d_raw.values, index=t_raw, name="depth")
    s = s[s.index.notna() & s.notna()]
    s = s.sort_index()

    if len(s) == 0:
        raise RuntimeError(
            f"{evl_path.name}: No valid (time, depth) points after cleaning. "
            "Check that the EVL time range overlaps the echogram."
        )

    if debug:
        logger.debug(
            f"{evl_path.name}: {len(s)} valid line points, "
            f"depth range {s.min():.2f}-{s.max():.2f} m, "
            f"time range {s.index.min()} -> {s.index.max()}"
        )

    return s


def _interpolate_line_to_pings(
    line_series: pd.Series,
    ping_times: np.ndarray,
    fill_value_min: float,
    fill_value_max: float,
) -> np.ndarray:
    """
    Linearly interpolate a (time -> depth) line Series to every ping_time in the
    echogram.  Pings outside the EVL time range are filled with the nearest
    boundary value (clamp extrapolation rather than NaN-fill).

    Returns a 1-D float64 array of shape (n_pings,).
    """
    # Convert everything to float64 nanoseconds for interpolation
    evl_t_ns = line_series.index.astype(np.int64).astype(float)
    evl_d = line_series.values.astype(float)

    # ping times as float64 ns
    ping_t_ns = pd.to_datetime(ping_times).astype(np.int64).astype(float)

    # Use numpy interp (clamps at boundaries automatically)
    interp_depth = np.interp(ping_t_ns, evl_t_ns, evl_d)

    # Apply explicit clamp to echogram depth range
    interp_depth = np.clip(interp_depth, fill_value_min, fill_value_max)

    return interp_depth


# ---------------------------
# Mask building + application
# ---------------------------

def _build_line_mask(
    ds: xr.Dataset,
    evl_files: List[Path],
    var: str,
    time_dim: str,
    depth_dim: str,
    channel_index: int,
    keep: str,
    depth_offset: float,
    debug: bool,
) -> Tuple[xr.DataArray, np.ndarray]:
    """
    Build a boolean mask (True = keep) aligned to ds[var] on (time_dim, depth_dim).

    Returns
    -------
    mask : xr.DataArray
        Boolean, dims (time_dim, depth_dim), positional coordinates.
    composite_line : np.ndarray
        1-D float array of shape (n_pings,): the interpolated line depth after
        offset, in metres (or depth-index units if no metre coord found).
    """

    var_da = ds[var]

    # --- channel stripping ---
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

    # --- original coordinate arrays (positional labels for output mask) ---
    orig_time_vals = np.asarray(
        ds[time_dim].values if time_dim in ds.coords else da.coords[time_dim].values
    )
    orig_depth_vals = (
        np.asarray(da.coords[depth_dim].values)
        if depth_dim in da.coords
        else np.arange(da.sizes[depth_dim])
    )

    # --- resolve metre-valued depth axis ---
    depth_vals_m = _get_depth_coord_metres(
        ds, var_da, time_dim, depth_dim, channel_index
    )

    if depth_vals_m is not None:
        ech_depth_min = float(np.nanmin(depth_vals_m))
        ech_depth_max = float(np.nanmax(depth_vals_m))
    else:
        ech_depth_min = float(np.nanmin(orig_depth_vals))
        ech_depth_max = float(np.nanmax(orig_depth_vals))
        logger.warning(
            "No metre-valued depth coordinate found; using range_sample indices "
            "as depth proxy. Masking may be inaccurate if the depth axis is not "
            "in metres. Provide echo_range or depth in the dataset for best results."
        )

    if debug:
        logger.debug(
            f"Echogram depth range: {ech_depth_min:.2f} -> {ech_depth_max:.2f} m"
        )
        logger.debug(
            f"Ping time range: {orig_time_vals.min()} -> {orig_time_vals.max()}"
        )

    # --- read and interpolate each EVL ---
    per_file_lines: List[np.ndarray] = []
    for evl_path in evl_files:
        s = _read_evl_to_series(evl_path, ech_depth_min, ech_depth_max, debug)
        interp = _interpolate_line_to_pings(
            s, orig_time_vals, ech_depth_min, ech_depth_max
        )
        per_file_lines.append(interp)
        if debug:
            logger.debug(
                f"{evl_path.name}: interpolated line depth "
                f"min={interp.min():.2f} max={interp.max():.2f} m"
            )

    # --- build composite line ---
    composite_line: Optional[np.ndarray] = None
    upper_line: Optional[np.ndarray] = None
    lower_line: Optional[np.ndarray] = None

    if keep == "above":
        # Shallowest of all lines -> keep anything above even the shallowest
        composite_line = np.min(np.stack(per_file_lines, axis=0), axis=0)
    elif keep == "below":
        # Deepest of all lines -> keep anything below even the deepest
        composite_line = np.max(np.stack(per_file_lines, axis=0), axis=0)
    else:  # between - validated to have exactly 2
        upper_line = per_file_lines[0]
        lower_line = per_file_lines[1]
        # Ensure correct ordering (upper should be shallower)
        if np.median(upper_line) > np.median(lower_line):
            logger.warning(
                "The first EVL appears deeper than the second for --keep between. "
                "Swapping so the shallower line is treated as upper."
            )
            upper_line, lower_line = lower_line, upper_line

    # --- apply depth offset ---
    if keep == "between":
        upper_line = np.clip(upper_line + depth_offset, ech_depth_min, ech_depth_max)
        lower_line = np.clip(lower_line + depth_offset, ech_depth_min, ech_depth_max)
    else:
        composite_line = np.clip(
            composite_line + depth_offset, ech_depth_min, ech_depth_max
        )

    if debug and composite_line is not None:
        logger.debug(
            f"Composite line after offset: min={composite_line.min():.2f} "
            f"max={composite_line.max():.2f} m"
        )

    # --- depth axis in metres for comparison ---
    depth_axis = depth_vals_m if depth_vals_m is not None else orig_depth_vals.astype(float)

    # --- build boolean mask (n_pings, n_depth) ---
    # depth_axis: shape (n_depth,)
    # composite/upper/lower: shape (n_pings,)
    # We need a (n_pings, n_depth) boolean array.
    # Expand dims for broadcasting:
    #   depth_axis_2d : (1, n_depth)
    #   line_2d       : (n_pings, 1)

    depth_2d = depth_axis[np.newaxis, :]  # (1, n_depth)

    if keep == "above":
        # Keep cells where depth <= line threshold (shallower than / on the line)
        line_2d = composite_line[:, np.newaxis]  # (n_pings, 1)
        mask_np = depth_2d <= line_2d

    elif keep == "below":
        # Keep cells where depth >= line threshold (deeper than / on the line)
        line_2d = composite_line[:, np.newaxis]
        mask_np = depth_2d >= line_2d

    else:  # between
        upper_2d = upper_line[:, np.newaxis]
        lower_2d = lower_line[:, np.newaxis]
        mask_np = (depth_2d >= upper_2d) & (depth_2d <= lower_2d)

    inside = int(mask_np.sum())
    total = mask_np.size
    logger.info(
        f"Line mask coverage: {inside}/{total} cells kept "
        f"({100.0 * inside / max(total, 1):.1f}%)"
    )

    # --- wrap into DataArray with original coordinate labels ---
    mask_da = xr.DataArray(
        mask_np,
        dims=(time_dim, depth_dim),
        coords={
            time_dim: orig_time_vals,
            depth_dim: orig_depth_vals,
        },
    )

    # For "between" mode there is no single composite line; return the upper
    # one so callers that want to write a representative line still get something.
    returned_line = composite_line if keep != "between" else upper_line
    return mask_da, returned_line


def _apply_mask(
    ds: xr.Dataset,
    mask: xr.DataArray,
    time_dim: str,
    depth_dim: str,
    write_line: bool,
    composite_line: np.ndarray,
) -> xr.Dataset:
    """
    Apply boolean mask to all variables containing (time_dim, depth_dim).
    Uses positional masking (coordinate-stripped) to avoid the silent
    coordinate-mismatch / NaN-fill bug from aa-evr.

    Variables that represent the depth/range *axis* (echo_range, depth,
    range, range_meter, etc.) are intentionally NOT masked. They define
    the coordinate system and need to remain valid for downstream tools
    (especially aa-plot's auto-range — masking them creates a NaN-filled
    axis that visibly breaks the y-axis scale).
    """
    AXIS_VARS = frozenset({
        "echo_range", "depth", "range", "range_m", "range_meter", "range_sample",
    })

    ds_out = ds.copy(deep=False)

    # Pre-compute positional numpy mask (n_time, n_depth)
    mask_np = np.asarray(mask.transpose(time_dim, depth_dim).values, dtype=bool)

    for name, da in list(ds_out.data_vars.items()):
        if name in AXIS_VARS:
            continue  # Skip axis variables — masking them breaks the coord system
        if (time_dim in da.dims) and (depth_dim in da.dims):
            m_bare = xr.DataArray(mask_np, dims=(time_dim, depth_dim))
            m_broadcast = m_bare.broadcast_like(da)
            ds_out[name] = xr.where(m_broadcast, da, np.nan)

    if write_line and composite_line is not None:
        # Store the composite line depth per ping
        time_vals = np.asarray(
            ds[time_dim].values if time_dim in ds.coords
            else ds_out[time_dim].values
        )
        ds_out["evl_line_depth"] = xr.DataArray(
            composite_line,
            dims=(time_dim,),
            coords={time_dim: time_vals},
        )
        ds_out["evl_line_depth"].attrs["long_name"] = (
            "Composite EVL line depth (metres) after offset"
        )
        ds_out["evl_line_depth"].attrs["units"] = "m"

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
    evl_files: List[Path],
    output_path: Path,
    overwrite: bool,
    var: str,
    time_dim: Optional[str],
    depth_dim: Optional[str],
    channel_index: int,
    keep: str,
    depth_offset: float,
    write_line: bool,
    fail_empty: bool,
    debug: bool,
) -> Optional[Path]:
    if output_path.exists() and not overwrite:
        logger.error(f"Output exists (use --overwrite): {output_path}")
        return None

    # Guard against clobbering the input
    if output_path.resolve() == input_path.resolve():
        logger.error(f"Refusing to overwrite input file: {input_path.resolve()}")
        return None

    # Read into memory and release the file handle so we can write into the
    # same directory without xarray holding a read lock.
    with xr.open_dataset(input_path) as ds_in:
        ds = ds_in.load()

    tdim, ddim = _infer_dims(ds, var=var, time_dim=time_dim, depth_dim=depth_dim)
    if debug:
        logger.debug(
            f"Using dims: time_dim='{tdim}', depth_dim='{ddim}', "
            f"{var}.dims={ds[var].dims}, shape={ds[var].shape}"
        )

    mask, composite_line = _build_line_mask(
        ds=ds,
        evl_files=evl_files,
        var=var,
        time_dim=tdim,
        depth_dim=ddim,
        channel_index=channel_index,
        keep=keep,
        depth_offset=depth_offset,
        debug=debug,
    )

    inside = int(mask.values.sum())
    total = int(mask.size)

    if inside == 0:
        logger.error(
            "Line mask is EMPTY - 0 cells would be kept; output will be all-NaN.\n"
            "Possible causes:\n"
            "  - EVL time range does not overlap the NetCDF ping_time range\n"
            "  - --keep direction is inverted for this line\n"
            "  - Sentinel depth values were not resolved correctly\n"
            "Run with --debug to inspect time/depth ranges."
        )
        if fail_empty:
            return None

    if inside == total:
        logger.warning(
            "Line mask is FULL - all cells are kept; no data will be masked.\n"
            "Check that the EVL line is within the echogram depth range and that\n"
            "--keep direction is correct."
        )

    ds_out = _apply_mask(
        ds,
        mask=mask,
        time_dim=tdim,
        depth_dim=ddim,
        write_line=write_line,
        composite_line=composite_line,
    )
    ds_out.attrs["aa_tool"] = "aa-evl"
    ds_out.attrs["aa_evl_files"] = ",".join(str(p) for p in evl_files)
    ds_out.attrs["aa_evl_keep"] = keep
    ds_out.attrs["aa_evl_depth_offset"] = str(depth_offset)

    ds_out.to_netcdf(output_path)

    return output_path


# ---------------------------
# Entry point
# ---------------------------

def main() -> int:
    if len(sys.argv) == 1 and sys.stdin.isatty():
        print_help()
        return 0

    parser = argparse.ArgumentParser(
        description="Mask echogram NetCDF (.nc) using Echoview EVL line files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "input_paths", nargs="*", type=Path,
        help="Input .nc/.netcdf4 paths (or read from stdin).",
    )
    parser.add_argument(
        "--evl", required=True, nargs="+", type=Path, metavar="EVL",
        help="One or more .evl paths.",
    )
    parser.add_argument(
        "-o", "--output-path", dest="output_path", type=Path,
        help="Output path (only valid for a single input file).",
    )
    parser.add_argument(
        "--out-dir", type=Path,
        help="Output directory (for pipelines / multiple inputs).",
    )
    parser.add_argument(
        "--suffix", type=str, default="_evl",
        help="Suffix appended to output stem (default: _evl).",
    )
    parser.add_argument(
        "--overwrite", action="store_true",
        help="Overwrite existing output files.",
    )
    parser.add_argument(
        "--keep", choices=["above", "below", "between"], default="above",
        help=(
            "Which side of the line to KEEP (default: above).  "
            "'above' keeps shallower data (masks seafloor/bottom).  "
            "'below' keeps deeper data (masks surface).  "
            "'between' requires exactly 2 EVL files."
        ),
    )
    parser.add_argument(
        "--depth-offset", type=float, default=0.0, metavar="METRES",
        help=(
            "Shift the composite line by this many metres before masking.  "
            "Negative = shift line up (shallower boundary).  "
            "Default: 0.0."
        ),
    )
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
        help="Channel index for metre-depth resolution (default: 0).",
    )
    parser.add_argument(
        "--write-line", action="store_true",
        help="Write interpolated composite line as 'evl_line_depth' in output.",
    )
    parser.add_argument(
        "--fail-empty", action="store_true",
        help="Exit non-zero if the line mask keeps zero cells.",
    )
    parser.add_argument(
        "--debug", action="store_true",
        help="Verbose diagnostics to stderr.",
    )
    args = parser.parse_args()

    _configure_logging(args.debug)

    input_paths = list(_iter_input_paths(args.input_paths))
    if not input_paths:
        logger.error("No input paths provided (positional or via stdin).")
        return 1

    input_paths = [p.expanduser().resolve() for p in input_paths]
    evl_files = [p.expanduser().resolve() for p in args.evl]

    _validate_inputs(input_paths, evl_files, args.keep)

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
        logger.debug(f"\naa-evl args:\n{pprint.pformat(vars(args))}")

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
                evl_files=evl_files,
                output_path=out_path,
                overwrite=args.overwrite,
                var=args.var,
                time_dim=args.time_dim,
                depth_dim=args.depth_dim,
                channel_index=args.channel_index,
                keep=args.keep,
                depth_offset=args.depth_offset,
                write_line=args.write_line,
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