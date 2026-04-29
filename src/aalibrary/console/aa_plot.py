#!/usr/bin/env python3
"""
aa-plot - Interactive echogram plotting (HTML) for Echopype/xarray NetCDF datasets

Goals:
- Accept .nc path from argv OR stdin (pipeline-friendly).
- Plot a variable (default: Sv if present).
- Plot ALL channels and/or frequencies in a tabbed UI (Panel + hvPlot).
  * If Sv has a 'channel' dimension, tabs are per-channel (labels include
    frequency_nominal if present as a coord on channel).
  * If Sv has a 'frequency_nominal' dimension, tabs are per-frequency.
- No matplotlib. Output is standalone HTML.
- Print absolute HTML path to stdout for downstream chaining.
- Keep stdout clean except for final path (logs go to stderr via loguru).
- Drawing tools (freehand, polyline, region polygon) overlay the echogram;
  annotations export to EVL (lines) or EVR (regions) Echoview-compatible files.

Notes for cloud / JupyterLab workspaces:
- JupyterLab opens HTML files in a restrictive iframe sandbox that blocks
  file downloads (the sandbox lacks `allow-downloads`). The "Export EVL/EVR"
  buttons attempt a download but may silently fail. The "Show EVL/EVR text"
  buttons always work: they open a modal with the file content for copy/paste.
"""

from __future__ import annotations

# === Silence logs BEFORE any heavy imports ===
import logging
import sys
import warnings

logging.disable(logging.CRITICAL)
warnings.filterwarnings("ignore")

from loguru import logger
logger.remove()
# Default sink: WARNING+ to stderr so real errors aren't swallowed.
# _configure_logging() below replaces this once --quiet is parsed.
logger.add(sys.stderr, level="WARNING")

# Now the heavy imports - anything they log gets squashed
import argparse
import io
import json
from contextlib import redirect_stdout, redirect_stderr
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple, Any

import numpy as np
import xarray as xr

import holoviews as hv
from holoviews import opts
import hvplot.xarray  # noqa: F401
import panel as pn

pn.extension()


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
# Y-axis names that represent "depth / range" and should be drawn top-down
# ---------------------------------------------------------------------------
_DEPTH_RANGE_NAMES = frozenset({
    "echo_range", "range", "range_meter", "range_sample",
    "depth", "range_bin", "distance", "range_m",
})

_CMAP_OPTIONS = [
    "inferno", "viridis", "plasma", "magma", "cividis",
    "turbo", "coolwarm", "gray", "RdYlBu_r", "Spectral_r",
]


def print_help() -> None:
    help_text = r"""
Usage: aa-plot [OPTIONS] [INPUT_PATH]

Arguments:
  INPUT_PATH                Path to a NetCDF file (.nc). Optional; if omitted,
                            reads a single path token from stdin.

Core selection:
  --var VAR                 Variable to plot (default: Sv if present, else first data_var).
  --all                     Plot all channels/frequencies as tabs.
  --frequency FLOAT         Select single nominal frequency (Hz) (nearest match).
  --channel NAME            Select single channel by name (exact match preferred).
  --group-by {auto,channel,freq}
                            When --all and both channel+freq dimensions are available:
                              auto   -> frequency outer tabs, channel inner tabs
                              channel-> channel outer tabs, frequency inner tabs
                              freq   -> frequency outer tabs, channel inner tabs

Axes:
  --x NAME                  Override x-axis dim/coord (default: auto-detect).
  --y NAME                  Override y-axis dim/coord (default: auto-detect).
  --no-flip                 Disable automatic y-axis inversion for range/depth axes.

Appearance:
  --vmin FLOAT              Lower color limit.
  --vmax FLOAT              Upper color limit.
  --cmap NAME               Initial colormap name (default: inferno).
  --width INT               Minimum plot width in px; stretches beyond this (default: 800).
  --height INT              Plot height (default: 450).
  --toolbar STR             Toolbar: above/below/left/right/disable (default: above).
  --no-hover                Disable hover tooltip overlay.
  --no-crosshair            Disable crosshair cursor.
  --no-cmap-picker          Disable the interactive colormap picker in the HTML.
  --no-log                  Disable the copyable data-summary log panel.

Drawing & annotation:
  --no-draw                 Disable the freehand/polyline/region drawing tools.

Subsetting / performance:
  --decimate INT            Take every Nth sample along x-axis (default: 1).
  --ymin FLOAT              Crop lower y-limit.
  --ymax FLOAT              Crop upper y-limit.

Output:
  -o, --output_path PATH    Output HTML path (default: <stem>_plot.html).
  --no-overwrite            Fail if output already exists.
  --quiet                   Suppress info logs; still prints final path.
  -h, --help                Show this help and exit.
"""
    print(help_text.strip())


def _configure_logging(quiet: bool) -> None:
    """Replace the default suppression sink with a user-visible one.
    Standard logging stays fully disabled."""
    logger.remove()
    if quiet:
        logger.add(sys.stderr, level="WARNING", backtrace=False, diagnose=False)
    else:
        logger.add(sys.stderr, level="INFO", backtrace=True, diagnose=False)


def _read_input_path_from_stdin() -> Optional[str]:
    if sys.stdin.isatty():
        return None
    token = sys.stdin.readline().strip()
    return token or None


def _coord_to_str(val: Any) -> str:
    try:
        if hasattr(val, "item"):
            val = val.item()
    except Exception:
        pass
    if isinstance(val, bytes):
        try:
            return val.decode("utf-8", errors="replace")
        except Exception:
            return repr(val)
    return str(val)


def _detect_axis(ds: xr.Dataset, candidates: Tuple[str, ...], fallback_index: int) -> str:
    """
    Find the first matching candidate as a dim, coordinate, OR data variable.
    Echopype stores `echo_range` as a data_var (not a coord), so without the
    data_vars check, axis detection falls through to `range_sample` integer
    indices — clicks then return sample indices, polygons get exported as
    if they were metres, and downstream masking fails silently.
    """
    for c in candidates:
        if c in ds.dims or c in ds.coords or c in ds.data_vars:
            return c
    dims = list(ds.dims)
    if dims:
        return dims[min(fallback_index, len(dims) - 1)]
    raise ValueError("Dataset has no dimensions; cannot detect axes.")


def _detect_axes(ds: xr.Dataset) -> Tuple[str, str]:
    x_name = _detect_axis(ds, ("ping_time", "time", "ping", "profile_time"), fallback_index=0)
    # Prefer metre-valued y over integer-index range_sample whenever possible.
    # Order: depth (post-aa-depth) > echo_range (Echopype default) > range > range_meter
    #        > range_sample (last resort, integer indices) > depth-as-coord fallback.
    y_name = _detect_axis(
        ds,
        ("depth", "echo_range", "range_meter", "range", "range_sample"),
        fallback_index=1,
    )
    return x_name, y_name


def _warn_range_sample_y(ds: xr.Dataset) -> None:
    """Emit a one-line warning when y-axis is range_sample (integer indices).

    Polygons drawn against an integer-index y will be exported with those
    indices treated as metres, and aa-evl/aa-evr will silently produce empty
    masks. Tell the user how to fix the upstream pipeline or override --y.
    """
    has_metre = any(
        n in ds.coords or n in ds.data_vars
        for n in ("depth", "echo_range", "range_meter", "range")
    )
    suggestion = (
        "Pass --y depth or --y echo_range to use that axis instead."
        if has_metre
        else "Run the data through aa-depth first to add a metre-valued depth coord."
    )
    logger.warning(
        "Y-axis resolved to 'range_sample' (integer sample indices, NOT metres). "
        "Drawn polygons exported via the EVL/EVR buttons will be in sample-index "
        f"units and will produce empty masks in aa-evl/aa-evr. {suggestion}"
    )


def _ensure_y_axis_coord(
    ds: xr.Dataset,
    da: xr.DataArray,
    y_name: str,
    x_name: str,
) -> Tuple[xr.DataArray, str]:
    """
    Ensure `y_name` is a 1-D coordinate on `da` so hvplot.quadmesh can use it
    AND so that drawing-tool click coordinates are in the same units as the
    user sees on the y-axis.

    If y_name is a data_var (e.g. Echopype's `echo_range`) or a 2-D coord
    (e.g. depth varying per ping), reduce it to a 1-D vector along the depth
    dimension and assign it as a coord on da.

    Falls back gracefully: if nothing can be done, returns da unchanged with
    its original dim-coord. Logs a warning so the user knows.
    """
    # Already a 1-D coord on da?
    if y_name in da.coords and da.coords[y_name].ndim == 1:
        if y_name == "range_sample":
            _warn_range_sample_y(ds)
            return da, y_name
        # Promote to dim coordinate if it's a non-dim aux coord on a single
        # axis. Avoids the holoviews "squeeze non-singleton axis" error that
        # occurs when both the integer dim coord (range_sample) and a metre
        # aux coord (depth) share the same axis.
        coord_da = da.coords[y_name]
        if y_name not in da.dims and len(coord_da.dims) == 1:
            host_dim = coord_da.dims[0]
            if host_dim in da.dims and host_dim != y_name:
                try:
                    da = da.swap_dims({host_dim: y_name})
                except Exception as exc:
                    logger.debug(f"swap_dims early-return fallback for '{y_name}': {exc}")
        return da, y_name

    # Find the source variable in the dataset
    src = None
    if y_name in da.coords:
        src = da.coords[y_name]
    elif y_name in ds.coords:
        src = ds[y_name]
    elif y_name in ds.data_vars:
        src = ds[y_name]

    if src is None:
        # y_name is just a dim name with no values (rare).
        if y_name in da.dims and y_name == "range_sample":
            _warn_range_sample_y(ds)
        return da, y_name

    # Reduce src to 1-D along the depth dim
    depth_dim = None
    for d in src.dims:
        if d in da.dims and d != x_name:
            depth_dim = d
            break

    if depth_dim is None:
        return da, y_name

    # If src varies with x (e.g. echo_range is (channel, ping_time, range_sample)),
    # collapse non-depth dims by taking the first index. This is a standard echogram
    # display approximation; per-ping depth variation is small at the cell level.
    reduced = src
    for d in list(reduced.dims):
        if d == depth_dim:
            continue
        try:
            reduced = reduced.isel({d: 0})
        except Exception:
            pass
    # Drop any remaining non-depth-dim coordinates that came along
    for c in list(reduced.coords):
        if c != depth_dim and c in reduced.coords:
            try:
                reduced = reduced.drop_vars(c)
            except Exception:
                pass

    if reduced.ndim != 1:
        # Could not reduce cleanly; fall back to original dim
        logger.warning(
            f"Could not reduce '{y_name}' to a 1-D depth axis; falling back to '{depth_dim}'."
        )
        return da, depth_dim

    # Assign reduced values as a coord on da's depth dim, then SWAP it onto
    # the dim so it becomes the dimension coordinate. Holoviews/hvplot's
    # quadmesh chokes when there's a dim coordinate AND a non-dim aux coord
    # on the same axis (it tries to squeeze a non-singleton axis during
    # canonicalization). Swapping makes the metre values the canonical axis.
    try:
        vals = np.asarray(reduced.values)
        da = da.assign_coords({y_name: (depth_dim, vals)})
        if y_name != depth_dim:
            try:
                da = da.swap_dims({depth_dim: y_name})
            except Exception as exc:
                # If swap fails (e.g. duplicate values), fall back to the
                # plain assign-coord behavior. hvplot may still work if
                # it picks the right coord.
                logger.debug(f"swap_dims fallback for '{y_name}': {exc}")
    except Exception as exc:
        logger.warning(f"Could not promote '{y_name}' to a coord on '{depth_dim}': {exc}")
        return da, depth_dim

    return da, y_name


def _should_flip_y(y_name: str) -> bool:
    return y_name.lower() in _DEPTH_RANGE_NAMES


def _ensure_variable(ds: xr.Dataset, var: Optional[str]) -> str:
    if var:
        if var not in ds.data_vars:
            raise ValueError(f"Variable '{var}' not found. Available: {list(ds.data_vars)}")
        return var
    if "Sv" in ds.data_vars:
        return "Sv"
    if len(ds.data_vars) == 0:
        raise ValueError("No data variables found to plot.")
    return list(ds.data_vars)[0]


def _downsample_da(da: xr.DataArray, x_name: str, step: int) -> xr.DataArray:
    if step <= 1:
        return da
    if x_name not in da.dims:
        return da
    return da.isel({x_name: slice(0, None, step)})


def _apply_ylim(da: xr.DataArray, y_name: str, ymin: Optional[float], ymax: Optional[float]) -> xr.DataArray:
    if ymin is None and ymax is None:
        return da
    if y_name in da.coords:
        coord = da[y_name]
        lo = ymin if ymin is not None else float(coord.min())
        hi = ymax if ymax is not None else float(coord.max())
        try:
            return da.sel({y_name: slice(lo, hi)})
        except Exception:
            return da
    return da


def _nearest_index(coord: xr.DataArray, target: float) -> int:
    return int(abs(coord - target).argmin().item())


def _get_freq_coord_for_channel(ds: xr.Dataset, chan_dim: str) -> Optional[xr.DataArray]:
    if "frequency_nominal" not in ds:
        return None
    try:
        f = ds["frequency_nominal"]
        if chan_dim in f.dims:
            return f
    except Exception:
        return None
    return None


def _y_label(y_name: str) -> str:
    nice = {
        "echo_range": "Range (m)", "range": "Range (m)",
        "range_meter": "Range (m)", "range_m": "Range (m)",
        "depth": "Depth (m)", "range_sample": "Range sample", "range_bin": "Range bin",
    }
    return nice.get(y_name, y_name)


def _x_label(x_name: str) -> str:
    nice = {
        "ping_time": "Ping time", "time": "Time",
        "ping": "Ping #", "profile_time": "Profile time",
    }
    return nice.get(x_name, x_name)


# ===========================================================================
#  CATEGORICAL DETECTION (for cluster labels, masks, and similar)
# ===========================================================================

def _is_categorical(da: xr.DataArray, finite_vals: np.ndarray) -> bool:
    """
    Decide whether a variable should be summarized categorically.

    Triggers as categorical when:
      - dtype is integer/unsigned/bool, OR
      - dtype is float but every finite value is integer-valued AND there are
        fewer than ~50 unique values. This catches KMeans labels, region masks,
        and similar discrete data stored as float (a common xarray quirk).
    """
    if da.dtype.kind in ("i", "u", "b"):
        return True
    if finite_vals.size == 0:
        return False
    # Subsample for the integer-valued check; full scan for unique-count below.
    sample_size = min(50_000, finite_vals.size)
    step = max(1, finite_vals.size // sample_size)
    sample = finite_vals[::step]
    try:
        if np.all(np.equal(np.mod(sample, 1.0), 0.0)):
            unique_count = int(np.unique(finite_vals).size)
            if unique_count < 50:
                return True
    except Exception:
        pass
    return False


# ===========================================================================
#  DATA SUMMARY PANEL
# ===========================================================================

_SIDEBAR_CSS = """\
<style>
.aa-sidebar {
    border-radius: 8px;
    font-family: 'Menlo', 'Consolas', 'DejaVu Sans Mono', monospace;
    font-size: 0.78em;
    color: #1e293b;
    overflow-y: auto;
    overflow-x: hidden;
    width: 100%;
    user-select: text;
    cursor: text;
    line-height: 1.6;
    background: #f8fafc;
    border: 1px solid #cbd5e1;
}
.aa-sidebar-title {
    background: linear-gradient(135deg, #e0f2fe 0%, #f0f9ff 100%);
    color: #0369a1;
    padding: 10px 14px;
    font-weight: 700;
    font-size: 1.05em;
    letter-spacing: 0.04em;
    border-radius: 8px 8px 0 0;
    border-bottom: 1px solid #bae6fd;
    user-select: none;
    display: flex;
    align-items: center;
    gap: 8px;
}
.aa-sidebar-title svg { flex-shrink: 0; }
.aa-sections-grid { display: flex; flex-wrap: wrap; gap: 0; }
.aa-section {
    padding: 8px 14px 4px 14px;
    min-width: 260px;
    flex: 1 1 260px;
    box-sizing: border-box;
}
.aa-section.aa-section-pipeline {
    background: #fef3c7;
    border-left: 3px solid #f59e0b;
}
.aa-section.aa-section-pipeline .aa-section-head { color: #78350f; }
.aa-section.aa-section-varinfo {
    background: #ede9fe;
    border-left: 3px solid #8b5cf6;
}
.aa-section.aa-section-varinfo .aa-section-head { color: #4c1d95; }
.aa-section-head {
    color: #475569;
    font-weight: 600;
    font-size: 0.9em;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    border-bottom: 1px solid #e2e8f0;
    padding-bottom: 4px;
    margin-bottom: 5px;
}
.aa-row { display: flex; justify-content: space-between; padding: 1px 0; }
.aa-key { color: #94a3b8; white-space: nowrap; padding-right: 8px; }
.aa-val { color: #334155; text-align: right; word-break: break-all; }
.aa-val-em { color: #0369a1; text-align: right; font-weight: 600; }
.aa-chan-row { padding: 1px 0; display: flex; gap: 6px; }
.aa-chan-idx { color: #94a3b8; min-width: 24px; }
.aa-chan-name { color: #1e293b; }
.aa-chan-freq { color: #6366f1; margin-left: auto; }
.aa-cluster-row {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 1px 0;
}
.aa-cluster-label { color: #94a3b8; min-width: 80px; }
.aa-cluster-bar-wrap {
    flex: 1;
    background: #e2e8f0;
    border-radius: 3px;
    height: 10px;
    overflow: hidden;
    min-width: 60px;
}
.aa-cluster-bar { height: 100%; background: linear-gradient(90deg, #0369a1, #0ea5e9); }
.aa-cluster-count { color: #334155; min-width: 90px; text-align: right; }
.aa-divider { border: none; border-top: 1px solid #e2e8f0; margin: 4px 0; }
.aa-copy-btn {
    background: #f1f5f9;
    border: 1px solid #cbd5e1;
    border-radius: 5px;
    color: #475569;
    padding: 5px 12px;
    margin: 8px 14px 10px 14px;
    cursor: pointer;
    font-size: 0.9em;
    font-family: inherit;
    transition: all 0.15s;
    max-width: 220px;
    text-align: center;
}
.aa-copy-btn:hover { background: #e2e8f0; color: #1e293b; border-color: #94a3b8; }
.aa-details {
    margin: 4px 14px 8px 14px;
    border: 1px solid #e2e8f0;
    border-radius: 5px;
    padding: 6px 10px;
    background: #ffffff;
}
.aa-details summary {
    cursor: pointer;
    color: #475569;
    font-weight: 600;
    font-size: 0.86em;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    user-select: none;
    outline: none;
}
.aa-details summary:hover { color: #0369a1; }
.aa-details-body { margin-top: 6px; padding-top: 6px; border-top: 1px solid #f1f5f9; }
</style>
"""

_CLIPBOARD_SVG = (
    '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" '
    'stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">'
    '<rect x="9" y="9" width="13" height="13" rx="2" ry="2"/>'
    '<path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/>'
    '</svg>'
)

# Robust clipboard JS.
_COPY_JS_TEMPLATE = """\
(function(btn) {{
    var text = {text_json};
    function onSuccess() {{
        var orig = btn.innerText;
        btn.innerText = 'Copied!';
        btn.style.color = '#34d399';
        setTimeout(function() {{ btn.innerText = orig; btn.style.color = ''; }}, 1800);
    }}
    function onFail() {{
        var orig = btn.innerText;
        btn.innerText = 'Copy failed \u2014 select manually';
        btn.style.color = '#f87171';
        setTimeout(function() {{ btn.innerText = orig; btn.style.color = ''; }}, 2500);
    }}
    function fallbackCopy(t) {{
        var ta = document.createElement('textarea');
        ta.value = t;
        ta.style.cssText = 'position:fixed;top:0;left:0;width:1px;height:1px;opacity:0;';
        document.body.appendChild(ta);
        ta.focus(); ta.select();
        try {{
            var ok = document.execCommand('copy');
            document.body.removeChild(ta);
            ok ? onSuccess() : onFail();
        }} catch(e) {{ document.body.removeChild(ta); onFail(); }}
    }}
    if (window.isSecureContext && navigator.clipboard && navigator.clipboard.writeText) {{
        navigator.clipboard.writeText(text).then(onSuccess, function() {{ fallbackCopy(text); }});
    }} else {{
        fallbackCopy(text);
    }}
}})(this);
"""


def _esc(s: str) -> str:
    """HTML-escape a string for safe insertion into the summary panel."""
    return (
        s.replace("&", "&amp;")
         .replace("<", "&lt;")
         .replace(">", "&gt;")
    )


def _html_row(key: str, val: str, em: bool = False) -> str:
    cls = "aa-val-em" if em else "aa-val"
    return f'<div class="aa-row"><span class="aa-key">{_esc(key)}</span><span class="{cls}">{_esc(val)}</span></div>'


def _format_attr_value(v: Any) -> str:
    """Format an xarray attr value for display, truncating overly long strings."""
    if isinstance(v, (bytes, bytearray)):
        try:
            v = v.decode("utf-8", errors="replace")
        except Exception:
            v = repr(v)
    if isinstance(v, (list, tuple, np.ndarray)):
        try:
            arr = np.asarray(v)
            if arr.size <= 10:
                return ", ".join(_coord_to_str(x) for x in arr.tolist())
            return f"[{arr.size} items: " + ", ".join(_coord_to_str(x) for x in arr.flat[:5]) + ", ...]"
        except Exception:
            pass
    s = str(v)
    if len(s) > 200:
        s = s[:200] + " ..."
    return s


def _build_data_log(
    ds: xr.Dataset,
    var: str,
    x_name: str,
    y_name: str,
    flip_y: bool,
) -> pn.pane.HTML:
    sections: list[str] = []
    sections.append(f'<div class="aa-sidebar-title">{_CLIPBOARD_SVG} Data Summary</div>')

    # ------------------------------------------------------------------
    # Source
    # ------------------------------------------------------------------
    src = ds.encoding.get("source", "(in-memory)")
    src_short = Path(src).name if src != "(in-memory)" else src
    sec = '<div class="aa-section"><div class="aa-section-head">Source</div>'
    sec += _html_row("file", src_short)
    sec += _html_row("variable", var, em=True)
    sec += _html_row("dtype", str(ds[var].dtype))
    sec += _html_row("x-axis", x_name)
    sec += _html_row("y-axis", f"{y_name} \u2195 inverted" if flip_y else y_name)
    sec += '</div>'
    sections.append(sec)

    attrs = ds.attrs

    # ------------------------------------------------------------------
    # Pipeline provenance — any aa_* attrs left by upstream tools.
    # This is the key context for KMeans / EVL / EVR / depth / etc:
    # the parameters that produced this file live here.
    # ------------------------------------------------------------------
    aa_attrs = {k: v for k, v in attrs.items() if k.startswith("aa_") or k.lower() == "history"}
    if aa_attrs:
        sec = '<div class="aa-section aa-section-pipeline"><div class="aa-section-head">Pipeline / Provenance</div>'
        for k in sorted(aa_attrs.keys()):
            sec += _html_row(k, _format_attr_value(aa_attrs[k]),
                             em=(k == "aa_tool"))
        sec += '</div>'
        sections.append(sec)

    # ------------------------------------------------------------------
    # Curated common metadata
    # ------------------------------------------------------------------
    interesting_keys = [
        ("sonar_model", "sonar"), ("survey_name", "survey"), ("title", "title"),
        ("institution", "institution"), ("platform_name", "platform"),
        ("instrument_type", "instrument"), ("date_created", "created"),
        ("time_coverage_start", "time start"), ("time_coverage_end", "time end"),
    ]
    attr_rows = [
        _html_row(lbl, _format_attr_value(attrs[ak]))
        for ak, lbl in interesting_keys
        if ak in attrs and attrs[ak] not in (None, "", b"")
    ]
    if attr_rows:
        sec = '<div class="aa-section"><div class="aa-section-head">Metadata</div>'
        sec += "".join(attr_rows) + '</div>'
        sections.append(sec)

    # ------------------------------------------------------------------
    # Dimensions
    # ------------------------------------------------------------------
    da = ds[var]
    sec = '<div class="aa-section"><div class="aa-section-head">Dimensions</div>'
    for d, s in da.sizes.items():
        sec += _html_row(d, f"{s:,}")
    sec += '</div>'
    sections.append(sec)

    # ------------------------------------------------------------------
    # Coord ranges
    # ------------------------------------------------------------------
    range_rows = []
    for cname in da.dims:
        if cname in ds.coords:
            c = ds[cname]
            try:
                cmin = _coord_to_str(c.min().values)
                cmax = _coord_to_str(c.max().values)
                if len(cmin) > 26:
                    cmin = cmin[:19]
                if len(cmax) > 26:
                    cmax = cmax[:19]
                range_rows.append(_html_row(cname, f"{cmin} \u2192 {cmax}"))
            except Exception:
                range_rows.append(_html_row(cname, "(n/a)"))
    if range_rows:
        sec = '<div class="aa-section"><div class="aa-section-head">Coord Ranges</div>'
        sec += "".join(range_rows) + '</div>'
        sections.append(sec)

    # ------------------------------------------------------------------
    # Channels
    # ------------------------------------------------------------------
    chan_dim = "channel" if "channel" in da.dims else None
    f_on_chan = None
    if chan_dim:
        ccoord = ds[chan_dim]
        if "frequency_nominal" in ds:
            f = ds["frequency_nominal"]
            if chan_dim in f.dims:
                f_on_chan = f

        sec = '<div class="aa-section"><div class="aa-section-head">Channels</div>'
        for ci in range(ccoord.size):
            ch_str = _coord_to_str(ccoord.isel({chan_dim: ci}).values)
            if len(ch_str) > 30:
                ch_str = "\u2026" + ch_str[-28:]
            freq_html = ""
            if f_on_chan is not None:
                fv = f_on_chan.isel({chan_dim: ci}).values
                try:
                    fv_num = float(fv)
                    freq_html = (
                        f'<span class="aa-chan-freq">{fv_num / 1000:.0f} kHz</span>'
                        if fv_num >= 1000
                        else f'<span class="aa-chan-freq">{fv_num:.0f} Hz</span>'
                    )
                except Exception:
                    freq_html = f'<span class="aa-chan-freq">{_esc(_coord_to_str(fv))}</span>'
            sec += (
                f'<div class="aa-chan-row">'
                f'<span class="aa-chan-idx">[{ci}]</span>'
                f'<span class="aa-chan-name">{_esc(ch_str)}</span>'
                f'{freq_html}</div>'
            )
        sec += '</div>'
        sections.append(sec)

    # ------------------------------------------------------------------
    # Active Variable Info — variable-level attrs (units, long_name,
    # _FillValue, plus any aa_* / kmeans / clustering parameters set
    # by upstream tools on the variable itself).
    # ------------------------------------------------------------------
    var_attrs = {k: v for k, v in da.attrs.items()}
    if var_attrs:
        sec = f'<div class="aa-section aa-section-varinfo"><div class="aa-section-head">{_esc(var)} Attributes</div>'
        for k in sorted(var_attrs.keys()):
            sec += _html_row(k, _format_attr_value(var_attrs[k]))
        sec += '</div>'
        sections.append(sec)

    # ------------------------------------------------------------------
    # Statistics — categorical or continuous
    # ------------------------------------------------------------------
    finite = np.array([])
    cluster_summary_for_text: list[str] = []  # populated below for plain-text copy
    sec = f'<div class="aa-section"><div class="aa-section-head">{_esc(var)} Statistics</div>'
    try:
        vals = np.asarray(da.values)
        if vals.dtype.kind == "f":
            finite = vals[np.isfinite(vals)]
        else:
            finite = vals.ravel()
        total = vals.size
        nan_count = total - finite.size
        sec += _html_row("samples", f"{total:,}")
        sec += _html_row("NaN / Inf", f"{nan_count:,} ({100 * nan_count / max(total, 1):.1f}%)")

        if finite.size > 0:
            categorical = _is_categorical(da, finite)

            if categorical:
                # Cluster / class summary — for KMeans labels, region masks, etc.
                sec += _html_row("kind", "categorical / discrete", em=True)
                # Compute counts on the FULL data so percentages are exact.
                if vals.dtype.kind == "f":
                    flat = vals.ravel()
                    flat = flat[np.isfinite(flat)]
                else:
                    flat = vals.ravel()
                unique, counts = np.unique(flat, return_counts=True)
                sec += _html_row("classes", f"{unique.size}", em=True)
                sec += '<hr class="aa-divider"/>'
                # Sort by count desc so largest cluster is on top.
                order = np.argsort(-counts)
                # Limit to top 20 to keep panel readable.
                limit = min(20, unique.size)
                max_count = int(counts.max()) if counts.size else 1
                for j in order[:limit]:
                    u = unique[j]
                    c = int(counts[j])
                    pct = 100.0 * c / finite.size
                    label = _coord_to_str(u)
                    if vals.dtype.kind == "f":
                        try:
                            label = str(int(u))
                        except Exception:
                            pass
                    bar_pct = (c / max_count) * 100
                    cluster_summary_for_text.append(f"  class {label}: {c:,} ({pct:.2f}%)")
                    sec += (
                        f'<div class="aa-cluster-row">'
                        f'<span class="aa-cluster-label">class {_esc(label)}</span>'
                        f'<span class="aa-cluster-bar-wrap">'
                        f'<span class="aa-cluster-bar" style="width:{bar_pct:.1f}%"></span>'
                        f'</span>'
                        f'<span class="aa-cluster-count">{c:,} ({pct:.1f}%)</span>'
                        f'</div>'
                    )
                if unique.size > limit:
                    sec += _html_row("(truncated)", f"+ {unique.size - limit} more classes")
            else:
                # Continuous — existing min/max/mean/std/percentiles
                sec += _html_row("kind", "continuous")
                sec += '<hr class="aa-divider"/>'
                sec += _html_row("min", f"{finite.min():.2f}", em=True)
                sec += _html_row("max", f"{finite.max():.2f}", em=True)
                sec += _html_row("mean", f"{finite.mean():.2f}")
                sec += _html_row("std", f"{finite.std():.2f}")
                sec += '<hr class="aa-divider"/>'
                for q in (5, 25, 50, 75, 95):
                    sec += _html_row(f"P{q}", f"{np.percentile(finite, q):.2f}")
    except Exception as exc:
        sec += _html_row("error", str(exc))
    sec += '</div>'
    sections.append(sec)

    # ------------------------------------------------------------------
    # All Variables (just shapes + dtype)
    # ------------------------------------------------------------------
    sec = '<div class="aa-section"><div class="aa-section-head">All Variables</div>'
    for dv in ds.data_vars:
        shape_str = ", ".join(f"{s}" for s in ds[dv].shape)
        dtype_str = str(ds[dv].dtype)
        sec += _html_row(dv, f"({shape_str}) {dtype_str}")
    sec += '</div>'
    sections.append(sec)

    # ------------------------------------------------------------------
    # Collapsible: full dataset attributes (everything not shown above)
    # ------------------------------------------------------------------
    shown_keys = {ak for ak, _ in interesting_keys} | set(aa_attrs.keys())
    other_attrs = {k: v for k, v in attrs.items() if k not in shown_keys}
    if other_attrs:
        det = '<details class="aa-details"><summary>All Dataset Attributes</summary><div class="aa-details-body">'
        for k in sorted(other_attrs.keys()):
            det += _html_row(k, _format_attr_value(other_attrs[k]))
        det += '</div></details>'
    else:
        det = ""

    # ------------------------------------------------------------------
    # Plain-text copy
    # ------------------------------------------------------------------
    plain_lines: list[str] = []
    plain_lines.append(f"aa-plot Data Summary - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    plain_lines.append(f"File: {ds.encoding.get('source', '(in-memory)')}")
    plain_lines.append(f"Variable: {var} (dtype={ds[var].dtype})")
    plain_lines.append(f"Axes: x={x_name}  y={y_name}{' (inverted)' if flip_y else ''}")
    if aa_attrs:
        plain_lines.append("")
        plain_lines.append("Pipeline / Provenance:")
        for k in sorted(aa_attrs.keys()):
            plain_lines.append(f"  {k}: {_format_attr_value(aa_attrs[k])}")
    if var_attrs:
        plain_lines.append("")
        plain_lines.append(f"{var} Attributes:")
        for k in sorted(var_attrs.keys()):
            plain_lines.append(f"  {k}: {_format_attr_value(var_attrs[k])}")
    plain_lines.append("")
    plain_lines.append("Dimensions:")
    for d, s in da.sizes.items():
        plain_lines.append(f"  {d}: {s}")
    if chan_dim:
        plain_lines.append("")
        plain_lines.append("Channels:")
        ccoord = ds[chan_dim]
        for ci in range(ccoord.size):
            ch_str = _coord_to_str(ccoord.isel({chan_dim: ci}).values)
            freq_part = ""
            if f_on_chan is not None:
                fv = f_on_chan.isel({chan_dim: ci}).values
                freq_part = f"  ({_coord_to_str(fv)} Hz)"
            plain_lines.append(f"  [{ci}] {ch_str}{freq_part}")
    try:
        if cluster_summary_for_text:
            plain_lines.append("")
            plain_lines.append(f"{var} Class Counts:")
            plain_lines.extend(cluster_summary_for_text)
        elif finite.size > 0 and not _is_categorical(da, finite):
            plain_lines.append("")
            plain_lines.append(
                f"  {var}: min={finite.min():.4f}  max={finite.max():.4f}  "
                f"mean={finite.mean():.4f}  std={finite.std():.4f}"
            )
            pcts = {q: np.percentile(finite, q) for q in (5, 25, 50, 75, 95)}
            plain_lines.append("  Percentiles: " + "  ".join(f"P{q}={v:.4f}" for q, v in pcts.items()))
    except Exception:
        pass

    text_json = json.dumps("\n".join(plain_lines))
    copy_js = _COPY_JS_TEMPLATE.format(text_json=text_json)
    copy_btn = f'<button class="aa-copy-btn" onclick="{copy_js.strip()}">Copy summary</button>'

    html = (
        f'{_SIDEBAR_CSS}<div class="aa-sidebar">'
        + sections[0]
        + '<div class="aa-sections-grid">'
        + "".join(sections[1:])
        + '</div>'
        + det
        + copy_btn
        + '</div>'
    )
    return pn.pane.HTML(html, sizing_mode="stretch_width")


# ===========================================================================
#  COLORMAP PICKER
# ===========================================================================

def _get_bokeh_palette(name: str, n: int = 256) -> list[str]:
    from bokeh.palettes import all_palettes
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        cmap = plt.get_cmap(name, n)
        return [
            "#%02x%02x%02x" % (int(r * 255), int(g * 255), int(b * 255))
            for r, g, b, _ in (cmap(i) for i in range(n))
        ]
    except Exception:
        pass
    if name in all_palettes:
        biggest = max(all_palettes[name].keys())
        return list(all_palettes[name][biggest])
    from bokeh.palettes import Inferno256
    return list(Inferno256)


def _build_cmap_picker(default_cmap: str) -> pn.pane.Bokeh:
    from bokeh.models import CustomJS, Select as BokehSelect

    palette_map: dict[str, list[str]] = {cm: _get_bokeh_palette(cm, 256) for cm in _CMAP_OPTIONS}
    palettes_json = json.dumps(palette_map)

    js_code = """
    var palettes = JSON.parse(palettes_json);
    var pal = palettes[cb_obj.value];
    if (!pal) return;
    function applyPalette(model, palette) {
        if (!model) return;
        if (model.palette !== undefined) { model.palette = palette; if (model.change) model.change.emit(); }
        if (model.color_mapper && model.color_mapper.palette !== undefined) {
            model.color_mapper.palette = palette;
            if (model.color_mapper.change) model.color_mapper.change.emit();
        }
    }
    try {
        var doc = Bokeh.documents[0];
        var models = doc._all_models;
        if (models) {
            var iter = (typeof models.forEach === 'function')
                ? function(fn){ models.forEach(fn); }
                : (typeof models.values === 'function')
                    ? function(fn){ for (var m of models.values()) fn(m); }
                    : function(fn){ for (var k in models) fn(models[k]); };
            iter(function(m) { applyPalette(m, pal); });
        }
    } catch(e) { console.warn('aa-plot cmap picker:', e); }
    """

    select = BokehSelect(
        title="Colormap",
        value=default_cmap if default_cmap in _CMAP_OPTIONS else _CMAP_OPTIONS[0],
        options=_CMAP_OPTIONS,
        width=180,
    )
    select.js_on_change("value", CustomJS(args={"palettes_json": palettes_json}, code=js_code))
    return pn.pane.Bokeh(select, sizing_mode="fixed")


# ===========================================================================
#  HOVER + CROSSHAIR + TAP (click-to-pin)
# ===========================================================================

def _build_interaction_tools(var: str, x_name: str, y_name: str, pin_div):
    from bokeh.models import HoverTool, CrosshairTool, TapTool, CustomJS

    x_is_time = "time" in x_name.lower()
    y_lbl = _y_label(y_name)
    x_lbl = _x_label(x_name)

    hover = HoverTool(
        tooltips=[
            (var,  f"@{var}{{0.3f}} dB"),
            (x_lbl, "$x{%F %T}" if x_is_time else "$x{0.4f}"),
            (y_lbl, "$y{0.2f} m"),
        ],
        formatters={"$x": "datetime" if x_is_time else "numeral"},
        mode="mouse",
    )

    crosshair = CrosshairTool(line_color="#ffffff", line_alpha=0.5, line_width=1)

    tap_js = """
    if (!cb_data || !cb_data.geometries || cb_data.geometries.length === 0) return;
    var pt = cb_data.geometries[0];
    var x = pt.x, y = pt.y;
    var xStr;
    if (x_is_time) {
        var d = new Date(x);
        xStr = d.toISOString().replace('T', ' ').slice(0, 19) + ' UTC';
    } else {
        xStr = x.toFixed(4);
    }
    pin_div.text = (
        '<div style="font-family:monospace;font-size:0.82em;padding:6px 12px;'
        + 'background:#0f172a;border:1px solid #1e3a5f;border-radius:5px;display:inline-block;">'
        + '<span style="color:#38bdf8;font-weight:700;">&#128205; Pinned</span>'
        + '&nbsp;&nbsp;'
        + '<span style="color:#64748b;">' + x_lbl + ':</span> '
        + '<span style="color:#e2e8f0;">' + xStr + '</span>'
        + '&nbsp;&nbsp;&middot;&nbsp;&nbsp;'
        + '<span style="color:#64748b;">' + y_lbl + ':</span> '
        + '<span style="color:#e2e8f0;">' + y.toFixed(2) + '</span>'
        + '</div>'
    );
    """
    tap = TapTool(
        callback=CustomJS(
            args={"pin_div": pin_div, "x_is_time": x_is_time, "x_lbl": x_lbl, "y_lbl": y_lbl},
            code=tap_js,
        )
    )

    return hover, crosshair, tap


# ===========================================================================
#  DRAWING TOOLS  (freehand / polyline / region polygon)
# ===========================================================================

_DRAW_CSS = """\
<style>
.aa-draw-wrap {
    border-radius: 8px;
    font-family: 'Menlo','Consolas','DejaVu Sans Mono',monospace;
    font-size: 0.80em;
    background: #f8fafc;
    border: 1px solid #cbd5e1;
    overflow: hidden;
}
.aa-draw-title {
    background: linear-gradient(135deg,#f0fdf4 0%,#f8fafc 100%);
    color: #166534;
    padding: 9px 14px;
    font-weight: 700;
    font-size: 1.0em;
    letter-spacing: 0.04em;
    border-bottom: 1px solid #bbf7d0;
    display: flex;
    align-items: center;
    gap: 8px;
}
.aa-draw-body { padding: 10px 14px 12px 14px; }
.aa-draw-legend {
    display: flex;
    flex-wrap: wrap;
    gap: 14px;
    margin-bottom: 10px;
    line-height: 1.5;
}
.aa-draw-legend-item {
    display: flex;
    align-items: center;
    gap: 7px;
    color: #475569;
    font-size: 0.92em;
}
.aa-swatch-freehand {
    width: 26px; height: 3px;
    background: #FFD700;
    border-radius: 2px;
    flex-shrink: 0;
}
.aa-swatch-poly {
    width: 26px; height: 0;
    border-top: 2px dashed #00FFFF;
    flex-shrink: 0;
}
.aa-swatch-region {
    width: 20px; height: 12px;
    background: rgba(68,153,255,0.35);
    border: 2px solid #4499FF;
    border-radius: 3px;
    flex-shrink: 0;
}
.aa-draw-btnrow {
    display: flex;
    flex-wrap: wrap;
    gap: 7px;
    margin-top: 8px;
}
.aa-draw-btnrow + .aa-draw-btnrow { margin-top: 6px; }
.aa-draw-btn {
    background: #f1f5f9;
    border: 1px solid #cbd5e1;
    border-radius: 5px;
    color: #475569;
    padding: 5px 13px;
    cursor: pointer;
    font-size: 0.91em;
    font-family: inherit;
    transition: all 0.14s;
    white-space: nowrap;
}
.aa-draw-btn:hover { background:#e2e8f0; color:#1e293b; border-color:#94a3b8; }
.aa-draw-btn.evl  { border-color:#00BBCC; color:#0369a1; }
.aa-draw-btn.evl:hover  { background:#e0f2fe; border-color:#0284c7; }
.aa-draw-btn.evr  { border-color:#818CF8; color:#4338ca; }
.aa-draw-btn.evr:hover  { background:#eef2ff; border-color:#6366f1; }
.aa-draw-btn.txt  { background:#f8fafc; border-style:dashed; }
.aa-draw-btn.clr  { border-color:#FCA5A5; color:#be123c; }
.aa-draw-btn.clr:hover  { background:#fff1f2; border-color:#fb7185; }
.aa-draw-hint {
    color: #94a3b8;
    font-size: 0.82em;
    margin-top: 9px;
    line-height: 1.55;
}
.aa-draw-hint b { color: #475569; }
.aa-draw-warn {
    margin-top: 8px;
    padding: 6px 10px;
    border-left: 3px solid #f59e0b;
    background: #fef3c7;
    color: #78350f;
    font-size: 0.82em;
    line-height: 1.5;
    border-radius: 0 4px 4px 0;
}
</style>
"""

# Shared JS helpers embedded once in the panel <script> block.
_DRAW_JS_HELPERS = """\
<script>
(function(W){
  W._aaGetModels = function() {
    var out = [];
    try {
      var docs = window.Bokeh && Bokeh.documents;
      if (!docs || !docs.length) return out;
      var m = docs[0]._all_models;
      if (!m) return out;
      if (typeof m.forEach === 'function') { m.forEach(function(v){ if(v) out.push(v); }); }
      else { for (var k in m) { if (m.hasOwnProperty(k)) out.push(m[k]); } }
    } catch(e) { console.warn('aa-plot getModels:', e); }
    return out;
  };

  // ---------------------------------------------------------------
  // Date / time formatters per Echoview spec.
  //
  // EVL/EVR date format: CCYYMMDD       (e.g. "20240315")
  // EVL/EVR time format: HHmmSSssss     (e.g. "1530453205" = 15:30:45.3205)
  //   ssss is fractional seconds in TEN-THOUSANDTHS, written as 4 integer
  //   digits with no decimal point. JavaScript Date only resolves to
  //   milliseconds, so we multiply ms x 10 and zero-pad to fill.
  // ---------------------------------------------------------------
  W._aaEvlDate = function(ms) {
    var d = new Date(ms);
    var p2 = function(n){ return String(n).padStart(2,'0'); };
    return String(d.getUTCFullYear()) + p2(d.getUTCMonth()+1) + p2(d.getUTCDate());
  };
  W._aaEvlTime = function(ms) {
    var d = new Date(ms);
    var p2 = function(n){ return String(n).padStart(2,'0'); };
    var p4 = function(n){ return String(n).padStart(4,'0'); };
    var ssss = d.getUTCMilliseconds() * 10;
    return p2(d.getUTCHours()) + p2(d.getUTCMinutes()) + p2(d.getUTCSeconds()) + p4(ssss);
  };

  W._aaIsTime = function(xs) {
    if (!xs || !xs.length) return false;
    var v = xs[0]; if (Array.isArray(v)) v = v[0];
    return typeof v === 'number' && v > 1e12;
  };

  // Collect sources whose names start with prefix
  W._aaCollect = function(prefix) {
    var result = [];
    _aaGetModels().forEach(function(m) {
      if (m.name && m.name.indexOf(prefix) === 0 && m.data && m.data.xs) {
        for (var i = 0; i < m.data.xs.length; i++) {
          result.push({ xs: m.data.xs[i], ys: m.data.ys[i] });
        }
      }
    });
    return result;
  };

  // ---------------------------------------------------------------
  // Diagnostic summary — collects the bounding ranges of the drawn
  // geometry. Used in the show-text modal so the user can verify
  // the polygon actually overlaps their echogram before they bother
  // running the file through aa-evr/aa-evl.
  // ---------------------------------------------------------------
  W._aaSummariseSegs = function(segs) {
    if (!segs || !segs.length) return null;
    var isTime = _aaIsTime(segs[0].xs);
    var minX = +Infinity, maxX = -Infinity, minY = +Infinity, maxY = -Infinity;
    var totalPts = 0;
    segs.forEach(function(seg) {
      for (var i = 0; i < seg.xs.length; i++) {
        var x = seg.xs[i], y = seg.ys[i];
        if (x < minX) minX = x;
        if (x > maxX) maxX = x;
        if (y < minY) minY = y;
        if (y > maxY) maxY = y;
        totalPts += 1;
      }
    });
    function fmtX(x) {
      if (isTime) {
        var d = new Date(x);
        return d.toISOString().replace('T', ' ').slice(0, 23) + ' UTC';
      }
      return x.toFixed(4);
    }
    return {
      strokeCount: segs.length,
      pointCount:  totalPts,
      isTime:      isTime,
      xMin:        fmtX(minX),
      xMax:        fmtX(maxX),
      yMin:        minY.toFixed(4),
      yMax:        maxY.toFixed(4),
      yMinRaw:     minY,
      yMaxRaw:     maxY
    };
  };

  // Format the summary as an HTML block for the modal header.
  W._aaSummaryHtml = function(summary, kindLabel) {
    if (!summary) return '';
    var depthHint = '';
    // Heuristic: if the y range looks like integer indices (0..N with small N),
    // the user probably drew on a range_sample axis — flag this.
    if (summary.yMaxRaw < 2000 && summary.yMaxRaw > 0 &&
        Math.abs(summary.yMaxRaw - Math.round(summary.yMaxRaw)) < 0.001 &&
        Math.abs(summary.yMinRaw - Math.round(summary.yMinRaw)) < 0.001) {
      // Could be either depth-in-metres or range-sample — surface a warning.
      depthHint = (
        '<div style="margin-top:6px;padding:6px 8px;border-left:3px solid #f59e0b;'
        + 'background:#fef3c7;color:#78350f;font-size:0.92em;line-height:1.4;">'
        + '<b>Heads-up:</b> the y values look integer-like. If your plot used '
        + '<code>range_sample</code> indices instead of metre-valued depth, '
        + 'aa-evr/aa-evl will not find a match. Re-plot with the depth coordinate '
        + '(or pass <code>--y depth</code>) before drawing.'
        + '</div>'
      );
    }
    var timeHint = summary.isTime
      ? ''
      : ('<div style="margin-top:6px;padding:6px 8px;border-left:3px solid #ef4444;'
         + 'background:#fee2e2;color:#7f1d1d;font-size:0.92em;line-height:1.4;">'
         + '<b>Cannot export:</b> the x-axis is not time-based. EVL/EVR require ping_time.'
         + '</div>');
    return (
      '<div style="margin:6px 0;padding:8px 10px;background:#eff6ff;'
      + 'border:1px solid #bfdbfe;border-radius:6px;font-size:0.92em;line-height:1.5;color:#1e3a5f;">'
      + '<b>Polygon summary</b> &mdash; verify these match your echogram before importing'
      + '<div style="font-family:Menlo,Consolas,monospace;font-size:0.92em;margin-top:4px;">'
      + '&nbsp;&nbsp;' + kindLabel + ': ' + summary.strokeCount
      + ' (' + summary.pointCount + ' points total)<br>'
      + '&nbsp;&nbsp;time:&nbsp; ' + summary.xMin + '<br>'
      + '&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&rarr;&nbsp; ' + summary.xMax + '<br>'
      + '&nbsp;&nbsp;depth: ' + summary.yMin + ' &rarr; ' + summary.yMax
      + '</div>'
      + timeHint
      + depthHint
      + '</div>'
    );
  };
  //
  // Per Echoview spec
  // (https://support.echoview.com/.../Exporting_line_data.htm):
  //
  //   Line 1:  "EVBD <format_version> <generator_version>"
  //   Line 2:  <total point count>
  //   Lines 3..N+2:  <date> <time> <depth> <line_status>
  //                  fields separated by single space
  //                  line_status: 0=none 1=unverified 2=bad 3=good
  //   Line endings: CR/LF (DOS).
  //
  // Crucially: an EVL file represents EXACTLY ONE LINE — there is no
  // "named line" concept. If the user has drawn multiple freehand strokes
  // and/or polylines, we concatenate all points and sort by time, treating
  // the strokes as pieces of a single boundary (which is the typical
  // workflow when refining a bottom or surface line).
  // ---------------------------------------------------------------
  W.aaGenerateEvl = function() {
    var segs = _aaCollect('aa_freehand_').concat(_aaCollect('aa_lines_'));
    segs = segs.filter(function(s){ return s.xs && s.xs.length > 0; });
    if (!segs.length) return null;
    if (!_aaIsTime(segs[0].xs)) {
      return { error: 'EVL export requires a time-based x-axis (e.g. ping_time). Re-plot with a time x-axis to enable export.' };
    }

    // Concatenate all points from all strokes, sort by time.
    var pts = [];
    segs.forEach(function(seg) {
      for (var i = 0; i < seg.xs.length; i++) {
        pts.push({ t: seg.xs[i], d: seg.ys[i] });
      }
    });
    pts.sort(function(a, b) { return a.t - b.t; });

    var LINE_STATUS = '1';  // 1 = unverified — safe default for user-drawn lines
    var rows = [];
    rows.push('EVBD 3 aa-plot-1.0');
    rows.push(String(pts.length));
    pts.forEach(function(p) {
      rows.push(
        _aaEvlDate(p.t) + ' ' + _aaEvlTime(p.t) + ' ' +
        p.d.toFixed(4) + ' ' + LINE_STATUS
      );
    });
    return rows.join('\\r\\n') + '\\r\\n';
  };

  // ---------------------------------------------------------------
  // EVR — Echoview 2D Region Definition File
  //
  // Per Echoview spec
  // (https://support.echoview.com/.../2D_Region_definition_file_format.htm):
  //
  //   Line 1:  "EVRG <format_version=7> <generator_version>"
  //   Line 2:  <region count>
  //
  //   Then for each region:
  //     <blank line>
  //     <header line, 13 space-separated tokens, CR/LF>:
  //         13 <pcount> <id> 0 <ctype> -1 1
  //         <left_date> <left_time> <top_depth>
  //         <right_date> <right_time> <bot_depth>
  //     <#notes lines, CR/LF>
  //     <#detection-settings lines, CR/LF>
  //     <region classification, CR/LF>
  //     <points line: date1 time1 depth1 date2 time2 depth2 ... <region_type>>
  //     <region name, CR/LF>
  //
  // Region creation type 2 = polygon tool (matches what the user draws).
  // Region type 1 = analysis (the typical use for polygon ROIs).
  // ---------------------------------------------------------------
  W.aaGenerateEvr = function() {
    var segs = _aaCollect('aa_regions_');
    segs = segs.filter(function(s){ return s.xs && s.xs.length > 0; });
    if (!segs.length) return null;
    if (!_aaIsTime(segs[0].xs)) {
      return { error: 'EVR export requires a time-based x-axis (e.g. ping_time). Re-plot with a time x-axis to enable export.' };
    }

    var REGION_STRUCT = '13';
    var SELECTED      = '0';
    var CREATION_TYPE = '2';   // polygon tool
    var DUMMY         = '-1';
    var REGION_TYPE   = '1';   // analysis

    var rows = [];
    rows.push('EVRG 7 aa-plot-1.0');
    rows.push(String(segs.length));

    segs.forEach(function(seg, idx) {
      var rid = idx + 1;
      var n = seg.xs.length;

      // Bounding rectangle
      var minX = seg.xs[0], maxX = seg.xs[0];
      var minY = seg.ys[0], maxY = seg.ys[0];
      for (var i = 1; i < n; i++) {
        if (seg.xs[i] < minX) minX = seg.xs[i];
        if (seg.xs[i] > maxX) maxX = seg.xs[i];
        if (seg.ys[i] < minY) minY = seg.ys[i];
        if (seg.ys[i] > maxY) maxY = seg.ys[i];
      }

      rows.push('');  // blank-line region separator

      // Header line: 13 tokens (date and time count as separate tokens
      // so the bounding-rectangle x-coords are 2 tokens each).
      rows.push([
        REGION_STRUCT, String(n), String(rid),
        SELECTED, CREATION_TYPE, DUMMY, '1',
        _aaEvlDate(minX), _aaEvlTime(minX), minY.toFixed(4),
        _aaEvlDate(maxX), _aaEvlTime(maxX), maxY.toFixed(4)
      ].join(' '));

      rows.push('0');                       // # notes lines
      rows.push('0');                       // # detection-settings lines
      rows.push('Unclassified regions');    // region classification

      // All polygon points on ONE line, region type at the end.
      var tokens = [];
      for (var i = 0; i < n; i++) {
        tokens.push(_aaEvlDate(seg.xs[i]));
        tokens.push(_aaEvlTime(seg.xs[i]));
        tokens.push(seg.ys[i].toFixed(10));
      }
      tokens.push(REGION_TYPE);
      rows.push(tokens.join(' '));

      rows.push('aa-plot region ' + rid);   // region name
    });

    return rows.join('\\r\\n') + '\\r\\n';
  };

  // ---------------------------------------------------------------
  // Delivery: try a real download, fall back to modal.
  // ---------------------------------------------------------------
  W._aaTryDownload = function(content, filename) {
    // Strategy 1: data: URI anchor click — works in most contexts but
    // is BLOCKED in JupyterLab's default sandboxed iframe (no allow-downloads).
    // We can't detect this synchronously, so on Jupyter the user sees nothing
    // happen — which is why we also expose explicit "Show text" buttons.
    try {
      var uri = 'data:text/plain;charset=utf-8,' + encodeURIComponent(content);
      var a = document.createElement('a');
      a.href = uri;
      a.download = filename;
      a.style.display = 'none';
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      return;
    } catch(e1) { /* fall through */ }

    // Strategy 2: open data: URI in a new tab
    try {
      var uri2 = 'data:text/plain;charset=utf-8,' + encodeURIComponent(content);
      var w = window.open(uri2, '_blank');
      if (w) return;
    } catch(e2) { /* fall through */ }

    // Strategy 3: modal
    _aaShowTextModal(content, filename, 'Download blocked - copy text below');
  };

  // ---------------------------------------------------------------
  // Always-show modal — used by the "Show ... text" buttons.
  // Works in any environment, including sandboxed iframes.
  //
  // summary_html : optional HTML to inject between the title and the
  //                copy-paste textarea (used to display the polygon's
  //                time/depth ranges so the user can sanity-check).
  // ---------------------------------------------------------------
  W._aaShowTextModal = function(content, filename, title_text, summary_html) {
    var overlay = document.createElement('div');
    overlay.style.cssText = [
      'position:fixed','top:0','left:0','width:100%','height:100%',
      'background:rgba(15,23,42,0.72)','z-index:99999',
      'display:flex','align-items:center','justify-content:center'
    ].join(';');

    var box = document.createElement('div');
    box.style.cssText = [
      'background:#f8fafc','border:1px solid #cbd5e1','border-radius:10px',
      'padding:18px 20px','max-width:760px','width:92%','max-height:80vh',
      'display:flex','flex-direction:column','gap:10px',
      'font-family:Menlo,Consolas,DejaVu Sans Mono,monospace','font-size:0.82em'
    ].join(';');

    var title = document.createElement('div');
    title.style.cssText = 'font-weight:700;color:#0369a1;font-size:1.05em;';
    title.textContent = title_text + ' (suggested filename: ' + filename + ')';

    var hint = document.createElement('div');
    hint.style.cssText = 'color:#64748b;font-size:0.92em;line-height:1.5;';
    hint.innerHTML = (
      'Select-all and copy the text below, then paste into a plain text file ' +
      'and save as <b>' + filename + '</b>.<br>' +
      '<span style="color:#94a3b8;">In JupyterLab: File &rarr; New &rarr; Text File &rarr; paste &rarr; rename.</span>'
    );

    var ta = document.createElement('textarea');
    ta.value = content;
    ta.style.cssText = [
      'width:100%','min-height:240px','resize:vertical',
      'background:#0f172a','color:#e2e8f0',
      'border:1px solid #334155','border-radius:6px',
      'padding:8px 10px','font-family:inherit','font-size:1em',
      'box-sizing:border-box'
    ].join(';');
    ta.readOnly = false;
    ta.spellcheck = false;

    var btnRow = document.createElement('div');
    btnRow.style.cssText = 'display:flex;gap:8px;flex-wrap:wrap;';

    var copyBtn = document.createElement('button');
    copyBtn.textContent = 'Copy to clipboard';
    copyBtn.style.cssText = [
      'background:#e0f2fe','border:1px solid #7dd3fc','border-radius:5px',
      'color:#0369a1','padding:5px 14px','cursor:pointer','font-family:inherit'
    ].join(';');
    copyBtn.onclick = function() {
      ta.select();
      try {
        if (navigator.clipboard && window.isSecureContext) {
          navigator.clipboard.writeText(content).then(function(){
            copyBtn.textContent = 'Copied!'; copyBtn.style.color='#16a34a';
            setTimeout(function(){ copyBtn.textContent='Copy to clipboard'; copyBtn.style.color=''; }, 1800);
          }, function() {
            document.execCommand('copy');
            copyBtn.textContent = 'Copied!'; copyBtn.style.color='#16a34a';
            setTimeout(function(){ copyBtn.textContent='Copy to clipboard'; copyBtn.style.color=''; }, 1800);
          });
        } else {
          document.execCommand('copy');
          copyBtn.textContent = 'Copied!'; copyBtn.style.color='#16a34a';
          setTimeout(function(){ copyBtn.textContent='Copy to clipboard'; copyBtn.style.color=''; }, 1800);
        }
      } catch(e) { copyBtn.textContent = 'Copy failed'; }
    };

    var dlBtn = document.createElement('button');
    dlBtn.textContent = 'Try download';
    dlBtn.style.cssText = [
      'background:#f0fdf4','border:1px solid #86efac','border-radius:5px',
      'color:#166534','padding:5px 14px','cursor:pointer','font-family:inherit'
    ].join(';');
    dlBtn.onclick = function() {
      try {
        var uri = 'data:text/plain;charset=utf-8,' + encodeURIComponent(content);
        var a = document.createElement('a');
        a.href = uri; a.download = filename; a.style.display = 'none';
        document.body.appendChild(a); a.click(); document.body.removeChild(a);
      } catch(e) { dlBtn.textContent = 'Blocked'; }
    };

    var closeBtn = document.createElement('button');
    closeBtn.textContent = 'Close';
    closeBtn.style.cssText = [
      'background:#f1f5f9','border:1px solid #cbd5e1','border-radius:5px',
      'color:#475569','padding:5px 14px','cursor:pointer','font-family:inherit'
    ].join(';');
    closeBtn.onclick = function() { document.body.removeChild(overlay); };
    overlay.onclick = function(e) { if (e.target === overlay) document.body.removeChild(overlay); };

    btnRow.appendChild(copyBtn);
    btnRow.appendChild(dlBtn);
    btnRow.appendChild(closeBtn);
    box.appendChild(title);
    box.appendChild(hint);
    if (summary_html) {
      var summaryDiv = document.createElement('div');
      summaryDiv.innerHTML = summary_html;
      box.appendChild(summaryDiv);
    }
    box.appendChild(ta);
    box.appendChild(btnRow);
    overlay.appendChild(box);
    document.body.appendChild(overlay);
    setTimeout(function(){ ta.select(); }, 80);
  };

  // ---------------------------------------------------------------
  // Public buttons
  //
  // The generator functions return one of:
  //   - null              : nothing drawn
  //   - { error: "..." }  : drawn but x-axis isn't time-based
  //   - string            : valid EVL/EVR content
  // ---------------------------------------------------------------
  function _handleGenResult(content, kindLabel, onContent) {
    if (content === null) {
      alert('No ' + kindLabel + ' drawn yet.\\nActivate the relevant tool in the echogram toolbar, draw, then try again.');
      return false;
    }
    if (typeof content === 'object' && content && content.error) {
      alert(content.error);
      return false;
    }
    onContent(content);
    return true;
  }

  W.aaExportEvl = function() {
    _handleGenResult(aaGenerateEvl(), 'lines', function(c) {
      _aaTryDownload(c, 'aa_lines.evl');
    });
  };
  W.aaShowEvlText = function() {
    _handleGenResult(aaGenerateEvl(), 'lines', function(c) {
      var segs = _aaCollect('aa_freehand_').concat(_aaCollect('aa_lines_'));
      segs = segs.filter(function(s){ return s.xs && s.xs.length > 0; });
      var summary = _aaSummariseSegs(segs);
      var summaryHtml = _aaSummaryHtml(summary, 'strokes');
      _aaShowTextModal(c, 'aa_lines.evl', 'EVL Line File', summaryHtml);
    });
  };
  W.aaExportEvr = function() {
    _handleGenResult(aaGenerateEvr(), 'regions', function(c) {
      _aaTryDownload(c, 'aa_regions.evr');
    });
  };
  W.aaShowEvrText = function() {
    _handleGenResult(aaGenerateEvr(), 'regions', function(c) {
      var segs = _aaCollect('aa_regions_');
      segs = segs.filter(function(s){ return s.xs && s.xs.length > 0; });
      var summary = _aaSummariseSegs(segs);
      var summaryHtml = _aaSummaryHtml(summary, 'regions');
      _aaShowTextModal(c, 'aa_regions.evr', 'EVR Region File', summaryHtml);
    });
  };

  W.aaClearDraw = function() {
    if (!confirm('Clear all drawn lines and regions?')) return;
    _aaGetModels().forEach(function(m) {
      if (!m.name) return;
      var pfx = m.name;
      if (pfx.indexOf('aa_freehand_') === 0 ||
          pfx.indexOf('aa_lines_') === 0 ||
          pfx.indexOf('aa_regions_') === 0) {
        try {
          m.data = { xs: [], ys: [] };
          if (m.change) m.change.emit();
        } catch(e) {}
      }
    });
  };
})(window);
</script>
"""


def _apply_draw_tools(fig, draw_idx: int) -> None:
    """
    Inject three drawing layers + tools onto an existing Bokeh figure.

    Layers
    ------
    aa_freehand_{i}  MultiLine  gold  #FFD700   FreehandDrawTool
    aa_lines_{i}     MultiLine  cyan  #00FFFF   PolyDrawTool  (double-click to finish)
    aa_regions_{i}   Patches    blue  #4499FF   PolyDrawTool  (double-click to close)

    PolyEditTool is also added so drawn geometry can be edited (shift-click vertex to delete).
    """
    from bokeh.models import (
        FreehandDrawTool, PolyDrawTool, PolyEditTool,
        ColumnDataSource,
    )

    # -- Freehand (gold) -- exports as EVL --
    fh_src = ColumnDataSource(data={"xs": [], "ys": []}, name=f"aa_freehand_{draw_idx}")
    fh_rend = fig.multi_line(
        "xs", "ys", source=fh_src,
        line_color="#FFD700", line_width=2.0, line_alpha=0.92,
        line_cap="round", line_join="round",
    )
    freehand_tool = FreehandDrawTool(renderers=[fh_rend], num_objects=0)
    freehand_tool.description = "Freehand line (-> EVL)"

    # -- Polyline segments (cyan dashed) -- exports as EVL --
    ln_src = ColumnDataSource(data={"xs": [], "ys": []}, name=f"aa_lines_{draw_idx}")
    ln_rend = fig.multi_line(
        "xs", "ys", source=ln_src,
        line_color="#00FFFF", line_width=2.0, line_alpha=0.92,
    )
    ln_rend.glyph.line_dash = [8, 4]
    line_tool = PolyDrawTool(renderers=[ln_rend], num_objects=0)
    line_tool.description = "Polyline segments (-> EVL, dbl-click to finish)"

    # -- Region polygons (blue translucent) -- exports as EVR --
    rg_src = ColumnDataSource(data={"xs": [], "ys": []}, name=f"aa_regions_{draw_idx}")
    rg_rend = fig.patches(
        "xs", "ys", source=rg_src,
        fill_color="#4499FF", fill_alpha=0.18,
        line_color="#4499FF", line_width=2.0, line_alpha=0.92,
    )
    region_tool = PolyDrawTool(renderers=[rg_rend], num_objects=0)
    region_tool.description = "Region polygon (-> EVR, dbl-click to close)"

    # -- Vertex editor -- lets you drag / delete vertices after drawing --
    # Using fig.scatter(marker="circle", size=...) instead of fig.circle(size=...)
    # because the latter was deprecated in Bokeh 3.4.
    try:
        vtx_src = ColumnDataSource(data={"x": [], "y": []})
        vtx_rend = fig.scatter(
            "x", "y", source=vtx_src,
            marker="circle",
            size=9, color="white", fill_alpha=0.85,
            line_color="#64748b", line_width=1.2,
        )
        edit_tool = PolyEditTool(
            renderers=[ln_rend, rg_rend],
            vertex_renderer=vtx_rend,
        )
        edit_tool.description = "Edit vertices (shift-click to delete)"
        fig.add_tools(freehand_tool, line_tool, region_tool, edit_tool)
    except Exception as exc:
        logger.debug(f"PolyEditTool unavailable ({exc}); skipping vertex editor.")
        fig.add_tools(freehand_tool, line_tool, region_tool)


def _build_annotation_panel() -> pn.pane.HTML:
    """
    Build the annotation controls pane.

    Two rows of buttons:
      Row 1: download buttons (work in normal browser tabs)
      Row 2: "Show text" buttons (work everywhere, including JupyterLab sandbox)

    EVL format
      EVBD 3 9.0.120.30842
      <n_lines>
      <line_name>
      -10000.0000 0 0          (bad_data_value  status  editable)
      <n_points>
      YYYYMMDD HHMMSS.ssss     depth_m

    EVR format
      EVBD 3 9.0.120.30842
      <n_regions> 4
      <region_name>
                               (notes)
                               (detection_settings)
      1                        (region type: 1=analysis)
      <n_points>
      YYYYMMDD HHMMSS.ssss     depth_m
    """
    pencil_svg = (
        '<svg width="15" height="15" viewBox="0 0 24 24" fill="none" '
        'stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round">'
        '<path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/>'
        '<path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/>'
        '</svg>'
    )

    html = f"""{_DRAW_CSS}
{_DRAW_JS_HELPERS}
<div class="aa-draw-wrap">
  <div class="aa-draw-title">{pencil_svg} Annotation Tools</div>
  <div class="aa-draw-body">
    <div class="aa-draw-legend">
      <div class="aa-draw-legend-item">
        <div class="aa-swatch-freehand"></div>
        <span><b>Freehand</b> &mdash; activate &#9999; in toolbar, draw freely</span>
      </div>
      <div class="aa-draw-legend-item">
        <div class="aa-swatch-poly"></div>
        <span><b>Polyline</b> &mdash; activate in toolbar, click points, dbl-click to finish</span>
      </div>
      <div class="aa-draw-legend-item">
        <div class="aa-swatch-region"></div>
        <span><b>Region</b> &mdash; activate in toolbar, click vertices, dbl-click to close</span>
      </div>
    </div>
    <div class="aa-draw-btnrow">
      <button class="aa-draw-btn evl" onclick="aaExportEvl()">&#11015; Download EVL</button>
      <button class="aa-draw-btn evr" onclick="aaExportEvr()">&#11015; Download EVR</button>
      <button class="aa-draw-btn clr" onclick="aaClearDraw()">&#128465; Clear all</button>
    </div>
    <div class="aa-draw-btnrow">
      <button class="aa-draw-btn evl txt" onclick="aaShowEvlText()">&#128203; Show EVL text</button>
      <button class="aa-draw-btn evr txt" onclick="aaShowEvrText()">&#128203; Show EVR text</button>
    </div>
    <div class="aa-draw-warn">
      <b>JupyterLab / cloud workspace?</b> The Download buttons are blocked by the
      iframe sandbox and may silently do nothing. Use the <b>Show text</b> buttons
      instead &mdash; they open a copyable text view that works everywhere.
    </div>
    <div class="aa-draw-hint">
      <b>Tip:</b> tools appear in the plot toolbar above the echogram &mdash; switch freely between draw, zoom and pan.<br>
      Drag any vertex with the <b>Edit</b> tool to adjust; shift-click a vertex to delete it.<br>
      EVL / EVR files use Echoview line/region format and open directly in Echoview.<br>
      <b>Note:</b> export requires a time-based x-axis (ping_time). Multiple freehand/polyline
      strokes are merged into a single time-sorted EVL line; each polygon becomes a distinct EVR region.
    </div>
  </div>
</div>
"""
    return pn.pane.HTML(html, sizing_mode="stretch_width")


# ===========================================================================
#  PLOT CONSTRUCTION
# ===========================================================================

def _plot_echogram(
    da: xr.DataArray,
    var: str,
    x_name: str,
    y_name: str,
    title: str,
    cmap: str,
    vmin: Optional[float],
    vmax: Optional[float],
    min_width: int,
    height: int,
    toolbar: str,
    pin_div,
    flip_y: bool = True,
    show_hover: bool = True,
    show_crosshair: bool = True,
    draw_idx: int = 0,
    show_draw: bool = True,
):
    da = da.rename(var)

    clim = (vmin, vmax) if (vmin is not None or vmax is not None) else None

    common_kw = dict(
        x=x_name,
        y=y_name,
        cmap=cmap,
        clim=clim,
        responsive=True,
        min_width=min_width,
        height=height,
        colorbar=True,
        toolbar=toolbar,
        title=title,
        xlabel=_x_label(x_name),
        ylabel=_y_label(y_name),
    )

    try:
        plot = da.hvplot.quadmesh(**common_kw)
    except Exception:
        try:
            plot = da.hvplot.image(**common_kw)
        except Exception:
            plot = da.hvplot(**common_kw)

    if flip_y:
        plot = plot.opts(invert_yaxis=True)

    extra_tools = []
    try:
        hover, crosshair, tap = _build_interaction_tools(var, x_name, y_name, pin_div)
        if show_hover:
            extra_tools.append(hover)
        if show_crosshair:
            extra_tools.append(crosshair)
        extra_tools.append(tap)
    except Exception as exc:
        logger.debug(f"Could not build interaction tools: {exc}")

    if extra_tools:
        plot = plot.opts(
            opts.QuadMesh(tools=extra_tools, active_tools=["wheel_zoom"]),
            opts.Image(tools=extra_tools, active_tools=["wheel_zoom"]),
        )

    _draw_idx = draw_idx
    _add_draw = show_draw

    def _combined_hook(bokeh_plot, element):
        bokeh_plot.state.sizing_mode = "stretch_width"
        if _add_draw:
            try:
                _apply_draw_tools(bokeh_plot.state, _draw_idx)
            except Exception as exc:
                logger.warning(f"Drawing tools unavailable: {exc}")

    plot = plot.opts(
        opts.QuadMesh(hooks=[_combined_hook]),
        opts.Image(hooks=[_combined_hook]),
    )

    return plot


def _prep_da(
    da: xr.DataArray,
    x_name: str,
    y_name: str,
    decimate: int,
    ymin: Optional[float],
    ymax: Optional[float],
) -> xr.DataArray:
    da = _downsample_da(da, x_name=x_name, step=decimate)
    da = _apply_ylim(da, y_name=y_name, ymin=ymin, ymax=ymax)
    return da


def _build_single_plot(
    ds: xr.Dataset,
    var: str,
    x_name: str,
    y_name: str,
    frequency: Optional[float],
    channel: Optional[str],
    cmap: str,
    vmin: Optional[float],
    vmax: Optional[float],
    min_width: int,
    height: int,
    toolbar: str,
    decimate: int,
    ymin: Optional[float],
    ymax: Optional[float],
    pin_div,
    flip_y: bool = True,
    show_hover: bool = True,
    show_crosshair: bool = True,
    draw_idx: int = 0,
    show_draw: bool = True,
):
    da = ds[var]
    chan_dim = "channel" if "channel" in da.dims else None
    freq_dim = "frequency_nominal" if "frequency_nominal" in da.dims else None
    label = var

    if channel is not None and chan_dim is not None:
        coord = ds[chan_dim]
        vals = [_coord_to_str(v) for v in coord.values]
        idx = vals.index(channel) if channel in vals else 0
        da = da.isel({chan_dim: idx})
        ch_val = coord.isel({chan_dim: idx}).values
        label = _coord_to_str(ch_val)
        fcoord = _get_freq_coord_for_channel(ds, chan_dim)
        if fcoord is not None:
            f_val = fcoord.isel({chan_dim: idx}).values
            label = f"{_coord_to_str(ch_val)} \u2022 {_coord_to_str(f_val)} Hz"
    elif frequency is not None and freq_dim is not None:
        fcoord = ds[freq_dim]
        idx = _nearest_index(fcoord, frequency)
        da = da.isel({freq_dim: idx})
        label = f"frequency={_coord_to_str(fcoord.isel({freq_dim: idx}).values)}"
    elif frequency is not None and chan_dim is not None:
        fcoord = _get_freq_coord_for_channel(ds, chan_dim)
        if fcoord is not None:
            idx = _nearest_index(fcoord, frequency)
            da = da.isel({chan_dim: idx})
            ch_val = ds[chan_dim].isel({chan_dim: idx}).values
            f_val = fcoord.isel({chan_dim: idx}).values
            label = f"{_coord_to_str(ch_val)} \u2022 {_coord_to_str(f_val)} Hz"
    else:
        if chan_dim is not None:
            da = da.isel({chan_dim: 0})
            ch_val = ds[chan_dim].isel({chan_dim: 0}).values
            label = _coord_to_str(ch_val)
            fcoord = _get_freq_coord_for_channel(ds, chan_dim)
            if fcoord is not None:
                f_val = fcoord.isel({chan_dim: 0}).values
                label = f"{_coord_to_str(ch_val)} \u2022 {_coord_to_str(f_val)} Hz"
        elif freq_dim is not None:
            da = da.isel({freq_dim: 0})
            f_val = ds[freq_dim].isel({freq_dim: 0}).values
            label = f"frequency={_coord_to_str(f_val)}"

    da, y_name = _ensure_y_axis_coord(ds, da, y_name, x_name)
    da = _prep_da(da, x_name, y_name, decimate, ymin, ymax)
    return _plot_echogram(
        da=da, var=var, x_name=x_name, y_name=y_name,
        title=f"{var} \u2022 {label}", cmap=cmap, vmin=vmin, vmax=vmax,
        min_width=min_width, height=height, toolbar=toolbar,
        pin_div=pin_div, flip_y=flip_y,
        show_hover=show_hover, show_crosshair=show_crosshair,
        draw_idx=draw_idx, show_draw=show_draw,
    )


def _build_all_tabs(
    ds: xr.Dataset,
    var: str,
    x_name: str,
    y_name: str,
    group_by: str,
    cmap: str,
    vmin: Optional[float],
    vmax: Optional[float],
    min_width: int,
    height: int,
    toolbar: str,
    decimate: int,
    ymin: Optional[float],
    ymax: Optional[float],
    pin_div,
    flip_y: bool = True,
    show_hover: bool = True,
    show_crosshair: bool = True,
    show_draw: bool = True,
):
    da = ds[var]

    if "channel" in da.dims:
        chan_dim = "channel"
        ccoord = ds[chan_dim]

        f_on_chan = None
        if "frequency_nominal" in ds:
            f = ds["frequency_nominal"]
            if chan_dim in f.dims:
                f_on_chan = f

        tabs = []
        for ci in range(ccoord.size):
            c_val = ccoord.isel({chan_dim: ci}).values
            label = _coord_to_str(c_val)
            if f_on_chan is not None:
                f_val = f_on_chan.isel({chan_dim: ci}).values
                label = f"{_coord_to_str(c_val)} \u2022 {_coord_to_str(f_val)} Hz"

            da2 = da.isel({chan_dim: ci})
            da2, y_name_resolved = _ensure_y_axis_coord(ds, da2, y_name, x_name)
            da2 = _prep_da(da2, x_name, y_name_resolved, decimate, ymin, ymax)

            plot = _plot_echogram(
                da=da2, var=var, x_name=x_name, y_name=y_name_resolved,
                title=f"{var} \u2022 {label}", cmap=cmap, vmin=vmin, vmax=vmax,
                min_width=min_width, height=height, toolbar=toolbar,
                pin_div=pin_div, flip_y=flip_y,
                show_hover=show_hover, show_crosshair=show_crosshair,
                draw_idx=ci, show_draw=show_draw,
            )
            tabs.append((label, plot))

        return pn.Tabs(*tabs, sizing_mode="stretch_width", dynamic=False)

    # Fallback - no channel dim
    da2, y_name_resolved = _ensure_y_axis_coord(ds, da, y_name, x_name)
    da2 = _prep_da(da2, x_name, y_name_resolved, decimate, ymin, ymax)
    plot = _plot_echogram(
        da=da2, var=var, x_name=x_name, y_name=y_name_resolved, title=var,
        cmap=cmap, vmin=vmin, vmax=vmax, min_width=min_width, height=height,
        toolbar=toolbar, pin_div=pin_div, flip_y=flip_y,
        show_hover=show_hover, show_crosshair=show_crosshair,
        draw_idx=0, show_draw=show_draw,
    )
    return pn.Column(
        pn.pane.Markdown("No channel dimension detected; plotting a single array."),
        plot, sizing_mode="stretch_width",
    )


# ===========================================================================
#  HEADER
# ===========================================================================

def _build_header(ds: xr.Dataset, var: str, x_name: str, y_name: str, flip_y: bool) -> pn.pane.Markdown:
    source = ds.encoding.get("source", "(in-memory)")
    dim_info = " \u00d7 ".join(f"{d}={s}" for d, s in ds[var].sizes.items())
    attrs = ds.attrs
    sonar_model = attrs.get("sonar_model", attrs.get("keywords", ""))
    survey_name = attrs.get("survey_name", attrs.get("title", ""))
    aa_tool = attrs.get("aa_tool", "")

    meta_lines = []
    if survey_name:
        meta_lines.append(f"- **survey:** `{survey_name}`")
    if sonar_model:
        meta_lines.append(f"- **sonar:** `{sonar_model}`")
    if aa_tool:
        meta_lines.append(f"- **last pipeline step:** `{aa_tool}`")

    orient_note = "y-axis inverted (surface at top)" if flip_y else "y-axis normal"
    md = (
        f"### aa-plot echogram\n"
        f"- **file:** `{source}`\n"
        f"- **var:** `{var}` \u00a0 ({dim_info})\n"
        f"- **x:** `{x_name}`  \u2022  **y:** `{y_name}` \u00a0 *({orient_note})*\n"
        + ("\n".join(meta_lines) + "\n" if meta_lines else "") + "\n"
        "<span style='color:#777; font-size:0.85em;'>"
        "Scroll to zoom \u00b7 Shift+drag to pan \u00b7 Click to pin coordinates \u00b7 "
        "Hover for values \u00b7 Colormap picker below \u00b7 "
        "Drawing tools in plot toolbar"
        "</span>"
    )
    return pn.pane.Markdown(md, sizing_mode="stretch_width")


# ===========================================================================
#  LAYOUT ASSEMBLY
# ===========================================================================

def _render_layout(
    ds: xr.Dataset,
    var: str,
    all_plots: bool,
    group_by: str,
    frequency: Optional[float],
    channel: Optional[str],
    x_override: Optional[str],
    y_override: Optional[str],
    vmin: Optional[float],
    vmax: Optional[float],
    cmap: str,
    decimate: int,
    ymin: Optional[float],
    ymax: Optional[float],
    width: int,
    height: int,
    toolbar: str,
    flip_y: bool = True,
    show_hover: bool = True,
    show_crosshair: bool = True,
    show_cmap_picker: bool = True,
    show_log: bool = True,
    show_draw: bool = True,
) -> pn.viewable.Viewable:
    x_name, y_name = _detect_axes(ds)
    if x_override:
        x_name = x_override
    if y_override:
        y_name = y_override

    if flip_y:
        flip_y = _should_flip_y(y_name)
        if flip_y:
            logger.info(f"Y-axis '{y_name}' recognised as range/depth -> inverting (surface at top).")
        else:
            logger.info(f"Y-axis '{y_name}' not in depth/range list -> keeping default orientation.")

    min_width = max(width, 100)

    from bokeh.models import Div as BokehDiv
    pin_div = BokehDiv(
        text=(
            '<div style="font-family:monospace;font-size:0.82em;padding:5px 10px;'
            'color:#475569;font-style:italic;">'
            '\U0001f4cd Click the plot to pin coordinates'
            '</div>'
        ),
        sizing_mode="stretch_width",
    )
    pin_pane = pn.pane.Bokeh(pin_div, sizing_mode="stretch_width")

    header = _build_header(ds, var, x_name, y_name, flip_y)

    if all_plots:
        body = _build_all_tabs(
            ds=ds, var=var, x_name=x_name, y_name=y_name, group_by=group_by,
            cmap=cmap, vmin=vmin, vmax=vmax, min_width=min_width, height=height,
            toolbar=toolbar, decimate=decimate, ymin=ymin, ymax=ymax,
            pin_div=pin_div, flip_y=flip_y,
            show_hover=show_hover, show_crosshair=show_crosshair,
            show_draw=show_draw,
        )
    else:
        body = _build_single_plot(
            ds=ds, var=var, x_name=x_name, y_name=y_name,
            frequency=frequency, channel=channel,
            cmap=cmap, vmin=vmin, vmax=vmax, min_width=min_width, height=height,
            toolbar=toolbar, decimate=decimate, ymin=ymin, ymax=ymax,
            pin_div=pin_div, flip_y=flip_y,
            show_hover=show_hover, show_crosshair=show_crosshair,
            draw_idx=0, show_draw=show_draw,
        )

    plot_inner: list = []
    if show_cmap_picker:
        plot_inner.append(_build_cmap_picker(cmap))
        plot_inner.append(pn.Spacer(height=2))
    plot_inner += [body, pin_pane]
    plot_section = pn.Column(*plot_inner, sizing_mode="stretch_width")

    parts: list = [header, plot_section]

    if show_draw:
        parts.append(pn.Spacer(height=6))
        parts.append(_build_annotation_panel())

    if show_log:
        parts.append(pn.Spacer(height=8))
        parts.append(_build_data_log(ds, var, x_name, y_name, flip_y))

    return pn.Column(*parts, sizing_mode="stretch_width")


# ===========================================================================
#  CLI
# ===========================================================================

def main() -> None:
    if len(sys.argv) == 1:
        token = _read_input_path_from_stdin()
        if token:
            sys.argv.append(token)
        else:
            print_help()
            raise SystemExit(0)

    p = argparse.ArgumentParser(
        description="Interactive echogram plotting (hvPlot + Panel) -> standalone HTML",
        add_help=False,
    )
    p.add_argument("input_path", type=Path, nargs="?")
    p.add_argument("--var", default=None)
    p.add_argument("--all", action="store_true")
    p.add_argument("--frequency", type=float, default=None)
    p.add_argument("--channel", type=str, default=None)
    p.add_argument("--group-by", type=str, default="auto", choices=["auto", "channel", "freq"])
    p.add_argument("--x", dest="x_override", type=str, default=None)
    p.add_argument("--y", dest="y_override", type=str, default=None)
    p.add_argument("--no-flip", action="store_true")
    p.add_argument("--vmin", type=float, default=None)
    p.add_argument("--vmax", type=float, default=None)
    p.add_argument("--cmap", type=str, default="inferno")
    p.add_argument("--width", type=int, default=250,
                   help="Minimum plot width in px; stretches responsively (default: 250).")
    p.add_argument("--height", type=int, default=450)
    p.add_argument("--toolbar", type=str, default="above",
                   choices=["above", "below", "left", "right", "disable"])
    p.add_argument("--no-hover", action="store_true")
    p.add_argument("--no-crosshair", action="store_true")
    p.add_argument("--no-cmap-picker", action="store_true")
    p.add_argument("--no-log", action="store_true")
    p.add_argument("--no-draw", action="store_true",
                   help="Disable freehand/polyline/region drawing tools.")
    p.add_argument("--decimate", type=int, default=1)
    p.add_argument("--ymin", type=float, default=None)
    p.add_argument("--ymax", type=float, default=None)
    p.add_argument("-o", "--output_path", type=Path, default=None)
    p.add_argument("--no-overwrite", action="store_true")
    p.add_argument("--quiet", action="store_true")
    p.add_argument("-h", "--help", action="store_true")

    args = p.parse_args()

    if args.help:
        print_help()
        raise SystemExit(0)

    _configure_logging(args.quiet)

    if args.input_path is None:
        token = _read_input_path_from_stdin()
        if not token:
            logger.error("No INPUT_PATH provided and no stdin token available.")
            raise SystemExit(2)
        args.input_path = Path(token)
        logger.info(f"Read input path from stdin: {args.input_path}")

    if not args.input_path.exists():
        logger.error(f"Input file does not exist: {args.input_path}")
        raise SystemExit(1)

    if args.output_path is None:
        args.output_path = args.input_path.with_stem(args.input_path.stem + "_plot").with_suffix(".html")

    if args.output_path.exists() and args.no_overwrite:
        logger.error(f"Output exists and --no-overwrite set: {args.output_path}")
        raise SystemExit(1)

    if args.all and (args.frequency is not None or args.channel is not None):
        logger.error("Use either --all OR a specific --frequency/--channel (not both).")
        raise SystemExit(2)

    try:
        buf = io.StringIO()
        with redirect_stdout(buf), redirect_stderr(buf):
            with xr.open_dataset(args.input_path) as ds_in:
                ds = ds_in.load()

        ds.encoding["source"] = str(args.input_path)
        var = _ensure_variable(ds, args.var)
        logger.info(f"Plotting var='{var}' from {args.input_path.name}")

        layout = _render_layout(
            ds=ds, var=var, all_plots=args.all, group_by=args.group_by,
            frequency=args.frequency, channel=args.channel,
            x_override=args.x_override, y_override=args.y_override,
            vmin=args.vmin, vmax=args.vmax, cmap=args.cmap,
            decimate=args.decimate, ymin=args.ymin, ymax=args.ymax,
            width=args.width, height=args.height, toolbar=args.toolbar,
            flip_y=not args.no_flip, show_hover=not args.no_hover,
            show_crosshair=not args.no_crosshair,
            show_cmap_picker=not args.no_cmap_picker,
            show_log=not args.no_log,
            show_draw=not args.no_draw,
        )

        args.output_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"Saving HTML: {args.output_path}")
        pn.io.save.save(
            layout,
            filename=str(args.output_path),
            embed=True,
            resources="inline",
            title="aa-plot echogram",
        )

        print(args.output_path.resolve())

    except Exception as e:
        logger.exception(f"aa-plot failed: {e}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()