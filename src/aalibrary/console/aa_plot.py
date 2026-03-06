#!/usr/bin/env python3
"""
aa-plot — Interactive echogram plotting (HTML) for Echopype/xarray NetCDF datasets

Goals:
- Accept .nc path from argv OR stdin (pipeline-friendly).
- Plot a variable (default: Sv if present).
- Plot ALL channels and/or frequencies in a tabbed UI (Panel + hvPlot).
  * If Sv has a 'channel' dimension, tabs are per-channel (labels include
    frequency_nominal if present as a coord on channel).
  * If Sv has a 'frequency_nominal' dimension, tabs are per-frequency.
  * If BOTH are dimensions, tabs are nested: frequency -> channel, or channel -> frequency.
- No matplotlib. Output is standalone HTML.
- Print absolute HTML path to stdout for downstream chaining.
- Keep stdout clean except for final path (logs go to stderr via loguru).
"""

from __future__ import annotations

import argparse
import io
import json
import sys
from contextlib import redirect_stdout, redirect_stderr
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple, Any

import numpy as np
import xarray as xr
from loguru import logger

import holoviews as hv
from holoviews import opts
import hvplot.xarray  # noqa: F401
import panel as pn

pn.extension()

# ---------------------------------------------------------------------------
# Y-axis names that represent "depth / range" and should be drawn top-down
# (0 at ocean surface, increasing downward).
# ---------------------------------------------------------------------------
_DEPTH_RANGE_NAMES = frozenset({
    "echo_range", "range", "range_meter", "range_sample",
    "depth", "range_bin", "distance", "range_m",
})

# ---------------------------------------------------------------------------
# Colormaps curated for active-acoustics echograms.
# The JS callback swaps palettes in-browser; no re-render needed.
# ---------------------------------------------------------------------------
_CMAP_OPTIONS = [
    "inferno",
    "viridis",
    "plasma",
    "magma",
    "cividis",
    "turbo",
    "coolwarm",
    "gray",
    "RdYlBu_r",
    "Spectral_r",
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
  --cmap NAME               Initial colormap name (default: inferno). A live
                            switcher is embedded in the HTML output.
  --width INT               Plot width  (default: 1200).
  --height INT              Plot height (default: 450).
  --toolbar STR             Toolbar: above/below/left/right/disable (default: above).
  --no-hover                Disable hover tooltip overlay.
  --no-crosshair            Disable crosshair cursor.
  --no-cmap-picker          Disable the interactive colormap picker in the HTML.
  --no-log                  Disable the copyable data-summary log panel.

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
    for c in candidates:
        if c in ds.dims or c in ds.coords:
            return c

    dims = list(ds.dims)
    if dims:
        return dims[min(fallback_index, len(dims) - 1)]

    raise ValueError("Dataset has no dimensions; cannot detect axes.")


def _detect_axes(ds: xr.Dataset) -> Tuple[str, str]:
    x_name = _detect_axis(ds, ("ping_time", "time", "ping", "profile_time"), fallback_index=0)
    y_name = _detect_axis(ds, ("echo_range", "range", "range_meter", "range_sample", "depth"), fallback_index=1)
    return x_name, y_name


def _should_flip_y(y_name: str) -> bool:
    """Return True when the y-axis represents depth/range and should be inverted
    so that 0 (ocean surface) is at the top and range increases downward —
    the standard orientation for active-acoustic echograms."""
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
    """Produce a human-friendly y-axis label."""
    nice = {
        "echo_range": "Range (m)",
        "range": "Range (m)",
        "range_meter": "Range (m)",
        "range_m": "Range (m)",
        "depth": "Depth (m)",
        "range_sample": "Range sample",
        "range_bin": "Range bin",
    }
    return nice.get(y_name, y_name)


def _x_label(x_name: str) -> str:
    """Produce a human-friendly x-axis label."""
    nice = {
        "ping_time": "Ping time",
        "time": "Time",
        "ping": "Ping #",
        "profile_time": "Profile time",
    }
    return nice.get(x_name, x_name)


# ═══════════════════════════════════════════════════════════════════════════
#  DATA SUMMARY SIDEBAR  —  styled card beside the echogram
# ═══════════════════════════════════════════════════════════════════════════

_SIDEBAR_CSS = """\
<style>
.aa-sidebar {
    background: #111827;
    border: 1px solid #1e293b;
    border-radius: 8px;
    font-family: 'Menlo', 'Consolas', 'DejaVu Sans Mono', monospace;
    font-size: 0.78em;
    color: #cbd5e1;
    overflow-y: auto;
    overflow-x: hidden;
    width: 100%;
    user-select: text;
    cursor: text;
    line-height: 1.6;
}
.aa-sidebar-title {
    background: linear-gradient(135deg, #1e3a5f 0%, #0f172a 100%);
    color: #38bdf8;
    padding: 10px 14px;
    font-weight: 700;
    font-size: 1.05em;
    letter-spacing: 0.04em;
    border-radius: 8px 8px 0 0;
    border-bottom: 1px solid #1e293b;
    user-select: none;
    display: flex;
    align-items: center;
    gap: 8px;
}
.aa-sidebar-title svg {
    flex-shrink: 0;
}
.aa-sections-grid {
    display: flex;
    flex-wrap: wrap;
    gap: 0;
}
.aa-section {
    padding: 8px 14px 4px 14px;
    min-width: 260px;
    flex: 1 1 260px;
    box-sizing: border-box;
}
.aa-section-head {
    color: #94a3b8;
    font-weight: 600;
    font-size: 0.9em;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    border-bottom: 1px solid #1e293b;
    padding-bottom: 4px;
    margin-bottom: 5px;
}
.aa-row {
    display: flex;
    justify-content: space-between;
    padding: 1px 0;
}
.aa-key {
    color: #64748b;
    white-space: nowrap;
    padding-right: 8px;
}
.aa-val {
    color: #e2e8f0;
    text-align: right;
    word-break: break-all;
}
.aa-val-em {
    color: #38bdf8;
    text-align: right;
    font-weight: 600;
}
.aa-chan-row {
    padding: 1px 0;
    display: flex;
    gap: 6px;
}
.aa-chan-idx {
    color: #475569;
    min-width: 24px;
}
.aa-chan-name {
    color: #e2e8f0;
}
.aa-chan-freq {
    color: #818cf8;
    margin-left: auto;
}
.aa-divider {
    border: none;
    border-top: 1px solid #1e293b;
    margin: 4px 0;
}
.aa-copy-btn {
    background: #1e293b;
    border: 1px solid #334155;
    border-radius: 5px;
    color: #94a3b8;
    padding: 5px 12px;
    margin: 8px 14px 10px 14px;
    cursor: pointer;
    font-size: 0.9em;
    font-family: inherit;
    transition: all 0.15s;
    max-width: 220px;
    text-align: center;
}
.aa-copy-btn:hover {
    background: #334155;
    color: #e2e8f0;
    border-color: #475569;
}
</style>
"""

# Clipboard icon SVG (16×16)
_CLIPBOARD_SVG = (
    '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" '
    'stroke="currentColor" stroke-width="2" stroke-linecap="round" '
    'stroke-linejoin="round">'
    '<rect x="9" y="9" width="13" height="13" rx="2" ry="2"/>'
    '<path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/>'
    '</svg>'
)


def _html_row(key: str, val: str, em: bool = False) -> str:
    cls = "aa-val-em" if em else "aa-val"
    # Escape HTML entities
    val = val.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return f'<div class="aa-row"><span class="aa-key">{key}</span><span class="{cls}">{val}</span></div>'


def _build_data_log(
    ds: xr.Dataset,
    var: str,
    x_name: str,
    y_name: str,
    flip_y: bool,
) -> pn.pane.HTML:
    """Build a styled full-width card with dataset summary, copyable via button."""

    sections: list[str] = []

    # --- Title bar -----------------------------------------------------------
    sections.append(
        f'<div class="aa-sidebar-title">{_CLIPBOARD_SVG} Data Summary</div>'
    )

    # --- Source section -------------------------------------------------------
    src = ds.encoding.get("source", "(in-memory)")
    src_short = Path(src).name if src != "(in-memory)" else src
    sec = '<div class="aa-section">'
    sec += '<div class="aa-section-head">Source</div>'
    sec += _html_row("file", src_short)
    sec += _html_row("variable", var, em=True)
    sec += _html_row("x-axis", x_name)
    orient = f"{y_name} ↕ inverted" if flip_y else y_name
    sec += _html_row("y-axis", orient)
    sec += '</div>'
    sections.append(sec)

    # --- Dataset attributes --------------------------------------------------
    attrs = ds.attrs
    interesting_keys = [
        ("sonar_model", "sonar"),
        ("survey_name", "survey"),
        ("title", "title"),
        ("institution", "institution"),
        ("platform_name", "platform"),
        ("instrument_type", "instrument"),
        ("date_created", "created"),
        ("time_coverage_start", "time start"),
        ("time_coverage_end", "time end"),
    ]
    attr_rows = []
    for ak, label in interesting_keys:
        if ak in attrs and attrs[ak]:
            attr_rows.append(_html_row(label, str(attrs[ak])))

    if attr_rows:
        sec = '<div class="aa-section">'
        sec += '<div class="aa-section-head">Metadata</div>'
        sec += "".join(attr_rows)
        sec += '</div>'
        sections.append(sec)

    # --- Dimensions ----------------------------------------------------------
    da = ds[var]
    sec = '<div class="aa-section">'
    sec += '<div class="aa-section-head">Dimensions</div>'
    for d, s in da.sizes.items():
        sec += _html_row(d, f"{s:,}")
    sec += '</div>'
    sections.append(sec)

    # --- Coordinate ranges ---------------------------------------------------
    range_rows = []
    for cname in da.dims:
        if cname in ds.coords:
            c = ds[cname]
            try:
                cmin = _coord_to_str(c.min().values)
                cmax = _coord_to_str(c.max().values)
                # Shorten long timestamps
                if len(cmin) > 26:
                    cmin = cmin[:19]
                if len(cmax) > 26:
                    cmax = cmax[:19]
                range_rows.append(_html_row(cname, f"{cmin} → {cmax}"))
            except Exception:
                range_rows.append(_html_row(cname, "(n/a)"))

    if range_rows:
        sec = '<div class="aa-section">'
        sec += '<div class="aa-section-head">Coord Ranges</div>'
        sec += "".join(range_rows)
        sec += '</div>'
        sections.append(sec)

    # --- Channel / frequency summary -----------------------------------------
    chan_dim = "channel" if "channel" in da.dims else None
    f_on_chan = None
    if chan_dim:
        ccoord = ds[chan_dim]
        for loc in (ds.data_vars, ds.coords):
            if "frequency_nominal" in loc:
                f = ds["frequency_nominal"]
                if chan_dim in f.dims:
                    f_on_chan = f
                    break

        sec = '<div class="aa-section">'
        sec += '<div class="aa-section-head">Channels</div>'
        for ci in range(ccoord.size):
            ch_str = _coord_to_str(ccoord.isel({chan_dim: ci}).values)
            # Shorten long channel IDs for sidebar display
            if len(ch_str) > 30:
                ch_str = "…" + ch_str[-28:]
            freq_html = ""
            if f_on_chan is not None:
                fv = f_on_chan.isel({chan_dim: ci}).values
                try:
                    fv_num = float(fv)
                    if fv_num >= 1000:
                        freq_html = f'<span class="aa-chan-freq">{fv_num / 1000:.0f} kHz</span>'
                    else:
                        freq_html = f'<span class="aa-chan-freq">{fv_num:.0f} Hz</span>'
                except Exception:
                    freq_html = f'<span class="aa-chan-freq">{_coord_to_str(fv)}</span>'
            sec += (
                f'<div class="aa-chan-row">'
                f'<span class="aa-chan-idx">[{ci}]</span>'
                f'<span class="aa-chan-name">{ch_str}</span>'
                f'{freq_html}'
                f'</div>'
            )
        sec += '</div>'
        sections.append(sec)

    # --- Variable statistics -------------------------------------------------
    sec = '<div class="aa-section">'
    sec += f'<div class="aa-section-head">{var} Statistics</div>'
    finite = np.array([])  # default so plain_lines block below is safe
    try:
        vals = da.values
        finite = vals[np.isfinite(vals)]
        total = vals.size
        nan_count = total - finite.size
        sec += _html_row("samples", f"{total:,}")
        pct_nan = f"{100 * nan_count / max(total, 1):.1f}%"
        sec += _html_row("NaN / Inf", f"{nan_count:,} ({pct_nan})")
        if finite.size > 0:
            sec += '<hr class="aa-divider"/>'
            sec += _html_row("min", f"{finite.min():.2f}", em=True)
            sec += _html_row("max", f"{finite.max():.2f}", em=True)
            sec += _html_row("mean", f"{finite.mean():.2f}")
            sec += _html_row("std", f"{finite.std():.2f}")
            sec += '<hr class="aa-divider"/>'
            for q in (5, 25, 50, 75, 95):
                label = f"P{q}"
                sec += _html_row(label, f"{np.percentile(finite, q):.2f}")
    except Exception as exc:
        sec += _html_row("error", str(exc))
    sec += '</div>'
    sections.append(sec)

    # --- All data variables --------------------------------------------------
    sec = '<div class="aa-section">'
    sec += '<div class="aa-section-head">All Variables</div>'
    for dv in ds.data_vars:
        shape_str = ", ".join(f"{s}" for s in ds[dv].shape)
        sec += _html_row(dv, f"({shape_str})")
    sec += '</div>'
    sections.append(sec)

    # --- Build the plain-text version for the copy button --------------------
    plain_lines: list[str] = []
    plain_lines.append(f"aa-plot Data Summary — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    plain_lines.append(f"File: {ds.encoding.get('source', '(in-memory)')}")
    plain_lines.append(f"Variable: {var}  |  X: {x_name}  |  Y: {y_name} {'(inverted)' if flip_y else ''}")
    plain_lines.append("")
    for d, s in da.sizes.items():
        plain_lines.append(f"  {d}: {s}")
    if chan_dim:
        plain_lines.append("")
        ccoord = ds[chan_dim]
        for ci in range(ccoord.size):
            ch_str = _coord_to_str(ccoord.isel({chan_dim: ci}).values)
            freq_part = ""
            if f_on_chan is not None:
                fv = f_on_chan.isel({chan_dim: ci}).values
                freq_part = f"  ({_coord_to_str(fv)} Hz)"
            plain_lines.append(f"  [{ci}] {ch_str}{freq_part}")
    try:
        if finite.size > 0:
            plain_lines.append("")
            plain_lines.append(f"  {var}: min={finite.min():.4f}  max={finite.max():.4f}  "
                               f"mean={finite.mean():.4f}  std={finite.std():.4f}")
            pcts = {q: np.percentile(finite, q) for q in (5, 25, 50, 75, 95)}
            plain_lines.append("  Percentiles: " + "  ".join(f"P{q}={v:.4f}" for q, v in pcts.items()))
    except Exception:
        pass

    plain_text_escaped = "\\n".join(
        line.replace("\\", "\\\\").replace("'", "\\'").replace('"', '\\"')
        for line in plain_lines
    )

    # --- Copy button with inline JS -----------------------------------------
    copy_btn = (
        f'<button class="aa-copy-btn" onclick="'
        f"navigator.clipboard.writeText('{plain_text_escaped}')"
        f".then(function(){{ this.innerText='Copied!'; var b=this; "
        f"setTimeout(function(){{ b.innerText='Copy to clipboard'; }}, 1500); }}.bind(this))"
        f'.catch(function(){{ /* fallback: select pre text */ }});'
        f'">Copy to clipboard</button>'
    )

    # --- Assemble panel HTML -------------------------------------------------
    html = (
        f'{_SIDEBAR_CSS}'
        f'<div class="aa-sidebar">'
        + sections[0]  # title bar
        + '<div class="aa-sections-grid">'
        + "".join(sections[1:])
        + '</div>'
        + copy_btn
        + '</div>'
    )

    return pn.pane.HTML(html, sizing_mode="stretch_width")


# ═══════════════════════════════════════════════════════════════════════════
#  DYNAMIC COLORMAP PICKER  — pure Bokeh JS callback, zero re-render cost
# ═══════════════════════════════════════════════════════════════════════════

def _get_bokeh_palette(name: str, n: int = 256) -> list[str]:
    """Resolve a colormap name into a list of hex color strings."""
    from bokeh.palettes import all_palettes

    # Try matplotlib first (most reliable for continuous 256-colour ramps)
    try:
        import matplotlib
        matplotlib.use("Agg")          # no GUI backend needed
        import matplotlib.pyplot as plt
        cmap = plt.get_cmap(name, n)
        return [
            "#%02x%02x%02x" % (int(r * 255), int(g * 255), int(b * 255))
            for r, g, b, _ in (cmap(i) for i in range(n))
        ]
    except Exception:
        pass

    # Fallback: bokeh all_palettes
    if name in all_palettes:
        biggest = max(all_palettes[name].keys())
        return list(all_palettes[name][biggest])

    from bokeh.palettes import Inferno256
    return list(Inferno256)


def _build_cmap_picker(default_cmap: str) -> pn.pane.Bokeh:
    """Return a Bokeh Select widget + CustomJS that swaps the color-mapper
    palette on every figure in the document.  Pure client-side — works in
    static ``embed=True`` HTML with zero extra pre-rendered states."""
    from bokeh.models import CustomJS, Select as BokehSelect

    # Pre-compute palette arrays for each option
    palette_map: dict[str, list[str]] = {}
    for cm in _CMAP_OPTIONS:
        palette_map[cm] = _get_bokeh_palette(cm, 256)

    palettes_json = json.dumps(palette_map)

    js_code = """
    var palettes = JSON.parse(palettes_json);
    var chosen = cb_obj.value;
    var pal = palettes[chosen];
    if (!pal) return;

    /* Helper: apply palette to anything that looks like a colour mapper */
    function applyPalette(model, palette) {
        if (!model) return;
        if (model.palette !== undefined) {
            model.palette = palette;
            if (model.change) model.change.emit();
        }
        if (model.color_mapper && model.color_mapper.palette !== undefined) {
            model.color_mapper.palette = palette;
            if (model.color_mapper.change) model.color_mapper.change.emit();
        }
    }

    /* Walk the entire Bokeh document model graph */
    try {
        var doc = Bokeh.documents[0];
        var models = doc._all_models;
        if (models) {
            /* _all_models can be a Map or a plain object depending on Bokeh version */
            var iter = (typeof models.forEach === 'function')
                ? function(fn){ models.forEach(fn); }
                : (typeof models.values === 'function')
                    ? function(fn){ for (var m of models.values()) fn(m); }
                    : function(fn){ for (var k in models) fn(models[k]); };
            iter(function(m) { applyPalette(m, pal); });
        }
    } catch(e) {
        console.warn('aa-plot cmap picker:', e);
    }
    """

    select = BokehSelect(
        title="Colormap",
        value=default_cmap if default_cmap in _CMAP_OPTIONS else _CMAP_OPTIONS[0],
        options=_CMAP_OPTIONS,
        width=180,
    )
    select.js_on_change(
        "value",
        CustomJS(args={"palettes_json": palettes_json}, code=js_code),
    )

    return pn.pane.Bokeh(select, sizing_mode="fixed")


# ═══════════════════════════════════════════════════════════════════════════
#  HOVER + CROSSHAIR
# ═══════════════════════════════════════════════════════════════════════════

def _build_hover_tools(var: str, x_name: str, y_name: str):
    """Return Bokeh HoverTool + CrosshairTool instances for echogram overlays."""
    from bokeh.models import HoverTool, CrosshairTool

    hover = HoverTool(
        tooltips=[
            (var, "@image{0.2f}"),
            (_x_label(x_name), "$x{%F %T}" if "time" in x_name.lower() else "$x{0.2f}"),
            (_y_label(y_name), "$y{0.2f}"),
        ],
        formatters={
            "$x": "datetime" if "time" in x_name.lower() else "numeral",
        },
        mode="mouse",
    )

    crosshair = CrosshairTool(
        line_color="#ffffff",
        line_alpha=0.6,
        line_width=1,
    )

    return hover, crosshair


# ═══════════════════════════════════════════════════════════════════════════
#  PLOT CONSTRUCTION
# ═══════════════════════════════════════════════════════════════════════════

def _plot_echogram(
    da: xr.DataArray,
    x_name: str,
    y_name: str,
    title: str,
    cmap: str,
    vmin: Optional[float],
    vmax: Optional[float],
    width: int,
    height: int,
    toolbar: str,
    flip_y: bool = True,
    show_hover: bool = True,
    show_crosshair: bool = True,
):
    clim = (vmin, vmax) if (vmin is not None or vmax is not None) else None

    common_kw = dict(
        x=x_name,
        y=y_name,
        cmap=cmap,
        clim=clim,
        width=width,
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

    # --- Apply depth-down orientation -------------------------------------------
    if flip_y:
        plot = plot.opts(invert_yaxis=True)

    # --- Overlay Bokeh tools (hover + crosshair) --------------------------------
    extra_tools = []
    try:
        hover, crosshair = _build_hover_tools(da.name or "value", x_name, y_name)
        if show_hover:
            extra_tools.append(hover)
        if show_crosshair:
            extra_tools.append(crosshair)
    except Exception as exc:
        logger.debug(f"Could not build hover/crosshair tools: {exc}")

    if extra_tools:
        plot = plot.opts(
            opts.QuadMesh(tools=extra_tools, active_tools=["wheel_zoom"]),
            opts.Image(tools=extra_tools, active_tools=["wheel_zoom"]),
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
    width: int,
    height: int,
    toolbar: str,
    decimate: int,
    ymin: Optional[float],
    ymax: Optional[float],
    flip_y: bool = True,
    show_hover: bool = True,
    show_crosshair: bool = True,
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
        label = f"{_coord_to_str(ch_val)}"
        fcoord = _get_freq_coord_for_channel(ds, chan_dim)
        if fcoord is not None:
            f_val = fcoord.isel({chan_dim: idx}).values
            label = f"{_coord_to_str(ch_val)} • {_coord_to_str(f_val)} Hz"

    elif frequency is not None and freq_dim is not None:
        fcoord = ds[freq_dim]
        idx = _nearest_index(fcoord, frequency)
        da = da.isel({freq_dim: idx})
        f_val = fcoord.isel({freq_dim: idx}).values
        label = f"frequency={_coord_to_str(f_val)}"

    elif frequency is not None and chan_dim is not None:
        fcoord = _get_freq_coord_for_channel(ds, chan_dim)
        if fcoord is not None:
            idx = _nearest_index(fcoord, frequency)
            da = da.isel({chan_dim: idx})
            ch_val = ds[chan_dim].isel({chan_dim: idx}).values
            f_val = fcoord.isel({chan_dim: idx}).values
            label = f"{_coord_to_str(ch_val)} • {_coord_to_str(f_val)} Hz"

    else:
        if chan_dim is not None:
            da = da.isel({chan_dim: 0})
            ch_val = ds[chan_dim].isel({chan_dim: 0}).values
            label = f"{_coord_to_str(ch_val)}"
            fcoord = _get_freq_coord_for_channel(ds, chan_dim)
            if fcoord is not None:
                f_val = fcoord.isel({chan_dim: 0}).values
                label = f"{_coord_to_str(ch_val)} • {_coord_to_str(f_val)} Hz"
        elif freq_dim is not None:
            da = da.isel({freq_dim: 0})
            f_val = ds[freq_dim].isel({freq_dim: 0}).values
            label = f"frequency={_coord_to_str(f_val)}"

    da = _prep_da(da, x_name, y_name, decimate, ymin, ymax)

    return _plot_echogram(
        da=da,
        x_name=x_name,
        y_name=y_name,
        title=f"{var} • {label}",
        cmap=cmap,
        vmin=vmin,
        vmax=vmax,
        width=width,
        height=height,
        toolbar=toolbar,
        flip_y=flip_y,
        show_hover=show_hover,
        show_crosshair=show_crosshair,
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
    width: int,
    height: int,
    toolbar: str,
    decimate: int,
    ymin: Optional[float],
    ymax: Optional[float],
    flip_y: bool = True,
    show_hover: bool = True,
    show_crosshair: bool = True,
):
    da = ds[var]

    if "channel" in da.dims:
        chan_dim = "channel"
        ccoord = ds[chan_dim]

        f_on_chan = None
        if "frequency_nominal" in ds.data_vars:
            f = ds["frequency_nominal"]
            if chan_dim in f.dims:
                f_on_chan = f
        elif "frequency_nominal" in ds.coords:
            f = ds["frequency_nominal"]
            if chan_dim in f.dims:
                f_on_chan = f

        tabs = []
        for ci in range(ccoord.size):
            c_val = ccoord.isel({chan_dim: ci}).values

            label = _coord_to_str(c_val)
            if f_on_chan is not None:
                f_val = f_on_chan.isel({chan_dim: ci}).values
                label = f"{_coord_to_str(c_val)} • {_coord_to_str(f_val)} Hz"

            da2 = da.isel({chan_dim: ci})
            da2 = _prep_da(da2, x_name, y_name, decimate, ymin, ymax)

            plot = _plot_echogram(
                da=da2,
                x_name=x_name,
                y_name=y_name,
                title=f"{var} • {label}",
                cmap=cmap,
                vmin=vmin,
                vmax=vmax,
                width=width,
                height=height,
                toolbar=toolbar,
                flip_y=flip_y,
                show_hover=show_hover,
                show_crosshair=show_crosshair,
            )

            tabs.append((label, plot))

        return pn.Tabs(*tabs, sizing_mode="stretch_both", dynamic=False)

    # Fallback — no channel dim
    da2 = _prep_da(da, x_name, y_name, decimate, ymin, ymax)
    plot = _plot_echogram(
        da=da2,
        x_name=x_name,
        y_name=y_name,
        title=var,
        cmap=cmap,
        vmin=vmin,
        vmax=vmax,
        width=width,
        height=height,
        toolbar=toolbar,
        flip_y=flip_y,
        show_hover=show_hover,
        show_crosshair=show_crosshair,
    )

    return pn.Column(
        pn.pane.Markdown("No channel dimension detected; plotting a single array."),
        plot,
        sizing_mode="stretch_both",
    )


# ═══════════════════════════════════════════════════════════════════════════
#  HEADER
# ═══════════════════════════════════════════════════════════════════════════

def _build_header(ds: xr.Dataset, var: str, x_name: str, y_name: str, flip_y: bool) -> pn.pane.Markdown:
    """Build a richer metadata header with dataset summary."""
    source = ds.encoding.get("source", "(in-memory)")

    dim_info = " × ".join(f"{d}={s}" for d, s in ds[var].sizes.items())

    attrs = ds.attrs
    sonar_model = attrs.get("sonar_model", attrs.get("keywords", ""))
    survey_name = attrs.get("survey_name", attrs.get("title", ""))

    meta_lines = []
    if survey_name:
        meta_lines.append(f"- **survey:** `{survey_name}`")
    if sonar_model:
        meta_lines.append(f"- **sonar:** `{sonar_model}`")

    orient_note = "y-axis inverted (surface at top)" if flip_y else "y-axis normal"
    meta_block = "\n".join(meta_lines)

    md = (
        f"### aa-plot echogram\n"
        f"- **file:** `{source}`\n"
        f"- **var:** `{var}` &nbsp; ({dim_info})\n"
        f"- **x:** `{x_name}`  •  **y:** `{y_name}` &nbsp; *({orient_note})*\n"
        f"{meta_block}\n"
        f"\n"
        f"<span style='color:#777; font-size:0.85em;'>"
        f"Hover for values · Scroll-wheel to zoom · Shift+drag to pan · "
        f"Use the colormap picker to change palette live"
        f"</span>"
    )
    return pn.pane.Markdown(md, sizing_mode="stretch_width")


# ═══════════════════════════════════════════════════════════════════════════
#  LAYOUT ASSEMBLY
# ═══════════════════════════════════════════════════════════════════════════

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
) -> pn.viewable.Viewable:
    x_name, y_name = _detect_axes(ds)
    if x_override:
        x_name = x_override
    if y_override:
        y_name = y_override

    # Auto-detect whether to flip the y-axis
    if flip_y:
        flip_y = _should_flip_y(y_name)
        if flip_y:
            logger.info(f"Y-axis '{y_name}' recognised as range/depth → inverting (surface at top).")
        else:
            logger.info(f"Y-axis '{y_name}' not in depth/range list → keeping default orientation.")

    header = _build_header(ds, var, x_name, y_name, flip_y)

    # --- Colormap picker (client-side JS, zero render cost) ------------------
    controls = None
    if show_cmap_picker:
        controls = _build_cmap_picker(cmap)

    # --- Plots ---------------------------------------------------------------
    if all_plots:
        body = _build_all_tabs(
            ds=ds,
            var=var,
            x_name=x_name,
            y_name=y_name,
            group_by=group_by,
            cmap=cmap,
            vmin=vmin,
            vmax=vmax,
            width=width,
            height=height,
            toolbar=toolbar,
            decimate=decimate,
            ymin=ymin,
            ymax=ymax,
            flip_y=flip_y,
            show_hover=show_hover,
            show_crosshair=show_crosshair,
        )
    else:
        body = _build_single_plot(
            ds=ds,
            var=var,
            x_name=x_name,
            y_name=y_name,
            frequency=frequency,
            channel=channel,
            cmap=cmap,
            vmin=vmin,
            vmax=vmax,
            width=width,
            height=height,
            toolbar=toolbar,
            decimate=decimate,
            ymin=ymin,
            ymax=ymax,
            flip_y=flip_y,
            show_hover=show_hover,
            show_crosshair=show_crosshair,
        )

    # --- Assemble layout -----------------------------------------------------
    # Colormap picker sits inline with the header (top bar).
    # Data summary panel goes full-width below the plot.
    if controls:
        header_row = pn.Row(header, controls, sizing_mode="stretch_width")
    else:
        header_row = header

    parts = [header_row, body]

    if show_log:
        log_panel = _build_data_log(ds, var, x_name, y_name, flip_y)
        parts.append(pn.Spacer(height=8))
        parts.append(log_panel)

    return pn.Column(*parts, sizing_mode="stretch_both")


# ═══════════════════════════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════════════════════════

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
    p.add_argument("input_path", type=Path, nargs="?", help="Path to a NetCDF file (.nc).")

    p.add_argument("--var", default=None, help="Variable to plot (default: Sv if present).")
    p.add_argument("--all", action="store_true", help="Plot all channels/frequencies as tabs.")
    p.add_argument("--frequency", type=float, default=None, help="Select nominal frequency (Hz) (nearest match).")
    p.add_argument("--channel", type=str, default=None, help="Select channel by name (exact match preferred).")
    p.add_argument(
        "--group-by",
        type=str,
        default="auto",
        choices=["auto", "channel", "freq"],
        help="When --all and both channel+freq dimensions exist, controls tab nesting order.",
    )

    p.add_argument("--x", dest="x_override", type=str, default=None, help="Override x-axis dim/coord name.")
    p.add_argument("--y", dest="y_override", type=str, default=None, help="Override y-axis dim/coord name.")
    p.add_argument("--no-flip", action="store_true", help="Disable automatic y-axis inversion for range/depth.")

    p.add_argument("--vmin", type=float, default=None, help="Lower color limit.")
    p.add_argument("--vmax", type=float, default=None, help="Upper color limit.")
    p.add_argument("--cmap", type=str, default="inferno", help="Initial colormap (default: inferno).")
    p.add_argument("--width", type=int, default=1200, help="Plot width (px).")
    p.add_argument("--height", type=int, default=450, help="Plot height (px).")
    p.add_argument(
        "--toolbar",
        type=str,
        default="above",
        choices=["above", "below", "left", "right", "disable"],
        help="Toolbar placement (default: above).",
    )

    p.add_argument("--no-hover", action="store_true", help="Disable hover tooltip with Sv + coord readout.")
    p.add_argument("--no-crosshair", action="store_true", help="Disable crosshair cursor.")
    p.add_argument("--no-cmap-picker", action="store_true", help="Disable the interactive colormap picker.")
    p.add_argument("--no-log", action="store_true", help="Disable the copyable data-summary log panel.")

    p.add_argument("--decimate", type=int, default=1, help="Keep every Nth x sample (default: 1).")
    p.add_argument("--ymin", type=float, default=None, help="Y-axis min crop.")
    p.add_argument("--ymax", type=float, default=None, help="Y-axis max crop.")

    p.add_argument("-o", "--output_path", type=Path, default=None, help="Output HTML path.")
    p.add_argument("--no-overwrite", action="store_true", help="Do not overwrite output if it exists.")
    p.add_argument("--quiet", action="store_true", help="Reduce logs; still prints final path.")
    p.add_argument("-h", "--help", action="store_true", help="Show help and exit.")

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
            ds = xr.open_dataset(args.input_path)

        ds.encoding["source"] = str(args.input_path)

        var = _ensure_variable(ds, args.var)
        logger.info(f"Plotting var='{var}' from {args.input_path.name}")

        layout = _render_layout(
            ds=ds,
            var=var,
            all_plots=args.all,
            group_by=args.group_by,
            frequency=args.frequency,
            channel=args.channel,
            x_override=args.x_override,
            y_override=args.y_override,
            vmin=args.vmin,
            vmax=args.vmax,
            cmap=args.cmap,
            decimate=args.decimate,
            ymin=args.ymin,
            ymax=args.ymax,
            width=args.width,
            height=args.height,
            toolbar=args.toolbar,
            flip_y=not args.no_flip,
            show_hover=not args.no_hover,
            show_crosshair=not args.no_crosshair,
            show_cmap_picker=not args.no_cmap_picker,
            show_log=not args.no_log,
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