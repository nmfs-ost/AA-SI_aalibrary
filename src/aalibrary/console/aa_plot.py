#!/usr/bin/env python3
"""
aa-plot — Interactive echogram plotting (HTML) for Echopype/xarray NetCDF datasets
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

_DEPTH_RANGE_NAMES = frozenset({
    "echo_range", "range", "range_meter", "range_sample",
    "depth", "range_bin", "distance", "range_m",
})

_CMAP_OPTIONS = [
    "inferno", "viridis", "plasma", "magma", "cividis",
    "turbo", "coolwarm", "gray", "RdYlBu_r", "Spectral_r",
]


def print_help() -> None:
    print(r"""
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

Axes:
  --x NAME / --y NAME       Override axis dim/coord names.
  --no-flip                 Disable automatic y-axis inversion for range/depth.

Appearance:
  --vmin FLOAT / --vmax FLOAT   Color limits.
  --cmap NAME               Initial colormap (default: inferno).
  --width INT               Plot width hint px (default: 1200; plot is responsive).
  --height INT              Plot height px (default: 450).
  --toolbar STR             above/below/left/right/disable (default: above).
  --no-hover / --no-crosshair / --no-cmap-picker / --no-log

Subsetting:
  --decimate INT            Every Nth x sample (default: 1).
  --ymin FLOAT / --ymax FLOAT   Y crop.

Output:
  -o, --output_path PATH    HTML output path.
  --no-overwrite            Fail if output exists.
  --quiet                   Suppress info logs.
  -h, --help                Show help.
""".strip())


def _configure_logging(quiet: bool) -> None:
    logger.remove()
    level = "WARNING" if quiet else "INFO"
    logger.add(sys.stderr, level=level, backtrace=not quiet, diagnose=False)


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
    x = _detect_axis(ds, ("ping_time", "time", "ping", "profile_time"), 0)
    y = _detect_axis(ds, ("echo_range", "range", "range_meter", "range_sample", "depth"), 1)
    return x, y


def _should_flip_y(y_name: str) -> bool:
    return y_name.lower() in _DEPTH_RANGE_NAMES


def _ensure_variable(ds: xr.Dataset, var: Optional[str]) -> str:
    if var:
        if var not in ds.data_vars:
            raise ValueError(f"Variable '{var}' not found. Available: {list(ds.data_vars)}")
        return var
    if "Sv" in ds.data_vars:
        return "Sv"
    if not ds.data_vars:
        raise ValueError("No data variables found to plot.")
    return list(ds.data_vars)[0]


def _downsample_da(da: xr.DataArray, x_name: str, step: int) -> xr.DataArray:
    if step <= 1 or x_name not in da.dims:
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
            pass
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
        pass
    return None


def _y_label(y: str) -> str:
    return {"echo_range": "Range (m)", "range": "Range (m)", "range_meter": "Range (m)",
            "range_m": "Range (m)", "depth": "Depth (m)",
            "range_sample": "Range sample", "range_bin": "Range bin"}.get(y, y)


def _x_label(x: str) -> str:
    return {"ping_time": "Ping time", "time": "Time",
            "ping": "Ping #", "profile_time": "Profile time"}.get(x, x)


# ───────────────────────────────────────────────────────────────────────────
#  COPY BUTTON  — works on GCP / plain HTTP / iframes
#  Uses async Clipboard API when available, falls back to execCommand.
# ───────────────────────────────────────────────────────────────────────────

def _build_copy_button(plain_text: str) -> str:
    # json.dumps gives a safe JS string literal (handles newlines, quotes, etc.)
    text_literal = json.dumps(plain_text)
    # Embed inside onclick="…" — replace " with &quot; so the attribute stays valid.
    js = (
        "(function(btn){"
        f"var t={text_literal};"
        "function ok(){var o=btn.innerText;btn.innerText='Copied \u2713';"
        "setTimeout(function(){btn.innerText=o;},1600);}"
        "function fb(){var ta=document.createElement('textarea');"
        "ta.value=t;"
        "ta.style.cssText='position:fixed;top:0;left:0;width:1px;height:1px;"
        "opacity:0;pointer-events:none;';"
        "document.body.appendChild(ta);ta.focus();ta.select();"
        "try{var ok2=document.execCommand('copy');if(ok2){ok();}else{"
        "btn.innerText='Copy failed';}}catch(e){btn.innerText='Copy failed';}"
        "document.body.removeChild(ta);}"
        "if(navigator.clipboard&&window.isSecureContext){"
        "navigator.clipboard.writeText(t).then(ok).catch(fb);}else{fb();}"
        "})(this);"
    ).replace('"', "&quot;")
    return f'<button class="aa-copy-btn" onclick="{js}">Copy to clipboard</button>'


# ───────────────────────────────────────────────────────────────────────────
#  DATA SUMMARY PANEL
# ───────────────────────────────────────────────────────────────────────────

_SIDEBAR_CSS = """
<style>
.aa-sidebar{background:#111827;border:1px solid #1e293b;border-radius:8px;
font-family:'Menlo','Consolas','DejaVu Sans Mono',monospace;font-size:.78em;
color:#cbd5e1;overflow-y:auto;overflow-x:hidden;width:100%;
user-select:text;cursor:text;line-height:1.6;}
.aa-sidebar-title{background:linear-gradient(135deg,#1e3a5f,#0f172a);color:#38bdf8;
padding:10px 14px;font-weight:700;font-size:1.05em;letter-spacing:.04em;
border-radius:8px 8px 0 0;border-bottom:1px solid #1e293b;user-select:none;
display:flex;align-items:center;gap:8px;}
.aa-sidebar-title svg{flex-shrink:0;}
.aa-sections-grid{display:flex;flex-wrap:wrap;}
.aa-section{padding:8px 14px 4px;min-width:260px;flex:1 1 260px;box-sizing:border-box;}
.aa-section-head{color:#94a3b8;font-weight:600;font-size:.9em;text-transform:uppercase;
letter-spacing:.08em;border-bottom:1px solid #1e293b;padding-bottom:4px;margin-bottom:5px;}
.aa-row{display:flex;justify-content:space-between;padding:1px 0;}
.aa-key{color:#64748b;white-space:nowrap;padding-right:8px;}
.aa-val{color:#e2e8f0;text-align:right;word-break:break-all;}
.aa-val-em{color:#38bdf8;text-align:right;font-weight:600;}
.aa-chan-row{padding:1px 0;display:flex;gap:6px;}
.aa-chan-idx{color:#475569;min-width:24px;}
.aa-chan-name{color:#e2e8f0;}
.aa-chan-freq{color:#818cf8;margin-left:auto;}
.aa-divider{border:none;border-top:1px solid #1e293b;margin:4px 0;}
.aa-copy-btn{background:#1e293b;border:1px solid #334155;border-radius:5px;
color:#94a3b8;padding:5px 12px;margin:8px 14px 10px;cursor:pointer;
font-size:.9em;font-family:inherit;transition:all .15s;max-width:220px;}
.aa-copy-btn:hover{background:#334155;color:#e2e8f0;border-color:#475569;}
</style>
"""

_CLIPBOARD_SVG = (
    '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" '
    'stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">'
    '<rect x="9" y="9" width="13" height="13" rx="2" ry="2"/>'
    '<path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>'
)


def _hr(key: str, val: str, em: bool = False) -> str:
    cls = "aa-val-em" if em else "aa-val"
    val = val.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return f'<div class="aa-row"><span class="aa-key">{key}</span><span class="{cls}">{val}</span></div>'


def _build_data_log(ds: xr.Dataset, var: str, x_name: str, y_name: str, flip_y: bool) -> pn.pane.HTML:
    secs: list[str] = [f'<div class="aa-sidebar-title">{_CLIPBOARD_SVG} Data Summary</div>']

    src = ds.encoding.get("source", "(in-memory)")
    s = '<div class="aa-section"><div class="aa-section-head">Source</div>'
    s += _hr("file", Path(src).name if src != "(in-memory)" else src)
    s += _hr("variable", var, em=True)
    s += _hr("x-axis", x_name)
    s += _hr("y-axis", f"{y_name} ↕ inverted" if flip_y else y_name)
    s += '</div>'
    secs.append(s)

    attrs = ds.attrs
    attr_rows = [_hr(lbl, str(attrs[k]))
                 for k, lbl in [("sonar_model","sonar"),("survey_name","survey"),
                                 ("title","title"),("institution","institution"),
                                 ("platform_name","platform"),("instrument_type","instrument"),
                                 ("date_created","created"),("time_coverage_start","time start"),
                                 ("time_coverage_end","time end")]
                 if k in attrs and attrs[k]]
    if attr_rows:
        secs.append('<div class="aa-section"><div class="aa-section-head">Metadata</div>'
                    + "".join(attr_rows) + '</div>')

    da = ds[var]
    s = '<div class="aa-section"><div class="aa-section-head">Dimensions</div>'
    for d, sz in da.sizes.items():
        s += _hr(d, f"{sz:,}")
    s += '</div>'
    secs.append(s)

    rr = []
    for cname in da.dims:
        if cname in ds.coords:
            try:
                cmin = _coord_to_str(ds[cname].min().values)
                cmax = _coord_to_str(ds[cname].max().values)
                if len(cmin) > 26: cmin = cmin[:19]
                if len(cmax) > 26: cmax = cmax[:19]
                rr.append(_hr(cname, f"{cmin} → {cmax}"))
            except Exception:
                rr.append(_hr(cname, "(n/a)"))
    if rr:
        secs.append('<div class="aa-section"><div class="aa-section-head">Coord Ranges</div>'
                    + "".join(rr) + '</div>')

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
        s = '<div class="aa-section"><div class="aa-section-head">Channels</div>'
        for ci in range(ccoord.size):
            ch = _coord_to_str(ccoord.isel({chan_dim: ci}).values)
            if len(ch) > 30: ch = "…" + ch[-28:]
            fh = ""
            if f_on_chan is not None:
                fv = f_on_chan.isel({chan_dim: ci}).values
                try:
                    fn = float(fv)
                    fh = (f'<span class="aa-chan-freq">{fn/1000:.0f} kHz</span>'
                          if fn >= 1000 else f'<span class="aa-chan-freq">{fn:.0f} Hz</span>')
                except Exception:
                    fh = f'<span class="aa-chan-freq">{_coord_to_str(fv)}</span>'
            s += (f'<div class="aa-chan-row"><span class="aa-chan-idx">[{ci}]</span>'
                  f'<span class="aa-chan-name">{ch}</span>{fh}</div>')
        s += '</div>'
        secs.append(s)

    finite = np.array([])
    s = f'<div class="aa-section"><div class="aa-section-head">{var} Statistics</div>'
    try:
        vals = da.values
        finite = vals[np.isfinite(vals)]
        total = vals.size
        nc = total - finite.size
        s += _hr("samples", f"{total:,}")
        s += _hr("NaN / Inf", f"{nc:,} ({100*nc/max(total,1):.1f}%)")
        if finite.size > 0:
            s += '<hr class="aa-divider"/>'
            s += _hr("min",  f"{finite.min():.2f}", em=True)
            s += _hr("max",  f"{finite.max():.2f}", em=True)
            s += _hr("mean", f"{finite.mean():.2f}")
            s += _hr("std",  f"{finite.std():.2f}")
            s += '<hr class="aa-divider"/>'
            for q in (5, 25, 50, 75, 95):
                s += _hr(f"P{q}", f"{np.percentile(finite, q):.2f}")
    except Exception as exc:
        s += _hr("error", str(exc))
    s += '</div>'
    secs.append(s)

    s = '<div class="aa-section"><div class="aa-section-head">All Variables</div>'
    for dv in ds.data_vars:
        s += _hr(dv, f"({', '.join(str(x) for x in ds[dv].shape)})")
    s += '</div>'
    secs.append(s)

    # plain-text payload for copy
    pl = [
        f"aa-plot Data Summary — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"File: {ds.encoding.get('source','(in-memory)')}",
        f"Variable: {var}  |  X: {x_name}  |  Y: {y_name} {'(inverted)' if flip_y else ''}",
        "",
    ]
    for d, sz in da.sizes.items():
        pl.append(f"  {d}: {sz}")
    if chan_dim:
        pl.append("")
        for ci in range(ds[chan_dim].size):
            ch = _coord_to_str(ds[chan_dim].isel({chan_dim: ci}).values)
            fp = (f"  ({_coord_to_str(f_on_chan.isel({chan_dim:ci}).values)} Hz)"
                  if f_on_chan is not None else "")
            pl.append(f"  [{ci}] {ch}{fp}")
    try:
        if finite.size > 0:
            pl += ["", f"  {var}: min={finite.min():.4f}  max={finite.max():.4f}"
                       f"  mean={finite.mean():.4f}  std={finite.std():.4f}",
                   "  Percentiles: " + "  ".join(
                       f"P{q}={np.percentile(finite,q):.4f}" for q in (5,25,50,75,95))]
    except Exception:
        pass

    html = (
        _SIDEBAR_CSS
        + '<div class="aa-sidebar">'
        + secs[0]
        + '<div class="aa-sections-grid">'
        + "".join(secs[1:])
        + "</div>"
        + _build_copy_button("\n".join(pl))
        + "</div>"
    )
    return pn.pane.HTML(html, sizing_mode="stretch_width")


# ───────────────────────────────────────────────────────────────────────────
#  COLORMAP PICKER
# ───────────────────────────────────────────────────────────────────────────

def _get_bokeh_palette(name: str, n: int = 256) -> list[str]:
    from bokeh.palettes import all_palettes
    try:
        import matplotlib; matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        cm = plt.get_cmap(name, n)
        return ["#%02x%02x%02x" % (int(r*255), int(g*255), int(b*255))
                for r, g, b, _ in (cm(i) for i in range(n))]
    except Exception:
        pass
    if name in all_palettes:
        return list(all_palettes[name][max(all_palettes[name])])
    from bokeh.palettes import Inferno256
    return list(Inferno256)


def _build_cmap_picker(default_cmap: str) -> pn.pane.Bokeh:
    from bokeh.models import CustomJS, Select as BokehSelect
    pm = {cm: _get_bokeh_palette(cm, 256) for cm in _CMAP_OPTIONS}
    pj = json.dumps(pm)
    js = """
var palettes=JSON.parse(palettes_json),pal=palettes[cb_obj.value];
if(!pal)return;
function ap(m,p){if(!m)return;
if(m.palette!==undefined){m.palette=p;if(m.change)m.change.emit();}
if(m.color_mapper&&m.color_mapper.palette!==undefined){
m.color_mapper.palette=p;if(m.color_mapper.change)m.color_mapper.change.emit();}}
try{var doc=Bokeh.documents[0],models=doc._all_models;
if(models){var it=(typeof models.forEach==='function')?function(fn){models.forEach(fn);}:
(typeof models.values==='function')?function(fn){for(var m of models.values())fn(m);}:
function(fn){for(var k in models)fn(models[k]);};it(function(m){ap(m,pal);});}}
catch(e){console.warn('aa-plot cmap:',e);}
"""
    sel = BokehSelect(
        title="Colormap",
        value=default_cmap if default_cmap in _CMAP_OPTIONS else _CMAP_OPTIONS[0],
        options=_CMAP_OPTIONS, width=180)
    sel.js_on_change("value", CustomJS(args={"palettes_json": pj}, code=js))
    return pn.pane.Bokeh(sel, sizing_mode="fixed")


# ───────────────────────────────────────────────────────────────────────────
#  HOVER / CROSSHAIR
# ───────────────────────────────────────────────────────────────────────────

def _build_hover_tools(var: str, x_name: str, y_name: str):
    from bokeh.models import HoverTool, CrosshairTool
    hover = HoverTool(
        tooltips=[(var, "@image{0.2f}"),
                  (_x_label(x_name), "$x{%F %T}" if "time" in x_name.lower() else "$x{0.2f}"),
                  (_y_label(y_name), "$y{0.2f}")],
        formatters={"$x": "datetime" if "time" in x_name.lower() else "numeral"},
        mode="mouse")
    xhair = CrosshairTool(line_color="#ffffff", line_alpha=0.6, line_width=1)
    return hover, xhair


# ───────────────────────────────────────────────────────────────────────────
#  PLOT CONSTRUCTION
# ───────────────────────────────────────────────────────────────────────────

def _plot_echogram(da, x_name, y_name, title, cmap, vmin, vmax,
                   width, height, toolbar, flip_y=True,
                   show_hover=True, show_crosshair=True):
    clim = (vmin, vmax) if (vmin is not None or vmax is not None) else None
    kw = dict(x=x_name, y=y_name, cmap=cmap, clim=clim,
              responsive=True,        # ← fills container width, like the data panel
              height=height,
              colorbar=True, toolbar=toolbar, title=title,
              xlabel=_x_label(x_name), ylabel=_y_label(y_name))
    try:
        plot = da.hvplot.quadmesh(**kw)
    except Exception:
        try:
            plot = da.hvplot.image(**kw)
        except Exception:
            plot = da.hvplot(**kw)

    if flip_y:
        plot = plot.opts(invert_yaxis=True)

    # Ensure stretch_width propagates through HoloViews opts
    plot = plot.opts(
        opts.QuadMesh(sizing_mode="stretch_width"),
        opts.Image(sizing_mode="stretch_width"),
    )

    extra = []
    try:
        h, c = _build_hover_tools(da.name or "value", x_name, y_name)
        if show_hover:    extra.append(h)
        if show_crosshair: extra.append(c)
    except Exception as e:
        logger.debug(f"hover/crosshair unavailable: {e}")
    if extra:
        plot = plot.opts(
            opts.QuadMesh(tools=extra, active_tools=["wheel_zoom"]),
            opts.Image(tools=extra,    active_tools=["wheel_zoom"]),
        )
    return plot


def _prep_da(da, x_name, y_name, decimate, ymin, ymax):
    da = _downsample_da(da, x_name, decimate)
    da = _apply_ylim(da, y_name, ymin, ymax)
    return da


def _build_single_plot(ds, var, x_name, y_name, frequency, channel,
                       cmap, vmin, vmax, width, height, toolbar,
                       decimate, ymin, ymax, flip_y, show_hover, show_crosshair):
    da = ds[var]
    chan_dim = "channel" if "channel" in da.dims else None
    freq_dim = "frequency_nominal" if "frequency_nominal" in da.dims else None
    label = var

    if channel is not None and chan_dim is not None:
        coord = ds[chan_dim]
        vals = [_coord_to_str(v) for v in coord.values]
        idx = vals.index(channel) if channel in vals else 0
        da = da.isel({chan_dim: idx})
        label = _coord_to_str(coord.isel({chan_dim: idx}).values)
        fc = _get_freq_coord_for_channel(ds, chan_dim)
        if fc is not None:
            label += f" • {_coord_to_str(fc.isel({chan_dim:idx}).values)} Hz"
    elif frequency is not None and freq_dim is not None:
        fc = ds[freq_dim]; idx = _nearest_index(fc, frequency)
        da = da.isel({freq_dim: idx})
        label = f"frequency={_coord_to_str(fc.isel({freq_dim:idx}).values)}"
    elif frequency is not None and chan_dim is not None:
        fc = _get_freq_coord_for_channel(ds, chan_dim)
        if fc is not None:
            idx = _nearest_index(fc, frequency)
            da = da.isel({chan_dim: idx})
            label = (f"{_coord_to_str(ds[chan_dim].isel({chan_dim:idx}).values)}"
                     f" • {_coord_to_str(fc.isel({chan_dim:idx}).values)} Hz")
    else:
        if chan_dim is not None:
            da = da.isel({chan_dim: 0})
            label = _coord_to_str(ds[chan_dim].isel({chan_dim:0}).values)
            fc = _get_freq_coord_for_channel(ds, chan_dim)
            if fc is not None:
                label += f" • {_coord_to_str(fc.isel({chan_dim:0}).values)} Hz"
        elif freq_dim is not None:
            da = da.isel({freq_dim: 0})
            label = f"frequency={_coord_to_str(ds[freq_dim].isel({freq_dim:0}).values)}"

    da = _prep_da(da, x_name, y_name, decimate, ymin, ymax)
    return _plot_echogram(da, x_name, y_name, f"{var} • {label}", cmap, vmin, vmax,
                          width, height, toolbar, flip_y, show_hover, show_crosshair)


def _build_all_tabs(ds, var, x_name, y_name, group_by, cmap, vmin, vmax,
                    width, height, toolbar, decimate, ymin, ymax,
                    flip_y, show_hover, show_crosshair):
    da = ds[var]
    if "channel" in da.dims:
        chan_dim = "channel"; ccoord = ds[chan_dim]
        f_on_chan = None
        for loc in (ds.data_vars, ds.coords):
            if "frequency_nominal" in loc:
                f = ds["frequency_nominal"]
                if chan_dim in f.dims:
                    f_on_chan = f; break
        tabs = []
        for ci in range(ccoord.size):
            lbl = _coord_to_str(ccoord.isel({chan_dim:ci}).values)
            if f_on_chan is not None:
                lbl += f" • {_coord_to_str(f_on_chan.isel({chan_dim:ci}).values)} Hz"
            da2 = _prep_da(da.isel({chan_dim:ci}), x_name, y_name, decimate, ymin, ymax)
            tabs.append((lbl, _plot_echogram(da2, x_name, y_name, f"{var} • {lbl}",
                                             cmap, vmin, vmax, width, height, toolbar,
                                             flip_y, show_hover, show_crosshair)))
        return pn.Tabs(*tabs, sizing_mode="stretch_width", dynamic=False)

    da2 = _prep_da(da, x_name, y_name, decimate, ymin, ymax)
    return pn.Column(
        pn.pane.Markdown("No channel dimension detected; plotting a single array."),
        _plot_echogram(da2, x_name, y_name, var, cmap, vmin, vmax, width, height,
                       toolbar, flip_y, show_hover, show_crosshair),
        sizing_mode="stretch_width")


# ───────────────────────────────────────────────────────────────────────────
#  HEADER
# ───────────────────────────────────────────────────────────────────────────

def _build_header(ds, var, x_name, y_name, flip_y):
    src = ds.encoding.get("source", "(in-memory)")
    dim_info = " × ".join(f"{d}={s}" for d, s in ds[var].sizes.items())
    attrs = ds.attrs
    meta = []
    if attrs.get("survey_name") or attrs.get("title"):
        meta.append(f"- **survey:** `{attrs.get('survey_name', attrs.get('title'))}`")
    if attrs.get("sonar_model") or attrs.get("keywords"):
        meta.append(f"- **sonar:** `{attrs.get('sonar_model', attrs.get('keywords'))}`")
    orient = "y-axis inverted (surface at top)" if flip_y else "y-axis normal"
    md = (f"### aa-plot echogram\n"
          f"- **file:** `{src}`\n"
          f"- **var:** `{var}` &nbsp; ({dim_info})\n"
          f"- **x:** `{x_name}`  •  **y:** `{y_name}` &nbsp; *({orient})*\n"
          + "\n".join(meta)
          + "\n\n<span style='color:#777;font-size:.85em;'>"
          "Hover for values · Scroll-wheel to zoom · Shift+drag to pan · "
          "Colormap picker changes palette live</span>")
    return pn.pane.Markdown(md, sizing_mode="stretch_width")


# ───────────────────────────────────────────────────────────────────────────
#  LAYOUT
# ───────────────────────────────────────────────────────────────────────────

def _render_layout(ds, var, all_plots, group_by, frequency, channel,
                   x_override, y_override, vmin, vmax, cmap, decimate,
                   ymin, ymax, width, height, toolbar,
                   flip_y=True, show_hover=True, show_crosshair=True,
                   show_cmap_picker=True, show_log=True):
    x_name, y_name = _detect_axes(ds)
    if x_override: x_name = x_override
    if y_override: y_name = y_override

    if flip_y:
        flip_y = _should_flip_y(y_name)
        logger.info(f"Y-axis '{y_name}' → {'inverted' if flip_y else 'normal'}")

    header   = _build_header(ds, var, x_name, y_name, flip_y)
    controls = _build_cmap_picker(cmap) if show_cmap_picker else None

    if all_plots:
        body = _build_all_tabs(ds, var, x_name, y_name, group_by, cmap, vmin, vmax,
                               width, height, toolbar, decimate, ymin, ymax,
                               flip_y, show_hover, show_crosshair)
    else:
        body = _build_single_plot(ds, var, x_name, y_name, frequency, channel,
                                  cmap, vmin, vmax, width, height, toolbar,
                                  decimate, ymin, ymax, flip_y, show_hover, show_crosshair)

    header_row = pn.Row(header, controls, sizing_mode="stretch_width") if controls else header
    parts = [header_row, body]

    if show_log:
        parts += [pn.Spacer(height=8), _build_data_log(ds, var, x_name, y_name, flip_y)]

    return pn.Column(*parts, sizing_mode="stretch_width")


# ───────────────────────────────────────────────────────────────────────────
#  CLI
# ───────────────────────────────────────────────────────────────────────────

def main() -> None:
    if len(sys.argv) == 1:
        token = _read_input_path_from_stdin()
        if token:
            sys.argv.append(token)
        else:
            print_help()
            raise SystemExit(0)

    p = argparse.ArgumentParser(add_help=False)
    p.add_argument("input_path", type=Path, nargs="?")
    p.add_argument("--var"); p.add_argument("--all", action="store_true")
    p.add_argument("--frequency", type=float); p.add_argument("--channel")
    p.add_argument("--group-by", default="auto", choices=["auto","channel","freq"])
    p.add_argument("--x", dest="x_override"); p.add_argument("--y", dest="y_override")
    p.add_argument("--no-flip", action="store_true")
    p.add_argument("--vmin", type=float); p.add_argument("--vmax", type=float)
    p.add_argument("--cmap", default="inferno")
    p.add_argument("--width", type=int, default=1200); p.add_argument("--height", type=int, default=450)
    p.add_argument("--toolbar", default="above",
                   choices=["above","below","left","right","disable"])
    p.add_argument("--no-hover", action="store_true")
    p.add_argument("--no-crosshair", action="store_true")
    p.add_argument("--no-cmap-picker", action="store_true")
    p.add_argument("--no-log", action="store_true")
    p.add_argument("--decimate", type=int, default=1)
    p.add_argument("--ymin", type=float); p.add_argument("--ymax", type=float)
    p.add_argument("-o", "--output_path", type=Path)
    p.add_argument("--no-overwrite", action="store_true")
    p.add_argument("--quiet", action="store_true")
    p.add_argument("-h", "--help", action="store_true")
    args = p.parse_args()

    if args.help:
        print_help(); raise SystemExit(0)

    _configure_logging(args.quiet)

    if args.input_path is None:
        token = _read_input_path_from_stdin()
        if not token:
            logger.error("No INPUT_PATH provided."); raise SystemExit(2)
        args.input_path = Path(token)

    if not args.input_path.exists():
        logger.error(f"File not found: {args.input_path}"); raise SystemExit(1)

    if args.output_path is None:
        args.output_path = args.input_path.with_stem(args.input_path.stem + "_plot").with_suffix(".html")

    if args.output_path.exists() and args.no_overwrite:
        logger.error(f"Output exists: {args.output_path}"); raise SystemExit(1)

    if args.all and (args.frequency or args.channel):
        logger.error("Use --all OR --frequency/--channel, not both."); raise SystemExit(2)

    try:
        buf = io.StringIO()
        with redirect_stdout(buf), redirect_stderr(buf):
            ds = xr.open_dataset(args.input_path)
        ds.encoding["source"] = str(args.input_path)
        var = _ensure_variable(ds, args.var)
        logger.info(f"Plotting '{var}' from {args.input_path.name}")

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
        )

        args.output_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"Saving HTML → {args.output_path}")
        pn.io.save.save(layout, filename=str(args.output_path),
                        embed=True, resources="inline", title="aa-plot echogram")
        print(args.output_path.resolve())

    except Exception as e:
        logger.exception(f"aa-plot failed: {e}"); raise SystemExit(1)


if __name__ == "__main__":
    main()