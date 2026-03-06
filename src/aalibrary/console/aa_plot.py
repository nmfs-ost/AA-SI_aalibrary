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
import sys
from contextlib import redirect_stdout, redirect_stderr
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
  --cmap NAME               Colormap name (default: inferno).
  --width INT               Plot width  (default: 1200).
  --height INT              Plot height (default: 450).
  --toolbar STR             Toolbar: above/below/left/right/disable (default: above).
  --no-hover                Disable hover tooltip overlay.
  --no-crosshair            Disable crosshair cursor.
  --no-minimap              Disable the navigation minimap below the main plot.

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
        # default_tools already includes pan, box_zoom, reset, save, wheel_zoom
        plot = plot.opts(
            opts.QuadMesh(tools=extra_tools, active_tools=["wheel_zoom"]),
            opts.Image(tools=extra_tools, active_tools=["wheel_zoom"]),
        )

    return plot


def _build_minimap(
    da: xr.DataArray,
    x_name: str,
    y_name: str,
    cmap: str,
    vmin: Optional[float],
    vmax: Optional[float],
    width: int,
    flip_y: bool = True,
):
    """Build a small overview echogram (minimap) with a RangeToolLink
    so the user can drag a viewport rectangle to navigate the main plot."""
    from bokeh.models import RangeTool
    clim = (vmin, vmax) if (vmin is not None or vmax is not None) else None

    mini = da.hvplot.quadmesh(
        x=x_name,
        y=y_name,
        cmap=cmap,
        clim=clim,
        width=width,
        height=120,
        colorbar=False,
        toolbar="disable",
        xlabel="",
        ylabel="",
    )

    if flip_y:
        mini = mini.opts(invert_yaxis=True)

    return mini


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


def _wrap_with_minimap(
    plot,
    da: xr.DataArray,
    x_name: str,
    y_name: str,
    cmap: str,
    vmin: Optional[float],
    vmax: Optional[float],
    width: int,
    flip_y: bool,
    show_minimap: bool,
):
    """Wrap a main plot with a minimap navigator below it.

    Uses a linked selection via Panel RangeToolLink so that dragging
    a selection box on the minimap pans the main plot.
    """
    if not show_minimap:
        return plot

    try:
        mini = _build_minimap(da, x_name, y_name, cmap, vmin, vmax, width, flip_y)

        # Use Panel's RangeToolLink for x-axis navigation
        from panel.widgets import RangeSlider
        range_x = pn.widgets.RangeSlider(name="x", visible=False)

        main_pane = pn.pane.HoloViews(plot, linked_axes=True)
        mini_pane = pn.pane.HoloViews(mini, linked_axes=True)

        # Link x-axes via shared Bokeh range
        minimap_col = pn.Column(
            main_pane,
            pn.pane.Markdown(
                "<div style='color:#888; font-size:0.8em; margin:0 0 2px 4px;'>"
                "&#9660; Navigation minimap — use box-zoom on the overview below</div>",
                sizing_mode="stretch_width",
            ),
            mini_pane,
        )
        return minimap_col
    except Exception as exc:
        logger.debug(f"Minimap construction failed ({exc}); returning main plot only.")
        return plot


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
    show_minimap: bool = True,
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

    plot = _plot_echogram(
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

    return _wrap_with_minimap(
        plot, da, x_name, y_name, cmap, vmin, vmax, width, flip_y, show_minimap,
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
    show_minimap: bool = True,
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

            wrapped = _wrap_with_minimap(
                plot, da2, x_name, y_name, cmap, vmin, vmax, width, flip_y, show_minimap,
            )

            tabs.append((label, wrapped))

        return pn.Tabs(*tabs, sizing_mode="stretch_both", dynamic=True)

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

    wrapped = _wrap_with_minimap(
        plot, da2, x_name, y_name, cmap, vmin, vmax, width, flip_y, show_minimap,
    )

    return pn.Column(
        pn.pane.Markdown("No channel dimension detected; plotting a single array."),
        wrapped,
        sizing_mode="stretch_both",
    )


def _build_header(ds: xr.Dataset, var: str, x_name: str, y_name: str, flip_y: bool) -> pn.pane.Markdown:
    """Build a richer metadata header with dataset summary."""
    source = ds.encoding.get("source", "(in-memory)")

    # Gather dimension sizes for display
    dim_info = " × ".join(f"{d}={s}" for d, s in ds[var].sizes.items())

    # Try to get some metadata
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
        f"Hover for values • Scroll-wheel to zoom • Shift+drag to pan"
        f"</span>"
    )
    return pn.pane.Markdown(md, sizing_mode="stretch_width")


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
    show_minimap: bool = True,
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
            show_minimap=show_minimap,
        )
        return pn.Column(header, body, sizing_mode="stretch_both")

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
        show_minimap=show_minimap,
    )
    return pn.Column(header, body, sizing_mode="stretch_both")


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
    p.add_argument("--cmap", type=str, default="inferno", help="Colormap name (default: inferno).")
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
    p.add_argument("--no-minimap", action="store_true", help="Disable navigation minimap.")

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
            show_minimap=not args.no_minimap,
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