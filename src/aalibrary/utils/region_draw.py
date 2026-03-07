#!/usr/bin/env python3
"""
aa_evr_draw.py  —  Interactive echogram region drawing via Bokeh.

Called by aa_evr.py when --evr is NOT supplied (drawing mode).

Pipeline usage:
    cat D20191001-T003423_Sv_depth.nc | aa-evr --name my_region.evr

Produces:
    1. <evr_name>.evr  — Echoview EVR file with the drawn polygons.
    2. <input_stem>_evr.nc  — Masked NetCDF (cells inside regions kept,
       rest NaN); path is emitted to stdout for downstream piping.

Design notes:
  - All Bokeh imports are deferred (inside run_drawing_mode) so that
    importing this module never fails if Bokeh is absent.
  - The mask is built directly from the in-memory drawn polygon coordinates
    (matplotlib.path point-in-polygon); no echoregions round-trip is needed.
  - The EVR file is still written so it can be re-used with aa-evr --evr.
  - Coordinates are normalised before point-in-polygon tests for numerical
    stability when time values are large millisecond-since-epoch integers.
  - Echogram image: EK500-inspired 25-stop colormap; NaN bins are
    transparent (rendered as dark background).
  - Y-axis is inverted so depth 0 (surface) is at the top of the plot,
    matching standard echogram convention.
"""

import socket
import threading
import webbrowser
from pathlib import Path
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd
import xarray as xr
from loguru import logger


# ══════════════════════════════════════════════════════════════
# Port helper
# ══════════════════════════════════════════════════════════════

def _find_free_port(start: int = 5006) -> int:
    """Return the first free TCP port at or after *start*."""
    for port in range(start, start + 100):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex(("localhost", port)) != 0:
                return port
    raise RuntimeError("No free port found in range 5006–5105.")


# ══════════════════════════════════════════════════════════════
# EVR writer
# ══════════════════════════════════════════════════════════════

def _fmt_evr_time(t: "np.datetime64") -> str:
    """Return 'YYYYMMDD HHMMSS 0000' for a datetime64 value."""
    ts = pd.Timestamp(t)
    return ts.strftime("%Y%m%d %H%M%S") + " 0000"


def write_evr(
    evr_path: Path,
    polygons: List[Tuple[np.ndarray, np.ndarray]],
) -> None:
    """
    Write a minimal EVRG-7 EVR file readable by echoregions.

    Parameters
    ----------
    evr_path : Path
        Destination file.
    polygons : list of (times, depths)
        times  – 1-D datetime64 array (polygon vertices)
        depths – 1-D float array, metres (same length as times)

    Polygons are auto-closed (first vertex appended if not already equal
    to last vertex).  Degenerate polygons (< 2 points) are skipped.
    """
    valid: List[Tuple[List, List]] = []
    for t_arr, d_arr in polygons:
        t_list = list(t_arr)
        d_list = list(d_arr)
        if len(t_list) < 2:
            logger.debug("Skipped degenerate polygon (< 2 vertices).")
            continue
        # close polygon
        if (np.datetime64(t_list[0], "ms") != np.datetime64(t_list[-1], "ms")
                or d_list[0] != d_list[-1]):
            t_list.append(t_list[0])
            d_list.append(d_list[0])
        valid.append((t_list, d_list))

    if not valid:
        raise ValueError("No valid polygons to write (need ≥ 2 distinct vertices).")

    lines: List[str] = ["EVRG 7 7.0.0.0", str(len(valid))]

    for idx, (t_list, d_list) in enumerate(valid, 1):
        n = len(t_list)
        lines += [
            f"13 {n} 0 -1",
            "0",
            "",
            f"Region {idx} -1 -1 0 0 3 0",
            "0",
            "",
            str(n),
        ]
        lines += [_fmt_evr_time(np.datetime64(t, "ms")) for t in t_list]
        lines += [str(n)]
        lines += [f"{float(d):.6f}" for d in d_list]

    evr_path.write_text("\n".join(lines) + "\n")
    logger.info(f"EVR written → {evr_path}  ({len(valid)} region(s))")


# ══════════════════════════════════════════════════════════════
# Echogram loader
# ══════════════════════════════════════════════════════════════

def _load_echogram(
    nc_path: Path,
    var: str = "Sv",
    time_dim: Optional[str] = None,
    depth_dim: Optional[str] = None,
    channel_index: int = 0,
) -> Tuple[xr.Dataset, np.ndarray, np.ndarray, np.ndarray, str, str]:
    """
    Open a NetCDF echogram and extract a 2-D Sv array.

    Returns
    -------
    ds          : xr.Dataset (kept open; caller must close)
    sv_2d       : float ndarray, shape (n_time, n_depth)
    time_vals   : datetime64 ndarray, shape (n_time,)
    depth_vals  : float ndarray, shape (n_depth,)  [metres, ascending]
    tdim        : inferred time-dimension name
    ddim        : inferred depth-dimension name
    """
    ds = xr.open_dataset(nc_path)

    if var not in ds.data_vars:
        raise ValueError(
            f"Variable '{var}' not found.  Available: {list(ds.data_vars)}"
        )

    da = ds[var]

    # ── infer time dim ──
    if time_dim:
        tdim = time_dim
    elif "ping_time" in da.dims:
        tdim = "ping_time"
    elif "time" in da.dims:
        tdim = "time"
    else:
        raise ValueError(
            f"Cannot infer time dimension from {list(da.dims)}.  "
            "Use --time-dim."
        )

    # ── infer depth dim ──
    if depth_dim:
        ddim = depth_dim
    elif "depth" in da.dims:
        ddim = "depth"
    elif "range_sample" in da.dims:
        ddim = "range_sample"
    elif "range_bin" in da.dims:
        ddim = "range_bin"
    else:
        raise ValueError(
            f"Cannot infer depth dimension from {list(da.dims)}.  "
            "Use --depth-dim."
        )

    # ── strip channel ──
    if "channel" in da.dims:
        ci = min(channel_index, da.sizes["channel"] - 1)
        da = da.isel(channel=ci)
        try:
            da = da.drop_vars("channel")
        except Exception:
            pass

    try:
        da = da.compute()
    except Exception:
        pass

    # ── time coordinate ──
    time_vals = (
        da.coords[tdim].values
        if tdim in da.coords
        else ds.coords[tdim].values
    )

    # ── physical depth coordinate (prefer echo_range or depth variable) ──
    depth_vals: Optional[np.ndarray] = None
    for cname in ("echo_range", "depth"):
        src = ds[cname] if cname in ds else None
        if src is None and cname in da.coords:
            src = da.coords[cname]
        if src is None:
            continue
        if "channel" in src.dims:
            try:
                src = src.isel(channel=channel_index)
            except Exception:
                continue
        if ddim in src.dims and tdim not in src.dims:
            depth_vals = np.asarray(src.values, dtype=float)
            break
        if ddim in src.dims and tdim in src.dims:
            depth_vals = np.asarray(src.isel({tdim: 0}).values, dtype=float)
            break

    if depth_vals is None:
        depth_vals = (
            np.asarray(da.coords[ddim].values, dtype=float)
            if ddim in da.coords
            else np.arange(da.sizes[ddim], dtype=float)
        )

    # ── 2-D Sv array, shape (n_time, n_depth) ──
    sv_2d = np.asarray(da.transpose(tdim, ddim).values, dtype=float)

    # Ensure depth increases with index (surface first, then descending)
    if len(depth_vals) > 1 and depth_vals[-1] < depth_vals[0]:
        depth_vals = depth_vals[::-1].copy()
        sv_2d = sv_2d[:, ::-1].copy()

    return ds, sv_2d, time_vals, depth_vals, tdim, ddim


# ══════════════════════════════════════════════════════════════
# Echogram colourmap  (EK500-inspired 25-stop palette)
# ══════════════════════════════════════════════════════════════

_EK500_HEX = [
    "#ffffff", "#d4ebf7", "#a8d3ed", "#7cb4de", "#508dcf",
    "#2460b0", "#0a3d8f", "#006400", "#228b22", "#32cd32",
    "#7fff00", "#adff2f", "#ffff00", "#ffd700", "#ffa500",
    "#ff7f00", "#ff4500", "#ff0000", "#cc0000", "#990000",
    "#660000", "#330000", "#1a0000", "#0d0000", "#000000",
]

_PALETTE_RGB = np.array(
    [[int(h[1:3], 16), int(h[3:5], 16), int(h[5:7], 16)] for h in _EK500_HEX],
    dtype=np.uint8,
)


def _sv_to_rgba_uint32(
    sv_2d: np.ndarray,
    vmin: float = -82.0,
    vmax: float = -30.0,
) -> np.ndarray:
    """
    Map a (n_time, n_depth) Sv array to a (n_depth, n_time) uint32 RGBA image.

    Row 0 of the output = shallowest depth (surface), which Bokeh
    image_rgba places at the lowest y data-coordinate.  With the plot's
    inverted y_range (d_max at bottom, d_min=0 at top) that lowest y
    therefore appears at the TOP of the screen — correct for echograms.

    NaN cells are rendered with alpha=0 (transparent).
    """
    n_stops = len(_EK500_HEX)

    # Transpose to (n_depth, n_time) — rows = depth bins
    sv_t = sv_2d.T.copy()
    nan_mask = ~np.isfinite(sv_t)
    normed = np.clip((sv_t - vmin) / (vmax - vmin), 0.0, 1.0)
    normed[nan_mask] = 0.0

    idx = np.round(normed * (n_stops - 1)).astype(np.int32)
    rgb = _PALETTE_RGB[idx]                             # (n_depth, n_time, 3)
    alpha = np.where(nan_mask, np.uint8(0), np.uint8(255))

    img = np.empty(sv_t.shape, dtype=np.uint32)
    view = img.view(np.uint8).reshape(*sv_t.shape, 4)
    view[..., 0] = rgb[..., 0]   # R
    view[..., 1] = rgb[..., 1]   # G
    view[..., 2] = rgb[..., 2]   # B
    view[..., 3] = alpha
    return img                                          # (n_depth, n_time)


# ══════════════════════════════════════════════════════════════
# Polygon → 2-D boolean mask
# ══════════════════════════════════════════════════════════════

def _polygon_mask_2d(
    time_ms: np.ndarray,
    depth_vals: np.ndarray,
    polygons: List[Tuple[np.ndarray, np.ndarray]],
) -> np.ndarray:
    """
    Return a (n_time, n_depth) boolean mask.  True = inside at least one polygon.

    Coordinates are normalised to [0, 1] before the point-in-polygon test
    for numerical stability (time values are large ms-since-epoch integers).

    Requires matplotlib (pure-Python fallback not provided; matplotlib is a
    standard scientific-Python dependency alongside xarray).
    """
    try:
        from matplotlib.path import Path as MplPath
    except ImportError:
        raise ImportError(
            "matplotlib is required for mask building. "
            "Install with:  pip install matplotlib"
        )

    t_min, t_max = float(time_ms.min()), float(time_ms.max())
    d_min, d_max = float(depth_vals.min()), float(depth_vals.max())
    t_span = max(t_max - t_min, 1.0)
    d_span = max(d_max - d_min, 1e-9)

    def _nt(t):  return (np.asarray(t, dtype=float) - t_min) / t_span
    def _nd(d):  return (np.asarray(d, dtype=float) - d_min) / d_span

    # Grid of normalised (time, depth) pairs — shape (n_time*n_depth, 2)
    T_n = _nt(time_ms)[:, np.newaxis] * np.ones((1, len(depth_vals)))  # (T, D)
    D_n = np.ones((len(time_ms), 1)) * _nd(depth_vals)[np.newaxis, :]  # (T, D)
    pts = np.stack([T_n.ravel(), D_n.ravel()], axis=1)

    mask = np.zeros(len(time_ms) * len(depth_vals), dtype=bool)

    for t_arr, d_arr in polygons:
        t_ms_poly = t_arr.astype("datetime64[ms]").astype(np.int64).astype(float)
        poly = np.stack([_nt(t_ms_poly), _nd(d_arr)], axis=1)
        if len(poly) < 3:
            logger.debug("Skipping polygon with < 3 points in mask step.")
            continue
        path = MplPath(poly)
        mask |= path.contains_points(pts)

    return mask.reshape(len(time_ms), len(depth_vals))


# ══════════════════════════════════════════════════════════════
# Main entry point
# ══════════════════════════════════════════════════════════════

def run_drawing_mode(
    nc_path: Path,
    evr_name: str,
    out_dir: Optional[Path],
    var: str,
    time_dim: Optional[str],
    depth_dim: Optional[str],
    channel_index: int,
    overwrite: bool,
    port: int,
    debug: bool,
) -> Optional[Path]:
    """
    Launch the Bokeh drawing app and block until the user saves regions.

    On success, returns the Path of the masked NetCDF output (also printed
    to stdout by the caller for pipeline chaining).  Returns None on failure.

    Stdout / stderr contract (mirrors aa_evr.py):
      - Output NC path → stdout (print by caller).
      - All log messages → stderr (loguru).
    """
    # ── deferred Bokeh import ──
    try:
        from bokeh.server.server import Server
        from bokeh.application import Application
        from bokeh.application.handlers.function import FunctionHandler
        from bokeh.plotting import figure
        from bokeh.models import (
            ColumnDataSource,
            FreehandDrawTool,
            Range1d,
            Button,
            Div,
        )
        from bokeh.layouts import column, row as bk_row
    except ImportError:
        logger.error(
            "Bokeh is required for drawing mode.  "
            "Install with:  pip install bokeh"
        )
        return None

    # ── output paths ──
    out_base = out_dir or nc_path.parent
    out_base.mkdir(parents=True, exist_ok=True)

    stem = nc_path.with_suffix("").name
    evr_stem = evr_name if evr_name.endswith(".evr") else evr_name + ".evr"
    evr_out  = out_base / evr_stem
    nc_out   = out_base / (stem + "_evr.nc")

    if evr_out.exists() and not overwrite:
        logger.error(f"EVR output already exists (use --overwrite): {evr_out}")
        return None
    if nc_out.exists() and not overwrite:
        logger.error(f"NC output already exists (use --overwrite): {nc_out}")
        return None

    # ── load echogram ──
    logger.info(f"Loading echogram: {nc_path}")
    try:
        ds, sv_2d, time_vals, depth_vals, tdim, ddim = _load_echogram(
            nc_path, var, time_dim, depth_dim, channel_index
        )
    except Exception as exc:
        logger.error(f"Failed to load echogram: {exc}")
        return None

    n_time, n_depth = sv_2d.shape
    logger.info(
        f"Echogram: {n_time} pings × {n_depth} depth bins | "
        f"depth {depth_vals[0]:.1f}–{depth_vals[-1]:.1f} m"
    )

    # ── convert time to ms-since-epoch (Bokeh datetime axis convention) ──
    time_ms = (
        time_vals.astype("datetime64[ms]").astype(np.int64).astype(float)
    )
    t0, t1 = float(time_ms.min()), float(time_ms.max())
    d0, d1 = float(depth_vals.min()), float(depth_vals.max())
    pad_t   = max((t1 - t0) * 0.005, 1.0)
    pad_d   = max((d1 - d0) * 0.02,  0.5)

    # ── echogram RGBA image, shape (n_depth, n_time) ──
    img_rgba = _sv_to_rgba_uint32(sv_2d)

    # ── shared state ──
    shared: dict = {"polygons": None}
    done_event = threading.Event()

    # ── Bokeh document factory ──
    def make_doc(doc):  # noqa: C901  (complexity is intentional — single callback scope)

        # ── figure ──
        # y_range is inverted: d1 (deepest) maps to screen-bottom,
        # d0 (surface) maps to screen-top — standard echogram orientation.
        p = figure(
            title=(
                "aa-evr  ·  Draw ROI regions  ·  "
                "Freehand Draw Tool active by default"
            ),
            width=1300,
            height=600,
            x_axis_type="datetime",
            x_range=Range1d(t0 - pad_t, t1 + pad_t),
            y_range=Range1d(d1 + pad_d, d0 - pad_d),  # inverted
            tools="pan,wheel_zoom,reset",
            toolbar_location="above",
        )
        p.xaxis.axis_label = "Time (UTC)"
        p.yaxis.axis_label = "Depth (m)"
        p.background_fill_color = "#111118"
        p.border_fill_color     = "#1a1a2e"
        p.grid.grid_line_color  = "#2a2a44"
        p.grid.grid_line_alpha  = 0.6
        p.title.text_color      = "#cdd6f4"
        p.title.text_font_size  = "13px"

        # ── echogram image ──
        # image_rgba places row-0 of arr at (x=t0, y=d0), which with the
        # inverted y_range renders at the TOP of the screen (surface). ✓
        p.image_rgba(
            image=[img_rgba],
            x=t0,
            y=d0,
            dw=t1 - t0,
            dh=d1 - d0,
        )

        # ── freehand draw layer ──
        draw_src = ColumnDataSource({"xs": [], "ys": []})
        ml = p.multi_line(
            "xs", "ys",
            source=draw_src,
            line_color="lime",
            line_width=2.2,
            line_alpha=0.9,
        )
        draw_tool = FreehandDrawTool(renderers=[ml], num_objects=200)
        p.add_tools(draw_tool)
        p.toolbar.active_drag = draw_tool

        # ── UI widgets ──
        hint = Div(text=(
            '<div style="'
            "font-size:13px;padding:7px 12px;"
            "background:#1e1e3a;color:#cdd6f4;"
            "border-left:3px solid #a6e3a1;border-radius:3px;"
            '">'
            "🖊 <b>Freehand Draw</b> is active — click and drag to draw a region.  "
            "Draw multiple regions if needed.  "
            "Use <kbd>Backspace / Delete</kbd> to remove the last shape.  "
            'Click <b style="color:#a6e3a1">Save Regions &amp; Exit</b> when done.'
            "</div>"
        ))

        status = Div(text="")

        count_div = Div(text=(
            '<span style="color:#89dceb;font-size:13px;line-height:32px">'
            "Regions drawn: <b>0</b></span>"
        ))

        save_btn  = Button(label="💾  Save Regions & Exit",
                           button_type="success", width=230)
        clear_btn = Button(label="🗑  Clear All",
                           button_type="warning", width=130)

        def _refresh_count(attr, old, new):
            n = sum(1 for x in new.get("xs", []) if len(x) >= 2)
            count_div.text = (
                f'<span style="color:#89dceb;font-size:13px;line-height:32px">'
                f"Regions drawn: <b>{n}</b></span>"
            )

        draw_src.on_change("data", _refresh_count)

        def on_clear():
            draw_src.data = {"xs": [], "ys": []}
            status.text = (
                '<div style="color:#fab387;font-size:13px;padding:4px 0">'
                "🗑 Cleared all regions.</div>"
            )

        def on_save():
            xs = list(draw_src.data.get("xs", []))
            ys = list(draw_src.data.get("ys", []))

            polygons = []
            for x_list, y_list in zip(xs, ys):
                if len(x_list) < 2:
                    continue
                t_arr = np.array(
                    [np.datetime64(int(v), "ms") for v in x_list],
                    dtype="datetime64[ms]",
                )
                d_arr = np.asarray(y_list, dtype=float)
                polygons.append((t_arr, d_arr))

            if not polygons:
                status.text = (
                    '<div style="color:#f38ba8;font-size:13px;padding:4px 0">'
                    "⚠  No valid regions yet — draw at least one region first."
                    "</div>"
                )
                return

            shared["polygons"] = polygons
            status.text = (
                f'<div style="color:#a6e3a1;font-size:13px;padding:4px 0">'
                f"✓ {len(polygons)} region(s) captured.  "
                f"Processing outputs…  You can close this tab."
                f"</div>"
            )
            # signal the IOLoop-polling checker in the main thread
            done_event.set()

        clear_btn.on_click(on_clear)
        save_btn.on_click(on_save)

        controls = bk_row(save_btn, clear_btn, count_div)
        doc.add_root(column(hint, controls, status, p))
        doc.title = "aa-evr · Draw Regions"

    # ── start Bokeh server ──
    port = _find_free_port(port)
    server = Server(
        {"/" : Application(FunctionHandler(make_doc))},
        port=port,
        num_procs=1,
        allow_websocket_origin=[f"localhost:{port}", f"127.0.0.1:{port}"],
    )
    server.start()
    url = f"http://localhost:{port}"
    logger.info(f"Drawing app ready at:  {url}")
    logger.info("Draw your regions, then click 'Save Regions & Exit'.")

    server.io_loop.add_callback(webbrowser.open, url)

    # Periodic poller — stops the IOLoop once done_event fires
    def _poll():
        if done_event.is_set():
            server.io_loop.stop()
        else:
            server.io_loop.call_later(0.25, _poll)

    server.io_loop.add_callback(_poll)

    try:
        server.io_loop.start()          # blocks until _poll stops it
    except KeyboardInterrupt:
        logger.warning("Drawing mode interrupted (Ctrl-C).")
        try:
            server.stop()
        except Exception:
            pass
        try:
            ds.close()
        except Exception:
            pass
        return None

    try:
        server.stop()
    except Exception:
        pass

    # ── process drawn polygons ──
    polygons = shared.get("polygons")
    if not polygons:
        logger.error("No regions were captured — nothing to save.")
        try:
            ds.close()
        except Exception:
            pass
        return None

    logger.info(f"Captured {len(polygons)} polygon(s). Building mask…")

    # Write EVR (best-effort; failure here should not abort the NC output)
    evr_written = False
    try:
        write_evr(evr_out, polygons)
        evr_written = True
    except Exception as exc:
        logger.error(f"EVR write failed: {exc}")

    # Build mask from drawn coordinates (no echoregions round-trip)
    try:
        mask_2d = _polygon_mask_2d(time_ms, depth_vals, polygons)
    except Exception as exc:
        logger.error(f"Mask building failed: {exc}")
        try:
            ds.close()
        except Exception:
            pass
        return None

    inside = int(mask_2d.sum())
    total  = int(mask_2d.size)
    pct    = 100.0 * inside / max(total, 1)
    logger.info(f"Mask coverage: {inside}/{total} cells ({pct:.1f}%)")

    if inside == 0:
        logger.warning(
            "Mask is entirely empty — output NetCDF will be all-NaN. "
            "Check that drawn regions overlap the echogram extent."
        )

    # Apply mask to all (tdim, ddim) variables
    ds_out = ds.copy(deep=False)
    for name, da in list(ds_out.data_vars.items()):
        if (tdim in da.dims) and (ddim in da.dims):
            bare = xr.DataArray(mask_2d, dims=(tdim, ddim))
            ds_out[name] = xr.where(bare.broadcast_like(da), da, np.nan)

    ds_out.attrs["aa_tool"]      = "aa-evr-draw"
    ds_out.attrs["aa_evr_files"] = str(evr_out) if evr_written else "not_written"

    try:
        ds_out.to_netcdf(nc_out)
    except Exception as exc:
        logger.error(f"Failed to write masked NetCDF: {exc}")
        try:
            ds.close()
        except Exception:
            pass
        return None

    try:
        ds.close()
    except Exception:
        pass

    if evr_written:
        logger.success(f"EVR saved        → {evr_out}")
    logger.success(f"Masked NC saved  → {nc_out}")
    return nc_out
