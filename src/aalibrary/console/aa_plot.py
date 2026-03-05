#!/usr/bin/env python3
"""
aa-plot — Interactive echogram plotting (HTML) for Echopype/xarray NetCDF datasets

Goals:
- Accept .nc path from argv OR stdin (pipeline-friendly).
- Plot a variable (default: Sv if present).
- Plot ALL channels and/or frequencies in a tabbed UI (Panel + hvPlot).
  * If Sv has a 'channel' dimension, tabs are per-channel (labels include frequency_nominal if present).
  * If Sv has a 'frequency_nominal' dimension, tabs are per-frequency.
  * If BOTH are dimensions, tabs are nested: frequency -> channel.
- No matplotlib. Output is standalone HTML.
- Print absolute HTML path to stdout for downstream chaining.
- Keep stdout clean except for final path (logs go to stderr via loguru).

Examples:
  aa-plot data.nc --all -o plots/echogram.html
  aa-nc raw.raw | aa-sv | aa-clean | aa-plot --all
  aa-plot data.nc --var Sv --channel "GPT 38 kHz 0090720d" -o one.html
  aa-plot data.nc --all --group-by freq   # force outer tabs by frequency when possible
"""

from __future__ import annotations

import argparse
import io
import sys
from contextlib import redirect_stdout, redirect_stderr
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List, Tuple, Any, Dict

import xarray as xr
from loguru import logger

# Interactive stack (no matplotlib)
import hvplot.xarray  # noqa: F401 registers hvplot accessor
import panel as pn

pn.extension()


# ---------------------------
# CLI help
# ---------------------------

def print_help() -> None:
    help_text = r"""
Usage: aa-plot [OPTIONS] [INPUT_PATH]

Arguments:
  INPUT_PATH                Path to a NetCDF file (.nc). Optional; if omitted,
                            reads a single path token from stdin.

Core selection:
  --var VAR                 Variable to plot (default: Sv if present, else first data_var).
  --all                     Plot all channels/frequencies as tabs (recommended).
  --frequency FLOAT         Select single nominal frequency (Hz) (nearest match).
  --channel NAME            Select single channel by name (exact match preferred).
  --group-by {auto,channel,freq}
                            When --all and both channel+frequency are available:
                              auto   -> frequency outer tabs, channel inner tabs (default)
                              channel-> channel outer tabs, frequency inner tabs
                              freq   -> frequency outer tabs, channel inner tabs

Axes:
  --x NAME                  Override x-axis dim/coord (default: auto-detect).
  --y NAME                  Override y-axis dim/coord (default: auto-detect).

Appearance:
  --vmin FLOAT              Lower color limit (e.g., -90).
  --vmax FLOAT              Upper color limit (e.g., -40).
  --cmap NAME               Colormap name (default: inferno).
  --width INT               Plot width  (default: 1200).
  --height INT              Plot height (default: 450).
  --toolbar STR             Toolbar: above/below/left/right/disable (default: above).

Subsetting / performance:
  --decimate INT            Take every Nth sample along x-axis to speed up (default: 1).
  --ymin FLOAT              Crop lower y-limit.
  --ymax FLOAT              Crop upper y-limit.

Output:
  -o, --output_path PATH    Output HTML path (default: <stem>_plot.html).
  --no-overwrite            Fail if output already exists.
  --quiet                   Suppress info logs; still prints final path.
  -h, --help                Show this help and exit.

Notes:
  • Writes an interactive HTML file (Bokeh/Panel) and prints its absolute path.
  • Pipes pass filenames (not bytes). Example:
        aa-nc raw.raw --sonar_model EK60 | aa-sv | aa-clean | aa-plot --all
"""
    print(help_text.strip())


# ---------------------------
# Generic utilities
# ---------------------------

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
    """Robust-ish stringification for coords that may be bytes/numpy scalars."""
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
    # Works for numeric coords
    return int(abs(coord - target).argmin().item())


def _get_freq_coord_for_channel(ds: xr.Dataset, chan_dim: str) -> Optional[xr.DataArray]:
    """
    Many echopype datasets store frequency_nominal as a coord on the channel dimension:
      frequency_nominal(channel) -> values in Hz
    Return that coord if present and indexed by chan_dim.
    """
    if "frequency_nominal" not in ds.coords and "frequency_nominal" not in ds.variables:
        return None
    try:
        f = ds["frequency_nominal"]
        if chan_dim in f.dims:
            return f
    except Exception:
        return None
    return None


def _label_channel(ds: xr.Dataset, chan_dim: str, chan_value: Any) -> str:
    """
    Build a nice tab label for a channel. If frequency_nominal(channel) exists,
    append it to the label.
    """
    ch = _coord_to_str(chan_value)
    fcoord = _get_freq_coord_for_channel(ds, chan_dim)
    if fcoord is not None:
        # find matching index by value if possible; else caller should pass index-based selection
        # We'll let caller pass chan_value from coord.isel; then align by isel index there.
        return ch  # base; caller will add freq if it has index
    return ch


@dataclass
class SliceSpec:
    label: str
    da: xr.DataArray


# ---------------------------
# Slice planning
# ---------------------------

def _available_dims_for_var(ds: xr.Dataset, var: str) -> Tuple[bool, bool]:
    da = ds[var]
    has_chan = "channel" in da.dims or "channel" in ds.dims
    has_freq_dim = "frequency_nominal" in da.dims
    return has_chan, has_freq_dim


def _make_single_selection(
    ds: xr.Dataset,
    var: str,
    frequency: Optional[float],
    channel: Optional[str],
) -> List[SliceSpec]:
    da = ds[var]

    chan_dim = "channel" if "channel" in da.dims else None
    freq_dim = "frequency_nominal" if "frequency_nominal" in da.dims else None

    # Channel selection
    if channel is not None and chan_dim is not None:
        coord = ds[chan_dim]
        vals = [_coord_to_str(v) for v in coord.values]
        idx = vals.index(channel) if channel in vals else 0
        ch_val = coord.isel({chan_dim: idx}).values
        label = f"channel={_coord_to_str(ch_val)}"
        fcoord = _get_freq_coord_for_channel(ds, chan_dim)
        if fcoord is not None:
            f_val = fcoord.isel({chan_dim: idx}).values
            label = f"{_coord_to_str(ch_val)} • {_coord_to_str(f_val)} Hz"
        return [SliceSpec(label, da.isel({chan_dim: idx}))]

    # Frequency selection (dimension)
    if frequency is not None and freq_dim is not None:
        fcoord = ds[freq_dim]
        idxf = _nearest_index(fcoord, frequency)
        f_val = fcoord.isel({freq_dim: idxf}).values
        label = f"frequency~{_coord_to_str(f_val)}"
        return [SliceSpec(label, da.isel({freq_dim: idxf}))]

    # Frequency selection (coord on channel)
    if frequency is not None and chan_dim is not None:
        fcoord = _get_freq_coord_for_channel(ds, chan_dim)
        if fcoord is not None:
            idx = _nearest_index(fcoord, frequency)
            ch_val = ds[chan_dim].isel({chan_dim: idx}).values
            f_val = fcoord.isel({chan_dim: idx}).values
            label = f"{_coord_to_str(ch_val)} • {_coord_to_str(f_val)} Hz"
            return [SliceSpec(label, da.isel({chan_dim: idx}))]

    # Default first slice preference: channel -> freq -> raw
    if chan_dim is not None:
        ch_val = ds[chan_dim].isel({chan_dim: 0}).values
        label = f"channel={_coord_to_str(ch_val)}"
        fcoord = _get_freq_coord_for_channel(ds, chan_dim)
        if fcoord is not None:
            f_val = fcoord.isel({chan_dim: 0}).values
            label = f"{_coord_to_str(ch_val)} • {_coord_to_str(f_val)} Hz"
        return [SliceSpec(label, da.isel({chan_dim: 0}))]

    if freq_dim is not None:
        f_val = ds[freq_dim].isel({freq_dim: 0}).values
        return [SliceSpec(f"frequency={_coord_to_str(f_val)}", da.isel({freq_dim: 0}))]

    return [SliceSpec(var, da)]


def _make_all_tabs(
    ds: xr.Dataset,
    var: str,
    group_by: str,
) -> pn.viewable.Viewable:
    """
    Build a tabbed (possibly nested) Panel layout.

    Cases:
    1) Sv dims include BOTH frequency_nominal and channel -> nested tabs.
    2) Sv dims include channel only -> tabs per channel (labels include frequency_nominal(channel) if present).
    3) Sv dims include frequency_nominal only -> tabs per frequency.
    4) Neither -> single plot.
    """
    da = ds[var]
    has_chan = "channel" in da.dims
    has_freq_dim = "frequency_nominal" in da.dims

    # Helper to render one DA -> plot pane (returns HoloViews object)
    # (actual plotting happens later; here we just organize slices)
    if has_chan and has_freq_dim:
        freq_dim = "frequency_nominal"
        chan_dim = "channel"
        fcoord = ds[freq_dim]
        ccoord = ds[chan_dim]

        # Outer/inner control
        outer = "freq" if group_by in ("auto", "freq") else "channel"

        def make_channel_tab(fi: int, ci: int) -> SliceSpec:
            f_val = fcoord.isel({freq_dim: fi}).values
            c_val = ccoord.isel({chan_dim: ci}).values
            label = f"{_coord_to_str(c_val)} • {_coord_to_str(f_val)}"
            return SliceSpec(label, da.isel({freq_dim: fi, chan_dim: ci}))

        if outer == "freq":
            outer_tabs = []
            for fi in range(fcoord.size):
                f_val = fcoord.isel({freq_dim: fi}).values
                inner_items: List[Tuple[str, xr.DataArray]] = []
                for ci in range(ccoord.size):
                    spec = make_channel_tab(fi, ci)
                    inner_items.append((spec.label, spec.da))
                inner_tabs = pn.Tabs(*inner_items, sizing_mode="stretch_both")
                outer_tabs.append((f"freq={_coord_to_str(f_val)}", inner_tabs))
            return pn.Tabs(*outer_tabs, sizing_mode="stretch_both")

        # outer == channel
        outer_tabs = []
        for ci in range(ccoord.size):
            c_val = ccoord.isel({chan_dim: ci}).values
            inner_items: List[Tuple[str, xr.DataArray]] = []
            for fi in range(fcoord.size):
                spec = make_channel_tab(fi, ci)
                inner_items.append((spec.label, spec.da))
            inner_tabs = pn.Tabs(*inner_items, sizing_mode="stretch_both")
            outer_tabs.append((f"channel={_coord_to_str(c_val)}", inner_tabs))
        return pn.Tabs(*outer_tabs, sizing_mode="stretch_both")

    if has_chan:
        chan_dim = "channel"
        ccoord = ds[chan_dim]
        f_on_chan = _get_freq_coord_for_channel(ds, chan_dim)
        items: List[Tuple[str, xr.DataArray]] = []
        for ci in range(ccoord.size):
            c_val = ccoord.isel({chan_dim: ci}).values
            if f_on_chan is not None:
                f_val = f_on_chan.isel({chan_dim: ci}).values
                label = f"{_coord_to_str(c_val)} • {_coord_to_str(f_val)} Hz"
            else:
                label = f"{_coord_to_str(c_val)}"
            items.append((label, da.isel({chan_dim: ci})))
        return pn.Tabs(*items, sizing_mode="stretch_both")

    if has_freq_dim:
        freq_dim = "frequency_nominal"
        fcoord = ds[freq_dim]
        items: List[Tuple[str, xr.DataArray]] = []
        for fi in range(fcoord.size):
            f_val = fcoord.isel({freq_dim: fi}).values
            label = f"freq={_coord_to_str(f_val)}"
            items.append((label, da.isel({freq_dim: fi})))
        return pn.Tabs(*items, sizing_mode="stretch_both")

    # No stacking dims: single
    return pn.Column(pn.pane.Markdown("No channel/frequency dimension detected; plotting single array."))


# ---------------------------
# Plotting
# ---------------------------

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
):
    clim = (vmin, vmax) if (vmin is not None or vmax is not None) else None
    try:
        return da.hvplot.image(
            x=x_name,
            y=y_name,
            cmap=cmap,
            clim=clim,
            width=width,
            height=height,
            colorbar=True,
            toolbar=toolbar,
            title=title,
            xlabel=x_name,
            ylabel=y_name,
        )
    except Exception:
        return da.hvplot.quadmesh(
            x=x_name,
            y=y_name,
            cmap=cmap,
            clim=clim,
            width=width,
            height=height,
            colorbar=True,
            toolbar=toolbar,
            title=title,
            xlabel=x_name,
            ylabel=y_name,
        )


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
) -> pn.viewable.Viewable:
    x_name, y_name = _detect_axes(ds)
    if x_override:
        x_name = x_override
    if y_override:
        y_name = y_override

    header = pn.pane.Markdown(
        f"### aa-plot echogram\n"
        f"- **file:** `{ds.encoding.get('source','(in-memory)')}`\n"
        f"- **var:** `{var}`\n"
        f"- **x:** `{x_name}`  •  **y:** `{y_name}`\n",
        sizing_mode="stretch_width",
    )

    # If all_plots, build tabs first, then map each tab content to a plot
    if all_plots:
        tabs = _make_all_tabs(ds, var, group_by=group_by)

        # Recursively replace DataArray objects inside pn.Tabs with plots
        def materialize(node):
            if isinstance(node, pn.Tabs):
                new_items = []
                for (name, obj) in node.objects:
                    new_items.append((name, materialize(obj)))
                return pn.Tabs(*new_items, sizing_mode="stretch_both")
            if isinstance(node, xr.DataArray):
                da = node
                da = _downsample_da(da, x_name=x_name, step=decimate)
                da = _apply_ylim(da, y_name=y_name, ymin=ymin, ymax=ymax)
                return _plot_echogram(
                    da=da,
                    x_name=x_name,
                    y_name=y_name,
                    title=f"{var} • {node.name or ''}".strip(),
                    cmap=cmap,
                    vmin=vmin,
                    vmax=vmax,
                    width=width,
                    height=height,
                    toolbar=toolbar,
                )
            if isinstance(node, pn.Column) or isinstance(node, pn.Row):
                # not expected here, but keep safe
                return node
            return node

        body = materialize(tabs)
        return pn.Column(header, body, sizing_mode="stretch_both")

    # single selection mode
    specs = _make_single_selection(ds, var, frequency=frequency, channel=channel)
    spec = specs[0]
    da = spec.da
    da = _downsample_da(da, x_name=x_name, step=decimate)
    da = _apply_ylim(da, y_name=y_name, ymin=ymin, ymax=ymax)
    plot = _plot_echogram(
        da=da,
        x_name=x_name,
        y_name=y_name,
        title=f"{var} • {spec.label}",
        cmap=cmap,
        vmin=vmin,
        vmax=vmax,
        width=width,
        height=height,
        toolbar=toolbar,
    )
    return pn.Column(header, plot, sizing_mode="stretch_both")


# ---------------------------
# Main
# ---------------------------

def main() -> None:
    # stdin behavior: if invoked with no args and stdin has a token, treat it as input_path
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

    # selection
    p.add_argument("--var", default=None, help="Variable to plot (default: Sv if present).")
    p.add_argument("--all", action="store_true", help="Plot all channels/frequencies as tabs.")
    p.add_argument("--frequency", type=float, default=None, help="Select nominal frequency (Hz) (nearest match).")
    p.add_argument("--channel", type=str, default=None, help="Select channel by name (exact match preferred).")
    p.add_argument(
        "--group-by",
        type=str,
        default="auto",
        choices=["auto", "channel", "freq"],
        help="When --all and both channel+freq dims exist, controls tab nesting order.",
    )

    # axes
    p.add_argument("--x", dest="x_override", type=str, default=None, help="Override x-axis dim/coord name.")
    p.add_argument("--y", dest="y_override", type=str, default=None, help="Override y-axis dim/coord name.")

    # appearance
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

    # perf
    p.add_argument("--decimate", type=int, default=1, help="Keep every Nth x sample (default: 1).")
    p.add_argument("--ymin", type=float, default=None, help="Y-axis min crop.")
    p.add_argument("--ymax", type=float, default=None, help="Y-axis max crop.")

    # output & behavior
    p.add_argument("-o", "--output_path", type=Path, default=None, help="Output HTML path.")
    p.add_argument("--no-overwrite", action="store_true", help="Do not overwrite output if it exists.")
    p.add_argument("--quiet", action="store_true", help="Reduce logs; still prints final path.")
    p.add_argument("-h", "--help", action="store_true", help="Show help and exit.")

    args = p.parse_args()

    if args.help:
        print_help()
        raise SystemExit(0)

    _configure_logging(args.quiet)

    # resolve input path
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

    # resolve output path
    if args.output_path is None:
        args.output_path = args.input_path.with_stem(args.input_path.stem + "_plot").with_suffix(".html")

    if args.output_path.exists() and args.no_overwrite:
        logger.error(f"Output exists and --no-overwrite set: {args.output_path}")
        raise SystemExit(1)

    # guard conflicts
    if args.all and (args.frequency is not None or args.channel is not None):
        logger.error("Use either --all OR a specific --frequency/--channel (not both).")
        raise SystemExit(2)

    try:
        # keep stdout/stderr clean while opening dataset
        buf = io.StringIO()
        with redirect_stdout(buf), redirect_stderr(buf):
            ds = xr.open_dataset(args.input_path)

        # for nicer header display
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

        # piping contract: emit ONLY the path on stdout
        print(args.output_path.resolve())

    except Exception as e:
        logger.exception(f"aa-plot failed: {e}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()