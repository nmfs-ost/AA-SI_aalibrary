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

import xarray as xr
from loguru import logger

import hvplot.xarray  # noqa: F401
import panel as pn

pn.extension()


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

Appearance:
  --vmin FLOAT              Lower color limit.
  --vmax FLOAT              Upper color limit.
  --cmap NAME               Colormap name (default: inferno).
  --width INT               Plot width  (default: 1200).
  --height INT              Plot height (default: 450).
  --toolbar STR             Toolbar: above/below/left/right/disable (default: above).

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
):
    da = ds[var]
    has_chan = "channel" in da.dims
    has_freq_dim = "frequency_nominal" in da.dims

    if has_chan and has_freq_dim:
        chan_dim = "channel"
        freq_dim = "frequency_nominal"
        ccoord = ds[chan_dim]
        fcoord = ds[freq_dim]

        outer_mode = "freq" if group_by in ("auto", "freq") else "channel"

        if outer_mode == "freq":
            outer_tabs = []
            for fi in range(fcoord.size):
                f_val = fcoord.isel({freq_dim: fi}).values
                inner_tabs = []
                for ci in range(ccoord.size):
                    c_val = ccoord.isel({chan_dim: ci}).values
                    da2 = da.isel({freq_dim: fi, chan_dim: ci})
                    da2 = _prep_da(da2, x_name, y_name, decimate, ymin, ymax)
                    plot = _plot_echogram(
                        da=da2,
                        x_name=x_name,
                        y_name=y_name,
                        title=f"{var} • {_coord_to_str(c_val)} • {_coord_to_str(f_val)}",
                        cmap=cmap,
                        vmin=vmin,
                        vmax=vmax,
                        width=width,
                        height=height,
                        toolbar=toolbar,
                    )
                    inner_tabs.append((f"{_coord_to_str(c_val)}", plot))
                outer_tabs.append((f"{_coord_to_str(f_val)}", pn.Tabs(*inner_tabs, sizing_mode="stretch_both")))
            return pn.Tabs(*outer_tabs, sizing_mode="stretch_both")

        outer_tabs = []
        for ci in range(ccoord.size):
            c_val = ccoord.isel({chan_dim: ci}).values
            inner_tabs = []
            for fi in range(fcoord.size):
                f_val = fcoord.isel({freq_dim: fi}).values
                da2 = da.isel({chan_dim: ci, freq_dim: fi})
                da2 = _prep_da(da2, x_name, y_name, decimate, ymin, ymax)
                plot = _plot_echogram(
                    da=da2,
                    x_name=x_name,
                    y_name=y_name,
                    title=f"{var} • {_coord_to_str(c_val)} • {_coord_to_str(f_val)}",
                    cmap=cmap,
                    vmin=vmin,
                    vmax=vmax,
                    width=width,
                    height=height,
                    toolbar=toolbar,
                )
                inner_tabs.append((f"{_coord_to_str(f_val)}", plot))
            outer_tabs.append((f"{_coord_to_str(c_val)}", pn.Tabs(*inner_tabs, sizing_mode="stretch_both")))
        return pn.Tabs(*outer_tabs, sizing_mode="stretch_both")

    if has_chan:
        chan_dim = "channel"
        ccoord = ds[chan_dim]
        f_on_chan = _get_freq_coord_for_channel(ds, chan_dim)

        tabs = []
        for ci in range(ccoord.size):
            c_val = ccoord.isel({chan_dim: ci}).values
            da2 = da.isel({chan_dim: ci})
            da2 = _prep_da(da2, x_name, y_name, decimate, ymin, ymax)

            if f_on_chan is not None:
                f_val = f_on_chan.isel({chan_dim: ci}).values
                label = f"{_coord_to_str(c_val)} • {_coord_to_str(f_val)} Hz"
            else:
                label = f"{_coord_to_str(c_val)}"

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
            )
            tabs.append((label, plot))

        return pn.Tabs(*tabs, sizing_mode="stretch_both")

    if has_freq_dim:
        freq_dim = "frequency_nominal"
        fcoord = ds[freq_dim]

        tabs = []
        for fi in range(fcoord.size):
            f_val = fcoord.isel({freq_dim: fi}).values
            da2 = da.isel({freq_dim: fi})
            da2 = _prep_da(da2, x_name, y_name, decimate, ymin, ymax)

            label = f"{_coord_to_str(f_val)}"
            plot = _plot_echogram(
                da=da2,
                x_name=x_name,
                y_name=y_name,
                title=f"{var} • frequency={label}",
                cmap=cmap,
                vmin=vmin,
                vmax=vmax,
                width=width,
                height=height,
                toolbar=toolbar,
            )
            tabs.append((label, plot))

        return pn.Tabs(*tabs, sizing_mode="stretch_both")

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
    )
    return pn.Column(
        pn.pane.Markdown("No channel/frequency dimension detected; plotting a single array."),
        plot,
        sizing_mode="stretch_both",
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
        f"- **file:** `{ds.encoding.get('source', '(in-memory)')}`\n"
        f"- **var:** `{var}`\n"
        f"- **x:** `{x_name}`  •  **y:** `{y_name}`\n",
        sizing_mode="stretch_width",
    )

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