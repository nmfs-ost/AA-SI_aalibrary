#!/usr/bin/env python3
"""
aa-plot — Interactive echogram plotting (HTML) for Echopype/xarray NetCDF datasets

Goals:
- Accept .nc path from argv OR stdin (pipeline-friendly).
- Plot a variable (default: Sv if present) for ALL channels/frequencies in a tabbed UI (Panel).
- No matplotlib. Output is standalone HTML.
- Print absolute HTML path to stdout for downstream chaining.

Examples:
  aa-plot data.nc --all -o plots/echogram.html
  aa-nc raw.raw | aa-sv | aa-clean | aa-plot --all
  aa-plot data.nc --var Sv --channel "GPT 38 kHz 0090720d" -o one.html
"""

from __future__ import annotations

import argparse
import io
import sys
from contextlib import redirect_stdout, redirect_stderr
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List, Tuple, Dict, Any

import xarray as xr
from loguru import logger

# Interactive stack (no matplotlib)
import hvplot.xarray  # noqa: F401 registers hvplot accessor
import panel as pn

pn.extension()


# ---------------------------
# Helpers
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


def _read_input_path_from_stdin() -> Optional[str]:
    if sys.stdin.isatty():
        return None
    token = sys.stdin.readline().strip()
    return token or None


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


def _detect_stack_dims(ds: xr.Dataset) -> Tuple[Optional[str], Optional[str]]:
    """
    Return (freq_dim, chan_dim) if present as dims/coords.
    Common in echopype outputs:
      - frequency_nominal
      - channel
    """
    freq_dim = "frequency_nominal" if ("frequency_nominal" in ds.dims or "frequency_nominal" in ds.coords) else None
    chan_dim = "channel" if ("channel" in ds.dims or "channel" in ds.coords) else None
    return freq_dim, chan_dim


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
    # Prefer label-based slicing if possible
    if y_name in da.coords:
        coord = da[y_name]
        lo = ymin if ymin is not None else float(coord.min())
        hi = ymax if ymax is not None else float(coord.max())
        try:
            return da.sel({y_name: slice(lo, hi)})
        except Exception:
            # Fall back to original if slicing fails
            return da
    return da


def _coord_to_str(val: Any) -> str:
    # robust-ish string for coords that may be bytes/numpy scalars
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


@dataclass
class SliceSpec:
    label: str
    da: xr.DataArray


def _make_slices(
    ds: xr.Dataset,
    var: str,
    freq_dim: Optional[str],
    chan_dim: Optional[str],
    all_plots: bool,
    frequency: Optional[float],
    channel: Optional[str],
) -> List[SliceSpec]:
    """
    Builds a list of DataArray slices to plot.
    Strategy:
      - If --all: create one tab per channel if 'channel' exists; else per frequency if exists;
                if both exist, create per channel AND per frequency? (that can explode).
                We choose:
                  * If channel exists -> per channel (preferred)
                  * Else if frequency exists -> per frequency
      - If not --all:
          * If --channel: select that channel
          * Else if --frequency: nearest frequency
          * Else: default first slice (channel first, else frequency first, else raw)
    """
    da = ds[var]
    slices: List[SliceSpec] = []

    # Prefer channel stacking over frequency when both exist
    if all_plots:
        if chan_dim and chan_dim in da.dims:
            coord = ds[chan_dim]
            for i in range(coord.size):
                lab = f"{chan_dim}={_coord_to_str(coord.isel({chan_dim: i}).values)}"
                slices.append(SliceSpec(lab, da.isel({chan_dim: i})))
            return slices
        if freq_dim and freq_dim in da.dims:
            coord = ds[freq_dim]
            for i in range(coord.size):
                v = coord.isel({freq_dim: i}).values
                lab = f"{freq_dim}={_coord_to_str(v)}"
                slices.append(SliceSpec(lab, da.isel({freq_dim: i})))
            return slices
        return [SliceSpec(var, da)]

    # single selection mode
    if chan_dim and chan_dim in da.dims:
        coord = ds[chan_dim]
        if channel is not None:
            # exact match if possible
            vals = [_coord_to_str(v) for v in coord.values]
            try:
                idx = vals.index(channel)
            except ValueError:
                idx = 0
            v = coord.isel({chan_dim: idx}).values
            return [SliceSpec(f"{chan_dim}={_coord_to_str(v)}", da.isel({chan_dim: idx}))]

        # no channel requested; maybe frequency
        if frequency is not None and freq_dim and freq_dim in da.dims:
            # pick nearest on freq dim first, then keep default channel=0
            fcoord = ds[freq_dim]
            idxf = int(abs(fcoord - frequency).argmin().item())
            fv = fcoord.isel({freq_dim: idxf}).values
            da2 = da.isel({freq_dim: idxf, chan_dim: 0}) if chan_dim in da.dims else da.isel({freq_dim: idxf})
            return [SliceSpec(f"{freq_dim}~{_coord_to_str(fv)} & {chan_dim}=0", da2)]

        # default: first channel
        v = coord.isel({chan_dim: 0}).values
        return [SliceSpec(f"{chan_dim}={_coord_to_str(v)}", da.isel({chan_dim: 0}))]

    if freq_dim and freq_dim in da.dims:
        coord = ds[freq_dim]
        if frequency is not None:
            idx = int(abs(coord - frequency).argmin().item())
            v = coord.isel({freq_dim: idx}).values
            return [SliceSpec(f"{freq_dim}~{_coord_to_str(v)}", da.isel({freq_dim: idx}))]

        v = coord.isel({freq_dim: 0}).values
        return [SliceSpec(f"{freq_dim}={_coord_to_str(v)}", da.isel({freq_dim: 0}))]

    return [SliceSpec(var, da)]


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
    """
    Try hvplot.image first (fast for regular grids), else fallback quadmesh.
    """
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


def _build_panel(
    ds: xr.Dataset,
    var: str,
    all_plots: bool,
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

    freq_dim, chan_dim = _detect_stack_dims(ds)

    slices = _make_slices(
        ds=ds,
        var=var,
        freq_dim=freq_dim,
        chan_dim=chan_dim,
        all_plots=all_plots,
        frequency=frequency,
        channel=channel,
    )

    items = []
    for s in slices:
        da2 = s.da
        da2 = _downsample_da(da2, x_name=x_name, step=decimate)
        da2 = _apply_ylim(da2, y_name=y_name, ymin=ymin, ymax=ymax)

        plot = _plot_echogram(
            da=da2,
            x_name=x_name,
            y_name=y_name,
            title=f"{var} • {s.label}",
            cmap=cmap,
            vmin=vmin,
            vmax=vmax,
            width=width,
            height=height,
            toolbar=toolbar,
        )
        items.append((s.label, plot))

    header = pn.pane.Markdown(
        f"### aa-plot echogram\n"
        f"- **file:** `{ds.encoding.get('source','(in-memory)')}`\n"
        f"- **var:** `{var}`\n"
        f"- **x:** `{x_name}`  •  **y:** `{y_name}`\n",
        sizing_mode="stretch_width",
    )

    if len(items) == 1:
        return pn.Column(header, items[0][1], sizing_mode="stretch_both")

    tabs = pn.Tabs(*items, sizing_mode="stretch_both")
    return pn.Column(header, tabs, sizing_mode="stretch_both")


def _configure_logging(quiet: bool) -> None:
    logger.remove()
    if quiet:
        # keep stdout clean; silence almost everything
        logger.add(sys.stderr, level="WARNING", backtrace=False, diagnose=False)
    else:
        logger.add(sys.stderr, level="INFO", backtrace=True, diagnose=False)


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
        # keep stdout clean while opening dataset
        buf = io.StringIO()
        with redirect_stdout(buf), redirect_stderr(buf):
            ds = xr.open_dataset(args.input_path)

        # for nicer header display
        ds.encoding["source"] = str(args.input_path)

        var = _ensure_variable(ds, args.var)

        logger.info(f"Plotting var='{var}' from {args.input_path.name}")

        layout = _build_panel(
            ds=ds,
            var=var,
            all_plots=args.all,
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