#!/usr/bin/env python3
"""
aa-plot — Interactive echogram plotting for Echopype/xarray datasets (HTML output)

Design goals:
- Accept .nc path from argv *or* stdin (pipeline-friendly).
- Work on xarray Dataset/DataArray; default variable 'Sv' if present.
- Select a single frequency/channel or render **all** in tabbed UI.
- Stylish, interactive HTML (hvPlot + Panel); no matplotlib.
- Sensible defaults with explicit options for vmin/vmax, cmap, decimation, etc.
- Always writes to disk; prints absolute path to stdout for chaining.

Example:
    aa-plot data.nc --var Sv --all --vmin -90 --vmax -40 --cmap fire -o plots/echogram.html
"""

import io
from contextlib import redirect_stdout
import argparse
import sys
from pathlib import Path
from typing import Optional, List, Tuple

import xarray as xr
from loguru import logger

# Interactive stack (no matplotlib)
import hvplot.xarray  # noqa: F401 (registers hvplot accessors)
import panel as pn

pn.extension("tabulator")  # safe to load; enables richer tables if used


# ---------------------------
# Helpers
# ---------------------------

def _detect_axes(ds: xr.Dataset) -> Tuple[str, str]:
    """
    Heuristic to find the horizontal (time/ping) and vertical (range) axes.
    Returns (x_name, y_name).
    """
    # Candidates for time/ping axis
    for cand in ("ping_time", "time", "ping", "profile_time"):
        if cand in ds.dims or cand in ds.coords:
            x_name = cand
            break
    else:
        # Fallback to the first dimension
        x_name = list(ds.dims)[0]

    # Candidates for range axis
    for cand in ("echo_range", "range", "range_meter", "range_sample"):
        if cand in ds.dims or cand in ds.coords:
            y_name = cand
            break
    else:
        # Fallback to second dimension if it exists
        dims = list(ds.dims)
        y_name = dims[1] if len(dims) > 1 else dims[0]

    return x_name, y_name


def _detect_freq_dims(ds: xr.Dataset) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns a tuple (freq_dim, chan_dim). One or both may be None.
    - freq_dim typically 'frequency_nominal'
    - chan_dim typically 'channel'
    """
    freq_dim = "frequency_nominal" if "frequency_nominal" in ds.dims or "frequency_nominal" in ds.coords else None
    chan_dim = "channel" if "channel" in ds.dims or "channel" in ds.coords else None
    return freq_dim, chan_dim


def _ensure_variable(ds: xr.Dataset, var: Optional[str]) -> str:
    """
    Decide which variable to plot. Priority:
      1) user-specified --var
      2) 'Sv' if present
      3) first data_var
    """
    if var:
        if var not in ds.data_vars:
            raise ValueError(f"Variable '{var}' not found in dataset.")
        return var
    if "Sv" in ds.data_vars:
        return "Sv"
    # pick first data_var
    if len(ds.data_vars) == 0:
        raise ValueError("No data variables found to plot.")
    return list(ds.data_vars)[0]


def _subset_frequency(ds: xr.Dataset, var: str,
                      freq_dim: Optional[str],
                      chan_dim: Optional[str],
                      frequency: Optional[float],
                      channel: Optional[str],
                      all_plots: bool) -> List[Tuple[str, xr.DataArray]]:
    """
    Produce a list of (label, DataArray) to plot.
    - If all_plots: return one entry per frequency/channel (tabs).
    - Else: select a single frequency or channel if specified; otherwise default to first.
    """
    da = ds[var]

    # Build per-slice list
    slices = []

    # If there is a frequency dimension
    if freq_dim and freq_dim in da.dims:
        coord = ds[freq_dim]
        if all_plots:
            for i in range(coord.size):
                val = float(coord.isel({freq_dim: i}).values)
                label = f"{freq_dim}={val:g} Hz" if val > 1000 else f"{freq_dim}={val}"
                slices.append((label, da.isel({freq_dim: i})))
        else:
            if frequency is not None:
                # pick nearest frequency
                idx = int(abs(coord - frequency).argmin().item())
                val = float(coord.isel({freq_dim: idx}).values)
                label = f"{freq_dim}~{val:g} Hz"
                slices.append((label, da.isel({freq_dim: idx})))
            else:
                # default: first
                val = float(coord.isel({freq_dim: 0}).values)
                label = f"{freq_dim}={val:g} Hz"
                slices.append((label, da.isel({freq_dim: 0})))

    # If there is a channel dimension (and either no freq_dim, or also present)
    if chan_dim and chan_dim in da.dims:
        coord = ds[chan_dim]
        if all_plots:
            for i in range(coord.size):
                val = coord.isel({chan_dim: i}).values
                label = f"{chan_dim}={str(val)}"
                slices.append((label, da.isel({chan_dim: i})))
        else:
            if channel is not None:
                # try exact match; fallback to first
                if channel in coord.values:
                    idx = int((coord == channel).argmax().item())
                else:
                    idx = 0
                val = coord.isel({chan_dim: idx}).values
                label = f"{chan_dim}={str(val)}"
                slices.append((label, da.isel({chan_dim: idx})))
            elif not freq_dim:
                # default to first channel if no freq selection made
                val = coord.isel({chan_dim: 0}).values
                label = f"{chan_dim}={str(val)}"
                slices.append((label, da.isel({chan_dim: 0})))

    # If neither frequency nor channel dimension exists, just return the DA as-is
    if not slices:
        slices.append((var, da))

    return slices


def _downsample_da(da: xr.DataArray, x_name: str, step: int) -> xr.DataArray:
    """Downsample by taking every Nth along the x-axis (e.g., pings)."""
    if step <= 1:
        return da
    if x_name not in da.dims:
        return da
    return da.isel({x_name: slice(0, None, step)})


def _apply_ylim(da: xr.DataArray, y_name: str, ymin: Optional[float], ymax: Optional[float]) -> xr.DataArray:
    """Crop the y-axis (typically range) to [ymin, ymax] if provided."""
    if y_name not in da.coords:
        return da
    rng = da[y_name]
    # Handle numeric meters vs. sample index gracefully
    lo = ymin if ymin is not None else float(rng.min())
    hi = ymax if ymax is not None else float(rng.max())
    try:
        return da.sel({y_name: slice(lo, hi)})
    except Exception:
        # if not label-based, fallback to index-based slice by nearest index
        return da.isel({y_name: slice(int((rng >= lo).argmax()), int((rng <= hi)[::-1].argmax()))})


# ---------------------------
# Main plotting function
# ---------------------------

def _build_panel(ds: xr.Dataset, var: str, vmin: Optional[float], vmax: Optional[float],
                 cmap: str, decimate: int, ymin: Optional[float], ymax: Optional[float],
                 width: int, height: int, toolbar: str,
                 frequency: Optional[float], channel: Optional[str], all_plots: bool):
    """Create a Panel layout (tabs or single plot) of echograms using hvPlot."""
    x_name, y_name = _detect_axes(ds)
    freq_dim, chan_dim = _detect_freq_dims(ds)

    items = []
    for label, da in _subset_frequency(ds, var, freq_dim, chan_dim, frequency, channel, all_plots):
        # decimate along x and crop y if requested
        da2 = _downsample_da(da, x_name=x_name, step=decimate)
        da2 = _apply_ylim(da2, y_name=y_name, ymin=ymin, ymax=ymax)

        # Build an image-like plot (quadmesh handles irregular axes; image is faster for regular grid)
        # We try hvplot.image first; if it fails (non-uniform coords), fallback to quadmesh.
        try:
            plot = da2.hvplot.image(x=x_name, y=y_name, cmap=cmap, clim=(vmin, vmax),
                                     rasterize=False, width=width, height=height,
                                     colorbar=True, toolbar=toolbar, title=f"{var} • {label}")
        except Exception:
            plot = da2.hvplot.quadmesh(x=x_name, y=y_name, cmap=cmap, clim=(vmin, vmax),
                                       width=width, height=height, colorbar=True,
                                       toolbar=toolbar, title=f"{var} • {label}")
        items.append((label, plot))

    if len(items) == 1:
        # single pane layout with a light header
        header = pn.pane.Markdown(f"### {var} echogram  \n**x:** {x_name}  •  **y:** {y_name}", sizing_mode="stretch_width")
        return pn.Column(header, items[0][1], sizing_mode="stretch_both")
    else:
        tabs = pn.Tabs(*items, sizing_mode="stretch_both")
        header = pn.pane.Markdown(f"### {var} echograms (tabbed)  \n**x:** {x_name}  •  **y:** {y_name}", sizing_mode="stretch_width")
        return pn.Column(header, tabs, sizing_mode="stretch_both")


# ---------------------------
# CLI
# ---------------------------

def print_help():
    help_text = r"""
Usage: aa-plot [OPTIONS] [INPUT_PATH]

Arguments:
  INPUT_PATH                Path to a NetCDF file (.nc) with an echogram variable
                            (default variable: 'Sv'). Optional; if omitted, a single
                            path token may be read from stdin (piping model).

Core selection:
  --var VAR                 Data variable to plot (default: Sv).
  --all                     Plot all channels/frequencies as tabs.
  --frequency FLOAT         Select a single nominal frequency (Hz) (nearest match).
  --channel NAME            Select a single channel by name.

Appearance:
  --vmin FLOAT              Lower color limit (e.g., -90).
  --vmax FLOAT              Upper color limit (e.g., -40).
  --cmap NAME               Colormap name (e.g., 'fire','inferno','viridis'; default: 'inferno').
  --width INT               Plot width  (default: 1200).
  --height INT              Plot height (default: 450).
  --toolbar STR             Toolbar mode: 'above','below','left','right','disable' (default: 'above').

Subsetting / performance:
  --decimate INT            Take every Nth sample along x-axis (pings) to speed up (default: 1).
  --ymin FLOAT              Crop lower y-limit (e.g., 0).
  --ymax FLOAT              Crop upper y-limit (e.g., 500).

Output:
  -o, --output_path PATH    Output HTML path (default: <stem>_plot.html).
  --no-overwrite            Fail if output already exists.
  --quiet                   Suppress info logs; still prints final path.
  -h, --help                Show this help and exit.

Notes:
  • This tool writes an interactive HTML file (Bokeh) and prints its absolute path.
  • Pipes pass filenames (not bytes). Example:
        aa-nc raw.raw --sonar_model EK60 | aa-sv | aa-clean | aa-plot --all
"""
    print(help_text)


def _add_basic_attrs(ds: xr.Dataset) -> None:
    """Replace None attrs with strings to avoid NetCDF writer issues if we ever save DS."""
    for k, v in list(ds.attrs.items()):
        if v is None:
            ds.attrs[k] = "NA"
    for var in ds.data_vars:
        for kk, vv in list(ds[var].attrs.items()):
            if vv is None:
                ds[var].attrs[kk] = "NA"


def main():
    # stdin behavior for your suite
    if len(sys.argv) == 1:
        if not sys.stdin.isatty():
            token = sys.stdin.readline().strip()
            if token:
                sys.argv.append(token)
        else:
            print_help()
            sys.exit(0)

    p = argparse.ArgumentParser(description="Interactive echogram plotting (hvPlot + Panel).", add_help=False)
    p.add_argument("input_path", type=Path, nargs="?", help="Path to a NetCDF file (.nc).")

    # selection
    p.add_argument("--var", dest="var", default=None, help="Variable to plot (default: Sv).")
    p.add_argument("--all", action="store_true", help="Plot all channels/frequencies in tabs.")
    p.add_argument("--frequency", type=float, default=None, help="Select nominal frequency (Hz).")
    p.add_argument("--channel", type=str, default=None, help="Select a channel by name.")

    # appearance
    p.add_argument("--vmin", type=float, default=None, help="Lower color limit.")
    p.add_argument("--vmax", type=float, default=None, help="Upper color limit.")
    p.add_argument("--cmap", type=str, default="inferno", help="Colormap name (default: inferno).")
    p.add_argument("--width", type=int, default=1200, help="Plot width (px).")
    p.add_argument("--height", type=int, default=450, help="Plot height (px).")
    p.add_argument("--toolbar", type=str, default="above", choices=["above","below","left","right","disable"],
                   help="Toolbar placement (default: above).")

    # subsetting / perf
    p.add_argument("--decimate", type=int, default=1, help="Keep every Nth ping/time sample (default: 1).")
    p.add_argument("--ymin", type=float, default=None, help="Y-axis min (range/depth).")
    p.add_argument("--ymax", type=float, default=None, help="Y-axis max (range/depth).")

    # output & behavior
    p.add_argument("-o", "--output_path", type=Path, default=None, help="Output HTML path (default: <stem>_plot.html).")
    p.add_argument("--no-overwrite", action="store_true", help="Do not overwrite an existing output file.")
    p.add_argument("--quiet", action="store_true", help="Reduce logs; still prints final path.")
    p.add_argument("-h", "--help", action="store_true", help="Show this help and exit.")

    args = p.parse_args()

    if args.help:
        print_help()
        sys.exit(0)

    # resolve input path
    if args.input_path is None:
        args.input_path = Path(sys.stdin.readline().strip())
        if not args.quiet:
            logger.info(f"Read input path from stdin: {args.input_path}")

    if not args.input_path.exists():
        logger.error(f"File '{args.input_path}' does not exist.")
        sys.exit(1)

    # resolve output path
    if args.output_path is None:
        args.output_path = args.input_path.with_stem(args.input_path.stem + "_plot").with_suffix(".html")

    if args.output_path.exists() and args.no_overwrite:
        logger.error(f"Output file '{args.output_path}' exists and --no-overwrite was set.")
        sys.exit(1)

    # guard option conflicts
    if args.all and (args.frequency is not None or args.channel is not None):
        logger.error("Use either --all OR a specific --frequency/--channel (not both).")
        sys.exit(1)

    try:
        # Load dataset quietly to keep stdout clean for downstream pipes.
        f = io.StringIO()
        with redirect_stdout(f):
            ds = xr.open_dataset(args.input_path)

        var = _ensure_variable(ds, args.var)

        if not args.quiet:
            logger.info(f"Building echogram(s) for variable '{var}' ...")

        layout = _build_panel(
            ds=ds,
            var=var,
            vmin=args.vmin,
            vmax=args.vmax,
            cmap=args.cmap,
            decimate=args.decimate,
            ymin=args.ymin,
            ymax=args.ymax,
            width=args.width,
            height=args.height,
            toolbar=args.toolbar,
            frequency=args.frequency,
            channel=args.channel,
            all_plots=args.all,
        )

        # Ensure output directory exists
        args.output_path.parent.mkdir(parents=True, exist_ok=True)

        # Save to standalone HTML (inline resources for portability)
        if not args.quiet:
            logger.info(f"Saving interactive HTML to {args.output_path} ...")

        pn.io.save.save(layout, filename=str(args.output_path), embed=True, resources="inline", title="aa-plot echogram")

        # Emit absolute path for piping
        print(args.output_path.resolve())

        if not args.quiet:
            logger.info("aa-plot complete.")

    except Exception as e:
        logger.exception(f"Error during plotting: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
