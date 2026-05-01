#!/usr/bin/env python3
"""
aa-graph

Lightweight echogram plotter for Jupyter notebooks and CLI piping.

Designed as a complement to aa-plot:
  - aa-plot  -> full interactive HTML with drawing tools, provenance panels,
                EVL/EVR export. Heavyweight, browser-based.
  - aa-graph -> one PNG, one matplotlib subplot per channel. Lightweight,
                inline-friendly, fast.

Pipeline contract (mirrors the rest of the aa-suite):
    input  : positional path OR stdin (single-line token)
    output : PNG file; absolute path printed to stdout
    logs   : stderr via loguru

Two ways to use it (same code path):

CLI (saves PNG, prints path; pipeline-friendly):
    aa-graph input_Sv.nc
    aa-graph input_Sv.nc -o out.png --vmin -90 --vmax -30
    aa-sv input.raw | aa-graph

Jupyter (auto-displays inline):
    from aalibrary.graph import echogram
    echogram("input_Sv.nc")                       # all channels stacked
    echogram("input_Sv.nc", channel=0)            # single channel
    echogram("input_Sv.nc", frequency=38_000)     # nearest to 38 kHz

Subplot titles use a short descriptive label like "38 kHz" when a
frequency_nominal coord is present, falling back to "ch 0", "ch 1", ...
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

import argparse
from pathlib import Path
from typing import Optional

import numpy as np

# xarray and matplotlib are deferred to function bodies so `aa-graph --help`
# stays snappy (xarray alone is ~1s of import time).


# ---------------------------------------------------------------------------
# Utility / config (mirrors aa-plot's conventions)
# ---------------------------------------------------------------------------

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


def _configure_logging(quiet: bool) -> None:
    """Replace the default suppression sink with a user-visible one."""
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


def print_help() -> None:
    help_text = r"""
Usage: aa-graph [OPTIONS] [INPUT_PATH]

Arguments:
  INPUT_PATH                Path to a NetCDF file (.nc). Optional; if
                            omitted, reads a single path token from stdin.

Variable & channel selection:
  --var VAR                 Variable to plot (default: Sv if present, else
                            the first data_var).
  --channel N               Plot only channel index N.
  --frequency F             Plot only the channel nearest to F Hz.
  --single                  Shortcut for --channel 0.

Appearance:
  --vmin FLOAT              Lower color limit. Default: -80 dB for Sv-like
                            data; autoscaled for categorical data (e.g.
                            cluster labels, region masks).
  --vmax FLOAT              Upper color limit. Default: -30 dB for Sv-like
                            data; autoscaled for categorical data.
  --cmap NAME               Matplotlib colormap (default: viridis).
  --figwidth FLOAT          Figure width in inches (default: 10).
  --rowheight FLOAT         Per-channel row height in inches (default: 3).
  --no-flip                 Don't auto-invert the y-axis for depth/range.

Subsetting / performance:
  --decimate N              Take every Nth sample along x-axis (default: 1).
  --ymin FLOAT              Crop lower y-limit (in metres if axis is depth).
  --ymax FLOAT              Crop upper y-limit (in metres if axis is depth).

Output:
  -o, --output_path PATH    Output PNG path (default: <stem>_graph.png).
  --dpi INT                 Output DPI (default: 100).
  --quiet                   Suppress INFO logs; final path still prints.
  -h, --help                Show this help and exit.

By default, multi-channel datasets are plotted with one subplot per channel,
vertically stacked, sharing the x-axis. Subplot titles are short and
descriptive: "38 kHz", "200 kHz" (frequency_nominal), or "ch 0", "ch 1"
when no frequency coord is available.
"""
    print(help_text.strip())


# ---------------------------------------------------------------------------
# Variable / axis detection — simplified subset of aa-plot's logic
# ---------------------------------------------------------------------------

_DEPTH_RANGE_NAMES = frozenset({
    "echo_range", "range", "range_meter", "range_sample",
    "depth", "range_bin", "distance", "range_m",
})


def _ensure_variable(ds, var: Optional[str]) -> str:
    if var is not None:
        if var not in ds.data_vars:
            raise ValueError(f"Variable '{var}' not found. Available: {list(ds.data_vars)}")
        return var
    for cand in ("Sv", "Sv_clean", "NASC"):
        if cand in ds.data_vars:
            return cand
    if not ds.data_vars:
        raise ValueError("No data variables in file.")
    return list(ds.data_vars)[0]


def _detect_x_dim(da) -> str:
    for cand in ("ping_time", "time", "ping", "distance"):
        if cand in da.dims:
            return cand
    return da.dims[-2] if len(da.dims) >= 2 else da.dims[0]


def _detect_y_dim(da) -> str:
    for cand in ("depth", "echo_range", "range_meter", "range", "range_sample"):
        if cand in da.dims:
            return cand
    return da.dims[-1]


def _resolve_y_axis(ds, da, x_dim: str):
    """Make the y-axis show metres when it can.

    Echopype's raw Sv output stores `echo_range` as a 2-D data variable
    (channel, ping_time, range_sample) rather than a 1-D coord — so a
    naive plot ends up with integer sample indices on the y-axis. This
    helper reduces echo_range / depth across non-range dims to a 1-D
    vector and swaps it onto the range dimension.

    A simplified, no-fluff version of aa-plot's `_ensure_y_axis_coord`.
    """
    # Already a 1-D dim coord? Nothing to do.
    for cand in ("depth", "echo_range"):
        if cand in da.dims:
            return da, cand

    # Try to promote a 2-D data variable to a 1-D coord.
    for cand in ("depth", "echo_range"):
        if cand not in ds.data_vars:
            continue
        src = ds[cand]
        # The dim of `src` that's also on `da` and isn't the x-axis is
        # the range-like dim.
        range_dim = next(
            (d for d in src.dims if d in da.dims and d != x_dim),
            None,
        )
        if range_dim is None:
            continue

        other_dims = [d for d in src.dims if d != range_dim]
        try:
            if other_dims:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")  # "Mean of empty slice"
                    vec = src.mean(dim=other_dims, skipna=True)
            else:
                vec = src
            if vec.ndim != 1:
                continue
            vals = np.asarray(vec.values)
            if vals.dtype.kind == "f" and not np.any(np.isfinite(vals)):
                continue  # all-NaN; not useful
            da = da.assign_coords({cand: (range_dim, vals)})
            if cand != range_dim:
                da = da.swap_dims({range_dim: cand})
            return da, cand
        except Exception:
            continue

    # Fall back to whatever's there (typically range_sample integer indices).
    return da, _detect_y_dim(da)


def _should_flip_y(y_name: str) -> bool:
    return y_name.lower() in _DEPTH_RANGE_NAMES


def _is_categorical(da) -> bool:
    """Decide whether a variable is categorical / discrete (cluster labels,
    region masks, KMeans output, etc.).

    Ported from aa-plot's `_is_categorical` so aa-graph treats cluster maps
    the same way: integer/bool dtype is categorical outright; float arrays
    are categorical when every finite value is integer-valued AND there are
    fewer than ~50 unique values (catches labels stored as float, a common
    xarray quirk).
    """
    if da.dtype.kind in ("i", "u", "b"):
        return True
    try:
        vals = np.asarray(da.values).ravel()
        finite = vals[np.isfinite(vals)]
        if finite.size == 0:
            return False
        # Subsample for the integer-valued check; full pass for unique count.
        sample_size = min(50_000, finite.size)
        step = max(1, finite.size // sample_size)
        sample = finite[::step]
        if np.all(np.equal(np.mod(sample, 1.0), 0.0)):
            if int(np.unique(finite).size) < 50:
                return True
    except Exception:
        pass
    return False


# ---------------------------------------------------------------------------
# Channel labeling — keep tab/subplot titles SHORT
# ---------------------------------------------------------------------------

def _short_channel_label(ds, chan_dim: str, ci: int) -> str:
    """Short descriptive label for a channel.

    Preference: frequency-only ("38 kHz") when frequency_nominal is
    available, falling back to "ch N". This is the deliberate departure
    from aa-plot, whose tab titles concatenate the full channel string
    (often "GPT 1 ES200-7C ES200-7CDK-Split 1.0 ms") with the frequency
    in Hz — accurate but unreadable.
    """
    if "frequency_nominal" in ds:
        f = ds["frequency_nominal"]
        if chan_dim in f.dims:
            try:
                fv = float(f.isel({chan_dim: ci}).values)
                if fv >= 1000:
                    return f"{fv / 1000:.0f} kHz"
                return f"{fv:.0f} Hz"
            except Exception:
                pass
    return f"ch {ci}"


def _nearest_freq_index(ds, chan_dim: str, target_hz: float) -> Optional[int]:
    if "frequency_nominal" not in ds:
        return None
    f = ds["frequency_nominal"]
    if chan_dim not in f.dims:
        return None
    try:
        return int(abs(f - target_hz).argmin().item())
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Subsetting helpers
# ---------------------------------------------------------------------------

def _decimate(da, x_dim: str, step: int):
    if step <= 1 or x_dim not in da.dims:
        return da
    return da.isel({x_dim: slice(0, None, step)})


def _crop_y(da, y_dim: str, ymin, ymax):
    if ymin is None and ymax is None:
        return da
    if y_dim not in da.coords:
        return da
    coord = da[y_dim]
    lo = ymin if ymin is not None else float(coord.min())
    hi = ymax if ymax is not None else float(coord.max())
    try:
        return da.sel({y_dim: slice(lo, hi)})
    except Exception:
        return da


# ---------------------------------------------------------------------------
# Public function: usable directly in Jupyter, also called by main()
# ---------------------------------------------------------------------------

def echogram(
    path,
    *,
    var: Optional[str] = None,
    channel: Optional[int] = None,
    frequency: Optional[float] = None,
    cmap: str = "viridis",
    vmin: Optional[float] = None,
    vmax: Optional[float] = None,
    figwidth: float = 10,
    rowheight: float = 3,
    decimate: int = 1,
    ymin: Optional[float] = None,
    ymax: Optional[float] = None,
    flip_y: bool = True,
):
    """Plot a Sv-style echogram from a NetCDF file. Returns the matplotlib Figure.

    Auto-displays inline when called as the last expression of a Jupyter cell.
    From a script, use `fig.savefig(path)` to write to disk.

    Multi-channel datasets get one subplot per channel by default. Pass
    `channel=` or `frequency=` to plot a single channel.
    """
    import xarray as xr
    import matplotlib.pyplot as plt

    ds = xr.open_dataset(path)
    var = _ensure_variable(ds, var)
    da = ds[var]

    chan_dim = "channel" if "channel" in da.dims else None

    # Decide which channels to plot
    if channel is not None:
        if chan_dim is None:
            logger.warning(f"channel ignored: '{var}' has no 'channel' dim.")
            channel_idxs = [None]
        else:
            n = da.sizes[chan_dim]
            if not (0 <= channel < n):
                raise ValueError(f"channel index {channel} out of range [0, {n - 1}]")
            channel_idxs = [int(channel)]
    elif frequency is not None:
        if chan_dim is None:
            logger.warning(f"frequency ignored: '{var}' has no 'channel' dim.")
            channel_idxs = [None]
        else:
            idx = _nearest_freq_index(ds, chan_dim, frequency)
            if idx is None:
                logger.warning("frequency ignored: no frequency_nominal coord on channel.")
                channel_idxs = [0]
            else:
                channel_idxs = [idx]
    elif chan_dim is not None:
        channel_idxs = list(range(da.sizes[chan_dim]))
    else:
        channel_idxs = [None]

    n_panels = len(channel_idxs)

    fig, axes = plt.subplots(
        n_panels, 1,
        figsize=(figwidth, rowheight * n_panels),
        sharex=True,
        squeeze=False,
    )

    x_dim = _detect_x_dim(da)

    for i, ci in enumerate(channel_idxs):
        ax = axes[i, 0]

        if ci is None:
            da_panel = da
            label = None
        else:
            da_panel = da.isel({chan_dim: ci})
            label = _short_channel_label(ds, chan_dim, ci)

        da_panel, y_dim = _resolve_y_axis(ds, da_panel, x_dim)
        da_panel = _decimate(da_panel, x_dim, decimate)
        da_panel = _crop_y(da_panel, y_dim, ymin, ymax)

        # Resolve color limits and colorbar label. For categorical / discrete
        # variables (cluster labels, region masks, etc.) we let matplotlib
        # autoscale to the data range — the dB defaults of -80/-30 would
        # collapse integer labels to a single color. For continuous Sv-like
        # data, fall back to dB defaults when the caller didn't override.
        is_cat = _is_categorical(da_panel)
        if is_cat:
            eff_vmin, eff_vmax = vmin, vmax  # honor explicit overrides; else None
            cbar_label = f"{var}"
        else:
            eff_vmin = vmin if vmin is not None else -80
            eff_vmax = vmax if vmax is not None else -30
            cbar_label = f"{var} (dB)"

        do_flip = flip_y and _should_flip_y(y_dim)

        da_panel.plot.pcolormesh(
            x=x_dim,
            y=y_dim,
            ax=ax,
            cmap=cmap,
            vmin=eff_vmin,
            vmax=eff_vmax,
            yincrease=not do_flip,
            add_colorbar=True,
            cbar_kwargs={"label": cbar_label},
        )

        if label:
            ax.set_title(label, fontsize=10)
        # Drop the noisy auto-title xarray adds (it duplicates info that
        # the suptitle and axis labels already cover).
        else:
            ax.set_title("")

    fig.suptitle(f"{var}  ·  {Path(path).name}", fontsize=11)
    fig.tight_layout()
    return fig


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    # Empty argv + piped stdin → inject the path token so argparse sees it.
    if len(sys.argv) == 1:
        token = _read_input_path_from_stdin()
        if token:
            sys.argv.append(token)
        else:
            print_help()
            raise SystemExit(0)

    p = argparse.ArgumentParser(
        description="Lightweight echogram plotter (PNG output, Jupyter-friendly).",
        add_help=False,
    )
    p.add_argument("input_path", type=Path, nargs="?")
    p.add_argument("--var", default=None)
    p.add_argument("--channel", type=int, default=None)
    p.add_argument("--frequency", type=float, default=None)
    p.add_argument("--single", action="store_true",
                   help="Shortcut for --channel 0.")
    p.add_argument("--vmin", type=float, default=None)
    p.add_argument("--vmax", type=float, default=None)
    p.add_argument("--cmap", type=str, default="viridis")
    p.add_argument("--figwidth", type=float, default=10)
    p.add_argument("--rowheight", type=float, default=3)
    p.add_argument("--no-flip", action="store_true")
    p.add_argument("--decimate", type=int, default=1)
    p.add_argument("--ymin", type=float, default=None)
    p.add_argument("--ymax", type=float, default=None)
    p.add_argument("-o", "--output_path", type=Path, default=None)
    p.add_argument("--dpi", type=int, default=100)
    p.add_argument("--quiet", action="store_true")
    p.add_argument("-h", "--help", action="store_true")

    args = p.parse_args()

    if args.help:
        print_help()
        raise SystemExit(0)

    _configure_logging(args.quiet)

    # Force a non-interactive backend before any matplotlib import. Only
    # takes effect in CLI mode — Jupyter has already set its own backend
    # before this code runs, and setdefault leaves that alone.
    import os
    os.environ.setdefault("MPLBACKEND", "Agg")

    # Resolve input path (positional > stdin > fail)
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

    # Mutually exclusive selection flags
    if args.single and (args.channel is not None or args.frequency is not None):
        logger.error("Use either --single OR a specific --channel/--frequency (not both).")
        raise SystemExit(2)
    if args.channel is not None and args.frequency is not None:
        logger.error("Use either --channel OR --frequency (not both).")
        raise SystemExit(2)

    chan = args.channel
    if args.single and chan is None:
        chan = 0

    # Resolve output path
    if args.output_path is None:
        args.output_path = (
            args.input_path.with_stem(args.input_path.stem + "_graph")
            .with_suffix(".png")
        )

    try:
        logger.info(f"Plotting {args.input_path.name}")
        fig = echogram(
            args.input_path,
            var=args.var,
            channel=chan,
            frequency=args.frequency,
            cmap=args.cmap,
            vmin=args.vmin,
            vmax=args.vmax,
            figwidth=args.figwidth,
            rowheight=args.rowheight,
            decimate=args.decimate,
            ymin=args.ymin,
            ymax=args.ymax,
            flip_y=not args.no_flip,
        )

        args.output_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"Saving PNG: {args.output_path}")
        fig.savefig(args.output_path, dpi=args.dpi, bbox_inches="tight")

        import matplotlib.pyplot as plt
        plt.close(fig)

        # Pipeline contract: print the final PNG path on stdout.
        print(args.output_path.resolve())

    except Exception as e:
        logger.exception(f"aa-graph failed: {e}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()