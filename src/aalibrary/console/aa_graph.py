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
  --vmin FLOAT              Lower color limit. Default: per-variable
                            (-80 dB for Sv/Sv_clean/MVBS, -90 dB for TS,
                            autoscaled for NASC). For categorical data
                            (cluster maps, masks) vmin/vmax are ignored
                            and a discrete legend is used instead.
  --vmax FLOAT              Upper color limit. Default: per-variable
                            (-30 dB for Sv/Sv_clean/MVBS, -20 dB for TS,
                            autoscaled for NASC). Ignored for categorical.
  --cmap NAME               Matplotlib colormap (default: viridis). For
                            cluster maps with many clusters, try 'hsv',
                            'tab20', 'gist_rainbow' for more contrast.
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


# Per-variable plotting defaults: (vmin, vmax, units_for_cbar_label).
# Categorical variables (masks, cluster maps, region labels) bypass this
# entirely and autoscale to the data range — see _is_categorical below.
# Anything not in this table falls back to autoscale; that's the right
# behaviour for unknown variables since we have no idea what range they
# live in.  Add new entries here as new dB-domain (or other) variables
# come up — one line each, no other code changes needed.
_VAR_DISPLAY_DEFAULTS = {
    # name        : (vmin, vmax, units)
    "Sv":          (-80, -30, "dB"),
    "Sv_clean":    (-80, -30, "dB"),
    "MVBS":        (-80, -30, "dB"),
    "TS":          (-90, -20, "dB"),    # single-target TS spans wider than Sv
    "NASC":        (None, None, "m\u00b2 nmi\u207b\u00b2"),  # linear; autoscale
}

# Default-variable search order when --var is not given.  First hit wins.
# Cluster outputs come AFTER physical quantities so a file that happens to
# contain both still prefers the echogram (`Sv`); pass `--var cluster_map`
# (or whatever) to override.
_DEFAULT_VAR_CANDIDATES = (
    # dB-domain echograms first (most common)
    "Sv", "Sv_clean", "Sv_filtered", "Svf",
    "MVBS", "TS",
    # linear-domain
    "NASC",
    # cluster / label outputs (kept broad — different upstream tools use
    # different names; with this list `aa-graph foo_kmeans.nc` Just Works
    # whether it stored labels as `cluster_map`, `clusters`, or `labels`)
    "cluster_map", "cluster_labels", "clusters", "cluster",
    "kmeans_labels", "dbscan_labels", "hdbscan_labels", "labels",
)


def _score_var_for_plotting(da) -> int:
    """Score a data variable on how echogram-shaped it is.

    Used as a last-ditch fallback in `_ensure_variable` when neither
    ``--var`` nor any name in `_DEFAULT_VAR_CANDIDATES` matches. We'd
    rather pick a 3-D `(channel, ping_time, range_sample)` variable than
    something 1-D and unrelated like `frequency_nominal`.
    """
    score = 0
    has_time = any(d in da.dims for d in ("ping_time", "time", "ping"))
    has_range = any(
        d in da.dims for d in (_DEPTH_RANGE_NAMES | {"range_sample"})
    )
    if has_time and has_range:
        score += 100        # a real echogram-shaped array
    elif has_time or has_range:
        score += 10         # halfway there
    if da.ndim == 3:
        score += 5          # bonus for channel dim
    elif da.ndim == 2:
        score += 1
    return score


def _ensure_variable(ds, var: Optional[str]) -> str:
    """Resolve which data variable to plot.

    Resolution order:
      1. If --var was passed and matches a data_var exactly: use it.
      2. If --var was passed and matches case-insensitively: use that
         (with a log line so the user knows what happened).
      3. If --var was passed but matches nothing: raise, with a
         "did-you-mean" suggestion built from `difflib`.
      4. Otherwise scan `_DEFAULT_VAR_CANDIDATES` in order, first hit wins.
      5. Otherwise (file uses non-standard names) score every data_var
         on how echogram-shaped it is and pick the best — beats the old
         behaviour of blindly returning `list(ds.data_vars)[0]`, which
         could land on a 1-D coord-like variable.
    """
    if var is not None:
        if var in ds.data_vars:
            return var
        ci_match = next(
            (v for v in ds.data_vars if v.lower() == var.lower()), None
        )
        if ci_match is not None:
            logger.info(
                f"Variable '{var}' resolved to '{ci_match}' (case-insensitive match)."
            )
            return ci_match
        from difflib import get_close_matches
        # Case-insensitive matching with a generous cutoff so realistic
        # typos like "Sv_clena", "SVCLEAN", "kmeans" all pull useful
        # suggestions. We map lowercased candidates back to their
        # original-case names for the user-facing message.
        names = list(ds.data_vars)
        lower_to_orig = {n.lower(): n for n in names}
        ci_suggestions = get_close_matches(
            var.lower(), list(lower_to_orig.keys()), n=3, cutoff=0.4
        )
        suggestions = [lower_to_orig[s] for s in ci_suggestions]
        msg = f"Variable '{var}' not found. Available: {names}"
        if suggestions:
            msg += f". Did you mean: {suggestions}?"
        raise ValueError(msg)

    for cand in _DEFAULT_VAR_CANDIDATES:
        if cand in ds.data_vars:
            return cand

    if not ds.data_vars:
        raise ValueError("No data variables in file.")

    # Shape-based fallback: prefer (channel, time, range)-like arrays.
    ranked = sorted(
        ds.data_vars,
        key=lambda v: _score_var_for_plotting(ds[v]),
        reverse=True,
    )
    chosen = ranked[0]
    logger.info(
        f"No standard variable name matched; auto-selected '{chosen}' based on "
        f"its dim shape. Other data vars: {ranked[1:]}"
    )
    return chosen


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
    region masks, KMeans / DBSCAN / HDBSCAN output, etc.).

    Adapted from aa-plot's `_is_categorical`. Two triggers:
      - dtype is integer / unsigned / bool, OR
      - dtype is float but every finite value is integer-valued.

    Note: aa-plot's version also gated the float case on "< 50 unique values"
    to avoid mislabeling continuous-but-coincidentally-integer data as
    categorical. That gate is intentionally dropped here, because HDBSCAN /
    DBSCAN routinely produce hundreds of clusters, and noise represented as
    NaN forces the labels array to float dtype. In echo-sounder data,
    integer-valued floats are essentially always labels; continuous physical
    quantities (Sv, TS, range, depth, NASC) are not stored that way.

    HDBSCAN / DBSCAN noise label of -1 is handled correctly: it's still
    integer-valued, so the array is detected as categorical and matplotlib
    autoscales the colormap to include it.
    """
    if da.dtype.kind in ("i", "u", "b"):
        return True
    try:
        vals = np.asarray(da.values).ravel()
        finite = vals[np.isfinite(vals)]
        if finite.size == 0:
            return False
        # Subsample for the integer-valued check; full pass not needed.
        sample_size = min(50_000, finite.size)
        step = max(1, finite.size // sample_size)
        sample = finite[::step]
        if np.all(np.equal(np.mod(sample, 1.0), 0.0)):
            return True
    except Exception:
        pass
    return False


# ---------------------------------------------------------------------------
# Discrete / cluster-aware legends
# ---------------------------------------------------------------------------
# When the data is categorical (cluster_map, KMeans / DBSCAN / HDBSCAN
# labels, masks…) a continuous colorbar is the wrong tool. Two regimes:
#
#   * Few clusters (≤ _MAX_DISCRETE_TICKS, typically KMeans output) →
#     a regular discrete colorbar with one tick per cluster, annotated
#     with the cluster's record count.
#
#   * Many clusters (DBSCAN / HDBSCAN, can be hundreds) → a custom
#     "weighted" legend where each cluster's vertical band height is
#     proportional to log1p(count). This means the colors visible in
#     the legend are the same colors the eye picks out of the map:
#     the dominant cluster takes up the most legend space. A linear
#     proportion would let one giant cluster swallow the whole legend
#     and hide everything else, hence the log compression. Top-N
#     clusters and the noise label (-1) get text annotations; the
#     rest are colored bands only.
#
# DBSCAN/HDBSCAN noise (label -1) is always rendered in mid-gray
# regardless of `--cmap` — that's the conventional reading of "noise".

_MAX_DISCRETE_TICKS = 15        # cutoff between the two legend styles
_PROP_LEGEND_TOP_LABELS = 10    # # of top-N clusters to text-annotate
_NOISE_LABEL = -1               # DBSCAN / HDBSCAN noise convention
_NOISE_COLOR = (0.55, 0.55, 0.55, 1.0)  # mid gray RGBA


def _cluster_label_stats(da):
    """Return ``(sorted_unique_labels, counts)`` for a categorical array.

    NaN / non-finite values are excluded (HDBSCAN sometimes stores noise
    as NaN instead of -1; either way it's not a real cluster).  Returns
    integer label values when every finite value is integer-valued,
    which is the common case.
    """
    vals = np.asarray(da.values).ravel()
    finite = vals[np.isfinite(vals)]
    if finite.size == 0:
        return np.array([], dtype=np.int64), np.array([], dtype=np.int64)
    if np.all(np.equal(np.mod(finite, 1.0), 0.0)):
        finite = finite.astype(np.int64)
    unique, counts = np.unique(finite, return_counts=True)
    return unique, counts


def _build_discrete_cmap(cmap_name: str, labels):
    """Build a (ListedColormap, BoundaryNorm) pair aligned to the given labels.

    The base colormap is sampled at ``len(labels)`` evenly-spaced points,
    so neighboring cluster IDs get visually distinct colors. The noise
    label (-1), if present, is hard-coded to gray regardless of cmap.

    BoundaryNorm edges are placed at the midpoints between adjacent
    label values, so sparse / non-contiguous label sets (e.g. {0, 1, 5,
    99}) still map cleanly: each label gets its own band.
    """
    from matplotlib.colors import ListedColormap, BoundaryNorm
    import matplotlib.pyplot as plt

    n = len(labels)
    if n == 0:
        return ListedColormap([_NOISE_COLOR]), None

    base = plt.get_cmap(cmap_name)
    has_noise = bool((labels == _NOISE_LABEL).any())
    non_noise_count = n - 1 if has_noise else n

    if non_noise_count == 0:
        non_noise_colors = []
    elif non_noise_count == 1:
        non_noise_colors = [base(0.5)]
    else:
        non_noise_colors = [
            base(i / (non_noise_count - 1)) for i in range(non_noise_count)
        ]

    nn_iter = iter(non_noise_colors)
    colors = [
        _NOISE_COLOR if lbl == _NOISE_LABEL else next(nn_iter)
        for lbl in labels
    ]
    listed = ListedColormap(colors)

    if n == 1:
        edges = np.array([labels[0] - 0.5, labels[0] + 0.5], dtype=float)
    else:
        labels_f = labels.astype(float)
        mids = (labels_f[:-1] + labels_f[1:]) / 2.0
        edges = np.concatenate(
            [[labels_f[0] - 0.5], mids, [labels_f[-1] + 0.5]]
        )
    norm = BoundaryNorm(edges, ncolors=n)
    return listed, norm


def _text_color_for_bg(rgba) -> str:
    """Return 'white' or 'black' for max contrast against ``rgba`` background."""
    r, g, b = rgba[:3]
    # Standard ITU-R BT.601 luma coefficients
    lum = 0.299 * r + 0.587 * g + 0.114 * b
    return "white" if lum < 0.5 else "black"


def _add_discrete_colorbar(fig, ax, listed_cmap, norm, labels, counts, var):
    """One-tick-per-cluster colorbar; suitable when cluster count is small.

    Each tick label looks like ``"3  (12,847)"`` — cluster id, then the
    record count in parens, so the user can compare cluster sizes at a
    glance without leaving the figure.
    """
    from matplotlib.cm import ScalarMappable

    sm = ScalarMappable(norm=norm, cmap=listed_cmap)
    sm.set_array([])
    cbar = fig.colorbar(sm, ax=ax, ticks=list(labels))
    tick_labels = []
    for lbl, cnt in zip(labels, counts):
        tag = "noise" if lbl == _NOISE_LABEL else str(int(lbl))
        tick_labels.append(f"{tag}  ({cnt:,})")
    cbar.ax.set_yticklabels(tick_labels, fontsize=8)
    cbar.set_label(var)


def _add_proportional_legend(fig, ax, listed_cmap, labels, counts, var):
    """Vertical legend whose band heights are ~log of each cluster's count.

    Shape is roughly that of a stacked bar: each cluster gets a colored
    band whose height is ``log1p(count) / sum(log1p(counts))`` of the
    available vertical space.  Labels are added inside the bands for
    the largest ``_PROP_LEGEND_TOP_LABELS`` clusters plus noise (so the
    legend stays readable even with hundreds of clusters); smaller
    clusters are colored-only.

    To prevent unreadable text-stacking when several large bands cluster
    near each other (common with long-tail distributions), label
    placement is greedy: candidate labels are sorted by y-position and
    any label that would overlap a previously-placed one is dropped.
    """
    from mpl_toolkits.axes_grid1 import make_axes_locatable

    weights = np.log1p(counts.astype(float))
    if weights.sum() == 0:
        weights = np.ones_like(weights)
    weights = weights / weights.sum()
    cum = np.concatenate([[0.0], np.cumsum(weights)])

    divider = make_axes_locatable(ax)
    cax = divider.append_axes("right", size="5%", pad=0.12)
    cax.set_xlim(0, 1)
    cax.set_ylim(0, 1)

    # Draw all colored bands first (no labels yet).
    for i, lbl in enumerate(labels):
        cax.fill_between([0, 1], cum[i], cum[i + 1],
                         color=listed_cmap(i), linewidth=0)

    # Pick label candidates: largest-by-count clusters, plus noise (-1).
    top_n = min(_PROP_LEGEND_TOP_LABELS, len(labels))
    candidate_idxs = set(np.argsort(counts)[-top_n:].tolist())
    if _NOISE_LABEL in labels:
        candidate_idxs.add(int(np.where(labels == _NOISE_LABEL)[0][0]))

    # Greedy non-overlapping placement, ordered bottom→top so noise (which
    # is usually at index 0 / bottom) anchors first when present.
    candidates = sorted(
        candidate_idxs,
        key=lambda i: (cum[i] + cum[i + 1]) / 2.0,
    )
    # Min vertical separation between label centers, in axis coords.
    # ~0.035 = ~3.5% of legend height per text line; tuned for fontsize 6.5.
    min_sep = 0.035
    last_y = -1.0
    for i in candidates:
        y0, y1 = cum[i], cum[i + 1]
        y_mid = (y0 + y1) / 2.0
        if y_mid - last_y < min_sep:
            continue            # would overlap previous label → skip
        if (y1 - y0) < 0.008:   # band too thin to host any text
            continue
        lbl = labels[i]
        cnt = counts[i]
        tag = "noise" if lbl == _NOISE_LABEL else str(int(lbl))
        cax.text(
            0.5, y_mid, f"{tag} ({cnt:,})",
            ha="center", va="center",
            fontsize=6.5,
            color=_text_color_for_bg(listed_cmap(i)),
        )
        last_y = y_mid

    cax.set_xticks([])
    cax.set_yticks([])
    for spine in cax.spines.values():
        spine.set_visible(False)
    cax.set_title(f"{var}\n({len(labels)} clusters)", fontsize=8)


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

        # Resolve color limits and colorbar label.  Two regimes:
        #
        # CONTINUOUS data (Sv, TS, MVBS, NASC, …) uses xarray's built-in
        # pcolormesh + auto-colorbar with per-variable defaults from
        # `_VAR_DISPLAY_DEFAULTS`. User --vmin/--vmax always override.
        #
        # CATEGORICAL data (cluster maps, masks, KMeans/DBSCAN/HDBSCAN
        # labels) bypasses the auto-colorbar entirely. We build a
        # ListedColormap + BoundaryNorm so each cluster gets exactly one
        # color, then draw either a discrete colorbar (few clusters) or
        # a weighted/proportional legend (many clusters). vmin/vmax are
        # meaningless for label data and are ignored with a warning.
        is_cat = _is_categorical(da_panel)
        do_flip = flip_y and _should_flip_y(y_dim)

        plotted_categorical = False
        if is_cat:
            unique_labels, counts = _cluster_label_stats(da_panel)
            if len(unique_labels) > 0:
                if vmin is not None or vmax is not None:
                    logger.warning(
                        "vmin/vmax are ignored for categorical/cluster data; "
                        "using full label range."
                    )
                listed_cmap, norm = _build_discrete_cmap(cmap, unique_labels)
                da_panel.plot.pcolormesh(
                    x=x_dim, y=y_dim, ax=ax,
                    cmap=listed_cmap, norm=norm,
                    yincrease=not do_flip,
                    add_colorbar=False,
                )
                if len(unique_labels) <= _MAX_DISCRETE_TICKS:
                    _add_discrete_colorbar(
                        fig, ax, listed_cmap, norm,
                        unique_labels, counts, var,
                    )
                else:
                    _add_proportional_legend(
                        fig, ax, listed_cmap,
                        unique_labels, counts, var,
                    )
                plotted_categorical = True
            # else: all-NaN panel → fall through to the continuous branch
            # (which will produce a sensibly-empty plot).

        if not plotted_categorical:
            default_vmin, default_vmax, units = _VAR_DISPLAY_DEFAULTS.get(
                var, (None, None, "")
            )
            eff_vmin = vmin if vmin is not None else default_vmin
            eff_vmax = vmax if vmax is not None else default_vmax
            cbar_label = f"{var} ({units})" if units else f"{var}"
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