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
  --no-pie                  Suppress the pie-chart distribution row drawn
                            below the echogram. By default a pie is shown
                            per channel, sharing colors with the map: for
                            cluster data each non-noise cluster is a wedge,
                            for continuous data (Sv etc.) wedges represent
                            value bins between vmin and vmax.
  --pie-height FLOAT        Height of the pie row in inches (default: 2.6).

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


def _add_proportional_legend(fig, ax, listed_cmap, labels, counts, var,
                             compact=False):
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

    When ``compact=True`` (passed by callers that draw a pie chart with
    its own legend below the figure), text annotations are suppressed
    entirely except for the noise band.  The right-side strip becomes
    a pure visual size-distribution preview, and the cluster identity
    information lives in the pie legend instead — keeping the layout
    width stable at high cluster counts.
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

    # Pick label candidates. In compact mode, only noise (-1) gets a
    # label — full identity lives in the pie legend.  In non-compact
    # mode, also annotate the largest-by-count clusters.
    candidate_idxs = set()
    if _NOISE_LABEL in labels:
        candidate_idxs.add(int(np.where(labels == _NOISE_LABEL)[0][0]))
    if not compact:
        top_n = min(_PROP_LEGEND_TOP_LABELS, len(labels))
        candidate_idxs.update(np.argsort(counts)[-top_n:].tolist())

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
# Pie chart row — distribution-of-clusters (or value bins) under the map
# ---------------------------------------------------------------------------
# A geometrically-consistent companion to the per-channel echogram: one
# pie chart per channel, drawn in a row beneath the maps. Wedge colors
# are sampled from the *same* colormap as the main plot, so a yellow
# wedge in the pie always means the same thing as a yellow region on
# the map.
#
# For categorical data (cluster maps) the pie shows the relative size
# of each *non-noise* cluster — noise / -1 is excluded by convention,
# matching how downstream consumers treat DBSCAN/HDBSCAN output.
#
# For continuous data (Sv, MVBS, TS, NASC) the pie shows the share of
# pixels falling in each of N equal-width value bins between vmin and
# vmax. This effectively answers "how much of the echogram lives in
# the bright/dim parts of the colorbar" — useful as a quick sanity
# check on how full the dynamic range is being used.

# Cap on entries shown in the compact pie legend before the rest are
# rolled into a single "+ N more" entry. Tuned so a 2-column legend
# stays inside its allotted cell at default figure dimensions.
_PIE_LEGEND_MAX_VISIBLE = 16

# Minimum wedge percentage for an in-pie label (e.g. "12%"). Smaller
# wedges leave their label out — they'd just smear over neighbors —
# but they still appear in the legend.
_PIE_INLINE_LABEL_MIN_PCT = 4.0

# Number of bins used to slice continuous data into pie wedges.
_PIE_CONTINUOUS_BINS = 10


def _pie_data_categorical(unique_labels, counts, listed_cmap):
    """Wedge data for a categorical/cluster map.

    Filters out the noise label (-1) so the pie shows only meaningful
    clusters. Returns parallel arrays of (sizes, label_strings, RGBA
    colors) sorted large→small. Colors come from the same
    `listed_cmap` used on the main map, so wedge i and the matching
    map region share a hue exactly.
    """
    if len(unique_labels) == 0:
        return np.array([]), [], []

    is_noise = unique_labels == _NOISE_LABEL
    keep = ~is_noise
    if not np.any(keep):
        return np.array([]), [], []

    sizes = counts[keep].astype(np.int64)
    keep_labels = unique_labels[keep]
    # listed_cmap was built in the same order as unique_labels, so we
    # can index it directly with the kept positions.
    keep_idxs = np.where(keep)[0]
    colors = [listed_cmap(int(i)) for i in keep_idxs]
    label_strings = [str(int(v)) for v in keep_labels]

    order = np.argsort(sizes)[::-1]
    sizes = sizes[order]
    label_strings = [label_strings[i] for i in order]
    colors = [colors[i] for i in order]
    return sizes, label_strings, colors


def _pie_data_continuous(da_panel, cmap_name, eff_vmin, eff_vmax,
                         n_bins=_PIE_CONTINUOUS_BINS):
    """Wedge data for continuous data, binned by value.

    Bin edges span ``[eff_vmin, eff_vmax]``; pixels outside that range
    are dropped (matching how the main map clips them visually). Each
    bin's color is the colormap evaluated at the bin's midpoint so the
    pie reads as a discrete sampling of the same colorbar.
    """
    import matplotlib.pyplot as plt

    vals = np.asarray(da_panel.values).ravel()
    finite = vals[np.isfinite(vals)]
    if finite.size == 0:
        return np.array([]), [], []

    lo = eff_vmin if eff_vmin is not None else float(np.nanmin(finite))
    hi = eff_vmax if eff_vmax is not None else float(np.nanmax(finite))
    if not np.isfinite(lo) or not np.isfinite(hi) or hi <= lo:
        return np.array([]), [], []

    edges = np.linspace(lo, hi, n_bins + 1)
    counts, _ = np.histogram(finite, bins=edges)
    centers = (edges[:-1] + edges[1:]) / 2.0
    norm_centers = (centers - lo) / (hi - lo)

    base = plt.get_cmap(cmap_name)
    colors = [base(float(c)) for c in norm_centers]
    # Compact bin labels: "−72→−64" rather than "−72.00 to −64.00"
    label_strings = [
        f"{edges[i]:.0f}\u2192{edges[i + 1]:.0f}" for i in range(n_bins)
    ]

    # Drop empty bins from the pie entirely (they'd be invisible wedges
    # but would still take a legend row, wasting space).
    keep = counts > 0
    counts = counts[keep]
    label_strings = [label_strings[i] for i, k in enumerate(keep) if k]
    colors = [colors[i] for i, k in enumerate(keep) if k]

    if counts.size == 0:
        return np.array([]), [], []

    # Continuous wedges are NOT sorted by size — they read better in
    # the natural "low→high value" order of the colorbar.
    return counts.astype(np.int64), label_strings, colors


def _render_pie_panel(fig, gs_cell, sizes, label_strings, colors,
                     title, channel_label=None,
                     value_units="", noise_count=0):
    """Draw a pie + compact legend in one GridSpec cell.

    ``gs_cell`` is a single SubplotSpec; we subdivide it into a square
    pie on the left and a fixed-width legend on the right so the
    overall cell shape stays stable regardless of how many wedges
    there are.

    ``noise_count`` (categorical only) is appended to the legend as a
    grey footnote — it's not represented in the pie itself but the
    user usually wants to know how much of the data was discarded.
    """
    if len(sizes) == 0:
        ax = fig.add_subplot(gs_cell)
        ax.text(0.5, 0.5, "(no data)", ha="center", va="center",
                transform=ax.transAxes, fontsize=8, color="gray")
        ax.axis("off")
        return

    sub = gs_cell.subgridspec(1, 2, width_ratios=[1.0, 1.05], wspace=0.05)
    ax_pie = fig.add_subplot(sub[0, 0])
    ax_legend = fig.add_subplot(sub[0, 1])
    ax_legend.axis("off")

    sizes_arr = np.asarray(sizes, dtype=float)
    pct = 100.0 * sizes_arr / sizes_arr.sum()

    # In-wedge percent labels only for wedges large enough to host text.
    def _autopct(p):
        return f"{p:.0f}%" if p >= _PIE_INLINE_LABEL_MIN_PCT else ""

    wedges, _texts, autotexts = ax_pie.pie(
        sizes_arr,
        startangle=90,
        colors=colors,
        autopct=_autopct,
        pctdistance=0.72,
        textprops={"fontsize": 7},
        wedgeprops={"linewidth": 0.5, "edgecolor": "white"},
    )
    for at, color in zip(autotexts, colors):
        at.set_color(_text_color_for_bg(color))

    ax_pie.set_aspect("equal")
    if channel_label:
        ax_pie.set_title(channel_label, fontsize=8)

    _render_compact_legend(
        ax_legend, label_strings, sizes_arr, pct, colors,
        title=title, value_units=value_units, noise_count=noise_count,
    )


def _render_compact_legend(ax, label_strings, sizes, pct, colors,
                          title, value_units="", noise_count=0):
    """Multi-column legend with hard cap on visible rows.

    Limits visible entries to ``_PIE_LEGEND_MAX_VISIBLE`` and rolls the
    rest into a single grey "+ N more" patch.  The number of columns
    grows from 1 → 2 as entries pile up, but the total *area* of the
    legend is capped so the figure layout never expands to fit it.

    Result: stable layout at any cluster count, even at 200+ clusters.
    """
    from matplotlib.patches import Patch

    n = len(label_strings)
    pct = np.asarray(pct, dtype=float)

    # Column count grows with row count, but never past 2 — at 3+
    # columns the per-column width gets too narrow for "ID  count (pct)".
    if n <= 8:
        ncol = 1
    else:
        ncol = 2

    visible = min(_PIE_LEGEND_MAX_VISIBLE, n)

    handles = []
    text_labels = []
    suffix = f" {value_units}" if value_units else ""
    for i in range(visible):
        text_labels.append(
            f"{label_strings[i]}{suffix}  {int(sizes[i]):,}  ({pct[i]:.1f}%)"
        )
        handles.append(Patch(facecolor=colors[i], edgecolor="none"))

    if visible < n:
        remaining = n - visible
        rem_count = int(sizes[visible:].sum())
        rem_pct = pct[visible:].sum()
        handles.append(Patch(facecolor="lightgray", edgecolor="none"))
        text_labels.append(
            f"+ {remaining} more  {rem_count:,}  ({rem_pct:.1f}%)"
        )

    if noise_count > 0:
        handles.append(Patch(facecolor=_NOISE_COLOR, edgecolor="none"))
        text_labels.append(f"noise (excluded)  {noise_count:,}")

    legend_title = title
    if n > visible:
        legend_title = f"{title} — top {visible} of {n}"

    ax.legend(
        handles, text_labels,
        loc="center",
        ncol=ncol,
        fontsize=6.5,
        frameon=False,
        labelspacing=0.35,
        columnspacing=0.6,
        handlelength=1.2,
        handleheight=1.0,
        borderpad=0.2,
        title=legend_title,
        title_fontsize=7.5,
    )


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
    pie: bool = True,
    pie_height: float = 2.6,
):
    """Plot a Sv-style echogram from a NetCDF file. Returns the matplotlib Figure.

    Auto-displays inline when called as the last expression of a Jupyter cell.
    From a script, use `fig.savefig(path)` to write to disk.

    Multi-channel datasets get one subplot per channel by default. Pass
    `channel=` or `frequency=` to plot a single channel.

    A pie-chart row is drawn beneath the echogram showing the
    distribution of clusters (categorical data) or value bins
    (continuous data); pass ``pie=False`` to suppress it.
    """
    import xarray as xr
    import matplotlib.pyplot as plt
    import matplotlib.gridspec as gridspec

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
    x_dim = _detect_x_dim(da)

    # ---- Pre-pass: prepare per-panel data ---------------------------------
    # We resolve the data array, decide categorical-vs-continuous, and (for
    # categorical) compute the cluster stats and ListedColormap once. This
    # information is consumed twice — by the main map render and by the
    # pie chart render — so doing it up front avoids duplicated work and
    # ensures the two views stay perfectly in sync (same colors, same
    # cluster ordering).
    panel_specs = []
    for ci in channel_idxs:
        if ci is None:
            da_panel = da
            label = None
        else:
            da_panel = da.isel({chan_dim: ci})
            label = _short_channel_label(ds, chan_dim, ci)

        da_panel, y_dim = _resolve_y_axis(ds, da_panel, x_dim)
        da_panel = _decimate(da_panel, x_dim, decimate)
        da_panel = _crop_y(da_panel, y_dim, ymin, ymax)

        is_cat = _is_categorical(da_panel)
        spec = {
            "ci": ci,
            "da_panel": da_panel,
            "y_dim": y_dim,
            "label": label,
            "is_cat": is_cat,
        }

        if is_cat:
            unique_labels, counts = _cluster_label_stats(da_panel)
            spec["unique_labels"] = unique_labels
            spec["counts"] = counts
            if len(unique_labels) > 0:
                spec["listed_cmap"], spec["norm"] = _build_discrete_cmap(
                    cmap, unique_labels
                )
            else:
                spec["listed_cmap"], spec["norm"] = None, None

        panel_specs.append(spec)

    # ---- Layout ----------------------------------------------------------
    # GridSpec with one row per channel, plus an optional row at the bottom
    # for the pie chart distribution. Using GridSpec rather than subplots()
    # gives us control over per-row heights (the pie row is shorter than
    # the echogram rows) and lets the pie row hold its own per-channel
    # subgrid without disturbing sharex on the echograms.
    show_pie = bool(pie) and n_panels > 0
    n_rows = n_panels + (1 if show_pie else 0)
    height_ratios = [rowheight] * n_panels
    if show_pie:
        height_ratios.append(pie_height)
    fig_height = sum(height_ratios)

    fig = plt.figure(figsize=(figwidth, fig_height))
    gs = gridspec.GridSpec(
        n_rows, 1,
        height_ratios=height_ratios,
        figure=fig,
        hspace=0.35,
    )

    # ---- Render echogram panels ------------------------------------------
    map_axes = []
    for i, spec in enumerate(panel_specs):
        sharex = map_axes[0] if i > 0 else None
        ax = fig.add_subplot(gs[i, 0], sharex=sharex)
        map_axes.append(ax)

        da_panel = spec["da_panel"]
        y_dim = spec["y_dim"]
        is_cat = spec["is_cat"]
        do_flip = flip_y and _should_flip_y(y_dim)

        plotted_categorical = False
        if is_cat:
            unique_labels = spec["unique_labels"]
            counts = spec["counts"]
            listed_cmap = spec["listed_cmap"]
            norm = spec["norm"]
            if len(unique_labels) > 0:
                if vmin is not None or vmax is not None:
                    logger.warning(
                        "vmin/vmax are ignored for categorical/cluster data; "
                        "using full label range."
                    )
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
                    # When the pie row is also rendering, defer cluster
                    # identity to the pie's compact legend and let the
                    # right-side strip be a clean visual size preview.
                    _add_proportional_legend(
                        fig, ax, listed_cmap,
                        unique_labels, counts, var,
                        compact=show_pie,
                    )
                plotted_categorical = True
            # else: all-NaN panel → fall through to continuous branch.

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
            # Stash effective limits for the pie row to mirror.
            spec["eff_vmin"] = eff_vmin
            spec["eff_vmax"] = eff_vmax

        if spec["label"]:
            ax.set_title(spec["label"], fontsize=10)
        else:
            ax.set_title("")

    # ---- Render pie chart row --------------------------------------------
    # One pie per channel, side-by-side, sharing colors with the matching
    # echogram panel above. Width is split evenly across channels — for a
    # single channel the pie spans the full row and tends to look quite
    # large; that's intentional, since solo plots have screen real estate
    # to spare and a bigger pie reads better.
    if show_pie:
        pie_gs = gs[n_panels, 0].subgridspec(1, n_panels, wspace=0.25)
        for i, spec in enumerate(panel_specs):
            cell = pie_gs[0, i]
            channel_label = spec["label"]
            if spec["is_cat"]:
                if spec.get("listed_cmap") is None:
                    # All-NaN panel → empty cell with placeholder text.
                    _render_pie_panel(fig, cell, np.array([]), [], [],
                                     title=var, channel_label=channel_label)
                    continue
                sizes, labels_s, colors = _pie_data_categorical(
                    spec["unique_labels"], spec["counts"],
                    spec["listed_cmap"],
                )
                noise_count = int(spec["counts"][
                    spec["unique_labels"] == _NOISE_LABEL
                ].sum()) if _NOISE_LABEL in spec["unique_labels"] else 0
                _render_pie_panel(
                    fig, cell, sizes, labels_s, colors,
                    title=var,
                    channel_label=channel_label,
                    noise_count=noise_count,
                )
            else:
                _, _, units = _VAR_DISPLAY_DEFAULTS.get(var, (None, None, ""))
                sizes, labels_s, colors = _pie_data_continuous(
                    spec["da_panel"], cmap,
                    spec.get("eff_vmin"), spec.get("eff_vmax"),
                )
                _render_pie_panel(
                    fig, cell, sizes, labels_s, colors,
                    title=var,
                    channel_label=channel_label,
                    value_units=units,
                )

    fig.suptitle(f"{var}  ·  {Path(path).name}", fontsize=11)
    fig.tight_layout(rect=[0, 0, 1, 0.97])
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
    p.add_argument("--no-pie", action="store_true",
                   help="Suppress the pie-chart distribution row.")
    p.add_argument("--pie-height", type=float, default=2.6,
                   help="Pie row height in inches (default: 2.6).")
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
            pie=not args.no_pie,
            pie_height=args.pie_height,
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