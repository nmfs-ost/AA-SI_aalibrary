#!/usr/bin/env python3
"""
aa-integrate

Echo integration the way Echoview does it: Sv integrated into cells (intervals
along track x layers in depth), between exclusion lines, without bad data,
above a minimum threshold, by cells, by regions, or by region-cell
intersections; written as a CSV with Echoview's column names.

Definitions (Echoview's, for single-beam Sv; s indexes samples, p pings):

    e_s = 0 for a sample with no data (NaN), in a "bad (no data)" region, or
          outside the exclusion lines; else 1
    t_s = 0 for a sample below the minimum threshold or in a "bad (empty
          water)" region; else 1. Samples above the maximum threshold are
          set to it.
    Sv_mean        = 10 log10( sum e_s t_s sv_s / sum e_s )      (sv linear)
    Thickness_mean = (1/Np) sum_p dr_p sum_s e_s     Np: pings of the cell
                     with at least one sample counted; dr_p the range spacing
    Height_mean    = the same with the depth spacing (= Thickness_mean for a
                     vertical transducer)
    ABC            = 10^(Sv_mean/10) Thickness_mean               m2 m-2
    NASC           = 4 pi 1852^2 ABC                              m2 nmi-2
    PRC_ABC        = ABC_RC * Np_RC / Np_C       (region-cell intersection
    PRC_NASC       = NASC_RC * Np_RC / Np_C       RC within cell C): summed
                     over a cell's intersections they give the cell's value

A cell with no counted samples has Sv_mean -999 and NASC 0.
"""

from __future__ import annotations

import logging
import sys
import warnings

logging.disable(logging.CRITICAL)
warnings.filterwarnings("ignore")

from loguru import logger  # noqa: E402

logger.remove()
logger.add(sys.stderr, level="WARNING")

import argparse  # noqa: E402
import csv  # noqa: E402
import math  # noqa: E402
import re  # noqa: E402
from dataclasses import dataclass, field  # noqa: E402
from pathlib import Path  # noqa: E402

from aalibrary.console._core import (  # noqa: E402
    Help, Run, ToolSpec, add_common_flags, canon, render, show_help, stdio,
)

NMI = 1852.0
NASC_FACTOR = 4 * math.pi * NMI ** 2
NO_DATA_DB = -999.0

_QTY = re.compile(r"^\s*(?P<num>[0-9]*\.?[0-9]+)\s*(?P<unit>[a-zA-Z]*)\s*$")
_TIME_UNITS = {"": 1.0, "s": 1.0, "sec": 1.0, "min": 60.0, "h": 3600.0,
               "hr": 3600.0}
_DIST_UNITS = {"": 1.0, "m": 1.0, "km": 1000.0, "nmi": NMI, "nm": NMI}


def interval_size(kind: str, text) -> float:
    """--interval as seconds (time), metres (distance) or pings."""
    if text is None:
        raise ValueError("--interval is required")
    m = _QTY.match(str(text))
    if not m:
        raise ValueError(f"--interval: not a quantity: {text!r}")
    num, unit = float(m.group("num")), m.group("unit").lower()
    if kind == "time":
        if unit not in _TIME_UNITS:
            raise ValueError(f"--interval for time: s, min or h, not {unit!r}")
        value = num * _TIME_UNITS[unit]
    elif kind == "distance":
        if unit not in _DIST_UNITS:
            raise ValueError(f"--interval for distance: m, km or nmi, not {unit!r}")
        value = num * _DIST_UNITS[unit]
    else:
        if unit not in ("", "ping", "pings"):
            raise ValueError("--interval for ping: a number of pings")
        value = float(int(num))
    if value <= 0:
        raise ValueError("--interval must be positive")
    return value


def infer_interval_type(text, has_track: bool) -> str:
    """--interval-type auto: from --interval's unit (5min: time, 0.5nmi:
    distance, 100: pings), else distance with a track, else time."""
    if text:
        m = _QTY.match(str(text))
        unit = m.group("unit").lower() if m else ""
        if unit in _TIME_UNITS and unit != "":
            return "time"
        if unit in _DIST_UNITS and unit != "":
            return "distance"
        if unit in ("", "ping", "pings"):
            return "ping"
    return "distance" if has_track else "time"


def _frequencies(value):
    """--frequency values (38kHz, 38000, '18,38kHz') -> sorted Hz."""
    if not value:
        return None
    from aalibrary.console._calibration import parse_frequency

    out = set()
    for item in value if isinstance(value, (list, tuple)) else [value]:
        for part in str(item).split(","):
            if part.strip():
                hz = parse_frequency(part) if not isinstance(item, (int, float)) else float(item)
                out.add(int(hz) if float(hz).is_integer() else hz)
    return sorted(out) or None


def _qty(text):
    return None if text is None else str(text).strip().lower().replace(" ", "")


SPEC = ToolSpec(
    name="aa-integrate",
    role="transform",
    kind="integration",
    op="aa_integrate.echoview_cells",
    op_version=1,
    engines=("echopype",),
    ext=".csv",
    params={
        "by": canon.choice("lower"),
        "interval_type": canon.choice("lower"),
        "interval": _qty,
        "layer_type": canon.choice("lower"),
        "layer": canon.number,
        "min_sv": canon.number,
        "max_sv": canon.number,
        "surface_depth": canon.number,
        "bottom_offset": canon.number,
        "surface_offset": canon.number,
        "frequency": _frequencies,
        "var": canon.text,
    },
)

HELP = Help(
    summary="Echoview-style echo integration into cells / regions; NASC, Sv_mean, ABC as CSV.",
    does=(
        "Integrates Sv (linear) over cells: intervals along the track (--interval-type "
        "time | distance | ping, --interval 5min | 0.5nmi | 100) by layers (--layer-type "
        "depth | range | surface | bottom, --layer 10). Samples above the surface line "
        "(--surface FILE.evl or --surface-depth M) and below the bottom line (--bottom "
        "FILE.evl or a seafloor product, plus --bottom-offset) are excluded; bad-data "
        "regions (--bad FILE.evr) are excluded (type bad) or counted as empty water "
        "(type bad_empty); samples below --min-sv count as empty water; above --max-sv "
        "are set to it.\n\n"
        "--by cells (default): one row per channel x interval x layer. --by regions: one "
        "row per analysis region of --regions (whole region). --by region-cells: one row "
        "per region-cell intersection with PRC_NASC / PRC_ABC, which add up to the cell.\n\n"
        "Columns are Echoview's (Interval, Layer, Sv_mean, NASC, ABC, Height_mean, "
        "Thickness_mean, Depth_mean, Layer_depth_min/max, Samples, Good_samples, "
        "No_data_samples, Ping_S/E, Date_S/E/M (YYYYMMDD), Time_S/E/M (HH:MM:SS.ssss), "
        "Lat/Lon_S/E/M, Dist_S/E/M (metres along the track), VL_start/VL_end (nmi), "
        "Exclude_above/below_line_depth_mean, threshold columns, Frequency, Channel), "
        "plus Region_ID, Region_name, Region_class, PRC_NASC, PRC_ABC for region "
        "exports. Depths are in metres, from 'depth' (aa-depth) or else echo_range."
    ),
    stdin="One Sv product (.nc or .zarr) path or gs:// URI; depth (aa-depth) recommended; "
          "latitude/longitude (aa-location) for distance intervals and positions.",
    stdout="The CSV's path (or gs:// URI).",
    options=[
        ("--by cells|regions|region-cells", "what a row is (default cells)"),
        ("--interval-type time|distance|ping", "along-track cells (default: from "
                                               "--interval's unit; else distance with a "
                                               "track, else time)"),
        ("--interval Q", "cell length: 0.5nmi, 926m, 5min, 300s, 100 (pings)"),
        ("--layer-type depth|range|surface|bottom", "vertical reference (default depth)"),
        ("--layer M", "layer thickness, metres (default 10)"),
        ("--surface FILE.evl / --surface-depth M", "exclude above this line / depth"),
        ("--bottom FILE", "exclude below this line (.evl, or a seafloor product)"),
        ("--bottom-offset M", "added to the bottom line (negative: shallower), default 0"),
        ("--surface-offset M", "added to the surface line, default 0"),
        ("--bad FILE.evr", "bad-data regions (types bad, bad_empty); repeatable"),
        ("--regions FILE.evr", "analysis regions for --by regions / region-cells"),
        ("--min-sv DB / --max-sv DB", "integration thresholds"),
        ("--frequency F", "only these channels (38kHz, ...); repeatable"),
        ("--echodata FILE", "EchoData for the track when the Sv has no position"),
        ("-o, --output_path PATH", "exact output; local or gs://"),
    ],
    science={
        "by": "Rows: cells, regions or region-cells.",
        "interval_type": "Along-track cell type.",
        "interval": "Along-track cell length.",
        "layer_type": "Vertical reference of the layers.",
        "layer": "Layer thickness, metres.",
        "min_sv": "Minimum integration threshold, dB.",
        "max_sv": "Maximum integration threshold, dB.",
        "surface_depth": "Fixed exclude-above depth, metres.",
        "bottom_offset": "Metres added to the bottom line.",
        "surface_offset": "Metres added to the surface line.",
        "frequency": "Channels integrated (Hz).",
        "var": "The Sv variable integrated.",
    },
    files=(
        "Reads NetCDF or Zarr, local or gs://; line and region files too (their content "
        "is in the hash). Writes <base>_<hash8>.csv beside the input (current directory "
        "for gs:// input), or -o, or --dest, with <file>.aa.json."
    ),
    pipeline="... | aa-sv | aa-depth | aa-location | aa-integrate --bottom bottom.evl "
             "--bottom-offset -0.5 --interval 0.5nmi --layer 10 --min-sv -70",
    examples=[
        "aa-integrate sv.nc --interval 0.5nmi --layer 10 --min-sv -70 --bottom bottom.evl",
        "aa-integrate sv.nc --by region-cells --regions schools.evr --interval 5min",
        "aa-integrate sv.nc --layer-type bottom --layer 5 --bottom seafloor.nc",
    ],
)


def print_help():
    sys.stdout.write(render(SPEC, HELP, _build_parser()))


def _build_parser():
    p = argparse.ArgumentParser(prog="aa-integrate", add_help=False,
                                description="Echoview-style echo integration.")
    p.add_argument("input_path", type=str, nargs="?")
    p.add_argument("-o", "--output_path", type=str, default=None)
    p.add_argument("--by", choices=["cells", "regions", "region-cells"], default="cells")
    p.add_argument("--interval-type", dest="interval_type",
                   choices=["auto", "time", "distance", "ping"], default="auto")
    p.add_argument("--interval", type=str, default=None, metavar="Q")
    p.add_argument("--layer-type", dest="layer_type",
                   choices=["depth", "range", "surface", "bottom"], default="depth")
    p.add_argument("--layer", type=float, default=10.0, metavar="M")
    p.add_argument("--surface", type=str, default=None, metavar="FILE")
    p.add_argument("--surface-depth", dest="surface_depth", type=float, default=None)
    p.add_argument("--surface-offset", dest="surface_offset", type=float, default=0.0)
    p.add_argument("--bottom", type=str, default=None, metavar="FILE")
    p.add_argument("--bottom-offset", dest="bottom_offset", type=float, default=0.0)
    p.add_argument("--bad", action="append", default=None, metavar="FILE")
    p.add_argument("--regions", type=str, default=None, metavar="FILE")
    p.add_argument("--min-sv", dest="min_sv", type=float, default=None, metavar="DB")
    p.add_argument("--max-sv", dest="max_sv", type=float, default=None, metavar="DB")
    p.add_argument("--frequency", action="append", default=None, metavar="F")
    p.add_argument("--var", type=str, default=None)
    p.add_argument("--echodata", type=str, default=None, metavar="FILE")
    p.add_argument("--quiet", action="store_true")
    add_common_flags(p)
    return p


# --------------------------------------------------------------------------- #
# Lines and regions
# --------------------------------------------------------------------------- #
def line_at(points: list[dict], times_ms):
    """A line's depth at each ping (linear; held flat past its ends)."""
    import numpy as np

    pts = sorted((p for p in points if math.isfinite(p["depth"])), key=lambda p: p["t"])
    if not pts:
        raise ValueError("the line has no points")
    t = np.array([p["t"] for p in pts], dtype=float)
    d = np.array([p["depth"] for p in pts], dtype=float)
    return np.interp(np.asarray(times_ms, dtype=float), t, d)


def read_line(path: Path) -> list[dict]:
    from aalibrary.console import _echoview as ev
    from aalibrary.console.aa_annotate import seafloor_points

    if path.suffix.lower() == ".evl":
        return ev.read_evl(path)
    return seafloor_points(path)


@dataclass
class Region:
    id: int
    name: str
    klass: str
    kind: str
    t0: float
    t1: float
    d0: float
    d1: float
    path: object = None

    @classmethod
    def of(cls, r: dict) -> "Region":
        from matplotlib.path import Path as MPath

        pts = [(float(p["t"]), float(p["depth"])) for p in r["points"]]
        ts = [a for a, _ in pts]
        ds = [b for _, b in pts]
        t0 = min(ts)
        # Seconds from the region's start: the two axes of similar magnitude.
        local = [((a - t0) / 1000.0, b) for a, b in pts]
        return cls(int(r["id"]), r.get("name", ""), r.get("class", ""), r.get("kind", "analysis"),
                   t0, max(ts), min(ds), max(ds), MPath(local))

    def inside(self, t_ms, y):
        """Boolean (pings, samples): samples of this block inside the polygon."""
        import numpy as np

        out = np.zeros(y.shape, dtype=bool)
        rows = np.where((t_ms >= self.t0) & (t_ms <= self.t1))[0]
        if rows.size == 0:
            return out
        sub = y[rows]
        cand = (sub >= self.d0) & (sub <= self.d1)
        if not cand.any():
            return out
        rr, cc = np.nonzero(cand)
        pts = np.column_stack([(t_ms[rows][rr] - self.t0) / 1000.0, sub[rr, cc]])
        hit = self.path.contains_points(pts, radius=1e-9)
        out[rows[rr[hit]], cc[hit]] = True
        return out


# --------------------------------------------------------------------------- #
# Accumulation
# --------------------------------------------------------------------------- #
@dataclass
class Cell:
    sv_sum: float = 0.0
    good: int = 0
    samples: int = 0
    nodata: int = 0
    depth_sum: float = 0.0
    sv_max: float = -math.inf
    sv_min: float = math.inf
    thick_sum: float = 0.0
    height_sum: float = 0.0
    pings: int = 0
    ping_s: int = 1 << 62
    ping_e: int = -1
    extras: dict = field(default_factory=dict)

    def add(self, other: "Cell") -> None:
        self.sv_sum += other.sv_sum
        self.good += other.good
        self.samples += other.samples
        self.nodata += other.nodata
        self.depth_sum += other.depth_sum
        self.sv_max = max(self.sv_max, other.sv_max)
        self.sv_min = min(self.sv_min, other.sv_min)
        self.thick_sum += other.thick_sum
        self.height_sum += other.height_sum
        self.pings += other.pings
        self.ping_s = min(self.ping_s, other.ping_s)
        self.ping_e = max(self.ping_e, other.ping_e)

    def results(self) -> dict:
        if self.good == 0 or self.pings == 0:
            return {"Sv_mean": NO_DATA_DB, "NASC": 0.0, "ABC": 0.0,
                    "Thickness_mean": 0.0, "Height_mean": 0.0}
        mean_lin = self.sv_sum / self.good
        thickness = self.thick_sum / self.pings
        height = self.height_sum / self.pings
        abc = mean_lin * thickness
        return {
            "Sv_mean": 10 * math.log10(mean_lin) if mean_lin > 0 else NO_DATA_DB,
            "NASC": NASC_FACTOR * abc, "ABC": abc,
            "Thickness_mean": thickness, "Height_mean": height,
        }


def _accumulate(cells: dict, keys, ok, counted_lin, y, sv_db, ping_index, dr, dh, samples_mask):
    """Add one block's samples to the cells. keys: (pings, samples) int64 cell
    key per sample (-1: outside every cell)."""
    import numpy as np

    valid = keys >= 0
    if not valid.any():
        return
    flat_keys = keys[valid]
    uniq, inv = np.unique(flat_keys, return_inverse=True)
    n = len(uniq)
    okf = ok[valid]
    sv_sum = np.bincount(inv, weights=np.where(okf, counted_lin[valid], 0.0), minlength=n)
    good = np.bincount(inv, weights=okf.astype(float), minlength=n)
    samples = np.bincount(inv, weights=samples_mask[valid].astype(float), minlength=n)
    depth_sum = np.bincount(inv, weights=np.where(okf, y[valid], 0.0), minlength=n)
    sv_ok = np.where(okf, sv_db[valid], np.nan)
    smax = np.full(n, -np.inf)
    smin = np.full(n, np.inf)
    finite = np.isfinite(sv_ok)
    np.maximum.at(smax, inv[finite], sv_ok[finite])
    np.minimum.at(smin, inv[finite], sv_ok[finite])
    # Per ping and cell: samples counted -> thickness, and whether the ping counts.
    rows = np.broadcast_to(np.arange(keys.shape[0])[:, None], keys.shape)[valid]
    pair = inv.astype(np.int64) * keys.shape[0] + rows
    pair_good = np.bincount(pair, weights=okf.astype(float), minlength=n * keys.shape[0])
    pair_good = pair_good.reshape(n, keys.shape[0])
    thick = (pair_good * dr[None, :]).sum(axis=1)
    height = (pair_good * dh[None, :]).sum(axis=1)
    pings = (pair_good > 0).sum(axis=1)
    has = pair_good > 0
    first = np.where(has.any(axis=1), np.argmax(has, axis=1), -1)
    last = np.where(has.any(axis=1), keys.shape[0] - 1 - np.argmax(has[:, ::-1], axis=1), -1)
    for i, key in enumerate(uniq.tolist()):
        cell = cells.setdefault(key, Cell())
        part = Cell(sv_sum=float(sv_sum[i]), good=int(good[i]), samples=int(samples[i]),
                    nodata=int(samples[i] - good[i]), depth_sum=float(depth_sum[i]),
                    sv_max=float(smax[i]), sv_min=float(smin[i]), thick_sum=float(thick[i]),
                    height_sum=float(height[i]), pings=int(pings[i]),
                    ping_s=int(ping_index[first[i]]) if first[i] >= 0 else 1 << 62,
                    ping_e=int(ping_index[last[i]]) if last[i] >= 0 else -1)
        cell.add(part)


# --------------------------------------------------------------------------- #
# Integration
# --------------------------------------------------------------------------- #
def haversine_m(lat, lon):
    """Cumulative distance along a track, metres (NaN fixes carried over)."""
    import numpy as np

    lat = np.asarray(lat, dtype=float)
    lon = np.asarray(lon, dtype=float)
    good = np.isfinite(lat) & np.isfinite(lon)
    if good.sum() < 2:
        raise ValueError("no usable track (latitude/longitude) for distance intervals")
    idx = np.arange(len(lat))
    la = np.interp(idx, idx[good], lat[good])
    lo = np.interp(idx, idx[good], lon[good])
    r = 6371008.8
    p1, p2 = np.radians(la[:-1]), np.radians(la[1:])
    dphi = p2 - p1
    dlmb = np.radians(lo[1:] - lo[:-1])
    a = np.sin(dphi / 2) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(dlmb / 2) ** 2
    step = 2 * r * np.arcsin(np.sqrt(np.clip(a, 0, 1)))
    return np.concatenate([[0.0], np.cumsum(step)])


@dataclass
class Setup:
    by: str = "cells"
    interval_type: str = "time"
    interval: float = 300.0
    layer_type: str = "depth"
    layer: float = 10.0
    min_sv: float | None = None
    max_sv: float | None = None
    surface: list | None = None
    surface_depth: float | None = None
    surface_offset: float = 0.0
    bottom: list | None = None
    bottom_offset: float = 0.0
    bad: list = field(default_factory=list)
    regions: list = field(default_factory=list)
    frequency: list | None = None
    var: str | None = None


LAYER_KEY = 1 << 20
REGION_KEY = 1 << 40


def integrate(ds, setup: Setup, track=(None, None), block: int = 1024) -> list[dict]:
    """All rows (dicts with Echoview column names) for this dataset."""
    import numpy as np

    from aalibrary.console import _flat

    var = _flat.value_var(ds, setup.var)
    da = ds[var]
    tdim, rdim = _flat.time_dim(da), _flat.range_dim(da)
    if tdim is None or rdim is None:
        raise ValueError(f"{var} is not on (ping_time x range)")
    has_channel = "channel" in da.dims
    da = da.transpose(*((("channel",) if has_channel else ()) + (tdim, rdim)))
    n_pings = da.sizes[tdim]
    times = ds[tdim].values.astype("datetime64[ns]")
    t_ms = times.astype(np.int64) / 1e6
    lat, lon = track
    if lat is None and "latitude" in ds and ds["latitude"].dims == (tdim,):
        lat = np.asarray(ds["latitude"].values, dtype=float)
        lon = np.asarray(ds["longitude"].values, dtype=float)
    dist = None
    if lat is not None:
        try:
            dist = haversine_m(lat, lon)
        except ValueError:
            dist = None

    if setup.interval_type == "time":
        unit = setup.interval * 1000.0
        interval_of = np.floor(t_ms / unit).astype(np.int64)
        interval_of -= interval_of.min()
    elif setup.interval_type == "distance":
        if dist is None:
            raise ValueError("distance intervals need latitude/longitude (aa-location, "
                             "or --echodata)")
        interval_of = np.floor(dist / setup.interval).astype(np.int64)
    else:
        interval_of = (np.arange(n_pings) // int(setup.interval)).astype(np.int64)
    interval_of = interval_of + 1      # Echoview numbers intervals from 1

    surface = None
    if setup.surface is not None:
        surface = line_at(setup.surface, t_ms) + setup.surface_offset
    elif setup.surface_depth is not None:
        surface = np.full(n_pings, float(setup.surface_depth) + setup.surface_offset)
    bottom = None
    if setup.bottom is not None:
        bottom = line_at(setup.bottom, t_ms) + setup.bottom_offset
    if setup.layer_type == "bottom" and bottom is None:
        raise ValueError("--layer-type bottom needs --bottom")
    if setup.layer_type == "surface" and surface is None:
        raise ValueError("--layer-type surface needs --surface or --surface-depth")

    bad = [Region.of(r) for r in setup.bad if r.get("kind") in ("bad", "bad_empty")]
    analysis = [Region.of(r) for r in setup.regions if r.get("kind", "analysis") == "analysis"]
    if setup.by != "cells" and not analysis:
        raise ValueError(f"--by {setup.by} needs --regions with analysis regions")

    channels = list(range(da.sizes["channel"])) if has_channel else [None]
    freqs = (np.asarray(ds["frequency_nominal"].values, dtype=float).reshape(-1)
             if has_channel and "frequency_nominal" in ds else None)
    if setup.frequency:
        if freqs is None:
            raise ValueError("--frequency: the product has no frequency_nominal")
        channels = [c for c in channels if any(abs(freqs[c] - f) < 0.5 for f in setup.frequency)]
        if not channels:
            raise ValueError("--frequency: no channel at those frequencies")

    rows: list[dict] = []
    for ch in channels:
        y_name = "depth" if "depth" in ds else "echo_range" if "echo_range" in ds else None
        if y_name is None:
            raise ValueError("the product has neither depth nor echo_range")
        y_da = ds[y_name]
        r_da = ds["echo_range"] if "echo_range" in ds else y_da
        if ch is not None:
            if "channel" in y_da.dims:
                y_da = y_da.isel(channel=ch)
            if "channel" in r_da.dims:
                r_da = r_da.isel(channel=ch)

        def block_of(arr, start, stop):
            if tdim in arr.dims:
                part = arr.isel({tdim: slice(start, stop)}).transpose(tdim, rdim)
                return np.asarray(part.values, dtype=float)
            row = np.asarray(arr.transpose(rdim).values, dtype=float)
            return np.broadcast_to(row, (stop - start, row.size))

        cells: dict = {}       # key -> Cell (interval x layer [x region])
        rcells: dict = {}      # region-cells: key -> Cell
        regions_whole: dict = {}
        excl = {}              # interval -> [sum above, n, sum below, n]
        for start in range(0, n_pings, block):
            stop = min(n_pings, start + block)
            sel = {tdim: slice(start, stop)}
            if ch is not None:
                sel["channel"] = ch
            sv_db = np.asarray(da.isel(sel).values, dtype=float)
            y = block_of(y_da, start, stop)
            rng = block_of(r_da, start, stop)
            tb = t_ms[start:stop]
            pidx = np.arange(start, stop)
            within = np.isfinite(y)
            if surface is not None:
                within &= y >= surface[start:stop, None]
            if bottom is not None:
                within &= y <= bottom[start:stop, None]
            ok = within & np.isfinite(sv_db)
            counted = np.where(np.isfinite(sv_db), sv_db, -np.inf)
            if setup.max_sv is not None:
                counted = np.minimum(counted, setup.max_sv)
            lin = np.power(10.0, counted / 10.0)
            if setup.min_sv is not None:
                lin = np.where(counted < setup.min_sv, 0.0, lin)
            for region in bad:
                inside = region.inside(tb, y)
                if region.kind == "bad":
                    ok &= ~inside
                else:
                    lin = np.where(inside, 0.0, lin)
            dr = _spacing(rng)
            dh = _spacing(y)
            if setup.layer_type == "depth":
                rel = y
            elif setup.layer_type == "range":
                rel = rng
            elif setup.layer_type == "surface":
                rel = y - surface[start:stop, None]
            else:
                rel = bottom[start:stop, None] - y
            layer = np.floor(rel / setup.layer).astype(np.int64) + 1
            interval = interval_of[start:stop]
            cell_ok = within & (layer >= 1) & (layer < LAYER_KEY)
            keys = np.where(cell_ok, interval[:, None] * LAYER_KEY + layer, -1)
            _accumulate(cells, keys, ok & cell_ok, lin, y, sv_db, pidx, dr, dh, cell_ok)
            for i, b in enumerate(interval.tolist()):
                acc = excl.setdefault(b, [0.0, 0, 0.0, 0])
                if surface is not None:
                    acc[0] += surface[start + i]
                    acc[1] += 1
                if bottom is not None:
                    acc[2] += bottom[start + i]
                    acc[3] += 1
            if setup.by != "cells":
                for region in analysis:
                    inside = region.inside(tb, y) & within
                    if not inside.any():
                        continue
                    if setup.by == "regions":
                        whole = np.where(inside, region.id, -1)
                        _accumulate(regions_whole, whole, ok & inside, lin, y, sv_db, pidx,
                                    dr, dh, inside)
                    else:
                        rk = np.where(inside & cell_ok,
                                      region.id * REGION_KEY + interval[:, None] * LAYER_KEY
                                      + layer, -1)
                        _accumulate(rcells, rk, ok & inside & cell_ok, lin, y, sv_db, pidx,
                                    dr, dh, inside & cell_ok)

        freq = float(freqs[ch]) if freqs is not None and ch is not None else None
        cid = (str(ds["channel"].values[ch]) if ch is not None and "channel" in ds.coords
               else var)
        common = {"Frequency": freq / 1000 if freq else "", "Channel": cid}
        common.update(_thresholds(setup))
        position = _Position(t_ms, lat, lon, dist)
        by_id = {r.id: r for r in analysis}
        if setup.by == "cells":
            for key in sorted(cells):
                cell = cells[key]
                b, k = divmod(key, LAYER_KEY)
                rows.append(_row(cell, b, k, setup, position, excl, common))
        elif setup.by == "regions":
            for rid in sorted(regions_whole):
                cell = regions_whole[rid]
                region = by_id[rid]
                row = _row(cell, "", "", setup, position, {}, common)
                row.update(_region_cols(region))
                rows.append(row)
        else:
            for key in sorted(rcells):
                cell = rcells[key]
                rid, rest = divmod(key, REGION_KEY)
                b, k = divmod(rest, LAYER_KEY)
                region = by_id[rid]
                row = _row(cell, b, k, setup, position, excl, common)
                row.update(_region_cols(region))
                whole = cells.get(b * LAYER_KEY + k)
                share = cell.pings / whole.pings if whole is not None and whole.pings else 0.0
                res = cell.results()
                row["PRC_NASC"] = res["NASC"] * share
                row["PRC_ABC"] = res["ABC"] * share
                rows.append(row)
    return rows


def _spacing(y):
    """Sample spacing per ping (median of the differences), metres."""
    import numpy as np

    with np.errstate(all="ignore"):
        d = np.diff(y, axis=1)
        d[~np.isfinite(d)] = np.nan
        out = np.nanmedian(np.abs(d), axis=1) if d.shape[1] else np.zeros(y.shape[0])
    return np.nan_to_num(out, nan=0.0)


def _thresholds(setup: Setup) -> dict:
    return {
        "Minimum_Sv_threshold_applied": int(setup.min_sv is not None),
        "Minimum_integration_threshold": setup.min_sv if setup.min_sv is not None else -999,
        "Maximum_Sv_threshold_applied": int(setup.max_sv is not None),
        "Maximum_integration_threshold": setup.max_sv if setup.max_sv is not None else 999,
    }


class _Position:
    def __init__(self, t_ms, lat, lon, dist):
        self.t, self.lat, self.lon, self.dist = t_ms, lat, lon, dist

    def at(self, i: int) -> dict:
        import datetime as dt

        t = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc) + dt.timedelta(
            milliseconds=float(self.t[i]))
        out = {"date": t.strftime("%Y%m%d"),
               "time": f"{t:%H:%M:%S}.{t.microsecond // 100:04d}"}
        out["lat"] = _num(self.lat[i]) if self.lat is not None else 999.0
        out["lon"] = _num(self.lon[i]) if self.lon is not None else 999.0
        out["dist"] = _num(self.dist[i]) if self.dist is not None else 0.0
        return out


def _num(value) -> float:
    v = float(value)
    return v if math.isfinite(v) else 999.0


def _row(cell: Cell, interval, layer, setup: Setup, pos: _Position, excl: dict,
         common: dict) -> dict:
    res = cell.results()
    row = {"Interval": interval, "Layer": layer}
    row.update(res)
    row["Sv_max"] = cell.sv_max if math.isfinite(cell.sv_max) else NO_DATA_DB
    row["Sv_min"] = cell.sv_min if math.isfinite(cell.sv_min) else NO_DATA_DB
    row["Depth_mean"] = cell.depth_sum / cell.good if cell.good else 0.0
    if layer != "":
        # From the layer reference: depth or range from the transducer, metres
        # below the surface line, or metres above the bottom line.
        row["Layer_depth_min"] = (layer - 1) * setup.layer
        row["Layer_depth_max"] = layer * setup.layer
    else:
        row["Layer_depth_min"] = ""
        row["Layer_depth_max"] = ""
    row["Layer_reference"] = setup.layer_type
    row["Samples"] = cell.samples
    row["Good_samples"] = cell.good
    row["No_data_samples"] = cell.nodata
    row["Num_pings"] = cell.pings
    if cell.ping_e >= 0:
        s, e = cell.ping_s, cell.ping_e
        m = (s + e) // 2
        ps, pe, pm = pos.at(s), pos.at(e), pos.at(m)
        row.update({"Ping_S": s, "Ping_E": e,
                    "Date_S": ps["date"], "Time_S": ps["time"], "Date_E": pe["date"],
                    "Time_E": pe["time"], "Date_M": pm["date"], "Time_M": pm["time"],
                    "Lat_S": ps["lat"], "Lon_S": ps["lon"], "Lat_E": pe["lat"], "Lon_E": pe["lon"],
                    "Lat_M": pm["lat"], "Lon_M": pm["lon"],
                    "Dist_S": ps["dist"], "Dist_E": pe["dist"], "Dist_M": pm["dist"],
                    "VL_start": ps["dist"] / NMI, "VL_end": pe["dist"] / NMI})
    acc = excl.get(interval) if interval != "" else None
    row["Exclude_above_line_depth_mean"] = (acc[0] / acc[1] if acc and acc[1] else "")
    row["Exclude_below_line_depth_mean"] = (acc[2] / acc[3] if acc and acc[3] else "")
    row.update(common)
    return row


def _region_cols(region: Region) -> dict:
    return {"Region_ID": region.id, "Region_name": region.name, "Region_class": region.klass}


COLUMNS = [
    "Interval", "Layer", "Region_ID", "Region_name", "Region_class", "Sv_mean", "NASC", "ABC",
    "PRC_NASC", "PRC_ABC", "Sv_max", "Sv_min", "Height_mean", "Thickness_mean", "Depth_mean",
    "Layer_depth_min", "Layer_depth_max", "Layer_reference", "Samples", "Good_samples",
    "No_data_samples", "Num_pings", "Ping_S", "Ping_E", "Date_S", "Time_S", "Date_E", "Time_E",
    "Date_M", "Time_M", "Lat_S", "Lon_S", "Lat_E", "Lon_E", "Lat_M", "Lon_M", "Dist_S",
    "Dist_E", "Dist_M", "VL_start", "VL_end", "Exclude_above_line_depth_mean",
    "Exclude_below_line_depth_mean", "Minimum_Sv_threshold_applied",
    "Minimum_integration_threshold", "Maximum_Sv_threshold_applied",
    "Maximum_integration_threshold", "Frequency", "Channel",
]


def write_csv(rows: list[dict], path: Path, by: str) -> None:
    drop = set()
    if by == "cells":
        drop |= {"Region_ID", "Region_name", "Region_class", "PRC_NASC", "PRC_ABC"}
    elif by == "regions":
        drop |= {"Interval", "Layer", "PRC_NASC", "PRC_ABC", "Layer_depth_min",
                 "Layer_depth_max", "Exclude_above_line_depth_mean",
                 "Exclude_below_line_depth_mean"}
    cols = [c for c in COLUMNS if c not in drop]
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(cols)
        for row in rows:
            writer.writerow([_fmt(row.get(c, "")) for c in cols])


def _fmt(value) -> str:
    if isinstance(value, float):
        if not math.isfinite(value):
            return "-999"
        return f"{value:.10g}"
    return str(value)


def main() -> None:
    if len(sys.argv) == 1 and not stdio.stdin_is_piped():
        print_help()
        sys.exit(0)
    parser = _build_parser()
    if show_help(SPEC, HELP, parser):
        sys.exit(0)
    args = parser.parse_args()
    if args.surface and args.surface_depth is not None:
        print("aa-integrate: give --surface or --surface-depth, not both", file=sys.stderr)
        sys.exit(2)
    if args.layer <= 0:
        print("aa-integrate: --layer must be positive", file=sys.stderr)
        sys.exit(2)

    from aalibrary.console import _echoview as ev, _flat
    from aalibrary.console.aa_tiles import _track

    token = stdio.one_input(args.input_path, SPEC.name)
    probe = Run(SPEC, args)
    src = probe.input(token)
    track = (None, None)
    try:
        head = _flat.open_flat(src.local)
        var = _flat.value_var(head, args.var)
        has_track = "latitude" in head and "longitude" in head
        if not has_track and args.echodata:
            # The position is in the EchoData: read it now, since whether
            # there is one decides the default intervals (distance or time).
            ed_probe = probe.input(args.echodata, role="echodata")
            track = _track(head, _flat.time_dim(head[var]), ed_probe.local)
        head.close()
    except (ValueError, OSError) as exc:
        print(f"aa-integrate: {exc}", file=sys.stderr)
        sys.exit(1)
    import numpy as np

    has_position = has_track or (
        track[0] is not None and bool(np.isfinite(track[0]).any()))
    interval_type = args.interval_type
    if interval_type == "auto":
        interval_type = infer_interval_type(args.interval, has_position)
    interval_text = args.interval or {"distance": "0.5nmi", "time": "5min", "ping": "100"}[
        interval_type]
    try:
        size = interval_size(interval_type, interval_text)
        freqs = _frequencies(args.frequency)
    except ValueError as exc:
        print(f"aa-integrate: {exc}", file=sys.stderr)
        sys.exit(2)

    params = {"var": var, "interval_type": interval_type, "interval": _qty(interval_text),
              "frequency": freqs}
    run = Run(SPEC, args, params=params)
    run.input(token)
    files = {}
    for flag, role in (("surface", "lines"), ("bottom", "lines"), ("regions", "regions")):
        value = getattr(args, flag)
        if value:
            files[flag] = run.param_file(value, role=f"{role}:{flag}")
    bad_files = [run.param_file(b, role="regions:bad") for b in args.bad or []]
    echodata = run.input(args.echodata, role="echodata") if args.echodata else None
    out = run.plan(ext=".csv", explicit=args.output_path or None)
    if run.reusable(out):
        run.finish(out)
        return
    try:
        setup = Setup(by=args.by, interval_type=interval_type, interval=size,
                      layer_type=args.layer_type, layer=args.layer, min_sv=args.min_sv,
                      max_sv=args.max_sv, surface_depth=args.surface_depth,
                      surface_offset=args.surface_offset, bottom_offset=args.bottom_offset,
                      frequency=freqs, var=var)
        if "surface" in files:
            setup.surface = read_line(files["surface"].local)
        if "bottom" in files:
            setup.bottom = read_line(files["bottom"].local)
        if "regions" in files:
            setup.regions = ev.read_evr(files["regions"].local)
        for b in bad_files:
            setup.bad.extend(ev.read_evr(b.local))
        ds = _flat.open_flat(src.local, chunks={})
        if has_track or echodata is None:
            track = (None, None)   # the Sv's own position (or none) is used
        rows = integrate(ds, setup, track=track)
        ds.close()
        write_csv(rows, out.local, args.by)
    except ValueError as exc:
        run.discard(out)
        print(f"aa-integrate: {exc}", file=sys.stderr)
        sys.exit(1)
    except Exception as exc:  # noqa: BLE001
        run.discard(out)
        logger.exception(f"aa-integrate: {exc}")
        sys.exit(1)
    nasc = sum(r["NASC"] for r in rows if isinstance(r.get("NASC"), float))
    run.finish(out, extra={"rows": len(rows), "by": args.by,
                           "interval": f"{interval_type} {interval_text}",
                           "layer": f"{args.layer_type} {args.layer:g} m",
                           "nasc_total": round(nasc, 3)})


if __name__ == "__main__":
    main()
