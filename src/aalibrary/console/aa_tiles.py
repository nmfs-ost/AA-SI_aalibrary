#!/usr/bin/env python3
"""
aa-tiles

Make an echogram tile pack (.tiles) of an Sv, MVBS, TS or mask product: the
data, every channel, pre-reduced into a pyramid of tiles, so a viewer (the
Workbench's Echogram panel) can show a whole survey leg and zoom to single
samples while fetching only the tiles on screen. The values are kept (not
colours): the viewer applies thresholds and colour maps itself and reads
real values under the cursor.

Format: aalibrary/console/_tilepack.py.

Pipeline-friendly: reads the product's path (or gs:// URI) from a positional
argument or stdin, prints the pack's path (or gs:// URI), logs on stderr.
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
import datetime as _dt  # noqa: E402
import math  # noqa: E402

from aalibrary.console._core import (  # noqa: E402
    Help, Run, ToolSpec, add_common_flags, canon, render, show_help, stdio,
)

SPEC = ToolSpec(
    name="aa-tiles",
    role="representation",
    kind="tiles",
    op="aa_tiles.pyramid",
    op_version=1,
    engines=(),
    ext=".tiles",
    params={
        "var": canon.text,
        "y": canon.choice("lower"),
        "tile": canon.integer,
        "lag": canon.integer,
        "reduce": canon.choice("lower"),
    },
)

DB_VARS = {"Sv", "Sv_corrected", "MVBS", "TS", "Sv_noise", "Sv_clean"}

HELP = Help(
    summary="Echogram tile pack (.tiles) of an Sv / MVBS / TS / mask product, for viewers.",
    does=(
        "Reads one (ping_time x range) variable of every channel, puts it on a regular "
        "range (or depth) grid, and writes a pyramid of 256 x 256 tiles: level 0 is the "
        "data itself; each level above averages twice as many pings, and (after --lag "
        "levels) twice as many samples. dB values are averaged in the linear domain "
        "(--reduce mean) or the strongest is kept (--reduce max); masks keep the "
        "fraction (mean) or any (max) of their True samples.\n\n"
        "The pack also holds each ping's time and, when the product (or --echodata) "
        "has them, its latitude and longitude, and each channel's value percentiles "
        "for default display thresholds.\n\n"
        "dB values are stored to 0.004 dB (-180 .. +82 dB; weaker is -180, empty "
        "water at -999 dB too); other values as float32."
    ),
    stdin="One flat product (.nc or .zarr) path or gs:// URI: Sv, Sv with depth, MVBS, TS, a mask.",
    stdout="The tile pack's path (or gs:// URI).",
    options=[
        ("--var NAME", "the variable (default: Sv, Sv_corrected, MVBS, TS, or the mask)"),
        ("--y range|depth|sample", "vertical axis (default: depth when the product has "
                                   "it, else range)"),
        ("--reduce mean|max", "how cells are combined (default: mean; max for masks)"),
        ("--tile N", "tile size (default 256)"),
        ("--lag N", "levels before range is reduced too (default 2)"),
        ("--echodata FILE", "EchoData to read the ship's track from, when the product "
                            "has no latitude/longitude"),
        ("-o, --output_path PATH", "exact output; local or gs://"),
    ],
    science={
        "var": "The variable drawn.",
        "y": "Vertical axis.",
        "tile": "Tile size.",
        "lag": "Levels before range is reduced.",
        "reduce": "How cells are combined.",
    },
    files=(
        "Reads NetCDF or Zarr, local or gs://. Writes <product name>.tiles beside the "
        "input (current directory for gs:// input), or -o, or --dest, with "
        "<file>.aa.json. An identical pack already there is reused."
    ),
    pipeline="After any product: ... | aa-sv | aa-depth | aa-tiles. The Workbench "
             "runs it when an echogram is opened.",
    examples=[
        "aa-tiles sv.nc",
        "aa-tiles gs://bucket/derived_products/me/HB1603/x/x_1234abcd.nc --dest gs://bucket/derived_products/me/HB1603/x/",
        "aa-tiles mask.nc --reduce max",
    ],
)


def print_help():
    sys.stdout.write(render(SPEC, HELP, _build_parser()))


def _build_parser():
    p = argparse.ArgumentParser(prog="aa-tiles", add_help=False,
                                description="Echogram tile pack.")
    p.add_argument("input_path", type=str, nargs="?")
    p.add_argument("-o", "--output_path", type=str, default=None)
    p.add_argument("--var", type=str, default=None)
    p.add_argument("--y", choices=["auto", "range", "depth", "sample"], default="auto")
    p.add_argument("--reduce", choices=["auto", "mean", "max"], default="auto")
    p.add_argument("--tile", type=int, default=256)
    p.add_argument("--lag", type=int, default=2)
    p.add_argument("--echodata", type=str, default=None, metavar="FILE")
    p.add_argument("--block-levels", dest="block_levels", type=int, default=3,
                   help=argparse.SUPPRESS)
    p.add_argument("--quiet", action="store_true")
    add_common_flags(p)
    return p


# --------------------------------------------------------------------------- #
# What is drawn
# --------------------------------------------------------------------------- #
def resolve_var(ds, preferred: str | None) -> tuple[str, str]:
    """(variable, nature): nature is 'db', 'mask' or 'value'."""
    from aalibrary.console import _flat
    from aalibrary.console.aa_mask import MEANINGS

    if preferred:
        var = _flat.value_var(ds, preferred)
    else:
        masks = [n for n in MEANINGS if n in ds.data_vars]
        var = masks[0] if masks else _flat.value_var(ds)
    da = ds[var]
    units = str(da.attrs.get("units", "")).lower()
    if da.dtype == bool or var in MEANINGS:
        return var, "mask"
    if var in DB_VARS or "db" in units:
        return var, "db"
    return var, "value"


class YAxis:
    """One channel's vertical axis on a regular grid, and how to put a block of
    pings on it (nearest sample; samples outside the grid are dropped)."""

    def __init__(self, ds, var: str, channel, tdim: str, rdim: str, which: str):
        import numpy as np

        self.tdim, self.rdim = tdim, rdim
        self.name, self.unit = "range_sample", ""
        source = None
        if which in ("depth", "auto") and "depth" in ds:
            source, self.name = ds["depth"], "depth"
        elif which == "depth":
            raise ValueError("--y depth: the product has no depth (add it with aa-depth)")
        if source is None and which in ("range", "auto") and "echo_range" in ds:
            source, self.name = ds["echo_range"], "echo_range"
        if source is None and rdim in ("echo_range", "depth") and rdim in ds.coords:
            source, self.name = ds[rdim], rdim
        if source is not None and "channel" in source.dims:
            source = source.isel(channel=channel) if channel is not None else source.isel(channel=0)
        if source is not None and rdim not in source.dims:
            source = None
            self.name = "range_sample"
        self.source = source
        n_samples = ds.sizes[rdim]
        if source is None:
            self.unit = ""
            self.ref = np.arange(n_samples, dtype=float)
            self.per_ping = False
            self.start, self.step, self.count = 0.0, 1.0, n_samples
            self.idx = np.arange(n_samples)
            return
        self.unit = "m"
        if tdim not in source.dims:
            self.ref = np.asarray(source.values, dtype=float)
            self.per_ping = False
        else:
            source = source.transpose(tdim, rdim)
            self.source = source
            n = source.sizes[tdim]
            probe = sorted({0, n - 1, *[int(i) for i in np.linspace(0, n - 1, 64)]})
            rows = np.asarray(source.isel({tdim: probe}).values, dtype=float)
            first = next((r for r in rows if np.isfinite(r).any()), rows[0])
            self.ref = first
            same = all(np.allclose(r, first, equal_nan=True, atol=1e-6) for r in rows)
            self.per_ping = not same
        valid = self.ref[np.isfinite(self.ref)]
        if valid.size < 2:
            raise ValueError(f"{self.name} has fewer than two valid samples")
        steps = np.diff(valid)
        self.step = float(np.median(steps[steps > 0])) if (steps > 0).any() else 1.0
        if self.per_ping:
            lo, hi = math.inf, -math.inf
            for start in range(0, source.sizes[tdim], 4096):
                block = np.asarray(source.isel({tdim: slice(start, start + 4096)}).values,
                                   dtype=float)
                if np.isfinite(block).any():
                    lo = min(lo, float(np.nanmin(block)))
                    hi = max(hi, float(np.nanmax(block)))
            self.start = lo
            self.count = int(round((hi - lo) / self.step)) + 1
        else:
            self.start = float(valid[0])
            self.count = int(round((float(valid[-1]) - self.start) / self.step)) + 1
        self.count = max(1, min(self.count, 65536))
        if not self.per_ping:
            self.idx = self._nearest(self.ref)

    def grid(self):
        import numpy as np

        return self.start + np.arange(self.count) * self.step

    def _nearest(self, y):
        """For each grid row, the index of the sample nearest to it (-1: none)."""
        import numpy as np

        grid = self.grid()
        ok = np.isfinite(y)
        if not ok.any():
            return np.full(self.count, -1)
        ys = np.where(ok, y, np.inf)
        order = np.argsort(ys, kind="stable")
        sorted_y = ys[order]
        n_ok = int(ok.sum())
        pos = np.searchsorted(sorted_y[:n_ok], grid)
        lo = np.clip(pos - 1, 0, n_ok - 1)
        hi = np.clip(pos, 0, n_ok - 1)
        pick = np.where(np.abs(sorted_y[lo] - grid) <= np.abs(sorted_y[hi] - grid), lo, hi)
        idx = order[pick]
        dist = np.abs(ys[idx] - grid)
        idx[dist > self.step * 0.75] = -1
        return idx

    def regrid(self, block, start: int):
        """block: (pings, samples) -> (pings, grid rows), NaN where no sample."""
        import numpy as np

        if not self.per_ping:
            out = block[:, np.clip(self.idx, 0, None)]
            out[:, self.idx < 0] = np.nan
            return out
        ys = np.asarray(self.source.isel({self.tdim: slice(start, start + block.shape[0])}).values,
                        dtype=float)
        out = np.full((block.shape[0], self.count), np.nan, dtype=np.float32)
        same = np.all(np.isclose(ys, self.ref[None, :], equal_nan=True, atol=1e-6), axis=1)
        if same.any():
            idx = self._nearest(self.ref)
            sel = block[same][:, np.clip(idx, 0, None)]
            sel[:, idx < 0] = np.nan
            out[same] = sel
        for i in np.where(~same)[0]:
            idx = self._nearest(ys[i])
            row = block[i, np.clip(idx, 0, None)]
            row = np.where(idx < 0, np.nan, row)
            out[i] = row
        return out


def _track(ds, tdim: str, echodata_path):
    """(latitude, longitude) per ping, or (None, None)."""
    import numpy as np

    lat = lon = None
    if "latitude" in ds and "longitude" in ds:
        la, lo = ds["latitude"], ds["longitude"]
        if tdim in la.dims and la.ndim == 1:
            lat = np.asarray(la.values, dtype=float)
            lon = np.asarray(lo.values, dtype=float)
    if lat is None and echodata_path is not None:
        try:
            import echopype as ep

            ed = ep.open_converted(str(echodata_path))
            plat = ed["Platform"]
            tname = next((d for d in plat["latitude"].dims), None)
            t_src = plat[tname].values.astype("datetime64[ns]").astype(np.int64).astype(float)
            t_dst = ds[tdim].values.astype("datetime64[ns]").astype(np.int64).astype(float)
            la = np.asarray(plat["latitude"].values, dtype=float)
            lo = np.asarray(plat["longitude"].values, dtype=float)
            ok = np.isfinite(la) & np.isfinite(lo) & np.isfinite(t_src)
            if ok.sum() >= 2:
                order = np.argsort(t_src[ok])
                ts, las, los = t_src[ok][order], la[ok][order], lo[ok][order]
                lat = np.interp(t_dst, ts, las, left=np.nan, right=np.nan)
                lon = np.interp(t_dst, ts, los, left=np.nan, right=np.nan)
        except Exception as exc:  # noqa: BLE001 - the track is optional
            logger.warning(f"aa-tiles: no track from {echodata_path}: {exc}")
    return lat, lon


def _label(freq) -> str:
    if freq is None or not math.isfinite(freq):
        return ""
    return f"{freq / 1000:g} kHz"


def build(ds, var: str, nature: str, path, *, tile: int = 256, lag: int = 2,
          reduce: str = "mean", y: str = "auto", block_levels: int = 3,
          track=(None, None), product: dict | None = None):
    """Write the pack for ds[var] to *path*; returns the header."""
    import numpy as np

    from aalibrary.console import _flat, _tilepack as tp

    da = ds[var]
    tdim, rdim = _flat.time_dim(da), _flat.range_dim(da)
    if tdim is None or rdim is None:
        raise ValueError(f"{var} is not on (ping_time x range): {da.dims}")
    has_channel = "channel" in da.dims
    order = (("channel",) if has_channel else ()) + (tdim, rdim)
    da = da.transpose(*order)
    n_pings = da.sizes[tdim]
    channels = list(range(da.sizes["channel"])) if has_channel else [None]

    dtype = "uint16" if nature in ("db", "mask") else "float32"
    quant = tp.DB_QUANT if nature == "db" else tp.MASK_QUANT if nature == "mask" else None
    writer = tp.Writer(path, tile=tile, dtype=dtype, quant=quant)
    header_channels = []
    try:
        times = ds[tdim].values
        time_ms = (times.astype("datetime64[ns]").astype(np.int64) / 1e6).astype("<f8")
        axes = {"time": writer.blob(time_ms.tobytes()) + ["<f8"]}
        lat, lon = track
        if lat is not None and np.isfinite(lat).any():
            axes["latitude"] = writer.blob(np.asarray(lat, "<f8").tobytes()) + ["<f8"]
            axes["longitude"] = writer.blob(np.asarray(lon, "<f8").tobytes()) + ["<f8"]

        freqs = None
        if has_channel and "frequency_nominal" in ds:
            freqs = np.asarray(ds["frequency_nominal"].values, dtype=float).reshape(-1)

        for ch in channels:
            ax = YAxis(ds, var, ch, tdim, rdim, y)
            levels = tp.level_plan(n_pings, ax.count, tile, lag)
            index = [[[0, 0]] * (lv["tilesX"] * lv["tilesY"]) for lv in levels]
            top_block = min(len(levels) - 1, block_levels)
            width = tile * 2 ** top_block
            if nature == "db":
                hist = tp._Histogram(-180.0, 82.0)
            elif nature == "mask":
                hist = tp._Histogram(0.0, 1.0, bins=1000)
            else:
                hist = None
            vmin, vmax = math.inf, -math.inf
            carry = []
            for start in range(0, n_pings, width):
                sel = {tdim: slice(start, start + width)}
                if ch is not None:
                    sel["channel"] = ch
                block = np.asarray(da.isel(sel).values, dtype=np.float32)
                if nature == "mask":
                    block = np.where(np.isnan(block), np.nan, (block != 0).astype(np.float32))
                cells = ax.regrid(block, start)
                if hist is not None:
                    hist.add(cells)
                elif np.isfinite(cells).any():
                    vmin = min(vmin, float(np.nanmin(cells)))
                    vmax = max(vmax, float(np.nanmax(cells)))
                if cells.shape[0] < width:
                    cells = np.concatenate([cells, np.full((width - cells.shape[0], cells.shape[1]),
                                                           np.nan, np.float32)])
                work = (np.power(10.0, cells / 10.0, dtype=np.float64).astype(np.float32)
                        if nature == "db" else cells)
                for lv in levels[:top_block + 1]:
                    if lv["level"] > 0:
                        work = tp.halve(work, 0, reduce)
                        if lv["fy"] > levels[lv["level"] - 1]["fy"]:
                            work = tp.halve(work, 1, reduce)
                    shown = _to_db(work) if nature == "db" else work
                    writer.add_tiles(index[lv["level"]], lv,
                                     start // (tile * lv["fx"]), shown)
                carry.append(work)
            if len(levels) > top_block + 1:
                work = np.concatenate(carry, axis=0)
                carry = []
                for lv in levels[top_block + 1:]:
                    work = tp.halve(work, 0, reduce)
                    if lv["fy"] > levels[lv["level"] - 1]["fy"]:
                        work = tp.halve(work, 1, reduce)
                    shown = _to_db(work) if nature == "db" else work
                    writer.add_tiles(index[lv["level"]], lv, 0, shown)
            stats = hist.summary() if hist is not None else (
                {"min": vmin, "max": vmax} if math.isfinite(vmin) else {})
            freq = float(freqs[ch]) if freqs is not None and ch is not None else None
            cid = str(ds["channel"].values[ch]) if ch is not None and "channel" in ds.coords else var
            header_channels.append({
                "index": len(header_channels), "id": cid,
                "label": _label(freq) or (cid if ch is not None else var),
                "frequency": freq,
                "y": {"name": ax.name, "unit": ax.unit, "start": ax.start, "step": ax.step,
                      "count": ax.count},
                "levels": levels, "stats": stats, "index": index,
            })
        header = {
            "format": tp.FORMAT, "tile": tile, "dtype": dtype, "quant": quant,
            "shuffle": dtype == "uint16", "compression": "zlib", "reduce": reduce, "lag": lag,
            "variable": var, "nature": nature,
            "unit": "dB" if nature == "db" else str(ds[var].attrs.get("units", "")),
            "longName": str(ds[var].attrs.get("long_name", var)),
            "x": {"count": n_pings, "dim": tdim,
                  "start": str(times[0]) if n_pings else "", "end": str(times[-1]) if n_pings else ""},
            "axes": axes, "channels": header_channels, "product": product or {},
            "created": _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds"),
        }
        writer.finish(header)
        return header
    except BaseException:
        writer.abort()
        raise


def _to_db(linear):
    import numpy as np

    with np.errstate(divide="ignore", invalid="ignore"):
        out = 10.0 * np.log10(linear)
    out[np.isneginf(out)] = -999.0
    return out


def main() -> None:
    if len(sys.argv) == 1 and not stdio.stdin_is_piped():
        print_help()
        sys.exit(0)
    parser = _build_parser()
    if show_help(SPEC, HELP, parser):
        sys.exit(0)
    args = parser.parse_args()
    if not 32 <= args.tile <= 1024 or args.tile & (args.tile - 1):
        print("aa-tiles: --tile must be a power of two from 32 to 1024", file=sys.stderr)
        sys.exit(2)
    if not 0 <= args.lag <= 12:
        print("aa-tiles: --lag must be 0 to 12", file=sys.stderr)
        sys.exit(2)

    from aalibrary.console import _flat

    token = stdio.one_input(args.input_path, SPEC.name)
    probe = Run(SPEC, args)
    src = probe.input(token)
    try:
        head = _flat.open_flat(src.local)
        var, nature = resolve_var(head, args.var)
        has_depth = "depth" in head
        head.close()
    except (ValueError, OSError) as exc:
        print(f"aa-tiles: {exc}", file=sys.stderr)
        sys.exit(1)
    reduce = args.reduce if args.reduce != "auto" else ("max" if nature == "mask" else "mean")
    yaxis = args.y if args.y != "auto" else ("depth" if has_depth else "range")
    run = Run(SPEC, args, params={"var": var, "y": yaxis, "reduce": reduce})
    run.input(token)
    echodata = run.input(args.echodata, role="echodata") if args.echodata else None
    out = run.plan(ext=".tiles", explicit=args.output_path or None)
    if run.reusable(out):
        run.finish(out)
        return
    try:
        ds = _flat.open_flat(src.local, chunks={})
        tdim = _flat.time_dim(ds[var])
        track = _track(ds, tdim, echodata.local if echodata else None)
        product = {"name": src.name, "uri": src.uri,
                   "hash": ((src.prov or {}).get("product") or {}).get("hash", ""),
                   "kind": _flat.kind_of(src, "")}
        build(ds, var, nature, out.local, tile=args.tile, lag=args.lag, reduce=reduce,
              y=yaxis, block_levels=max(0, args.block_levels), track=track, product=product)
        ds.close()
    except ValueError as exc:
        run.discard(out)
        print(f"aa-tiles: {exc}", file=sys.stderr)
        sys.exit(1)
    except Exception as exc:  # noqa: BLE001
        run.discard(out)
        logger.exception(f"aa-tiles: {exc}")
        sys.exit(1)
    run.finish(out)


if __name__ == "__main__":
    main()
