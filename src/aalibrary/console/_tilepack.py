"""The echogram tile pack (``.tiles``): one file, every zoom level, every channel.

An echogram viewer has to show a whole survey leg (tens of thousands of pings
by thousands of samples) and zoom to single samples, without the data ever
being sent whole. The pack holds the data pre-reduced into a pyramid of
square tiles, so a viewer fetches only the tiles on screen at the level that
matches its zoom, and colours them itself (thresholds and colour maps change
instantly, and the cursor reads real values).

Layout (little-endian)::

    b"AATILES1"                 8 bytes, magic
    header length H             uint64
    header                      H bytes, UTF-8 JSON (below)
    data                        tile and axis blobs; offsets are from here

Each blob is zlib-compressed (RFC 1950: a browser inflates it with
``DecompressionStream("deflate")``). A tile is ``tile x tile`` values, rows =
range (top first), columns = pings (left first), padded past the data's
edge with "no data". uint16 tiles are byte-shuffled before compression (all
low bytes, then all high bytes): they compress about a third better.

Values: ``dtype`` "uint16" with ``quant`` {offset, scale}: q = 0 is no data,
otherwise value = offset + (q - 1) * scale. "float32": the values, NaN for no
data.

Levels: level l averages 2**l pings (``fx``) and ``fy`` = 2**max(0, l - lag)
samples into one cell. Range is reduced more slowly than pings because
echograms are far longer than they are deep: a survey-wide view still has
its depth structure. dB values are averaged in the linear domain (mean of
sv, as Echoview and echopype average), or the maximum is taken (``reduce``
"max": small strong targets stay visible); masks keep the fraction (mean) or
any (max) of their True samples.

Header (JSON)::

    {"format": "aa-tiles/1", "tile": 256, "dtype": "uint16",
     "quant": {"offset": -180, "scale": 0.004} | null, "shuffle": true,
     "reduce": "mean", "lag": 2, "unit": "dB", "variable": "Sv",
     "x": {"count": N},
     "axes": {"time": [off, len, "float64"], "latitude": [...], "longitude": [...]},
     "channels": [{"index": 0, "id": "...", "label": "38 kHz", "frequency": 38000.0,
                   "y": {"name": "depth", "unit": "m", "start": 0.0, "step": 0.192,
                         "count": M},
                   "levels": [{"level": 0, "fx": 1, "fy": 1, "width": N, "height": M,
                               "tilesX": .., "tilesY": ..}, ...],
                   "stats": {"min":.., "max":.., "p02":.., "p50":.., "p98":..},
                   "index": [[[off, len], ...] per level, row-major ty*tilesX+tx]}],
     ...}

A tile whose cells are all "no data" has length 0 and no blob.
"""

from __future__ import annotations

import json
import math
import struct
import tempfile
import zlib
from pathlib import Path

import numpy as np

MAGIC = b"AATILES1"
FORMAT = "aa-tiles/1"
DB_QUANT = {"offset": -180.0, "scale": 0.004}       # -180 .. +82 dB
MASK_QUANT = {"offset": 0.0, "scale": 1.0 / 65534.0}
EMPTY_DB = -999.0


def shuffle16(a: np.ndarray) -> bytes:
    b = np.ascontiguousarray(a, dtype="<u2").view(np.uint8).reshape(-1, 2)
    return np.concatenate([b[:, 0], b[:, 1]]).tobytes()


def unshuffle16(data: bytes, count: int) -> np.ndarray:
    raw = np.frombuffer(data, dtype=np.uint8)
    out = np.empty((count, 2), dtype=np.uint8)
    out[:, 0] = raw[:count]
    out[:, 1] = raw[count:2 * count]
    return out.reshape(-1).view("<u2")


def quantize(values: np.ndarray, quant: dict) -> np.ndarray:
    q = np.rint((values - quant["offset"]) / quant["scale"]) + 1
    q = np.clip(q, 1, 65535)
    q[~np.isfinite(values)] = 0
    return q.astype(np.uint16)


def dequantize(q: np.ndarray, quant: dict) -> np.ndarray:
    out = quant["offset"] + (q.astype(np.float64) - 1) * quant["scale"]
    out[q == 0] = np.nan
    return out


# --------------------------------------------------------------------------- #
# Reductions
# --------------------------------------------------------------------------- #
def _pad(a: np.ndarray, axis: int) -> np.ndarray:
    if a.shape[axis] % 2 == 0:
        return a
    pad = [(0, 0)] * a.ndim
    pad[axis] = (0, 1)
    return np.pad(a, pad, constant_values=np.nan)


def halve(a: np.ndarray, axis: int, how: str) -> np.ndarray:
    """Combine neighbouring pairs along *axis*, ignoring NaN (all NaN: NaN)."""
    a = _pad(a, axis)
    shape = list(a.shape)
    shape[axis] //= 2
    shape.insert(axis + 1, 2)
    pairs = a.reshape(shape)
    first = np.take(pairs, 0, axis=axis + 1)
    second = np.take(pairs, 1, axis=axis + 1)
    if how == "max":
        return np.fmax(first, second)
    with np.errstate(invalid="ignore"):
        total = np.nan_to_num(first) + np.nan_to_num(second)
        count = (~np.isnan(first)).astype(np.float32) + (~np.isnan(second))
        out = total / count
    out[count == 0] = np.nan
    return out.astype(np.float32)


# --------------------------------------------------------------------------- #
# Levels
# --------------------------------------------------------------------------- #
def level_plan(n: int, m: int, tile: int, lag: int) -> list[dict]:
    """Every level from full resolution until the pings fit one tile."""
    levels = []
    level = 0
    while True:
        fx = 2 ** level
        fy = 2 ** max(0, level - lag)
        width = math.ceil(n / fx)
        height = math.ceil(m / fy)
        levels.append({"level": level, "fx": fx, "fy": fy, "width": width, "height": height,
                       "tilesX": math.ceil(width / tile), "tilesY": math.ceil(height / tile)})
        if width <= tile or level >= 24:
            return levels
        level += 1


class _Histogram:
    """Percentiles of a channel's values without keeping them."""

    def __init__(self, lo: float, hi: float, bins: int = 4000):
        self.lo, self.hi, self.bins = lo, hi, bins
        self.counts = np.zeros(bins, dtype=np.int64)
        self.vmin, self.vmax = np.inf, -np.inf

    def add(self, values: np.ndarray) -> None:
        v = values[np.isfinite(values)]
        if v.size == 0:
            return
        self.vmin = min(self.vmin, float(v.min()))
        self.vmax = max(self.vmax, float(v.max()))
        idx = np.clip(((v - self.lo) / (self.hi - self.lo) * self.bins).astype(np.int64),
                      0, self.bins - 1)
        self.counts += np.bincount(idx, minlength=self.bins)

    def summary(self) -> dict:
        total = int(self.counts.sum())
        if total == 0:
            return {}
        cum = np.cumsum(self.counts)
        width = (self.hi - self.lo) / self.bins

        def pct(p: float) -> float:
            i = int(np.searchsorted(cum, p * total))
            return round(self.lo + (i + 0.5) * width, 3)

        return {"min": round(self.vmin, 3), "max": round(self.vmax, 3), "p02": pct(0.02),
                "p50": pct(0.5), "p98": pct(0.98), "count": total}


class Writer:
    """Collects tiles into a pack. Tiles go to a temporary file as they come,
    so memory holds one block at a time; :meth:`finish` writes the pack."""

    def __init__(self, path: Path, *, tile: int, dtype: str, quant: dict | None):
        self.path = Path(path)
        self.tile = tile
        self.dtype = dtype
        self.quant = quant
        self._tmp = tempfile.NamedTemporaryFile(dir=self.path.parent, prefix=".tiles-",
                                                delete=False)
        self._offset = 0
        self.header: dict = {}

    def blob(self, data: bytes) -> list[int]:
        packed = zlib.compress(data, 6)
        self._tmp.write(packed)
        start = self._offset
        self._offset += len(packed)
        return [start, len(packed)]

    def tile_bytes(self, values: np.ndarray) -> bytes | None:
        """values: (rows=range, cols=pings), already tile x tile. None if empty."""
        if self.dtype == "uint16":
            q = quantize(values, self.quant)
            if not q.any():
                return None
            return shuffle16(q)
        v = values.astype("<f4")
        if np.isnan(v).all():
            return None
        return v.tobytes()

    def add_tiles(self, index: list, level: dict, x0_tiles: int, cells: np.ndarray) -> None:
        """cells: (pings, range) for tile columns starting at x0_tiles; every
        tile row. Writes each tile and records it in ``index`` (row-major)."""
        t = self.tile
        n_cols = math.ceil(cells.shape[0] / t)
        for tx in range(n_cols):
            gx = x0_tiles + tx
            if gx >= level["tilesX"]:
                break
            part = cells[tx * t:(tx + 1) * t]
            for ty in range(level["tilesY"]):
                block = part[:, ty * t:(ty + 1) * t]
                full = np.full((t, t), np.nan, dtype=np.float32)
                full[:block.shape[1], :block.shape[0]] = block.T
                data = self.tile_bytes(full)
                slot = ty * level["tilesX"] + gx
                index[slot] = self.blob(data) if data is not None else [0, 0]

    def finish(self, header: dict) -> Path:
        self._tmp.flush()
        self._tmp.close()
        head = json.dumps(header, separators=(",", ":"), allow_nan=False).encode()
        with open(self.path, "wb") as out, open(self._tmp.name, "rb") as body:
            out.write(MAGIC)
            out.write(struct.pack("<Q", len(head)))
            out.write(head)
            while True:
                chunk = body.read(8 << 20)
                if not chunk:
                    break
                out.write(chunk)
        Path(self._tmp.name).unlink(missing_ok=True)
        return self.path

    def abort(self) -> None:
        try:
            self._tmp.close()
        finally:
            Path(self._tmp.name).unlink(missing_ok=True)


# --------------------------------------------------------------------------- #
# Reading (tests, tools)
# --------------------------------------------------------------------------- #
class Reader:
    """A pack, read from the file as it was when opened: the file stays open,
    so a pack written again at the same path (a refreshed cache copy) does not
    change what this reader returns. Safe to share between threads."""

    def __init__(self, path: str | Path):
        import threading

        self.path = Path(path)
        self._fh = open(self.path, "rb")
        self._lock = threading.Lock()
        try:
            if self._fh.read(8) != MAGIC:
                raise ValueError(f"{self.path} is not an aa tile pack")
            (length,) = struct.unpack("<Q", self._fh.read(8))
            self.header = json.loads(self._fh.read(length))
            self.data_start = 16 + length
        except Exception:
            self._fh.close()
            raise

    def close(self) -> None:
        self._fh.close()

    def __del__(self):
        fh = getattr(self, "_fh", None)
        if fh is not None:
            fh.close()

    def raw(self, ref: list[int]) -> bytes:
        """A blob as stored (zlib-compressed): what a server sends a viewer."""
        offset, length = int(ref[0]), int(ref[1])
        if length <= 0:
            return b""
        with self._lock:
            self._fh.seek(self.data_start + offset)
            return self._fh.read(length)

    def blob(self, ref: list[int]) -> bytes:
        return zlib.decompress(self.raw(ref))

    def tile_ref(self, channel: int, level: int, tx: int, ty: int) -> list[int] | None:
        """Where one tile is ([offset, length]; length 0: all no data), or None
        when there is no such tile."""
        channels = self.header["channels"]
        if not 0 <= channel < len(channels):
            return None
        levels = channels[channel]["levels"]
        if not 0 <= level < len(levels):
            return None
        lv = levels[level]
        if not (0 <= tx < lv["tilesX"] and 0 <= ty < lv["tilesY"]):
            return None
        return channels[channel]["index"][level][ty * lv["tilesX"] + tx]

    def tile(self, channel: int, level: int, tx: int, ty: int) -> np.ndarray:
        """Values of one tile, (range rows, ping columns), NaN for no data."""
        ch = self.header["channels"][channel]
        lv = ch["levels"][level]
        ref = ch["index"][level][ty * lv["tilesX"] + tx]
        t = self.header["tile"]
        if not ref or ref[1] == 0:
            return np.full((t, t), np.nan)
        raw = self.blob(ref)
        if self.header["dtype"] == "uint16":
            q = unshuffle16(raw, t * t) if self.header.get("shuffle") else \
                np.frombuffer(raw, dtype="<u2")
            return dequantize(q, self.header["quant"]).reshape(t, t)
        return np.frombuffer(raw, dtype="<f4").astype(np.float64).reshape(t, t)

    def axis(self, name: str) -> np.ndarray | None:
        ref = (self.header.get("axes") or {}).get(name)
        if not ref:
            return None
        return np.frombuffer(self.blob(ref), dtype=ref[2] if len(ref) > 2 else "<f8")


__all__ = ["MAGIC", "FORMAT", "DB_QUANT", "MASK_QUANT", "shuffle16", "unshuffle16",
           "quantize", "dequantize", "halve", "level_plan", "Writer", "Reader"]
