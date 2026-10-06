"""Echoview line (.evl) and region (.evr) files: read and write.

The formats, as Echoview writes them and echoregions reads them:

EVL ::

    EVBD 3 13.0.378.44817
    <point count>
    <CCYYMMDD> <HHmmSSssss> <depth m> <status>      one line per point

  status: 0 none, 1 unverified, 2 bad, 3 good. Times are UTC; ssss is 0.1 ms.

EVR (region structure version 13) ::

    EVRG 7 13.0.378.44817
    <region count>
    <blank line>
    13 <points> <id> 0 <creation type> -1 1 <left date> <left time> <top>
        <right date> <right time> <bottom>          one line: the bounding box
    <note line count>
    <note lines>
    <detection setting line count>
    <detection setting lines>
    <region class>
    <date> <time> <depth> ... <region type>          all points, one line
    <region name>

  region type: 0 bad (no data), 1 analysis, 2 marker, 3 fish tracks,
  4 bad (empty water). top is the shallowest depth, bottom the deepest.

Shapes are exchanged with the Workbench (and kept in tests) as plain JSON:
times in milliseconds since 1970 (UTC), depths in metres::

    {"type": "line", "name": "bottom", "points": [{"t": ms, "depth": m,
     "status": 3}, ...]}
    {"type": "regions", "name": "regions", "regions": [{"id": 1, "name": "...",
     "class": "Hake", "kind": "analysis", "notes": [...], "points": [...]}]}
"""

from __future__ import annotations

import datetime as _dt
import math
from pathlib import Path

EV_VERSION = "13.0.378.44817"
REGION_KINDS = {0: "bad", 1: "analysis", 2: "marker", 3: "fishtrack", 4: "bad_empty"}
KIND_CODES = {v: k for k, v in REGION_KINDS.items()}
LINE_STATUS = {0: "none", 1: "unverified", 2: "bad", 3: "good"}
SENTINEL = 9000.0           # |depth| >= this is Echoview's "no data" (-10000.99)

_EPOCH = _dt.datetime(1970, 1, 1, tzinfo=_dt.timezone.utc)


def ev_time(ms: float) -> tuple[str, str]:
    """ms since 1970 -> ('CCYYMMDD', 'HHmmSSssss')."""
    tenths = int(round(float(ms) * 10))          # 0.1 ms units
    seconds, rest = divmod(tenths, 10000)
    t = _EPOCH + _dt.timedelta(seconds=seconds)
    return t.strftime("%Y%m%d"), f"{t:%H%M%S}{rest:04d}"


def parse_ev_time(date: str, time: str) -> float:
    """('CCYYMMDD', 'HHmmSSssss') -> ms since 1970."""
    date, time = date.strip(), time.strip()
    if len(date) != 8 or not date.isdigit() or not time.isdigit() or len(time) < 6:
        raise ValueError(f"not an Echoview time: {date} {time}")
    base = _dt.datetime(int(date[:4]), int(date[4:6]), int(date[6:8]),
                        int(time[:2]), int(time[2:4]), int(time[4:6]), tzinfo=_dt.timezone.utc)
    frac = time[6:10].ljust(4, "0")
    return (base - _EPOCH).total_seconds() * 1000 + int(frac) / 10


def _depth(value: float) -> str:
    return f"{float(value):.6f}"


# --------------------------------------------------------------------------- #
# Lines
# --------------------------------------------------------------------------- #
def write_evl(path: str | Path, points: list[dict]) -> Path:
    path = Path(path)
    rows = sorted(points, key=lambda p: float(p["t"]))
    lines = [f"EVBD 3 {EV_VERSION}", str(len(rows))]
    for p in rows:
        d, t = ev_time(p["t"])
        status = int(p.get("status", 3))
        if status not in LINE_STATUS:
            raise ValueError(f"line point status must be 0-3, not {status}")
        lines.append(f"{d} {t}  {_depth(p['depth'])} {status}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def read_evl(path: str | Path) -> list[dict]:
    text = Path(path).read_text(encoding="utf-8-sig").splitlines()
    if not text or not text[0].startswith("EVBD"):
        raise ValueError(f"{path}: not an Echoview line file (no EVBD header)")
    n = int(text[1].strip())
    points = []
    for line in text[2:2 + n]:
        parts = line.split()
        if len(parts) < 3:
            continue
        depth = float(parts[2])
        status = int(parts[3]) if len(parts) > 3 else 3
        if abs(depth) >= SENTINEL:
            continue
        points.append({"t": parse_ev_time(parts[0], parts[1]), "depth": depth, "status": status})
    return points


# --------------------------------------------------------------------------- #
# Regions
# --------------------------------------------------------------------------- #
def write_evr(path: str | Path, regions: list[dict]) -> Path:
    path = Path(path)
    out = [f"EVRG 7 {EV_VERSION}", str(len(regions))]
    for i, region in enumerate(regions, start=1):
        pts = region.get("points") or []
        if len(pts) < 3:
            raise ValueError(f"region {region.get('name') or i} has fewer than three points")
        rid = int(region.get("id") or i)
        kind = region.get("kind", "analysis")
        code = KIND_CODES.get(kind)
        if code is None:
            raise ValueError(f"region kind must be one of {', '.join(KIND_CODES)}, not {kind!r}")
        ts = [float(p["t"]) for p in pts]
        ds = [float(p["depth"]) for p in pts]
        ld, lt = ev_time(min(ts))
        rd, rt = ev_time(max(ts))
        notes = [str(n).replace("\n", " ") for n in region.get("notes") or []]
        klass = _one_line(region.get("class") or "")
        name = _one_line(region.get("name") or f"Region{rid}")
        out.append("")
        out.append(f"13 {len(pts)} {rid} 0 2 -1 1 {ld} {lt}  {_depth(min(ds))} "
                   f"{rd} {rt}  {_depth(max(ds))}")
        out.append(str(len(notes)))
        out.extend(notes)
        out.append("0")
        out.append(klass)
        coords = " ".join(f"{d} {t} {_depth(p['depth'])}" for p in pts
                          for d, t in [ev_time(p["t"])])
        out.append(f"{coords} {code}")
        out.append(name)
    path.write_text("\n".join(out) + "\n", encoding="utf-8")
    return path


def _one_line(text: str) -> str:
    return " ".join(str(text).split())


def read_evr(path: str | Path) -> list[dict]:
    lines = Path(path).read_text(encoding="utf-8-sig").splitlines()
    if not lines or not lines[0].startswith("EVRG"):
        raise ValueError(f"{path}: not an Echoview region file (no EVRG header)")
    count = int(lines[1].strip())
    pos = 2
    regions = []
    for _ in range(count):
        while pos < len(lines) and not lines[pos].strip():
            pos += 1
        head = lines[pos].split()
        pos += 1
        rid = int(head[2])
        n_notes = int(lines[pos].strip())
        pos += 1
        notes = lines[pos:pos + n_notes]
        pos += n_notes
        n_det = int(lines[pos].strip())
        pos += 1 + n_det
        klass = lines[pos].strip()
        pos += 1
        coords = lines[pos].split()
        pos += 1
        name = lines[pos].strip() if pos < len(lines) else ""
        pos += 1
        code = int(coords[-1])
        coords = coords[:-1]
        pts = []
        for j in range(0, len(coords) - 2, 3):
            depth = float(coords[j + 2])
            pts.append({"t": parse_ev_time(coords[j], coords[j + 1]), "depth": depth})
        regions.append({"id": rid, "name": name, "class": klass,
                        "kind": REGION_KINDS.get(code, "analysis"), "notes": notes,
                        "points": pts})
    return regions


# --------------------------------------------------------------------------- #
# Thinning a long line (a detected bottom has one point per ping)
# --------------------------------------------------------------------------- #
def thin(points: list[dict], tolerance: float, max_points: int) -> list[dict]:
    """Ramer-Douglas-Peucker on (time, depth), keeping depth error <= tolerance
    (metres), raising the tolerance until at most max_points remain."""
    import numpy as np

    if len(points) <= 2:
        return list(points)
    t = np.array([p["t"] for p in points], dtype=float)
    d = np.array([p["depth"] for p in points], dtype=float)

    def rdp(tol: float) -> np.ndarray:
        keep = np.zeros(len(t), dtype=bool)
        keep[0] = keep[-1] = True
        stack = [(0, len(t) - 1)]
        while stack:
            a, b = stack.pop()
            if b - a < 2:
                continue
            span = t[b] - t[a]
            seg = np.arange(a + 1, b)
            if span <= 0:
                interp = np.full(len(seg), d[a])
            else:
                interp = d[a] + (d[b] - d[a]) * (t[seg] - t[a]) / span
            err = np.abs(d[seg] - interp)
            i = int(np.argmax(err))
            if err[i] > tol:
                k = seg[i]
                keep[k] = True
                stack.append((a, k))
                stack.append((k, b))
        return keep

    tol = max(tolerance, 1e-6)
    keep = rdp(tol)
    while keep.sum() > max_points:
        tol *= 1.6
        keep = rdp(tol)
    return [points[i] for i in np.where(keep)[0]]


def finite(points: list[dict]) -> list[dict]:
    return [p for p in points
            if math.isfinite(float(p["t"])) and math.isfinite(float(p["depth"]))]


__all__ = ["EV_VERSION", "REGION_KINDS", "KIND_CODES", "LINE_STATUS", "ev_time",
           "parse_ev_time", "write_evl", "read_evl", "write_evr", "read_evr", "thin", "finite"]
