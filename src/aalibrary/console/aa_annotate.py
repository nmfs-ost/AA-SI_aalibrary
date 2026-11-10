#!/usr/bin/env python3
"""
aa-annotate

Lines and regions as Echoview files (.evl, .evr), and back.

    aa-annotate shapes.json            write the line (.evl) or regions (.evr)
                                       the JSON describes; prints the file
    aa-annotate seafloor.nc            write a detected bottom (aa-detect-
                                       seafloor) as a line file (.evl)
    aa-annotate FILE --json            print a line, regions or a detected
                                       bottom as JSON shapes (the Workbench
                                       draws these)

Line and region files are products: <base>_<name>_<hash8>.evl|.evr with
<file>.aa.json. A drawn file's hash is its content, so the same line is the
same product wherever it was drawn; the product it was drawn on is recorded.
They are what aa-evl, aa-evr and aa-integrate take.

The JSON shapes (times in ms since 1970, UTC; depths in metres)::

    {"type": "line", "name": "bottom", "points": [{"t": 1781427600000,
     "depth": 182.4, "status": 3}, ...]}
    {"type": "regions", "name": "regions", "regions": [{"id": 1,
     "name": "school 1", "class": "Hake", "kind": "analysis",
     "points": [{"t": ..., "depth": ...}, ...]}]}

status: 0 none, 1 unverified, 2 bad, 3 good. kind: analysis, bad (no data),
bad_empty (empty water), marker, fishtrack.
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
import hashlib  # noqa: E402
import json  # noqa: E402
import re  # noqa: E402
from pathlib import Path  # noqa: E402

from aalibrary.console._core import (  # noqa: E402
    Help, Run, ToolSpec, add_common_flags, canon, identity, naming, render, show_help,
    stdio, uris,
)

SPEC = ToolSpec(
    name="aa-annotate",
    role="transform",
    kind="lines",
    op="aa_annotate.write",
    op_version=1,
    engines=(),
    params={"name": canon.text, "tolerance": canon.number, "max_points": canon.integer},
    ext=".evl",
)

HELP = Help(
    summary="Write lines and regions as Echoview .evl/.evr products; read them back as JSON.",
    does=(
        "From a JSON description (the Workbench's drawing, or your own) writes an "
        "Echoview line file (.evl: time, depth, status per point) or region file (.evr: "
        "polygons with a class and a type: analysis, bad data, marker, ...). From a "
        "detected bottom (aa-detect-seafloor's 'seafloor' per ping) writes the bottom "
        "as a line, every ping (or thinned with --tolerance).\n\n"
        "--json does the reverse: prints the line, regions or detected bottom as JSON "
        "shapes, thinned to --max-points so a long line can be drawn."
    ),
    stdin="One .json, .evl, .evr or seafloor .nc path or gs:// URI.",
    stdout="The written file's path (or gs:// URI); with --json, one line of JSON.",
    options=[
        ("--json", "print the input as JSON shapes instead of writing"),
        ("--name NAME", "name part of the file (default: the JSON's name, else bottom/regions)"),
        ("--reference PRODUCT", "the product the shapes were drawn on (recorded; gives "
                                "the base name)"),
        ("--tolerance M", "thin a detected bottom: keep the line within M metres "
                          "(default 0: every ping)"),
        ("--max-points N", "most points in --json output (default 4000)"),
        ("-o, --output_path PATH", "exact output; local or gs://"),
    ],
    science={
        "name": "Part of the name.",
        "tolerance": "Thinning tolerance for a detected bottom, metres.",
        "max_points": "Most points kept for a detected bottom.",
    },
    files=(
        "Reads local or gs://. Writes <base>_<name>_<hash8>.evl|.evr beside the input "
        "(current directory for gs:// input), or -o, or --dest, with <file>.aa.json. "
        "An identical file already there is reused."
    ),
    pipeline="aa-detect-seafloor ... | aa-annotate --name bottom  ->  a bottom.evl for "
             "aa-evl --evl / aa-integrate --bottom.",
    examples=[
        "aa-annotate shapes.json --reference sv.nc --dest gs://bucket/prefix/",
        "aa-annotate seafloor.nc --name bottom",
        "aa-annotate regions.evr --json",
    ],
    hash_note="The hash is the content written (points, classes, types), not how it was given.",
)


def print_help():
    sys.stdout.write(render(SPEC, HELP, _build_parser()))


def _build_parser():
    p = argparse.ArgumentParser(prog="aa-annotate", add_help=False,
                                description="Echoview lines and regions.")
    p.add_argument("input_path", type=str, nargs="?")
    p.add_argument("-o", "--output_path", type=str, default=None)
    p.add_argument("--json", action="store_true")
    p.add_argument("--name", type=str, default=None)
    p.add_argument("--reference", type=str, default=None, metavar="PRODUCT")
    p.add_argument("--tolerance", type=float, default=0.0, metavar="M")
    p.add_argument("--max-points", dest="max_points", type=int, default=4000, metavar="N")
    p.add_argument("--quiet", action="store_true")
    add_common_flags(p)
    return p


_NAME = re.compile(r"[^A-Za-z0-9_-]+")


def clean_name(text: str) -> str:
    return _NAME.sub("_", text.strip()).strip("_")[:40] or "shapes"


def _local(token: str) -> Path:
    if uris.is_gcs(token):
        return uris.localize(token).path
    return Path(uris.from_file_uri(token)).expanduser()


def seafloor_points(path: Path, var: str = "seafloor") -> list[dict]:
    """A detected bottom (one depth per ping) as line points."""
    import numpy as np

    from aalibrary.console import _flat

    ds = _flat.open_flat(path)
    try:
        name = var if var in ds.data_vars else next(
            (n for n, da in ds.data_vars.items() if da.ndim == 1), None)
        if name is None:
            raise ValueError("no bottom line (a 1-D 'seafloor' variable) in the file")
        da = ds[name]
        tdim = da.dims[0]
        t = ds[tdim].values.astype("datetime64[ns]").astype(np.int64) / 1e6
        d = np.asarray(da.values, dtype=float)
    finally:
        ds.close()
    return [{"t": float(a), "depth": float(b), "status": 3}
            for a, b in zip(t, d) if np.isfinite(a) and np.isfinite(b)]


def _recorded_name(path: Path) -> str:
    """The name a file was written with (its provenance), else its stem."""
    from aalibrary.console._core import provenance

    try:
        doc = provenance.read(path) or {}
    except Exception:  # noqa: BLE001
        doc = {}
    return str((doc.get("extra") or {}).get("name") or naming.stem_of(path.name))


def read_shapes(path: Path, *, max_points: int = 4000) -> dict:
    """Any supported file as JSON shapes."""
    from aalibrary.console import _echoview as ev

    suffix = path.suffix.lower()
    if suffix == ".evl":
        pts = ev.read_evl(path)
        thinned = ev.thin(pts, 0.0, max_points) if len(pts) > max_points else pts
        return {"type": "line", "name": _recorded_name(path), "points": thinned,
                "count": len(pts)}
    if suffix == ".evr":
        return {"type": "regions", "name": _recorded_name(path),
                "regions": ev.read_evr(path)}
    if suffix == ".json":
        return validate(json.loads(path.read_text(encoding="utf-8")))
    pts = seafloor_points(path)
    thinned = ev.thin(pts, 0.02, max_points) if len(pts) > max_points else pts
    return {"type": "line", "name": "bottom", "points": thinned, "count": len(pts),
            "detected": True}


def validate(doc: dict) -> dict:
    """The JSON shapes, checked and canonical (sorted points, numbers)."""
    import math

    from aalibrary.console import _echoview as ev

    if not isinstance(doc, dict) or doc.get("type") not in ("line", "regions"):
        raise ValueError('the JSON must be {"type": "line", ...} or {"type": "regions", ...}')

    def point(p, status: bool):
        try:
            t, d = float(p["t"]), float(p["depth"])
        except (KeyError, TypeError, ValueError):
            raise ValueError(f"a point needs t (ms) and depth (m): {p!r}") from None
        if not (math.isfinite(t) and math.isfinite(d)):
            raise ValueError("points must be finite numbers")
        out = {"t": round(t, 1), "depth": round(d, 6)}
        if status:
            s = int(p.get("status", 3))
            if s not in ev.LINE_STATUS:
                raise ValueError(f"line status must be 0-3, not {s}")
            out["status"] = s
        return out

    name = clean_name(str(doc.get("name") or ("bottom" if doc["type"] == "line" else "regions")))
    if doc["type"] == "line":
        pts = sorted((point(p, True) for p in doc.get("points") or []), key=lambda p: p["t"])
        if len(pts) < 2:
            raise ValueError("a line needs at least two points")
        return {"type": "line", "name": name, "points": pts}
    regions = []
    for i, r in enumerate(doc.get("regions") or [], start=1):
        kind = str(r.get("kind") or "analysis")
        if kind not in ev.KIND_CODES:
            raise ValueError(f"region kind must be one of {', '.join(ev.KIND_CODES)}")
        pts = [point(p, False) for p in r.get("points") or []]
        if len(pts) < 3:
            raise ValueError(f"region {r.get('name') or i} needs at least three points")
        regions.append({"id": int(r.get("id") or i), "name": " ".join(str(r.get("name") or f"Region{i}").split()),
                        "class": " ".join(str(r.get("class") or "").split()), "kind": kind,
                        "notes": [" ".join(str(n).split()) for n in r.get("notes") or []][:20],
                        "points": pts})
    if not regions:
        raise ValueError("no regions")
    ids = [r["id"] for r in regions]
    if len(set(ids)) != len(ids):
        raise ValueError("region ids must be unique")
    return {"type": "regions", "name": name, "regions": regions}


def main() -> None:
    if len(sys.argv) == 1 and not stdio.stdin_is_piped():
        print_help()
        sys.exit(0)
    parser = _build_parser()
    if show_help(SPEC, HELP, parser):
        sys.exit(0)
    args = parser.parse_args()

    from aalibrary.console import _echoview as ev

    token = stdio.one_input(args.input_path, SPEC.name)
    try:
        local = _local(token)
    except Exception as exc:  # noqa: BLE001
        print(f"aa-annotate: cannot read {token}: {exc}", file=sys.stderr)
        sys.exit(1)
    suffix = local.suffix.lower()

    if args.json:
        try:
            doc = read_shapes(local, max_points=max(2, args.max_points))
        except Exception as exc:  # noqa: BLE001
            print(f"aa-annotate: {type(exc).__name__}: {exc}", file=sys.stderr)
            sys.exit(1)
        doc["source"] = token
        print(json.dumps(doc, separators=(",", ":")))
        return

    detected = suffix not in (".json", ".evl", ".evr")
    try:
        if suffix == ".json":
            doc = validate(json.loads(local.read_text(encoding="utf-8")))
        elif suffix in (".evl", ".evr"):
            doc = validate(read_shapes(local, max_points=10**9))
        else:
            pts = seafloor_points(local)
            if args.tolerance > 0:
                pts = ev.thin(pts, args.tolerance, 10**9)
            doc = validate({"type": "line", "name": args.name or "bottom", "points": pts})
    except Exception as exc:  # noqa: BLE001
        print(f"aa-annotate: {exc}", file=sys.stderr)
        sys.exit(2)
    if args.name:
        doc["name"] = clean_name(args.name)

    ext = ".evl" if doc["type"] == "line" else ".evr"
    kind = "lines" if doc["type"] == "line" else "regions"
    content = {k: v for k, v in doc.items() if k != "name"}
    digest = hashlib.sha256(canon.dumps(content).encode()).hexdigest()

    reference = None
    if args.reference:
        probe = Run(SPEC, None)
        reference = probe.input(args.reference, role="reference")
    params = {"name": doc["name"], "content": digest}
    if not detected:
        params.update({"tolerance": None, "max_points": None})
    else:
        params["max_points"] = None
    if detected:
        run = Run(SPEC, args, params=params)
        src = run.input(token)
        base = src.base
    else:
        base = (naming.sanitize_base(args.base) if args.base
                else reference.base if reference is not None
                else naming.base_of(local.name))
        run = Run(SPEC, args, params=params, base=base)
    recipe = identity.recipe_hash(run.recipe_doc())
    name = naming.derived_name(f"{run.base()}_{doc['name']}", recipe, ext)
    directory = None
    if not (args.dest or args.output_path):
        anchor = reference if reference is not None else (run.primary if detected else None)
        if anchor is not None and anchor.via == "local":
            directory = str(anchor.local.parent)
        elif anchor is None and not uris.is_gcs(token):
            directory = str(local.parent)
    out = run.plan(ext=ext, explicit=args.output_path or None, name=name, kind=kind,
                   directory=directory)
    if run.reusable(out):
        run.finish(out)
        return
    try:
        if doc["type"] == "line":
            ev.write_evl(out.local, doc["points"])
            check = ev.read_evl(out.local)
            if len(check) != len(doc["points"]):
                raise ValueError("the line file did not read back")
        else:
            ev.write_evr(out.local, doc["regions"])
            if len(ev.read_evr(out.local)) != len(doc["regions"]):
                raise ValueError("the region file did not read back")
    except Exception as exc:  # noqa: BLE001
        run.discard(out)
        print(f"aa-annotate: could not write: {exc}", file=sys.stderr)
        sys.exit(1)
    extra = {"shape": doc["type"], "name": doc["name"]}
    if reference is not None:
        extra["drawn_on"] = reference.uri
        extra["drawn_on_product"] = ((reference.prov or {}).get("product") or {}).get("hash", "")
    if doc["type"] == "line":
        extra["points"] = len(doc["points"])
    else:
        extra["regions"] = len(doc["regions"])
        extra["classes"] = sorted({r["class"] for r in doc["regions"] if r["class"]})
    run.finish(out, extra=extra)


if __name__ == "__main__":
    main()
