"""File-level provenance: what made this file, embedded in the file.

Document shape (``schema: aa-provenance/1``)::

    {
      "schema":  "aa-provenance/1",
      "base":    "HB1603_EK60_20160703T060000-20160703T120000",
      "product": {"hash": "<64 hex>", "short": "…", "kind": "sv",
                  "role": "transform", "name": "<file name>", "variant": null,
                  "recipe": "<64 hex>"},   # the processing only; see identity
      "inputs":  [{"role": "source", "uri": "gs://.../x.nc", "id": "aa:<hash>",
                   "name": "x.nc"}],
      "sources": [{"id": "md5:...", "name": "D20160703-T060000.raw",
                   "origin": "s3://noaa-wcsd-pds/data/raw/..."}],   # the raw
                                               # files at the root of the chain
      "pipeline": [ <step>, <step>, ... ],     # oldest first, this step last
      "software": {"aalibrary": "...", "echopype": "...", "python": "..."},
      "created":  {"at": "2026-09-30T16:01:02Z", "user": "...", "host": "..."}
    }

    step = {"tool": "aa-sv", "tool_version": "...", "op": "calibrate.compute_Sv",
            "op_version": 1, "params": {...canonical...}, "engine": {...},
            "inputs": ["aa:<hash>"], "product": "<hash>", "variant": null,
            "recipe": "<hash>", "recipe_inputs": [{"role": ..., "recipe": ...}],
            "scientific": true}

``product.hash`` is the scientific identity; the same canonical step and
input identities that produced it are recorded in ``pipeline[-1]``, so
``aa-metadata --verify`` can recompute it. ``created`` and ``software`` are
context and never enter the hash (file bytes differ run to run anyway:
echopype stamps processing times).

Where it lives, by format (recognised by extension, or by the file's
signature when an explicit -o has none):
    .nc / .netcdf4   root-group attributes (works for EchoData groups too)
    .zarr            root attributes (re-consolidated when consolidated)
    .png             text chunks
    .html            <script type="application/json" id="aa-provenance">
                     right after <head>
    anything else    <file>.aa.json sidecar
Reading falls back to the sidecar for every format. When the document is
too large for an HDF5 attribute (long N:1 chains), the embedded copy is
trimmed and the complete one is written to the sidecar as well.

Flat attributes are written beside the JSON so tools that only look at
``aa_*`` attributes (aa-plot's Provenance panel) show something readable:
    aa_provenance, aa_product_hash, aa_base, aa_tool, aa_product_kind, history
(``aa_kind`` is left alone: the Workbench uses it for its layer names.)

The seal. xarray copies global attributes when a dataset is saved, so a
product modified in a notebook and saved as a new file would still claim
the original product hash, and the next tool would reuse results made
from the unmodified data. NetCDF and Zarr products therefore also carry a
seal: the product hash, and ``shape``, a fingerprint of every array's path
and shape in the product as written. Provenance whose seal is missing,
names another product, or whose shape no longer matches the file is
recognised as copied and ignored: the file is then treated like any file
without provenance (identified by its content).

Where the seal lives:
    EchoData     attribute ``aa_seal`` (JSON text) of its ``Provenance``
                 group. echopype names every group of an EchoData and
                 refuses to display one it does not know (KeyError), so an
                 EchoData gets no group of ours.
    other        a small subgroup ``aa_seal`` (attributes ``product_hash``,
                 ``shape``). Saving a dataset with xarray never copies a
                 subgroup.
echopype's own combine and DataTree copies carry every group and attribute
along, seal included; the shape is what tells those copies apart (a combine
has more pings, a subset fewer). Seals written before the shape existed
(an ``aa_seal`` group with ``product_hash`` only) are still honoured, and an
EchoData sealed that way is also refused when its Provenance lists a
different number of source files than its record.

Records and the other aa attributes are written as JSON text (Zarr stores
written before held them as JSON objects; both are read), so a product
opened with xarray or echopype can be saved to NetCDF unchanged.

Known limit: editing a product *in place* (netCDF4 'a' mode, or a Zarr
array) without changing any shape keeps the seal.
"""

from __future__ import annotations

import datetime as _dt
import getpass
import hashlib
import json
import os
import platform
import re
import socket
from pathlib import Path
from typing import Any

from . import canon

SCHEMA = "aa-provenance/1"
SIDECAR_SUFFIX = ".aa.json"
ATTR = "aa_provenance"
ATTR_HASH = "aa_product_hash"
ATTR_BASE = "aa_base"
ATTR_TOOL = "aa_tool"
ATTR_KIND = "aa_product_kind"
ATTR_RECIPE = "aa_recipe"
SEAL_GROUP = "aa_seal"            # the seal of a product that is not EchoData
SEAL_ATTR = "product_hash"
SEAL_SHAPE_ATTR = "shape"
SEAL_ECHODATA_GROUP = "Provenance"  # an EchoData's seal: an attribute of this group,
SEAL_ECHODATA_ATTR = "aa_seal"      # named so, holding {product_hash, shape} as JSON text
SHAPE_SCHEME = "s1"
# HDF5 compact attribute storage tops out near 64 KiB; stay well under it.
MAX_EMBED_BYTES = 48_000

_NETCDF = {".nc", ".netcdf4", ".nc4", ".h5", ".hdf5"}
_PNG = {".png"}
_HTML = {".html", ".htm"}
_HDF5_SIG = b"\x89HDF\r\n\x1a\n"
_PNG_SIG = b"\x89PNG\r\n\x1a\n"


def _format(path: Path) -> str | None:
    """'zarr' | 'netcdf' | 'png' | 'html' | None, by extension, then by signature."""
    suffix = path.suffix.lower()
    if path.is_dir():
        if suffix == ".zarr" or (path / ".zgroup").exists() or (path / "zarr.json").exists():
            return "zarr"
        return None
    if suffix in _NETCDF:
        return "netcdf"
    if suffix in _PNG:
        return "png"
    if suffix in _HTML:
        return "html"
    try:
        with open(path, "rb") as fh:
            sig = fh.read(8)
    except OSError:
        return None
    if sig == _HDF5_SIG:
        return "netcdf"
    if sig == _PNG_SIG:
        return "png"
    return None


# ---------------------------------------------------------------------------
# Building documents.
# ---------------------------------------------------------------------------

def now() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def software_versions(extra: tuple[str, ...] = ()) -> dict[str, str]:
    out = {"python": platform.python_version()}
    for name in ("aalibrary", "echopype", "xarray", "numpy", "pandas", "netCDF4", "zarr",
                 "flox", *extra):
        try:
            from importlib.metadata import version

            out[name] = version(name)
        except Exception:
            continue
    return out


def _who() -> dict[str, str]:
    info = {"at": now()}
    try:
        info["user"] = getpass.getuser()
    except Exception:
        pass
    try:
        info["host"] = socket.gethostname()
    except Exception:
        pass
    return info


def merge_pipelines(parents: list[dict | None]) -> list[dict]:
    """Union of the parents' pipelines, oldest first, identical steps once.

    N:1 steps (aa-combine) would otherwise repeat the same conversion step
    per input. Steps that differ only in their inputs/products (parallel
    runs of the same step) are grouped into one entry with ``count`` and
    lists of inputs/products. A step that consumes the product of an
    identical earlier step (aa-clean run twice in a row) is sequential and
    stays a separate entry.
    """
    entries: list[dict] = []
    by_key: dict[str, dict] = {}
    seen_products: set[str] = set()
    for prov in parents:
        for step in (prov or {}).get("pipeline", []) or []:
            prods = [p for p in _as_list(step.get("product")) if p]
            if prods and all(p in seen_products for p in prods):
                continue                      # the same step, reached twice
            key = canon.dumps({k: step.get(k) for k in
                               ("tool", "op", "op_version", "params", "engine",
                                "variant", "scientific")})
            entry = by_key.get(key)
            if entry is not None and not prods:
                continue
            earlier = {p for p in _as_list(entry.get("product")) if p} if entry else set()
            sequential = entry is not None and bool(
                set(_as_list(step.get("inputs"))) & (earlier | {f"aa:{p}" for p in earlier}))
            if entry is None or sequential:
                entry = dict(step)
                entries.append(entry)
                by_key[key] = entry
            else:
                all_prods = list(dict.fromkeys(
                    p for p in _as_list(entry.get("product")) + prods if p))
                entry["product"] = all_prods
                ins = _as_list(entry.get("inputs")) + _as_list(step.get("inputs"))
                entry["inputs"] = list(dict.fromkeys(i for i in ins if i))
                entry["count"] = len(all_prods)
            seen_products.update(prods)
    return entries


def _as_list(v: Any) -> list:
    if v is None:
        return []
    if isinstance(v, list):
        out = []
        for x in v:
            out.extend(_as_list(x))
        return out
    return [v]


def sources_of(inputs: list[dict], parents: list[dict | None]) -> list[dict]:
    """The raw files at the root of the chain, with their origin (NCEI/GCS).

    Carried forward from every parent, so any product can say which NCEI
    objects it came from. A direct input without a pipeline (a .raw) is a
    source itself.
    """
    out: dict[str, dict] = {}
    for inp, prov in zip(inputs, list(parents) + [None] * (len(inputs) - len(parents))):
        if prov and prov.get("pipeline"):
            for src in prov.get("sources") or []:
                if src.get("id"):
                    out.setdefault(src["id"], src)
        elif inp.get("role") in (None, "source") and inp.get("id"):
            rec = {"id": inp["id"], "name": inp.get("name")}
            if inp.get("origin"):
                rec["origin"] = inp["origin"]
            elif inp.get("uri") and not str(inp["uri"]).startswith("file://"):
                rec["origin"] = inp["uri"]
            out.setdefault(inp["id"], rec)
    return sorted(out.values(), key=lambda r: (str(r.get("name")), r["id"]))


def build(*, base: str, product_hash: str, kind: str, role: str, name: str,
          step: dict, inputs: list[dict], parents: list[dict | None],
          variant: str | None = None, extra: dict | None = None,
          engines: tuple[str, ...] = ()) -> dict:
    pipeline = merge_pipelines(parents) + [step]
    doc = {
        "schema": SCHEMA,
        "base": base,
        "product": {"hash": product_hash, "short": product_hash[:8], "kind": kind,
                    "role": role, "name": name, "variant": variant},
        "inputs": inputs,
        "sources": sources_of(inputs, parents),
        "pipeline": pipeline,
        "software": software_versions(engines),
        "created": _who(),
    }
    if extra:
        doc["extra"] = extra
    return doc


def history_line(step: dict) -> str:
    params = step.get("params") or {}
    flags = " ".join(f"--{k}={canon.dumps(v) if not isinstance(v, str) else v}"
                     for k, v in sorted(params.items()) if v is not None)
    return f"{now()} {step.get('tool')} {flags}".rstrip() + f"  [aa:{str(step.get('product'))[:8]}]"


def flat_attrs(doc: dict) -> dict[str, str]:
    attrs = {
        ATTR: compact(doc),
        ATTR_HASH: doc["product"]["hash"],
        ATTR_BASE: doc["base"],
        ATTR_TOOL: doc["pipeline"][-1]["tool"],
        ATTR_KIND: doc["product"].get("kind") or "",
    }
    if doc["product"].get("recipe"):
        attrs[ATTR_RECIPE] = doc["product"]["recipe"]
    return attrs


def compact(doc: dict) -> str:
    return json.dumps(doc, separators=(",", ":"), sort_keys=True, ensure_ascii=False)


def _size(doc: dict) -> int:
    return len(compact(doc).encode())


def _trimmed(doc: dict) -> dict:
    """A copy small enough to embed; the sidecar keeps the complete one.

    Drops, in order until it fits: earlier pipeline steps, the list of
    root sources, then per-input details (keeping role and id, which is
    all --verify needs).
    """
    small = dict(doc)
    small["pipeline"] = doc["pipeline"][-1:]
    small["pipeline_truncated"] = len(doc["pipeline"]) - 1
    if _size(small) > MAX_EMBED_BYTES and small.get("sources"):
        small["sources_truncated"] = len(small["sources"])
        small["sources"] = []
    if _size(small) > MAX_EMBED_BYTES:
        small["inputs"] = [{"role": i.get("role"), "id": i.get("id")} for i in doc["inputs"]]
        step = dict(small["pipeline"][-1])
        step["inputs"] = f"{len(doc['inputs'])} inputs (see the sidecar)"
        small["pipeline"] = [step]
        small["inputs_truncated"] = True
    if _size(small) > MAX_EMBED_BYTES:
        small["inputs"] = []
    return small


def _is_trimmed(doc: dict) -> bool:
    return bool(doc.get("pipeline_truncated") or doc.get("inputs_truncated")
                or doc.get("sources_truncated"))


# ---------------------------------------------------------------------------
# Reading.
# ---------------------------------------------------------------------------

def sidecar_path(path: Path) -> Path:
    return path.with_name(path.name + SIDECAR_SUFFIX)


def read(path: str | Path) -> dict | None:
    """Provenance embedded in (or beside) a local file; None if there is none."""
    return inspect(path)[0]


def inspect(path: str | Path) -> tuple[dict | None, str]:
    """(document, status). status is one of:

    "embedded"   provenance inside the file (sealed, for NetCDF/Zarr)
    "sidecar"    from <file>.aa.json
    "copied"     aa_* attributes without a matching seal: copied into a
                 file saved by another program; ignored (document None)
    "stale"      a sidecar describing a different file (it was replaced);
                 ignored (document None)
    "none"       nothing
    """
    path = Path(path)
    doc, status = None, "none"
    try:
        fmt = _format(path)
        if fmt == "zarr":
            doc, sealed = _read_zarr(path)
        elif fmt == "netcdf":
            doc, sealed = _read_netcdf(path)
        elif fmt == "png":
            doc, sealed = _read_png(path), True
        elif fmt == "html":
            doc, sealed = _read_html(path), True
        else:
            sealed = True
        if doc is not None:
            status = "embedded" if sealed else "copied"
            if not sealed:
                doc = None
    except Exception:
        doc = None
    side = sidecar_path(path)
    if side.is_file():
        try:
            full = json.loads(side.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            full = None
        if full is not None and not _sidecar_matches(path, side, full):
            if doc is None:
                status = "stale"
            full = None
        if full is not None:
            if doc is None:
                doc, status = full, "sidecar"
            elif _is_trimmed(doc) and (
                    full.get("product", {}).get("hash") == doc.get("product", {}).get("hash")):
                doc = full  # the embedded copy was trimmed; the sidecar is complete
    if doc is not None and doc.get("schema") != SCHEMA:
        return None, "none"
    return doc, status


def _sidecar_matches(path: Path, side: Path, doc: dict) -> bool:
    """A sidecar written for a file that has since been replaced doesn't apply.

    Sidecar-only products record the file's size and MD5 (``file``); a
    different size, or a newer file whose MD5 differs, means stale.
    Sidecars without that record (source sidecars are checked by the
    tools; trimmed-embed sidecars accompany the embedded copy) pass.
    """
    rec = doc.get("file")
    if not isinstance(rec, dict) or not path.is_file():
        return True
    try:
        st = path.stat()
        if int(rec.get("size", -1)) != st.st_size:
            return False
        if st.st_mtime_ns <= side.stat().st_mtime_ns + 2_000_000_000:
            return True
        from .identity import file_identity

        return file_identity(path) == rec.get("id")
    except (OSError, ValueError):
        return True


def _loads(value: Any) -> dict | None:
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    if isinstance(value, bytes):
        value = value.decode("utf-8", "replace")
    try:
        return json.loads(str(value))
    except ValueError:
        return None


def _hash_of(doc: dict | None) -> str | None:
    return ((doc or {}).get("product") or {}).get("hash")


def attr_json(value: Any) -> Any:
    """An attribute that holds JSON: parsed from text (how aa attributes are
    written, and how NetCDF always held them), or as stored (a JSON object,
    in Zarr stores written before catch-up 16). None if it doesn't parse."""
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8", "replace")
    if isinstance(value, str):
        try:
            return json.loads(value)
        except ValueError:
            return None
    return value


# ---------------------------------------------------------------------------
# The seal (see the module docstring).
# ---------------------------------------------------------------------------

def _is_seal_path(path: str) -> bool:
    return str(path).strip("/").split("/", 1)[0] == SEAL_GROUP


def shape_print(shapes: dict[str, Any]) -> str:
    """The seal's fingerprint of a product: every array's path and shape
    (the seal group itself excluded), hashed. Values are not looked at."""
    lines = sorted(
        f"{str(path).strip('/')}={'x'.join(str(int(n)) for n in (shape or ()))}"
        for path, shape in shapes.items() if not _is_seal_path(path))
    digest = hashlib.sha256("\n".join(lines).encode("utf-8")).hexdigest()[:16]
    return f"{SHAPE_SCHEME}:{digest}"


def _seal_record(attrs: dict | None, *, echodata: bool) -> dict | None:
    """{"hash", "shape"} from the attributes the seal is kept in (the
    Provenance group's for an EchoData, the aa_seal group's otherwise); shape
    is None for a seal written before it existed."""
    if not attrs:
        return None
    if echodata:
        value = attr_json(attrs.get(SEAL_ECHODATA_ATTR))
        if isinstance(value, dict) and value.get(SEAL_ATTR):
            return {"hash": str(value[SEAL_ATTR]), "shape": value.get(SEAL_SHAPE_ATTR)}
        return None
    value = attrs.get(SEAL_ATTR)
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8", "replace")
    if not value:
        return None
    shape = attrs.get(SEAL_SHAPE_ATTR)
    if isinstance(shape, (bytes, bytearray)):
        shape = shape.decode("utf-8", "replace")
    return {"hash": str(value), "shape": shape or None}


def _source_count(doc: dict) -> int | None:
    """How many raw files the record says the product came from."""
    if doc.get("sources_truncated"):
        try:
            return int(doc["sources_truncated"])
        except (TypeError, ValueError):
            return None
    return len(doc.get("sources") or []) or None


def _judge(doc: dict | None, seal: dict | None, *, shapes, walk, echodata_sources,
           name: str | None = None) -> bool:
    """Whether ``doc`` is the product's own record.

    shapes():  the product's array shapes from its consolidated metadata (one
               read already made), or None when there is none.
    walk():    the same, from the arrays themselves; None if unreadable.
    echodata_sources(): the length of Provenance/source_filenames, or None
               (not an EchoData, or unreadable).
    name:      the file or store's own name, where it lies.
    """
    product = _hash_of(doc)
    if not product or seal is None or seal["hash"] != product:
        return False
    if seal.get("shape"):
        listed = shapes()
        if listed is not None and shape_print(listed) == seal["shape"]:
            return True
        walked = walk()
        if walked is None:
            # Nothing to compare with but unreadable metadata: as before shapes.
            return listed is None
        return shape_print(walked) == seal["shape"]
    # Sealed before shapes were recorded, so a copy that carried the seal
    # along (echopype's combine, a DataTree saved elsewhere) has only these to
    # give it away: a record naming another file or store, and for an
    # EchoData combine, more source files in its Provenance than the record has.
    recorded_name = str(((doc or {}).get("product") or {}).get("name") or "")
    if name and recorded_name and recorded_name != str(name).rstrip("/"):
        return False
    files, recorded = echodata_sources(), _source_count(doc)
    return files is None or recorded is None or files == recorded


def _name_of(where: str | Path) -> str:
    return str(where).rstrip("/").rsplit("/", 1)[-1]


def _read_netcdf(path: Path) -> tuple[dict | None, bool]:
    """(document, sealed)."""
    try:
        import netCDF4
    except ImportError:
        netCDF4 = None
    if netCDF4 is not None:
        with netCDF4.Dataset(str(path), "r") as nc:
            if ATTR not in nc.ncattrs():
                return None, False
            doc = _loads(nc.getncattr(ATTR))
            seal = _nc_seal(nc)
            shapes = _nc_shapes(nc)
            prov = nc.groups.get(SEAL_ECHODATA_GROUP)
            files = prov.variables.get("source_filenames") if prov is not None else None
            return doc, _judge(doc, seal, shapes=lambda: shapes, walk=lambda: shapes,
                               echodata_sources=lambda: (files.shape[0] if files is not None
                                                         and files.ndim == 1 else None),
                               name=path.name)
    import h5py

    # Without netCDF4 the shapes are not compared (HDF5 lists netCDF
    # dimensions as datasets of its own); the hash still must match.
    with h5py.File(str(path), "r") as h5:
        doc = _loads(h5.attrs.get(ATTR))
        seal = None
        if SEAL_ECHODATA_GROUP in h5:
            seal = _seal_record(dict(h5[SEAL_ECHODATA_GROUP].attrs), echodata=True)
        if seal is None and SEAL_GROUP in h5:
            seal = _seal_record(dict(h5[SEAL_GROUP].attrs), echodata=False)
        if seal is not None:
            seal["shape"] = None
        return doc, _judge(doc, seal, shapes=lambda: None, walk=lambda: None,
                           echodata_sources=lambda: None, name=path.name)


def _nc_seal(nc) -> dict | None:
    prov = nc.groups.get(SEAL_ECHODATA_GROUP)
    if prov is not None and SEAL_ECHODATA_ATTR in prov.ncattrs():
        seal = _seal_record({SEAL_ECHODATA_ATTR: prov.getncattr(SEAL_ECHODATA_ATTR)},
                            echodata=True)
        if seal is not None:
            return seal
    group = nc.groups.get(SEAL_GROUP)
    if group is None:
        return None
    return _seal_record({k: group.getncattr(k) for k in group.ncattrs()}, echodata=False)


def _nc_shapes(nc) -> dict[str, tuple]:
    """Every variable's path and shape in an open netCDF4 file."""
    out: dict[str, tuple] = {}

    def visit(group, prefix: str) -> None:
        for name, var in group.variables.items():
            out[f"{prefix}{name}"] = tuple(int(n) for n in var.shape)
        for name, sub in group.groups.items():
            if not prefix and name == SEAL_GROUP:
                continue
            visit(sub, f"{prefix}{name}/")

    visit(nc, "")
    return out


def _nc_is_echodata(nc) -> bool:
    return SEAL_ECHODATA_GROUP in nc.groups and "Sonar" in nc.groups


class _StoreMeta:
    """A Zarr store's metadata documents, read on demand (local or fsspec)."""

    def __init__(self, read_json) -> None:
        self._read = read_json
        self._docs: dict[str, Any] = {}
        self.v3 = self.get("zarr.json") is not None

    def get(self, rel: str) -> dict | None:
        if rel not in self._docs:
            try:
                doc = self._read(rel)
            except (OSError, ValueError):
                doc = None
            self._docs[rel] = doc if isinstance(doc, dict) else None
        return self._docs[rel]

    def attrs(self, group: str = "") -> dict | None:
        prefix = f"{group}/" if group else ""
        if self.v3:
            doc = self.get(f"{prefix}zarr.json")
            return None if doc is None else (doc.get("attributes") or {})
        return self.get(f"{prefix}.zattrs")

    def consolidated_shapes(self) -> dict | None:
        if self.v3:
            meta = ((self.get("zarr.json") or {}).get("consolidated_metadata") or {}).get(
                "metadata")
            if not isinstance(meta, dict):
                return None
            return {path: tuple(node.get("shape") or ()) for path, node in meta.items()
                    if isinstance(node, dict) and node.get("node_type") == "array"}
        meta = (self.get(".zmetadata") or {}).get("metadata")
        if not isinstance(meta, dict):
            return None
        return {key[:-len(".zarray")].rstrip("/"): tuple(node.get("shape") or ())
                for key, node in meta.items()
                if key.endswith(".zarray") and isinstance(node, dict)}

    def array_shape(self, path: str) -> tuple | None:
        listed = self.consolidated_shapes()
        if listed is not None:
            return listed.get(path)
        doc = self.get(f"{path}/zarr.json" if self.v3 else f"{path}/.zarray")
        if not doc or (self.v3 and doc.get("node_type") != "array"):
            return None
        return tuple(doc.get("shape") or ())

    def seal(self) -> dict | None:
        """The seal: the Provenance group's attribute first (an EchoData),
        then the aa_seal group (any product, and EchoData sealed before)."""
        seal = _seal_record(self.attrs(SEAL_ECHODATA_GROUP), echodata=True)
        return seal if seal is not None else _seal_record(self.attrs(SEAL_GROUP),
                                                          echodata=False)

    def echodata_sources(self) -> int | None:
        shape = self.array_shape(f"{SEAL_ECHODATA_GROUP}/source_filenames")
        return int(shape[0]) if shape and len(shape) == 1 else None


def group_shapes(group) -> dict[str, tuple]:
    """Every array's path and shape under an open zarr group (zarr 3 or 2),
    the aa_seal group excluded."""
    import zarr

    out: dict[str, tuple] = {}
    if hasattr(group, "members"):          # zarr 3
        for name, node in group.members(max_depth=None):
            if isinstance(node, zarr.Array) and not _is_seal_path(name):
                out[name] = tuple(int(n) for n in node.shape)
        return out

    def visit(name, node):                 # zarr 2
        if isinstance(node, zarr.Array) and not _is_seal_path(name):
            out[name] = tuple(int(n) for n in node.shape)

    group.visititems(visit)
    return out


def _fresh_group(group):
    """The same store and group opened again without its consolidated
    metadata, so a walk sees the arrays as they are, not as last listed."""
    import zarr

    try:
        return zarr.open_group(store=group.store, path=group.path or "", mode="r",
                               use_consolidated=False)
    except TypeError:                      # zarr 2
        return zarr.open_group(store=group.store, path=group.path or "", mode="r")


def _walk_shapes(where: str | Path, storage_options: dict | None = None) -> dict | None:
    """Array shapes read from the arrays themselves; None if unreadable."""
    import zarr

    try:
        text = str(where)
        if "://" in text:
            if text.startswith(("gs://", "gcs://")) and os.getenv("AA_GCS_FAKE_ROOT"):
                from . import uris

                uris.install_fake_fsspec()   # the stand-in bucket, never GCS
            try:
                group = zarr.open_group(text, mode="r", use_consolidated=False,
                                        storage_options=storage_options or None)
            except TypeError:                # zarr 2
                import fsspec

                group = zarr.open_group(fsspec.get_mapper(text, **(storage_options or {})),
                                        mode="r")
        else:
            try:
                group = zarr.open_group(text, mode="r", use_consolidated=False)
            except TypeError:
                group = zarr.open_group(text, mode="r")
        return group_shapes(group)
    except Exception:  # noqa: BLE001 - unreadable is "unknown"
        return None


def _local_json(path: Path) -> dict | None:
    if not path.is_file():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _store_reader(where: str | Path, storage_options: dict | None):
    text = str(where).removeprefix("file://")
    if "://" in text:
        return text, lambda rel: _remote_json(text, rel, storage_options)
    return text, lambda rel: _local_json(Path(text) / rel)


def store_is_own(where: str | Path, doc: dict | None,
                 storage_options: dict | None = None) -> bool:
    """Whether ``doc`` (a record read from this Zarr store's root attributes)
    is the store's own: sealed for it, in its present shape. ``where`` is a
    local path, file://, or an fsspec URI (gs:// through gcsfs or the stand-in
    bucket). A few small metadata reads; the arrays only when the
    consolidated metadata disagrees or there is none."""
    location, reader = _store_reader(where, storage_options)
    meta = _StoreMeta(reader)
    return _judge(doc, meta.seal(), shapes=meta.consolidated_shapes,
                  walk=lambda: _walk_shapes(location, storage_options),
                  echodata_sources=meta.echodata_sources, name=_name_of(location))


def _read_zarr(path: Path) -> tuple[dict | None, bool]:
    meta = _StoreMeta(lambda rel: _local_json(path / rel))
    attrs = meta.attrs()
    if not attrs or ATTR not in attrs:
        return None, False
    doc = _loads(attrs.get(ATTR))
    return doc, _judge(doc, meta.seal(), shapes=meta.consolidated_shapes,
                       walk=lambda: _walk_shapes(path),
                       echodata_sources=meta.echodata_sources, name=path.name)


def _remote_json(where: str, name: str, storage_options: dict | None) -> dict | None:
    """``<where>/<name>`` parsed as JSON, through fsspec; None if absent."""
    import fsspec

    if str(where).startswith(("gs://", "gcs://")) and os.getenv("AA_GCS_FAKE_ROOT"):
        from . import uris

        uris.install_fake_fsspec()   # the stand-in bucket, never GCS

    fs, root = fsspec.core.url_to_fs(where, **(storage_options or {}))
    key = f"{root.rstrip('/')}/{name}"
    try:
        with fs.open(key, "rb") as handle:
            return json.loads(handle.read().decode("utf-8"))
    except FileNotFoundError:
        return None


def inspect_store(where: str | Path,
                  storage_options: dict | None = None) -> tuple[dict | None, str]:
    """(document, status) for a Zarr store read where it is: a local path or a
    gs:// URI (through fsspec: gcsfs, or the stand-in bucket). Only the root
    metadata and the seal are read, a few kilobytes, never the data (the
    arrays' own metadata only when the consolidated listing disagrees with
    the seal's shape).

    status: "embedded" (sealed), "copied" (aa attributes without a matching
    seal: not this store's own), or "none".
    """
    if "://" not in str(where):
        doc, sealed = _read_zarr(Path(where))
    else:
        location, reader = _store_reader(where, storage_options)
        meta = _StoreMeta(reader)
        attrs = meta.attrs()
        if not attrs or ATTR not in attrs:
            return None, "none"
        doc = _loads(attrs.get(ATTR))
        sealed = _judge(doc, meta.seal(), shapes=meta.consolidated_shapes,
                        walk=lambda: _walk_shapes(location, storage_options),
                        echodata_sources=meta.echodata_sources, name=_name_of(location))
    if doc is None:
        return None, "none"
    if not sealed:
        return None, "copied"
    if doc.get("schema") != SCHEMA:
        return None, "none"
    return doc, "embedded"


def _read_png(path: Path) -> dict | None:
    from PIL import Image

    with Image.open(path) as img:
        text = getattr(img, "text", {}) or img.info
        return _loads(text.get(ATTR))


_HTML_RE = re.compile(
    r'<script type="application/json" id="aa-provenance">(.*?)</script>', re.S)
_HTML_META_RE = re.compile(r'<meta name="aa-product-hash" content="[0-9a-f]*">')
_HTML_HEAD_RE = re.compile(r"<head(?:\s[^>]*)?>", re.I)
_HTML_CHARSET_RE = re.compile(r"<meta\b[^>]*charset[^>]*>", re.I)
_HTML_DOCTYPE_RE = re.compile(r"\s*<!doctype[^>]*>", re.I)


def _read_html(path: Path) -> dict | None:
    # Written right after <head>, so the first 256 KB normally hold it;
    # older pages had it at the end of a multi-MB head: read on if needed.
    with open(path, encoding="utf-8", errors="replace") as fh:
        text = fh.read(256_000)
        m = _HTML_RE.search(text)
        if m is None:
            text += fh.read()
            m = _HTML_RE.search(text)
    return _loads(m.group(1).replace("<\\/", "</")) if m else None


# ---------------------------------------------------------------------------
# Writing.
# ---------------------------------------------------------------------------

def write(path: str | Path, doc: dict) -> str:
    """Embed ``doc`` in the file at ``path``. Returns how it was stored.

    Writes a sidecar as well when the embedded copy had to be trimmed, and
    always for formats that can't carry it.
    """
    path = Path(path)
    fmt = _format(path)
    # Zarr attributes are plain JSON files: no size limit, never trimmed.
    too_big = fmt != "zarr" and _size(doc) > MAX_EMBED_BYTES
    embed_doc = _trimmed(doc) if too_big else doc
    need_sidecar = embed_doc is not doc
    how = "sidecar"
    try:
        if fmt == "zarr":
            _write_zarr(path, embed_doc)
            how = "zarr-attrs"
        elif fmt == "netcdf":
            _write_netcdf(path, embed_doc)
            how = "netcdf-attrs"
        elif fmt == "png":
            _write_png(path, embed_doc)
            how = "png-text"
        elif fmt == "html":
            _write_html(path, embed_doc)
            how = "html"
        else:
            need_sidecar = True
    except Exception:
        need_sidecar = True
        how = "sidecar"
    if need_sidecar:
        side_doc = doc
        if how == "sidecar" and path.is_file():
            # Sidecar-only: remember which file it describes (see _sidecar_matches).
            from .identity import file_identity

            side_doc = dict(doc, file={"size": path.stat().st_size, "id": file_identity(path)})
        sidecar_path(path).write_text(json.dumps(side_doc, indent=2, sort_keys=True),
                                      encoding="utf-8")
        if how != "sidecar":
            how += "+sidecar"
    else:
        # A sidecar left from an earlier product at this path would be stale.
        old = sidecar_path(path)
        if old.is_file():
            try:
                old.unlink()
            except OSError:
                pass
    return how


def write_sidecar(path: str | Path, doc: dict) -> Path:
    """Write the complete document to ``<file>.aa.json`` (in addition to any
    embedded copy). Used for bucket objects so the record can be read without
    downloading the product."""
    side = sidecar_path(Path(path))
    side.write_text(json.dumps(doc, indent=2, sort_keys=True), encoding="utf-8")
    return side


def _write_netcdf(path: Path, doc: dict) -> None:
    import netCDF4

    attrs = flat_attrs(doc)
    with netCDF4.Dataset(str(path), "a") as nc:
        for k, v in attrs.items():
            nc.setncattr(k, v)
        line = history_line(doc["pipeline"][-1])
        old = nc.getncattr("history") if "history" in nc.ncattrs() else ""
        old = old if isinstance(old, str) else str(old)
        nc.setncattr("history", (old.rstrip("\n") + "\n" + line).lstrip("\n"))
        seal = {SEAL_ATTR: doc["product"]["hash"], SEAL_SHAPE_ATTR: shape_print(_nc_shapes(nc))}
        if _nc_is_echodata(nc):
            # An attribute of echopype's own Provenance group: a group of ours
            # would make echopype refuse to display the EchoData. A seal group
            # copied in from an input (netCDF4 cannot remove groups) is left
            # behind; the Provenance seal is the one read.
            nc.groups[SEAL_ECHODATA_GROUP].setncattr(SEAL_ECHODATA_ATTR, _seal_text(seal))
        else:
            group = nc.groups.get(SEAL_GROUP) or nc.createGroup(SEAL_GROUP)
            for key, value in seal.items():
                group.setncattr(key, value)


def _seal_text(seal: dict) -> str:
    return json.dumps(seal, separators=(",", ":"), sort_keys=True)


def _zarr_is_echodata(group) -> bool:
    try:
        return SEAL_ECHODATA_GROUP in group and "Sonar" in group
    except Exception:  # noqa: BLE001 - an unreadable member: not an EchoData we can seal there
        return False


def write_zarr_group(group, doc: dict) -> None:
    """Embed ``doc`` in an open zarr group (local or fsspec-backed).

    Sets the flat aa_* attributes (the full document as JSON text, which
    NetCDF can hold too) and the seal: an attribute of the Provenance group
    for an EchoData (any aa_seal group copied in from an input is removed),
    the ``aa_seal`` subgroup otherwise. The seal's shape is read from the
    arrays as written, so call this after the data. Consolidating metadata is
    left to the caller.
    """
    group.attrs.update(flat_attrs(doc))
    seal = {SEAL_ATTR: doc["product"]["hash"],
            SEAL_SHAPE_ATTR: shape_print(group_shapes(_fresh_group(group)))}
    if _zarr_is_echodata(group):
        group[SEAL_ECHODATA_GROUP].attrs[SEAL_ECHODATA_ATTR] = _seal_text(seal)
        try:
            if SEAL_GROUP in group:
                del group[SEAL_GROUP]
        except Exception as exc:  # noqa: BLE001 - the Provenance seal is the one read
            import logging

            logging.getLogger(__name__).debug(f"could not remove an old {SEAL_GROUP}: {exc}")
        return
    try:
        sealed = group[SEAL_GROUP]
    except KeyError:
        sealed = group.create_group(SEAL_GROUP)
    sealed.attrs.update(seal)


def drop_echodata_seal(echodata) -> bool:
    """Remove an aa_seal group from an open EchoData (one read from a store
    sealed before catch-up 16, or carried along by echopype's combine), so it
    is neither displayed (echopype refuses groups it doesn't know) nor written
    into a new product. True if there was one."""
    tree = getattr(echodata, "_tree", None)
    if tree is None or SEAL_GROUP not in getattr(tree, "children", {}):
        return False
    echodata._tree = tree.drop_nodes(SEAL_GROUP)
    return True


def _write_zarr(path: Path, doc: dict) -> None:
    import zarr

    group = zarr.open_group(str(path), mode="r+")
    write_zarr_group(group, doc)
    if (path / ".zmetadata").exists() or _v3_consolidated(path):
        try:
            zarr.consolidate_metadata(str(path))
        except Exception:
            pass


def _v3_consolidated(path: Path) -> bool:
    meta = path / "zarr.json"
    if not meta.is_file():
        return False
    try:
        return "consolidated_metadata" in json.loads(meta.read_text(encoding="utf-8"))
    except ValueError:
        return False


def png_text(doc: dict) -> dict[str, str]:
    """Text chunks for matplotlib ``savefig(metadata=...)`` or PIL."""
    return {
        "Software": f"aalibrary {doc.get('software', {}).get('aalibrary', '')} "
                    f"{doc['pipeline'][-1]['tool']}".strip(),
        "Source": doc.get("inputs", [{}])[0].get("uri", "") if doc.get("inputs") else "",
        ATTR: compact(doc),
        ATTR_HASH: doc["product"]["hash"],
        ATTR_BASE: doc["base"],
    }


def _write_png(path: Path, doc: dict) -> None:
    from PIL import Image
    from PIL.PngImagePlugin import PngInfo

    with Image.open(path) as img:
        img.load()
        info = PngInfo()
        for k, v in (getattr(img, "text", {}) or {}).items():
            if k not in (ATTR, ATTR_HASH, ATTR_BASE, "Software", "Source"):
                info.add_text(k, str(v))
        for k, v in png_text(doc).items():
            info.add_itxt(k, v) if k == ATTR else info.add_text(k, v)
        dpi = img.info.get("dpi")
        tmp = path.with_name(path.name + ".tmp.png")
        img.save(tmp, pnginfo=info, **({"dpi": dpi} if dpi else {}))
    os.replace(tmp, path)


def _write_html(path: Path, doc: dict) -> None:
    """Insert right after the opening <head> tag.

    Not before '</head>': bundled JavaScript (Bokeh/Panel) contains that
    string inside its own code, and inserting there cuts the script in two.
    The opening tag comes before any script, so its first match is the real
    one; being near the top also keeps reading cheap.
    """
    text = path.read_text(encoding="utf-8", errors="replace")
    text = _HTML_META_RE.sub("", _HTML_RE.sub("", text))
    blob = compact(doc).replace("</", "<\\/")
    tag = (f'<meta name="aa-product-hash" content="{doc["product"]["hash"]}">'
           f'<script type="application/json" id="aa-provenance">{blob}</script>')
    m = _HTML_HEAD_RE.search(text)
    if m is not None:
        pos = m.end()
        # Keep <meta charset> first: browsers look for it in the first 1024 bytes.
        cs = _HTML_CHARSET_RE.match(text, pos) or _HTML_CHARSET_RE.search(text, pos, pos + 1024)
        if cs is not None:
            pos = cs.end()
    else:
        dt = _HTML_DOCTYPE_RE.match(text)
        pos = dt.end() if dt else 0
    text = text[:pos] + tag + text[pos:]
    path.write_text(text, encoding="utf-8")


__all__ = [
    "SCHEMA", "SIDECAR_SUFFIX", "ATTR", "ATTR_HASH", "ATTR_BASE", "ATTR_TOOL", "ATTR_KIND",
    "ATTR_RECIPE",
    "SEAL_GROUP", "SEAL_ATTR", "SEAL_SHAPE_ATTR", "SEAL_ECHODATA_GROUP", "SEAL_ECHODATA_ATTR",
    "inspect", "inspect_store", "store_is_own", "write_zarr_group", "write_sidecar",
    "attr_json", "shape_print", "group_shapes", "drop_echodata_seal",
    "now", "software_versions", "merge_pipelines", "sources_of", "build", "history_line",
    "flat_attrs",
    "compact", "sidecar_path", "read", "write", "png_text",
]
