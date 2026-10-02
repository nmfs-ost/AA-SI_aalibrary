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
from the unmodified data. NetCDF and Zarr products therefore also get a
small subgroup ``aa_seal`` (attribute ``product_hash``). Saving a dataset
with xarray (or echopype) copies the attributes but not the subgroup, so
provenance without a matching seal is recognised as copied and ignored:
the file is then treated like any file without provenance (identified by
its content). Known limit: editing a product *in place* (netCDF4 'a'
mode) keeps the seal.
"""

from __future__ import annotations

import datetime as _dt
import getpass
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
SEAL_GROUP = "aa_seal"
SEAL_ATTR = "product_hash"
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


def _read_netcdf(path: Path) -> tuple[dict | None, bool]:
    """(document, sealed)."""
    try:
        import netCDF4

        with netCDF4.Dataset(str(path), "r") as nc:
            if ATTR not in nc.ncattrs():
                return None, False
            doc = _loads(nc.getncattr(ATTR))
            seal = nc.groups.get(SEAL_GROUP)
            value = (seal.getncattr(SEAL_ATTR)
                     if seal is not None and SEAL_ATTR in seal.ncattrs() else None)
            return doc, value is not None and str(value) == _hash_of(doc)
    except ImportError:
        import h5py

        with h5py.File(str(path), "r") as h5:
            doc = _loads(h5.attrs.get(ATTR))
            value = h5[SEAL_GROUP].attrs.get(SEAL_ATTR) if SEAL_GROUP in h5 else None
            if isinstance(value, bytes):
                value = value.decode()
            return doc, value is not None and str(value) == _hash_of(doc)


def _zarr_attrs(path: Path) -> dict | None:
    for meta in (path / "zarr.json", path / ".zattrs"):
        if meta.is_file():
            data = json.loads(meta.read_text(encoding="utf-8"))
            return data.get("attributes", {}) if meta.name == "zarr.json" else data
    return None


def _read_zarr(path: Path) -> tuple[dict | None, bool]:
    attrs = _zarr_attrs(path)
    if not attrs or ATTR not in attrs:
        return None, False
    doc = _loads(attrs.get(ATTR))
    seal = _zarr_attrs(path / SEAL_GROUP) or {}
    return doc, seal.get(SEAL_ATTR) is not None and str(seal.get(SEAL_ATTR)) == _hash_of(doc)


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
        seal = nc.groups.get(SEAL_GROUP) or nc.createGroup(SEAL_GROUP)
        seal.setncattr(SEAL_ATTR, doc["product"]["hash"])


def write_zarr_group(group, doc: dict) -> None:
    """Embed ``doc`` in an open zarr group (local or fsspec-backed).

    Sets the flat aa_* attributes plus the full document and creates the
    ``aa_seal`` subgroup. Consolidating metadata is left to the caller.
    """
    attrs = flat_attrs(doc)
    attrs[ATTR] = doc  # Zarr attributes hold JSON natively
    group.attrs.update(attrs)
    try:
        seal = group[SEAL_GROUP]
    except KeyError:
        seal = group.create_group(SEAL_GROUP)
    seal.attrs[SEAL_ATTR] = doc["product"]["hash"]


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
    "SEAL_GROUP", "SEAL_ATTR", "inspect", "write_zarr_group", "write_sidecar",
    "now", "software_versions", "merge_pipelines", "sources_of", "build", "history_line",
    "flat_attrs",
    "compact", "sidecar_path", "read", "write", "png_text",
]
