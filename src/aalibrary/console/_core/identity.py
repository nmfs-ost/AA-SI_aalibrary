"""Identities of inputs, the recipe hash and the product hash.

Two hashes, for two questions:

    recipe   WHAT WAS DONE: every scientific step from the EchoData on, with
             its canonical options and library versions, but not the data.
             The same processing applied to any raw file gives the same
             recipe. Its first 8 hex digits are the <hash8> in file names,
             so the base says which data and the hash says which processing.
    product  WHAT THIS FILE IS: the recipe applied to these particular
             inputs. Used to decide whether an existing file can be reused,
             and recorded inside every file.

Input identity (what a step consumed), cheapest first:

    aa:<hash>                 the input carries provenance: its product hash
                              (which already covers its whole upstream chain)
    md5:<hex>:<size>          a file without provenance (e.g. a .raw). MD5 on
                              purpose: GCS stores the same MD5 for an object,
                              so a file has ONE identity whether it is read
                              locally, through a gcsfuse mount, or from the
                              bucket (no download needed to identify it).
                              Local results are remembered by (path, size,
                              mtime) so each file is hashed once.
    tree:<hex>                a directory (e.g. a Zarr store) without
                              provenance: hash over its metadata files and
                              the names and sizes of everything in it

Product hash::

    sha256( canonical_json({
        "schema": "aa-product/1",
        "tool": "aa-sv", "op": "...", "op_version": 1,
        "params": {...canonical scientific params...},
        "engine": {"echopype": "0.11"},        # major.minor of science libraries
        "variant": null,                        # e.g. "apply" for a second output
        "inputs": [{"role": "source", "id": "aa:..."}, ...]
    }) )

Everything that can change the numbers is in there; nothing that can't
(paths, timestamps, output format options, verbosity) is.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

from . import canon

PRODUCT_SCHEMA = "aa-product/1"
RECIPE_SCHEMA = "aa-recipe/1"
DATA = "data"   # what a data input (a raw file, plain EchoData) adds to a recipe
_DATA_ROLES = ("source", "echodata")
_CHUNK = 8 * 1024 * 1024


def engine_versions(names: tuple[str, ...]) -> dict[str, str]:
    """Version of each science library that enters the hash.

    Major.minor for libraries at 1.0 or later (a patch release shouldn't
    bust caches); the full version below 1.0, where patch releases have
    changed results (echopype 0.x calibration fixes).
    """
    out = {}
    for name in names:
        try:
            from importlib.metadata import version

            v = version(name)
        except Exception:
            continue
        parts = v.split(".")
        if parts[0] == "0" or len(parts) < 2:
            out[name] = v
        else:
            out[name] = ".".join(parts[:2])
    return out


def identity_document(*, tool: str, op: str, op_version: int, params: dict,
                      engine: dict, inputs: list[dict], variant: str | None = None) -> dict:
    return {
        "schema": PRODUCT_SCHEMA,
        "tool": tool,
        "op": op,
        "op_version": int(op_version),
        "params": canon.normalize(params),
        "engine": dict(engine),
        "variant": variant,
        "inputs": [{"role": i.get("role", "source"), "id": i["id"]} for i in inputs],
    }


def product_hash(doc: dict) -> str:
    return hashlib.sha256(canon.dumps(doc).encode("utf-8")).hexdigest()


def input_recipe(role: str, prov: dict | None, ident: str) -> str:
    """What one input contributes to a recipe.

    A data input contributes the recipe that produced it ("r:<recipe>"), or
    just "data" when it is raw data (a .raw, EchoData without provenance):
    which file it is doesn't matter. A parameter file (EVR regions, EVL
    lines) is part of the query itself, so its content counts.
    """
    if role not in _DATA_ROLES:
        return ident
    product = (prov or {}).get("product") or {}
    if product.get("role") not in (None, "source") and product.get("recipe"):
        return "r:" + str(product["recipe"])
    return DATA


def recipe_inputs(pairs: list[tuple[str, str]]) -> list[dict]:
    """Canonical list of (role, recipe) for a recipe document.

    Data inputs form a set (combining 3 or 300 files converted the same way
    is the same recipe); parameter files keep their order.
    """
    data = sorted({(r, t) for r, t in pairs if r in _DATA_ROLES})
    params = [(r, t) for r, t in pairs if r not in _DATA_ROLES]
    return [{"role": r, "recipe": t} for r, t in data + params]


def recipe_document(*, tool: str, op: str, op_version: int, params: dict, engine: dict,
                    inputs: list[dict], variant: str | None = None) -> dict:
    return {
        "schema": RECIPE_SCHEMA,
        "tool": tool,
        "op": op,
        "op_version": int(op_version),
        "params": canon.normalize(params),
        "engine": dict(engine),
        "variant": variant,
        "inputs": [{"role": i["role"], "recipe": i["recipe"]} for i in inputs],
    }


def recipe_hash(doc: dict) -> str:
    return hashlib.sha256(canon.dumps(doc).encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Content identities for inputs without provenance.
# ---------------------------------------------------------------------------

def _memo_file() -> Path:
    from .uris import cache_dir

    return cache_dir() / "identity-memo.json"


def _memo_get(key: str) -> str | None:
    try:
        return json.loads(_memo_file().read_text()).get(key)
    except (OSError, ValueError):
        return None


def _memo_put(key: str, value: str) -> None:
    try:
        path = _memo_file()
        data = {}
        if path.is_file():
            try:
                data = json.loads(path.read_text())
            except ValueError:
                data = {}
        data[key] = value
        if len(data) > 5000:  # keep it small
            data = dict(list(data.items())[-4000:])
        tmp = path.with_name(path.name + f".{os.getpid()}.tmp")
        tmp.write_text(json.dumps(data))
        os.replace(tmp, path)
    except OSError:
        pass


def file_identity(path: str | Path) -> str:
    path = Path(path)
    st = path.stat()
    key = f"{path.resolve()}|{st.st_size}|{st.st_mtime_ns}"
    hit = _memo_get(key)
    if hit:
        return hit
    h = hashlib.md5()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(_CHUNK), b""):
            h.update(chunk)
    ident = f"md5:{h.hexdigest()}:{st.st_size}"
    _memo_put(key, ident)
    return ident


def md5_b64(path: str | Path) -> str:
    """A file's MD5 in base64, the form GCS reports (memoized like file_identity)."""
    import base64

    hexdigest = file_identity(path).split(":")[1]
    return base64.b64encode(bytes.fromhex(hexdigest)).decode()


def gcs_identity(md5_b64: str | None, size: int) -> str | None:
    """The same identity string file_identity gives, from GCS's base64 MD5."""
    if not md5_b64:
        return None
    import base64

    return f"md5:{base64.b64decode(md5_b64).hex()}:{int(size)}"


_ZARR_META = {".zarray", ".zattrs", ".zgroup", ".zmetadata", "zarr.json"}


def tree_identity(path: str | Path) -> str:
    """Directory identity (a Zarr store without provenance) by content.

    Every file's bytes are hashed: chunk sizes alone can't tell stores apart
    (uncompressed or float chunks keep their size whatever the values). The
    result is remembered against the tree's listing (names, sizes, mtimes),
    so an unchanged store is hashed once.
    """
    path = Path(path)
    files = sorted((p for p in path.rglob("*") if p.is_file()),
                   key=lambda p: p.relative_to(path).as_posix())
    listing = hashlib.sha256()
    for p in files:
        st = p.stat()
        listing.update(f"{p.relative_to(path).as_posix()}|{st.st_size}|{st.st_mtime_ns}\n"
                       .encode())
    key = f"tree|{path.resolve()}|{listing.hexdigest()}"
    hit = _memo_get(key)
    if hit:
        return hit
    h = hashlib.sha256()
    for p in files:
        h.update(p.relative_to(path).as_posix().encode() + b"\0")
        with open(p, "rb") as fh:
            for chunk in iter(lambda: fh.read(_CHUNK), b""):
                h.update(chunk)
        h.update(b"\0")
    ident = f"tree:{h.hexdigest()}"
    _memo_put(key, ident)
    return ident


def content_identity(path: str | Path) -> str:
    path = Path(path)
    return tree_identity(path) if path.is_dir() else file_identity(path)


def of_provenance(doc: dict | None) -> str | None:
    """Identity of a file from its provenance.

    Source files (a downloaded .raw) are identified by content, exactly as
    if they had no provenance, so the sidecar never changes downstream
    hashes; derived products by their product hash.
    """
    if not doc:
        return None
    product = doc.get("product", {})
    if product.get("role") == "source" and product.get("content_id"):
        return product["content_id"]
    if product.get("hash"):
        return "aa:" + product["hash"]
    return None


__all__ = [
    "PRODUCT_SCHEMA", "RECIPE_SCHEMA", "DATA", "engine_versions", "identity_document",
    "product_hash", "input_recipe", "recipe_inputs", "recipe_document", "recipe_hash",
    "file_identity", "md5_b64", "gcs_identity", "tree_identity", "content_identity",
    "of_provenance",
]
