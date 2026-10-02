"""The file-naming rule.

    The file name identifies the product; the extension identifies its
    representation.

Base name
    Established once, where the EchoData is made:
      * the raw file's stem (NCEI names are kept verbatim), or
      * your choice: ``--base NAME``, or the stem of ``-o`` on aa-nc, aa-ed
        or aa-combine (naming the EchoData names everything made from it).
      * aa-combine without either: ``combined``.
    Stored in every product's provenance (``base``) and read back by the
    next tool, so it survives renames and gs:// round trips. Tools never
    change it on their own; ``--base`` on a later tool starts a new base
    from there on. If an input has no provenance, its file stem is the
    base.

Names
    EchoData (aa-nc, aa-ed, aa-combine)   <base>.nc | <base>.zarr
    scientific products                   <base>_<hash8>.<ext>
    renderings (aa-graph, aa-plot)        <name of the product shown>.<ext>
                                          e.g. <base>_<hash8>.png
    explicit -o PATH                      exactly as each tool always did

    <hash8> is the first 8 hex digits of the RECIPE hash: the chain of
    scientific steps and their options, without the data. The base says
    which data, the hash says which processing, so the same processing on
    two surveys gives HB1603_…_71957ca9.nc and HB1701_…_71957ca9.nc. The
    product hash (recipe + data, used for reuse) and the full recipe are
    inside the file (``aa-metadata FILE``).

``AA_NAMING=legacy`` restores the old suffix names (``_Sv``, ``_clean``,
``_mvbs`` ...) for anything that depends on them. Provenance is embedded
either way.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

HASH_LEN = 8
# Only what can't be in a file name or would change the path: separators and
# control characters. Everything else (spaces, parentheses, non-ASCII) is
# kept, so a raw file's stem is used verbatim, as the tools always did.
_UNSAFE = re.compile(r"[\x00-\x1f\x7f/\\]+")
# Suffixes that are part of the representation, not the product name.
_COMPOUND = (".aa.json", ".qc.json", ".tar.gz")


def mode() -> str:
    """'hash' (default) or 'legacy'."""
    value = (os.getenv("AA_NAMING") or "hash").strip().lower()
    return "legacy" if value in {"legacy", "suffix", "old"} else "hash"


def sanitize_base(name: str) -> str:
    """A base name usable as a file name and a gs:// key component.

    Path separators and control characters become '_'; leading dots and
    surrounding whitespace are dropped (no hidden files, no '..'). All other
    characters are kept verbatim.
    """
    name = _UNSAFE.sub("_", str(name).replace(os.sep, "_")).strip().lstrip(".").strip()
    return name or "product"


def stem_of(name: str | Path) -> str:
    """File name without its representation suffix(es)."""
    name = Path(str(name).rstrip("/")).name
    low = name.lower()
    for suffix in _COMPOUND:
        if low.endswith(suffix):
            return name[: -len(suffix)]
    return Path(name).stem if "." in name else name


def base_of(name: str | Path) -> str:
    """The base a file implies when it carries no provenance: its stem."""
    return sanitize_base(stem_of(name))


def echodata_name(base: str, ext: str) -> str:
    return f"{base}{ext}"


def derived_name(base: str, product_hash: str, ext: str) -> str:
    return f"{base}_{product_hash[:HASH_LEN]}{ext}"


def representation_name(depicted: str | Path, ext: str) -> str:
    return f"{stem_of(depicted)}{ext}"


def with_ext(target: str | Path, ext: str) -> str:
    """Replace the extension of a local path or a gs:// URI (``-o x`` -> ``x.nc``).

    A target ending in '/' is a folder and is returned unchanged (the
    standard name goes inside it).
    """
    target = str(target)
    if target.endswith("/"):
        return target
    if "://" in target:
        head, _, tail = target.rpartition("/")
        return f"{head}/{Path(tail).with_suffix(ext).name}"
    return str(Path(target).with_suffix(ext))


def with_stem_suffix(target: str | Path, suffix: str, ext: str) -> str:
    """Legacy '-o x.nc -> x_Sv.nc' rule, for local paths and gs:// URIs.

    A target ending in '/' is a folder and is returned unchanged.
    """
    target = str(target)
    if target.endswith("/"):
        return target
    if "://" in target:
        head, _, tail = target.rpartition("/")
        p = Path(tail)
        return f"{head}/{p.with_stem(p.stem + suffix).with_suffix(ext).name}"
    p = Path(target)
    return str(p.with_stem(p.stem + suffix).with_suffix(ext))


__all__ = [
    "HASH_LEN", "mode", "sanitize_base", "stem_of", "base_of",
    "echodata_name", "derived_name", "representation_name", "with_ext", "with_stem_suffix",
]
