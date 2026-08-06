#!/usr/bin/env python3
"""
aa-store

Console tool for describing and verifying a Zarr store: dimensions, chunk
shape, how many chunks were actually written, stored vs logical bytes, and
the lineage recorded by whichever tool produced it.

Read-only. Nothing here opens a write handle, which is what makes it safe
to run speculatively — on a store that is still being written, on a store
a SIGTERM interrupted, on a store you are not sure is a store.

    aa-store info   STORE      describe it
    aa-store verify STORE      check it is complete and coherent

Pipeline-friendly: reads the store path from a positional arg or stdin,
writes the store path (or, with --json, a handle line) to stdout, all logs
to stderr.

Why this reads metadata instead of opening the dataset
------------------------------------------------------
`xr.open_zarr` / `ep.open_converted` need the store to be *valid*. Half the
questions worth asking are about stores that are not: the write that died
two thirds of the way through, the store with 1,122 of 1,160 chunks on
disk. So aa-store parses `.zarray` / `zarr.json` itself and counts objects.
It answers for a broken store, and it costs one listing rather than a
metadata consolidation plus a decode.

The two ratios worth having
---------------------------
    chunkCount.written / expected   sparsity. Empty chunks are never
                                    written, so for a mask this is the
                                    number that shows the compression the
                                    layout is buying you.
    bytes.stored / logical          the compression ratio.

Everything else in the output is decoration around those two.

Typical usage:
    aa-store info combined.zarr
    aa-store info --json combined.zarr | jq .
    aa-combine *.nc -o out.zarr | aa-store verify --json
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
# _configure_logging() below replaces this once --quiet / --debug are parsed.
logger.add(sys.stderr, level="WARNING")

# Now the heavy imports — anything they log gets squashed
import argparse
import json
import math
import os
import signal
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Optional

# Pipeline tools should die cleanly when the downstream end of the pipe
# closes early (`... | head -n 1`), not throw BrokenPipeError. Guarded
# with hasattr because SIGPIPE doesn't exist on Windows.
if hasattr(signal, "SIGPIPE"):
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)


# Metadata documents, at every Zarr version. Everything else inside an array
# directory is a chunk (or a shard), which is what makes the census a file
# count rather than a decode.
ZARR_META_NAMES = {".zarray", ".zgroup", ".zattrs", ".zmetadata", "zarr.json"}

# Written into the root group's attributes by the tools that create stores
# (aa-combine does). Its absence is why `verify` cannot always tell a store
# that is *sparse* from a store that is *unfinished* — see _assess().
WRITE_MARKER = "aa_write"

# The variable names that identify a layer. Spelled to match
# frontend/src/types/layers.ts, which is the vocabulary the UI reads; keep
# these two lists identical or the badges start lying.
KIND_BY_VARIABLE = [
    ("Sv", "sv"),
    ("Sv_mvbs", "mvbs"),
    ("mask", "mask"),
]


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


def _configure_logging(quiet: bool, debug: bool) -> None:
    """Replace the suppression sink with one at the user's chosen level.
    --debug wins over --quiet (mutually-exclusive check happens in main)."""
    logger.remove()
    if debug:
        logger.add(sys.stderr, level="DEBUG", backtrace=True, diagnose=False)
    elif quiet:
        logger.add(sys.stderr, level="WARNING", backtrace=False, diagnose=False)
    else:
        logger.add(sys.stderr, level="INFO", backtrace=True, diagnose=False)


def print_help() -> None:
    help_text = """
    Usage: aa-store [OPTIONS] SUBCOMMAND [STORE]

    Subcommands:
      info                      Describe the store: dims, chunk shape,
                                chunks written vs expected, stored vs
                                logical bytes, codec, lineage.
      verify                    The same read, judged. Exits 0 when the
                                store is complete, 3 when it is coherent
                                but unfinished (resumable), 4 when it is
                                finished and wrong.

    Arguments:
      STORE                     Path to a .zarr store. Optional; falls
                                back to stdin, which may be a bare path
                                (what every other aa-* tool prints) or an
                                aa/1 handle line.

    Options:
      --json                    Emit one JSON document on stdout instead
                                of the human summary. This is what the
                                Workbench reads.
      --arrays                  Include the per-array breakdown in --json
                                output. Off by default: an EchoData store
                                has dozens of arrays and the UI wants the
                                summary.
      --group PATH              Restrict to one group, e.g. --group Sonar.
                                Default: the whole store.
      --no-census               Skip the object count. dims, chunks and
                                codec still come out; chunkCount and
                                bytes.stored do not. Use on a remote store
                                with millions of objects, where the
                                listing is the entire cost.
      --max-objects N           Give up the census after N objects and
                                report what was counted with
                                census.partial = true. Default: 2000000.
      --strict                  verify only: treat a store with no write
                                marker and missing chunks as unfinished
                                (exit 3) rather than assuming it is sparse
                                by design. Off by default — see below.

      -q, --quiet               Warnings and errors only.
      --debug                   Verbose logging.
      -h, --help                This message.

    Sparse or unfinished?
      A missing chunk means "every value here is the fill value". For a
      mask that is the point; for an interrupted write it is data loss.
      Nothing in the Zarr format distinguishes them, so aa-* tools record
      completion in the root group's attributes under `aa_write` when they
      finish. verify uses it:

        marker present, complete   missing chunks are sparsity   -> 0
        marker present, partial    missing chunks are unwritten  -> 3
        marker absent              unknowable; reported, not     -> 0
                                   judged, unless --strict          (3)

    Exit codes:
      0 ok        1 runtime error    2 usage
      3 partial (coherent, resumable)   4 verify failed

    Examples:
      aa-store info combined.zarr
      aa-store info --json --arrays combined.zarr | jq '.arrays[0]'
      aa-store verify --strict combined.zarr || echo "not finished"
      echo gs://bucket/tr07-sv.zarr | aa-store info --json
    """
    print(help_text)


# --------------------------------------------------------------------------- #
# Filesystem access
#
# Local stores go through pathlib and need nothing installed. Remote stores go
# through fsspec if it happens to be importable — it is, in the AA-SI venv,
# because zarr pulls it in — and produce a clean error if it is not. Keeping
# the local path dependency-free means `aa-store info` works in an environment
# where echopype is broken, which is exactly when you want to look at a store.
# --------------------------------------------------------------------------- #
class _Local:
    scheme = "file"

    def __init__(self, root: Path):
        self.root = root

    def exists(self, rel: str) -> bool:
        return (self.root / rel).exists()

    def read_text(self, rel: str) -> Optional[str]:
        target = self.root / rel
        try:
            return target.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            return None

    def walk(self, max_objects: int):
        """Yield (relative posix path, size in bytes) for every file."""
        count = 0
        for dirpath, _dirnames, filenames in os.walk(self.root):
            base = Path(dirpath)
            for filename in filenames:
                full = base / filename
                try:
                    size = full.stat().st_size
                except OSError:
                    size = 0
                yield full.relative_to(self.root).as_posix(), size
                count += 1
                if count >= max_objects:
                    return


class _Remote:
    def __init__(self, uri: str):
        try:
            import fsspec
        except ImportError as exc:  # pragma: no cover - depends on the venv
            raise RuntimeError(
                f"reading {uri} needs fsspec (plus gcsfs/s3fs for that scheme). "
                "Local stores need nothing."
            ) from exc
        self.fs, self.root = fsspec.core.url_to_fs(uri)
        self.scheme = uri.split("://", 1)[0]

    def _abs(self, rel: str) -> str:
        return f"{self.root.rstrip('/')}/{rel}" if rel else self.root

    def exists(self, rel: str) -> bool:
        return bool(self.fs.exists(self._abs(rel)))

    def read_text(self, rel: str) -> Optional[str]:
        try:
            with self.fs.open(self._abs(rel), "rb") as handle:
                return handle.read().decode("utf-8")
        except Exception:  # noqa: BLE001 - absent is the common case
            return None

    def walk(self, max_objects: int):
        count = 0
        root = self.root.rstrip("/")
        # detail=True gets key and size in one listing. Asking for the size
        # separately is a HEAD per object, which on a store with a hundred
        # thousand chunks is the difference between seconds and an afternoon.
        for path, info in self.fs.find(root, detail=True).items():
            if info.get("type") == "directory":
                continue
            rel = path[len(root):].lstrip("/")
            yield rel, int(info.get("size") or 0)
            count += 1
            if count >= max_objects:
                return


def _uri_from_line(line: str) -> Optional[str]:
    """One stdin line -> a store URI, or None for blanks and comments.

    stdin may be a bare path (what every other aa-* tool prints) or an aa/1
    handle line. Accepting both is what lets these tools drop into the
    pipelines that already exist rather than requiring the whole chain to be
    converted at once.
    """
    text = line.strip()
    if not text or text.startswith("#"):
        return None
    if text.startswith("{"):
        try:
            return str(json.loads(text)["uri"])
        except (json.JSONDecodeError, KeyError, TypeError) as exc:
            logger.warning(f"Skipping stdin line: not a handle with a uri ({exc})")
            return None
    return text


def _open_store(uri: str):
    if "://" in uri and not uri.startswith("file://"):
        return _Remote(uri)
    local = uri[len("file://"):] if uri.startswith("file://") else uri
    return _Local(Path(local).expanduser())


def _to_uri(value: str) -> str:
    """Normalise to a URI. A handle must never carry a bare path — it resolves
    against whatever directory the reader happens to be standing in, which is
    a bug that only appears once the handle crosses a machine."""
    text = str(value).strip()
    if "://" in text:
        return text
    return "file://" + os.path.abspath(os.path.expanduser(text))


# --------------------------------------------------------------------------- #
# Zarr metadata, v2 and v3
# --------------------------------------------------------------------------- #
def _load_metadata(store, group: str = "") -> dict[str, dict]:
    """Map array path -> its metadata document, for every array in the store.

    Tries three sources in order of cost: consolidated metadata (one read),
    a v3 root document, then a walk. The consolidated path is what makes this
    fast over the network, and its absence is itself worth reporting — an
    unconsolidated store on object storage opens slowly forever.
    """
    arrays: dict[str, dict] = {}

    # v2 consolidated: one document at .zmetadata holding every node.
    raw = store.read_text(".zmetadata")
    if raw:
        try:
            entries = json.loads(raw).get("metadata", {})
        except json.JSONDecodeError:
            entries = {}
        for key, value in entries.items():
            if key.endswith(".zarray"):
                path = key[: -len("/.zarray")] if "/" in key else ""
                arrays[path] = _normalise_v2(value)
        if arrays:
            return _filter_group(arrays, group)

    # v3 consolidated: the same idea in a different place — a
    # `consolidated_metadata` block inside the root zarr.json, nested to match
    # the group hierarchy rather than flattened. Worth handling, because
    # missing it means walking the tree on a store that went to the trouble of
    # making that unnecessary.
    raw = store.read_text("zarr.json")
    if raw:
        try:
            root = json.loads(raw)
        except json.JSONDecodeError:
            root = {}
        block = (root.get("consolidated_metadata") or {}).get("metadata")
        if isinstance(block, dict):
            for path, document in _flatten_v3_consolidated(block):
                if document.get("node_type") == "array":
                    arrays[path] = _normalise_v3(document)
            if arrays:
                return _filter_group(arrays, group)

    # v3: one zarr.json per node. There is no consolidated form to lean on in
    # the general case, so this is a walk of the metadata documents only.
    if store.exists("zarr.json") or store.exists(".zgroup"):
        for path, document in _walk_metadata(store):
            if document.get("node_type") == "array":
                arrays[path] = _normalise_v3(document)
            elif "shape" in document and "chunks" in document:
                arrays[path] = _normalise_v2(document)
    return _filter_group(arrays, group)


def _flatten_v3_consolidated(block: dict, prefix: str = ""):
    """Walk the nested v3 consolidated block into (path, document) pairs.

    v2 flattened the tree into dotted keys; v3 nests each group's children
    under its own `consolidated_metadata`. Same information, so flatten it to
    the same shape the rest of this module works in.
    """
    for name, document in block.items():
        if not isinstance(document, dict):
            continue
        path = f"{prefix}/{name}" if prefix else name
        yield path, document
        nested = (document.get("consolidated_metadata") or {}).get("metadata")
        if isinstance(nested, dict):
            yield from _flatten_v3_consolidated(nested, path)


def _is_consolidated(store) -> bool:
    """True when the store carries consolidated metadata, at either version."""
    if store.exists(".zmetadata"):
        return True
    raw = store.read_text("zarr.json")
    if not raw:
        return False
    try:
        return bool((json.loads(raw) or {}).get("consolidated_metadata"))
    except json.JSONDecodeError:
        return False


def _walk_metadata(store):
    """Yield (node path, parsed metadata) for every array node in the store.

    Walking the whole tree to find metadata is unavoidable without
    consolidation; it is bounded by the number of *nodes*, not chunks, so it
    stays cheap even when the census that follows does not.
    """
    seen: set[str] = set()
    for rel, _size in store.walk(max_objects=200_000):
        name = PurePosixPath(rel).name
        if name not in {"zarr.json", ".zarray"}:
            continue
        path = str(PurePosixPath(rel).parent)
        path = "" if path == "." else path
        if path in seen:
            continue
        seen.add(path)
        raw = store.read_text(rel)
        if not raw:
            continue
        try:
            yield path, json.loads(raw)
        except json.JSONDecodeError:
            logger.warning(f"Unparseable metadata at {rel}; skipping")


def _filter_group(arrays: dict[str, dict], group: str) -> dict[str, dict]:
    if not group:
        return arrays
    prefix = group.strip("/")
    return {
        path: meta
        for path, meta in arrays.items()
        if path == prefix or path.startswith(prefix + "/")
    }


def _normalise_v2(document: dict) -> dict:
    codec = document.get("compressor") or {}
    if isinstance(codec, list) and codec:
        codec = codec[0] if isinstance(codec[0], dict) else {}
    return {
        "zarr_format": 2,
        "shape": [int(n) for n in document.get("shape", [])],
        "chunks": [int(n) for n in document.get("chunks", [])],
        "shards": None,
        "dtype": str(document.get("dtype", "")),
        "fill_value": document.get("fill_value"),
        "codec": _codec_name(
            codec.get("id"),
            codec.get("level") if codec.get("level") is not None else codec.get("clevel"),
            codec.get("cname"),
        ),
        "dimension_separator": document.get("dimension_separator", "."),
    }


def _normalise_v3(document: dict) -> dict:
    grid = (document.get("chunk_grid") or {}).get("configuration", {})
    write_chunk = [int(n) for n in grid.get("chunk_shape", [])]
    inner = write_chunk
    shards = None
    codec_name = None
    level = None
    cname = None

    for codec in document.get("codecs", []) or []:
        name = codec.get("name") if isinstance(codec, dict) else str(codec)
        config = codec.get("configuration", {}) if isinstance(codec, dict) else {}
        if name == "sharding_indexed":
            # The stored object is the shard; the chunk is inside it. Reporting
            # the shard as the chunk shape is the mistake that makes a sharded
            # store look like it has 40x fewer, 40x larger chunks than it has.
            shards = write_chunk
            inner = [int(n) for n in config.get("chunk_shape", write_chunk)]
            for inner_codec in config.get("codecs", []) or []:
                inner_name = (
                    inner_codec.get("name")
                    if isinstance(inner_codec, dict)
                    else str(inner_codec)
                )
                inner_config = (
                    inner_codec.get("configuration", {})
                    if isinstance(inner_codec, dict)
                    else {}
                )
                if inner_name in {"zstd", "blosc", "gzip"}:
                    codec_name = inner_name
                    level = (
                        inner_config.get("level")
                        if inner_config.get("level") is not None
                        else inner_config.get("clevel")
                    )
                    cname = inner_config.get("cname")
        elif name in {"zstd", "blosc", "gzip"}:
            codec_name = name
            level = (
                config.get("level") if config.get("level") is not None
                else config.get("clevel")
            )
            cname = config.get("cname")

    data_type = document.get("data_type")
    if isinstance(data_type, dict):
        data_type = data_type.get("name", "")

    return {
        "zarr_format": 3,
        "shape": [int(n) for n in document.get("shape", [])],
        "chunks": inner,
        "shards": shards,
        "dtype": str(data_type or ""),
        "fill_value": document.get("fill_value"),
        "codec": _codec_name(codec_name, level, cname),
        "dimension_separator": "/",
    }


def _codec_name(
    name: Optional[str], level: Optional[Any], cname: Optional[str] = None
) -> Optional[str]:
    """`zstd:5`, `blosc-lz4:5`. Blosc names the codec it wraps in `cname`, and
    reporting a bare `blosc` hides the one thing anyone chose about it."""
    if not name:
        return None
    label = f"{name}-{cname}" if name == "blosc" and cname else str(name)
    return f"{label}:{level}" if level is not None else label


def _itemsize(dtype: str) -> int:
    """Bytes per element, without importing numpy for a string parse."""
    try:
        import numpy as np

        return int(np.dtype(dtype).itemsize)
    except Exception:  # noqa: BLE001 - fall through to the text form
        pass
    digits = "".join(character for character in str(dtype) if character.isdigit())
    if digits:
        bits = int(digits)
        return max(1, bits // 8) if bits >= 8 else 1
    return 8


def _expected_chunks(shape, chunks) -> int:
    if not shape or not chunks or len(shape) != len(chunks):
        return 0
    total = 1
    for extent, span in zip(shape, chunks):
        if span <= 0:
            return 0
        total *= max(1, math.ceil(extent / span))
    return total


def _attributes(store, path: str) -> dict:
    """Attributes for a node, at either Zarr version."""
    prefix = f"{path}/" if path else ""
    raw = store.read_text(f"{prefix}.zattrs")
    if raw:
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return {}
    raw = store.read_text(f"{prefix}zarr.json")
    if raw:
        try:
            return json.loads(raw).get("attributes", {}) or {}
        except json.JSONDecodeError:
            return {}
    return {}


# --------------------------------------------------------------------------- #
# The census
# --------------------------------------------------------------------------- #
def _census(store, arrays: dict[str, dict], max_objects: int) -> dict:
    """One listing, bucketed by array. Counts objects and sums their bytes.

    Doing this per array would be one listing per array — dozens, on an
    EchoData store, each paying the same round trip. One walk and a longest
    prefix match costs the same as the cheapest of them.
    """
    paths = sorted(arrays, key=len, reverse=True)
    counted = {path: {"objects": 0, "bytes": 0} for path in arrays}
    metadata_bytes = 0
    total_objects = 0
    partial = False

    for rel, size in store.walk(max_objects=max_objects + 1):
        total_objects += 1
        if total_objects > max_objects:
            partial = True
            break
        name = PurePosixPath(rel).name
        if name in ZARR_META_NAMES:
            metadata_bytes += size
            continue
        for path in paths:
            if not path or rel.startswith(path + "/"):
                counted[path]["objects"] += 1
                counted[path]["bytes"] += size
                break

    return {
        "byArray": counted,
        "metadataBytes": metadata_bytes,
        "objects": total_objects,
        "partial": partial,
    }


# --------------------------------------------------------------------------- #
# Description
# --------------------------------------------------------------------------- #
def describe(
    uri: str,
    group: str = "",
    census: bool = True,
    max_objects: int = 2_000_000,
    include_arrays: bool = False,
) -> dict:
    """Everything aa-store knows about a store, as one JSON-ready dict."""
    store = _open_store(uri)

    if not (store.exists(".zgroup") or store.exists("zarr.json") or store.exists(".zarray")):
        raise FileNotFoundError(
            f"{uri} has no .zgroup, .zarray or zarr.json — not a Zarr store"
        )

    arrays = _load_metadata(store, group)
    if not arrays:
        raise ValueError(f"{uri} contains no arrays" + (f" under {group!r}" if group else ""))

    root_attributes = _attributes(store, "")
    consolidated = _is_consolidated(store)

    counts = (
        _census(store, arrays, max_objects)
        if census
        else {"byArray": {}, "metadataBytes": 0, "objects": 0, "partial": False}
    )

    entries = []
    for path, meta in sorted(arrays.items()):
        expected = _expected_chunks(meta["shape"], meta["chunks"])
        logical = _itemsize(meta["dtype"])
        for extent in meta["shape"]:
            logical *= max(0, extent)

        found = counts["byArray"].get(path)
        sharded = meta["shards"] is not None
        written = None
        if found is not None and not sharded:
            written = found["objects"]

        entry = {
            "path": path,
            "shape": meta["shape"],
            "chunks": meta["chunks"],
            "shards": meta["shards"],
            "dtype": meta["dtype"],
            "fillValue": meta["fill_value"],
            "codec": meta["codec"],
            "zarrFormat": meta["zarr_format"],
            "chunkCount": {"expected": expected, "written": written},
            "bytes": {
                "stored": found["bytes"] if found else None,
                "logical": logical,
            },
        }
        if sharded:
            # Proving an inner chunk exists means decoding the shard index,
            # which is a read of the data. Say so rather than reporting the
            # shard count as if it were the chunk count.
            entry["objects"] = {
                "expected": _expected_chunks(meta["shape"], meta["shards"]),
                "written": found["objects"] if found else None,
            }
            entry["note"] = "sharded: chunkCount.written needs a shard-index read"
        entries.append(entry)

    primary = _primary_array(entries)
    dims = _dimension_names(store, primary["path"], len(primary["shape"])) if primary else []

    summary: dict[str, Any] = {
        "schema": "aa/1",
        "kind": _detect_kind(arrays, root_attributes),
        "uri": _to_uri(uri),
        "zarrFormat": primary["zarrFormat"] if primary else None,
        "consolidated": consolidated,
        "group": group or None,
    }

    if primary:
        summary["dims"] = dict(zip(dims, primary["shape"]))
        summary["chunks"] = primary["chunks"]
        summary["shards"] = primary["shards"]
        summary["dtype"] = primary["dtype"]
        summary["codec"] = primary["codec"]
        summary["primaryArray"] = primary["path"]

    # scale / offset, when the producer wrote packed integers. Reported at the
    # top level because a consumer that ignores them reads numbers that are
    # wrong by two orders of magnitude and look plausible.
    packed = _attributes(store, primary["path"]) if primary else {}
    if "scale_factor" in packed:
        summary["scale"] = packed["scale_factor"]
    if "add_offset" in packed:
        summary["offset"] = packed["add_offset"]

    if census:
        expected_total = sum(item["chunkCount"]["expected"] for item in entries)
        written_total = sum(
            item["chunkCount"]["written"] or 0
            for item in entries
            if item["chunkCount"]["written"] is not None
        )
        unknown = any(item["chunkCount"]["written"] is None for item in entries)
        stored_total = sum(item["bytes"]["stored"] or 0 for item in entries)
        summary["chunkCount"] = {
            "expected": expected_total,
            "written": None if unknown else written_total,
        }
        summary["bytes"] = {
            "stored": stored_total + counts["metadataBytes"],
            "logical": sum(item["bytes"]["logical"] for item in entries),
        }
        summary["objects"] = counts["objects"]
        if counts["partial"]:
            summary["census"] = {"partial": True, "limit": max_objects}

    summary["arrayCount"] = len(entries)
    if include_arrays:
        summary["arrays"] = entries

    marker = root_attributes.get(WRITE_MARKER)
    if isinstance(marker, dict):
        summary["write"] = marker
    provenance = root_attributes.get("provenance")
    if isinstance(provenance, dict):
        summary["provenance"] = provenance
    if root_attributes.get("report"):
        summary["report"] = root_attributes["report"]
    for field in ("layout", "variantOf"):
        if root_attributes.get(field):
            summary[field] = root_attributes[field]

    time_range = _time_range(root_attributes)
    if time_range:
        summary["time"] = time_range

    return summary


def _primary_array(entries: list[dict]) -> Optional[dict]:
    """The array the summary describes: the largest one with rank > 1.

    A store's headline dims and chunk shape should come from the data, not
    from whichever coordinate happened to sort first. Coordinates are rank-1
    and tiny, so size plus rank picks the right one without a name list that
    would need maintaining per layer.
    """
    candidates = [item for item in entries if len(item["shape"]) > 1]
    if not candidates:
        candidates = entries
    if not candidates:
        return None
    return max(candidates, key=lambda item: item["bytes"]["logical"])


def _dimension_names(store, path: str, rank: int) -> list[str]:
    """Dimension names from `_ARRAY_DIMENSIONS` (v2) or `dimension_names` (v3).

    Both are xarray/Zarr conventions rather than requirements, so fall back to
    positional names instead of failing: a store with unnamed dims is still
    worth describing.
    """
    attributes = _attributes(store, path)
    names = attributes.get("_ARRAY_DIMENSIONS")
    if not names:
        prefix = f"{path}/" if path else ""
        raw = store.read_text(f"{prefix}zarr.json")
        if raw:
            try:
                names = json.loads(raw).get("dimension_names")
            except json.JSONDecodeError:
                names = None
    if not names or len(names) != rank:
        return [f"dim_{index}" for index in range(rank)]
    return [str(name) for name in names]


def _detect_kind(arrays: dict[str, dict], attributes: dict) -> str:
    """Which layer this store holds, in layers.ts spelling.

    An explicit `aa_kind` attribute wins — the producing tool knows, and
    guessing over the top of it is how a mask store gets badged `sv`. Name
    matching is the fallback for stores written before the convention.
    """
    declared = attributes.get("aa_kind") or attributes.get("kind")
    if declared:
        return str(declared)

    names = {PurePosixPath(path).name for path in arrays}
    groups = {str(PurePosixPath(path).parent) for path in arrays}
    for variable, kind in KIND_BY_VARIABLE:
        if variable in names:
            return kind
    if any(group.startswith("Sonar/Beam_group") for group in groups):
        return "l1"
    # Not in layers.ts, deliberately. Better an honest "unclassified" badge
    # than a confident wrong one.
    return "unknown"


def _time_range(attributes: dict) -> Optional[list]:
    for key in ("time_coverage_start", "time_coverage_end"):
        if key not in attributes:
            return None
    return [attributes["time_coverage_start"], attributes["time_coverage_end"]]


# --------------------------------------------------------------------------- #
# Verification
# --------------------------------------------------------------------------- #
def _assess(summary: dict, strict: bool) -> tuple[int, list[str], list[str]]:
    """Judge a description. Returns (exit code, problems, notes)."""
    problems: list[str] = []
    notes: list[str] = []

    counts = summary.get("chunkCount") or {}
    expected = counts.get("expected")
    written = counts.get("written")
    marker = summary.get("write") or {}
    complete = marker.get("complete")

    if written is None:
        notes.append("chunk census unavailable (sharded or --no-census); not judged")
    elif expected and written > expected:
        problems.append(
            f"{written} chunk objects for {expected} expected — stale chunks from a "
            "previous write with a different shape, which will be read as data"
        )
    elif expected and written < expected:
        missing = expected - written
        share = missing / expected
        if complete is True:
            notes.append(
                f"{missing} of {expected} chunks absent ({share:.1%}); the write "
                "declared itself complete, so these are empty and cost nothing"
            )
        elif complete is False:
            problems.append(
                f"{missing} of {expected} chunks not yet written; the marker says "
                "the write did not finish"
            )
        elif strict:
            problems.append(
                f"{missing} of {expected} chunks absent and no write marker "
                "(--strict)"
            )
        else:
            notes.append(
                f"{missing} of {expected} chunks absent ({share:.1%}). No write "
                "marker, so this is either sparsity or an unfinished write — "
                "re-run with --strict to treat it as unfinished"
            )

    if not summary.get("consolidated") and summary.get("zarrFormat") == 2:
        notes.append(
            "no consolidated metadata: opening this store costs one request per "
            "array, which is slow over the network and free to fix with "
            "zarr.consolidate_metadata"
        )

    if summary.get("census", {}).get("partial"):
        notes.append("census hit --max-objects; counts are a lower bound")

    if not problems:
        return 0, problems, notes

    # A store that never claimed to be finished is unfinished, not wrong.
    # That difference is the whole reason exit 3 exists: 3 is resumable and
    # the UI can offer Resume, 4 means stop and look.
    unfinished = complete is False or (complete is None and strict)
    return (3 if unfinished else 4), problems, notes


# --------------------------------------------------------------------------- #
# Human output
# --------------------------------------------------------------------------- #
def _bytes_human(value: Optional[int]) -> str:
    if value is None:
        return "—"
    size = float(value)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


def _print_summary(summary: dict) -> None:
    """The human view, on stderr — stdout belongs to the pipeline."""
    out = sys.stderr.write
    out(f"{summary['uri']}\n")
    out(f"  kind        {summary.get('kind', 'unknown')}"
        f"   zarr v{summary.get('zarrFormat', '?')}"
        f"   {'consolidated' if summary.get('consolidated') else 'unconsolidated'}\n")

    dims = summary.get("dims") or {}
    if dims:
        shape = ", ".join(f"{name}: {size:,}" for name, size in dims.items())
        out(f"  dims        {shape}\n")
    if summary.get("chunks"):
        chunks = "x".join(str(n) for n in summary["chunks"])
        line = f"  chunks      {chunks}"
        if summary.get("shards"):
            line += f"   shards {'x'.join(str(n) for n in summary['shards'])}"
        if summary.get("codec"):
            line += f"   {summary['codec']}"
        out(line + f"   {summary.get('dtype', '')}\n")

    counts = summary.get("chunkCount") or {}
    if counts.get("expected"):
        written = counts.get("written")
        if written is None:
            out(f"  chunks      {counts['expected']:,} expected (written: unknown)\n")
        else:
            share = written / counts["expected"] if counts["expected"] else 0
            out(f"  written     {written:,} / {counts['expected']:,} chunks "
                f"({share:.1%} materialised)\n")

    size = summary.get("bytes") or {}
    if size.get("logical"):
        stored = size.get("stored")
        ratio = f"{size['logical'] / stored:.1f}x" if stored else "—"
        out(f"  bytes       {_bytes_human(stored)} stored / "
            f"{_bytes_human(size['logical'])} logical  ({ratio})\n")

    if summary.get("time"):
        out(f"  time        {summary['time'][0]} .. {summary['time'][1]}\n")
    provenance = summary.get("provenance") or {}
    if provenance.get("tool"):
        parents = provenance.get("parents") or []
        out(f"  produced by {provenance['tool']} {provenance.get('version', '')}"
            f"   parents: {len(parents)}\n")
    out(f"  arrays      {summary.get('arrayCount', 0)}\n")


# --------------------------------------------------------------------------- #
# main
# --------------------------------------------------------------------------- #
def main() -> None:
    if len(sys.argv) == 1:
        print_help()
        sys.exit(0)
    if "--help" in sys.argv or "-h" in sys.argv:
        print_help()
        sys.exit(0)

    parser = argparse.ArgumentParser(
        description="Describe or verify a Zarr store.", add_help=False
    )
    # One greedy positional rather than `subcommand` + `store nargs="?"`.
    # argparse cannot reliably interleave optionals with an optional
    # positional: `info --json store.zarr` parses as subcommand=info with
    # store.zarr left over, and reports it as an unrecognised argument. One
    # list and a manual split makes flag position irrelevant, and gets
    # multi-store input for free.
    parser.add_argument("args", nargs="*", default=[])
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--arrays", action="store_true")
    parser.add_argument("--group", default="")
    parser.add_argument("--no-census", "--no_census", dest="census",
                        action="store_false", default=True)
    parser.add_argument("--max-objects", "--max_objects", dest="max_objects",
                        type=int, default=2_000_000)
    parser.add_argument("--strict", action="store_true")
    parser.add_argument("-q", "--quiet", action="store_true")
    parser.add_argument("--debug", action="store_true")

    # `parse_known_args` rather than `parse_args`, because argparse cannot
    # match a variadic positional across an optional: given
    # `info --json store.zarr` it consumes ["info"], reads --json, then has no
    # positional left for store.zarr and calls it unrecognised. Collecting the
    # leftovers by hand makes flag position irrelevant — which matters, since
    # `aa-store info --json x.zarr` is the spelling everyone reaches for.
    args, leftover = parser.parse_known_args()
    stray_flags = [item for item in leftover if item.startswith("-")]
    if stray_flags:
        logger.error(f"Unknown option(s): {' '.join(stray_flags)}")
        sys.exit(2)
    args.args = list(args.args) + [item for item in leftover if not item.startswith("-")]

    if args.debug and args.quiet:
        logger.error("Use --debug OR --quiet, not both.")
        sys.exit(2)
    _configure_logging(args.quiet, args.debug)

    # ---------------------------
    # Validate input
    # ---------------------------
    if not args.args:
        logger.error("Give a subcommand: info or verify.")
        sys.exit(2)

    subcommand, stores = args.args[0], list(args.args[1:])
    if subcommand not in {"info", "verify"}:
        logger.error(f"Unknown subcommand {subcommand!r}. Use info or verify.")
        sys.exit(2)

    if not stores:
        if sys.stdin.isatty():
            logger.error("No store given and no stdin available.")
            sys.exit(1)
        for line in sys.stdin:
            uri = _uri_from_line(line)
            if uri:
                stores.append(uri)
        if not stores:
            logger.error("Empty stdin.")
            sys.exit(1)
        logger.info(f"Read {len(stores)} store(s) from stdin.")

    if args.max_objects < 1:
        logger.error("--max-objects must be at least 1.")
        sys.exit(2)

    # ---------------------------
    # Describe each store
    # ---------------------------
    worst = 0
    for uri in stores:
        try:
            summary = describe(
                uri,
                group=args.group,
                census=args.census,
                max_objects=args.max_objects,
                include_arrays=args.arrays,
            )
        except (FileNotFoundError, ValueError, RuntimeError) as exc:
            logger.error(str(exc))
            worst = max(worst, 1)
            continue
        except Exception as exc:  # noqa: BLE001 - report, never traceback at a user
            logger.exception(f"Could not read {uri}: {exc}")
            worst = max(worst, 1)
            continue

        code = 0
        if subcommand == "verify":
            code, problems, notes = _assess(summary, args.strict)
            summary["verify"] = {
                "ok": code == 0,
                "exit": code,
                "problems": problems,
                "notes": notes,
                "checkedAt": datetime.now(timezone.utc)
                .isoformat(timespec="seconds")
                .replace("+00:00", "Z"),
            }
            for note in notes:
                logger.info(note)
            for problem in problems:
                logger.warning(problem)

        if args.json:
            # One line per store, so many stores make one NDJSON stream.
            print(json.dumps(summary, separators=(",", ":"), default=str))
        else:
            _print_summary(summary)
            if subcommand == "verify":
                verdict = {0: "ok", 3: "partial (resumable)", 4: "FAILED"}[code]
                sys.stderr.write(f"  verify      {verdict}\n")
            # The path, so the store keeps flowing down a pipe of path-passing
            # tools even when this stage was only a look.
            print(summary["uri"])

        # The worst outcome wins: one failed store in a stream of ten is a
        # failure, and a caller that only reads $? must not be told otherwise.
        worst = max(worst, code)

    sys.exit(worst)


if __name__ == "__main__":
    main()
