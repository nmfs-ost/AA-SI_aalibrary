#!/usr/bin/env python3
"""
aa-download

Copy objects from gs:// to local files, reuse-aware, and print the local
paths for the next stage.

    aa-download gs://bucket/raw/D20160703-T060000.raw          -> ./D20160703-T060000.raw
    aa-download gs://bucket/derived/x_71957ca9.nc --dest data/
    aa-download gs://bucket/l1/HB1603.zarr/                     -> ./HB1603.zarr (a folder)
    aa-download gs://bucket/raw/HB1603/ --pattern '*.raw'       -> ./HB1603/ (printed once)
    cat uris.txt | aa-download --dest data/ | ...

A file that is already there with the same content (GCS MD5) is not
downloaded again. Provenance travels with the file: products carry it
inside, a <key>.aa.json sidecar in the bucket is copied along, and a plain
file (a .raw) gets a sidecar recording its gs:// origin so aa-nc lists it.

Most aa-* tools read gs:// URIs directly (through a gcsfuse mount or the
download cache), so aa-download is for when you want a real local copy:
another program, offline work, or a folder of .raw files for aa-ed.

Exit codes: 0 ok, 1 an object could not be downloaded (the others still
are), 2 bad arguments.
"""

from __future__ import annotations

import argparse
import fnmatch
import os
import shutil
import sys
import uuid
from pathlib import Path

from aalibrary.console._core import (
    Help, ToolSpec, identity, provenance, record_source, render, show_help, stdio, uris,
)

SPEC = ToolSpec(name="aa-download", role="source", engines=())

HELP = Help(
    summary="Copy gs:// objects to local files (skips identical ones).",
    does=(
        "Downloads each gs:// object to a local file and prints its path. A file "
        "already there with the same content (GCS MD5) is kept, not downloaded "
        "again. A prefix ending in / is copied as a folder (a .zarr store, or a "
        "folder of .raw files, optionally filtered with --pattern) and the folder "
        "is printed once. When a gcsfuse mount shows the object, it is copied from "
        "the mount."
    ),
    stdin="gs:// URIs, one per line (or as arguments). aa/1 JSON handles work too.",
    stdout="One local absolute path per input (a folder for a prefix).",
    metadata=(
        "Provenance travels with the file. Products carry it inside (NetCDF "
        "attributes, PNG text, ...); a <key>.aa.json sidecar in the bucket is "
        "copied along; a plain file without either (e.g. a .raw) gets a new "
        "<file>.aa.json recording its gs:// origin and MD5, so aa-nc lists "
        "where the data came from. Nothing about the bytes changes, so hashes "
        "are the same as reading the gs:// URI directly."
    ),
    options=[
        ("--dest DIR", "download into DIR (default: the current directory)"),
        ("-o, --output PATH", "exact local path; one input only"),
        ("--pattern GLOB", "for a prefix: only names matching GLOB, e.g. '*.raw'"),
        ("--no-copy", "if a gcsfuse mount shows the object, print that path "
                      "instead of copying (no disk used)"),
        ("--force", "download even if an identical local file exists"),
        ("--dry-run", "print what would be downloaded; download nothing"),
    ],
    files=(
        "Reads gs:// objects with your Application Default Credentials (the "
        "project aalibrary is configured for). gcsfuse mounts (e.g. "
        "~/ggn-nmfs-aa-prod-1-data) are detected from /proc/mounts or "
        "AA_GCS_MOUNTS. Writes into --dest, -o, or the current directory."
    ),
    pipeline=(
        "A first stage: aa-download gs://.../x.raw | aa-nc --sonar_model EK60 | "
        "aa-sv. You rarely need it for .nc/.zarr inputs: every tool accepts "
        "gs:// URIs directly."
    ),
    examples=[
        "aa-download gs://ggn-nmfs-aa-prod-1-data/raw/D20160703-T060000.raw | aa-nc --sonar_model EK60",
        "aa-download gs://bucket/raw/HB1603/ --pattern '*.raw' --dest data/ | aa-ed --sonar_model EK60",
    ],
    common=False,
)


def print_help():
    """Curated help (also used by the docs generator)."""
    sys.stdout.write(render(SPEC, HELP, _build_parser()))


def print_help_full():
    print(__doc__)
    sys.stdout.write(_build_parser().format_help())


def _build_parser():
    p = argparse.ArgumentParser(prog="aa-download", add_help=False,
                                description="Copy gs:// objects to local files.")
    p.add_argument("uris", nargs="*", help="gs:// object URIs or prefixes (ending in /).")
    p.add_argument("--dest", default=None, metavar="DIR",
                   help="Download into DIR (default: current directory).")
    p.add_argument("-o", "--output", default=None, metavar="PATH",
                   help="Exact local path (one input only).")
    p.add_argument("--pattern", default=None, metavar="GLOB",
                   help="For a prefix: only object names matching GLOB.")
    p.add_argument("--no-copy", "--no_copy", dest="no_copy", action="store_true",
                   help="Print the gcsfuse mount path instead of copying, when mounted.")
    p.add_argument("--force", action="store_true", help="Download even if identical.")
    p.add_argument("--dry-run", "--dry_run", dest="dry_run", action="store_true",
                   help="Print what would be downloaded.")
    p.add_argument("--quiet", action="store_true", help="Only errors on stderr.")
    return p


def _say(args, msg: str) -> None:
    if not args.quiet:
        print(f"aa-download: {msg}", file=sys.stderr)


def _same_content(local: Path, info: uris.ObjectInfo) -> bool:
    """Is the local file byte-identical to the object (by GCS MD5)?"""
    if not local.is_file() or local.stat().st_size != info.size:
        return False
    remote_id = identity.gcs_identity(info.md5, info.size)
    if remote_id is None:          # composite objects have no MD5: can't tell
        return False
    return identity.file_identity(local) == remote_id


# Instrument files: the only kind that gets an origin sidecar when the bucket
# has none (products carry provenance inside; store chunks must not get one).
_RAW_TYPES = {".raw", ".idx", ".bot", ".ad2cp", ".01a", ".azfp", ".out", ".xml"}


def _fetch(info: uris.ObjectInfo, dest: Path, mount: Path | None) -> None:
    """Write the object to dest atomically (from the mount when its size
    agrees), pinned to the looked-up generation and checked by MD5."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(f".{dest.name}.{uuid.uuid4().hex[:8]}.part")
    try:
        if mount is not None and mount.is_file() and mount.stat().st_size == info.size:
            shutil.copyfile(mount, tmp)
        else:
            uris.backend().download(info.bucket, info.key, tmp, info.generation)
        if info.md5 and identity.gcs_identity(info.md5, info.size) != \
                f"md5:{_md5_hex(tmp)}:{tmp.stat().st_size}":
            raise IOError(f"download of {info.uri} is corrupt (MD5 mismatch); try again")
        os.replace(tmp, dest)
    finally:
        if tmp.exists():
            tmp.unlink()


def _md5_hex(path: Path) -> str:
    import hashlib

    h = hashlib.md5()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(8 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _record_origin(path: Path, uri: str) -> None:
    """Give a plain instrument file a sidecar with its gs:// origin, unless it
    already has one that describes exactly this content."""
    if path.suffix.lower() not in _RAW_TYPES:
        return
    if provenance.sidecar_path(path).exists():
        doc = provenance.read(path) or {}
        if (doc.get("product") or {}).get("content_id") == identity.file_identity(path) \
                and (doc.get("extra") or {}).get("origin") == uri:
            return
    record_source(path, tool=SPEC.name, origin=uri)


def _object(uri: str, dest: Path, args) -> Path:
    """Download one object to dest; returns the local path."""
    bucket, key = uris.parse_gcs(uri)
    mount = uris.mounted_path(uri)
    if args.no_copy and mount is not None:
        _say(args, f"{uri} is on a gcsfuse mount: {mount}")
        return mount
    info = uris.backend().stat(bucket, key)
    if info is None:
        raise FileNotFoundError(f"no such object: {uri}")
    side_info = uris.backend().stat(bucket, key + uris.SIDECAR_SUFFIX)
    side_dest = provenance.sidecar_path(dest)

    if args.dry_run:
        _say(args, f"would download {uri} ({info.size:,} bytes) -> {dest}")
        return dest.resolve()

    if not args.force and _same_content(dest, info):
        _say(args, f"reusing {dest} (identical to {uri}; --force downloads again)")
    else:
        _fetch(info, dest, mount)
        _say(args, f"downloaded {uri} -> {dest} ({info.size:,} bytes"
                   f"{', from the mount' if mount is not None else ''})")

    if side_info is not None:
        if args.force or not _same_content(side_dest, side_info):
            _fetch(side_info, side_dest, uris.mounted_path(uri + uris.SIDECAR_SUFFIX))
    else:
        # A plain instrument file: remember where it came from (as aa-raw does
        # for NCEI), replacing any sidecar left from a different file.
        _record_origin(dest, uri)
    return dest.resolve()


def _prefix_name(uri: str) -> str:
    bucket, key = uris.parse_gcs(uri)
    last = key.rstrip("/").rsplit("/", 1)[-1]
    try:
        return uris.safe_relpath(last) if last else bucket
    except ValueError:        # '..' or '.': not a usable folder name
        return bucket


def _prefix(uri: str, root: Path, args) -> Path:
    """Mirror everything under a prefix into the folder root."""
    bucket, key = uris.parse_gcs(uri)
    prefix = key.rstrip("/") + "/" if key.rstrip("/") else ""
    name = _prefix_name(uri)
    mount = uris.mounted_path(uri)
    if args.no_copy and mount is not None and mount.is_dir():
        _say(args, f"{uri} is on a gcsfuse mount: {mount}")
        return mount
    objects = [o for o in uris.backend().list(bucket, prefix)
               if not o.key.endswith("/") and o.key[len(prefix):]]
    if args.pattern:
        objects = [o for o in objects
                   if fnmatch.fnmatch(o.key.rsplit("/", 1)[-1], args.pattern)
                   or o.key.endswith(uris.SIDECAR_SUFFIX)
                   and fnmatch.fnmatch(o.key[:-len(uris.SIDECAR_SUFFIX)].rsplit("/", 1)[-1],
                                       args.pattern)]
    if not objects:
        what = "nothing under" if uri.endswith("/") else "no such object or prefix:"
        raise FileNotFoundError(f"{what} {uri}"
                                + (f" matching {args.pattern!r}" if args.pattern else ""))
    is_store = name.endswith(".zarr") or any(
        o.key[len(prefix):] in ("zarr.json", ".zgroup") for o in objects)
    fetched = kept = 0
    for obj in objects:
        target = root / uris.safe_relpath(obj.key[len(prefix):])
        if args.dry_run:
            continue
        if not args.force and _same_content(target, obj):
            kept += 1
            continue
        _fetch(obj, target, uris.mounted_path(obj.uri))
        fetched += 1
    total = sum(o.size for o in objects)
    if args.dry_run:
        _say(args, f"would download {len(objects)} objects ({total:,} bytes) under {uri} -> {root}")
        return root.resolve()
    removed = 0
    if is_store and not args.pattern:
        # Chunks the bucket no longer has would be read as data: remove them.
        removed = uris.prune_mirror(
            root, {uris.safe_relpath(o.key[len(prefix):]) for o in objects})
    _say(args, f"{uri} -> {root}: {fetched} downloaded, {kept} already identical"
               + (f", {removed} stale removed" if removed else "")
               + f" ({len(objects)} objects, {total:,} bytes)")
    if not is_store:
        # Instrument files without a sidecar in the bucket get one with their
        # origin. Nothing inside a .zarr store is touched.
        names = {o.key for o in objects}
        for obj in objects:
            rel = uris.safe_relpath(obj.key[len(prefix):])
            if obj.key.endswith(uris.SIDECAR_SUFFIX) or ".zarr/" in f"/{rel}" \
                    or obj.key + uris.SIDECAR_SUFFIX in names:
                continue
            _record_origin(root / rel, obj.uri)
    return root.resolve()


def main():
    if len(sys.argv) == 1 and not stdio.stdin_is_piped():
        print_help()
        sys.exit(0)
    parser = _build_parser()
    if show_help(SPEC, HELP, parser, full=print_help_full):
        sys.exit(0)
    args = parser.parse_args()

    tokens = stdio.many_inputs(args.uris, SPEC.name)
    bad = [t for t in tokens if not uris.is_gcs(t)]
    if bad:
        stdio.fail(SPEC.name, f"not a gs:// URI: {bad[0]} (aa-download only reads from GCS)", 2)
    if args.output and len(tokens) != 1:
        stdio.fail(SPEC.name, "-o/--output takes exactly one input; use --dest for several", 2)
    if args.output and args.dest:
        stdio.fail(SPEC.name, "use -o or --dest, not both", 2)

    dest_dir = Path(args.dest or ".").expanduser()
    code = 0
    for uri in tokens:
        try:
            bucket, key = uris.parse_gcs(uri)
            # A prefix: given with a trailing '/', or a name with no object
            # behind it (a .zarr store, a folder).
            mounted = uris.mounted_path(uri)
            is_prefix = (not key or key.endswith("/")
                         or (mounted is not None and mounted.is_dir())
                         or (mounted is None and uris.backend().stat(bucket, key) is None))
            if is_prefix:
                root = (Path(args.output).expanduser() if args.output
                        else dest_dir / _prefix_name(uri))
                stdio.emit(_prefix(uri, root, args))
                continue
            dest = (Path(args.output).expanduser() if args.output
                    else dest_dir / uris.safe_relpath(uris.basename(uri)))
            if dest.is_dir():                 # -o an existing folder: put it inside
                dest = dest / uris.safe_relpath(uris.basename(uri))
            stdio.emit(_object(uri, dest, args))
        except FileNotFoundError as exc:
            print(f"aa-download: {exc}", file=sys.stderr)
            code = 1
        except Exception as exc:  # credentials, permissions, disk full
            print(f"aa-download: cannot download {uri}: {exc}", file=sys.stderr)
            code = 1
    sys.exit(code)


if __name__ == "__main__":
    main()
