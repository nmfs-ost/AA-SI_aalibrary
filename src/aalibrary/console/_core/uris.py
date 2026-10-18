"""gs:// URIs as ordinary pipeline objects.

Every console tool can take ``gs://bucket/key`` wherever it takes a path,
and can write to ``gs://`` wherever it writes a file. The scientific code
still works on local files, so this module moves bytes at the edges:

``localize(uri)``
    Returns a local path for reading. In order of preference:
    1. a copy already in the download cache whose generation matches the
       object's current generation (e.g. just published by the previous
       stage),
    2. a gcsfuse mount that covers the object (no copy at all; your
       ``~/ggn-nmfs-aa-prod-1-data`` mount qualifies) when its size agrees
       with the object's,
    3. a fresh download into the cache, pinned to the generation that was
       looked up and checked against its MD5.
    A ``<key>.aa.json`` provenance sidecar next to the object comes along.
    A prefix (a Zarr store) is mirrored, and files the bucket no longer has
    are removed from the mirror.

``in_place(uri)``
    What to open to read a Zarr store where it is, with no copy: a gcsfuse
    mount that shows it, else the gs:// URI itself (read lazily through
    fsspec). Tools that can read a store in place (aa-combine, aa-sv) use
    this instead of ``localize``, so a store never has to fit on this disk.

``stage(uri)`` / ``publish(local, uri)``
    Writing to gs://: the tool writes to a local staging file, then
    ``publish`` uploads it (plus sidecar), stamps the product hash into the
    object's custom metadata, and moves the staged file into the download
    cache so the next pipeline stage reads it without downloading again.

Environment:
    AA_CACHE_DIR       download/staging cache (default ~/.cache/aalibrary).
                       Point it at a tmpfs to keep everything in RAM.
    AA_GCS_MOUNTS      explicit mounts, "bucket=/path[,bucket2=/path2]".
                       gcsfuse mounts in /proc/mounts are found automatically.
    AA_GCS_FAKE_ROOT   use a local directory as the object store (tests and
                       rehearsals). fsspec's gs:// is pointed at it too, so a
                       store read or written through fsspec (in place, as on
                       GCS) lands in the same folder and never reaches GCS.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import shutil
import tempfile
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator

SIDECAR_SUFFIX = ".aa.json"  # also defined in provenance; kept here to avoid a cycle
META_HASH = "aa-product-hash"
META_BASE = "aa-base"
META_TOOL = "aa-tool"
META_MD5 = "aa-content-md5"     # the MD5 the tool published; a rewrite changes the object's
META_RECIPE = "aa-recipe"       # the processing recipe (the <hash8> in the name, in full)
META_KIND = "aa-kind"           # what the product is (sv, mask, lines, ...): one tool can write several


# ---------------------------------------------------------------------------
# Parsing.
# ---------------------------------------------------------------------------

def is_gcs(value: object) -> bool:
    return isinstance(value, str) and value.startswith("gs://")


def is_remote(value: object) -> bool:
    return isinstance(value, str) and "://" in value and not value.startswith("file://")


def parse_gcs(uri: str) -> tuple[str, str]:
    """'gs://bucket/a/b.nc' -> ('bucket', 'a/b.nc'). The key may be '' or end in '/'."""
    if not is_gcs(uri):
        raise ValueError(f"not a gs:// URI: {uri!r}")
    rest = uri[len("gs://"):]
    bucket, _, key = rest.partition("/")
    if not bucket:
        raise ValueError(f"gs:// URI has no bucket: {uri!r}")
    return bucket, key


def join(uri: str, name: str) -> str:
    return uri.rstrip("/") + "/" + name.lstrip("/")


def basename(uri_or_path: str) -> str:
    if is_remote(uri_or_path):
        return uri_or_path.rstrip("/").rsplit("/", 1)[-1]
    return Path(uri_or_path).name


def to_uri(path: str | Path) -> str:
    """Local path -> file:// URI; URIs pass through."""
    s = str(path)
    if "://" in s:
        return s
    return Path(s).expanduser().resolve().as_uri()


def from_file_uri(value: str) -> str:
    if value.startswith("file://"):
        from urllib.parse import unquote, urlparse
        return unquote(urlparse(value).path)
    return value


# ---------------------------------------------------------------------------
# Object store backends.
# ---------------------------------------------------------------------------

@dataclass
class ObjectInfo:
    bucket: str
    key: str
    size: int
    md5: str | None = None          # base64, as GCS reports it
    crc32c: str | None = None
    generation: str | None = None
    metadata: dict = field(default_factory=dict)
    #: GCS storage class (STANDARD, NEARLINE, COLDLINE, ARCHIVE): what keeping it costs.
    storage_class: str | None = None

    @property
    def uri(self) -> str:
        return f"gs://{self.bucket}/{self.key}"

    def identity(self) -> str:
        """Content identity: md5 when GCS has one, else crc32c, else generation."""
        if self.md5:
            return f"gcs-md5:{self.md5}:{self.size}"
        if self.crc32c:
            return f"gcs-crc32c:{self.crc32c}:{self.size}"
        return f"gcs-gen:{self.bucket}/{self.key}#{self.generation}"


class _GcsBackend:
    """google-cloud-storage, with Application Default Credentials."""

    def __init__(self) -> None:
        from google.cloud import storage  # heavy; import on first use

        project = os.getenv("AALIBRARY_GCP_PROJECT_ID") or None
        self._client = storage.Client(project=project)

    def stat(self, bucket: str, key: str) -> ObjectInfo | None:
        blob = self._client.bucket(bucket).get_blob(key)
        if blob is None:
            return None
        return ObjectInfo(bucket, key, int(blob.size or 0), blob.md5_hash,
                          blob.crc32c, str(blob.generation), dict(blob.metadata or {}),
                          blob.storage_class)

    def list(self, bucket: str, prefix: str) -> Iterator[ObjectInfo]:
        for blob in self._client.list_blobs(bucket, prefix=prefix):
            yield ObjectInfo(bucket, blob.name, int(blob.size or 0), blob.md5_hash,
                             blob.crc32c, str(blob.generation), dict(blob.metadata or {}),
                             blob.storage_class)

    def download(self, bucket: str, key: str, dest: Path, generation: str | None = None) -> None:
        gen = int(generation) if generation and str(generation).isdigit() else None
        self._client.bucket(bucket).blob(key, generation=gen).download_to_filename(str(dest))

    def upload(self, src: Path, bucket: str, key: str, metadata: dict | None) -> ObjectInfo:
        blob = self._client.bucket(bucket).blob(key)
        if metadata:
            blob.metadata = {k: str(v) for k, v in metadata.items()}
        blob.upload_from_filename(str(src))
        blob.reload()
        return ObjectInfo(bucket, key, int(blob.size or 0), blob.md5_hash,
                          blob.crc32c, str(blob.generation), dict(blob.metadata or {}))

    def delete(self, bucket: str, key: str) -> None:
        self._client.bucket(bucket).blob(key).delete()


class _FakeBackend:
    """A directory standing in for GCS: <root>/<bucket>/<key>, metadata beside it.

    Behaves like GCS where it matters for the tools: md5 in base64,
    a generation that changes on every write, custom metadata.
    """

    def __init__(self, root: str) -> None:
        self.root = Path(root)

    def _path(self, bucket: str, key: str) -> Path:
        return self.root / bucket / key

    def _meta(self, bucket: str, key: str) -> Path:
        return self.root / ".meta" / bucket / (key + ".json")

    def stat(self, bucket: str, key: str) -> ObjectInfo | None:
        p = self._path(bucket, key)
        if not key or not p.is_file():
            return None
        meta = {}
        if self._meta(bucket, key).is_file():
            meta = json.loads(self._meta(bucket, key).read_text())
        md5 = base64.b64encode(hashlib.md5(p.read_bytes()).digest()).decode()
        return ObjectInfo(bucket, key, p.stat().st_size, md5, None,
                          str(meta.get("generation", p.stat().st_mtime_ns)),
                          dict(meta.get("metadata", {})),
                          str(meta.get("storageClass") or "STANDARD"))

    def list(self, bucket: str, prefix: str) -> Iterator[ObjectInfo]:
        base = self.root / bucket
        if not base.is_dir():
            return
        for p in sorted(base.rglob("*")):
            if p.is_file():
                key = p.relative_to(base).as_posix()
                if key.startswith(prefix):
                    info = self.stat(bucket, key)
                    if info:
                        yield info

    def download(self, bucket: str, key: str, dest: Path, generation: str | None = None) -> None:
        shutil.copyfile(self._path(bucket, key), dest)

    def delete(self, bucket: str, key: str) -> None:
        for p in (self._path(bucket, key), self._meta(bucket, key)):
            if p.is_file():
                p.unlink()

    def upload(self, src: Path, bucket: str, key: str, metadata: dict | None) -> ObjectInfo:
        p = self._path(bucket, key)
        p.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, p)
        m = self._meta(bucket, key)
        m.parent.mkdir(parents=True, exist_ok=True)
        m.write_text(json.dumps({"generation": str(time.time_ns()),
                                 "metadata": {k: str(v) for k, v in (metadata or {}).items()}}))
        info = self.stat(bucket, key)
        assert info is not None
        return info


_BACKEND = None


def backend():
    global _BACKEND
    if _BACKEND is None:
        fake = os.getenv("AA_GCS_FAKE_ROOT")
        if fake:
            install_fake_fsspec()
        _BACKEND = _FakeBackend(fake) if fake else _GcsBackend()
    return _BACKEND


# ---------------------------------------------------------------------------
# The stand-in bucket, for fsspec too.
# ---------------------------------------------------------------------------

def _fake_root() -> Path:
    root = os.getenv("AA_GCS_FAKE_ROOT")
    if not root:
        # Registered for a stand-in bucket that is no longer set: refuse
        # rather than fall through to anything that could reach GCS.
        raise RuntimeError("gs:// is the stand-in bucket, but AA_GCS_FAKE_ROOT is not set")
    return Path(root).expanduser().resolve()


_FAKE_FS = None


def install_fake_fsspec() -> None:
    """Point fsspec's gs:// (and gcs://) at AA_GCS_FAKE_ROOT.

    The tools read and write Zarr stores in place through fsspec, as gcsfs
    does on GCS. With the stand-in bucket those reads and writes must land
    in the same folder the object store above uses (<root>/<bucket>/<key>),
    and must never reach GCS: registering this filesystem for the gs://
    protocol (in place of gcsfs) does both. A local filesystem whose paths
    are the stand-in's; storage options meant for gcsfs are ignored.
    """
    global _FAKE_FS
    import fsspec

    if _FAKE_FS is None:
        from fsspec.implementations.local import LocalFileSystem

        class FakeGcsFileSystem(LocalFileSystem):
            protocol = ("gs", "gcs")

            def __init__(self, *args, **kwargs):  # noqa: ARG002 - gcsfs options
                super().__init__(auto_mkdir=True)

            @classmethod
            def _strip_protocol(cls, path):
                if isinstance(path, (list, tuple)):
                    return [cls._strip_protocol(p) for p in path]
                path = str(path)
                for prefix in ("gs://", "gcs://"):
                    if path.startswith(prefix):
                        key = path[len(prefix):].strip("/")
                        return str(_fake_root() / safe_relpath(key)) if key else str(_fake_root())
                return LocalFileSystem._strip_protocol(path)

            def unstrip_protocol(self, name):
                name, root = str(name), str(_fake_root())
                if name == root or name.startswith(root + "/"):
                    return "gs://" + name[len(root):].lstrip("/")
                return name

        _FAKE_FS = FakeGcsFileSystem
    for protocol in ("gs", "gcs"):
        fsspec.register_implementation(protocol, _FAKE_FS, clobber=True)


if os.getenv("AA_GCS_FAKE_ROOT"):
    try:
        install_fake_fsspec()
    except ImportError:  # pragma: no cover - fsspec comes with xarray/echopype
        pass


def is_store(value: object) -> bool:
    """A Zarr store, by name: '<anything>.zarr' (local or gs://)."""
    return isinstance(value, (str, Path)) and str(value).rstrip("/").lower().endswith(".zarr")


def in_place(uri: str) -> str:
    """What to open to read a gs:// store (or object) where it is, with no copy.

    A gcsfuse mount that shows it, else the URI itself, for fsspec (gcsfs on
    GCS; the stand-in folder under AA_GCS_FAKE_ROOT). Nothing is downloaded.
    """
    mounted = mounted_path(uri)
    if mounted is not None:
        return str(mounted)
    if os.getenv("AA_GCS_FAKE_ROOT"):
        install_fake_fsspec()
    return uri


def store_meta_info(uri: str) -> ObjectInfo | None:
    """The root metadata object of a gs:// Zarr store (zarr.json, or .zattrs
    for Zarr 2), which is where publish stamps the product hash; None if
    the store has none (absent, or never finished)."""
    for name in ("zarr.json", ".zattrs"):
        try:
            info = stat(join(uri, name))
        except Exception:  # noqa: BLE001 - unreadable is "unknown"
            info = None
        if info is not None:
            return info
    return None


def evict(uri: str) -> int:
    """Drop the download cache's copy of a gs:// object (and its sidecar);
    bytes freed. A mount or the bucket is never touched."""
    bucket, key = parse_gcs(uri)
    try:
        path = _cache_path(bucket, key.rstrip("/"))
    except ValueError:
        return 0
    freed = 0
    for item in (path, path.with_name(path.name + SIDECAR_SUFFIX)):
        try:
            if item.is_dir() and not item.is_symlink():
                freed += sum(p.stat().st_size for p in item.rglob("*") if p.is_file())
                shutil.rmtree(item, ignore_errors=True)
            elif item.exists():
                freed += item.stat().st_size
                item.unlink()
        except OSError:
            continue
        record = _cache_record(item)
        if record.is_file():
            try:
                record.unlink()
            except OSError:
                pass
    return freed


def stat(uri: str) -> ObjectInfo | None:
    bucket, key = parse_gcs(uri)
    return backend().stat(bucket, key) if key else None


# ---------------------------------------------------------------------------
# Mounts and the local cache.
# ---------------------------------------------------------------------------

def cache_dir() -> Path:
    root = os.getenv("AA_CACHE_DIR")
    if not root:
        xdg = os.getenv("XDG_CACHE_HOME") or str(Path.home() / ".cache")
        root = str(Path(xdg) / "aalibrary")
    p = Path(root).expanduser()
    p.mkdir(parents=True, exist_ok=True)
    return p


def _mounts() -> dict[str, list[Path]]:
    out: dict[str, list[Path]] = {}
    for item in filter(None, (os.getenv("AA_GCS_MOUNTS") or "").split(",")):
        bucket, _, path = item.partition("=")
        if bucket and path:
            out.setdefault(bucket.strip(), []).append(Path(path.strip()).expanduser())
    try:
        with open("/proc/mounts", encoding="utf-8") as fh:
            for line in fh:
                parts = line.split()
                if len(parts) >= 3 and parts[2] == "fuse.gcsfuse":
                    out.setdefault(parts[0], []).append(Path(parts[1].replace("\\040", " ")))
    except OSError:
        pass
    return out


def mounted_path(uri: str) -> Path | None:
    """A path on a gcsfuse mount that shows this object, if there is one.

    A mount made with --only-dir shows a sub-folder at its root, which
    /proc/mounts doesn't reveal; requiring the path to exist rules out a
    wrong match.
    """
    bucket, key = parse_gcs(uri)
    try:
        key = safe_relpath(key)
    except ValueError:
        return None
    for root in _mounts().get(bucket, []):
        candidate = root / key
        if key and candidate.exists():
            return candidate
    return None


@dataclass
class Localized:
    path: Path              # local path to read
    uri: str                # the gs:// URI it came from
    info: ObjectInfo | None # object metadata (None when read through a mount)
    via: str                # "mount" | "cache" | "download"


def safe_relpath(key: str) -> str:
    """An object key as a relative local path that cannot leave its folder.

    GCS keys may contain '..' or start with '/'; those parts are dropped
    rather than followed.
    """
    parts = [p for p in key.replace("\\", "/").split("/") if p not in ("", ".", "..")]
    if not parts:
        raise ValueError(f"object key has no usable name: {key!r}")
    return "/".join(parts)


def _cache_path(bucket: str, key: str) -> Path:
    return cache_dir() / "objects" / safe_relpath(bucket) / safe_relpath(key)


def _cache_record(path: Path) -> Path:
    """Bookkeeping for a cached object, kept OUTSIDE the mirrored tree (a
    record inside a cached .zarr store would become part of the store)."""
    try:
        rel = path.resolve().relative_to((cache_dir() / "objects").resolve())
        return cache_dir() / "records" / (rel.as_posix() + ".json")
    except ValueError:
        return path.with_name(f".{path.name}.aa-object.json")


def _is_placeholder(key: str) -> bool:
    """Folder placeholder objects ('dir/') that consoles and gcsfuse create."""
    return key.endswith("/")


def _is_store_root(path: Path) -> bool:
    return path.name.endswith(".zarr") or (path / "zarr.json").is_file() \
        or (path / ".zgroup").is_file()


def localize(uri: str, *, use_mount: bool = True) -> Localized:
    """Make a gs:// object (or prefix, e.g. a .zarr store) readable locally."""
    bucket, key = parse_gcs(uri)
    m = mounted_path(uri) if use_mount else None
    try:
        info = backend().stat(bucket, key) if key and not key.endswith("/") else None
    except Exception:
        if m is None:
            raise
        # No API access (credentials, network) but the mount shows it.
        return Localized(m, uri, None, "mount")
    if info is None:
        if m is not None:
            # The mount shows it (a folder such as a .zarr store, or a file
            # the API doesn't list for us): read it there.
            return Localized(m, uri, None, "mount")
        # A prefix: a Zarr store or a folder. Mirror the tree.
        prefix = key.rstrip("/") + "/"
        dest = _cache_path(bucket, key.rstrip("/"))
        wanted: set[str] = set()
        for obj in backend().list(bucket, prefix):
            if _is_placeholder(obj.key) or not obj.key[len(prefix):]:
                continue
            rel = safe_relpath(obj.key[len(prefix):])
            wanted.add(rel)
            target = dest / rel
            if _fresh(target, obj):
                continue
            _download(obj, target)
        if not wanted:
            raise FileNotFoundError(f"No such object or prefix: {uri}")
        prune_mirror(dest, wanted)
        return Localized(dest, uri, None, "download")

    dest = _cache_path(bucket, key)
    if _fresh(dest, info):
        via = "cache"
    elif m is not None and m.is_file() and m.stat().st_size == info.size:
        return Localized(m, uri, info, "mount")
    else:
        _download(info, dest)
        via = "download"
    side = backend().stat(bucket, key + SIDECAR_SUFFIX)
    side_dest = dest.with_name(dest.name + SIDECAR_SUFFIX)
    if side is not None:
        if not _fresh(side_dest, side):
            _download(side, side_dest)
    elif side_dest.exists():
        side_dest.unlink()          # the bucket no longer has one: don't keep a stale copy
    return Localized(dest, uri, info, via)


def _fresh(path: Path, info: ObjectInfo) -> bool:
    rec = _cache_record(path)
    if not (path.exists() and rec.is_file()):
        return False
    try:
        data = json.loads(rec.read_text())
    except (OSError, ValueError):
        return False
    return data.get("generation") == info.generation and data.get("size") == info.size


def prune_mirror(root: Path, wanted: set[str]) -> int:
    """Delete files under a mirrored store that the bucket no longer has.

    Zarr omits chunks that are entirely fill value, so a rewritten store can
    have fewer chunks than the old one; leftovers would be read as data.
    """
    removed = 0
    if not root.is_dir():
        return 0
    for p in sorted(root.rglob("*"), reverse=True):
        if p.is_file():
            rel = p.relative_to(root).as_posix()
            if rel not in wanted and not p.name.endswith(".part"):
                p.unlink()
                rec = _cache_record(p)
                if rec.is_file():
                    rec.unlink()
                removed += 1
        elif p.is_dir():
            try:
                p.rmdir()          # only if now empty
            except OSError:
                pass
    return removed


def _md5_b64_of(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(8 * 1024 * 1024), b""):
            h.update(chunk)
    return base64.b64encode(h.digest()).decode()


def _download(info: ObjectInfo, dest: Path) -> None:
    """Fetch exactly the generation that was looked up, and check its MD5."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(f".{dest.name}.{uuid.uuid4().hex[:8]}.part")
    try:
        backend().download(info.bucket, info.key, tmp, info.generation)
        if info.md5 and _md5_b64_of(tmp) != info.md5:
            raise IOError(f"download of {info.uri} is corrupt (MD5 mismatch); try again")
        os.replace(tmp, dest)
    finally:
        if tmp.exists():
            tmp.unlink()
    rec = _cache_record(dest)
    rec.parent.mkdir(parents=True, exist_ok=True)
    rec.write_text(json.dumps(
        {"uri": info.uri, "generation": info.generation, "size": info.size, "md5": info.md5}))


# ---------------------------------------------------------------------------
# Writing to gs://.
# ---------------------------------------------------------------------------

def stage(uri: str) -> Path:
    """A local path to write before ``publish`` uploads it to ``uri``."""
    name = basename(uri) or "output"
    d = Path(tempfile.mkdtemp(prefix="stage-", dir=str(cache_dir())))
    return d / name


_STORE_ROOT_META = {"zarr.json", ".zattrs", ".zgroup", ".zmetadata"}


def publish_tree(local: Path, uri: str, *, metadata: dict | None = None,
                 prune: bool | None = None) -> tuple[int, int, int]:
    """Upload a folder under ``uri``/; returns (uploaded, unchanged, deleted).

    Files whose MD5 already matches the object are skipped. For a Zarr store
    (prune defaults to True) objects the local store doesn't have are
    deleted, so a rewritten store is never mixed with the old one, and the
    root metadata goes up last, so a half-finished upload doesn't look like
    a complete store. ``metadata`` is stamped on the root metadata object.

    Replacing a store: its old root metadata is deleted first and the old
    objects the new store does not have are deleted before the new root goes
    up. A replacement stopped at any point leaves a store with no root
    metadata (unreadable, and never taken for a finished product) rather than
    the old product's stamp over a body that is partly new, or the new
    product's stamp over leftover chunks of the old one.
    """
    bucket, key = parse_gcs(uri)
    prefix = key.rstrip("/") + "/"
    local = Path(local)
    is_store = _is_store_root(local)
    prune = is_store if prune is None else prune
    remote = {o.key[len(prefix):]: o for o in backend().list(bucket, prefix)
              if not _is_placeholder(o.key)}
    files = sorted((p for p in local.rglob("*") if p.is_file() and not p.name.endswith(".part")),
                   key=lambda p: p.relative_to(local).as_posix())
    if not files:
        raise FileNotFoundError(f"nothing to upload in {local}")
    root_meta = [p for p in files if p.parent == local and p.name in _STORE_ROOT_META]
    body = [p for p in files if p not in root_meta]
    deleted = 0
    if is_store:
        # The old product's root goes first: from here until the new root is
        # up, the store is visibly unfinished. (Counted as deleted only when
        # the new store has no such object.)
        replaced = {p.name for p in root_meta}
        for rel in sorted(r for r in remote if "/" not in r and r in _STORE_ROOT_META):
            backend().delete(bucket, prefix + rel)
            remote.pop(rel)
            deleted += rel not in replaced
    uploaded = unchanged = 0
    for p in body:
        rel = p.relative_to(local).as_posix()
        obj = remote.get(rel)
        if obj is not None and obj.md5 and obj.md5 == _md5_b64_of(p):
            unchanged += 1
            continue
        backend().upload(p, bucket, prefix + rel, None)
        uploaded += 1
    if prune:
        keep = {p.relative_to(local).as_posix() for p in files}
        for rel in sorted(set(remote) - keep):
            backend().delete(bucket, prefix + rel)
            deleted += 1
    for p in root_meta:
        rel = p.relative_to(local).as_posix()
        stamp = metadata if p.name in ("zarr.json", ".zattrs") else None
        obj = remote.get(rel)
        if obj is not None and obj.md5 and obj.md5 == _md5_b64_of(p) and not stamp:
            unchanged += 1
            continue
        backend().upload(p, bucket, prefix + rel, stamp)
        uploaded += 1
    return uploaded, unchanged, deleted


def publish(local: Path, uri: str, *, metadata: dict | None = None,
            keep_in_cache: bool = True) -> ObjectInfo | None:
    """Upload a finished local file (and its sidecar) to ``uri``.

    A folder (Zarr store) goes through ``publish_tree``.
    """
    bucket, key = parse_gcs(uri)
    local = Path(local)
    if local.is_dir():
        publish_tree(local, uri, metadata=metadata)
        side = local.with_name(local.name + SIDECAR_SUFFIX)
        if side.is_file():
            backend().upload(side, bucket, key.rstrip("/") + SIDECAR_SUFFIX, None)
        return None
    if not key or key.endswith("/"):
        raise ValueError(f"publish needs a full object URI, got {uri!r}")
    info = backend().upload(local, bucket, key, metadata)
    side = local.with_name(local.name + SIDECAR_SUFFIX)
    if side.is_file():
        backend().upload(side, bucket, key + SIDECAR_SUFFIX, None)
    if keep_in_cache:
        dest = _cache_path(bucket, key)
        dest.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.replace(local, dest)
            if side.is_file():
                os.replace(side, dest.with_name(dest.name + SIDECAR_SUFFIX))
            rec = _cache_record(dest)
            rec.parent.mkdir(parents=True, exist_ok=True)
            rec.write_text(json.dumps(
                {"uri": info.uri, "generation": info.generation, "size": info.size, "md5": info.md5}))
        except OSError:
            pass
    return info


__all__ = [
    "SIDECAR_SUFFIX", "META_HASH", "META_BASE", "META_TOOL", "META_MD5", "META_RECIPE", "META_KIND",
    "is_gcs", "is_remote", "parse_gcs", "join", "basename", "to_uri", "from_file_uri",
    "ObjectInfo", "backend", "stat", "cache_dir", "mounted_path", "Localized", "safe_relpath",
    "localize", "stage", "publish", "publish_tree", "prune_mirror", "install_fake_fsspec",
    "is_store", "in_place", "store_meta_info", "evict",
]
