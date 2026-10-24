"""Writing an EchoData as a Zarr store: a local folder or a gs:// URI.

echopype's ``EchoData.to_zarr`` names a store by an fsspec mapper and then
hands xarray the mapper's *root*, which for a remote URL is the path with the
protocol taken off: ``to_zarr("gs://bucket/x.zarr")`` writes a local folder
``bucket/x.zarr`` in the current directory and nothing to the bucket
(echopype 0.11). This writes the same store the same way — group by group,
Top-level first, with echopype's own per-variable encodings (codec and chunk
shape) — but gives xarray the URL itself and its storage options, so a gs://
store is written in place (gcsfs on GCS; the stand-in folder under
AA_GCS_FAKE_ROOT) and a local one is written where it was named.

Encodings a variable carries from where it was read (a Zarr 3 store's
``compressors``, a NetCDF file's ``zlib``) are dropped first: the output's
codec is chosen here, and xarray refuses a variable that names two.

Codecs are spelled the way the installed zarr takes them. echopype 0.11 names
its codec ``compressor`` (Zarr 2's spelling), which xarray 2026.9 no longer
accepts: "unexpected encoding parameters for zarr backend: ['compressor']",
from echopype's own ``to_zarr`` too. With zarr 3 it becomes
``compressors: [codec]``, as a zarr 3 codec (echopype already gives one; an
older numcodecs codec is translated); with zarr 2 it stays as it is.
"""

from __future__ import annotations

import os
from typing import Callable

#: Encoding a variable may keep from its source: how its values are stored
#: (type, fill, units, time calendar, packing), never how they were compressed.
KEEP_ENCODING = {"dtype", "_FillValue", "units", "calendar", "scale_factor", "add_offset"}


def _is_remote(store: str) -> bool:
    return "://" in str(store) and not str(store).startswith("file://")


def clean_encoding(dataset) -> None:
    """Drop source-specific encoding (codecs, chunking) from every variable."""
    for name in list(dataset.variables):
        encoding = dataset[name].encoding
        for key in [k for k in encoding if k not in KEEP_ENCODING]:
            encoding.pop(key, None)


def echopype_encoding(dataset, compress: bool = True) -> dict:
    """The encodings echopype's own writer would use for this group."""
    from echopype.utils.coding import COMPRESSION_SETTINGS, set_storage_encodings

    return set_storage_encodings(dataset, COMPRESSION_SETTINGS["zarr"] if compress else None,
                                 "zarr")


def _zarr_major() -> int:
    import zarr

    try:
        return int(str(zarr.__version__).split(".")[0])
    except ValueError:
        return 3


def as_zarr3_codec(codec):
    """A codec as zarr 3 takes it in ``compressors``: zarr 3 codecs as they
    are; a numcodecs codec (Zarr 2) translated to its zarr 3 equivalent."""
    from zarr.abc.codec import BytesBytesCodec

    if isinstance(codec, BytesBytesCodec):
        return codec
    import zarr.codecs

    config = dict(codec.get_config()) if hasattr(codec, "get_config") else {}
    name = str(config.pop("id", getattr(codec, "codec_id", "")))
    if name == "blosc":
        cname = config.get("cname", "zstd")
        cname = cname.decode() if isinstance(cname, bytes) else str(cname)
        # numcodecs: 0 none, 1 byte, 2 bit, -1 automatic (zarr 3: None).
        shuffle = {0: "noshuffle", 1: "shuffle", 2: "bitshuffle"}.get(config.get("shuffle"))
        return zarr.codecs.BloscCodec(cname=cname, clevel=int(config.get("clevel", 5)),
                                      shuffle=shuffle, blocksize=int(config.get("blocksize", 0)))
    if name == "zstd":
        return zarr.codecs.ZstdCodec(level=int(config.get("level", 0)))
    if name in ("zlib", "gzip"):
        return zarr.codecs.GzipCodec(level=int(config.get("level", 5)))
    try:  # numcodecs' own zarr 3 wrappers (numcodecs >= 0.14)
        import numcodecs.zarr3 as wrappers

        return getattr(wrappers, type(codec).__name__)(**config)
    except Exception as exc:  # noqa: BLE001 - say which codec, not a TypeError deep in zarr
        raise ValueError(f"no zarr 3 form of the codec {codec!r}") from exc


def zarr_codecs(spec: dict) -> dict:
    """Each variable's codec spelled the way the installed zarr takes it:
    ``compressor`` (Zarr 2's word, which echopype 0.11 uses and xarray
    2026.9 refuses) becomes ``compressors: [codec]`` with zarr 3."""
    if _zarr_major() < 3:
        return spec
    for item in spec.values():
        if isinstance(item, dict) and "compressor" in item:
            codec = item.pop("compressor")
            item.setdefault("compressors", None if codec is None else [as_zarr3_codec(codec)])
    return spec


#: Bytes of float64 per chunk of a ping_time dataset written here (one
#: channel, a run of pings, every sample): small enough to compute one at a
#: time, large enough that a bucket store is not millions of objects.
CHUNK_BYTES = 64 * 1000 * 1000


def ping_chunk_for(dataset, chunk_bytes: int = CHUNK_BYTES) -> int:
    """Pings per chunk for a channel x ping_time x range_sample dataset."""
    depth = int(dataset.sizes.get("range_sample", 1) or 1)
    return max(1, int(chunk_bytes // (8 * depth)))


def chunked_encoding(dataset, ping_chunk: int, compress: bool = True) -> dict:
    """Per-variable encoding: one channel, ``ping_chunk`` pings and the whole
    of every other axis per chunk; echopype's codec for the dtype (none when
    ``compress`` is False), spelled for the installed zarr."""
    try:
        from echopype.utils.coding import COMPRESSION_SETTINGS, get_zarr_compression
    except Exception:  # noqa: BLE001 - internals may move: the backend's default codec
        COMPRESSION_SETTINGS, get_zarr_compression = None, None
    spec: dict = {}
    for name, variable in dataset.variables.items():
        item: dict = {}
        if not compress:
            item["compressor"] = None
        elif get_zarr_compression is not None:
            try:
                item.update(get_zarr_compression(variable, COMPRESSION_SETTINGS["zarr"]))
            except Exception:  # noqa: BLE001
                pass
        if variable.ndim:
            item["chunks"] = tuple(
                ping_chunk if dim == "ping_time" else (1 if dim == "channel" else size)
                for dim, size in variable.sizes.items())
        if item:
            spec[str(name)] = item
    return zarr_codecs(spec)


def groups_of(echodata) -> list[str]:
    """Top-level first, then every other group echopype has, in its order."""
    paths = [g for g in (getattr(echodata, "group_paths", None) or []) if g != "Top-level"]
    return ["Top-level", *paths]


def write_echodata(echodata, store: str, *, storage_options: dict | None = None,
                   overwrite: bool = True, compress: bool = True,
                   encoding: Callable | None = None, consolidate: bool = True,
                   on_group: Callable[[int, int], None] | None = None) -> None:
    """Write every group of ``echodata`` to the Zarr store ``store``.

    ``encoding(dataset) -> dict``: the per-variable encoding of a group
    (default: echopype's). Dask arrays are rechunked to the encoding's chunk
    shape, as echopype's writer does, since Zarr refuses chunks that differ.
    ``on_group(done, total)`` after each group (progress).
    """
    remote = _is_remote(store)
    if remote and str(store).startswith(("gs://", "gcs://")) and os.getenv("AA_GCS_FAKE_ROOT"):
        from . import uris

        uris.install_fake_fsspec()   # the stand-in bucket, never GCS
    options = (storage_options or None) if remote else None
    groups = groups_of(echodata)
    total = len(groups)
    first = True
    for index, group in enumerate(groups, start=1):
        try:
            dataset = echodata[group]
        except Exception:  # noqa: BLE001 - a group this sonar does not have
            dataset = None
        if dataset is None:
            continue
        clean_encoding(dataset)
        spec = zarr_codecs(
            encoding(dataset) if encoding is not None else echopype_encoding(dataset, compress)
        )
        for name, item in spec.items():
            chunks = item.get("chunks")
            if chunks and hasattr(dataset[name].data, "chunks"):
                dims = dataset[name].dims
                dataset[name] = dataset[name].chunk(
                    dict(zip(dims, chunks)) if isinstance(chunks, (tuple, list)) else chunks)
        mode = ("w" if overwrite else "w-") if first else "a"
        dataset.to_zarr(store=str(store), mode=mode,
                        group=None if group == "Top-level" else group,
                        encoding=spec, consolidated=False, storage_options=options)
        first = False
        if on_group is not None:
            on_group(index, total)
    if consolidate:
        consolidate_store(store, storage_options)


def consolidate_store(store: str, storage_options: dict | None = None) -> None:
    """Consolidated metadata: one read opens the whole store (on GCS, one
    request instead of one per array), which is what makes opening many
    stores in place cheap."""
    import zarr

    if _is_remote(store):
        import fsspec

        zarr.consolidate_metadata(fsspec.get_mapper(str(store), **(storage_options or {})))
    else:
        zarr.consolidate_metadata(str(store))


__all__ = ["KEEP_ENCODING", "CHUNK_BYTES", "clean_encoding", "echopype_encoding", "groups_of",
           "as_zarr3_codec", "zarr_codecs", "ping_chunk_for", "chunked_encoding",
           "write_echodata", "consolidate_store"]
