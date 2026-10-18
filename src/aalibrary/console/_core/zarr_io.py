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
        spec = encoding(dataset) if encoding is not None else echopype_encoding(dataset, compress)
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


__all__ = ["KEEP_ENCODING", "clean_encoding", "echopype_encoding", "groups_of",
           "write_echodata", "consolidate_store"]
