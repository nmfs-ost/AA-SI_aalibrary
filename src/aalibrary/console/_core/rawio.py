"""Reading a raw file into EchoData without holding the expanded data in memory.

``echopype.open_raw`` parses a file's pings and then expands them into
rectangular float64 arrays (every ping padded to the longest), held in memory
by default. That is several times the file's size: a 1 GB EK80 file needs
more memory than many workstations have, and the system kills the process
(exit -9) with no error of its own. echopype can park those arrays on disk
instead (``use_swap``: a temporary Zarr store, read lazily from then on), but
only when asked, and it puts that store in the system's temporary folder and
does not reliably delete it.

Swap alone does not bound the memory, though: echopype 0.11 pads each
channel's pings into one whole array, and copies it to float64, before it
writes it to the swap store (a 1 GB EK60 file passed 5.5 GB there either way).

``open_raw`` here asks for swap for any file of ``SWAP_ABOVE_BYTES`` or more
(and leaves smaller ones to echopype's own judgement of the memory free), and
while it swaps, pads and writes each channel a block of pings at a time
(``BLOCK_BYTES``), handing each block's pings back as it goes: the same
values in the same arrays as echopype's swap, with the parsed pings plus one
block in memory. It puts the swap in the tools' cache (``AA_CACHE_DIR``: in a
Workbench run, the run's own working folder, so it is counted and removed
with it), and deletes it once the caller is done. Swap changes where the
values wait, never the values: it is not part of any product's identity.
"""

from __future__ import annotations

import contextlib
import os
import shutil
import tempfile
from pathlib import Path
from typing import Iterator

from . import uris

#: Files at least this large are always expanded on disk, not in memory.
SWAP_ABOVE_BYTES = 200 * 1000 * 1000

SWAP_CHOICES = ("auto", "on", "off")

#: Pings padded and written to the swap at a time, as float64 bytes.
BLOCK_BYTES = 64 * 1000 * 1000


def use_swap_for(raw_path: str | Path, swap: str = "auto") -> bool | str:
    """What to pass echopype's ``use_swap`` for this file."""
    if swap == "on":
        return True
    if swap == "off":
        return False
    try:
        size = Path(raw_path).stat().st_size
    except OSError:
        size = 0
    return True if size >= SWAP_ABOVE_BYTES else "auto"


def _pad_block(block: list):
    """Pings of one block as one array, shorter pings NaN-padded: echopype's
    pad_shorter_ping (complex stays complex64, with NaN+0j padding; anything
    else becomes float64 with NaN padding when lengths differ), for any block,
    including one whose first ping is empty. None when it holds no data."""
    import numpy as np

    present = [a for a in block if a is not None and a.size]
    if not present:
        return None
    ndim = max(a.ndim for a in present)
    shape = tuple(max(a.shape[d] for a in present if a.ndim == ndim) for d in range(ndim))
    complex_data = np.iscomplexobj(present[0])
    if all(a is not None and a.shape == shape for a in block):
        return np.array(block)  # echopype keeps the type when no ping is short
    out = np.full((len(block),) + shape, np.nan,
                  dtype=np.complex64 if complex_data else np.float64)
    for i, a in enumerate(block):
        if a is not None and a.size:
            out[(i,) + tuple(slice(0, k) for k in a.shape)] = a
    return out


def _blockwise(original):
    """echopype's ParseEK._parse_and_pad_datagram with swap, a block at a time.

    The same arrays as echopype's swap writes (shape, chunks, dtype float64,
    NaN fill, the power factor, complex split with imaginary zeros as NaN);
    without swap, echopype's own."""

    def _parse_and_pad_datagram(self, data_type, data_type_shapes={}, raw_type="receive",
                                use_swap=False, zarr_root=None, max_chunk_size="100MB"):
        if not use_swap:
            return original(self, data_type, data_type_shapes, raw_type, use_swap, zarr_root,
                            max_chunk_size)
        import dask.array as da
        import numpy as np
        from dask.array.core import auto_chunks
        from echopype.convert import parse_base as pb

        ping_data_dict = (self.ping_data_dict_tx if raw_type == "transmit"
                          else self.ping_data_dict)
        if data_type_shapes[data_type] is None:
            ping_data_dict[data_type] = {ch: None for ch in ping_data_dict[data_type].keys()}
            return
        if zarr_root is None:
            raise ValueError("zarr_root cannot be None when use_swap is True")
        data_shape = data_type_shapes[data_type]
        chunks = ("auto",) + (data_shape[1],)
        chunks = auto_chunks(chunks=chunks, shape=data_shape, limit=max_chunk_size,
                             dtype=np.dtype("float64"))
        chunks = chunks + data_shape[2:]
        chunks = tuple(c[0] if isinstance(c, tuple) else c for c in chunks)

        for ch_id, arr_list in ping_data_dict[data_type].items():
            if all((a is None) or (a.size == 0) for a in arr_list):
                ping_data_dict[data_type][ch_id] = None
                continue
            if raw_type == "receive":
                self.ch_ids[data_type].append(ch_id)
            if data_shape[0] != len(self.ping_time[ch_id]):
                data_shape = (len(self.ping_time[ch_id]),) + data_shape[1:]
            safe_ch = pb._sanitize_component(ch_id)
            parts = ("real", "imag") if data_type == "complex" else (None,)
            stores = {}
            for part in parts:
                path = (os.path.join(raw_type, data_type, safe_ch, pb._sanitize_component(part))
                        if part else os.path.join(raw_type, data_type, safe_ch))
                stores[part] = zarr_root.full(name=path, shape=data_shape, chunks=chunks,
                                              dtype="f8", fill_value=np.nan,
                                              config={"write_empty_chunks": False})
            per_ping = 8 * max(1, int(np.prod(data_shape[1:])))
            step = max(1, BLOCK_BYTES // per_ping)
            for start in range(0, len(arr_list), step):
                block = arr_list[start:start + step]
                padded = _pad_block(block)
                if padded is not None:
                    if data_type == "power":
                        padded = padded.astype("float32") * pb.INDEX2POWER
                    if data_type == "complex":
                        imag = np.imag(padded).astype("float64")
                        values = {"real": np.real(padded).astype("float64"),
                                  "imag": np.where(imag == 0, np.nan, imag)}
                    else:
                        values = {None: padded}
                    for part, arr in values.items():
                        region = ((slice(start, start + arr.shape[0]),)
                                  + tuple(slice(0, k) for k in arr.shape[1:]))
                        stores[part].set_basic_selection(region, arr)
                # The parsed pings of this block are not needed any more.
                arr_list[start:start + step] = [None] * len(block)
            if data_type == "complex":
                ping_data_dict[data_type][ch_id] = {p: da.from_zarr(z) for p, z in stores.items()}
            else:
                ping_data_dict[data_type][ch_id] = da.from_zarr(stores[None])

    return _parse_and_pad_datagram


@contextlib.contextmanager
def _padding_in_blocks() -> Iterator[None]:
    """ParseEK._parse_and_pad_datagram replaced while a file is read, when the
    installed echopype has the internals this mirrors (0.11); else unchanged."""
    try:
        from echopype.convert import parse_base as pb

        original = pb.ParseEK._parse_and_pad_datagram
        needed = all(hasattr(pb, n) for n in ("INDEX2POWER", "_sanitize_component"))
    except (ImportError, AttributeError):
        needed, original, pb = False, None, None
    if not needed:
        yield
        return
    pb.ParseEK._parse_and_pad_datagram = _blockwise(original)
    try:
        yield
    finally:
        pb.ParseEK._parse_and_pad_datagram = original


@contextlib.contextmanager
def open_raw(raw_path: str | Path, sonar_model: str, *, swap: str = "auto") -> Iterator:
    """``echopype.open_raw``, with swap as above; the swap is deleted on exit.

    Use the EchoData (write it out) inside the ``with``: once it closes, a
    swapped EchoData no longer has its data.
    """
    import echopype as ep
    import echopype.utils.io as epio

    use = use_swap_for(raw_path, swap)
    folder = None
    previous = getattr(epio, "ECHOPYPE_TEMP_DIR", None)
    if use:
        # echopype makes its swap store under this module-level folder.
        folder = Path(tempfile.mkdtemp(prefix="aa-swap-", dir=str(uris.cache_dir())))
        epio.ECHOPYPE_TEMP_DIR = folder
    echodata = None
    try:
        with _padding_in_blocks() if use else contextlib.nullcontext():
            echodata = ep.open_raw(raw_file=str(raw_path), sonar_model=sonar_model,
                                   use_swap=use)
        yield echodata
    finally:
        if folder is not None:
            epio.ECHOPYPE_TEMP_DIR = previous
            if echodata is not None:
                # Its destructor would try to delete the swap again, which is
                # gone by then; it skips that for an EchoData that was saved.
                echodata.converted_raw_path = str(raw_path)
            shutil.rmtree(folder, ignore_errors=True)


__all__ = ["SWAP_ABOVE_BYTES", "SWAP_CHOICES", "BLOCK_BYTES", "use_swap_for", "open_raw"]
