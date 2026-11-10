"""Helpers for tools that read and write flat (non-EchoData) datasets.

Sv, TS, MVBS, masks: one xarray Dataset with (channel,) ping_time and a range
dimension. The operator tools (aa-crop, aa-mask, aa-threshold, aa-tiles,
aa-integrate) share how they open such a product, which dimensions they treat
as time and range, which variables are axes (never masked), and how they write
the result.
"""

from __future__ import annotations

import io
from contextlib import redirect_stdout
from pathlib import Path

#: Variables that describe the axes; masking or thresholding them breaks the
#: coordinate system (aa-evl learned this first).
AXIS_VARS = frozenset({
    "echo_range", "depth", "range", "range_m", "range_meter", "range_sample",
    "latitude", "longitude", "ping_time", "frequency_nominal",
})
TIME_DIMS = ("ping_time", "time")
RANGE_DIMS = ("range_sample", "echo_range", "depth", "range_bin")
#: The variable a product's values are in, by preference.
VALUE_VARS = ("Sv", "Sv_corrected", "MVBS", "TS", "NASC")


def open_flat(path: str | Path, *, chunks: dict | None = None):
    """Open a flat NetCDF or Zarr product (lazily)."""
    import xarray as xr

    path = Path(path)
    quiet = io.StringIO()
    with redirect_stdout(quiet):
        if path.is_dir() or path.suffix.lower() == ".zarr":
            return xr.open_zarr(str(path), chunks=chunks or {})
        return xr.open_dataset(str(path), chunks=chunks)


def is_echodata(path: str | Path) -> bool:
    """True for an EchoData file (groups, no flat variables)."""
    try:
        ds = open_flat(path)
    except Exception:  # noqa: BLE001
        return False
    try:
        return not ds.data_vars and not ds.dims
    finally:
        ds.close()


def time_dim(da) -> str | None:
    return next((d for d in TIME_DIMS if d in da.dims), None)


def range_dim(da) -> str | None:
    return next((d for d in RANGE_DIMS if d in da.dims), None)


def value_var(ds, preferred: str | None = None) -> str:
    """The variable holding the product's values."""
    if preferred:
        if preferred not in ds.data_vars:
            raise ValueError(f"no variable {preferred!r} (have: {', '.join(ds.data_vars)})")
        return preferred
    for name in VALUE_VARS:
        if name in ds.data_vars:
            return name
    for name, da in ds.data_vars.items():
        if time_dim(da) and range_dim(da) and name not in AXIS_VARS:
            return name
    raise ValueError(f"no (ping_time x range) variable in the file (have: {', '.join(ds.data_vars)})")


def gridded(ds) -> list[str]:
    """Data variables on (time, range) that operators act on (not the axes)."""
    return [name for name, da in ds.data_vars.items()
            if name not in AXIS_VARS and time_dim(da) and range_dim(da)]


def kind_of(src, default: str = "sv") -> str:
    """An operator keeps what the data is: masked Sv is sv, cropped MVBS is mvbs."""
    kind = (((src.prov or {}).get("product") or {}).get("kind") or "").strip()
    return kind if kind and kind not in {"echodata", "source"} else default


def clean_attrs(ds):
    """NetCDF attributes cannot be None or bool."""
    for target in [ds, *ds.variables.values()]:
        for k, v in list(target.attrs.items()):
            if v is None:
                target.attrs[k] = "NA"
            elif isinstance(v, bool):
                target.attrs[k] = int(v)
    return ds


def write_netcdf(ds, path: str | Path) -> None:
    """Write in memory that does not grow with the data (one chunk at a time)."""
    import dask

    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    ds = clean_attrs(ds)
    for variable in ds.variables.values():
        variable.encoding.pop("contiguous", None)
        chunks = getattr(variable.data, "chunksize", None)
        if chunks and variable.ndim and variable.dtype.kind in "biufcmM":
            variable.encoding["chunksizes"] = tuple(int(c) for c in chunks)
        elif "chunksizes" in variable.encoding:
            enc = variable.encoding["chunksizes"]
            if enc is not None and len(enc) != variable.ndim:
                variable.encoding.pop("chunksizes")
    with dask.config.set(scheduler="synchronous"):
        ds.to_netcdf(path)


def range_axis_m(ds, var: str, channel_index: int = 0):
    """A 1-D range or depth axis in metres for var's range dimension, from
    echo_range (or depth) of one channel at the first ping; None if absent."""
    import numpy as np

    da = ds[var]
    rdim = range_dim(da)
    tdim = time_dim(da)
    if rdim in ("echo_range", "depth") and rdim in ds.coords:
        return np.asarray(ds[rdim].values, dtype=float)
    for name in ("echo_range", "depth"):
        if name not in ds:
            continue
        x = ds[name]
        if "channel" in x.dims:
            x = x.isel(channel=channel_index)
        if rdim in x.dims and tdim in x.dims:
            x = x.isel({tdim: 0})
        if x.dims == (rdim,):
            return np.asarray(x.values, dtype=float)
    return None


__all__ = [
    "AXIS_VARS", "TIME_DIMS", "RANGE_DIMS", "VALUE_VARS", "open_flat", "is_echodata",
    "time_dim", "range_dim", "value_var", "gridded", "kind_of", "clean_attrs",
    "write_netcdf", "range_axis_m",
]
