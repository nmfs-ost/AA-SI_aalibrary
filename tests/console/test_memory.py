"""Memory must not grow with the length of a survey: aa-combine, aa-sv, aa-graph.

Offline and small. Each test pins one of the mechanisms that took a 39-file
EK60 combine past 31 GB, or keeps the streaming paths giving the same answer
as the in-memory ones.
"""

from __future__ import annotations

import sys

import numpy as np
import pytest
import xarray as xr

netCDF4 = pytest.importorskip("netCDF4")
pytest.importorskip("dask")


@pytest.fixture(autouse=True)
def _restore_chunk_cache():
    previous = netCDF4.get_chunk_cache()
    yield
    netCDF4.set_chunk_cache(*previous)


def _lazy(values, chunks):
    dims = ("channel", "ping_time", "range_sample")
    return xr.DataArray(values, dims=dims).chunk(chunks)


# ---------------------------------------------------------------------------
# aa-combine
# ---------------------------------------------------------------------------

def test_combine_keeps_the_small_read_cache_for_the_whole_run(monkeypatch):
    """main() sets it before the QC pass and it stays set.

    It was entered as a context manager nobody kept, which restored the 64 MB
    default at once; the QC pass then opened every input with big caches.
    """
    from aalibrary.console import aa_combine

    netCDF4.set_chunk_cache(64 << 20, 1000, 0.75)
    # Exits at argument checking, after the cache is set.
    monkeypatch.setattr(sys, "argv", ["aa-combine", "--debug", "--quiet"])
    with pytest.raises(SystemExit):
        aa_combine.main()
    assert netCDF4.get_chunk_cache()[0] == aa_combine._READ_CACHE_BYTES


def test_combine_restores_the_cache_after_a_scoped_use():
    from aalibrary.console import aa_combine

    netCDF4.set_chunk_cache(64 << 20, 1000, 0.75)
    with aa_combine._small_read_cache():
        assert netCDF4.get_chunk_cache()[0] == 1 << 20
    assert netCDF4.get_chunk_cache()[0] == 64 << 20


class _Groups:
    """The slice of EchoData that _align_netcdf_chunks uses."""

    def __init__(self, groups):
        self._groups = groups
        self.group_paths = list(groups)

    def __getitem__(self, key):
        return self._groups[key]

    def __setitem__(self, key, value):
        self._groups[key] = value


def test_netcdf_chunks_are_even_and_match_the_hdf5_chunks():
    """Uneven per-file chunks made every output chunk a read-modify-write."""
    from aalibrary.console import aa_combine

    beam = xr.Dataset(
        {"backscatter_r": _lazy(np.zeros((2, 2500, 10), "f4"),
                                {"ping_time": (900, 1100, 500)})}
    )
    beam["backscatter_r"].encoding["contiguous"] = True
    groups = _Groups({"Sonar/Beam_group1": beam, "Sonar": xr.Dataset(), "Vendor": None})

    aa_combine._align_netcdf_chunks(groups, 1000)

    out = groups["Sonar/Beam_group1"]["backscatter_r"]
    assert out.chunks[1] == (1000, 1000, 500)
    assert out.encoding["chunksizes"] == (2, 1000, 10)
    assert "contiguous" not in out.encoding


# ---------------------------------------------------------------------------
# aa-sv
# ---------------------------------------------------------------------------

def test_sv_streaming_write_is_the_same_dataset(tmp_path):
    from aalibrary.console import aa_sv

    rng = np.random.default_rng(0)
    echo_range = _lazy(rng.random((2, 3000, 50)), {"ping_time": 1000})
    ds = xr.Dataset(
        {
            # Sv shares echo_range's graph, as compute_Sv's output does.
            "Sv": 10 * np.log10(echo_range + 1),
            "echo_range": echo_range,
            "frequency_nominal": ("channel", [38000.0, 120000.0]),
        },
        coords={"ping_time": np.arange(3000)},
        attrs={"title": "x"},
    )
    path = tmp_path / "sv.nc"
    aa_sv._write_streaming(ds, path)

    back = xr.open_dataset(path)
    xr.testing.assert_identical(back.load(), ds.compute())
    assert back["Sv"].encoding["chunksizes"] == (2, 1000, 50)


def test_sv_sets_a_small_read_cache():
    from aalibrary.console import aa_sv

    netCDF4.set_chunk_cache(64 << 20, 1000, 0.75)
    aa_sv._small_read_cache()
    assert netCDF4.get_chunk_cache()[0] == 1 << 20


# ---------------------------------------------------------------------------
# aa-graph
# ---------------------------------------------------------------------------

def test_graph_draws_no_more_cells_than_the_picture_has_pixels():
    from aalibrary.console import aa_graph

    da = xr.DataArray(np.zeros((48000, 1000)), dims=("ping_time", "echo_range"))
    drawn = aa_graph._for_display(da, "ping_time", "echo_range", 10, 3, 100)
    assert drawn.sizes["ping_time"] <= 2 * 10 * 100
    assert drawn.sizes["echo_range"] <= 2 * 3 * 100
    # Small panels are drawn whole.
    small = da.isel(ping_time=slice(0, 500), echo_range=slice(0, 200))
    drawn = aa_graph._for_display(small, "ping_time", "echo_range", 10, 3, 100)
    assert drawn.identical(small)


def _panel(values):
    return xr.DataArray(values, dims=("ping_time", "range_sample"))


def test_graph_statistics_are_the_same_lazy_or_in_memory():
    from aalibrary.console import aa_graph

    rng = np.random.default_rng(1)
    sv = rng.uniform(-90, -20, (4000, 60))
    sv[::7, 3] = np.nan
    eager, lazy = _panel(sv), _panel(sv).chunk({"ping_time": 700})

    for vmin, vmax in ((None, None), (-80, -30)):
        a = aa_graph._pie_data_continuous(eager, "viridis", vmin, vmax)
        b = aa_graph._pie_data_continuous(lazy, "viridis", vmin, vmax)
        assert np.array_equal(a[0], b[0]) and a[1] == b[1] and a[2] == b[2]

    labels = rng.integers(-1, 6, (4000, 60)).astype(float)
    labels[::5, 2] = np.nan
    eager, lazy = _panel(labels), _panel(labels).chunk({"ping_time": 700})
    stats = aa_graph._cluster_label_stats
    for a, b in zip(stats(eager), stats(lazy)):
        assert np.array_equal(a, b) and a.dtype == b.dtype

    assert aa_graph._is_categorical(lazy)
    assert not aa_graph._is_categorical(_panel(sv).chunk({"ping_time": 700}))
