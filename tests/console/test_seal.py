"""The seal: where it lives, what it is checked against, and what it lets others do.

An EchoData's seal is an attribute of its Provenance group (echopype refuses
to display an EchoData with a group it doesn't know: ``KeyError: 'aa_seal'``),
any other product's an ``aa_seal`` subgroup; both record the product's array
shapes, so a copy that carries the seal along (echopype's combine, a DataTree
subset) is still told apart. Records and markers are JSON text, so a product
opened with xarray saves to NetCDF. Products sealed before (an ``aa_seal``
group with the hash only, records as JSON objects) are still their own.
No network, no GCP: the bucket is a folder (AA_GCS_FAKE_ROOT).
"""

from __future__ import annotations

import json
import warnings
from pathlib import Path

import numpy as np
import pytest
from test_in_place import BUCKET, lines, tool

from aalibrary.console._core import Run, ToolSpec, provenance, uris

ep = pytest.importorskip("echopype")
xr = pytest.importorskip("xarray")
zarr = pytest.importorskip("zarr")


@pytest.fixture(scope="module")
def raws(tmp_path_factory):
    """Three EK60 files, one after another, of different lengths."""
    from aalibrary.utils.ek60_synth import write_ek60_raw

    folder = tmp_path_factory.mktemp("raws")
    names = []
    for i, minute in enumerate((0, 15, 30)):
        name = f"D20160703-T06{minute:02d}00.raw"
        write_ek60_raw(folder / name, n_pings=30 + 10 * i,
                       start=f"2016-07-03T06:{minute:02d}:00", seed=i)
        names.append(name)
    return folder, names


@pytest.fixture(autouse=True)
def _isolated(tmp_path, monkeypatch):
    monkeypatch.setenv("AA_CACHE_DIR", str(tmp_path / "cache"))
    monkeypatch.setenv("AA_GCS_FAKE_ROOT", str(tmp_path / "gcs"))
    monkeypatch.delenv("AA_GCS_MOUNTS", raising=False)
    uris._BACKEND = None
    uris.install_fake_fsspec()
    warnings.simplefilter("ignore")
    yield
    uris._BACKEND = None


@pytest.fixture
def products(raws, tmp_path):
    """File by file into the stand-in bucket, as Prepare EchoData does it:
    each file's EchoData and Sv, the combined EchoData, the joined Sv."""
    folder, names = raws
    shelf = tmp_path / "gcs" / BUCKET / "arch" / "EK60"
    shelf.mkdir(parents=True)
    for name in names:
        (shelf / name).write_bytes((folder / name).read_bytes())
    files = f"gs://{BUCKET}/derived/HB1603/EK60/"
    asset = f"gs://{BUCKET}/derived/HB1603/HB1603_EK60_X/"
    stores = lines(tool(tmp_path, "ed", *[f"gs://{BUCKET}/arch/EK60/{n}" for n in names],
                        "--format", "zarr", "--dest", files, "--sonar_model", "EK60",
                        "--cleanup-raw"))
    svs = lines(tool(tmp_path, "sv", *stores, "--format", "zarr", "--dest", files))
    combined = lines(tool(tmp_path, "combine", *stores, "-o", asset + "HB1603_EK60_X.zarr",
                          "--no-report"))[-1]
    joined = lines(tool(tmp_path, "combine", *svs, "--dest", asset, "--base", "HB1603_EK60_X",
                        "--no-report"))[-1]
    return {"stores": stores, "svs": svs, "combined": combined, "joined": joined,
            "files": files, "asset": asset}


def _local(tmp_path: Path, uri: str) -> Path:
    bucket, key = uris.parse_gcs(uri)
    return tmp_path / "gcs" / bucket / key


def _root_attrs(path: Path) -> dict:
    return json.loads((path / "zarr.json").read_text())["attributes"]


def _displays(echodata) -> None:
    """What a notebook does with an EchoData: print it, and render it."""
    assert "EchoData" in repr(echodata)
    assert echodata._repr_html_()


# --------------------------------------------------------------------------- #
# Where the seal lives
# --------------------------------------------------------------------------- #
def test_echodata_has_no_group_echopype_does_not_know(products, tmp_path):
    for uri in [*products["stores"], products["combined"]]:
        path = _local(tmp_path, uri)
        group = zarr.open_group(str(path), mode="r")
        assert provenance.SEAL_GROUP not in group
        seal = json.loads(group["Provenance"].attrs[provenance.SEAL_ECHODATA_ATTR])
        doc, status = provenance.inspect_store(uri)
        assert status == "embedded" and seal["product_hash"] == doc["product"]["hash"]
        assert seal["shape"].startswith("s1:")
        echodata = ep.open_converted(uris.in_place(uri))
        assert provenance.SEAL_GROUP not in echodata.group_paths
        _displays(echodata)


def test_other_products_keep_the_seal_group_with_their_shape(products, tmp_path):
    for uri in [*products["svs"], products["joined"]]:
        group = zarr.open_group(str(_local(tmp_path, uri)), mode="r")
        attrs = dict(group[provenance.SEAL_GROUP].attrs)
        assert provenance.inspect_store(uri)[1] == "embedded"
        assert attrs["product_hash"] == provenance.inspect_store(uri)[0]["product"]["hash"]
        assert attrs["shape"] == provenance.shape_print(provenance.group_shapes(group))


def test_echodata_netcdf_is_sealed_in_its_provenance_group(raws, tmp_path):
    import netCDF4

    folder, names = raws
    raw = tmp_path / names[0]
    raw.write_bytes((folder / names[0]).read_bytes())
    nc = Path(lines(tool(tmp_path, "ed", f"./{names[0]}", "--sonar_model", "EK60",
                         "--dest", str(tmp_path / "out")))[-1])
    with netCDF4.Dataset(str(nc)) as handle:
        assert provenance.SEAL_GROUP not in handle.groups
        assert provenance.SEAL_ECHODATA_ATTR in handle.groups["Provenance"].ncattrs()
    assert provenance.inspect(nc)[1] == "embedded"
    _displays(ep.open_converted(str(nc)))


# --------------------------------------------------------------------------- #
# What it lets others do
# --------------------------------------------------------------------------- #
def test_records_and_markers_are_text(products, tmp_path):
    for uri in [*products["stores"], *products["svs"], products["combined"],
                products["joined"]]:
        attrs = _root_attrs(_local(tmp_path, uri))
        structured = [k for k, v in attrs.items() if isinstance(v, (dict, list, bool))]
        assert structured == [], uri
        assert isinstance(attrs[provenance.ATTR], str)
    # aa-store still reports the marker and the lineage as objects.
    done = tool(tmp_path, "store", "info", "--json", products["joined"])
    info = json.loads(lines(done)[-1])
    assert info["write"]["complete"] is True and info["provenance"]["tool"] == "aa-combine"


def test_sv_opened_with_xarray_saves_to_netcdf(products, tmp_path):
    for index, uri in enumerate([products["svs"][0], products["joined"]]):
        out = tmp_path / f"sv{index}.nc"
        with xr.open_zarr(uris.in_place(uri)) as dataset:
            dataset.to_netcdf(out)
        with xr.open_dataset(out) as back:
            assert back.attrs[provenance.ATTR_HASH]


def test_aa_store_verifies_an_sv_store_with_a_scalar_array(products, tmp_path):
    from aalibrary.console import aa_store

    assert aa_store._expected_chunks((), ()) == 1
    assert aa_store._expected_chunks([], []) == 1
    done = tool(tmp_path, "store", "verify", "--json", products["joined"], products["svs"][0])
    for line in lines(done):
        verdict = json.loads(line)["verify"]
        assert verdict["ok"] and not verdict["problems"], verdict


# --------------------------------------------------------------------------- #
# Copies that carry the seal along are not the product
# --------------------------------------------------------------------------- #
def _write_every_group(echodata, path: Path) -> None:
    """A colleague's own save of an EchoData: every group echopype holds,
    attributes and all (echopype's to_zarr itself fails on xarray 2026.9)."""
    groups = ["Top-level", *[g for g in echodata.group_paths if g != "Top-level"]]
    for index, group in enumerate(groups):
        dataset = echodata[group]
        for variable in dataset.variables.values():
            variable.encoding = {}
        dataset.to_zarr(path, group=None if group == "Top-level" else group,
                        mode="w" if index == 0 else "a", consolidated=False)
    zarr.consolidate_metadata(str(path))


def _not_the_product(path: Path, product: str) -> None:
    assert provenance.inspect(path) == (None, "copied")
    assert provenance.inspect_store(str(path)) == (None, "copied")
    given = Run(ToolSpec(name="aa-x", role="transform")).input(str(path))
    assert given.prov is None and given.id != f"aa:{product}"


def test_an_echopype_combine_of_two_products_is_not_the_first(products, tmp_path):
    first, second = (ep.open_converted(uris.in_place(u)) for u in products["stores"][:2])
    product = provenance.inspect_store(products["stores"][0])[0]["product"]["hash"]
    combined = ep.combine_echodata([first, second])
    out = tmp_path / "theirs.zarr"
    _write_every_group(combined, out)
    # The record and the seal both rode along: only the shape tells.
    assert json.loads(_root_attrs(out)[provenance.ATTR])["product"]["hash"] == product
    assert provenance.SEAL_ECHODATA_ATTR in zarr.open_group(str(out))["Provenance"].attrs
    _not_the_product(out, product)


def test_a_datatree_subset_of_sv_is_not_the_product(products, tmp_path):
    uri = products["joined"]
    product = provenance.inspect_store(uri)[0]["product"]["hash"]
    tree = xr.open_datatree(uris.in_place(uri), engine="zarr")
    out = tmp_path / "subset.zarr"
    tree.isel(ping_time=slice(0, 10)).to_zarr(out)
    assert (out / provenance.SEAL_GROUP).is_dir()          # carried along
    _not_the_product(out, product)


def test_a_masked_xarray_copy_of_sv_is_not_the_product(products, tmp_path):
    uri = products["svs"][0]
    product = provenance.inspect_store(uri)[0]["product"]["hash"]
    with xr.open_zarr(uris.in_place(uri)) as dataset:
        dataset["Sv"] = dataset["Sv"].where(dataset["Sv"] > -70)
        out = tmp_path / "masked.zarr"
        dataset.to_zarr(out)
    _not_the_product(out, product)


def test_the_same_product_unchanged_is_still_its_own_through_a_stale_listing(
        products, tmp_path):
    """Consolidated metadata that lags the arrays (an array missing from the
    listing) is checked against the arrays themselves before calling it a copy."""
    path = _local(tmp_path, products["svs"][1])
    root = json.loads((path / "zarr.json").read_text())
    listing = root["consolidated_metadata"]["metadata"]
    listing.pop(next(k for k in listing if k != provenance.SEAL_GROUP))
    (path / "zarr.json").write_text(json.dumps(root))
    assert provenance.inspect_store(products["svs"][1])[1] == "embedded"


# --------------------------------------------------------------------------- #
# Products sealed before catch-up 16
# --------------------------------------------------------------------------- #
def _as_sealed_before(path: Path) -> None:
    """Rewrite a store the way the tools wrote it before: the record and the
    markers as JSON objects, an aa_seal group with the hash only, nothing in
    the Provenance group."""
    group = zarr.open_group(str(path), mode="r+")
    for key in (provenance.ATTR, "aa_write", "provenance"):
        if isinstance(group.attrs.get(key), str):
            group.attrs[key] = json.loads(group.attrs[key])
    product = group.attrs[provenance.ATTR]["product"]["hash"]
    if "Provenance" in group and provenance.SEAL_ECHODATA_ATTR in group["Provenance"].attrs:
        del group["Provenance"].attrs[provenance.SEAL_ECHODATA_ATTR]
    if provenance.SEAL_GROUP in group:
        del group[provenance.SEAL_GROUP]
    group.create_group(provenance.SEAL_GROUP).attrs[provenance.SEAL_ATTR] = product
    zarr.consolidate_metadata(str(path))


@pytest.fixture
def before(products, tmp_path):
    for uri in [*products["stores"], *products["svs"], products["combined"],
                products["joined"]]:
        _as_sealed_before(_local(tmp_path, uri))
    return products


def test_products_sealed_before_are_still_their_own_and_reused(before, tmp_path):
    for uri in [*before["stores"], *before["svs"], before["combined"], before["joined"]]:
        assert provenance.inspect_store(uri)[1] == "embedded", uri
    again = tool(tmp_path, "sv", *before["stores"], "--format", "zarr", "--dest",
                 before["files"])
    assert again.stderr.count("reusing") == 3 and lines(again) == before["svs"]
    joined = tool(tmp_path, "combine", *before["svs"], "--dest", before["asset"],
                  "--base", "HB1603_EK60_X", "--no-report")
    assert lines(joined)[-1] == before["joined"]
    assert "identical product" in joined.stderr and "already exists" in joined.stderr
    info = json.loads(lines(tool(tmp_path, "store", "info", "--json", before["joined"]))[-1])
    assert info["write"]["complete"] is True and info["product"]


def test_an_echopype_combine_of_products_sealed_before_is_not_the_first(before, tmp_path):
    first, second = (ep.open_converted(uris.in_place(u)) for u in before["stores"][:2])
    assert provenance.SEAL_GROUP in first.group_paths    # read as it is
    product = provenance.inspect_store(before["stores"][0])[0]["product"]["hash"]
    combined = ep.combine_echodata([first, second])
    out = tmp_path / "theirs.zarr"
    _write_every_group(combined, out)
    assert (out / provenance.SEAL_GROUP).is_dir()      # the old seal rode along
    _not_the_product(out, product)


def test_combining_products_sealed_before_drops_their_seal_group(before, tmp_path):
    out = f"gs://{BUCKET}/derived/HB1603/again/HB1603_EK60_Y.zarr"
    made = lines(tool(tmp_path, "combine", *before["stores"], "-o", out, "--no-report"))[-1]
    group = zarr.open_group(str(_local(tmp_path, made)), mode="r")
    assert provenance.SEAL_GROUP not in group
    assert provenance.inspect_store(made)[1] == "embedded"
    _displays(ep.open_converted(uris.in_place(made)))
    # The same EchoData as the combine of the products sealed the new way.
    a = xr.open_zarr(uris.in_place(made), group="Sonar/Beam_group1")
    b = xr.open_zarr(uris.in_place(before["combined"]), group="Sonar/Beam_group1")
    np.testing.assert_array_equal(a["backscatter_r"].values, b["backscatter_r"].values)


def test_a_datatree_copy_of_a_product_sealed_before_is_not_it(before, tmp_path):
    """An old seal records no shape, so a copy that carries it along is told
    apart by its record naming another store."""
    uri = before["joined"]
    product = provenance.inspect_store(uri)[0]["product"]["hash"]
    tree = xr.open_datatree(uris.in_place(uri), engine="zarr")
    out = tmp_path / "subset.zarr"
    tree.isel(ping_time=slice(0, 10)).to_zarr(out)
    assert (out / provenance.SEAL_GROUP).is_dir()      # the old seal rode along
    _not_the_product(out, product)
