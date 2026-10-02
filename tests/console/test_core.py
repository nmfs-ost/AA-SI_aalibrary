"""Offline tests for aalibrary.console._core (no network, no GCP)."""

from __future__ import annotations

import argparse
import json
import os
import random
from pathlib import Path

import numpy as np
import pytest
import xarray as xr

from aalibrary.console._core import (
    Help, Run, ToolSpec, add_common_flags, canon, identity, naming, provenance,
    render, stdio, uris,
)


@pytest.fixture(autouse=True)
def _isolated(tmp_path, monkeypatch):
    monkeypatch.setenv("AA_CACHE_DIR", str(tmp_path / "cache"))
    monkeypatch.setenv("AA_GCS_FAKE_ROOT", str(tmp_path / "gcs"))
    monkeypatch.delenv("AA_NAMING", raising=False)
    monkeypatch.delenv("AA_GCS_MOUNTS", raising=False)
    monkeypatch.delenv("AA_REUSE", raising=False)
    uris._BACKEND = None
    yield
    uris._BACKEND = None


# ---------------------------------------------------------------------------
# canon
# ---------------------------------------------------------------------------

def test_numbers_and_quantities_canonicalize():
    assert canon.number(20.0) == canon.number("20") == 20
    assert canon.number(-0.0) == 0
    assert canon.number(float("nan")) == "NaN"
    q = canon.quantity("dB")
    assert q("10dB") == q("10.0 dB") == q("10.0db") == q(10) == "10dB"
    m = canon.quantity("m")
    assert m("5m") == m("5.0m") == m(5.0) == "5m"
    assert canon.quantity()("0.5nmi") == "0.5nmi"


def test_kv_set_ordered_expression():
    kv = canon.kv()
    assert kv(["b=2", "a=1.0"]) == kv(["a=1", "b=2.0"]) == {"a": 1, "b": 2}
    assert canon.set_of()(["b", "a", "b"]) == ["a", "b"]
    assert canon.ordered()(["b", "a"]) == ["b", "a"]
    assert canon.expression('38.0kHz - 120kHz >= 10.0 dB') == \
        canon.expression('38kHz-120kHz>=10dB') == "38kHz-120kHz>=10dB"
    # Quoted channel names are matched verbatim downstream: kept exactly.
    assert canon.expression('"GPT  38 kHz 009072" - "GPT 120 kHz 00907" > 2.0dB') == \
        '"GPT  38 kHz 009072"-"GPT 120 kHz 00907">2dB'


def test_dumps_is_order_free_and_strict():
    a = canon.dumps({"b": 1.0, "a": [2.0, {"z": -0.0}]})
    b = canon.dumps({"a": [2, {"z": 0}], "b": 1})
    assert a == b
    # Non-finite values become strings, so canonical JSON never contains NaN.
    assert canon.dumps([float("nan"), float("inf")]) == '["NaN","Infinity"]'
    # Large integers stay exact (float() would merge 2**60 and 2**60+1).
    assert canon.number(2**60 + 1) != canon.number(2**60)
    assert canon.integer("7") == canon.integer(7.0) == 7


# ---------------------------------------------------------------------------
# Hash invariance: flag order, aliases, explicit defaults.
# ---------------------------------------------------------------------------

SPEC = ToolSpec(
    name="aa-demo", role="transform", kind="demo", op="demo.op", op_version=1,
    params={"ping_num": canon.integer, "range_bin": canon.quantity("m"),
            "snr_threshold": canon.number, "flox": canon.kv()},
    engines=(),
)


def _parser():
    p = argparse.ArgumentParser()
    p.add_argument("input_path", nargs="?")
    p.add_argument("-o", "--output_path")
    p.add_argument("--ping_num", "--ping-num", dest="ping_num", type=int, default=20)
    p.add_argument("--range_bin", "--range-bin", dest="range_bin", default="20m")
    p.add_argument("--snr_threshold", type=float, default=3.0)
    p.add_argument("--flox_kwargs", dest="flox", nargs="*", default=[])
    p.add_argument("--quiet", action="store_true")
    add_common_flags(p)
    return p


def _hash(argv, src):
    args = _parser().parse_args(argv)
    run = Run(SPEC, args)
    run.input(str(src))
    return run.plan().hash


def test_flag_order_alias_default_and_io_flags_do_not_change_hash(tmp_path):
    src = tmp_path / "x.nc"
    xr.Dataset({"Sv": ("t", np.arange(3.0))}).to_netcdf(src)
    variants = [
        [str(src)],
        [str(src), "--ping_num", "20", "--range_bin", "20m"],
        ["--range-bin", "20.0 m", str(src), "--ping-num", "20"],
        [str(src), "--snr_threshold", "3", "--quiet", "-o", "elsewhere.nc"],
        [str(src), "--flox_kwargs"],
    ]
    hashes = {_hash(v, src) for v in variants}
    assert len(hashes) == 1
    kv1 = _hash([str(src), "--flox_kwargs", "min_count=1", "engine=numpy"], src)
    kv2 = _hash([str(src), "--flox_kwargs", "engine=numpy", "min_count=1.0"], src)
    assert kv1 == kv2
    assert _hash([str(src), "--ping_num", "21"], src) not in hashes


def test_hash_depends_on_input_content(tmp_path):
    a, b = tmp_path / "a.nc", tmp_path / "b.nc"
    xr.Dataset({"Sv": ("t", np.arange(3.0))}).to_netcdf(a)
    xr.Dataset({"Sv": ("t", np.arange(4.0))}).to_netcdf(b)
    assert _hash([str(a)], a) != _hash([str(b)], b)


# ---------------------------------------------------------------------------
# naming
# ---------------------------------------------------------------------------

def test_naming_rules():
    assert naming.base_of("D20160703-T060000.raw") == "D20160703-T060000"
    assert naming.base_of("x.aa.json") == "x"
    assert naming.derived_name("B", "71957ca9deadbeef", ".nc") == "B_71957ca9.nc"
    assert naming.representation_name("B_71957ca9.nc", ".png") == "B_71957ca9.png"
    # Names are kept verbatim (as the tools always did) except separators,
    # control characters and leading dots.
    assert naming.sanitize_base("a b/c") == "a b_c"
    assert naming.sanitize_base("Station_Ä (1)") == "Station_Ä (1)"
    assert naming.sanitize_base("../../x") == "_.._x"
    assert naming.sanitize_base("..") == "product"
    assert naming.with_ext("gs://b/dir/", ".nc") == "gs://b/dir/"


# ---------------------------------------------------------------------------
# provenance round trips
# ---------------------------------------------------------------------------

def _doc(h="ab" * 32):
    step = {"tool": "aa-demo", "op": "demo", "op_version": 1, "params": {"x": 1},
            "inputs": ["md5:00:1"], "product": h, "scientific": True}
    return provenance.build(base="B", product_hash=h, kind="demo", role="transform",
                            name="B_abababab.nc", step=step,
                            inputs=[{"role": "source", "uri": "file:///x", "id": "md5:00:1"}],
                            parents=[None])


def test_netcdf_flat_and_grouped(tmp_path):
    import netCDF4

    flat = tmp_path / "flat.nc"
    xr.Dataset({"Sv": ("t", np.arange(3.0))}, attrs={"history": "old"}).to_netcdf(flat)
    assert provenance.write(flat, _doc()) == "netcdf-attrs"
    got = provenance.read(flat)
    assert got["product"]["hash"] == "ab" * 32
    ds = xr.open_dataset(flat)
    assert ds.attrs["aa_base"] == "B" and ds.attrs["history"].startswith("old\n")
    ds.close()

    grouped = tmp_path / "echodata.nc"
    with netCDF4.Dataset(grouped, "w") as nc:
        nc.createGroup("Sonar").createGroup("Beam_group1").setncattr("x", 1)
    provenance.write(grouped, _doc())
    assert provenance.read(grouped)["base"] == "B"


def test_zarr_png_html_sidecar(tmp_path):
    store = tmp_path / "s.zarr"
    xr.Dataset({"Sv": ("t", np.arange(3.0))}).to_zarr(store)
    assert provenance.write(store, _doc()).startswith("zarr-attrs")
    assert provenance.read(store)["product"]["kind"] == "demo"
    assert xr.open_zarr(store).attrs["aa_product_hash"] == "ab" * 32

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    png = tmp_path / "p.png"
    plt.figure(figsize=(1, 1)).savefig(png)
    provenance.write(png, _doc())
    assert provenance.read(png)["base"] == "B"

    html = tmp_path / "p.html"
    html.write_text("<html><head><title>x</title></head><body>hi</body></html>")
    provenance.write(html, _doc())
    assert provenance.read(html)["base"] == "B"
    assert "<body>hi</body>" in html.read_text()

    csv = tmp_path / "t.csv"
    csv.write_text("a,b\n1,2\n")
    assert provenance.write(csv, _doc()) == "sidecar"
    assert csv.read_text() == "a,b\n1,2\n"
    assert provenance.read(csv)["base"] == "B"


def test_big_pipeline_is_trimmed_in_file_and_complete_in_sidecar(tmp_path):
    doc = _doc()
    doc["pipeline"] = [dict(doc["pipeline"][0], params={"i": i, "pad": "x" * 400})
                       for i in range(300)]
    nc = tmp_path / "big.nc"
    xr.Dataset({"a": ("t", [1.0])}).to_netcdf(nc)
    how = provenance.write(nc, doc)
    assert how == "netcdf-attrs+sidecar"
    assert len(provenance.read(nc)["pipeline"]) == 300


def test_merge_pipelines_groups_identical_steps():
    a = {"pipeline": [{"tool": "aa-nc", "op": "c", "op_version": 1, "params": {"s": "EK60"},
                       "inputs": ["i1"], "product": "p1", "scientific": True}]}
    b = {"pipeline": [{"tool": "aa-nc", "op": "c", "op_version": 1, "params": {"s": "EK60"},
                       "inputs": ["i2"], "product": "p2", "scientific": True}]}
    merged = provenance.merge_pipelines([a, b])
    assert len(merged) == 1 and merged[0]["count"] == 2
    assert merged[0]["product"] == ["p1", "p2"]


# ---------------------------------------------------------------------------
# stdio
# ---------------------------------------------------------------------------

def test_tokens():
    assert stdio.normalize_token('{"schema":"aa/1","uri":"file:///a/b.nc"}') == "/a/b.nc"
    assert stdio.normalize_token("  gs://b/k.nc \n") == "gs://b/k.nc"
    assert stdio.normalize_token("# comment") is None


# ---------------------------------------------------------------------------
# Run: compute, reuse, --force, gs:// targets, base propagation
# ---------------------------------------------------------------------------

def _compute(src, dst):
    ds = xr.open_dataset(src).load()
    (ds * 2).to_netcdf(dst)


def test_run_reuse_force_and_chain(tmp_path, capsys):
    src = tmp_path / "D20160703-T060000.nc"
    xr.Dataset({"Sv": ("t", np.arange(3.0))}).to_netcdf(src)

    def once(argv):
        args = _parser().parse_args(argv)
        run = Run(SPEC, args)
        inp = run.input(args.input_path)
        out = run.plan(explicit=args.output_path)
        if not run.reusable(out):
            _compute(inp.local, out.local)
        run.finish(out)
        return out

    out1 = once([str(src)])
    assert Path(out1.target).name == f"D20160703-T060000_{out1.recipe_short}.nc"
    assert not out1.reused
    out2 = once([str(src)])
    assert out2.reused and out2.target == out1.target
    out3 = once([str(src), "--force"])
    assert not out3.reused

    # Second stage: base carried through provenance, hash chains.
    out4 = once([out1.target, "--ping_num", "5"])
    doc = provenance.read(out4.target)
    assert doc["base"] == "D20160703-T060000"
    assert [s["tool"] for s in doc["pipeline"]] == ["aa-demo", "aa-demo"]
    assert doc["inputs"][0]["id"] == f"aa:{out1.hash}"
    assert Path(out4.target).name.startswith("D20160703-T060000_")

    # --base overrides; legacy naming honoured.
    out5 = once([str(src), "--base", "MyBase"])
    assert Path(out5.target).name.startswith("MyBase_")
    os.environ["AA_NAMING"] = "legacy"
    try:
        args = _parser().parse_args([str(src)])
        run = Run(SPEC, args)
        run.input(args.input_path)
        out = run.plan(legacy=lambda: src.with_stem(src.stem + "_demo"))
        assert Path(out.target).name == "D20160703-T060000_demo.nc"
    finally:
        del os.environ["AA_NAMING"]


def test_gcs_output_input_and_reuse(tmp_path, capsys):
    src = tmp_path / "S.nc"
    xr.Dataset({"Sv": ("t", np.arange(3.0))}).to_netcdf(src)

    def stage(argv):
        args = _parser().parse_args(argv)
        run = Run(SPEC, args)
        inp = run.input(args.input_path)
        out = run.plan(explicit=args.output_path)
        if not run.reusable(out):
            _compute(inp.local, out.local)
        return run.finish(out), out

    uri, out = stage([str(src), "--dest", "gs://bkt/derived/"])
    assert uri == f"gs://bkt/derived/S_{out.recipe_short}.nc"
    info = uris.stat(uri)
    assert info.metadata[uris.META_HASH] == out.hash
    # Reading it back: served from the cache (publish kept a copy), no download.
    loc = uris.localize(uri)
    assert loc.via == "cache"
    assert provenance.read(loc.path)["product"]["hash"] == out.hash
    # Same computation again: reused from the bucket, nothing uploaded.
    _, again = stage([str(src), "--dest", "gs://bkt/derived/"])
    assert again.reused
    # gs:// input to the next stage; output lands in the cwd by default.
    os.chdir(tmp_path)
    uri2, out2 = stage([uri, "--ping_num", "7"])
    assert Path(uri2).parent == tmp_path
    assert provenance.read(uri2)["inputs"][0]["uri"] == uri


def test_gcs_identity_matches_local_identity(tmp_path):
    f = tmp_path / "x.raw"
    f.write_bytes(os.urandom(5000))
    info = uris.backend().upload(f, "bkt", "raw/x.raw", None)
    assert identity.gcs_identity(info.md5, info.size) == identity.file_identity(f)


def test_mount_is_preferred(tmp_path, monkeypatch):
    mount = tmp_path / "mnt"
    (mount / "a").mkdir(parents=True)
    (mount / "a" / "x.nc").write_bytes(b"x")
    monkeypatch.setenv("AA_GCS_MOUNTS", f"bkt={mount}")
    loc = uris.localize("gs://bkt/a/x.nc")
    assert loc.via == "mount" and loc.path == mount / "a" / "x.nc"


def test_help_renders_science_from_spec():
    text = render(SPEC, Help(summary="Demo.", stdin="A path.", stdout="A path.",
                             examples=["aa-demo x.nc"]), _parser())
    assert "SCIENTIFIC OPTIONS" in text and "--ping_num, --ping-num" in text
    assert "--quiet" not in text.split("SCIENTIFIC OPTIONS")[1].split("\n\n")[0]
    for section in ("INPUT", "OUTPUT", "METADATA", "EXAMPLES", "COMMON OPTIONS"):
        assert section in text


# ---------------------------------------------------------------------------
# Review fixes: seal, atomic writes, stale sidecars, stores, guards
# ---------------------------------------------------------------------------

def _product(tmp_path, name="D20160703-T060000.nc", argv=()):
    src = tmp_path / name
    if not src.exists():
        xr.Dataset({"Sv": ("t", np.arange(3.0))}).to_netcdf(src)
    args = _parser().parse_args([str(src), *argv])
    run = Run(SPEC, args)
    inp = run.input(args.input_path)
    out = run.plan(explicit=args.output_path)
    if not run.reusable(out):
        _compute(inp.local, out.local)
    run.finish(out)
    return out


def test_resaved_copy_does_not_inherit_provenance(tmp_path, capsys):
    out = _product(tmp_path)
    doc, status = provenance.inspect(out.target)
    assert status == "embedded" and doc["product"]["hash"] == out.hash
    # A notebook edit saved with xarray keeps the attributes, not the seal.
    ds = xr.open_dataset(out.target).load()
    ds["Sv"] = ds.Sv * 0
    copy = tmp_path / "edited.nc"
    ds.to_netcdf(copy)
    assert provenance.inspect(copy) == (None, "copied")
    run = Run(SPEC, _parser().parse_args([str(copy)]))
    inp = run.input(str(copy))
    assert inp.prov is None and inp.id.startswith("md5:")
    assert "copied from another product" in capsys.readouterr().err


def test_atomic_write_leaves_no_temp_and_replaces(tmp_path):
    out1 = _product(tmp_path)
    assert out1.atomic and not out1.local.exists()
    assert not [p for p in tmp_path.iterdir() if ".aa-" in p.name]
    out2 = _product(tmp_path, argv=["--force"])
    assert out2.target == out1.target and provenance.read(out2.target)


def test_finish_fails_when_nothing_was_written(tmp_path):
    src = tmp_path / "S.nc"
    xr.Dataset({"Sv": ("t", np.arange(3.0))}).to_netcdf(src)
    run = Run(SPEC, _parser().parse_args([str(src)]))
    run.input(str(src))
    out = run.plan()
    with pytest.raises(SystemExit):
        run.finish(out)
    assert not Path(out.target).exists()


def test_output_equal_to_an_input_is_refused(tmp_path):
    src = tmp_path / "S.nc"
    xr.Dataset({"Sv": ("t", np.arange(3.0))}).to_netcdf(src)
    run = Run(SPEC, _parser().parse_args([str(src)]))
    run.input(str(src))
    with pytest.raises(SystemExit):
        run.plan(explicit=str(src))


def test_default_output_beside_symlink_not_its_target(tmp_path):
    data = tmp_path / "data"
    work = tmp_path / "work"
    data.mkdir()
    work.mkdir()
    xr.Dataset({"Sv": ("t", np.arange(3.0))}).to_netcdf(data / "S.nc")
    (work / "S.nc").symlink_to(data / "S.nc")
    run = Run(SPEC, _parser().parse_args([str(work / "S.nc")]))
    run.input(str(work / "S.nc"))
    assert Path(run.plan().target).parent == work


def test_folder_target_gets_standard_name(tmp_path):
    src = tmp_path / "S.nc"
    xr.Dataset({"Sv": ("t", np.arange(3.0))}).to_netcdf(src)
    run = Run(SPEC, _parser().parse_args([str(src)]))
    run.input(str(src))
    out = run.plan(explicit="gs://bkt/dir/")
    assert out.target == f"gs://bkt/dir/S_{out.recipe_short}.nc"
    run.discard(out)


def test_stale_sidecar_is_ignored(tmp_path):
    f = tmp_path / "table.csv"
    f.write_text("a,b\n1,2\n")
    doc = _doc()
    assert provenance.write(f, doc) == "sidecar"
    assert provenance.read(f)["product"]["hash"] == doc["product"]["hash"]
    f.write_text("a,b\n1,2,3,4\n")            # replaced by another program
    assert provenance.inspect(f) == (None, "stale")


def test_source_sidecar_checked_by_content(tmp_path):
    from aalibrary.console._core import record_source

    raw = tmp_path / "x.raw"
    raw.write_bytes(b"A" * 100)
    record_source(raw, tool="t", origin="s3://bucket/x.raw")
    raw.write_bytes(b"B" * 100)                 # same size, different file
    side = provenance.sidecar_path(raw)
    os.utime(raw, ns=(side.stat().st_mtime_ns - 10**10,) * 2)   # older mtime
    run = Run(SPEC, None)
    inp = run.input(str(raw))
    assert inp.prov is None and inp.id == identity.file_identity(raw)


def test_tree_identity_sees_chunk_values(tmp_path):
    import zarr

    ids = []
    for i, value in enumerate((-60.0, -30.0)):
        store = tmp_path / f"s{i}.zarr"
        z = zarr.create_array(store=str(store), shape=(4,), dtype="f8", compressors=None)
        z[:] = value
        ids.append(identity.tree_identity(store))
    assert ids[0] != ids[1]


def test_store_publish_and_localize_drop_stale_chunks(tmp_path):
    def store(path, values):
        xr.Dataset({"Sv": (("p", "r"), values)}).chunk({"p": 2}).to_zarr(
            path, mode="w", zarr_format=2)

    a = np.arange(20.0).reshape(4, 5)
    store(tmp_path / "v1" / "S.zarr", a)
    store(tmp_path / "v2" / "S.zarr", np.where(np.arange(4)[:, None] >= 2, np.nan, a))
    uris.publish(tmp_path / "v1" / "S.zarr", "gs://bkt/p/S.zarr")
    first = uris.localize("gs://bkt/p/S.zarr").path
    assert xr.open_zarr(first).Sv.values[3, 0] == 15.0
    up, same, gone = uris.publish_tree(tmp_path / "v2" / "S.zarr", "gs://bkt/p/S.zarr")
    assert gone == 1
    again = uris.localize("gs://bkt/p/S.zarr").path
    assert np.isnan(xr.open_zarr(again).Sv.values[2:]).all()
    assert not [p for p in again.rglob("*") if "aa-object" in p.name]


def test_placeholder_objects_are_skipped(tmp_path):
    root = tmp_path / "gcs" / "bkt" / "d"
    root.mkdir(parents=True)
    (root / "x.bin").write_bytes(b"1")
    (tmp_path / "gcs" / "bkt" / "d" / "sub").mkdir()
    loc = uris.localize("gs://bkt/d/")
    assert (loc.path / "x.bin").is_file()


def test_sequential_identical_steps_stay_separate():
    a = {"tool": "t", "op": "o", "op_version": 1, "params": {}, "inputs": ["md5:1"],
         "product": "A", "scientific": True}
    b = dict(a, inputs=["aa:A"], product="B")
    merged = provenance.merge_pipelines([{"pipeline": [a, b]}])
    assert [s["product"] for s in merged] == ["A", "B"]


def test_html_block_goes_after_charset(tmp_path):
    page = tmp_path / "p.html"
    page.write_text('<!DOCTYPE html><html><head><meta charset="utf-8"><script>'
                    'var s="</head>";</script></head><body></body></html>')
    provenance.write(page, _doc())
    text = page.read_text()
    assert text.index('charset="utf-8"') < text.index("aa-provenance") < text.index("<script>")
    assert provenance.read(page)["product"]["hash"] == _doc()["product"]["hash"]


def test_name_hash_is_the_recipe_not_the_data(tmp_path):
    """Same processing on different raw data: same <hash8>, different product."""
    outs = []
    for name, values in (("A.nc", np.arange(3.0)), ("B.nc", np.arange(3.0) + 7)):
        src = tmp_path / name
        xr.Dataset({"Sv": ("t", values)}).to_netcdf(src)
        outs.append(_product(tmp_path, name, ["--ping_num", "5"]))
    a, b = outs
    assert a.recipe == b.recipe and a.hash != b.hash
    assert Path(a.target).name == f"A_{a.recipe_short}.nc"
    assert Path(b.target).name == f"B_{a.recipe_short}.nc"
    # A different scientific option is a different recipe.
    c = _product(tmp_path, "A.nc", ["--ping_num", "6"])
    assert c.recipe != a.recipe
    # The recipe of a second step includes the first (a chain, not one step).
    d = _product(tmp_path, Path(a.target).name, ["--ping_num", "5"])
    e = _product(tmp_path, Path(b.target).name, ["--ping_num", "5"])
    assert d.recipe == e.recipe != a.recipe
    doc = provenance.read(d.target)
    assert doc["product"]["recipe"] == d.recipe
    assert doc["pipeline"][-1]["recipe_inputs"] == [{"role": "source", "recipe": f"r:{a.recipe}"}]


def test_bucket_products_carry_a_sidecar_read_without_download(tmp_path, monkeypatch):
    from aalibrary.console import aa_metadata

    src = tmp_path / "S.nc"
    xr.Dataset({"Sv": ("t", np.arange(3.0))}).to_netcdf(src)
    args = _parser().parse_args([str(src), "--dest", "gs://bkt/derived/"])
    run = Run(SPEC, args)
    inp = run.input(args.input_path)
    out = run.plan(explicit=args.output_path)
    _compute(inp.local, out.local)
    uri = run.finish(out)
    assert uris.stat(uri + ".aa.json") is not None
    # aa-metadata answers from the sidecar: the product itself is never fetched.
    monkeypatch.setattr(uris, "localize", lambda *_a, **_k: pytest.fail("downloaded"))
    doc, _ = aa_metadata._load(uri)
    assert doc["product"]["hash"] == out.hash
    # A sidecar that names another product is not believed.
    bucket, key = uris.parse_gcs(uri)
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps(doc | {"product": {"hash": "0" * 64}}))
    uris.backend().upload(bad, bucket, key + ".aa.json", None)
    assert aa_metadata._remote_doc(uri) is None
