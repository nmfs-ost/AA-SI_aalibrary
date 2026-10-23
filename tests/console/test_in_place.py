"""Offline tests for EchoData kept in the bucket file by file and combined there.

aa-ed --format zarr (one store per raw file, published to gs://), aa-ed with
several inputs (one file at a time), aa-combine and aa-sv reading gs:// stores
where they are, and the stand-in bucket that fsspec's gs:// points at. No
network, no GCP: the bucket is a folder (AA_GCS_FAKE_ROOT).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pytest

from aalibrary.console._core import Run, ToolSpec, provenance, record_source, uris, zarr_io

BUCKET = "bkt"


def _env(root: Path) -> dict:
    env = dict(os.environ)
    env.update({"AA_CACHE_DIR": str(root / "cache"), "AA_GCS_FAKE_ROOT": str(root / "gcs"),
                "PYTHONUNBUFFERED": "1"})
    for name in ("AA_NAMING", "AA_REUSE", "AA_GCS_MOUNTS"):
        env.pop(name, None)
    return env


def tool(root: Path, name: str, *args: str, ok: bool = True,
         cwd: Path | None = None) -> subprocess.CompletedProcess:
    module = "aalibrary.console.aa_" + name.replace("-", "_")
    done = subprocess.run([sys.executable, "-m", module, *map(str, args)], cwd=cwd or root,
                          env=_env(root), capture_output=True, text=True,
                          stdin=subprocess.DEVNULL, timeout=600)
    if ok and done.returncode != 0:
        raise AssertionError(f"{name} failed ({done.returncode}):\n{done.stderr[-3000:]}")
    return done


def lines(done: subprocess.CompletedProcess) -> list[str]:
    return [line.strip() for line in done.stdout.splitlines() if line.strip()]


def cached_files(root: Path) -> list[str]:
    """Everything the tools left in their cache (downloads and staging)."""
    objects = root / "cache"
    if not objects.is_dir():
        return []
    return sorted(str(p.relative_to(objects)) for p in objects.rglob("*")
                  if p.is_file() and "records" not in p.parts
                  and p.name != "identity-memo.json")


@pytest.fixture(scope="module")
def raws(tmp_path_factory):
    """Four short EK60 files on disk."""
    from aalibrary.utils.ek60_synth import write_ek60_raw

    folder = tmp_path_factory.mktemp("raws")
    names = []
    for i in range(4):
        name = f"D20160703-T06{i}000.raw"
        write_ek60_raw(folder / name, n_pings=30, start=f"2016-07-03T06:{i}0:00", seed=i)
        names.append(name)
    return folder, names


@pytest.fixture(autouse=True)
def _isolated(tmp_path, monkeypatch):
    monkeypatch.setenv("AA_CACHE_DIR", str(tmp_path / "cache"))
    monkeypatch.setenv("AA_GCS_FAKE_ROOT", str(tmp_path / "gcs"))
    monkeypatch.delenv("AA_GCS_MOUNTS", raising=False)
    uris._BACKEND = None
    # In this process too, gs:// through fsspec is the stand-in: whichever
    # test runs first, nothing here can reach GCS.
    uris.install_fake_fsspec()
    yield
    uris._BACKEND = None


@pytest.fixture
def archive(raws, tmp_path):
    """The same files as an archive in this test's stand-in bucket
    (gs://bkt/arch/EK60/...), and on disk."""
    folder, names = raws
    shelf = tmp_path / "gcs" / BUCKET / "arch" / "EK60"
    shelf.mkdir(parents=True)
    for name in names:
        (shelf / name).write_bytes((folder / name).read_bytes())
    return {"raw": folder, "names": names,
            "uris": [f"gs://{BUCKET}/arch/EK60/{n}" for n in names]}


# --------------------------------------------------------------------------- #
# The stand-in bucket, for fsspec too
# --------------------------------------------------------------------------- #
def test_fsspec_gs_is_the_stand_in_bucket(tmp_path, monkeypatch):
    import fsspec

    monkeypatch.chdir(tmp_path)
    uris.backend()  # installs it
    with fsspec.open("gs://b/a/x.txt", "w") as handle:
        handle.write("hello")
    assert (tmp_path / "gcs" / "b" / "a" / "x.txt").read_text() == "hello"
    assert not (tmp_path / "b").exists()  # never a relative local path
    fs, path = fsspec.core.url_to_fs("gs://b/a/x.txt", token="anon", project="p")
    assert fs.exists(path) and fs.unstrip_protocol(path) == "gs://b/a/x.txt"
    # The object store sees what fsspec wrote, as GCS would.
    info = uris.stat("gs://b/a/x.txt")
    assert info is not None and info.size == 5


def test_zarr_io_writes_a_gs_store_in_place(tmp_path, monkeypatch):
    import echopype as ep
    from aalibrary.utils.ek60_synth import write_ek60_raw

    monkeypatch.chdir(tmp_path)
    ed = ep.open_raw(str(write_ek60_raw(tmp_path / "a.raw", n_pings=20)), sonar_model="EK60")
    zarr_io.write_echodata(ed, "gs://b/x/a.zarr")
    store = tmp_path / "gcs" / "b" / "x" / "a.zarr"
    assert (store / "zarr.json").is_file() or (store / ".zgroup").is_file()
    assert not (tmp_path / "b").exists()      # echopype's to_zarr would have put it here
    meta = json.loads((store / "zarr.json").read_text())
    assert "consolidated_metadata" in meta      # opens in one read
    back = ep.open_converted("gs://b/x/a.zarr", storage_options={}, chunks={})
    np.testing.assert_array_equal(back["Sonar/Beam_group1"]["backscatter_r"].values,
                                  ed["Sonar/Beam_group1"]["backscatter_r"].values)


def test_zarr_io_hands_xarray_the_url_itself(tmp_path, monkeypatch):
    """On GCS, echopype's to_zarr hands xarray the mapper's root, 'bucket/key'
    without gs://, which xarray writes as a local folder. The stand-in cannot
    show that (its paths are local), so the store argument is checked."""
    import echopype as ep
    import xarray as xr
    from aalibrary.utils.ek60_synth import write_ek60_raw

    ed = ep.open_raw(str(write_ek60_raw(tmp_path / "a.raw", n_pings=10)), sonar_model="EK60")
    stores = []
    real = xr.Dataset.to_zarr

    def spy(self, store=None, *args, **kwargs):
        stores.append(store)
        return real(self, store, *args, **kwargs)

    monkeypatch.setattr(xr.Dataset, "to_zarr", spy)
    zarr_io.write_echodata(ed, "gs://b/x/a.zarr")
    assert stores and set(stores) == {"gs://b/x/a.zarr"}


def test_codecs_are_spelled_the_way_the_installed_zarr_takes_them():
    """echopype 0.11 names its codec 'compressor', which xarray 2026.9 refuses
    ("unexpected encoding parameters for zarr backend: ['compressor']"). With
    zarr 3 it becomes 'compressors': [a zarr 3 codec]."""
    import numcodecs
    import zarr
    from zarr.codecs import BloscCodec

    if int(zarr.__version__.split(".")[0]) < 3:
        pytest.skip("zarr 2 takes 'compressor' as it is")
    blosc3 = BloscCodec(cname="zstd", clevel=3, shuffle="bitshuffle")
    spec = zarr_io.zarr_codecs({
        "a": {"compressor": blosc3, "chunks": (2,)},
        "b": {"compressor": numcodecs.Blosc(cname="lz4", clevel=5,
                                             shuffle=numcodecs.Blosc.SHUFFLE)},
        "c": {"compressor": None},
        "d": {"compressors": [blosc3]},
    })
    assert spec["a"] == {"compressors": [blosc3], "chunks": (2,)}
    (b,) = spec["b"]["compressors"]
    def word(x):
        return str(getattr(x, "value", x))

    assert isinstance(b, BloscCodec) and word(b.cname) == "lz4" and b.clevel == 5
    assert word(b.shuffle) == "shuffle"
    assert spec["c"] == {"compressors": None} and spec["d"] == {"compressors": [blosc3]}
    assert all("compressor" not in item for item in spec.values())


# --------------------------------------------------------------------------- #
# aa-ed --format zarr
# --------------------------------------------------------------------------- #
def test_aa_ed_publishes_a_store_and_reuses_it(archive, tmp_path):
    root = tmp_path
    raw = archive["raw"] / archive["names"][0]
    target = f"gs://{BUCKET}/files/one.zarr"
    first = tool(root, "ed", raw, "-o", target, "--sonar_model", "EK60")
    assert lines(first)[-1] == target
    store = root / "gcs" / BUCKET / "files" / "one.zarr"
    assert store.is_dir()
    # Provenance in the store, sealed, and beside it for readers that do not
    # open stores; the hash stamped on the root metadata, uploaded last.
    doc, status = provenance.inspect_store(target)
    assert status == "embedded" and doc["product"]["kind"] == "echodata"
    side = json.loads((store.parent / "one.zarr.aa.json").read_text())
    assert side["product"]["hash"] == doc["product"]["hash"]
    assert uris.store_meta_info(target).metadata[uris.META_HASH] == doc["product"]["hash"]
    assert cached_files(root) == []            # the staging folder is gone

    again = tool(root, "ed", raw, "-o", target, "--sonar_model", "EK60")
    assert "reusing" in again.stderr

    # -o NAME.zarr means Zarr; --format nc forces .nc as -o always did.
    local = tool(root, "ed", raw, "-o", root / "out" / "x.zarr", "--sonar_model", "EK60")
    assert lines(local)[-1].endswith("x.zarr") and (root / "out" / "x.zarr").is_dir()
    forced = tool(root, "ed", raw, "-o", root / "out" / "y.zarr", "--format", "nc",
                  "--sonar_model", "EK60")
    assert lines(forced)[-1].endswith("y.nc")
    # The same science either way: one product hash for the .nc and the store.
    assert provenance.read(root / "out" / "x.zarr")["product"]["hash"] == \
        provenance.read(root / "out" / "y.nc")["product"]["hash"]


def test_a_store_whose_upload_never_finished_is_not_reused(archive, tmp_path):
    raw = archive["raw"] / archive["names"][1]
    target = f"gs://{BUCKET}/files/two.zarr"
    tool(tmp_path, "ed", raw, "-o", target, "--sonar_model", "EK60")
    store = tmp_path / "gcs" / BUCKET / "files" / "two.zarr"
    root_meta = store / "zarr.json"
    if not root_meta.exists():
        root_meta = store / ".zattrs"
    root_meta.unlink()        # what an upload stopped before its last object leaves
    again = tool(tmp_path, "ed", raw, "-o", target, "--sonar_model", "EK60")
    assert "reusing" not in again.stderr
    assert root_meta.exists()


def test_aa_ed_finds_an_ncei_product_in_the_bucket_without_a_download(archive, tmp_path):
    name = "HB1603_L1-D20160703-T060000.raw"
    raw = tmp_path / "dl" / name
    raw.parent.mkdir()
    raw.write_bytes((archive["raw"] / archive["names"][0]).read_bytes())
    record_source(raw, tool="aa-ed", origin=f"s3://noaa-wcsd-pds/data/raw/Henry_B._Bigelow/"
                                            f"HB1603/EK60/{name}")
    dest = f"gs://{BUCKET}/per_file/EK60/"
    tool(tmp_path, "ed", raw, "--format", "zarr", "--dest", dest, "--sonar_model", "EK60")
    raw.unlink()
    raw.with_name(raw.name + ".aa.json").unlink()
    # NCEI mode (a bare name): no BigQuery (all three overrides), and no
    # download, since the product recorded as made from that object is there.
    done = tool(tmp_path, "ed", name, "--ship_name", "Henry_B._Bigelow", "--survey_name",
                "HB1603", "--sonar_model", "EK60", "--format", "zarr", "--dest", dest,
                "--file_download_directory", tmp_path / "empty", cwd=tmp_path)
    assert "reusing" in done.stderr and "NCEI object" in done.stderr
    assert lines(done)[-1] == dest + "HB1603_L1-D20160703-T060000.zarr"
    assert not (tmp_path / "empty" / name).exists()


# --------------------------------------------------------------------------- #
# aa-ed with several files: one at a time
# --------------------------------------------------------------------------- #
def test_aa_ed_converts_a_list_one_file_at_a_time(archive, tmp_path):
    dest = f"gs://{BUCKET}/per_file/EK60/"
    done = tool(tmp_path, "ed", *archive["uris"], "--format", "zarr", "--dest", dest,
                "--sonar_model", "EK60", "--cleanup-raw", "--progress")
    made = lines(done)
    assert made == [dest + Path(n).stem + ".zarr" for n in archive["names"]]
    events = [json.loads(line) for line in done.stderr.splitlines()
              if line.startswith("{") and '"event"' in line]
    progress = [e for e in events if e["event"] == "progress" and e["stage"] == "aa-ed"]
    assert progress[0]["done"] == 0 and progress[-1]["done"] == progress[-1]["total"] == 4
    # --cleanup-raw: each raw file's cached copy is gone once it is converted.
    assert [p for p in cached_files(tmp_path) if p.endswith(".raw")] == []

    # Again: every store is found from the objects' MD5s, nothing downloaded.
    again = tool(tmp_path, "ed", *archive["uris"], "--format", "zarr", "--dest", dest,
                 "--sonar_model", "EK60")
    assert again.stderr.count("reusing") == 4
    assert cached_files(tmp_path) == []


def test_a_list_goes_on_past_a_bad_file_and_says_so(archive, tmp_path):
    bad = tmp_path / "gcs" / BUCKET / "arch" / "EK60" / "D20160703-T069999.raw"
    bad.write_bytes(b"not a raw file")
    inputs = [archive["uris"][0], f"gs://{BUCKET}/arch/EK60/{bad.name}", archive["uris"][1]]
    done = tool(tmp_path, "ed", *inputs, "--format", "zarr", "--dest",
                f"gs://{BUCKET}/pf/", "--sonar_model", "EK60", ok=False)
    assert done.returncode == 1
    assert len(lines(done)) == 2 and "1 failed" in done.stderr

    three = tool(tmp_path, "ed", *[f"gs://{BUCKET}/arch/EK60/{bad.name}"] * 3,
                 archive["uris"][0], "--format", "zarr", "--dest", f"gs://{BUCKET}/pf2/",
                 "--sonar_model", "EK60", ok=False)
    assert three.returncode == 1 and "not tried" in three.stderr and lines(three) == []


def test_a_list_needs_a_destination(archive, tmp_path):
    done = tool(tmp_path, "ed", *archive["uris"][:2], "--format", "zarr", ok=False)
    assert done.returncode == 2 and "--dest" in done.stderr


# --------------------------------------------------------------------------- #
# aa-combine and aa-sv: stores read where they are
# --------------------------------------------------------------------------- #
def test_combine_reads_stores_in_place_and_writes_one_in_the_bucket(archive, tmp_path):
    import echopype as ep

    dest = f"gs://{BUCKET}/per_file/EK60/"
    stores = lines(tool(tmp_path, "ed", *archive["uris"], "--format", "zarr", "--dest", dest,
                        "--sonar_model", "EK60", "--cleanup-raw"))
    target = f"gs://{BUCKET}/out/all.zarr"
    work = tmp_path / "work"
    work.mkdir()
    done = tool(tmp_path, "combine", *reversed(stores), "-o", target, "--sort", "time",
                "--sonar_model", "EK60", "--report", cwd=work)
    assert lines(done)[-1] == target
    assert "read where they are" in done.stderr
    # Nothing was copied here: no mirror of any store, and no local store
    # (the echopype writer's mistake: a relative 'bkt/out/all.zarr').
    assert cached_files(tmp_path) == []
    assert list(work.iterdir()) == []
    combined = ep.open_converted(target, storage_options={}, chunks={})
    assert combined["Sonar/Beam_group1"].sizes["ping_time"] == 4 * 30
    reference = ep.combine_echodata([ep.open_raw(str(archive["raw"] / n), sonar_model="EK60")
                                     for n in archive["names"]])
    np.testing.assert_allclose(combined["Sonar/Beam_group1"]["backscatter_r"].values,
                               reference["Sonar/Beam_group1"]["backscatter_r"].values)
    doc, status = provenance.inspect_store(target)
    assert status == "embedded"
    assert sorted(i["uri"] for i in doc["inputs"]) == sorted(stores)
    # The same inputs again: the store is reused, still with nothing copied.
    again = tool(tmp_path, "combine", *stores, "-o", target, "--sonar_model", "EK60", cwd=work)
    assert "reusing" in again.stderr and cached_files(tmp_path) == []

    sv = tool(tmp_path, "sv", target, "--dest", f"gs://{BUCKET}/out/", cwd=work)
    assert lines(sv)[-1].startswith(f"gs://{BUCKET}/out/all_")
    # aa-sv read the combined store in place: only its own Sv is in the cache.
    assert all(p.endswith((".nc", ".nc.aa.json")) for p in cached_files(tmp_path))


def test_input_in_place_identity_matches_a_copy(archive, tmp_path):
    """Reading in place never changes a hash: the identity is the one a
    downloaded copy gets (the MD5 for a file, the product hash for a store)."""
    spec = ToolSpec(name="aa-x", role="transform")
    uri = archive["uris"][2]
    in_place = Run(spec).input(uri, fetch=False)
    copied = Run(spec).input(uri)
    assert in_place.local is None and in_place.via == "in-place"
    assert in_place.id == copied.id
    # A store without aa provenance is identified by its content: copied.
    import echopype as ep

    ed = ep.open_raw(str(archive["raw"] / archive["names"][2]), sonar_model="EK60")
    zarr_io.write_echodata(ed, f"gs://{BUCKET}/plain.zarr")
    plain = Run(spec).input(f"gs://{BUCKET}/plain.zarr", fetch=False)
    assert plain.via != "in-place" and plain.local is not None


# --------------------------------------------------------------------------- #
# Edges: what a copy would have done, in place too
# --------------------------------------------------------------------------- #
def test_a_stale_source_sidecar_in_the_bucket_is_ignored(archive, tmp_path):
    """A .raw replaced in the bucket without its .aa.json: identified by what
    is there now, as a copy would be, not by the old record."""
    uri = archive["uris"][0]
    shelf = tmp_path / "gcs" / BUCKET / uris.parse_gcs(uri)[1]
    old = shelf.with_name("old.raw")
    old.write_bytes(b"older bytes")
    record_source(old, tool="aa-ed", origin="s3://noaa-wcsd-pds/data/raw/x/old.raw")
    old.with_name("old.raw.aa.json").rename(shelf.with_name(shelf.name + ".aa.json"))
    spec = ToolSpec(name="aa-x", role="transform")
    in_place = Run(spec).input(uri, fetch=False)
    copied = Run(spec).input(uri)
    assert in_place.id == copied.id and in_place.prov is None


def test_a_raw_on_a_mount_without_api_access_is_read_through_the_mount(
        archive, tmp_path, monkeypatch):
    uri = archive["uris"][1]
    mount = tmp_path / "mnt"
    (mount / "arch" / "EK60").mkdir(parents=True)
    (mount / "arch" / "EK60" / archive["names"][1]).write_bytes(
        (archive["raw"] / archive["names"][1]).read_bytes())
    monkeypatch.setenv("AA_GCS_MOUNTS", f"{BUCKET}={mount}")

    def no_access(_uri):
        raise PermissionError("no credentials")

    monkeypatch.setattr(uris, "stat", no_access)
    got = Run(ToolSpec(name="aa-x", role="transform")).input(uri, fetch=False)
    assert got.via == "mount" and got.local is not None and got.local.is_file()


def test_legacy_names_with_a_store_read_in_place(archive, tmp_path, monkeypatch):
    import echopype as ep

    ed = ep.open_raw(str(archive["raw"] / archive["names"][3]), sonar_model="EK60")
    target = f"gs://{BUCKET}/legacy/x.zarr"
    zarr_io.write_echodata(ed, target)
    monkeypatch.setenv("AA_NAMING", "legacy")
    run = Run(ToolSpec(name="aa-x", role="transform"))
    run.input(target, fetch=False)
    out = run.plan(ext=".nc", legacy=lambda: "x_Sv.nc")
    assert out.target.endswith("x_Sv.nc")


def test_replacing_a_store_never_shows_the_wrong_stamp(tmp_path):
    """While a store is replaced, its root metadata is gone: a stop half-way
    can leave neither the old stamp over new chunks nor the new over old."""
    import zarr

    old = tmp_path / "old.zarr"
    new = tmp_path / "new.zarr"
    zarr.open_group(str(old), mode="w").create_array("v", shape=(4,), chunks=(1,),
                                                     dtype="f8")[:] = 1.0
    zarr.open_group(str(new), mode="w").create_array("v", shape=(2,), chunks=(1,),
                                                     dtype="f8")[:] = 2.0
    target = f"gs://{BUCKET}/s/store.zarr"
    uris.publish_tree(old, target, metadata={uris.META_HASH: "old"})
    assert uris.store_meta_info(target).metadata[uris.META_HASH] == "old"

    calls = []
    real = uris.backend()

    class Recorder:
        def __getattr__(self, name):
            return getattr(real, name)

        def upload(self, src, bucket, key, metadata):
            calls.append(("up", key.rsplit("/store.zarr/", 1)[-1], (metadata or {}).get(
                uris.META_HASH)))
            return real.upload(src, bucket, key, metadata)

        def delete(self, bucket, key):
            calls.append(("del", key.rsplit("/store.zarr/", 1)[-1], None))
            return real.delete(bucket, key)

    uris._BACKEND = Recorder()
    try:
        uris.publish_tree(new, target, metadata={uris.META_HASH: "new"})
    finally:
        uris._BACKEND = real
    root = [i for i, c in enumerate(calls) if c[1] == "zarr.json"]
    first_delete_of_root, last_upload_of_root = root[0], root[-1]
    assert calls[first_delete_of_root][0] == "del"          # the old stamp goes first
    assert calls[last_upload_of_root] == ("up", "zarr.json", "new")
    assert last_upload_of_root == len(calls) - 1             # and the new one last
    stale = [i for i, c in enumerate(calls) if c[0] == "del" and c[1] != "zarr.json"]
    assert stale and max(stale) < last_upload_of_root        # leftovers gone before it
    assert uris.store_meta_info(target).metadata[uris.META_HASH] == "new"
