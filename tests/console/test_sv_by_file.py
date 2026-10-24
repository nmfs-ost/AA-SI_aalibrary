"""Sv file by file: each raw file's Sv on its own, then joined into the range's.

aa-sv --format zarr (several inputs, one at a time, into --dest), aa-combine
given Sv (joined along ping_time a chunk at a time, written in place), and
aa-graph reading a gs:// store in place. No network, no GCP: the bucket is a
folder (AA_GCS_FAKE_ROOT).
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest
from test_in_place import BUCKET, cached_files, lines, tool

from aalibrary.console._core import provenance, uris

pytest.importorskip("echopype")


@pytest.fixture(scope="module")
def raws(tmp_path_factory):
    """Three EK60 files, one after another (no overlap), of different lengths."""
    from aalibrary.utils.ek60_synth import write_ek60_raw

    folder = tmp_path_factory.mktemp("raws")
    names = []
    for i, minute in enumerate((0, 15, 30)):
        name = f"D20160703-T06{minute:02d}00.raw"
        write_ek60_raw(folder / name, n_pings=40 + 10 * i,
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
    yield
    uris._BACKEND = None


@pytest.fixture
def stores(raws, tmp_path):
    """The files converted, one store each, in the stand-in bucket."""
    folder, names = raws
    shelf = tmp_path / "gcs" / BUCKET / "arch" / "EK60"
    shelf.mkdir(parents=True)
    for name in names:
        (shelf / name).write_bytes((folder / name).read_bytes())
    dest = f"gs://{BUCKET}/derived/HB1603/EK60/"
    made = lines(tool(tmp_path, "ed", *[f"gs://{BUCKET}/arch/EK60/{n}" for n in names],
                      "--format", "zarr", "--dest", dest, "--sonar_model", "EK60",
                      "--cleanup-raw"))
    assert len(made) == 3
    return {"dest": dest, "stores": made, "raw": folder, "names": names}


def _open(uri):
    import xarray as xr

    return xr.open_zarr(uri, chunks={})


# --------------------------------------------------------------------------- #
# aa-sv: Zarr, and several files one at a time
# --------------------------------------------------------------------------- #
def test_aa_sv_writes_each_file_s_sv_beside_it_and_reuses_it(stores, tmp_path):
    done = tool(tmp_path, "sv", *stores["stores"], "--format", "zarr", "--dest",
                stores["dest"], "--progress")
    made = lines(done)
    recipe = made[0].rsplit("_", 1)[1]
    assert made == [s[: -len(".zarr")] + "_" + recipe for s in stores["stores"]]
    events = [json.loads(line) for line in done.stderr.splitlines() if line.startswith("{")]
    progress = [e for e in events if e["stage"] == "aa-sv" and e["event"] == "progress"]
    assert progress[-1]["done"] == progress[-1]["total"] == 3
    # Each is its own product: sealed provenance, a record beside it, the hash
    # on the root metadata (uploaded last), nothing left in the staging cache.
    for uri in made:
        doc, status = provenance.inspect_store(uri)
        assert status == "embedded" and doc["product"]["kind"] == "sv"
        assert uris.store_meta_info(uri).metadata[uris.META_HASH] == doc["product"]["hash"]
        assert uris.stat(uri + ".aa.json") is not None
    assert [p for p in cached_files(tmp_path) if ".zarr" in p] == []

    again = tool(tmp_path, "sv", *stores["stores"], "--format", "zarr", "--dest",
                 stores["dest"])
    assert again.stderr.count("reusing") == 3 and lines(again) == made


def test_zarr_sv_is_the_netcdf_sv(stores, tmp_path):
    import xarray as xr

    store = lines(tool(tmp_path, "sv", stores["stores"][0], "--format", "zarr", "--dest",
                       stores["dest"]))[-1]
    nc = lines(tool(tmp_path, "sv", stores["stores"][0], "--dest", str(tmp_path / "nc")))[-1]
    a, b = _open(store), xr.open_dataset(nc)
    # One product either way: the same name and hash, another extension.
    assert Path(nc).stem == uris.basename(store)[: -len(".zarr")]
    for name in ("Sv", "echo_range", "sound_absorption"):
        np.testing.assert_array_equal(a[name].values, b[name].values)


def test_several_inputs_need_a_destination(stores, tmp_path):
    done = tool(tmp_path, "sv", *stores["stores"][:2], "--format", "zarr", ok=False)
    assert done.returncode == 2 and "--dest" in done.stderr


# --------------------------------------------------------------------------- #
# aa-combine given Sv: the join
# --------------------------------------------------------------------------- #
@pytest.fixture
def per_file_sv(stores, tmp_path):
    return lines(tool(tmp_path, "sv", *stores["stores"], "--format", "zarr", "--dest",
                      stores["dest"]))


def test_joined_sv_is_each_file_s_sv_in_time_order(stores, per_file_sv, tmp_path):
    import xarray as xr

    work = tmp_path / "work"
    work.mkdir()
    done = tool(tmp_path, "combine", *reversed(per_file_sv), "--dest",
                f"gs://{BUCKET}/derived/HB1603/RANGE/", "--base", "HB1603_EK60_range",
                "--sort", "time", "--sonar_model", "EK60", cwd=work)
    joined = lines(done)[-1]
    recipe = per_file_sv[0].rsplit("_", 1)[1][: -len(".zarr")]
    assert joined.startswith(f"gs://{BUCKET}/derived/HB1603/RANGE/HB1603_EK60_range_")
    assert not joined.endswith(f"_{recipe}.zarr")    # its own recipe: Sv joined
    assert "read where they are" in done.stderr
    assert list(work.iterdir()) == [] and cached_files(tmp_path) == []

    j = _open(joined)
    parts = [_open(u) for u in per_file_sv]
    assert j.sizes["ping_time"] == sum(p.sizes["ping_time"] for p in parts) == 40 + 50 + 60
    reference = xr.concat(parts, dim="ping_time", data_vars="minimal", coords="minimal",
                          compat="override")
    for name in ("Sv", "echo_range", "sound_speed", "gain_correction"):
        np.testing.assert_array_equal(j[name].transpose(*reference[name].dims).values,
                                      reference[name].values)
    assert (np.diff(j["ping_time"].values).astype("int64") > 0).all()
    assert j["source_filenames"].sizes["filenames"] == 3

    # The same as calibrating the combined EchoData, for files that share
    # their calibration and environment.
    combined = lines(tool(tmp_path, "combine", *stores["stores"], "-o",
                          f"gs://{BUCKET}/derived/HB1603/RANGE/HB1603_EK60_range.zarr",
                          "--sonar_model", "EK60", "--no-report", cwd=work))[-1]
    via_combined = _open(lines(tool(tmp_path, "sv", combined, "--format", "zarr", "--dest",
                                    f"gs://{BUCKET}/derived/HB1603/RANGE/", cwd=work))[-1])
    np.testing.assert_array_equal(j["Sv"].values, via_combined["Sv"].values)

    doc, status = provenance.inspect_store(joined)
    assert status == "embedded" and doc["product"]["kind"] == "sv"
    steps = [(s["tool"], s.get("count")) for s in doc["pipeline"]]
    assert steps == [("aa-ed", 3), ("aa-sv", 3), ("aa-combine", None)]
    assert sorted(s["name"] for s in doc["sources"]) == sorted(stores["names"])
    assert uris.stat(joined + ".aa.json") is not None
    assert j.attrs["aa_write"]["complete"] is True
    assert j.attrs["processing_software_name"] == "echopype"

    again = tool(tmp_path, "combine", *per_file_sv, "--dest",
                 f"gs://{BUCKET}/derived/HB1603/RANGE/", "--base", "HB1603_EK60_range",
                 "--sonar_model", "EK60", cwd=work)
    assert "reusing" in again.stderr and lines(again)[-1] == joined


def test_a_join_that_never_finished_is_written_again(per_file_sv, tmp_path):
    import zarr

    dest = f"gs://{BUCKET}/derived/HB1603/RANGE/"
    joined = lines(tool(tmp_path, "combine", *per_file_sv, "--dest", dest, "--base", "R"))[-1]
    # What a join killed part way leaves: the marker unfinished, no provenance.
    group = zarr.open_group(str(tmp_path / "gcs" / joined[len("gs://"):]), mode="a")
    for key in [k for k in group.attrs if k.startswith("aa_") and k != "aa_write"]:
        del group.attrs[key]
    group.attrs["aa_write"] = {"complete": False, "tool": "aa-combine"}
    again = tool(tmp_path, "combine", *per_file_sv, "--dest", dest, "--base", "R")
    assert "never finished; writing it again" in again.stderr
    assert provenance.inspect_store(joined)[1] == "embedded"


def test_sv_and_echodata_are_not_mixed(stores, per_file_sv, tmp_path):
    done = tool(tmp_path, "combine", per_file_sv[0], stores["stores"][1], "-o",
                f"gs://{BUCKET}/x.zarr", ok=False)
    assert done.returncode == 2 and "join Sv with Sv" in done.stderr


def _sv(path: Path, start: str, pings: int, depth: int, gain: float, values: float):
    import xarray as xr

    times = np.datetime64(start) + np.arange(pings) * np.timedelta64(2, "s")
    channels = ["A 38 kHz", "B 120 kHz"]
    sv = np.full((2, pings, depth), values)
    ds = xr.Dataset(
        {"Sv": (("channel", "ping_time", "range_sample"), sv),
         "echo_range": (("channel", "ping_time", "range_sample"),
                        np.broadcast_to(np.arange(depth) * 0.2, (2, pings, depth)).copy()),
         "sound_speed": (("channel", "ping_time"), np.full((2, pings), 1490.0)),
         "equivalent_beam_angle": (("channel",), np.array([-20.7, -21.0])),
         "gain_correction": (("channel",), np.array([gain, 26.0])),
         "source_filenames": (("filenames",), np.array([f"{start}.raw"]))},
        coords={"channel": channels, "ping_time": times, "range_sample": np.arange(depth),
                "filenames": [0]},
    )
    ds.to_zarr(str(path), mode="w")
    return str(path)


def test_join_pads_depth_and_keeps_differing_values_per_ping(tmp_path):
    from aalibrary.console import aa_combine

    a = _sv(tmp_path / "a.zarr", "2021-10-12T14:00:00", 7, 5, 25.0, -60.0)
    b = _sv(tmp_path / "b.zarr", "2021-10-12T15:00:00", 9, 8, 25.5, -70.0)
    target = aa_combine.Target(str(tmp_path / "out.zarr"))
    notes = aa_combine.join_sv([a, b], target, None, True, 4, "default", {}, False)
    assert notes == {"pings": 16, "depth": 8, "perPing": ["gain_correction"], "leftOut": []}
    j = _open(str(tmp_path / "out.zarr"))
    assert dict(j["Sv"].sizes) == {"channel": 2, "ping_time": 16, "range_sample": 8}
    # The shallower file is padded below its last sample; nothing else changes.
    assert np.isnan(j["Sv"].values[:, :7, 5:]).all()
    assert (j["Sv"].values[:, :7, :5] == -60).all() and (j["Sv"].values[:, 7:] == -70).all()
    # Same in both files: as it was. Different: per ping, each its own file's.
    assert j["equivalent_beam_angle"].dims == ("channel",)
    assert j["gain_correction"].dims == ("ping_time", "channel")
    np.testing.assert_array_equal(j["gain_correction"].values[:, 0], [25.0] * 7 + [25.5] * 9)
    assert j["source_filenames"].values.tolist() == ["2021-10-12T14:00:00.raw",
                                                     "2021-10-12T15:00:00.raw"]
    # Every chunk is one channel, 4 pings (here) and every sample: the store's
    # chunks line up with the files' boundaries, none rewritten.
    assert j["Sv"].encoding["chunks"] == (1, 4, 8)


# --------------------------------------------------------------------------- #
# aa-graph: a gs:// store read in place
# --------------------------------------------------------------------------- #
def test_aa_graph_reads_a_store_where_it_is(per_file_sv, tmp_path):
    work = tmp_path / "work"
    work.mkdir()
    png = lines(tool(tmp_path, "graph", per_file_sv[0], "--dest",
                     f"gs://{BUCKET}/derived/HB1603/RANGE/", "--var", "Sv", cwd=work))[-1]
    assert png.endswith(".png") and uris.stat(png) is not None
    # Nothing of the store was copied here (only the image is kept, for the
    # next stage).
    assert [p for p in cached_files(tmp_path) if ".zarr" in p] == []
