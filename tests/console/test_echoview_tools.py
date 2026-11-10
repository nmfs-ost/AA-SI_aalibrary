"""Offline tests for the Echoview-parity tools: aa-ecs, aa-crop, aa-mask,
aa-threshold, aa-tiles, aa-annotate, aa-integrate, and --describe.

The tools run as subprocesses (python -m aalibrary.console.aa_x), on a small
synthetic EK60 file, with a local stand-in bucket: no network, no GCP.
"""

from __future__ import annotations

import csv
import json
import math
import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pytest
import xarray as xr

from aalibrary.console import _calibration as calib
from aalibrary.console import _echoview as ev
from aalibrary.console import _tilepack as tp
from aalibrary.console._core import Run, ToolSpec, canon, chaining, identity

HERE = Path(__file__).resolve().parent


def _env(root: Path) -> dict:
    env = dict(os.environ)
    env.update({"AA_CACHE_DIR": str(root / "cache"), "AA_GCS_FAKE_ROOT": str(root / "gcs"),
                "PYTHONUNBUFFERED": "1"})
    env.pop("AA_NAMING", None)
    env.pop("AA_REUSE", None)
    return env


def tool(root: Path, name: str, *args: str, ok: bool = True) -> subprocess.CompletedProcess:
    module = "aalibrary.console.aa_" + name.replace("-", "_")
    done = subprocess.run([sys.executable, "-m", module, *map(str, args)], cwd=root,
                          env=_env(root), capture_output=True, text=True,
                          stdin=subprocess.DEVNULL, timeout=600)
    if ok and done.returncode != 0:
        raise AssertionError(f"{name} failed ({done.returncode}):\n{done.stderr[-3000:]}")
    return done


def out(done: subprocess.CompletedProcess) -> str:
    return done.stdout.strip().splitlines()[-1]


@pytest.fixture(scope="module")
def survey(tmp_path_factory):
    """A small EchoData, its Sv with depth and position, and a bottom line."""
    from aalibrary.utils.ek60_synth import write_ek60_raw

    root = tmp_path_factory.mktemp("survey")
    raw = write_ek60_raw(root / "D20160703-T060000.raw", n_pings=90)
    ed = out(tool(root, "nc", raw, "--sonar_model", "EK60"))
    sv = out(tool(root, "sv", ed))
    depth = out(tool(root, "depth", sv))
    loc = out(tool(root, "location", depth, "--echodata", ed))
    return {"root": root, "ed": Path(ed), "sv": Path(sv), "depth": Path(loc)}


# --------------------------------------------------------------------------- #
# The core: options added later stay out of the hash while unset
# --------------------------------------------------------------------------- #
def test_optional_params_do_not_change_old_hashes():
    old = ToolSpec(name="aa-x", role="transform", params={"a": canon.number})
    new = ToolSpec(name="aa-x", role="transform",
                   params={"a": canon.number, "b": canon.kv()}, optional=frozenset({"b"}))

    class Args:
        a = 3
        b = None

    assert Run(old, Args()).identity_doc() == Run(new, Args()).identity_doc()
    assert Run(new, Args(), params={"b": None}).params == {"a": 3}
    Args.b = ["k=1"]
    assert Run(new, Args()).params == {"a": 3, "b": {"k": 1}}


# --------------------------------------------------------------------------- #
# Calibration
# --------------------------------------------------------------------------- #
def test_parse_kv_and_frequency():
    assert calib.parse_frequency("38kHz") == calib.parse_frequency("38000") == 38000.0
    assert calib.parse_frequency("120 kHz") == 120000.0
    kv = calib.parse_kv(["gain_correction@38kHz=26.1", "sa_correction=-0.7"])
    assert kv == {"gain_correction@38000": 26.1, "sa_correction": -0.7}
    assert calib.parse_kv(None) is None
    with pytest.raises(ValueError):
        calib.parse_kv(["gain=abc"])
    with pytest.raises(ValueError):
        calib.parse_kv(["nan_value=nan"])


def test_ecs_round_trip_through_echopype(tmp_path):
    channels = [
        {"frequency": 38000.0, "values": {"gain_correction": 26.12, "sa_correction": -0.68,
                                          "equivalent_beam_angle": -20.7, "sound_speed": 1490.0,
                                          "sound_absorption": 0.0098}},
        {"frequency": 120000.0, "values": {"gain_correction": 27.0, "sa_correction": -0.5,
                                           "equivalent_beam_angle": -20.7, "sound_speed": 1490.0,
                                           "sound_absorption": 0.0378}},
    ]
    path = calib.write_ecs(tmp_path / "x.ecs", channels, notes=["a test"])
    doc = calib.read_ecs(path)
    by_freq = {round(s["frequency"]): s["values"] for s in doc["sources"]}
    assert by_freq[38000]["gain_correction"] == pytest.approx(26.12)
    assert by_freq[120000]["sound_absorption"] == pytest.approx(0.0378)
    # Common environment values go in FileSet and still reach every source.
    assert by_freq[120000]["sound_speed"] == pytest.approx(1490.0)
    assert "FileSet" not in path.read_text() or "SoundSpeed" in path.read_text()


def test_aa_ecs_write_equals_overrides(survey):
    root, ed = survey["root"], survey["ed"]
    shown = json.loads(out(tool(root, "ecs", ed, "--json")))
    assert shown["sonarModel"] == "EK60" and len(shown["channels"]) == 2
    gain = next(v for v in shown["channels"][0]["values"] if v["name"] == "gain_correction")
    assert gain["changed"] is False
    ecs = out(tool(root, "ecs", ed, "--write", "--cal-param", "gain_correction@38kHz=25.0"))
    assert ecs.endswith(".ecs") and Path(ecs + ".aa.json").is_file()
    again = tool(root, "ecs", ed, "--write", "--cal-param", "gain_correction@38kHz=25.0")
    assert "reusing" in again.stderr
    with_ecs = out(tool(root, "sv", ed, "--ecs", ecs))
    with_kv = out(tool(root, "sv", ed, "--cal-param", "gain_correction@38kHz=25.0"))
    plain = survey["sv"]
    a, b, c = (xr.open_dataset(p) for p in (with_ecs, with_kv, plain))
    assert float(abs(a.Sv - b.Sv).max()) < 1e-4
    # 1.5 dB less gain: +3 dB of Sv at 38 kHz, nothing at 120 kHz.
    assert float((a.Sv - c.Sv).isel(channel=0).mean()) == pytest.approx(3.0, abs=0.01)
    assert float(abs(a.Sv - c.Sv).isel(channel=1).max()) < 1e-4
    refused = tool(root, "sv", ed, "--ecs", ecs, "--cal-param", "gain_correction=1", ok=False)
    assert refused.returncode == 2


# --------------------------------------------------------------------------- #
# Operators
# --------------------------------------------------------------------------- #
def test_crop_threshold_mask(survey):
    root, sv = survey["root"], survey["depth"]
    cropped = out(tool(root, "crop", sv, "--pings", "10:40", "--max-range", "60",
                       "--frequency", "38kHz"))
    d = xr.open_dataset(cropped)
    assert d.sizes["ping_time"] == 30 and d.sizes["channel"] == 1
    assert float(d.echo_range.max()) <= 60
    assert tool(root, "crop", sv, "--start", "2030-01-01", ok=False).returncode == 1

    th = out(tool(root, "threshold", sv, "--min", "-70"))
    t, s = xr.open_dataset(th), xr.open_dataset(sv)
    assert int((t.Sv == -999).sum()) == int((s.Sv < -70).sum())
    assert bool((t.depth.fillna(0) == s.depth.fillna(0)).all())

    fd = out(tool(root, "freqdiff", sv, "--freqABEq", "120kHz - 38kHz > 2dB"))
    masked = out(tool(root, "mask", sv, "--mask", fd))
    m, o = xr.open_dataset(fd), xr.open_dataset(masked)
    # freqdiff is a selection (keep): outside it, every channel becomes NaN.
    outside = ~m.freqdiff_mask.values.astype(bool)
    assert np.isnan(o.Sv.isel(channel=0).transpose("ping_time", "range_sample").values[outside]).all()
    remove = out(tool(root, "mask", sv, "--remove", fd))
    r = xr.open_dataset(remove)
    inside = m.freqdiff_mask.values.astype(bool)
    assert np.isnan(r.Sv.isel(channel=1).transpose("ping_time", "range_sample").values[inside]).all()


# --------------------------------------------------------------------------- #
# Tiles
# --------------------------------------------------------------------------- #
def test_tilepack_primitives(tmp_path):
    a = np.array([[1.0, 3.0], [np.nan, 5.0], [np.nan, np.nan]], dtype=np.float32)
    assert tp.halve(a, 0, "mean").tolist()[0] == [1.0, 4.0]
    assert np.isnan(tp.halve(a, 0, "mean")[1]).all()
    assert tp.halve(a, 1, "max").reshape(-1).tolist()[:2] == [3.0, 5.0]
    values = np.array([-180.0, -70.123, 0.0, np.nan, 82.0])
    back = tp.dequantize(tp.quantize(values, tp.DB_QUANT), tp.DB_QUANT)
    assert np.allclose(back[[0, 1, 2, 4]], values[[0, 1, 2, 4]], atol=0.0021)
    assert np.isnan(back[3])
    q = np.arange(10, dtype=np.uint16)
    assert (tp.unshuffle16(tp.shuffle16(q), 10) == q).all()
    levels = tp.level_plan(5000, 800, 256, 2)
    assert levels[0]["fx"] == 1 and levels[-1]["width"] <= 256
    assert levels[3]["fy"] == 2 and levels[2]["fy"] == 1


def test_aa_tiles_values_and_levels(survey):
    root, sv = survey["root"], survey["depth"]
    pack = out(tool(root, "tiles", sv, "--tile", "32"))
    reader = tp.Reader(pack)
    h = reader.header
    assert h["format"] == "aa-tiles/1" and h["nature"] == "db" and len(h["channels"]) == 2
    ds = xr.open_dataset(sv)
    data = ds.Sv.isel(channel=0).transpose("ping_time", "range_sample").values
    tile = reader.tile(0, 0, 0, 0)
    ref = data[:32, :32].T
    ok = np.isfinite(tile)
    assert ok.sum() > 0 and np.nanmax(np.abs(tile[ok] - ref[ok])) < 0.0021
    lin = 10 ** (data[:64, :32] / 10)
    with np.errstate(all="ignore"):
        expect = 10 * np.log10(np.nanmean(lin.reshape(32, 2, 32), axis=1)).T
    lvl1 = reader.tile(0, 1, 0, 0)
    assert np.nanmax(np.abs(lvl1 - expect)) < 0.0021
    assert len(reader.axis("time")) == ds.sizes["ping_time"]
    assert np.isfinite(reader.axis("latitude")).all()
    assert "reusing" in tool(root, "tiles", sv, "--tile", "32").stderr


# --------------------------------------------------------------------------- #
# Lines and regions
# --------------------------------------------------------------------------- #
def test_echoview_files_round_trip(tmp_path):
    t0 = 1467525600000.0
    pts = [{"t": t0 + i * 1000.25, "depth": 100 + i, "status": 3} for i in range(5)]
    ev.write_evl(tmp_path / "a.evl", pts)
    back = ev.read_evl(tmp_path / "a.evl")
    assert [round(p["t"], 1) for p in back] == [round(p["t"], 1) for p in pts]
    regions = [{"id": 3, "name": "school 1", "class": "Hake", "kind": "analysis",
                "notes": ["seen twice"],
                "points": [{"t": t0, "depth": 10}, {"t": t0 + 5000, "depth": 10},
                           {"t": t0 + 5000, "depth": 30}]},
               {"id": 4, "name": "spike", "class": "", "kind": "bad",
                "points": [{"t": t0, "depth": 0}, {"t": t0 + 1000, "depth": 0},
                           {"t": t0 + 1000, "depth": 50}]}]
    ev.write_evr(tmp_path / "a.evr", regions)
    rb = ev.read_evr(tmp_path / "a.evr")
    assert [(r["id"], r["class"], r["kind"], r["name"], r["notes"]) for r in rb] == [
        (3, "Hake", "analysis", "school 1", ["seen twice"]), (4, "", "bad", "spike", [])]
    import echoregions as er

    data = er.read_evr(str(tmp_path / "a.evr")).data
    assert list(data["region_id"]) == [3, 4]
    assert er.read_evl(str(tmp_path / "a.evl")).data.shape[0] == 5
    assert ev.ev_time(t0 + 1.5) == ("20160703", "0600000015")


def test_aa_annotate_lines_regions_and_seafloor(survey, tmp_path):
    root, sv = survey["root"], survey["depth"]
    t = xr.open_dataset(sv).ping_time.values.astype("datetime64[ns]").astype(np.int64) / 1e6
    shapes = {"type": "regions", "name": "my schools", "regions": [
        {"id": 1, "name": "A", "class": "Hake", "kind": "analysis",
         "points": [{"t": t[10], "depth": 35}, {"t": t[60], "depth": 35},
                    {"t": t[60], "depth": 60}, {"t": t[10], "depth": 60}]}]}
    src = root / "shapes.json"
    src.write_text(json.dumps(shapes))
    evr = out(tool(root, "annotate", src, "--reference", sv))
    assert Path(evr).name.startswith("D20160703-T060000_my_schools_") and evr.endswith(".evr")
    meta = json.loads(Path(evr + ".aa.json").read_text())
    assert meta["product"]["kind"] == "regions" and meta["extra"]["drawn_on"]
    # The same content is the same product, wherever it was drawn.
    again = tool(root, "annotate", src, "--reference", survey["sv"])
    assert "reusing" in again.stderr
    shown = json.loads(out(tool(root, "annotate", evr, "--json")))
    assert shown["name"] == "my_schools" and shown["regions"][0]["class"] == "Hake"

    sf = out(tool(root, "detect-seafloor", sv, "--method", "basic", "--param", "var_name=Sv",
                  "channel=GPT   38 kHz 00907205c001-1 ES38B", "threshold=(-40,10)"))
    evl = out(tool(root, "annotate", sf, "--name", "bottom"))
    assert len(ev.read_evl(evl)) == xr.open_dataset(sf).sizes["ping_time"]
    thin = json.loads(out(tool(root, "annotate", sf, "--json", "--max-points", "10")))
    assert thin["type"] == "line" and len(thin["points"]) <= 10 and thin["detected"]
    bad = root / "bad.json"
    bad.write_text(json.dumps({"type": "line", "points": [{"t": 1, "depth": 2}]}))
    assert tool(root, "annotate", bad, ok=False).returncode == 2


def test_detect_seafloor_takes_the_channel_by_frequency(survey):
    """channel=38kHz (or no channel: the one nearest 38 kHz) is the same
    product as the channel's id written out; var_name defaults to Sv."""
    root, sv = survey["root"], survey["depth"]
    by_id = tool(root, "detect-seafloor", sv, "--method", "basic", "--param", "var_name=Sv",
                 "channel=GPT   38 kHz 00907205c001-1 ES38B", "threshold=(-40,10)")
    by_freq = tool(root, "detect-seafloor", sv, "--method", "basic", "--param",
                   "channel=38kHz", "threshold=(-40,10)")
    default = tool(root, "detect-seafloor", sv, "--method", "basic", "--param",
                   "threshold=(-40,10)")
    assert out(by_id) == out(by_freq) == out(default)
    assert "reusing" in by_freq.stderr and "reusing" in default.stderr
    missing = tool(root, "detect-seafloor", sv, "--method", "basic", "--param",
                   "channel=200kHz", ok=False)
    assert missing.returncode == 1 and "No channel at 200 kHz" in missing.stderr


# --------------------------------------------------------------------------- #
# Integration
# --------------------------------------------------------------------------- #
def _brute(ds, ch, p0, p1, lo, hi, min_sv, bottom=None, surface=0.0):
    sv = ds.Sv.isel(channel=ch, ping_time=slice(p0, p1 + 1)).transpose(
        "ping_time", "range_sample").values
    y = ds.depth.isel(channel=ch, ping_time=slice(p0, p1 + 1)).transpose(
        "ping_time", "range_sample").values
    rng = ds.echo_range.isel(channel=ch, ping_time=slice(p0, p1 + 1)).transpose(
        "ping_time", "range_sample").values
    sel = (y >= lo) & (y < hi) & np.isfinite(sv) & (y >= surface)
    if bottom is not None:
        sel &= y <= bottom[p0:p1 + 1, None]
    lin = np.where(sv < min_sv, 0.0, 10 ** (sv / 10))
    mean = lin[sel].sum() / max(sel.sum(), 1)
    dr = np.nanmedian(np.diff(rng, axis=1), axis=1)
    count = sel.sum(axis=1)
    thick = (dr * count)[count > 0].mean() if (count > 0).any() else 0.0
    return 4 * math.pi * 1852 ** 2 * mean * thick, int(sel.sum())


def test_aa_integrate_matches_the_definitions(survey):
    root, sv = survey["root"], survey["depth"]
    ds = xr.open_dataset(sv)
    t = ds.ping_time.values.astype("datetime64[ns]").astype(np.int64) / 1e6
    line = {"type": "line", "name": "bottom",
            "points": [{"t": t[0], "depth": 110}, {"t": t[-1], "depth": 118}]}
    (root / "bottom.json").write_text(json.dumps(line))
    bottom_file = out(tool(root, "annotate", root / "bottom.json"))
    csv_path = out(tool(root, "integrate", sv, "--interval", "30", "--layer", "20",
                        "--min-sv", "-80", "--bottom", bottom_file, "--surface-depth", "5"))
    rows = list(csv.DictReader(open(csv_path)))
    assert rows and {"Interval", "Layer", "NASC", "Sv_mean", "Lat_M", "Dist_M"} <= set(rows[0])
    bottom = np.interp(t, [p["t"] for p in line["points"]], [p["depth"] for p in line["points"]])
    checked = 0
    for row in rows[:: max(1, len(rows) // 8)]:
        ch = [i for i, f in enumerate(ds.frequency_nominal.values)
              if abs(f / 1000 - float(row["Frequency"])) < 0.5][0]
        layer = int(row["Layer"])
        nasc, good = _brute(ds, ch, int(row["Ping_S"]), int(row["Ping_E"]), (layer - 1) * 20,
                            layer * 20, -80, bottom, surface=5)
        assert good == int(row["Good_samples"])
        assert float(row["NASC"]) == pytest.approx(nasc, rel=1e-6, abs=1e-9)
        checked += 1
    assert checked >= 4


def test_aa_integrate_region_cells_add_up(survey):
    root, sv = survey["root"], survey["depth"]
    ds = xr.open_dataset(sv)
    t = ds.ping_time.values.astype("datetime64[ns]").astype(np.int64) / 1e6
    shapes = {"type": "regions", "name": "split", "regions": [
        {"id": 1, "name": "upper", "class": "A", "kind": "analysis",
         "points": [{"t": t[0] - 1, "depth": 0}, {"t": t[-1] + 1, "depth": 0},
                    {"t": t[-1] + 1, "depth": 50}, {"t": t[0] - 1, "depth": 50}]},
        {"id": 2, "name": "lower", "class": "B", "kind": "analysis",
         "points": [{"t": t[0] - 1, "depth": 50}, {"t": t[-1] + 1, "depth": 50},
                    {"t": t[-1] + 1, "depth": 400}, {"t": t[0] - 1, "depth": 400}]}]}
    (root / "split.json").write_text(json.dumps(shapes))
    evr = out(tool(root, "annotate", root / "split.json"))
    def rows(*args):
        return list(csv.DictReader(open(out(tool(root, "integrate", sv, *args)))))

    cells = rows("--interval", "45", "--layer", "25", "--frequency", "38kHz")
    rc = rows("--interval", "45", "--layer", "25", "--frequency", "38kHz",
              "--by", "region-cells", "--regions", evr)
    total = {}
    for row in rc:
        key = (row["Interval"], row["Layer"])
        total[key] = total.get(key, 0.0) + float(row["PRC_NASC"])
    for row in cells:
        key = (row["Interval"], row["Layer"])
        assert total.get(key, 0.0) == pytest.approx(float(row["NASC"]), rel=1e-6, abs=1e-9)
    whole = rows("--by", "regions", "--regions", evr, "--frequency", "38kHz")
    assert sorted(r["Region_class"] for r in whole) == ["A", "B"]


# --------------------------------------------------------------------------- #
# --describe and the chaining table
# --------------------------------------------------------------------------- #
def test_describe_and_chaining(tmp_path):
    for name in ("aa-sv", "aa-integrate", "aa-tiles", "aa-mask"):
        done = tool(tmp_path, name[3:], "--describe")
        doc = json.loads(done.stdout)
        assert doc["schema"] == "aa-describe/1" and doc["name"] == name
        assert doc["traits"]["produces"] == chaining.TRAITS[name].produces
        assert any(a["flags"] for a in doc["actions"])
    table = chaining.table()
    assert table["kinds"]["integration"]["level"] == "L3"
    for name, traits in table["tools"].items():
        assert traits["produces"] in table["kinds"], name
        for kinds in traits["inputs"].values():
            assert set(kinds) <= set(table["kinds"]), name
    assert identity.product_hash({"a": 1}) == identity.product_hash({"a": 1.0})


# --------------------------------------------------------------------------- #
# The commands that made a product
# --------------------------------------------------------------------------- #
def test_commands_remake_the_same_product_without_local_paths(survey, tmp_path):
    root = survey["root"]
    product = survey["depth"]          # aa-nc -> aa-sv -> aa-depth -> aa-location
    done = tool(root, "metadata", product, "--json", "--commands")
    lines = [json.loads(line) for line in done.stdout.splitlines() if line.startswith("{")]
    doc, cmds = lines[0], lines[1]
    assert cmds["schema"] == "aa-commands/1"
    assert cmds["command"].startswith("aa-location ")
    script = cmds["script"]
    # Portable: no folder of this machine, in either form.
    for text in (script, cmds["command"]):
        assert str(root) not in text and str(Path.home()) not in text
    assert '"$RAW/D20160703-T060000.raw"' in script and "--echodata" in script
    # Run it elsewhere, from the raw file: the same product hash.
    (tmp_path / "out").mkdir()
    env = _env(tmp_path)
    env.update({"RAW": str(root), "DEST": str(tmp_path / "out"),
                "PATH": str(Path(sys.executable).parent) + os.pathsep + env.get("PATH", "")})
    run = subprocess.run(["bash", "-c", script], cwd=tmp_path, env=env, capture_output=True,
                         text=True, timeout=600, check=False)
    assert run.returncode == 0, run.stderr[-2000:]
    remade = run.stdout.strip().splitlines()[-1]
    again = json.loads(out(tool(root, "metadata", remade, "--json")))
    assert again["product"]["hash"] == doc["product"]["hash"]


def test_flags_for_canonical_settings():
    from aalibrary.console._core import replay

    actions = replay.tool_actions("aa-detect-seafloor")
    argv, _missing = replay.flags_for(actions, {
        "method": "basic", "param": {"channel": "GPT 38 kHz", "threshold": (-40, 10)},
        "range_label": "echo_range"})
    assert argv[:2] == ["--method", "basic"] and "threshold=(-40,10)" in argv
    assert "--range-label" not in argv          # the default: nothing to say
    argv, _ = replay.flags_for(replay.tool_actions("aa-depth"), {
        "downward": False, "use_beam_angles": True, "depth_offset": None})
    assert "--use-beam-angles" in argv and "--no-downward" in argv


def test_a_record_cannot_put_a_command_in_the_script(tmp_path):
    """Names, ops and settings come from a record anyone with write access to
    the bucket could have edited: the script quotes them, so running it runs
    the console tools and nothing else."""
    from aalibrary.console._core import replay

    marker = tmp_path / "pwned"
    doc = {
        "product": {"hash": "ab" * 32, "name": f"x\ntouch {marker}_name #"},
        "sources": [{"id": "aa:raw1", "name": "D1.raw"}],
        "inputs": [],
        "pipeline": [
            {"tool": "aa-annotate", "op": "lines", "product": "a1", "inputs": [],
             "params": {"name": f"my line\ntouch {marker}_label"}},
            {"tool": "aa-nc", "op": f"convert\ntouch {marker}_op #", "product": "p1",
             "inputs": ["aa:raw1"],
             "params": {"sonar_model": f'"$(touch {marker}_param)"'}},
            {"tool": "aa-sv", "op": "x", "product": "p2", "inputs": ["aa:p1", "aa:a1"],
             "params": {}, "recipe_inputs": [{"recipe": "aa:a1", "role": "lines:bottom"}]},
        ],
    }
    script = replay.commands(doc)["script"]
    stubs = tmp_path / "bin"
    stubs.mkdir()
    for name in ("aa-nc", "aa-sv"):
        (stubs / name).write_text('#!/bin/sh\necho "gs://b/out"\n')
        (stubs / name).chmod(0o755)
    env = {"PATH": f"{stubs}{os.pathsep}/usr/bin:/bin", "RAW": str(tmp_path), "DEST": "."}
    run = subprocess.run(["bash", "-c", script], cwd=tmp_path, env=env,
                         capture_output=True, text=True, check=False)
    assert run.returncode == 0, run.stderr + script
    assert not list(tmp_path.glob("pwned*")), script
    # A drawn line's name ("my line …") became a variable the shell accepted.
    assert "MY_LINE" in script
