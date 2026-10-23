"""Reading a raw file with the expansion on disk, a block of pings at a time.

_core.rawio replaces echopype's ParseEK._parse_and_pad_datagram while it
swaps. These tests hold it to echopype's own results, array for array, on
pings built to cover what EK60 and EK80 files hold: power, split-beam angles
and complex samples with transducer sectors (EK80), receive and transmit,
pings of different lengths, empty pings (one of them first in a block), and a
channel with fewer pings than another.
"""

from __future__ import annotations

import copy
from collections import defaultdict

import numpy as np
import pytest

from aalibrary.console._core import rawio

pb = pytest.importorskip("echopype.convert.parse_base")
from echopype.convert.utils.ek_swap import calc_final_shapes  # noqa: E402

DATA_TYPES = ["power", "angle", "complex"]


def _pings():
    rng = np.random.default_rng(7)
    lengths = rng.integers(60, 100, size=47)
    receive = {
        "power": {
            "A": [rng.integers(-2000, 2000, size=n).astype("int16") for n in lengths],
            "B": [rng.integers(-2000, 2000, size=120).astype("int16") for _ in range(30)],
        },
        "angle": {
            "A": [rng.integers(-60, 60, size=(n, 2)).astype("int8") for n in lengths],
            "B": [rng.integers(-60, 60, size=(120, 2)).astype("int8") for _ in range(30)],
        },
        "complex": {
            "C": [(rng.normal(size=(n, 4)) + 1j * rng.normal(size=(n, 4))).astype("complex64")
                  for n in rng.integers(40, 90, size=33)],
        },
    }
    # Empty pings, in the middle of a channel and where a block will start;
    # and an exact zero imaginary part, which echopype turns into NaN.
    receive["power"]["A"][9] = np.array([], dtype="int16")
    receive["angle"]["A"][9] = np.zeros((0, 2), dtype="int8")
    receive["power"]["A"][24] = np.array([], dtype="int16")
    receive["complex"]["C"][5][3, 1] = 1.5 + 0j
    transmit = {"complex": {"C": [(np.ones((8, 4)) * (1 + 2j)).astype("complex64")
                                  for _ in range(33)]}}
    return receive, transmit


def _parser(receive, transmit):
    p = pb.ParseEK.__new__(pb.ParseEK)
    p.ping_data_dict = defaultdict(lambda: defaultdict(list))
    p.ping_data_dict_tx = defaultdict(lambda: defaultdict(list))
    for target, source in ((p.ping_data_dict, receive), (p.ping_data_dict_tx, transmit)):
        for data_type, channels in source.items():
            for ch, arrays in channels.items():
                target[data_type][ch] = list(arrays)
    for data_type in DATA_TYPES:  # what parse_raw leaves for absent types
        p.ping_data_dict[data_type]
        p.ping_data_dict_tx[data_type]
    p.ch_ids = defaultdict(list)
    p.ping_time = {ch: [0] * len(arrays) for channels in receive.values()
                   for ch, arrays in channels.items()}
    return p


def _run(method, raw_type, receive, transmit, tmp_path, name):
    import zarr

    p = _parser(copy.deepcopy(receive), copy.deepcopy(transmit))
    source = p.ping_data_dict_tx if raw_type == "transmit" else p.ping_data_dict
    shapes = calc_final_shapes(DATA_TYPES, source)
    root = zarr.group(store=str(tmp_path / f"{name}.zarr"), overwrite=True)
    for data_type in DATA_TYPES:
        method(p, data_type, shapes, raw_type, True, root, "100MB")
    out = {}
    for data_type in DATA_TYPES:
        for ch, value in (source[data_type] or {}).items():
            if isinstance(value, dict):
                for part, arr in value.items():
                    out[(data_type, ch, part)] = np.asarray(arr)
            else:
                out[(data_type, ch)] = None if value is None else np.asarray(value)
    return out, dict(p.ch_ids)


@pytest.mark.parametrize("raw_type", ["receive", "transmit"])
@pytest.mark.parametrize("block_bytes", [3000, rawio.BLOCK_BYTES])
def test_blocks_give_echopypes_own_arrays(raw_type, block_bytes, tmp_path, monkeypatch):
    receive, transmit = _pings()
    original = pb.ParseEK._parse_and_pad_datagram
    monkeypatch.setattr(rawio, "BLOCK_BYTES", block_bytes)  # small: many blocks
    theirs, their_ids = _run(original, raw_type, receive, transmit, tmp_path, "theirs")
    ours, our_ids = _run(rawio._blockwise(original), raw_type, receive, transmit, tmp_path,
                         "ours")
    assert our_ids == their_ids
    assert ours.keys() == theirs.keys() and ours
    for key, expected in theirs.items():
        got = ours[key]
        if expected is None:
            assert got is None, key
            continue
        assert got.shape == expected.shape and got.dtype == expected.dtype, key
        np.testing.assert_array_equal(got, expected, err_msg=str(key))


def test_without_swap_echopypes_own_runs(tmp_path):
    calls = []

    def original(self, *args):
        calls.append(args[3])

    rawio._blockwise(original)(object(), "power", {}, "receive", False, None, "100MB")
    assert calls == [False]


def test_large_files_are_expanded_on_disk(tmp_path):
    small, large = tmp_path / "s.raw", tmp_path / "l.raw"
    small.write_bytes(b"x" * 10)
    with open(large, "wb") as handle:
        handle.truncate(rawio.SWAP_ABOVE_BYTES)
    assert rawio.use_swap_for(small) == "auto" and rawio.use_swap_for(large) is True
    assert rawio.use_swap_for(large, "off") is False and rawio.use_swap_for(small, "on") is True


def test_the_swap_is_deleted_and_never_in_the_system_temp(tmp_path, monkeypatch):
    import echopype.utils.io as epio
    from aalibrary.utils.ek60_synth import write_ek60_raw

    monkeypatch.setenv("AA_CACHE_DIR", str(tmp_path / "cache"))
    raw = write_ek60_raw(tmp_path / "D20160703-T060000.raw", n_pings=40)
    system_temp = epio.ECHOPYPE_TEMP_DIR
    with rawio.open_raw(raw, "EK60", swap="on") as ed:
        swaps = list((tmp_path / "cache").glob("aa-swap-*"))
        assert len(swaps) == 1 and any(swaps[0].rglob("*"))
        assert ed["Sonar/Beam_group1"]["backscatter_r"].shape[1] == 40
    assert not list((tmp_path / "cache").glob("aa-swap-*"))
    assert epio.ECHOPYPE_TEMP_DIR == system_temp
