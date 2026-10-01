"""Write small, valid Simrad EK60 ``.raw`` files for offline tests.

Real survey files come from NCEI's S3 bucket, which tests cannot rely on.
This module builds a file from scratch using echopype's own datagram
packers (``echopype.convert.utils.ek_raw_parsers``), so whatever echopype
writes is exactly what echopype expects to read back:

* one CON0 configuration datagram (split-beam transceivers),
* NME0 datagrams with $GPGGA fixes along a straight track (so
  ``aa-location`` has a position to interpolate),
* RAW0 sample datagrams (power + split-beam angles) for every ping and
  channel, containing a seabed echo, a fish school, and background noise.

Channels may have different sample counts, which is what makes combined
files NaN-padded (the case that broke aa-graph's depth axis).

Usage (CLI)::

    python -m aalibrary.utils.ek60_synth out.raw --pings 60 --start 2016-07-03T06:00:00

Usage (Python)::

    from aalibrary.utils.ek60_synth import write_ek60_raw
    write_ek60_raw("out.raw", n_pings=60)
"""

from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path

import numpy as np
from echopype.convert.utils.ek_raw_parsers import (
    SimradConfigParser,
    SimradNMEAParser,
    SimradRawParser,
)

_NT_EPOCH = dt.datetime(1601, 1, 1, tzinfo=dt.timezone.utc)
# echopype converts raw power with 10*log10(2)/256 dB per count.
_DB_PER_COUNT = 10 * np.log10(2) / 256

DEFAULT_CHANNELS = (
    # frequency Hz, sample count, pulse length s, gain dB, channel id suffix
    (38000.0, 1000, 1.024e-3, 26.5, "ES38B"),
    (120000.0, 800, 1.024e-3, 27.0, "ES120-7C"),
)


def _nt(t: dt.datetime) -> tuple[int, int]:
    """(low, high) 100-ns ticks since 1601-01-01, as Simrad datagrams store time."""
    ticks = int(round((t - _NT_EPOCH).total_seconds() * 1e7))
    return ticks & 0xFFFFFFFF, ticks >> 32


def _pad(text: str, size: int) -> bytes:
    return text.encode("ascii")[:size].ljust(size, b"\x00")


def _pack(parser, data: dict) -> bytes:
    """Serialize one datagram with echopype's packer.

    ``to_string`` validates ``type`` as text but struct-packs it as bytes,
    which fails on Python 3, so call the packer directly with bytes.
    """
    version = int(data["type"][3])
    body = parser._pack_contents(dict(data, type=data["type"].encode()), version=version)
    return parser.finalize_datagram(body)


def _gga(t: dt.datetime, lat: float, lon: float) -> str:
    def dm(value: float, width: int) -> str:
        deg = int(abs(value))
        minutes = (abs(value) - deg) * 60
        return f"{deg:0{width}d}{minutes:07.4f}"

    body = (
        f"GPGGA,{t:%H%M%S}.00,{dm(lat, 2)},{'N' if lat >= 0 else 'S'},"
        f"{dm(lon, 3)},{'E' if lon >= 0 else 'W'},1,08,0.9,0.0,M,0.0,M,,"
    )
    checksum = 0
    for ch in body:
        checksum ^= ord(ch)
    return f"${body}*{checksum:02X}"


def write_ek60_raw(
    path: str | Path,
    *,
    n_pings: int = 60,
    start: dt.datetime | str = "2016-07-03T06:00:00",
    ping_interval_s: float = 2.0,
    channels=DEFAULT_CHANNELS,
    seabed_m: float = 120.0,
    seed: int = 0,
    survey_name: str = "SYNTH",
) -> Path:
    """Write a synthetic EK60 file and return its path."""
    path = Path(path)
    if isinstance(start, str):
        start = dt.datetime.fromisoformat(start)
    if start.tzinfo is None:
        start = start.replace(tzinfo=dt.timezone.utc)
    rng = np.random.default_rng(seed)
    sound_speed = 1500.0

    cfg_parser, raw_parser, nmea_parser = (
        SimradConfigParser(), SimradRawParser(), SimradNMEAParser())

    transceivers = {}
    for i, (freq, _count, pulse, gain, name) in enumerate(channels, start=1):
        transceivers[i] = {
            "channel_id": _pad(f"GPT  {int(freq / 1000):3d} kHz 00907205c{i:03d}-1 {name}", 128),
            "beam_type": 1,
            "frequency": freq,
            "gain": gain,
            "equivalent_beam_angle": -20.7,
            "beamwidth_alongship": 7.0,
            "beamwidth_athwartship": 7.0,
            "angle_sensitivity_alongship": 21.97,
            "angle_sensitivity_athwartship": 21.97,
            "angle_offset_alongship": 0.0,
            "angle_offset_athwartship": 0.0,
            "pos_x": 0.0, "pos_y": 0.0, "pos_z": 0.0,
            "dir_x": 0.0, "dir_y": 0.0, "dir_z": 0.0,
            "pulse_length_table": [2.56e-4, 5.12e-4, 1.024e-3, 2.048e-3, 4.096e-3],
            "spare1": _pad("", 8),
            "gain_table": [gain] * 5,
            "spare2": _pad("", 8),
            "sa_correction_table": [-0.7] * 5,
            "spare3": _pad("", 8),
            "gpt_software_version": _pad("070413", 16),
            "spare4": _pad("", 28),
        }

    low, high = _nt(start)
    out = bytearray()
    out += _pack(cfg_parser, {
        "type": "CON0", "low_date": low, "high_date": high,
        "survey_name": _pad(survey_name, 128),
        "transect_name": _pad("T1", 128),
        "sounder_name": _pad("ER60", 128),
        "version": _pad("2.4.3", 30),
        "spare0": _pad("", 98),
        "transceiver_count": len(transceivers),
        "transceivers": transceivers,
    })

    lat0, lon0 = 41.5, -69.0
    for p in range(n_pings):
        t = start + dt.timedelta(seconds=p * ping_interval_s)
        low, high = _nt(t)
        lat, lon = lat0 + p * 1e-4, lon0 + p * 1.5e-4
        out += _pack(nmea_parser, {
            "type": "NME0", "low_date": low, "high_date": high,
            "nmea_string": _gga(t, lat, lon),
        })
        school_on = n_pings // 3 <= p < 2 * n_pings // 3
        for i, (freq, count, pulse, _gain, _name) in enumerate(channels, start=1):
            dr = pulse / 4 * sound_speed / 2          # metres per sample
            r = np.arange(count) * dr
            noise_db = -150.0 + 20 * np.log10(np.maximum(r, 1.0)) + 2 * freq / 1e5
            power_db = noise_db + rng.normal(0, 1.0, count)
            power_db[r < 3] = -40.0                    # transmit pulse / near field
            bottom = np.abs(r - (seabed_m + 3 * np.sin(p / 7))) < 1.5
            power_db[bottom] = -35.0
            if school_on:
                school = (r > 40) & (r < 55)
                power_db[school] = np.maximum(power_db[school], -80.0 + rng.normal(0, 2, school.sum()))
            power = np.clip(np.round(power_db / _DB_PER_COUNT), -32768, 32767).astype(np.int16)
            angle = (rng.integers(-3, 4, count).astype(np.uint16) & 0xFF) * 256 + (
                rng.integers(-3, 4, count).astype(np.uint16) & 0xFF)
            out += _pack(raw_parser, {
                "type": "RAW0", "low_date": low, "high_date": high,
                "channel": i, "mode": 3,
                "transducer_depth": 5.0, "frequency": freq,
                "transmit_power": 2000.0, "pulse_length": pulse,
                "bandwidth": 2425.0, "sample_interval": pulse / 4,
                "sound_velocity": sound_speed,
                "absorption_coefficient": 0.0098 if freq < 50e3 else 0.0378,
                "heave": 0.0, "roll": 0.0, "pitch": 0.0, "temperature": 10.0,
                "heading": 45.0, "transmit_mode": 0, "spare0": _pad("", 6),
                "offset": 0, "count": count,
                "power": power.tolist(), "angle": angle.tolist(),
            })

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(bytes(out))
    return path


def _main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("out", type=Path)
    ap.add_argument("--pings", type=int, default=60)
    ap.add_argument("--start", default="2016-07-03T06:00:00")
    ap.add_argument("--interval", type=float, default=2.0)
    ap.add_argument("--seed", type=int, default=0)
    a = ap.parse_args()
    print(write_ek60_raw(a.out, n_pings=a.pings, start=a.start,
                         ping_interval_s=a.interval, seed=a.seed).resolve())


if __name__ == "__main__":
    _main()
