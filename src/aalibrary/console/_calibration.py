"""Calibration values for aa-sv, aa-ts and aa-ecs: overrides, ECS files, reports.

Three ways a calibration value reaches echopype, and the rules between them:

* **The file's own values** (gain from the transceiver's gain table, the
  equivalent beam angle, the sound speed and absorption the sounder used...).
  echopype reads these when nothing overrides them.
* **An Echoview calibration supplement (.ECS)**: per-transducer values, matched
  to channels by frequency, applied with Echoview's FileSet < SourceCal <
  LocalCal hierarchy. echopype reads it itself (``ecs_file=``). When an ECS is
  given, echopype ignores ``env_params`` and ``cal_params``, so the tools refuse
  the combination rather than silently dropping one of them.
* **Overrides on the command line**: ``--cal-param gain_correction=26.2``
  (every channel) or ``--cal-param gain_correction@38kHz=26.2`` (one channel),
  and the same for ``--env-param``.

:func:`report` says what echopype will actually use, per channel, by building
echopype's own calibrator (never a re-implementation of its rules), and
:func:`write_ecs` writes an ECS that echopype's ECS parser reads back.
"""

from __future__ import annotations

import datetime as _dt
import math
import re
from pathlib import Path
from typing import Any

# --------------------------------------------------------------------------- #
# Names
# --------------------------------------------------------------------------- #
#: echopype calibration parameter -> (ECS name, unit shown in the ECS comment).
#: The mapping is echopype's own (echopype.calibrate.ecs.EV_EP_MAP), inverted.
CAL_ECS: dict[str, tuple[str, str]] = {
    "gain_correction": ("TransducerGain", "(decibels)"),
    "sa_correction": ("SaCorrectionFactor", "(decibels)"),
    "equivalent_beam_angle": ("TwoWayBeamAngle", "(decibels re 1 steradian)"),
    "beamwidth_athwartship": ("MajorAxis3dbBeamAngle", "(degrees)"),
    "beamwidth_alongship": ("MinorAxis3dbBeamAngle", "(degrees)"),
    "angle_offset_athwartship": ("MajorAxisAngleOffset", "(degrees)"),
    "angle_offset_alongship": ("MinorAxisAngleOffset", "(degrees)"),
    "angle_sensitivity_athwartship": ("MajorAxisAngleSensitivity", ""),
    "angle_sensitivity_alongship": ("MinorAxisAngleSensitivity", ""),
}
ENV_ECS: dict[str, tuple[str, str]] = {
    "sound_speed": ("SoundSpeed", "(meters per second)"),
    "sound_absorption": ("AbsorptionCoefficient", "(decibels per meter)"),
    # EK80 only (echopype reads these from an ECS for EK80 data alone).
    "temperature": ("Temperature", "(degrees C)"),
    "salinity": ("Salinity", "(parts per thousand)"),
    "pressure": ("AbsorptionDepth", "(meters)"),
    "pH": ("Acidity", "(pH)"),
}
EK80_ONLY = {"temperature", "salinity", "pressure", "pH"}

#: Human labels and units, for reports and the Workbench.
LABELS: dict[str, tuple[str, str]] = {
    "gain_correction": ("Transducer gain", "dB"),
    "sa_correction": ("Sa correction", "dB"),
    "equivalent_beam_angle": ("Equivalent beam angle", "dB re 1 sr"),
    "beamwidth_alongship": ("Beam width, alongship", "°"),
    "beamwidth_athwartship": ("Beam width, athwartship", "°"),
    "angle_offset_alongship": ("Angle offset, alongship", "°"),
    "angle_offset_athwartship": ("Angle offset, athwartship", "°"),
    "angle_sensitivity_alongship": ("Angle sensitivity, alongship", ""),
    "angle_sensitivity_athwartship": ("Angle sensitivity, athwartship", ""),
    "sound_speed": ("Sound speed", "m/s"),
    "sound_absorption": ("Absorption", "dB/m"),
    "temperature": ("Temperature", "°C"),
    "salinity": ("Salinity", "PSU"),
    "pressure": ("Pressure", "dbar"),
    "pH": ("pH", ""),
    "impedance_transducer": ("Transducer impedance", "Ω"),
    "impedance_transceiver": ("Transceiver impedance", "Ω"),
    "receiver_sampling_frequency": ("Receiver sampling frequency", "Hz"),
}

TEXT_ENV = {"formula_sound_speed", "formula_absorption"}

_FREQ = re.compile(r"^\s*(?P<num>\d+(?:\.\d+)?)\s*(?P<unit>[kKmMgG]?)(?:[hH][zZ])?\s*$")


def parse_frequency(text: str) -> float:
    """'38kHz' / '38 kHz' / '38000' / '120k' -> Hz."""
    m = _FREQ.match(str(text))
    if not m:
        raise ValueError(f"not a frequency: {text!r} (e.g. 38kHz or 38000)")
    scale = {"": 1.0, "k": 1e3, "m": 1e6, "g": 1e9}[m.group("unit").lower()]
    return float(m.group("num")) * scale


def parse_kv(pairs: list[str] | None, *, text_keys: set[str] = frozenset()) -> dict[str, Any] | None:
    """['gain_correction=26.2', 'sa_correction@38kHz=-0.68'] -> {key: value}.

    A key may name one channel by frequency (``name@38kHz``); the key is kept
    in the canonical spelling ``name@38000`` so the hash does not depend on
    how the frequency was written. None when there are no pairs.
    """
    if not pairs:
        return None
    out: dict[str, Any] = {}
    for pair in pairs:
        if "=" not in pair:
            raise ValueError(f"expected KEY=VALUE, got {pair!r}")
        key, value = (part.strip() for part in pair.split("=", 1))
        name, _, freq = key.partition("@")
        name = name.strip()
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
            raise ValueError(f"not a parameter name: {name!r}")
        if freq:
            hz = parse_frequency(freq)
            key = f"{name}@{int(hz) if hz.is_integer() else hz}"
        else:
            key = name
        if name in text_keys:
            out[key] = value
            continue
        try:
            number = float(value)
        except ValueError:
            raise ValueError(f"{key}: {value!r} is not a number") from None
        if math.isnan(number) or math.isinf(number):
            raise ValueError(f"{key}: must be a finite number")
        out[key] = number
    return out


# --------------------------------------------------------------------------- #
# echopype's calibrator
# --------------------------------------------------------------------------- #
def calibrator(ed, *, env: dict | None = None, cal: dict | None = None,
               ecs: str | Path | None = None, waveform_mode: str | None = None,
               encode_mode: str | None = None):
    """echopype's calibration object for this EchoData, as compute_Sv builds it."""
    from echopype.calibrate.api import CALIBRATOR

    model = ed.sonar_model
    if model in ("EK80", "ES80", "EA640"):
        if not waveform_mode or not encode_mode:
            raise ValueError("EK80 data needs --waveform_mode and --encode_mode")
        waveform_mode = "BB" if waveform_mode == "FM" else waveform_mode
        return CALIBRATOR[model](ed, env_params=env, cal_params=cal, ecs_file=ecs,
                                 waveform_mode=waveform_mode, encode_mode=encode_mode)
    return CALIBRATOR[model](ed, env_params=env, cal_params=cal,
                             ecs_file=str(ecs) if ecs else None)


def _channels(cal_obj) -> list[tuple[str, float]]:
    beam = cal_obj.beam
    channels = [str(c) for c in beam["channel"].values]
    freqs = [float(f) for f in beam["frequency_nominal"].values]
    return list(zip(channels, freqs))


def _scalar(value, channel: str):
    """One channel's value of a parameter, reduced over pings (median)."""
    import numpy as np
    import xarray as xr

    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, xr.DataArray):
        da = value
        if "channel" in da.dims:
            try:
                da = da.sel(channel=channel)
            except (KeyError, ValueError):
                return None
        extra = [d for d in da.dims]
        if extra:
            data = np.asarray(da.values, dtype=float)
            if data.size == 0 or np.all(np.isnan(data)):
                return None
            return float(np.nanmedian(data))
        try:
            return float(da.values)
        except (TypeError, ValueError):
            return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def summarize(cal_obj) -> list[dict]:
    """Per channel: the calibration and environment values echopype will use."""
    out = []
    for channel, freq in _channels(cal_obj):
        cal = {}
        for name, value in (cal_obj.cal_params or {}).items():
            v = _scalar(value, channel)
            if v is not None and not (isinstance(v, float) and math.isnan(v)):
                cal[name] = v
        env = {}
        for name, value in (cal_obj.env_params or {}).items():
            v = _scalar(value, channel)
            if v is not None and not (isinstance(v, float) and math.isnan(v)):
                env[name] = v
        out.append({"channel": channel, "frequency": freq, "cal": cal, "env": env})
    return out


def overrides_for_echopype(ed, kv: dict | None, *, kind: str, base: list[dict] | None,
                           waveform_mode=None, encode_mode=None) -> dict | None:
    """--cal-param / --env-param values as echopype takes them.

    Plain keys go through as scalars (every channel). ``name@freq`` keys need
    a per-channel array: the other channels keep the value echopype would
    have used (``base``, from :func:`summarize` without overrides).
    """
    if not kv:
        return None
    import xarray as xr

    plain = {k: v for k, v in kv.items() if "@" not in k}
    per = {k: v for k, v in kv.items() if "@" in k}
    if not per:
        return plain
    if base is None:
        raise ValueError("per-channel overrides need the file's values")
    out: dict[str, Any] = dict(plain)
    names = sorted({k.split("@", 1)[0] for k in per})
    channels = [b["channel"] for b in base]
    freqs = [b["frequency"] for b in base]
    for name in names:
        values = []
        for b in base:
            if name in plain:
                values.append(plain[name])
            else:
                values.append(b[kind].get(name, float("nan")))
        for key, value in per.items():
            k_name, _, k_freq = key.partition("@")
            if k_name != name:
                continue
            hz = float(k_freq)
            hits = [i for i, f in enumerate(freqs) if abs(f - hz) < 0.5]
            if not hits:
                known = ", ".join(f"{f / 1000:g} kHz" for f in freqs)
                raise ValueError(f"{key}: no channel at {hz / 1000:g} kHz (channels: {known})")
            for i in hits:
                values[i] = value
        out[name] = xr.DataArray(values, dims=["channel"], coords={"channel": channels})
    return out


def report(ed, *, ecs=None, env_kv=None, cal_kv=None, waveform_mode=None,
           encode_mode=None) -> dict:
    """What echopype will use for this EchoData: per channel, the file's own
    values and the values with the given ECS / overrides, side by side."""
    file_obj = calibrator(ed, waveform_mode=waveform_mode, encode_mode=encode_mode)
    file_values = summarize(file_obj)
    used = file_values
    source = "file"
    if ecs or env_kv or cal_kv:
        env = overrides_for_echopype(ed, env_kv, kind="env", base=file_values)
        cal = overrides_for_echopype(ed, cal_kv, kind="cal", base=file_values)
        used = summarize(calibrator(ed, env=env, cal=cal, ecs=ecs,
                                    waveform_mode=waveform_mode, encode_mode=encode_mode))
        source = "ecs" if ecs else "overrides"
    channels = []
    for f, u in zip(file_values, used):
        names = list(dict.fromkeys([*f["cal"], *f["env"], *u["cal"], *u["env"]]))
        values = []
        for name in names:
            group = "cal" if name in f["cal"] or name in u["cal"] else "env"
            label, unit = LABELS.get(name, (name.replace("_", " "), ""))
            in_file = f[group].get(name)
            now = u[group].get(name)
            values.append({
                "name": name, "group": group, "label": label, "unit": unit,
                "file": in_file, "used": now,
                "changed": _differs(in_file, now),
                "ecs": (CAL_ECS.get(name) or ENV_ECS.get(name) or ("", ""))[0],
            })
        channels.append({"channel": f["channel"], "frequency": f["frequency"], "values": values})
    return {
        "sonarModel": ed.sonar_model,
        "source": source,
        "waveformMode": waveform_mode or "",
        "encodeMode": encode_mode or "",
        "channels": channels,
    }


def _differs(a, b) -> bool:
    if a is None or b is None:
        return a is not b
    if isinstance(a, str) or isinstance(b, str):
        return str(a) != str(b)
    # float32 in the file vs float64 from an ECS: equal to float32 precision.
    return not math.isclose(float(a), float(b), rel_tol=2e-7, abs_tol=1e-12)


# --------------------------------------------------------------------------- #
# ECS files
# --------------------------------------------------------------------------- #
_SEP = "#" + "=" * 88 + "#"


def _boxed(text: str) -> str:
    """'#  text   ...   #' padded to the separator's width."""
    inner = f"  {text}"
    return "#" + inner.ljust(88)[:88] + "#"


def _num(value: float) -> str:
    """Fixed-point with a decimal point: echopype's ECS parser reads no
    exponent and needs a '.' in a negative number. Seven significant digits:
    the files store float32, whose noise (21.96999931) is not a value."""
    value = float(f"{float(value):.7g}")
    text = f"{value:.10f}".rstrip("0")
    return text + "0" if text.endswith(".") else text


def write_ecs(path: str | Path, channels: list[dict], *, sonar_model: str = "EK60",
              notes: list[str] | None = None, now: _dt.datetime | None = None) -> Path:
    """Write an ECS: one SourceCal per channel (T1, T2, ...), matched by frequency.

    ``channels``: [{"frequency": Hz, "values": {echopype name: number}}].
    Environment values that are the same on every channel go in FileSet;
    everything else is per transducer. Values echopype would not read for
    this sonar (temperature etc. on EK60) are left out rather than written
    and ignored.
    """
    path = Path(path)
    now = now or _dt.datetime.now(_dt.timezone.utc)
    ek80 = sonar_model.upper() in ("EK80", "ES80", "EA640")
    data_type = "SimradEK80Raw" if ek80 else "SimradEK60Raw"
    known = dict(CAL_ECS)
    for name, item in ENV_ECS.items():
        if ek80 or name not in EK80_ONLY:
            known[name] = item

    def usable(values: dict) -> dict:
        return {k: float(v) for k, v in values.items()
                if k in known and v is not None and not (isinstance(v, float) and math.isnan(v))}

    rows = [usable(ch.get("values") or {}) for ch in channels]
    common = {}
    if rows:
        for name in ENV_ECS:
            vals = [r.get(name) for r in rows]
            if name != "sound_absorption" and all(v is not None for v in vals) and \
                    all(math.isclose(v, vals[0], rel_tol=0, abs_tol=1e-9) for v in vals):
                common[name] = vals[0]

    notes = [n for n in (notes or []) if n.strip()][:6]
    lines = [
        _SEP,
        _boxed(f"ECHOVIEW CALIBRATION SUPPLEMENT (.ECS) FILE ({data_type})"),
        _boxed(now.strftime("%m/%d/%Y %H:%M:%S")),
        _SEP,
    ]
    body_notes = (notes + [""] * 6)[:6]
    lines += [_boxed(n) for n in body_notes]
    lines += [_SEP, "", "Version 1.00", ""]

    lines += [_SEP, _boxed("FILESET SETTINGS"), _SEP, ""]
    for name, value in common.items():
        ecs_name, unit = known[name]
        lines.append(f"  {ecs_name} = {_num(value)} # {unit}".rstrip())
    lines.append("")

    lines += [_SEP, _boxed("SOURCECAL SETTINGS"), _SEP, ""]
    for i, (ch, row) in enumerate(zip(channels, rows), start=1):
        lines.append(f"SourceCal T{i}")
        lines.append(f"  Frequency = {_num(float(ch['frequency']) / 1000)} # (kilohertz)")
        for name, value in row.items():
            if name in common:
                continue
            ecs_name, unit = known[name]
            lines.append(f"  {ecs_name} = {_num(value)} # {unit}".rstrip())
        lines.append("")

    lines += [_SEP, _boxed("LOCALCAL SETTINGS"), _SEP, ""]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def read_ecs(path: str | Path) -> dict:
    """An ECS as echopype interprets it: per source, echopype names where known."""
    from echopype.calibrate.ecs import ECSParser, EV_EP_MAP

    parser = ECSParser(str(path))
    parser.parse()
    resolved = parser.get_cal_params()
    to_ep = {**EV_EP_MAP["EK60"], **EV_EP_MAP["EK80"]}
    sources = []
    for source, values in resolved.items():
        mapped = {}
        frequency = None
        for key, value in values.items():
            if key == "Frequency":
                frequency = float(value) * 1000
                continue
            name = to_ep.get(key, key)
            try:
                mapped[name] = float(value)
            except (TypeError, ValueError):
                mapped[name] = value if isinstance(value, str) else str(value)
        sources.append({"source": source, "frequency": frequency, "values": mapped})
    return {"dataType": parser.data_type or "", "version": parser.version or "",
            "sources": sources}


__all__ = [
    "CAL_ECS", "ENV_ECS", "LABELS", "TEXT_ENV", "parse_frequency", "parse_kv", "calibrator",
    "summarize", "overrides_for_echopype", "report", "write_ecs", "read_ecs",
]
