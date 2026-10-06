#!/usr/bin/env python3
"""
aa-ecs

Calibration values for EchoData, and Echoview calibration supplement (.ECS)
files to carry them.

    aa-ecs ED.nc                       what echopype will use, per channel
    aa-ecs ED.nc --ecs cal.ecs         ... with this ECS (file vs used)
    aa-ecs cal.ecs                     an ECS as echopype reads it
    aa-ecs ED.nc --write [overrides]   write an ECS: the file's values, with
                                       --cal-param / --env-param / --values
                                       applied; prints its path

An ECS written here is read back by echopype (aa-sv --ecs, aa-ts --ecs) and
by Echoview. It is a product: named <base>_<label>_<hash8>.ecs, with its
provenance in <file>.aa.json, and its hash is its content, so the same values
are the same calibration wherever they were written.
"""

from __future__ import annotations

import logging
import sys
import warnings

logging.disable(logging.CRITICAL)
warnings.filterwarnings("ignore")

from loguru import logger  # noqa: E402

logger.remove()
logger.add(sys.stderr, level="WARNING")

import argparse  # noqa: E402
import hashlib  # noqa: E402
import json  # noqa: E402
from pathlib import Path  # noqa: E402

from aalibrary.console._core import (  # noqa: E402
    Help, Run, ToolSpec, add_common_flags, canon, identity, naming, render, show_help,
    stdio, uris,
)

SPEC = ToolSpec(
    name="aa-ecs",
    role="transform",
    kind="calibration",
    op="aa_ecs.write",
    op_version=1,
    engines=("echopype",),
    # Only --write makes a product, and its hash is the values written (passed
    # to Run explicitly); the EchoData they were read from is recorded, not
    # hashed, so the same values are the same calibration.
    params={"label": canon.text},
    ext=".ecs",
)

HELP = Help(
    summary="Show the calibration echopype will use; write it as an Echoview .ecs file.",
    does=(
        "Builds echopype's own calibrator for the EchoData (as compute_Sv does) and "
        "reports, per channel, each calibration and environment value: the one in "
        "the file and the one that will be used with an ECS or overrides.\n\n"
        "--write writes those values as an Echoview calibration supplement (.ECS): "
        "one SourceCal per channel, matched by frequency; environment values shared "
        "by every channel in FileSet. echopype (aa-sv --ecs) and Echoview read it. "
        "Every channel gets every value, so the ECS never leaves a channel to "
        "echopype's NaN for a missing entry.\n\n"
        "Given an .ecs file instead of EchoData, prints the file as echopype "
        "interprets it (FileSet < SourceCal < LocalCal)."
    ),
    stdin="One EchoData .nc/.zarr (or an .ecs) path or gs:// URI.",
    stdout=(
        "Show: a table, or one line of JSON with --json. --write: the ECS file's "
        "path or gs:// URI."
    ),
    options=[
        ("--ecs FILE", "apply this ECS (local or gs://) when showing, or start from it "
                       "when writing"),
        ("--env-param KEY=VALUE", "environment override; KEY@38kHz=VALUE for one channel"),
        ("--cal-param KEY=VALUE", "calibration override; KEY@38kHz=VALUE for one channel"),
        ("--values FILE.json", "values to write, {\"channels\": [{\"frequency\": Hz, "
                               "\"values\": {name: number}}]} (the Workbench's form)"),
        ("--write", "write an ECS instead of showing"),
        ("--label TEXT", "name part of the ECS (default cal): <base>_<label>_<hash8>.ecs"),
        ("--json", "show as one line of JSON"),
        ("--waveform_mode / --encode_mode", "EK80 only, as for aa-sv"),
        ("-o, --output_path PATH", "exact path for --write; local or gs://"),
    ],
    science={"label": "Part of the name; recorded."},
    files=(
        "Reads EchoData .nc/.zarr and .ecs, local or gs://. --write writes "
        "<base>_<label>_<hash8>.ecs beside the input, in --dest DIR|gs://PREFIX, or "
        "at -o, with <file>.aa.json. An identical ECS already there is reused."
    ),
    pipeline="Before aa-sv: aa-ecs ED.nc --write --cal-param gain_correction@38kHz=26.1 "
             "then aa-sv ED.nc --ecs <that file>.",
    examples=[
        "aa-ecs HB1603.nc",
        "aa-ecs HB1603.nc --ecs cal.ecs --json",
        "aa-ecs HB1603.nc --write --cal-param gain_correction@38kHz=26.12 --dest gs://b/cal/",
        "aa-ecs cal.ecs",
    ],
    hash_note="--write: the hash is the values written (every channel, every value), "
              "not the options that produced them.",
)


def print_help():
    sys.stdout.write(render(SPEC, HELP, _build_parser()))


def _build_parser():
    p = argparse.ArgumentParser(prog="aa-ecs", add_help=False,
                                description="Calibration values and ECS files.")
    p.add_argument("input_path", type=str, nargs="?")
    p.add_argument("-o", "--output_path", type=str, default=None)
    p.add_argument("--ecs", type=str, default=None, metavar="FILE")
    p.add_argument("--env-param", dest="env_params", action="append", default=None,
                   metavar="KEY=VALUE")
    p.add_argument("--cal-param", dest="cal_params", action="append", default=None,
                   metavar="KEY=VALUE")
    p.add_argument("--values", type=str, default=None, metavar="FILE.json")
    p.add_argument("--write", action="store_true")
    p.add_argument("--label", type=str, default="cal")
    p.add_argument("--json", action="store_true")
    p.add_argument("--waveform_mode", choices=["CW", "BB", "FM"], default=None)
    p.add_argument("--encode_mode", choices=["complex", "power"], default=None)
    p.add_argument("--quiet", action="store_true")
    add_common_flags(p)
    return p


def _open(path: Path):
    import echopype as ep

    return ep.open_converted(str(path), chunks={})


def _local(token: str) -> Path:
    if uris.is_gcs(token):
        return uris.localize(token).path
    return Path(uris.from_file_uri(token)).expanduser()


def _table(rep: dict) -> str:
    rows = []
    for ch in rep["channels"]:
        rows.append(f"{ch['frequency'] / 1000:g} kHz  {ch['channel']}")
        for v in ch["values"]:
            unit = f" {v['unit']}" if v["unit"] else ""
            mark = "  *" if v["changed"] else ""
            used = "-" if v["used"] is None else f"{v['used']:.6g}"
            file = "-" if v["file"] is None else f"{v['file']:.6g}"
            rows.append(f"    {v['label']:<32} {used:>14}{unit}"
                        + (f"   (file {file}){mark}" if v["changed"] else ""))
    head = f"{rep['sonarModel']}: calibration from {rep['source']}"
    return head + "\n" + "\n".join(rows) + "\n"


def _merge_values(base: list[dict], values_doc: dict | None) -> list[dict]:
    """The used values (from report) with the --values document on top."""
    out = []
    by_freq = {}
    for item in (values_doc or {}).get("channels") or []:
        try:
            by_freq[round(float(item["frequency"]))] = item.get("values") or {}
        except (KeyError, TypeError, ValueError):
            raise ValueError("--values: each channel needs a frequency (Hz) and values")
    for ch in base:
        values = {v["name"]: v["used"] for v in ch["values"]
                  if isinstance(v["used"], (int, float))}
        for name, value in by_freq.get(round(ch["frequency"]), {}).items():
            if value is None:
                continue
            values[name] = float(value)
        out.append({"frequency": ch["frequency"], "channel": ch["channel"], "values": values})
    return out


def _canonical(channels: list[dict]) -> list[dict]:
    """What the hash covers: frequency and values, rounded past float noise."""
    from aalibrary.console._calibration import CAL_ECS, ENV_ECS

    known = set(CAL_ECS) | set(ENV_ECS)
    return [{"frequency": round(float(c["frequency"]), 3),
             "values": {k: float(f"{v:.7g}") for k, v in sorted(c["values"].items())
                        if k in known}}
            for c in sorted(channels, key=lambda c: c["frequency"])]


def main() -> None:
    if len(sys.argv) == 1 and not stdio.stdin_is_piped():
        print_help()
        sys.exit(0)
    parser = _build_parser()
    if show_help(SPEC, HELP, parser):
        sys.exit(0)
    args = parser.parse_args()

    from aalibrary.console import _calibration as calib

    try:
        env_kv = calib.parse_kv(args.env_params, text_keys=calib.TEXT_ENV)
        cal_kv = calib.parse_kv(args.cal_params)
    except ValueError as exc:
        print(f"aa-ecs: {exc}", file=sys.stderr)
        sys.exit(2)
    if args.ecs and (env_kv or cal_kv):
        print("aa-ecs: --ecs cannot be combined with --env-param/--cal-param "
              "(echopype ignores overrides when it has an ECS); use --values to "
              "change values on top of an ECS.", file=sys.stderr)
        sys.exit(2)

    token = stdio.one_input(args.input_path, SPEC.name)
    if uris.basename(token).lower().endswith(".ecs"):
        try:
            doc = calib.read_ecs(_local(token))
        except Exception as exc:  # noqa: BLE001
            print(f"aa-ecs: cannot read {token}: {exc}", file=sys.stderr)
            sys.exit(1)
        if args.json:
            print(json.dumps(doc))
        else:
            for src in doc["sources"]:
                freq = f"{src['frequency'] / 1000:g} kHz" if src["frequency"] else "?"
                print(f"{src['source']} ({freq})")
                for k, v in src["values"].items():
                    print(f"    {k:<32} {v}")
        return

    try:
        ed_local = _local(token)
        ecs_local = _local(args.ecs) if args.ecs else None
        ed = _open(ed_local)
        rep = calib.report(ed, ecs=ecs_local, env_kv=env_kv, cal_kv=cal_kv,
                           waveform_mode=args.waveform_mode, encode_mode=args.encode_mode)
    except Exception as exc:  # noqa: BLE001
        print(f"aa-ecs: {type(exc).__name__}: {exc}", file=sys.stderr)
        sys.exit(1)

    if not args.write:
        rep["input"] = token
        rep["ecs"] = args.ecs or ""
        if args.json:
            print(json.dumps(rep))
        else:
            sys.stdout.write(_table(rep))
        return

    values_doc = None
    if args.values:
        try:
            values_doc = json.loads(_local(args.values).read_text())
        except (OSError, ValueError) as exc:
            print(f"aa-ecs: --values: {exc}", file=sys.stderr)
            sys.exit(2)
    try:
        channels = _merge_values(rep["channels"], values_doc)
    except ValueError as exc:
        print(f"aa-ecs: {exc}", file=sys.stderr)
        sys.exit(2)
    content = _canonical(channels)
    label = naming.sanitize_base(args.label or "cal").replace(" ", "_")

    # The EchoData gives the base and is recorded; it does not enter the hash.
    probe = Run(SPEC, None)
    ref = probe.input(token)
    run = Run(SPEC, args, base=ref.base,
              params={"values": content, "sonar_model": rep["sonarModel"]})
    digest = hashlib.sha256(canon.dumps(content).encode()).hexdigest()
    explicit = args.output_path or None
    recipe = identity.recipe_hash(run.recipe_doc())
    name = naming.derived_name(f"{run.base()}_{label}", recipe, ".ecs")
    out = run.plan(ext=".ecs", explicit=explicit, name=name,
                   directory=None if (args.dest or explicit) else
                   (str(ref.local.parent) if ref.via == "local" else None))
    if run.reusable(out):
        run.finish(out)
        return
    try:
        notes = [f"Written by aa-ecs for {ref.name}",
                 f"{len(channels)} transducer(s); values hash {digest[:12]}"]
        calib.write_ecs(out.local, channels, sonar_model=rep["sonarModel"], notes=notes)
        # Read it back the way aa-sv will: a file echopype cannot parse is not kept.
        calib.read_ecs(out.local)
    except Exception as exc:  # noqa: BLE001
        run.discard(out)
        print(f"aa-ecs: could not write the ECS: {exc}", file=sys.stderr)
        sys.exit(1)
    run.finish(out, extra={"reference": ref.uri, "label": label,
                           "channels": [{"frequency": c["frequency"], "channel": c["channel"]}
                                        for c in channels]})


if __name__ == "__main__":
    main()
