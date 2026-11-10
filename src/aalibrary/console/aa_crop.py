#!/usr/bin/env python3
"""
aa-crop

Cut a window out of a flat product (Sv, TS, MVBS, a mask): a time span, a
range (or depth) span, a ping or sample span, some channels. The result is the
same kind of product as the input, smaller, with its own hash.

Pipeline-friendly: reads the input path (or gs:// URI) from a positional
argument or stdin, prints the output's path (or gs:// URI) on stdout, logs on
stderr.
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
from pathlib import Path  # noqa: E402

from aalibrary.console._core import (  # noqa: E402
    Help, Run, ToolSpec, add_common_flags, canon, render, show_help, stdio,
)


def _time(value):
    """ISO time, canonical: '2016-07-03 06:00' == '2016-07-03T06:00:00'."""
    if value is None:
        return None
    import pandas as pd

    t = pd.Timestamp(str(value))
    if t.tzinfo is not None:
        t = t.tz_convert("UTC").tz_localize(None)
    return t.isoformat()


def _span(value):
    """'A:B' index span (either side may be empty) -> [A, B] with None."""
    if value is None:
        return None
    text = str(value).strip()
    if ":" not in text:
        raise ValueError(f"expected START:END, got {text!r}")
    a, b = (part.strip() for part in text.split(":", 1))
    return [int(a) if a else None, int(b) if b else None]


def _frequencies(value):
    if not value:
        return None
    from aalibrary.console._calibration import parse_frequency

    out = []
    for item in value:
        for part in str(item).split(","):
            if part.strip():
                hz = parse_frequency(part)
                out.append(int(hz) if hz.is_integer() else hz)
    return sorted(set(out)) or None


SPEC = ToolSpec(
    name="aa-crop",
    role="transform",
    kind="sv",   # the default: an output keeps its input's kind
    op="aa_crop.window",
    op_version=1,
    params={
        "start": _time,
        "end": _time,
        "min_range": canon.number,
        "max_range": canon.number,
        "pings": _span,
        "samples": _span,
        "frequency": _frequencies,
    },
)

HELP = Help(
    summary="Cut a time / range / channel window out of an Sv, TS, MVBS or mask product.",
    does=(
        "Selects, on every (ping_time x range) variable and its coordinates:\n\n"
        "  --start / --end      pings in this time span (inclusive; ISO times, UTC)\n"
        "  --pings A:B          pings by index (Python slice: A included, B not)\n"
        "  --min-range / --max-range   samples whose range (echo_range of the first "
        "channel at the first ping; depth when the range dimension is depth) is in "
        "this span, metres\n"
        "  --samples A:B        samples by index\n"
        "  --frequency 38kHz    channels by nominal frequency (repeatable or comma list)\n\n"
        "The output is the same kind of product as the input (cropped Sv is Sv), "
        "with every other variable kept. Nothing is resampled."
    ),
    stdin="One flat product (.nc or .zarr) path or gs:// URI: Sv, TS, MVBS, a mask.",
    stdout="The cropped product's path (or gs:// URI).",
    options=[
        ("--start TIME / --end TIME", "time span, ISO (UTC)"),
        ("--pings A:B", "ping indices"),
        ("--min-range M / --max-range M", "range (or depth) span in metres"),
        ("--samples A:B", "sample indices"),
        ("--frequency F", "keep only these channels (38kHz, 120000, ...)"),
        ("-o, --output_path PATH", "exact output; local or gs://"),
    ],
    science={
        "start": "Start of the time span (canonical ISO).",
        "end": "End of the time span.",
        "min_range": "Shallowest range kept, metres.",
        "max_range": "Deepest range kept, metres.",
        "pings": "Ping index span.",
        "samples": "Sample index span.",
        "frequency": "Channels kept, by nominal frequency in Hz (order-free).",
    },
    files=(
        "Reads NetCDF or Zarr, local or gs://. Writes <base>_<hash8>.nc beside the "
        "input (current directory for gs:// input), or -o, or --dest. An identical "
        "earlier product is reused."
    ),
    pipeline="Anywhere after aa-sv: ... | aa-sv | aa-crop --start 2016-07-03T06:10 "
             "--end 2016-07-03T06:20 --max-range 200 | aa-graph",
    examples=[
        "aa-crop sv.nc --start 2016-07-03T06:10 --end 2016-07-03T06:20",
        "aa-crop sv.nc --frequency 38kHz --max-range 250",
        "aa-crop mvbs.nc --pings 0:500",
    ],
)


def print_help():
    sys.stdout.write(render(SPEC, HELP, _build_parser()))


def _build_parser():
    p = argparse.ArgumentParser(prog="aa-crop", add_help=False,
                                description="Cut a window out of a flat product.")
    p.add_argument("input_path", type=str, nargs="?")
    p.add_argument("-o", "--output_path", type=str, default=None)
    p.add_argument("--start", type=str, default=None, metavar="TIME")
    p.add_argument("--end", type=str, default=None, metavar="TIME")
    p.add_argument("--pings", type=str, default=None, metavar="A:B")
    p.add_argument("--min-range", dest="min_range", type=float, default=None, metavar="M")
    p.add_argument("--max-range", dest="max_range", type=float, default=None, metavar="M")
    p.add_argument("--samples", type=str, default=None, metavar="A:B")
    p.add_argument("--frequency", action="append", default=None, metavar="F")
    p.add_argument("--quiet", action="store_true")
    add_common_flags(p)
    return p


def crop(ds, *, start=None, end=None, pings=None, min_range=None, max_range=None,
         samples=None, frequency=None, var=None):
    """The window, as a new (lazy) dataset. Raises ValueError when it is empty."""
    import numpy as np

    from aalibrary.console import _flat

    name = _flat.value_var(ds, var)
    da = ds[name]
    tdim, rdim = _flat.time_dim(da), _flat.range_dim(da)
    if tdim is None or rdim is None:
        raise ValueError(f"{name} has no ping_time/range dimensions: {da.dims}")
    out = ds

    if frequency:
        if "channel" not in out.dims:
            raise ValueError("--frequency: this product has no channel dimension")
        freqs = np.asarray(out["frequency_nominal"].values, dtype=float) \
            if "frequency_nominal" in out else None
        if freqs is None:
            raise ValueError("--frequency: the product has no frequency_nominal")
        keep = [i for i, f in enumerate(freqs) if any(abs(f - w) < 0.5 for w in frequency)]
        if not keep:
            have = ", ".join(f"{f / 1000:g} kHz" for f in freqs)
            raise ValueError(f"--frequency: no channel at those frequencies (have {have})")
        out = out.isel(channel=keep)

    if pings:
        out = out.isel({tdim: slice(pings[0], pings[1])})
    if start or end:
        times = out[tdim].values
        lo = np.datetime64(start) if start else times.min()
        hi = np.datetime64(end) if end else times.max()
        sel = np.where((times >= lo) & (times <= hi))[0]
        if sel.size == 0:
            raise ValueError("the time span holds no pings "
                             f"(the product runs {times.min()} to {times.max()})")
        out = out.isel({tdim: slice(int(sel[0]), int(sel[-1]) + 1)})

    if samples:
        out = out.isel({rdim: slice(samples[0], samples[1])})
    if min_range is not None or max_range is not None:
        axis = _flat.range_axis_m(out, name)
        if axis is None:
            raise ValueError("--min-range/--max-range: no echo_range or depth in the product")
        lo = -np.inf if min_range is None else min_range
        hi = np.inf if max_range is None else max_range
        sel = np.where((axis >= lo) & (axis <= hi))[0]
        if sel.size == 0:
            raise ValueError(f"the range span holds no samples (the product covers "
                             f"{np.nanmin(axis):.1f} to {np.nanmax(axis):.1f} m)")
        out = out.isel({rdim: slice(int(sel[0]), int(sel[-1]) + 1)})

    if out.sizes.get(tdim, 0) == 0 or out.sizes.get(rdim, 0) == 0:
        raise ValueError("the window is empty")
    return out


def main() -> None:
    if len(sys.argv) == 1 and not stdio.stdin_is_piped():
        print_help()
        sys.exit(0)
    parser = _build_parser()
    if show_help(SPEC, HELP, parser):
        sys.exit(0)
    args = parser.parse_args()
    try:
        canonical = {k: fn(getattr(args, k)) for k, fn in SPEC.params.items()}
    except (ValueError, TypeError) as exc:
        print(f"aa-crop: {exc}", file=sys.stderr)
        sys.exit(2)
    if not any(v is not None for v in canonical.values()):
        print("aa-crop: give a window (--start/--end, --pings, --min-range/--max-range, "
              "--samples or --frequency)", file=sys.stderr)
        sys.exit(2)

    from aalibrary.console import _flat

    token = stdio.one_input(args.input_path, SPEC.name)
    run = Run(SPEC, args, params=canonical)
    src = run.input(token)
    out = run.plan(ext=".nc", explicit=args.output_path or None, kind=_flat.kind_of(src))
    if run.reusable(out):
        run.finish(out)
        return
    try:
        ds = _flat.open_flat(src.local, chunks={})
        cropped = crop(ds, **{k: canonical[k] for k in canonical})
        cropped.attrs["aa_crop"] = ", ".join(f"{k}={v}" for k, v in canonical.items()
                                             if v is not None)
        _flat.write_netcdf(cropped, out.local)
        ds.close()
    except ValueError as exc:
        run.discard(out)
        print(f"aa-crop: {exc}", file=sys.stderr)
        sys.exit(1)
    except Exception as exc:  # noqa: BLE001
        run.discard(out)
        logger.exception(f"aa-crop: {exc}")
        sys.exit(1)
    run.finish(out)


if __name__ == "__main__":
    main()
