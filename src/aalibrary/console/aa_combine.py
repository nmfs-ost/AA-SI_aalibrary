#!/usr/bin/env python3
"""
aa-combine

Console tool for combining many converted EchoData files into one store, using
echopype.combine_echodata, with a QC pass in front of it and a report beside
it.

Pipeline-friendly: reads inputs from positional args, a working directory, or
stdin; writes the output path to stdout; all logs to stderr.

    aa-ed ./HB1603/ | aa-combine -o HB1603_L1.zarr | aa-sv

Why there is a QC pass in front of echopype
-------------------------------------------
`combine_echodata` enforces five preconditions, and when one fails it raises
after doing the work of loading everything. The message names the constraint
but usually not the file:

    all EchoData objects must have the same sonar_model value
    EchoData objects have conflicting filenames
    the channels {...} are not found in all EchoData objects being combined
    the coordinate ping_time is not in ascending order ... combine cannot be used
    ... have a channel dimension with repeating values

aa-combine checks all five first, in the same order, and says *which input*.
It then adds the one check echopype does not make and cannot make, because it
is a question about the survey rather than about the data.

Combining is not concatenation. The result is one unbroken ping axis, and
anything binned from it afterwards — MVBS above all — will average straight
across a transit gap and produce a number that looks entirely plausible and is
not real. By then the gap is indistinguishable from quiet water. So the seam
check happens here or nowhere. The thresholds match the Workbench's own
client-side check (`frontend/src/components/panels/ncei/seams.ts`) so the
panel and the tool cannot disagree: a gap is a gap when the dead time exceeds
15 minutes *and* the cadence gap exceeds 6x the median file interval.

    --check      run the QC pass, write nothing, exit 4 if anything is wrong
    --strict     refuse to write across a seam rather than warning about it

Three things echopype gets wrong on this path, worked around here
-----------------------------------------------------------------
1. A .nc -> .zarr combine fails outright. `open_converted` leaves NetCDF
   encoding on every variable (zlib, chunksizes, fletcher32, szip...);
   `set_zarr_encodings` copies that dict wholesale into the Zarr encoding; and
   xarray's Zarr backend rejects every one of those keys. So `aa-ed *.raw`
   followed by a combine to .zarr — the ordinary path through this pipeline —
   cannot work. See _strip_netcdf_encoding.

2. `consolidated=True` never takes. echopype writes the groups one at a time
   in append mode, so a per-group consolidation is immediately invalidated by
   the next group's write. Consolidating once, at the end, is the only
   spelling that works. Without it, opening the store costs one request per
   array, on every open, forever.

3. Nothing records that a write finished. Zarr has no notion of a complete
   store, so a missing chunk is ambiguous: it may be a chunk that is all fill
   value, or one the writer never reached. aa-combine stamps `aa_write` into
   the root attributes — on success, and from its SIGTERM handler — which is
   what lets `aa-store verify` tell sparsity from an interrupted write, and
   what makes exit 3 mean something a job runner can act on.

Chunk shape and codec
---------------------
echopype's writer takes no argument for either; it targets ~100 MB chunks and
picks a codec per dtype. That is a reasonable default and the wrong one as
soon as you know your query shape. When --chunk-pings or --compression is
given, aa-combine writes the groups itself with xarray — which is what
echopype's `save_file` does underneath — so the request actually takes effect
rather than being silently overridden.

Typical usage:
    aa-combine ./converted/ --check
    aa-combine *.nc -o HB1603_L1.zarr --chunk-pings 500
    aa-combine -o gs://bucket/surveys/HB1603_L1.zarr --workdir ./converted
    aa-ed ./raw/ | aa-combine -o out.zarr --json | aa-store verify --json
"""
from __future__ import annotations

# === Silence logs BEFORE any heavy imports ===
import logging
import sys
import warnings

logging.disable(logging.CRITICAL)
warnings.filterwarnings("ignore")

from loguru import logger
logger.remove()
# Default sink: WARNING+ to stderr so real errors aren't swallowed.
# _configure_logging() below replaces this once --quiet / --debug are parsed.
logger.add(sys.stderr, level="WARNING")

# Now the heavy imports — anything they log gets squashed
import argparse
import contextlib
import inspect
import json
import os
import pprint
import signal
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

# Pipeline tools should die cleanly when the downstream end of the pipe
# closes early (`... | head -n 1`), not throw BrokenPipeError. Guarded
# with hasattr because SIGPIPE doesn't exist on Windows.
if hasattr(signal, "SIGPIPE"):
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)


TOOL = "aa-combine"
VERSION = "0.2.0"

# Seam thresholds. Kept identical to seams.ts in the Workbench frontend, which
# runs the same test on the NCEI listing before the command is even composed.
# Two implementations of one judgement is already one too many; two that
# disagree would mean the panel calls a selection clean and the tool does not.
#
# FLOOR: a ship does not stop logging for ninety seconds and call it a
# transit. On a fast cadence that is a large multiple of the median and
# nothing at all in wall-clock terms, and a warning that fires on acquisition
# hiccups is one people learn to click past.
GAP_FLOOR_SECONDS = 15 * 60
# FACTOR: files land on a near-fixed cadence, so an outlier is usually orders
# of magnitude out rather than a little over. 6x clears the jitter of a file
# that ran long and sits well under a real transit.
GAP_FACTOR = 6

INPUT_SUFFIXES = {".nc", ".netcdf4", ".zarr"}
NETCDF_SUFFIXES = {".nc", ".netcdf4"}

# Encoding keys the Zarr backend understands. Everything else is dropped
# before the write — see _strip_netcdf_encoding.
ZARR_SAFE_ENCODING = {
    "dtype", "_FillValue", "units", "calendar", "scale_factor", "add_offset",
    "compressor", "compressors", "filters", "serializer", "write_empty_chunks",
    "shards",
}

COMPRESSIONS = ("default", "none", "zlib", "blosc-lz4", "blosc-zstd")


def silence_all_logs():
    """Re-apply suppression in case a library re-enabled logging
    or added its own loguru sink during initialization."""
    logging.disable(logging.CRITICAL)
    for name in [None] + list(logging.root.manager.loggerDict):
        lg = logging.getLogger(name)
        lg.handlers.clear()
        lg.propagate = True
    logger.remove()
    logger.add(sys.stderr, level="WARNING")


def _configure_logging(quiet: bool, debug: bool) -> None:
    """Replace the suppression sink with one at the user's chosen level.
    --debug wins over --quiet (mutually-exclusive check happens in main)."""
    logger.remove()
    if debug:
        logger.add(sys.stderr, level="DEBUG", backtrace=True, diagnose=False)
    elif quiet:
        logger.add(sys.stderr, level="WARNING", backtrace=False, diagnose=False)
    else:
        logger.add(sys.stderr, level="INFO", backtrace=True, diagnose=False)


def print_help() -> None:
    help_text = """
    Usage: aa-combine [OPTIONS] [INPUTS...]

    Arguments:
      INPUTS                    Converted EchoData files (.nc / .zarr), or a
                                directory containing them. Optional. With no
                                inputs, aa-combine reads stdin; with neither,
                                it globs --workdir.

    Input:
      --workdir DIR             Where to look when no inputs are given.
                                Default: the current directory.
      --recursive               Search --workdir recursively.
      --sort {time,given,name}  Order the inputs before combining.
                                time  — by first ping_time (default). This is
                                        what echopype requires; unsorted input
                                        is its most common hard failure.
                                given — the order they arrived in.
                                name  — lexical, which for D…-T… names is
                                        chronological.
      --channels LIST           Comma-separated channel names to keep, passed
                                to echopype as channel_selection. Leave unset
                                to keep every channel. Required when the
                                inputs do not all carry the same channels;
                                echopype refuses that combine outright.
      --sonar_model MODEL       Assert the expected model (EK60, EK80, ...).
                                Fails before loading anything if an input
                                disagrees.

    Output:
      -o, --output_path PATH    Output store or file. A .zarr suffix writes a
                                store, .nc writes a single NetCDF export. May
                                be a gs:// or s3:// URI for .zarr, which
                                writes there directly rather than writing
                                locally and copying a directory of thousands
                                of objects afterwards. Default: combined.zarr
                                in --workdir.
      --overwrite               Replace an existing output.
      --chunk-pings N           Chunk length along ping_time. Unset lets
                                echopype target ~100 MB chunks, which is a
                                good default and the wrong one once you know
                                your query shape. Aim for 1-20 MB compressed;
                                5-10 MB is the sweet spot on object storage.
      --compression WHICH       default | none | zlib | blosc-lz4 | blosc-zstd
                                Default lets echopype pick per dtype (zstd for
                                floats, lz4 for ints). zlib applies to NetCDF
                                output only.
      --consolidated            Write consolidated metadata (default on).
                                Costs one small object; saves one request per
                                array on every open, forever.
      --no-consolidated         Skip it.

    QC:
      --check                   Run the QC pass and stop. Writes no store.
                                Exit 4 if anything would have blocked or
                                warned. This is the safe thing to run first.
      --plan                    Estimate the combine — files, pings, channels,
                                bytes — and stop. Emits aa/plan/1 JSON.
      --strict                  Treat seams, overlaps and duplicate ping times
                                as blocking rather than advisory. Use this in
                                a recipe, where nobody reads the warnings.
      --gap_seconds N           Minimum dead time before a gap counts as a
                                seam. Default: 900 (15 minutes).
      --gap_factor N            ...and how many times the median file cadence
                                it must also exceed. Default: 6.
      --report [PATH]           Write the QC report. Bare --report, or the
                                flag omitted entirely, writes it beside the
                                output. --report PATH chooses the path.
                                --no-report skips it. The report URI is named
                                in the handle, which is the only way the UI
                                can surface it.
      --no-report               Skip the QC report.

    Machine interfaces:
      --json                    Emit an aa/1 handle line on stdout instead of
                                the bare path.
      --progress                Emit NDJSON progress events on stderr for a
                                job runner to parse.
      --describe                Emit this tool's own parameter schema as JSON
                                and exit, so the catalogue can be generated
                                rather than hand-maintained.

      -q, --quiet               Warnings and errors only.
      --debug                   Verbose logging.
      -h, --help                This message.

    Exit codes:
      0 ok        1 runtime error    2 usage
      3 partial (interrupted; store marked resumable)
      4 QC failed (--check, or --strict with findings)

    Uploading:
      There is no --upload. `aa-combine ... | aa-upload --as-is` composes, and
      -o gs://... writes to the bucket directly, which is better than either:
      no second pass over a store that may be hundreds of gigabytes.

    Examples:
      aa-combine ./converted/ --check
      aa-combine *.nc -o HB1603_L1.zarr --chunk-pings 500 --compression blosc-zstd
      aa-combine --workdir ./converted -o gs://bucket/HB1603_L1.zarr
      aa-ed ./raw/ | aa-combine -o out.zarr --json | aa-store verify --json
    """
    print(help_text)


# --------------------------------------------------------------------------- #
# Self-description
#
# The Workbench hand-maintains a flag schema for ~25 tools in
# `toolCatalog.ts`, in a different repository on a different release cadence,
# with a `verified` flag that is a manual assertion and goes stale silently.
# A tool that can describe itself turns that file into something generated.
#
# Flags, defaults and choices are read back off the parser rather than
# repeated here, so this cannot drift from the real command line — only the
# labels and roles, which argparse has no place to hold, are written by hand.
# --------------------------------------------------------------------------- #
PARAM_META: dict[str, dict] = {
    "output_path": {"label": "Output store", "primary": True, "type": "string"},
    "workdir": {"label": "Working directory", "type": "path", "role": "input"},
    "channels": {"label": "Channels", "type": "string", "primary": True},
    "sonar_model": {"label": "Sonar model", "type": "string"},
    "sort": {"label": "Input order", "type": "enum"},
    "chunk_pings": {"label": "Pings per chunk", "type": "number", "primary": True},
    "compression": {"label": "Compression", "type": "enum"},
    "consolidated": {"label": "Consolidated metadata", "type": "boolean"},
    "overwrite": {"label": "Overwrite existing output", "type": "boolean"},
    "check": {"label": "QC only (no write)", "type": "boolean", "primary": True},
    "plan": {"label": "Plan only (no write)", "type": "boolean"},
    "strict": {"label": "Block on seams", "type": "boolean"},
    "gap_seconds": {"label": "Seam floor (seconds)", "type": "number"},
    "gap_factor": {"label": "Seam factor", "type": "number"},
    "report": {"label": "Write QC report", "type": "boolean", "default": True},
    "recursive": {"label": "Search recursively", "type": "boolean"},
    "json": {"label": "Machine output", "type": "boolean"},
    "progress": {"label": "NDJSON progress", "type": "boolean"},
}

# Flags that describe how the tool talks, not what it does. The catalogue has
# no use for them and listing them would bury the ones it does.
PARAM_SKIP = {"quiet", "debug", "describe", "help", "inputs", "no_report"}


def describe(parser: argparse.ArgumentParser) -> dict:
    params = []
    for action in parser._actions:  # noqa: SLF001 - the only way to read them back
        if action.dest in PARAM_SKIP or action.dest == argparse.SUPPRESS:
            continue
        meta = PARAM_META.get(action.dest, {})
        flag = action.option_strings[0] if action.option_strings else None
        default = meta.get("default", action.default)
        if isinstance(default, Path):
            default = str(default)
        entry: dict[str, Any] = {
            "id": action.dest,
            "label": meta.get("label", action.dest.replace("_", " ").capitalize()),
            "type": meta.get(
                "type", "boolean" if isinstance(action.const, bool) else "string"
            ),
            "default": default if default is not None else "",
        }
        if flag:
            entry["flag"] = flag
        if action.choices:
            entry["options"] = list(action.choices)
        if action.help:
            entry["help"] = " ".join(str(action.help).split())
        if meta.get("primary"):
            entry["primary"] = True
        if meta.get("role"):
            entry["role"] = meta["role"]
        params.append(entry)

    return {
        "schema": "aa/describe/1",
        "tool": TOOL,
        "version": VERSION,
        # Spelled as in frontend/src/types/layers.ts.
        "consumes": "l1",
        "produces": "l1",
        # The output layer depends on the suffix: a .zarr store is the L1
        # layer, a .nc file is an export that nothing downstream reads back.
        "producesByFormat": {"zarr": "l1", "nc": "netcdf"},
        "params": params,
    }


# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #
def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _to_uri(value: str | os.PathLike) -> str:
    """Normalise to a URI. A handle must never carry a bare path: it resolves
    against whatever directory the reader is standing in, which is a bug that
    only surfaces once the handle crosses a machine."""
    text = str(value).strip()
    if "://" in text:
        return text
    return "file://" + os.path.abspath(os.path.expanduser(text))


def _progress(enabled: bool, event: str, **fields: Any) -> None:
    """One flat NDJSON event on stderr. done/total/unit is all a progress bar
    needs; anything richer gets ignored by the UI and still has to be parsed."""
    if not enabled:
        return
    payload = {"t": _now(), "stage": TOOL, "event": event}
    payload.update(fields)
    sys.stderr.write(json.dumps(payload, separators=(",", ":"), default=str) + "\n")
    sys.stderr.flush()


def _iso(value: Any) -> Optional[str]:
    """numpy datetime64 / pandas Timestamp -> ISO 8601 Z, or None."""
    if value is None:
        return None
    try:
        import pandas as pd

        stamp = pd.Timestamp(value)
        if stamp is pd.NaT:
            return None
        text = stamp.tz_localize("UTC") if stamp.tzinfo is None else stamp.tz_convert("UTC")
        return text.isoformat(timespec="seconds").replace("+00:00", "Z")
    except Exception:  # noqa: BLE001 - a time we cannot parse is not fatal
        return str(value)


def _epoch(value: Any) -> Optional[float]:
    try:
        import pandas as pd

        stamp = pd.Timestamp(value)
        return None if stamp is pd.NaT else float(stamp.value) / 1e9
    except Exception:  # noqa: BLE001
        return None


class Target:
    """The output, local or remote, behind one interface.

    Local and object-store outputs differ in exactly three operations —
    does it exist, can zarr open it, what is its path — and threading an
    `if "://" in output` through the tool would put that branch in a dozen
    places. It lives here instead.
    """

    def __init__(self, value: str):
        self.uri = _to_uri(value)
        self.remote = "://" in str(value) and not str(value).startswith("file://")
        self.raw = str(value) if self.remote else os.path.abspath(os.path.expanduser(str(value)))
        self.name = self.raw.rstrip("/").rsplit("/", 1)[-1]
        self.suffix = ("." + self.name.rsplit(".", 1)[-1].lower()) if "." in self.name else ""

    @property
    def is_netcdf(self) -> bool:
        return self.suffix in NETCDF_SUFFIXES

    @property
    def path(self) -> Path:
        if self.remote:
            raise ValueError(f"{self.uri} is not a local path")
        return Path(self.raw)

    def exists(self) -> bool:
        if not self.remote:
            return Path(self.raw).exists()
        try:
            import fsspec

            fs, key = fsspec.core.url_to_fs(self.raw)
            return bool(fs.exists(key))
        except Exception as exc:  # noqa: BLE001 - unreachable is not "absent"
            logger.debug(f"Could not check {self.raw}: {exc}")
            return False

    def sibling(self, suffix: str) -> str:
        """A path beside the output, e.g. the QC report."""
        stem = self.raw[: -len(self.suffix)] if self.suffix else self.raw
        return stem + suffix

    def __str__(self) -> str:
        return self.raw


def _same_target(path: Path, target) -> bool:
    """True when a discovered input is the output we are about to write."""
    if target.remote:
        return False
    try:
        return path.resolve() == target.path.resolve()
    except OSError:
        return str(path) == target.raw


def _is_aa_product(path: Path) -> bool:
    """True when a .zarr directory carries this toolset's write marker.

    Read as text rather than through zarr: this runs over every discovered
    input before anything is opened, and importing zarr to answer a yes/no
    about a directory would be the most expensive part of the pass.
    """
    if path.suffix.lower() != ".zarr" or not path.is_dir():
        return False
    for name in ("zarr.json", ".zattrs"):
        candidate = path / name
        if candidate.is_file():
            try:
                text = candidate.read_text(encoding="utf-8")
            except OSError:
                continue
            if "aa_write" in text or "aa_kind" in text:
                return True
    return False


def _collect_inputs(raw_inputs: list[str], recursive: bool) -> list[Path]:
    """Expand directories, keep files, preserve order, drop duplicates."""
    found: list[Path] = []
    seen: set[str] = set()
    for item in raw_inputs:
        path = Path(item).expanduser()
        if path.is_dir() and path.suffix.lower() != ".zarr":
            pattern = "**/*" if recursive else "*"
            candidates = sorted(
                child
                for child in path.glob(pattern)
                if child.suffix.lower() in INPUT_SUFFIXES
            )
            if not candidates:
                logger.warning(
                    f"No .nc or .zarr files "
                    f"{'anywhere under' if recursive else 'directly inside'} {path}"
                )
            found.extend(candidates)
        else:
            found.append(path)
    ordered: list[Path] = []
    for path in found:
        key = str(path.resolve()) if path.exists() else str(path)
        if key in seen:
            logger.warning(f"Ignoring repeated input: {path}")
            continue
        seen.add(key)
        ordered.append(path)
    return ordered


def _read_stdin_inputs() -> list[str]:
    """Bare paths or aa/1 handle lines. Both, because every tool already
    installed prints the former and the Workbench wants the latter."""
    values: list[str] = []
    for line in sys.stdin:
        text = line.strip()
        if not text or text.startswith("#"):
            continue
        if text.startswith("{"):
            try:
                document = json.loads(text)
            except json.JSONDecodeError as exc:
                logger.warning(f"Skipping unparseable stdin line: {exc}")
                continue
            uri = document.get("uri")
            if not uri:
                logger.warning("Skipping stdin JSON with no uri")
                continue
            values.append(uri[len("file://"):] if uri.startswith("file://") else uri)
        else:
            values.append(text)
    return values


# --------------------------------------------------------------------------- #
# Inspection — one lazy open per input, no data read
# --------------------------------------------------------------------------- #
def inspect_inputs(paths: list[Path], progress: bool) -> list[dict]:
    """Open each input lazily and record what the QC pass needs to judge it.

    `open_converted` is lazy, and ping_time is a coordinate, so xarray has it
    in memory the moment the group opens. Reading first/last from it costs
    nothing; reading the data would cost everything.
    """
    import echopype as ep

    records: list[dict] = []
    total = len(paths)
    for index, path in enumerate(paths, start=1):
        _progress(progress, "progress", done=index - 1, total=total, unit="files")
        record: dict[str, Any] = {
            "path": str(path),
            "uri": _to_uri(path),
            "name": path.name,
            "error": None,
        }
        try:
            echodata = ep.open_converted(str(path))
        except Exception as exc:  # noqa: BLE001 - one bad file must not stop the pass
            record["error"] = f"{type(exc).__name__}: {exc}"
            logger.error(f"Could not open {path}: {exc}")
            records.append(record)
            continue

        record["sonar_model"] = getattr(echodata, "sonar_model", None)

        beam_group = _first_beam_group(echodata)
        record["group"] = beam_group
        if beam_group is not None:
            dataset = echodata[beam_group]
            times = dataset["ping_time"].values if "ping_time" in dataset.coords else None
            if times is not None and len(times):
                record["pings"] = int(len(times))
                record["start"] = _iso(times.min())
                record["end"] = _iso(times.max())
                record["start_epoch"] = _epoch(times.min())
                record["end_epoch"] = _epoch(times.max())
                # Duplicates inside one file are echopype's problem only once
                # they reach the combined axis, where they are silent.
                record["duplicate_pings"] = int(len(times) - len(set(times.tolist())))
                record["monotonic"] = (
                    bool((times[1:] >= times[:-1]).all()) if len(times) > 1 else True
                )
            channels = dataset["channel"].values.tolist() if "channel" in dataset.coords else []
            record["channels"] = [str(channel) for channel in channels]
            if "range_sample" in dataset.dims:
                record["range_samples"] = int(dataset.sizes["range_sample"])
            # A repeated channel name inside one object is a hard stop for
            # echopype's combine, and the message it gives names the file
            # through the Provenance group rather than the path you passed.
            record["duplicate_channels"] = len(record["channels"]) != len(set(record["channels"]))
        records.append(record)
    _progress(progress, "progress", done=total, total=total, unit="files")
    return records


def _first_beam_group(echodata) -> Optional[str]:
    """The group carrying ping_time and channel. Beam_group1 in practice, but
    ask rather than assume: EK80 complex data lands in a different arrangement
    and a hard-coded path turns that into an obscure KeyError."""
    paths = list(getattr(echodata, "group_paths", []) or [])
    for candidate in paths:
        if candidate.startswith("Sonar/Beam_group"):
            return candidate
    for candidate in paths:
        if "Beam" in candidate:
            return candidate
    return None


# --------------------------------------------------------------------------- #
# The QC pass
# --------------------------------------------------------------------------- #
def qc(
    records: list[dict],
    channel_selection: Optional[list[str]],
    expect_model: Optional[str],
    gap_seconds: int,
    gap_factor: float,
    sort_mode: str = "time",
) -> dict:
    """Everything wrong with this combine, before echopype is asked to do it.

    `problems` block — each one is a precondition echopype itself enforces,
    checked here so the message can name the file. `warnings` are the
    survey-level judgements echopype has no basis to make; advisory unless
    --strict. `notes` are things observed and already handled by an option in
    force: neither blocks nor warns, but silence would be wrong too, because
    the tool changed something about the run.
    """
    problems: list[dict] = []
    warnings_: list[dict] = []
    notes: list[dict] = []

    usable = [record for record in records if not record.get("error")]
    for record in records:
        if record.get("error"):
            problems.append(
                {"code": "unreadable", "file": record["name"], "detail": record["error"]}
            )

    if len(usable) < 2:
        problems.append(
            {
                "code": "too-few-inputs",
                "detail": f"{len(usable)} readable input(s); a combine needs at least 2",
            }
        )

    # 1. sonar_model — echopype: "all EchoData objects must have the same
    #    sonar_model value", raised without saying which one differs.
    models = {record.get("sonar_model") for record in usable}
    if None in models:
        for record in usable:
            if record.get("sonar_model") is None:
                problems.append({"code": "no-sonar-model", "file": record["name"]})
    if len({model for model in models if model}) > 1:
        problems.append(
            {
                "code": "mixed-sonar-model",
                "detail": ", ".join(
                    f"{record['name']}={record.get('sonar_model')}" for record in usable
                ),
            }
        )
    if expect_model:
        for record in usable:
            if record.get("sonar_model") and record["sonar_model"] != expect_model:
                problems.append(
                    {
                        "code": "unexpected-sonar-model",
                        "file": record["name"],
                        "detail": f"expected {expect_model}, found {record['sonar_model']}",
                    }
                )

    # 2. filenames — echopype: "EchoData objects have conflicting filenames".
    #    It compares basenames, so two identically-named files in different
    #    directories collide even though the paths are distinct.
    names: dict[str, str] = {}
    for record in usable:
        if record["name"] in names:
            problems.append(
                {
                    "code": "duplicate-filename",
                    "file": record["name"],
                    "detail": (
                        f"{record['path']} and {names[record['name']]} share a basename; "
                        "echopype identifies inputs by basename, not by path"
                    ),
                }
            )
        names[record["name"]] = record["path"]

    # 3. channels — echopype refuses a combine whose inputs carry different
    #    channel sets unless channel_selection names a subset present in all.
    channel_sets = [frozenset(record.get("channels") or []) for record in usable]
    if channel_sets and len(set(channel_sets)) > 1:
        shared = set.intersection(*[set(item) for item in channel_sets])
        every = set.union(*[set(item) for item in channel_sets])
        if channel_selection is None:
            problems.append(
                {
                    "code": "channel-mismatch",
                    "detail": (
                        f"inputs carry different channels ({sorted(every - shared)} "
                        f"not in all). Pass --channels with a subset of "
                        f"{sorted(shared)} to combine anyway"
                    ),
                }
            )
        else:
            missing = set(channel_selection) - shared
            if missing:
                problems.append(
                    {
                        "code": "channel-selection-unavailable",
                        "detail": (
                            f"--channels asks for {sorted(missing)}, absent from at "
                            f"least one input. Available in all: {sorted(shared)}"
                        ),
                    }
                )
    elif channel_selection and channel_sets:
        missing = set(channel_selection) - set(channel_sets[0])
        if missing:
            problems.append(
                {
                    "code": "channel-selection-unavailable",
                    "detail": f"--channels asks for {sorted(missing)}, not in the inputs",
                }
            )

    for record in usable:
        if record.get("duplicate_channels"):
            problems.append({"code": "repeated-channel", "file": record["name"]})

    # 4. ordering — echopype: "the coordinate ping_time is not in ascending
    #    order for group ... combine cannot be used". It checks the order of
    #    the list it was handed, which is why --sort time is the default.
    #
    #    Which bucket this lands in depends on whether anything will fix it.
    #    Under --sort time it is already corrected by the time the combine
    #    runs, so blocking would mean refusing to do something the tool has
    #    just done. Under --sort given nothing corrects it and echopype will
    #    refuse, so it blocks here where the message can name the file.
    timed = [record for record in usable if record.get("start_epoch") is not None]
    disordered = [
        (previous, current)
        for previous, current in zip(timed, timed[1:])
        if current["start_epoch"] < previous["start_epoch"]
    ]
    if disordered and sort_mode == "given":
        for previous, current in disordered:
            problems.append(
                {
                    "code": "out-of-order",
                    "file": current["name"],
                    "detail": (
                        f"starts before {previous['name']}, and --sort given keeps "
                        "that order; echopype requires ascending ping_time"
                    ),
                }
            )
    elif disordered:
        notes.append(
            {
                "code": "reordered",
                "count": len(disordered),
                "detail": (
                    f"{len(disordered)} input(s) arrived out of time order and were "
                    f"reordered by --sort {sort_mode}"
                ),
            }
        )

    # Everything below judges the combined ping axis, so it has to read the
    # order that will actually be written — not the order the arguments
    # happened to arrive in. Testing the given order would report overlaps
    # between every adjacent pair of a reversed glob, none of which survive
    # the sort.
    sequence = (
        timed if sort_mode == "given" else sorted(timed, key=lambda item: item["start_epoch"])
    )

    # 5. overlaps — echopype only tests the *first* time of each file, so an
    #    input that starts after its predecessor but ends inside it passes and
    #    produces a combined axis with pings out of order in the middle.
    for previous, current in zip(sequence, sequence[1:]):
        if previous.get("end_epoch") and current["start_epoch"] < previous["end_epoch"]:
            seconds = previous["end_epoch"] - current["start_epoch"]
            warnings_.append(
                {
                    "code": "overlap",
                    "file": current["name"],
                    "seconds": round(seconds, 3),
                    "detail": (
                        f"overlaps {previous['name']} by {seconds:.0f}s; the combined "
                        "ping axis will contain duplicated time"
                    ),
                }
            )

    for record in usable:
        if record.get("duplicate_pings"):
            warnings_.append(
                {
                    "code": "duplicate-ping-times",
                    "file": record["name"],
                    "count": record["duplicate_pings"],
                }
            )
        if record.get("monotonic") is False:
            warnings_.append({"code": "non-monotonic-pings", "file": record["name"]})

    # Differing range_sample lengths are legal — echopype pads — but they mean
    # the combined array is as deep as its deepest input, with everything
    # shallower filled. Worth saying, because the store is then larger than
    # the inputs suggest and the extra is nothing.
    depths = {record.get("range_samples") for record in usable if record.get("range_samples")}
    if len(depths) > 1:
        warnings_.append(
            {
                "code": "ragged-range",
                "detail": (
                    f"inputs have different range_sample lengths {sorted(depths)}; the "
                    f"combined array will be {max(depths)} deep with the shallower "
                    "files padded"
                ),
            }
        )

    # 6. seams — the check echopype cannot make, because it is a question
    #    about the survey rather than about the data.
    seams, median = _seams(sequence, gap_seconds, gap_factor)
    for seam in seams:
        warnings_.append(
            {
                "code": "seam",
                "file": seam["after"],
                "seconds": seam["seconds"],
                "detail": (
                    f"{seam['seconds'] / 60:.0f} min gap after {seam['before']} "
                    f"({seam['factor']:.0f}x the median cadence). Combining across "
                    "this makes MVBS average over water the ship was not in"
                ),
            }
        )

    return {
        "schema": "aa/report/1",
        "tool": TOOL,
        "version": VERSION,
        "at": _now(),
        "inputs": [
            {
                key: record.get(key)
                for key in (
                    "name", "uri", "sonar_model", "pings", "start", "end",
                    "channels", "error",
                )
            }
            for record in records
        ],
        "medianIntervalSeconds": median,
        "seams": seams,
        "problems": problems,
        "warnings": warnings_,
        "notes": notes,
        "ok": not problems and not warnings_,
    }


def _seams(
    timed: list[dict], gap_seconds: int, gap_factor: float
) -> tuple[list[dict], Optional[float]]:
    """Gaps that exceed both the absolute floor and the relative factor.

    Both tests, not either: the floor alone fires on every coarse cadence, and
    the factor alone fires on every acquisition hiccup on a fine one.

    The two measure different things, and conflating them is a real trap. The
    *factor* compares start-to-start against the median start-to-start, which
    is the file cadence — the same quantity seams.ts computes from an NCEI
    listing, so the tool and the panel agree. The *floor* uses dead time,
    end-of-one to start-of-next, which is the only quantity that answers "was
    the ship logging?". Half-hour files on a half-hour cadence have a
    30-minute start-to-start interval and no gap at all; testing the floor
    against that would call a continuous run a transit every single time.
    """
    if len(timed) < 2:
        return [], None
    starts = [record["start_epoch"] for record in timed]
    cadences = [later - earlier for earlier, later in zip(starts, starts[1:])]
    if not cadences:
        return [], None
    median = statistics.median(cadences)

    seams: list[dict] = []
    for index, (previous, current) in enumerate(zip(timed, timed[1:])):
        cadence_gap = current["start_epoch"] - previous["start_epoch"]
        dead_time = current["start_epoch"] - (previous.get("end_epoch") or previous["start_epoch"])
        if dead_time <= 0:
            continue
        factor = cadence_gap / median if median else float("inf")
        if dead_time >= gap_seconds and factor >= gap_factor:
            seams.append(
                {
                    "index": index,
                    "before": previous["name"],
                    "after": current["name"],
                    # `seconds` is dead time — the number a human wants, and
                    # the one the warning text reads out.
                    "seconds": round(dead_time, 1),
                    "factor": round(factor, 1),
                }
            )
    return seams, round(median, 3) if median else None


# --------------------------------------------------------------------------- #
# Writing
# --------------------------------------------------------------------------- #
def _supported(function, wanted: dict) -> dict:
    """Keep only the kwargs *function* actually accepts, and say what was cut.

    echopype's combine signature has moved: 0.8 took `zarr_path`, `overwrite`,
    `storage_options` and `client` and wrote the store itself; 0.11 takes
    `echodata_list` and `channel_selection` and returns an in-memory EchoData
    for the caller to write. A tool pinned to either breaks on the other, and
    these tools install into a venv whose echopype version they do not
    control. Asking the function what it accepts costs one `inspect` call.
    """
    try:
        parameters = inspect.signature(function).parameters
    except (TypeError, ValueError):  # pragma: no cover - builtins
        return dict(wanted)
    if any(p.kind == inspect.Parameter.VAR_KEYWORD for p in parameters.values()):
        return dict(wanted)
    keep = {name: value for name, value in wanted.items() if name in parameters}
    dropped = sorted(set(wanted) - set(keep))
    if dropped:
        logger.debug(f"{function.__name__} does not accept {dropped}; not passing them")
    return keep


def _strip_netcdf_encoding(echodata) -> int:
    """Drop NetCDF-only encoding so the combined object can be written to Zarr.

    `open_converted` on a .nc file leaves each variable carrying the encoding
    it was read with — zlib, complevel, shuffle, fletcher32, contiguous,
    chunksizes, szip, bzip2. echopype's `set_zarr_encodings` then copies that
    dict wholesale (`encoding[name] = {**val.encoding}`) and hands it to
    xarray's Zarr backend, which rejects every one of those keys:

        ValueError: unexpected encoding parameters for zarr backend:
        ['zlib', 'szip', 'zstd', 'bzip2', 'blosc', 'shuffle', 'complevel', ...]

    So `aa-ed *.raw` (which writes NetCDF) followed by a combine to .zarr
    fails outright on stock echopype 0.11 — the ordinary path through this
    pipeline, not an edge case. Stripping to an allow-list here costs nothing
    and makes it work.

    This is the mirror image of the `clean_attrs` helper the other aa-* tools
    carry: that one removes None-valued attrs NetCDF cannot serialise, this
    one removes encoding Zarr cannot accept.

    An allow-list rather than a block-list because missing a key is a hard
    error at write time, while dropping one too many means Zarr picks its own
    value for it.
    """
    touched = 0
    for group in list(getattr(echodata, "group_paths", []) or []):
        try:
            dataset = echodata[group]
        except Exception:  # noqa: BLE001 - absent groups are normal
            continue
        if dataset is None:
            continue
        changed = False
        for name in list(dataset.variables):
            encoding = dataset[name].encoding
            for key in [item for item in encoding if item not in ZARR_SAFE_ENCODING]:
                encoding.pop(key, None)
                changed = True
        if changed:
            # Assign back: EchoData.__getitem__ hands out a view of the
            # DataTree node, and only __setitem__ puts the edit back in the
            # tree the writer will walk.
            echodata[group] = dataset
            touched += 1
    if touched:
        logger.debug(f"Stripped NetCDF-only encoding from {touched} group(s)")
    return touched


def _clean_netcdf_attrs(echodata) -> int:
    """Coerce attributes NetCDF cannot serialise, before a .nc export.

    The direct descendant of the `clean_attrs` helper the other aa-* tools
    carry, which replaces None with "NA" because `to_netcdf` raises on it.
    Two more cases show up on this path:

      * `combine_echodata` sets `is_combined: True` on the Provenance group —
        a Python bool, and NetCDF has no boolean attribute type:

            TypeError: illegal data type for attribute b'is_combined', must be
            one of ['S1','i1','u1','i2','u2','i4','u4','i8','u8','f4','f8'],
            got b1

        which means *every* combine-to-NetCDF export fails on stock echopype,
        not just unusual ones. Booleans become 1/0, which is what CF does with
        them anyway, having no boolean type either.

      * Anything structured — a dict or a ragged list — is rendered as a
        string rather than dropped, because an attribute that survives as text
        is worth more than one that silently disappears.
    """
    import numpy as np

    fixed = 0

    def _coerce(value):
        nonlocal fixed
        if value is None:
            fixed += 1
            return "NA"
        if isinstance(value, (bool, np.bool_)):
            fixed += 1
            return int(value)
        if isinstance(value, (dict, set)):
            fixed += 1
            return json.dumps(value, default=str)
        if isinstance(value, (list, tuple)) and any(
            isinstance(item, (bool, dict, type(None))) for item in value
        ):
            fixed += 1
            return json.dumps(list(value), default=str)
        return value

    for group in list(getattr(echodata, "group_paths", []) or []):
        try:
            dataset = echodata[group]
        except Exception:  # noqa: BLE001
            continue
        if dataset is None:
            continue
        before = fixed
        dataset.attrs = {key: _coerce(value) for key, value in dataset.attrs.items()}
        for name in list(dataset.variables):
            dataset[name].attrs = {
                key: _coerce(value) for key, value in dataset[name].attrs.items()
            }
        if fixed != before:
            echodata[group] = dataset
    if fixed:
        logger.debug(f"Coerced {fixed} attribute(s) for NetCDF serialisation")
    return fixed


def _blosc(cname: str, clevel: int = 5):
    """A Blosc codec in whatever spelling the installed zarr uses."""
    try:
        import zarr

        return zarr.codecs.BloscCodec(cname=cname, clevel=clevel)
    except Exception:  # noqa: BLE001 - zarr 2 / numcodecs fallback
        import numcodecs

        return numcodecs.Blosc(cname=cname, clevel=clevel)


def _build_encoding(dataset, chunk_pings: Optional[int], compression: str) -> dict:
    """Per-variable encoding for a direct write: chunk shape and codec.

    Mirrors what echopype's `set_zarr_encodings` builds, which is a codec per
    dtype plus a chunk shape, and differs only in taking the two things the
    caller asked for instead of computing both.
    """
    encoding: dict[str, dict] = {}
    for name in dataset.variables:
        variable = dataset[name]
        spec: dict[str, Any] = {}

        if compression in {"default", "zlib"}:
            # Reuse echopype's own per-dtype codec table, so that asking only
            # for a chunk shape changes only the chunk shape. Without this the
            # direct path falls through to zarr's default of zstd level 0 —
            # weaker than the zstd:3 / lz4:5 echopype would have chosen, and a
            # downgrade nobody asked for and nobody would notice.
            try:
                from echopype.utils.coding import COMPRESSION_SETTINGS, get_zarr_compression

                spec.update(get_zarr_compression(variable.variable, COMPRESSION_SETTINGS["zarr"]))
            except Exception as exc:  # noqa: BLE001 - internals may move
                logger.debug(f"Falling back to the backend default codec: {exc}")
        elif compression == "none":
            spec["compressors"] = None
        elif compression == "blosc-lz4":
            spec["compressors"] = [_blosc("lz4", 5)]
        elif compression == "blosc-zstd":
            spec["compressors"] = [_blosc("zstd", 3)]
        # "default" and "zlib" leave the codec to the backend: zlib is a
        # NetCDF codec and has no Zarr equivalent worth pretending about.

        if chunk_pings and "ping_time" in variable.dims:
            # Chunk only along ping_time. The other axes are already short
            # enough to keep whole — channel is single digits and range_sample
            # is a few thousand — and splitting them multiplies the object
            # count without making any query cheaper.
            shape = [
                min(chunk_pings, variable.sizes[dim]) if dim == "ping_time"
                else variable.sizes[dim]
                for dim in variable.dims
            ]
            spec["chunks"] = tuple(shape)

        if spec:
            encoding[str(name)] = spec
    return encoding


def _write_direct(
    combined,
    target: Target,
    chunk_pings: Optional[int],
    compression: str,
    overwrite: bool,
    storage_options: dict,
    progress: bool,
) -> None:
    """Write each group with xarray, so chunk shape and codec take effect.

    echopype's writer exposes neither. `set_zarr_encodings` computes its own
    chunk shape against a ~100 MB target and discards an existing one that
    differs by more than its tolerance, and it overwrites the compressor with
    its own per-dtype choice. So `--chunk-pings 500` handed to that path is
    silently ignored, which is worse than not offering the flag.

    What it does instead is exactly what `echopype.utils.io.save_file` does —
    build an encoding dict, align the dask chunks to it, write the group — so
    this is not a reimplementation of the writer, it is the same write with
    two values supplied rather than derived.
    """
    groups = [group for group in combined.group_paths if group != "Top-level"]
    total = len(groups) + 1

    root = combined["Top-level"]
    root.to_zarr(
        store=target.raw,
        mode="w" if overwrite else "w-",
        consolidated=False,
        storage_options=storage_options or None,
    )
    _progress(progress, "progress", done=1, total=total, unit="groups")

    for index, group in enumerate(groups, start=2):
        dataset = combined[group]
        if dataset is None:
            continue
        encoding = _build_encoding(dataset, chunk_pings, compression)
        for name, spec in encoding.items():
            # Same reason echopype does it: a dask chunking that disagrees
            # with the encoding chunks makes xarray raise rather than choose.
            if "chunks" in spec and hasattr(dataset[name].data, "chunks"):
                dataset[name] = dataset[name].chunk(
                    dict(zip(dataset[name].dims, spec["chunks"]))
                )
        dataset.to_zarr(
            store=target.raw,
            group=group,
            mode="a",
            encoding=encoding,
            consolidated=False,
            storage_options=storage_options or None,
        )
        _progress(progress, "progress", done=index, total=total, unit="groups")
    logger.info(f"Wrote {len(groups) + 1} group(s) with the requested chunk shape and codec")


def _consolidate(target: Target, storage_options: dict) -> None:
    """Write consolidated metadata. Non-fatal: a store without it is slow, not
    wrong, and failing the whole combine over an optimisation would be worse
    than the problem."""
    try:
        import zarr

        zarr.consolidate_metadata(
            target.raw if not target.remote else _fsspec_store(target, storage_options)
        )
        logger.info("Wrote consolidated metadata")
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"Could not consolidate metadata (store is still valid): {exc}")


def _fsspec_store(target: Target, storage_options: dict):
    import fsspec

    return fsspec.get_mapper(target.raw, **(storage_options or {}))


@contextlib.contextmanager
def _open_root(target: Target, storage_options: dict):
    """The store's root group, opened for attribute writes, or None.

    Yields None rather than raising: everything this is used for is metadata
    the store is better with and valid without, and none of it is worth
    failing a completed combine over.
    """
    group = None
    try:
        import zarr

        where = target.raw if not target.remote else _fsspec_store(target, storage_options)
        group = zarr.open_group(where, mode="a")
    except Exception as exc:  # noqa: BLE001
        logger.debug(f"Could not open {target.raw} for annotation: {exc}")
    yield group


def _stamp(
    target: Target, complete: bool, storage_options: dict, extra: Optional[dict] = None
) -> None:
    """Record write state in the store's root attributes.

    Zarr has no notion of a finished store, so a missing chunk is ambiguous
    forever: it may be a chunk that is all fill value, or one the writer never
    reached. This marker is what lets `aa-store verify` answer, and what makes
    exit 3 mean something a job runner can act on.

    Best effort by design — it runs from a signal handler, where raising would
    replace a useful partial store with a traceback.
    """
    if target.is_netcdf:
        # A NetCDF export is one file. Either it is there or it is not, and
        # there is no partial state for a marker to describe.
        return
    try:
        with _open_root(target, storage_options) as group:
            if group is None:
                return
            marker = {"complete": complete, "tool": TOOL, "version": VERSION, "at": _now()}
            if extra:
                marker.update(extra)
            group.attrs["aa_write"] = marker
    except Exception as exc:  # noqa: BLE001 - never let bookkeeping mask the outcome
        logger.debug(f"Could not stamp {target.raw}: {exc}")


def _annotate(
    target: Target,
    report_uri: Optional[str],
    parents: list[str],
    report: dict,
    storage_options: dict,
) -> None:
    """Everything the store should be able to say about itself once it exists.

    Provenance in the store as well as in the handle: a handle is a message
    and gets lost, a store attribute travels with the bytes. `aa-store info`
    reads these back, which is how the Metadata panel shows lineage for a
    store nobody has a handle for any more.
    """
    if target.is_netcdf:
        return
    with _open_root(target, storage_options) as group:
        if group is None:
            logger.warning("Store written but could not annotate it")
            return
        group.attrs["aa_kind"] = "l1"
        group.attrs["provenance"] = {
            "tool": TOOL,
            "version": VERSION,
            # Plural from the first line ever written. aa-combine is the first
            # N:1 stage and also the QC checkpoint, so the one place lineage
            # matters most is the one a singular `parent` cannot describe.
            "parents": parents,
            "at": _now(),
        }
        if report_uri:
            group.attrs["report"] = report_uri
        starts = [item["start"] for item in report["inputs"] if item.get("start")]
        ends = [item["end"] for item in report["inputs"] if item.get("end")]
        if starts and ends:
            group.attrs["time_coverage_start"] = min(starts)
            group.attrs["time_coverage_end"] = max(ends)


def combine(
    paths: list[Path],
    target: Target,
    channel_selection: Optional[list[str]],
    overwrite: bool,
    compression: str,
    chunk_pings: Optional[int],
    consolidated: bool,
    storage_options: dict,
    progress: bool,
) -> None:
    """Open, combine and write. Everything blocking has already been checked."""
    import echopype as ep

    _progress(progress, "progress", done=0, total=len(paths) + 1, unit="files")
    echodatas = []
    for index, path in enumerate(paths, start=1):
        logger.info(f"Opening {path.name}")
        # Lazy, and it must stay lazy: combine_echodata refuses objects with
        # no source file, and an eagerly-loaded survey does not fit in memory.
        echodatas.append(ep.open_converted(str(path)))
        _progress(progress, "progress", done=index, total=len(paths) + 1, unit="files")

    combine_kwargs = _supported(
        ep.combine_echodata,
        {
            "channel_selection": channel_selection,
            # Only present on older echopype, which wrote the store itself.
            "zarr_path": target.raw,
            "overwrite": overwrite,
            "storage_options": storage_options,
        },
    )
    wrote_itself = "zarr_path" in combine_kwargs and not target.is_netcdf
    if channel_selection is None:
        combine_kwargs.pop("channel_selection", None)
    if not wrote_itself:
        for key in ("zarr_path", "overwrite", "storage_options"):
            combine_kwargs.pop(key, None)

    logger.info(
        f"Combining {len(echodatas)} EchoData objects"
        + (f" (channels: {channel_selection})" if channel_selection else "")
    )
    combined = ep.combine_echodata(echodatas, **combine_kwargs)

    if wrote_itself:
        logger.success(f"Combine wrote {target.raw} directly (echopype legacy path)")
        return

    _strip_netcdf_encoding(combined)

    if target.is_netcdf:
        # An export, not a layer. NetCDF is HDF5 underneath and needs a
        # seekable local file, which is why a remote -o is refused upstream.
        _clean_netcdf_attrs(combined)
        write_kwargs = _supported(
            type(combined).to_netcdf,
            {
                "save_path": target.raw,
                "overwrite": overwrite,
                "compress": compression != "none",
            },
        )
        logger.info(f"Writing {target.raw} (NetCDF export)")
        combined.to_netcdf(**write_kwargs)
        return

    controlled = bool(chunk_pings) or compression not in {"default", "zlib"}
    if controlled:
        _write_direct(
            combined, target, chunk_pings, compression, overwrite, storage_options, progress
        )
    else:
        write_kwargs = _supported(
            type(combined).to_zarr,
            {
                "save_path": target.raw,
                "overwrite": overwrite,
                "compress": compression != "none",
                "output_storage_options": storage_options,
            },
        )
        logger.info(f"Writing {target.raw}")
        combined.to_zarr(**write_kwargs)

    if consolidated:
        # Deliberately not passed as a kwarg. echopype writes the groups one
        # at a time in append mode, so a per-group `consolidated=True` is
        # dropped or immediately invalidated by the next group's write; either
        # way the store comes out unconsolidated while the flag says
        # otherwise. Consolidating once, at the end, is the spelling that
        # works — and it is worth doing: without it, opening this store costs
        # one request per array on every open, forever.
        _consolidate(target, storage_options)


def _tree_bytes(path: Path) -> int:
    """Size of a file, or of every file under a store directory."""
    try:
        if path.is_file():
            return path.stat().st_size
        return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())
    except OSError:
        return 0


# --------------------------------------------------------------------------- #
# main
# --------------------------------------------------------------------------- #
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Combine converted EchoData files into one store.",
        add_help=False,
    )
    parser.add_argument("inputs", nargs="*", default=[])
    # Default None, not ".", so that giving it explicitly can mean "do not
    # read stdin" — see the input resolution in main().
    parser.add_argument("--workdir", default=None,
                        help="Where to look when no inputs are given. Implies not reading stdin.")
    parser.add_argument("--recursive", action="store_true",
                        help="Search --workdir recursively.")
    parser.add_argument("-o", "--output_path", "--output", dest="output_path", default=None,
                        help="Output .zarr store or .nc export. May be a gs:// URI.")
    parser.add_argument("--channels", default=None,
                        help="Comma-separated channel names to keep.")
    parser.add_argument("--sonar_model", "--sonar-model", dest="sonar_model", default=None,
                        help="Assert the expected sonar model.")
    parser.add_argument("--sort", choices=["time", "given", "name"], default="time",
                        help="Order the inputs before combining.")
    parser.add_argument("--chunk-pings", "--chunk_pings", dest="chunk_pings",
                        type=int, default=None,
                        help="Chunk length along ping_time.")
    parser.add_argument("--compression", choices=list(COMPRESSIONS), default="default",
                        help="Codec for the written store.")
    parser.add_argument("--consolidated", action="store_true", default=True,
                        help="Write consolidated metadata.")
    parser.add_argument("--no-consolidated", "--no_consolidated", dest="consolidated",
                        action="store_false")
    parser.add_argument("--overwrite", action="store_true",
                        help="Replace an existing output.")
    parser.add_argument("--check", action="store_true",
                        help="Run the QC pass and stop.")
    parser.add_argument("--plan", action="store_true",
                        help="Estimate the combine and stop.")
    parser.add_argument("--strict", action="store_true",
                        help="Treat seams and overlaps as blocking.")
    parser.add_argument("--gap_seconds", "--gap-seconds", dest="gap_seconds",
                        type=int, default=GAP_FLOOR_SECONDS,
                        help="Minimum dead time before a gap counts as a seam.")
    parser.add_argument("--gap_factor", "--gap-factor", dest="gap_factor",
                        type=float, default=GAP_FACTOR,
                        help="Multiple of the median cadence a seam must exceed.")
    # nargs="?" is what makes this compatible with the original's boolean
    # --report. Without it, `aa-combine --report -o out.zarr` consumes `-o` as
    # the report path, writes a file called "-o", and leaves the combine with
    # no output — a failure that produces no error message at all.
    parser.add_argument("--report", nargs="?", const="", default=None,
                        help="Write the QC report; bare flag writes it beside the output.")
    parser.add_argument("--no-report", "--no_report", dest="no_report", action="store_true",
                        help="Skip the QC report.")
    parser.add_argument("--storage-options", "--storage_options", dest="storage_options",
                        default=None, help="JSON passed to the remote filesystem.")
    parser.add_argument("--json", action="store_true",
                        help="Emit an aa/1 handle on stdout instead of the path.")
    parser.add_argument("--progress", action="store_true",
                        help="Emit NDJSON progress events on stderr.")
    parser.add_argument("--describe", action="store_true")
    parser.add_argument("-q", "--quiet", action="store_true")
    parser.add_argument("--debug", action="store_true")
    return parser


def main() -> None:
    if "--help" in sys.argv or "-h" in sys.argv:
        print_help()
        sys.exit(0)

    parser = build_parser()

    if "--describe" in sys.argv:
        print(json.dumps(describe(parser), separators=(",", ":"), default=str))
        sys.exit(0)

    if len(sys.argv) == 1 and sys.stdin.isatty():
        print_help()
        sys.exit(0)

    # See aa-store: argparse cannot match a variadic positional across an
    # optional, so `-o out.zarr a.nc b.nc` loses the inputs. Collect leftovers.
    args, leftover = parser.parse_known_args()
    stray = [item for item in leftover if item.startswith("-")]
    if stray:
        logger.error(f"Unknown option(s): {' '.join(stray)}")
        sys.exit(2)
    args.inputs = list(args.inputs) + [item for item in leftover if not item.startswith("-")]

    if args.debug and args.quiet:
        logger.error("Use --debug OR --quiet, not both.")
        sys.exit(2)
    _configure_logging(args.quiet, args.debug)

    storage_options: dict = {}
    if args.storage_options:
        try:
            storage_options = json.loads(args.storage_options)
        except json.JSONDecodeError as exc:
            logger.error(f"--storage-options is not valid JSON: {exc}")
            sys.exit(2)

    # ---------------------------
    # Resolve inputs
    # ---------------------------
    # Three sources, in a deliberate order. The order matters more than it
    # looks: reading stdin is a *blocking* read, and a tool that reaches for
    # it unasked deadlocks the moment something invokes it with an inherited
    # pipe that nobody ever writes to — which is precisely how a job runner
    # invokes a command composed of flags alone.
    #
    #   positionals     -> use them, never touch stdin
    #   --workdir DIR   -> glob it, never touch stdin
    #   a pipe on stdin -> read it, and fall back to the CWD if it was empty
    #   a terminal      -> glob the CWD
    #
    # `aa-ed ./raw/ | aa-combine` still works, because that is the third case
    # and aa-ed's path arrives whenever it arrives. A runner that wants the
    # directory behaviour passes --workdir, or attaches /dev/null to stdin.
    raw_inputs = list(args.inputs)
    discovered = not raw_inputs
    if not raw_inputs and args.workdir is not None:
        raw_inputs = [args.workdir]
        logger.info(f"Looking in --workdir {Path(args.workdir).resolve()}")
    if not raw_inputs and not sys.stdin.isatty():
        logger.debug("Reading inputs from stdin")
        raw_inputs = _read_stdin_inputs()
        if raw_inputs:
            logger.info(f"Read {len(raw_inputs)} input(s) from stdin.")
    if not raw_inputs:
        raw_inputs = ["."]
        logger.info(f"No inputs given; looking in {Path('.').resolve()}")

    paths = _collect_inputs(raw_inputs, args.recursive)
    missing = [path for path in paths if not path.exists()]
    if missing:
        for path in missing:
            logger.error(f"Input does not exist: {path}")
        sys.exit(1)
    if len(paths) < 2:
        logger.error(
            f"Combining needs at least 2 inputs; found {len(paths)}. "
            "A one-file combine is a copy — use aa-store info to inspect it instead."
        )
        sys.exit(2)

    channel_selection = None
    if args.channels:
        channel_selection = [item.strip() for item in args.channels.split(",") if item.strip()]
        if not channel_selection:
            logger.error("--channels was given but parsed to an empty list.")
            sys.exit(2)

    # ---------------------------
    # Resolve output
    # ---------------------------
    if args.output_path is None:
        default_output = Path(args.workdir or ".").expanduser() / "combined.zarr"
        target = Target(str(default_output))
        logger.info(f"No -o given; writing {target.raw}")
    else:
        target = Target(args.output_path)

    if not target.suffix:
        target = Target(target.raw + ".zarr")
        logger.info(f"No suffix on -o; writing {target.raw}")
    if target.suffix not in INPUT_SUFFIXES:
        logger.error(
            f"-o {target.name}: expected a .zarr store or a .nc export, got {target.suffix!r}."
        )
        sys.exit(2)
    if target.is_netcdf and target.remote:
        logger.error(
            "NetCDF is HDF5 underneath and needs a seekable local file, so -o cannot "
            "be a remote URI for a .nc export. Write locally, then aa-upload."
        )
        sys.exit(2)
    if target.is_netcdf:
        logger.warning(
            "Writing a single NetCDF. That is an export, not a working layer — nothing "
            "downstream reads it back, and it must be read whole. Prefer .zarr unless "
            "this is for handoff or archive."
        )
    if target.exists() and not (args.overwrite or args.check or args.plan):
        logger.error(f"{target.raw} exists. Pass --overwrite to replace it.")
        sys.exit(2)
    # Only for inputs the user named. An input that merely turned up in a
    # directory scan and happens to be the output is excluded below, not
    # refused — refusing there would make the second run in a folder fail on
    # something the tool did to itself.
    if not target.remote and not discovered:
        for path in paths:
            if _same_target(path, target):
                logger.error(f"Refusing to overwrite an input: {target.raw}")
                sys.exit(1)

    if discovered:
        # The default output lands in the directory being globbed, so the
        # second run of `aa-combine` in a folder finds its own store from the
        # first and tries to combine it back in. It fails deep inside
        # echopype, on a missing group, which is a long way from the cause.
        kept = [path for path in paths if not _same_target(path, target)]
        if len(kept) != len(paths):
            logger.info(f"Excluding the output store from the discovered inputs")
            paths = kept
        for path in paths:
            if _is_aa_product(path):
                # Not refused: combining a combined store with newer files is
                # a real operation. But finding one you did not ask for is
                # almost always the first case, so it gets named.
                logger.warning(
                    f"{path.name} was written by this toolset and was picked up by "
                    f"the directory scan, not named explicitly. Pass inputs "
                    f"explicitly if that was not intended."
                )
        if len(paths) < 2:
            logger.error(
                f"Only {len(paths)} input(s) left after excluding the output. "
                "Name the inputs explicitly, or point --workdir somewhere else."
            )
            sys.exit(2)

    args_summary = {
        "inputs": len(paths),
        "output": target.raw,
        "channels": channel_selection,
        "sonar_model": args.sonar_model,
        "sort": args.sort,
        "chunk_pings": args.chunk_pings,
        "compression": args.compression,
        "strict": args.strict,
    }
    logger.debug(
        f"Executing aa-combine configured with [OPTIONS]:\n{pprint.pformat(args_summary)}"
    )

    # ---------------------------
    # Interruption
    # ---------------------------
    # Installed here, before any work, rather than just before the write. A
    # SIGTERM that lands during the inspection pass would otherwise take the
    # default disposition and exit 143, which tells a job runner nothing about
    # whether anything was left behind. Every path out of this tool now ends
    # in a code the runner can act on.
    def _on_signal(signum, _frame):
        if target.exists():
            _stamp(target, False, storage_options, extra={"interruptedBy": int(signum)})
            logger.warning(f"Interrupted (signal {signum}); {target.raw} marked incomplete.")
        else:
            logger.warning(f"Interrupted (signal {signum}) before anything was written.")
        _progress(args.progress, "done", exit=3)
        sys.exit(3)

    for signum in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(signum, _on_signal)
        except (ValueError, OSError):  # pragma: no cover - not the main thread
            pass

    # ---------------------------
    # Inspect and judge
    # ---------------------------
    _progress(args.progress, "start", inputs=len(paths))
    try:
        records = inspect_inputs(paths, args.progress)
    except ImportError as exc:
        logger.error(f"echopype is required to read EchoData files: {exc}")
        sys.exit(1)
    except Exception as exc:  # noqa: BLE001
        logger.exception(f"Error inspecting inputs: {exc}")
        sys.exit(1)

    report = qc(
        records,
        channel_selection,
        args.sonar_model,
        args.gap_seconds,
        args.gap_factor,
        sort_mode=args.sort,
    )

    if args.sort == "time":
        records.sort(
            key=lambda item: (item.get("start_epoch") is None, item.get("start_epoch") or 0)
        )
    elif args.sort == "name":
        records.sort(key=lambda item: item["name"])
    paths = [Path(item["path"]) for item in records if not item.get("error")]
    report["order"] = [item["name"] for item in records if not item.get("error")]
    report["sort"] = args.sort

    for note in report.get("notes", []):
        logger.info(f"[{note['code']}] {note.get('detail', '')}".strip())
    for warning in report["warnings"]:
        logger.warning(
            f"[{warning['code']}] {warning.get('file', '')} "
            f"{warning.get('detail', '')}".strip()
        )
    for problem in report["problems"]:
        logger.error(
            f"[{problem['code']}] {problem.get('file', '')} "
            f"{problem.get('detail', '')}".strip()
        )

    # ---------------------------
    # QC report
    # ---------------------------
    report_uri = None
    if not args.no_report and args.report != "none":
        report_path = Path(args.report) if args.report else Path(target.sibling(".qc.json"))
        try:
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
            report_uri = _to_uri(report_path)
            logger.info(f"QC report: {report_path}")
        except OSError as exc:
            logger.warning(f"Could not write the QC report: {exc}")

    blocking = bool(report["problems"]) or (args.strict and bool(report["warnings"]))

    if args.plan:
        pings = sum(item.get("pings") or 0 for item in records if not item.get("error"))
        channels = channel_selection or (records[0].get("channels") if records else []) or []
        source_bytes = sum(
            _tree_bytes(Path(item["path"])) for item in records if not item.get("error")
        )
        chunks = None
        if args.chunk_pings and pings:
            chunks = {
                "count": -(-pings // args.chunk_pings) * max(1, len(channels)),
                "pings": args.chunk_pings,
            }
        plan = {
            "schema": "aa/plan/1",
            "tool": TOOL,
            "inputs": len(paths),
            "output": target.uri,
            "pings": pings,
            "channels": len(channels),
            "chunks": chunks,
            "estimate": {"readBytes": source_bytes, "writeBytes": source_bytes},
            "warnings": [item["detail"] for item in report["warnings"] if item.get("detail")],
            "problems": [item.get("detail") or item["code"] for item in report["problems"]],
            "report": report_uri,
        }
        print(json.dumps(plan, separators=(",", ":"), default=str))
        sys.exit(4 if blocking else 0)

    if args.check:
        verdict = (
            "problems" if report["problems"]
            else ("warnings" if report["warnings"] else "clean")
        )
        logger.info(
            f"QC {verdict}: {len(report['problems'])} problem(s), "
            f"{len(report['warnings'])} warning(s)"
        )
        if report_uri:
            print(report_uri if args.json else report_uri[len("file://"):])
        sys.exit(4 if (report["problems"] or report["warnings"]) else 0)

    if blocking:
        logger.error(
            "Refusing to combine. Fix the problems above, or re-run without --strict "
            "if the warnings are understood and intended."
        )
        sys.exit(4)

    # ---------------------------
    # Combine
    # ---------------------------
    if target.exists() and args.overwrite:
        logger.info(f"Overwriting {target.raw}")

    try:
        combine(
            paths=paths,
            target=target,
            channel_selection=channel_selection,
            overwrite=args.overwrite,
            compression=args.compression,
            chunk_pings=args.chunk_pings,
            consolidated=args.consolidated,
            storage_options=storage_options,
            progress=args.progress,
        )
    except Exception as exc:  # noqa: BLE001
        if target.exists():
            _stamp(target, False, storage_options, extra={"error": str(exc)})
        logger.exception(f"Error during combine: {exc}")
        _progress(args.progress, "done", exit=1)
        sys.exit(1)

    parents = [item["uri"] for item in records if not item.get("error")]
    _annotate(target, report_uri, parents, report, storage_options)
    _stamp(target, True, storage_options, extra={"inputs": len(parents)})

    logger.success(f"Generated {target.raw} with aa-combine.")
    _progress(args.progress, "done", exit=0)

    # ---------------------------
    # Output
    # ---------------------------
    if args.json:
        handle = {
            "schema": "aa/1",
            # A .zarr store is the L1 layer; a .nc file is an export that
            # nothing downstream reads back. Same tool, different product.
            "kind": "netcdf" if target.is_netcdf else "l1",
            "uri": target.uri,
            "provenance": {
                "tool": TOOL,
                "version": VERSION,
                "parents": parents,
                "at": _now(),
            },
        }
        starts = [item["start"] for item in report["inputs"] if item.get("start")]
        ends = [item["end"] for item in report["inputs"] if item.get("end")]
        if starts and ends:
            handle["time"] = [min(starts), max(ends)]
        if report_uri:
            handle["report"] = report_uri
        print(json.dumps(handle, separators=(",", ":"), default=str))
    else:
        # The path, matching every other aa-* tool, so this drops into an
        # existing pipeline without the whole chain being converted at once.
        print(target.raw)

    sys.exit(0)


if __name__ == "__main__":
    main()
