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

Provenance and reuse
--------------------
The output carries the shared aa provenance (aa_provenance, aa_product_hash,
aa_base): every input's chain, merged (N identical aa-nc steps show once as
"aa-nc x N"), then this step. The product hash covers the inputs' identities
in canonical order (sorted, so the order they arrive in never matters) and
--channels; nothing else. When the output already exists and holds that same
product in the same store layout (--chunk-pings, --compression,
--consolidated, recorded with it), it is reused (printed as usual, exit 0)
instead of refused, but only after the QC pass: --strict, --sonar_model and
every blocking problem still exit 4, and the report is still written.
--overwrite always rewrites. --check and --plan never hash or download inputs.

Writing order for a store: combine -> aa provenance -> aa_kind/provenance/
report/time_coverage (_annotate) -> aa_write marker -> consolidate. The
consolidated metadata is written last, so it includes everything.

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
import shutil
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

from aalibrary.console._core import (  # noqa: E402 - after the log silencing above
    Help, Run, ToolSpec, add_common_flags, canon, naming, provenance, render, show_help,
    stdio, uris,
)


TOOL = "aa-combine"
VERSION = "0.2.0"

SPEC = ToolSpec(
    name=TOOL,
    role="echodata",
    kind="echodata",
    op="echopype.combine_echodata",
    op_version=1,
    ext=".zarr",
    # The only option that changes the combined values: which channels are
    # kept, and in what order (channel_selection sets the output order).
    # --sort is not here: a combine only happens in ascending time order.
    params={"channels": canon.ordered()},
)

# Root attributes a combined store must not inherit from its first input.
# combine_echodata copies the first input's Top-level attributes, so without
# this an interrupted store would carry the first input's product hash (and,
# when that input is itself a combined store, its `aa_write: complete`).
INHERITED_ATTRS = (
    provenance.ATTR, provenance.ATTR_HASH, provenance.ATTR_BASE,
    provenance.ATTR_TOOL, provenance.ATTR_KIND, "aa_write",
    # aa-combine's own layer label: an input that is itself a combined store
    # (or an input written by an older core) carries one. A store gets its
    # own from _annotate; a .nc export gets none.
    "aa_kind",
)

WRITE_MARKER = "aa_write"

# The provenance seal (aalibrary.console._core.provenance): a subgroup whose
# product_hash attribute must match aa_provenance for the provenance to be
# the store's own (xarray copies root attributes into a re-saved store, but
# not the seal).
SEAL_GROUP = provenance.SEAL_GROUP
SEAL_ATTR = provenance.SEAL_ATTR

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


HELP = Help(
    summary="Combine converted EchoData files into one L1 Zarr store (or a .nc export).",
    does=(
        "QC-checks the inputs first (sonar model, file names, channels, ping order, "
        "overlaps, and seams: transit gaps that would make MVBS average across water "
        "the ship was not in), then runs echopype.combine_echodata in time order and "
        "writes one store with an unbroken ping axis. A QC report is written beside it."
    ),
    stdin=(
        "Used only when no INPUTS and no --workdir are given: paths, directories or "
        "gs:// URIs, one per line (bare paths or aa/1 JSON handles; '#' lines "
        "ignored). An empty pipe falls back to the current directory."
    ),
    stdout=(
        "The output's absolute path (or URI). With --json one aa/1 handle: schema, "
        "kind (l1 | netcdf), uri, provenance {tool, version, parents, at}, time, "
        "report, and product (hash), base, reused."
    ),
    metadata=(
        "Root attributes keep aa_kind=l1, provenance {tool, version, parents, at}, "
        "report, time_coverage_* and the aa_write marker, and add the aa provenance "
        "(aa_provenance, aa_product_hash, aa_base): each input's chain, identical "
        "steps grouped (aa-nc x3), then this step. The hash covers the inputs, in "
        "canonical order, and --channels. Base: --base, else the -o stem, else "
        "'combined'."
    ),
    options=[
        ("INPUTS | --workdir DIR", "files/directories to combine (default: stdin, else .)"),
        ("-o, --output_path PATH", ".zarr store or .nc export; local, gs:// or s3:// "
                                   "(default: <base>.zarr in --workdir or .)"),
        ("--channels A,B", "channels to keep; needed when inputs differ"),
        ("--check | --plan", "QC only | estimate only (exit 4 on findings)"),
        ("--strict", "block on seams, overlaps and duplicate pings"),
        ("--chunk-pings N, --compression C", "store layout; not scientific"),
        ("--json", "print an aa/1 handle instead of the path"),
        ("--overwrite", "replace an existing output (always rewrites)"),
        ("--force", "rebuild even when the identical output exists"),
        ("--base NAME", "product base name (default: the -o stem, else 'combined')"),
        ("--dest DIR|gs://PREFIX", "write <base>.zarr there instead of --workdir/."),
    ],
    science={"channels": "Channels kept, in this order (sets the output channel order)."},
    files=(
        "Reads .nc/.zarr EchoData, local or gs:// (an object, or a folder of .nc). "
        "Writes <base>.zarr in --workdir or ., or -o / --dest; a .zarr goes straight to "
        "gs:// or s3://, a gs:// .nc is staged and uploaded. An existing output holding "
        "the identical product in the same layout is reused once QC passes; anything "
        "else needs --overwrite (else exit 2). QC report: named after the output "
        "(<output stem>.qc.json), beside it (in the bucket when remote)."
    ),
    # The shared --force/--base/--dest texts ("the input's base name",
    # "beside the input") don't describe a combine; they're listed above.
    common=False,
    pipeline=(
        "N:1 stage after aa-nc / aa-ed: aa-ed ./raw/ | aa-combine -o HB1603_L1.zarr | "
        "aa-sv. Exit codes: 0 ok, 1 error, 2 usage or existing output, 3 interrupted "
        "(store marked incomplete), 4 QC failed."
    ),
    examples=[
        "aa-combine ./converted/ --check",
        "aa-combine *.nc -o HB1603_L1.zarr --chunk-pings 500",
        "aa-ed ./raw/ | aa-combine -o gs://bucket/HB1603_L1.zarr --json | aa-store verify --json",
    ],
)


def print_help() -> None:
    """Curated help (also used by the docs generator)."""
    sys.stdout.write(render(SPEC, HELP, build_parser()))


def print_help_full() -> None:
    help_text = """
    Usage: aa-combine [OPTIONS] [INPUTS...]

    Arguments:
      INPUTS                    Converted EchoData files (.nc / .zarr), or a
                                directory containing them; local paths,
                                file:// or gs:// URIs (a gs:// folder means
                                the .nc objects in it). Optional. With no
                                inputs, aa-combine reads stdin (bare paths or
                                aa/1 handle lines); with neither, it globs
                                --workdir. An empty pipe globs the current
                                directory.

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
                                Scientific: it enters the product hash (in
                                the order given, which is the output order).
      --sonar_model MODEL       Assert the expected model (EK60, EK80, ...).
                                Fails before loading anything if an input
                                disagrees.

    Output:
      -o, --output_path PATH    Output store or file. A .zarr suffix writes a
                                store, .nc writes a single NetCDF export. May
                                be a gs:// or s3:// URI for .zarr, which
                                writes there directly rather than writing
                                locally and copying a directory of thousands
                                of objects afterwards. A gs:// .nc is written
                                to a local staging file and uploaded (other
                                remote schemes are refused for .nc). No
                                suffix means .zarr. Default: <base>.zarr in
                                --workdir, or in the current directory
                                (combined.zarr unless --base is given).
      --base NAME               Base name of the product (default: the -o
                                stem, else "combined"). Recorded in the
                                provenance and kept by every later stage.
      --dest DIR|gs://PREFIX    Write <base>.zarr (or .nc) there instead.
      --overwrite               Replace an existing output. Always rewrites
                                (e.g. to apply a new --chunk-pings).
                                Without it, an existing output that holds
                                the identical product in the same layout
                                (same inputs and --channels; same
                                --chunk-pings, --compression and
                                --consolidated) is reused once the QC pass
                                has passed: its path is printed and the
                                exit code is 0. Anything else exits 2.
      --force                   Recompute even when the identical product
                                already exists (replaces it).
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
                                output, named after it (-o Y.zarr ->
                                Y.qc.json); for a remote output that is in
                                the bucket, beside the store, and if that
                                write fails, ./Y.qc.json in the current
                                directory. Written on every run that passes
                                the QC pass, a reused output included. --report PATH (or URI)
                                chooses the place. --no-report skips it. The
                                report URI is named in the handle, which is
                                the only way the UI can surface it.
      --no-report               Skip the QC report.

    Machine interfaces:
      --json                    Emit an aa/1 handle line on stdout instead of
                                the bare path. Keys: schema, kind, uri,
                                provenance {tool, version, parents, at},
                                time, report, and (added) product = the full
                                product hash, base, reused = true when an
                                identical existing output was reused.
      --progress                Emit NDJSON progress events on stderr for a
                                job runner to parse.
      --describe                Emit this tool's own parameter schema as JSON
                                and exit, so the catalogue can be generated
                                rather than hand-maintained.

      -q, --quiet               Warnings and errors only.
      --debug                   Verbose logging.
      -h, --help                The short help.
      --help-all                This reference.

    Provenance:
      The output carries the aa provenance (aa_provenance, aa_product_hash,
      aa_base; aa-metadata shows it): every input's chain, identical steps
      grouped (aa-nc x3), then this step. Inputs are registered in canonical
      order, so the same files in any order, or as stdin handles, give the
      same product hash and reuse the same output. Store attributes aa_kind,
      provenance, report, time_coverage_* and aa_write are kept as before.

    Exit codes:
      0 ok (including an identical output reused)
      1 runtime error    2 usage, or a different output already exists
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
    # Shared aa-* flags (add_common_flags). Appended after the tool's own, so
    # the existing entries keep their order.
    "force": {"label": "Recompute even if identical exists", "type": "boolean"},
    "base": {"label": "Base name", "type": "string"},
}

# Flags that describe how the tool talks, not what it does. The catalogue has
# no use for them and listing them would bury the ones it does. --dest is a
# second spelling of "where the output goes", which output_path already is
# for the catalogue.
PARAM_SKIP = {"quiet", "debug", "describe", "help", "inputs", "no_report", "dest"}


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

    @property
    def staged(self) -> bool:
        """Written locally, then uploaded by the core: a gs:// NetCDF export.

        A gs:// store is normally written in place through gcsfs (a large store
        should not need twice its size on local disk). With AA_GCS_FAKE_ROOT set
        there is no real bucket to write to, so a store is staged and published
        into the fake one like any other product, and never reaches real GCS.
        """
        if not (self.remote and uris.is_gcs(self.raw)):
            return False
        return self.is_netcdf or bool(os.getenv("AA_GCS_FAKE_ROOT"))

    def exists(self) -> bool:
        if not self.remote:
            return Path(self.raw).exists()
        if self.staged:
            # A single object, published through the core: ask the same
            # object store the upload will go to.
            try:
                return uris.stat(self.raw) is not None
            except Exception as exc:  # noqa: BLE001
                logger.debug(f"Could not check {self.raw}: {exc}")
                return False
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


def _expand_gcs(uri: str, recursive: bool) -> list[str]:
    """A gs:// input: an object or a .zarr store as given, else a folder whose
    .nc objects are the inputs (like a local directory)."""
    name = uri.rstrip("/").rsplit("/", 1)[-1]
    suffix = ("." + name.rsplit(".", 1)[-1].lower()) if "." in name else ""
    if suffix in INPUT_SUFFIXES and not uri.endswith("/"):
        return [uri]
    if suffix == ".zarr":
        return [uri.rstrip("/")]
    bucket, key = uris.parse_gcs(uri)
    prefix = key.rstrip("/") + "/" if key.strip("/") else ""
    found = []
    for obj in uris.backend().list(bucket, prefix):
        rel = obj.key[len(prefix):]
        if not recursive and "/" in rel:
            continue
        if Path(rel).suffix.lower() in NETCDF_SUFFIXES:
            found.append(f"gs://{bucket}/{obj.key}")
    if not found:
        logger.warning(
            f"No .nc objects {'anywhere under' if recursive else 'directly inside'} {uri}"
        )
    return sorted(found)


def _collect_inputs(raw_inputs: list[str], recursive: bool) -> list:
    """Expand directories, keep files, preserve order, drop duplicates.

    Local inputs come back as Paths, gs:// inputs as URI strings (the core
    localizes them when they are registered)."""
    found: list = []
    seen: set[str] = set()
    for item in raw_inputs:
        if uris.is_gcs(item):
            found.extend(_expand_gcs(item, recursive))
            continue
        path = Path(uris.from_file_uri(str(item))).expanduser()
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
    ordered: list = []
    for path in found:
        if isinstance(path, str):
            key = path
        else:
            key = str(path.resolve()) if path.exists() else str(path)
        if key in seen:
            logger.warning(f"Ignoring repeated input: {path}")
            continue
        seen.add(key)
        ordered.append(path)
    return ordered


def _read_stdin_inputs() -> list[str]:
    """Bare paths or aa/1 handle lines. Both, because every tool already
    installed prints the former and the Workbench wants the latter.

    Kept for callers of this module; main() reads stdin through the shared
    core (stdio.read_tokens), which applies the same rules plus file:// URIs
    and the "path"/"url" handle keys."""
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
def inspect_inputs(
    paths: list,
    progress: bool,
    uri_list: Optional[list[str]] = None,
    storage_options: Optional[dict] = None,
) -> list[dict]:
    """Open each input lazily and record what the QC pass needs to judge it.

    `open_converted` is lazy, and ping_time is a coordinate, so xarray has it
    in memory the moment the group opens. Reading first/last from it costs
    nothing; reading the data would cost everything.

    ``paths`` are local paths, or remote URIs (--check/--plan on gs://
    inputs: opened in place through fsspec with ``storage_options``).
    ``uri_list`` gives each input's URI when it is not the local path's (a
    gs:// input read from the cache or a mount).
    """
    import echopype as ep

    records: list[dict] = []
    total = len(paths)
    for index, path in enumerate(paths, start=1):
        _progress(progress, "progress", done=index - 1, total=total, unit="files")
        remote = uris.is_remote(str(path))
        record: dict[str, Any] = {
            "path": str(path),
            "uri": uri_list[index - 1] if uri_list else _to_uri(path),
            "name": uris.basename(str(path)) if remote else Path(path).name,
            "error": None,
        }
        try:
            if remote:
                echodata = ep.open_converted(str(path), storage_options=storage_options or {})
            else:
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
#: Pings per NetCDF chunk when --chunk_pings is not given.
_NETCDF_CHUNK_PINGS = 1000


def _align_netcdf_chunks(echodata, pings: int) -> None:
    """Chunk every ping_time group evenly and write HDF5 chunks to match.

    The combined arrays carry each source file's own chunking, which is
    uneven at every file boundary, while NetCDF picks one chunk shape per
    variable. Mismatched, every output chunk is written in pieces, read back
    and recompressed, which made a 24-file combine five times slower. Even
    dask chunks with the same HDF5 chunk shape make each write one whole
    chunk.
    """
    for group in echodata.group_paths:
        ds = echodata[group]
        if ds is None or "ping_time" not in ds.dims:
            continue
        ds = ds.chunk({"ping_time": pings})
        for variable in ds.variables.values():
            chunks = getattr(variable.data, "chunksize", None)
            if chunks and variable.ndim:
                variable.encoding["chunksizes"] = tuple(int(c) for c in chunks)
                variable.encoding.pop("contiguous", None)
        echodata[group] = ds


#: HDF5 chunk cache per variable per open NetCDF file (netCDF-C's default is 64 MB).
_READ_CACHE_BYTES = 1 << 20


def _set_read_cache(size: int) -> Optional[tuple]:
    """Give NetCDF files opened from now on an HDF5 chunk cache of *size*.

    Returns the previous setting, or None when netCDF4 is not installed.
    """
    try:
        import netCDF4
    except ImportError:  # pragma: no cover - h5netcdf-only installs
        return None
    previous = netCDF4.get_chunk_cache()
    netCDF4.set_chunk_cache(size, previous[1], previous[2])
    return previous


@contextlib.contextmanager
def _small_read_cache(size: int = _READ_CACHE_BYTES):
    """NetCDF files opened inside get a small HDF5 chunk cache (see combine())."""
    previous = _set_read_cache(size)
    try:
        yield
    finally:
        if previous is not None:
            import netCDF4

            netCDF4.set_chunk_cache(*previous)


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


def _drop_inherited_attrs(echodata) -> None:
    """Remove the first input's product identity from the combined root.

    combine_echodata copies the first input's Top-level attributes. For an
    aa-* input those include its aa_provenance / aa_product_hash (and, for a
    combined store, its aa_write marker), which would make an interrupted
    store look like its first input, or like a finished write. This tool
    writes its own values for all of them once the combine has succeeded.
    """
    try:
        top = echodata["Top-level"]
        dropped = [key for key in INHERITED_ATTRS if key in top.attrs]
        if not dropped:
            return
        for key in dropped:
            top.attrs.pop(key, None)
        echodata["Top-level"] = top
        logger.debug(f"Dropped inherited root attribute(s) {dropped}")
    except Exception as exc:  # noqa: BLE001 - cosmetic; finish() overwrites them anyway
        logger.debug(f"Could not drop inherited attributes: {exc}")


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


# --------------------------------------------------------------------------- #
# Provenance, reuse and the report (shared core on top of the above)
# --------------------------------------------------------------------------- #
def _read_root_doc(target: Target, name: str, storage_options: dict) -> Optional[dict]:
    """A store's root metadata document (zarr.json / .zattrs / .zmetadata),
    parsed, or None. Read as JSON, local or through fsspec: no zarr import."""
    try:
        if not target.remote:
            path = Path(target.raw) / name
            if not path.is_file():
                return None
            return json.loads(path.read_text(encoding="utf-8"))
        import fsspec

        fs, root = fsspec.core.url_to_fs(target.raw, **(storage_options or {}))
        key = f"{root.rstrip('/')}/{name}"
        if not fs.exists(key):
            return None
        with fs.open(key, "rb") as handle:
            return json.loads(handle.read().decode("utf-8"))
    except Exception as exc:  # noqa: BLE001 - unreadable is "unknown", never fatal
        logger.debug(f"Could not read {name} in {target.raw}: {exc}")
        return None


def _root_attrs(target: Target, storage_options: dict) -> dict:
    document = _read_root_doc(target, "zarr.json", storage_options)
    if document is not None:
        return document.get("attributes", {}) or {}
    return _read_root_doc(target, ".zattrs", storage_options) or {}


def _has_consolidated(target: Target, storage_options: dict) -> bool:
    if _read_root_doc(target, ".zmetadata", storage_options) is not None:
        return True
    document = _read_root_doc(target, "zarr.json", storage_options) or {}
    return bool(document.get("consolidated_metadata"))


def _as_doc(value: Any) -> Optional[dict]:
    """An aa provenance document from an attribute (JSON text or a dict)."""
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8", "replace")
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError:
            return None
    if isinstance(value, dict) and value.get("schema") == provenance.SCHEMA:
        return value
    return None


def _seal_hash(target: Target, storage_options: dict) -> Optional[str]:
    """The product hash in the store's aa_seal subgroup, or None.

    The seal is what makes a store's aa provenance genuine: xarray copies
    root attributes into anything re-saved from a store, but not the seal.
    """
    for name in (f"{SEAL_GROUP}/zarr.json", f"{SEAL_GROUP}/.zattrs"):
        document = _read_root_doc(target, name, storage_options)
        if document is None:
            continue
        attrs = document.get("attributes", {}) if name.endswith("zarr.json") else document
        value = (attrs or {}).get(SEAL_ATTR)
        return str(value) if value else None
    return None


def _existing_product(target: Target, storage_options: dict, run: Run, out) -> dict:
    """What the existing output holds: its product hash, whether its write
    finished, the store layout it was written with, and the attributes and
    provenance the reuse handle is built from.

    Provenance only counts when it is the output's own: sealed (a copy that
    rode along into a re-saved file is not), and for a gs:// object, not
    rewritten since it was published. A NetCDF export is judged by the core
    (Run.existing_hash); a store, local or remote, by its root attributes and
    seal, read here as JSON through the same filesystem it was written with.
    """
    info: dict[str, Any] = {"hash": None, "complete": None, "attrs": {}, "doc": None,
                            "layout": None}
    try:
        if target.is_netcdf:
            info["complete"] = True  # one file: either there or not
            info["hash"] = run.existing_hash(out)
            if target.staged:
                if info["hash"] and info["hash"] == out.hash:
                    # The recorded layout is inside the file.
                    info["doc"] = provenance.read(uris.localize(target.raw).path)
            else:
                info["doc"] = provenance.read(target.raw)
        else:
            attrs = _root_attrs(target, storage_options)
            doc = _as_doc(attrs.get(provenance.ATTR))
            product_hash = ((doc or {}).get("product") or {}).get("hash")
            if product_hash and _seal_hash(target, storage_options) != product_hash:
                logger.debug(f"{target.raw}: aa provenance without a matching seal; "
                             "not this store's own")
                doc, product_hash = None, None
            info["attrs"] = attrs
            info["doc"] = doc
            info["hash"] = product_hash
            marker = attrs.get(WRITE_MARKER)
            info["complete"] = marker.get("complete") if isinstance(marker, dict) else None
        info["layout"] = (((info["doc"] or {}).get("extra") or {}).get(TOOL) or {}).get("layout")
    except Exception as exc:  # noqa: BLE001
        logger.debug(f"Could not read the existing product at {target.raw}: {exc}")
    return info


def _layout(args, target: Target) -> dict:
    """The store layout a run asks for, in canonical form. Not part of the
    product (the values are the same), but a reused output must have it:
    --chunk-pings 50 on an existing store chunked by 180 is not a no-op."""
    if target.is_netcdf:
        return {"format": "netcdf",
                "compression": "none" if args.compression == "none" else "default"}
    return {
        "format": "zarr",
        "chunk_pings": args.chunk_pings,
        # zlib is a NetCDF codec; for a store it means echopype's default.
        "compression": "default" if args.compression in {"default", "zlib"} else args.compression,
        "consolidated": bool(args.consolidated),
    }


def _layout_text(layout: Optional[dict]) -> str:
    if not layout:
        return "an unrecorded layout"
    return ", ".join(f"{key}={value}" for key, value in layout.items() if key != "format")


def _input_bytes(path_or_uri: str) -> int:
    """Size of an input for --plan: a local file or store, or a gs:// object
    or store (object metadata only; nothing is downloaded)."""
    if uris.is_gcs(str(path_or_uri)):
        try:
            info = uris.stat(str(path_or_uri))
            if info is not None:
                return int(info.size)
            bucket, key = uris.parse_gcs(str(path_or_uri))
            prefix = key.rstrip("/") + "/"
            return sum(obj.size for obj in uris.backend().list(bucket, prefix))
        except Exception as exc:  # noqa: BLE001 - an estimate
            logger.debug(f"Could not size {path_or_uri}: {exc}")
            return 0
    return _tree_bytes(Path(path_or_uri))


def _embed_remote(run: Run, out, target: Target, storage_options: dict, extra: dict) -> None:
    """The core's provenance, written into a remote store's root group
    through the same fsspec mapper the combine wrote with (the core's
    provenance.write only handles local files; a remote Zarr is never staged
    locally). provenance.write_zarr_group sets the flat attributes and
    aa_provenance and creates the aa_seal subgroup; the caller consolidates.
    """
    document = run.document(out, extra=extra)
    with _open_root(target, storage_options) as group:
        if group is None:
            raise RuntimeError(f"could not open {target.raw} to record provenance")
        provenance.write_zarr_group(group, document)


def _write_report(report: dict, destination: str, storage_options: dict) -> Optional[str]:
    """Write the QC report; returns its URI, or None if it could not be written.

    A remote destination (the default for a remote store: beside it) is
    written there: gs:// through the core's object store (like every aa-*
    gs:// output), any other scheme through fsspec with --storage-options. If
    that fails, the report lands in the current directory under its own name,
    rather than in a local directory tree spelled like the URI ("./gs:/...").
    """
    text = json.dumps(report, indent=2, default=str)
    if uris.is_remote(destination):
        try:
            if uris.is_gcs(destination):
                import tempfile

                with tempfile.TemporaryDirectory() as tmp:
                    local = Path(tmp) / uris.basename(destination)
                    local.write_text(text, encoding="utf-8")
                    uris.publish(local, destination, keep_in_cache=False)
            else:
                import fsspec

                with fsspec.open(destination, "w", **(storage_options or {})) as handle:
                    handle.write(text)
            logger.info(f"QC report: {destination}")
            return destination
        except Exception as exc:  # noqa: BLE001
            fallback = Path.cwd() / uris.basename(destination)
            logger.warning(
                f"Could not write the QC report to {destination} ({exc}); "
                f"writing {fallback} instead"
            )
            destination = str(fallback)
    report_path = Path(destination).expanduser()
    try:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(text, encoding="utf-8")
        logger.info(f"QC report: {report_path}")
        return _to_uri(report_path)
    except OSError as exc:
        logger.warning(f"Could not write the QC report: {exc}")
        return None


def _handle(
    target: Target,
    parents: list[str],
    at: Optional[str],
    time_range: Optional[list],
    report_uri: Optional[str],
    product: str,
    base: str,
    reused: bool,
    version: str = VERSION,
    tool: str = TOOL,
) -> dict:
    """The aa/1 handle. The first six keys are the original contract; product,
    base and reused were added with the shared provenance."""
    handle: dict[str, Any] = {
        "schema": "aa/1",
        # A .zarr store is the L1 layer; a .nc file is an export that
        # nothing downstream reads back. Same tool, different product.
        "kind": "netcdf" if target.is_netcdf else "l1",
        "uri": target.uri,
        "provenance": {"tool": tool, "version": version, "parents": parents, "at": at},
    }
    if time_range:
        handle["time"] = list(time_range)
    if report_uri:
        handle["report"] = report_uri
    handle["product"] = product
    handle["base"] = base
    handle["reused"] = reused
    return handle


def _reuse_handle(
    target: Target,
    existing: dict,
    product: str,
    base: str,
    parents: list[str],
    time_range: Optional[list],
    report_uri: Optional[str],
) -> dict:
    """The handle for an output that already holds this product.

    parents, time and report are this run's: the inputs as given now (the
    same content as the ones the store was made from, possibly at other
    paths), and the report just written. The production time and version
    are the store's own. A report is still named when --no-report skipped
    this run's: the one recorded with the store.
    """
    attrs = existing.get("attrs") or {}
    doc = existing.get("doc")
    if doc is None and target.staged:
        try:
            doc = provenance.read(uris.localize(target.raw).path)
        except Exception as exc:  # noqa: BLE001
            logger.debug(f"Could not read the provenance of {target.raw}: {exc}")
    doc = doc or {}
    recorded = ((doc.get("extra") or {}).get(TOOL)) or {}
    lineage = attrs.get("provenance") if isinstance(attrs.get("provenance"), dict) else {}
    return _handle(
        target,
        parents=parents,
        at=lineage.get("at") or (doc.get("created") or {}).get("at"),
        time_range=time_range,
        report_uri=report_uri or attrs.get("report") or recorded.get("report"),
        product=product,
        base=base,
        reused=True,
        version=lineage.get("version") or VERSION,
        tool=lineage.get("tool") or TOOL,
    )


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
    """Open, combine and write. Everything blocking has already been checked.

    Memory must not grow with the number of files. Three things made it:

    * Opened without dask chunks, each file is lazily *indexed* but not lazily
      *computed*, so xr.concat inside combine_echodata loaded every file
      whole: ~8x the on-disk size, which a 5-hour EK60 range (39 files) took
      past 31 GB. Opening with chunks={} keeps every variable a dask array in
      the file's own chunking; combining builds a graph and the write streams
      it.
    * HDF5 keeps a read cache per variable per open file, 64 MB by default,
      and filled it as the write read through the inputs. The small cache is
      in force for the whole run, not just while opening: xarray reopens
      files it has closed with whatever default is current, and HDF5 hands a
      file that is already open (the QC pass's) its existing caches. main()
      therefore sets it before the QC pass opens anything. The output does
      not need a bigger one, because it is written in whole chunks (below).
    * With threads, reads ran ahead of the locked, compressing NetCDF writer
      and waited in memory for their turn.
    """
    with _small_read_cache():
        _combine(paths, target, channel_selection, overwrite, compression,
                 chunk_pings, consolidated, storage_options, progress)


def _combine(
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
    import echopype as ep

    _progress(progress, "progress", done=0, total=len(paths) + 1, unit="files")
    echodatas = []
    for index, path in enumerate(paths, start=1):
        logger.info(f"Opening {path.name}")
        # chunks={}: lazy and dask-backed; see combine() for why it matters.
        echodatas.append(ep.open_converted(str(path), chunks={}))
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
    _drop_inherited_attrs(combined)

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
        _align_netcdf_chunks(combined, chunk_pings or _NETCDF_CHUNK_PINGS)
        # One chunk at a time (see combine()).
        import dask

        with dask.config.set(scheduler="synchronous"):
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
    # --force / --base / --dest, last so --describe keeps its existing order.
    add_common_flags(parser)
    # The shared help texts assume a 1:1 tool (base = the input's; output
    # beside the input). --describe hands these to the Workbench verbatim.
    for action in parser._actions:  # noqa: SLF001 - argparse has no setter
        if action.dest == "base":
            action.help = ("Base name of the product (default: the -o stem, else "
                           "'combined').")
        elif action.dest == "dest":
            action.help = ("Write <base>.zarr (or .nc) here instead of --workdir or the "
                           "current directory.")
        elif action.dest == "force":
            action.help = ("Rebuild even when the output already holds the identical "
                           "product.")
    return parser


def _output_base(args) -> str:
    """--base, else the -o stem, else 'combined' (the old default name)."""
    if getattr(args, "base", None):
        return naming.sanitize_base(args.base)
    if args.output_path:
        return naming.base_of(uris.basename(uris.from_file_uri(str(args.output_path))))
    return "combined"


def main() -> None:
    parser = build_parser()

    # -h/--help: the short, curated help. --help-all: the full reference.
    if show_help(SPEC, HELP, parser, full=print_help_full):
        sys.exit(0)

    if "--describe" in sys.argv:
        print(json.dumps(describe(parser), separators=(",", ":"), default=str))
        sys.exit(0)

    # Small HDF5 read caches for every NetCDF file this run opens, the QC
    # pass included, for the life of the process (see combine()). Set
    # outright, not by entering _small_read_cache() by hand: a context
    # manager nobody keeps is closed as soon as it is collected, which
    # restored the 64 MB default at once. The QC pass then opened every
    # input with big caches, HDF5 shares a file that is opened twice, and the
    # combine read through those caches: memory grew with every file.
    _set_read_cache(_READ_CACHE_BYTES)

    if len(sys.argv) == 1 and not stdio.stdin_is_piped():
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
    #
    # Tokens follow the shared pipe contract (stdio): bare paths, file:// and
    # gs:// URIs, aa/1 JSON handles; blank and '#' lines are skipped.
    workdir = uris.from_file_uri(args.workdir) if args.workdir is not None else None
    raw_inputs = [token for token in map(stdio.normalize_token, args.inputs) if token]
    discovered = not raw_inputs
    if not raw_inputs and workdir is not None:
        raw_inputs = [workdir]
        where = workdir if uris.is_remote(workdir) else Path(workdir).resolve()
        logger.info(f"Looking in --workdir {where}")
    if not raw_inputs and stdio.stdin_is_piped():
        logger.debug("Reading inputs from stdin")
        raw_inputs = stdio.read_tokens()
        if raw_inputs:
            logger.info(f"Read {len(raw_inputs)} input(s) from stdin.")
    if not raw_inputs:
        raw_inputs = ["."]
        logger.info(f"No inputs given; looking in {Path('.').resolve()}")

    items = _collect_inputs(raw_inputs, args.recursive)
    missing = [item for item in items if isinstance(item, Path) and not item.exists()]
    if missing:
        for path in missing:
            logger.error(f"Input does not exist: {path}")
        sys.exit(1)
    if len(items) < 2:
        logger.error(
            f"Combining needs at least 2 inputs; found {len(items)}. "
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
    # Where it goes: -o as given (no suffix -> .zarr), else --dest, else
    # <base>.zarr in --workdir or the current directory. The base name is
    # --base, else the -o stem, else "combined", so the old default stays
    # combined.zarr. The location never depends on the inputs, so it is
    # known before anything is hashed.
    run = Run(SPEC, args)
    base = _output_base(args)
    run.base_override = base
    default_dir = workdir if workdir is not None else "."
    explicit = None
    ext = SPEC.ext
    if args.output_path is not None:
        explicit = uris.from_file_uri(str(args.output_path))
        name = uris.basename(explicit)
        if "." not in name:
            explicit = explicit.rstrip("/") + ".zarr"
            logger.info(f"No suffix on -o; writing {explicit}")
        ext = "." + uris.basename(explicit).rsplit(".", 1)[-1].lower()
    located = run.plan(ext=ext, explicit=explicit, directory=default_dir, stage=False)
    target = Target(located.target)
    if args.output_path is None:
        logger.info(f"No -o given; writing {target.raw}")

    if target.suffix not in INPUT_SUFFIXES:
        logger.error(
            f"-o {target.name}: expected a .zarr store or a .nc export, got {target.suffix!r}."
        )
        sys.exit(2)
    if target.is_netcdf and target.remote and not target.staged:
        logger.error(
            "NetCDF is HDF5 underneath and needs a seekable local file, so a .nc export "
            "can only go to a local path or a gs:// URI (written locally, then uploaded). "
            "Write locally, then aa-upload."
        )
        sys.exit(2)
    if target.is_netcdf:
        logger.warning(
            "Writing a single NetCDF. That is an export, not a working layer — nothing "
            "downstream reads it back, and it must be read whole. Prefer .zarr unless "
            "this is for handoff or archive."
        )

    # Only for inputs the user named. An input that merely turned up in a
    # directory scan and happens to be the output is excluded below, not
    # refused — refusing there would make the second run in a folder fail on
    # something the tool did to itself.
    named_target = (
        not target.remote
        and not discovered
        and any(isinstance(item, Path) and _same_target(item, target) for item in items)
    )
    if named_target:
        # A store can never be the product of combining itself, so there is
        # nothing to reuse here: the old order of checks and exit codes holds.
        if target.exists() and not (args.overwrite or args.check or args.plan):
            logger.error(f"{target.raw} exists. Pass --overwrite to replace it.")
            sys.exit(2)
        logger.error(f"Refusing to overwrite an input: {target.raw}")
        sys.exit(1)

    if discovered:
        # The default output lands in the directory being globbed, so the
        # second run of `aa-combine` in a folder finds its own store from the
        # first and tries to combine it back in. It fails deep inside
        # echopype, on a missing group, which is a long way from the cause.
        kept = [
            item for item in items
            if not (isinstance(item, Path) and _same_target(item, target))
        ]
        if len(kept) != len(items):
            logger.info("Excluding the output store from the discovered inputs")
            items = kept
        for item in items:
            if isinstance(item, Path) and _is_aa_product(item):
                # Not refused: combining a combined store with newer files is
                # a real operation. But finding one you did not ask for is
                # almost always the first case, so it gets named.
                logger.warning(
                    f"{item.name} was written by this toolset and was picked up by "
                    f"the directory scan, not named explicitly. Pass inputs "
                    f"explicitly if that was not intended."
                )
        if len(items) < 2:
            logger.error(
                f"Only {len(items)} input(s) left after excluding the output. "
                "Name the inputs explicitly, or point --workdir somewhere else."
            )
            sys.exit(2)

    # ---------------------------
    # Register inputs, hash the product — only when something may be written
    # ---------------------------
    # --check and --plan write nothing but the report, so they stay the lazy,
    # metadata-only pass they always were: no content hashing and no
    # downloads. A gs:// input is then read in place: through a gcsfuse mount
    # when one covers it, else lazily through fsspec.
    paths: list = []
    input_uris: list[str] = []
    out = None
    existing: Optional[dict] = None
    reuse = False
    replace = bool(args.overwrite)
    if args.check or args.plan:
        for item in items:
            if isinstance(item, Path):
                paths.append(item)
                input_uris.append(_to_uri(item))
            else:
                mounted = uris.mounted_path(item)
                paths.append(mounted if mounted is not None else item)
                input_uris.append(item)
    else:
        # Each input's identity is its own product hash when it carries aa
        # provenance (aa-nc / aa-ed output), else its content. gs:// inputs
        # are read through a gcsfuse mount or the download cache.
        for item in items:
            try:
                registered = run.input(str(item))
            except Exception as exc:  # noqa: BLE001 - a missing or unreadable gs:// object
                logger.error(f"Input does not exist: {item} ({exc})")
                sys.exit(1)
            if isinstance(item, Path):
                paths.append(item)
                input_uris.append(_to_uri(item))
            else:
                paths.append(registered.local)
                input_uris.append(registered.uri)
        # Canonical order: the combined product is sorted by time whatever
        # order the inputs arrive in, so the hash must not depend on it.
        run.inputs.sort(key=lambda registered: (registered.id, registered.uri))
        out = run.plan(ext=ext, explicit=explicit, directory=default_dir, stage=False)

        # An existing output. Decided here, before the (slower) inspection
        # pass, whenever the answer is "refuse": that stays the fast exit 2 it
        # always was. Reuse is only a candidate until the QC gate below has
        # passed: --strict, --sonar_model and every blocking problem apply to
        # a reused output exactly as to a fresh one.
        if target.exists():
            existing = _existing_product(target, storage_options, run, out)
            same = existing["hash"] == out.hash
            finished = existing["complete"] is True
            layout = _layout(args, target)
            same_layout = existing["layout"] == layout
            if same and finished and same_layout and not (args.overwrite or run.force):
                # Same inputs, same channels, same science, same store layout.
                reuse = True
            elif args.overwrite or (run.force and same):
                # --overwrite always rewrites (as it always did: e.g. to apply
                # a new --chunk-pings); --force recomputes an identical one.
                replace = True
            else:
                logger.error(f"{target.raw} exists. Pass --overwrite to replace it.")
                if same and not finished:
                    logger.info("It holds this product, but its write never finished "
                                "(aa_write marker); --overwrite rewrites it.")
                elif same:
                    logger.info(
                        "It holds this product, but not in the layout asked for "
                        f"(written with {_layout_text(existing['layout'])}; asked for "
                        f"{_layout_text(layout)}); --overwrite rewrites it."
                    )
                elif existing["hash"]:
                    logger.info(f"It holds a different product (aa:{str(existing['hash'])[:8]}; "
                                f"this one is aa:{out.short}).")
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
        "product": out.hash if out else None,
        "base": out.base if out else base,
        "reuse": reuse,
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
    state = {"write": target}

    def _on_signal(signum, _frame):
        written = state["write"]
        if written.exists():
            _stamp(written, False, storage_options, extra={"interruptedBy": int(signum)})
            logger.warning(f"Interrupted (signal {signum}); {written.raw} marked incomplete.")
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
        records = inspect_inputs(paths, args.progress, input_uris, storage_options)
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
    # Beside the output, named after it: <output stem>.qc.json. Written on
    # every run that gets this far, a reused output included (the report is
    # this run's QC verdict, and --report PATH asks for it). For a remote
    # output it goes beside
    # the store in the bucket (it used to become "./gs:/bucket/..." in the
    # current directory, with a file:// URI naming a path nobody meant).
    report_uri = None
    if not args.no_report and args.report != "none":
        destination = (
            uris.from_file_uri(args.report) if args.report else target.sibling(".qc.json")
        )
        report_uri = _write_report(report, destination, storage_options)

    blocking = bool(report["problems"]) or (args.strict and bool(report["warnings"]))

    if args.plan:
        pings = sum(item.get("pings") or 0 for item in records if not item.get("error"))
        channels = channel_selection or (records[0].get("channels") if records else []) or []
        source_bytes = sum(
            _input_bytes(item["path"]) for item in records if not item.get("error")
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
            local_report = report_uri.startswith("file://")
            print(report_uri if (args.json or not local_report) else report_uri[len("file://"):])
        sys.exit(4 if (report["problems"] or report["warnings"]) else 0)

    if blocking:
        logger.error(
            "Refusing to combine. Fix the problems above, or re-run without --strict "
            "if the warnings are understood and intended."
        )
        sys.exit(4)

    # ---------------------------
    # Reuse (the QC gate has passed)
    # ---------------------------
    if reuse:
        out.reused = True
        if not args.quiet:
            print(f"{TOOL}: reusing {target.raw} (identical product aa:{out.short} "
                  "already exists; --force recomputes)", file=sys.stderr)
        _progress(args.progress, "done", exit=0)
        if args.json:
            # This run's inputs, time range and report; the store's own
            # production time and version.
            parents = [item["uri"] for item in records if not item.get("error")]
            starts = [item["start"] for item in report["inputs"] if item.get("start")]
            ends = [item["end"] for item in report["inputs"] if item.get("end")]
            handle = _reuse_handle(
                target, existing, out.hash, out.base, parents,
                [min(starts), max(ends)] if starts and ends else None, report_uri,
            )
            print(json.dumps(handle, separators=(",", ":"), default=str))
        else:
            print(target.raw)
        sys.exit(0)

    # ---------------------------
    # Combine
    # ---------------------------
    if replace and target.exists():
        logger.info(f"Overwriting {target.raw}")

    write_target = target
    if target.staged:
        # gs:// .nc: write a local staging file; finish() uploads it.
        out = run.plan(ext=ext, explicit=explicit, directory=default_dir, stage=True)
        write_target = Target(str(out.local))
        state["write"] = write_target

    try:
        combine(
            paths=paths,
            target=write_target,
            channel_selection=channel_selection,
            overwrite=replace,
            compression=args.compression,
            chunk_pings=args.chunk_pings,
            # Consolidated once, below, after every attribute is written.
            consolidated=False,
            storage_options=storage_options,
            progress=args.progress,
        )
    except Exception as exc:  # noqa: BLE001
        if write_target.exists():
            _stamp(write_target, False, storage_options, extra={"error": str(exc)})
        logger.exception(f"Error during combine: {exc}")
        if out.staging is not None:
            shutil.rmtree(out.staging, ignore_errors=True)
        _progress(args.progress, "done", exit=1)
        sys.exit(1)

    parents = [item["uri"] for item in records if not item.get("error")]
    starts = [item["start"] for item in report["inputs"] if item.get("start")]
    ends = [item["end"] for item in report["inputs"] if item.get("end")]
    time_range = [min(starts), max(ends)] if starts and ends else None
    # Recorded in the provenance too, so a reused .nc export (which has no
    # store attributes) can still produce the same handle. A store keeps its
    # parents in its own `provenance` attribute; not repeated there.
    # The store layout asked for is recorded as well: reuse needs the same
    # product in the same layout (chunking, codec, consolidation).
    extra = {TOOL: {"time": time_range, "report": report_uri, "sort": args.sort,
                    "layout": _layout(args, target)}}
    if target.is_netcdf:
        extra[TOOL]["parents"] = parents

    # 1. The aa provenance. Written before this tool's own attributes because
    #    the core also sets aa_kind (to the provenance kind, "echodata") and
    #    the store's aa_kind must stay "l1".
    try:
        if target.remote and not target.staged:
            _embed_remote(run, out, target, storage_options, extra)
            run.finish(out, embed=False, publish=False, emit=False)
        else:
            # Local .nc / .zarr: embedded by the core. gs:// .nc: embedded in
            # the staging file, then uploaded with aa-product-hash metadata.
            run.finish(out, extra=extra, emit=False)
    except Exception as exc:  # noqa: BLE001
        if target.staged:
            logger.exception(f"Could not upload {target.raw}: {exc}")
            if out.staging is not None:
                shutil.rmtree(out.staging, ignore_errors=True)
            _progress(args.progress, "done", exit=1)
            sys.exit(1)
        logger.warning(f"Could not record the aa provenance in {target.raw}: {exc}")

    # 2. This tool's store attributes and the completion marker (no-ops for a
    #    NetCDF export). 3. Consolidated metadata last, so it includes all of
    #    the above. A consolidated block echopype left behind is refreshed even
    #    under --no-consolidated: a stale one is worse than either.
    _annotate(write_target, report_uri, parents, report, storage_options)
    _stamp(write_target, True, storage_options, extra={"inputs": len(parents)})
    if not target.is_netcdf and (
        args.consolidated or _has_consolidated(write_target, storage_options)
    ):
        _consolidate(write_target, storage_options)

    logger.success(f"Generated {target.raw} with aa-combine.")
    _progress(args.progress, "done", exit=0)

    # ---------------------------
    # Output
    # ---------------------------
    if args.json:
        handle = _handle(target, parents, _now(), time_range, report_uri,
                         out.hash, out.base, reused=False)
        print(json.dumps(handle, separators=(",", ":"), default=str))
    else:
        # The path, matching every other aa-* tool, so this drops into an
        # existing pipeline without the whole chain being converted at once.
        print(target.raw)

    sys.exit(0)


if __name__ == "__main__":
    main()
