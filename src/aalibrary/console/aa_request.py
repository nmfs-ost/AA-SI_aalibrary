#!/usr/bin/env python3
"""
aa-request

Console tool for building, merging and validating the request YAML that
aa-fetch reads — the vessel / survey / instrument / time-window document that
says which files to pull from NCEI.

    requests:
    - vessel: Alaska_Knight
      survey: CHS12AK
      instrument: ES60
      time-windows:
      - start-date: "2012-08-13"
        start-time: "00:00:00"
        end-date: "2012-08-14"
        end-time: "00:00:00"

aa-get already produces this by asking questions. That is the right shape for
a person at a prompt and the wrong shape for everything else: a job runner has
nobody to answer them, so the Workbench cannot drive it and a recipe cannot
either. aa-request produces the identical document from flags, from an
existing document, or from stdin — and aa-fetch does not have to change at
all, because the file it reads is the same file.

    aa-request --vessel Alaska_Knight --survey CHS12AK --instrument ES60 \\
               --from 2012-08-13 --to 2012-08-14 -o request.yaml
    aa-fetch request.yaml

Pipeline-friendly: with -o it writes the file and prints its path to stdout,
like every other aa-* tool. Without -o it prints the YAML itself, so it can be
piped straight into aa-fetch or into a heredoc-free `tee`.

On quoting, which is not cosmetic
---------------------------------
PyYAML implements YAML 1.1, where an unquoted `12:30:00` is a *sexagesimal
integer* and parses as 45000, and an unquoted `2012-08-13` parses as a
datetime.date rather than a string. A generator that emits those bare produces
a document that loads without error and is wrong. Every date and time this
tool writes is quoted, and --check flags any it reads that were not.

Typical usage:
    aa-request --vessel Alaska_Knight --survey CHS12AK --instrument ES60 \\
               --from 2012-08-13T00:00:00 --to 2012-08-16T00:00:00 \\
               --split-days 1 -o week.yaml
    aa-request --check week.yaml
    aa-request -i week.yaml --vessel Reuben_Lasker --survey RL2107 \\
               --instrument EK80 --from 2021-06-01 --to 2021-06-02
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
import json
import os
import re
import signal
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import yaml

# Pipeline tools should die cleanly when the downstream end of the pipe
# closes early (`... | head -n 1`), not throw BrokenPipeError. Guarded
# with hasattr because SIGPIPE doesn't exist on Windows.
if hasattr(signal, "SIGPIPE"):
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)


TOOL = "aa-request"
VERSION = "0.1.0"

# The document's own vocabulary. Kept as constants so a typo in this file is a
# NameError rather than a silently different key than aa-fetch expects.
ROOT = "requests"
WINDOWS = "time-windows"
REQUEST_KEYS = {"vessel", "survey", "instrument", WINDOWS}
WINDOW_KEYS = {"start-date", "start-time", "end-date", "end-time"}


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
    Usage: aa-request [OPTIONS] [EXISTING.yaml]

    Arguments:
      EXISTING.yaml             A request document to merge into or check.
                                Optional; also accepted via -i or stdin.

    Building a request:
      --vessel NAME             Vessel as NCEI spells it (Alaska_Knight).
                                Spaces are converted to underscores.
      --survey NAME             Survey identifier (CHS12AK).
      --instrument NAME         Echosounder (ES60, EK60, EK80).
      --from WHEN               Window start. A date (2012-08-13) or a
                                datetime (2012-08-13T06:00:00). A bare date
                                means 00:00:00.
      --to WHEN                 Window end. A bare date means 00:00:00, so
                                --from 2012-08-13 --to 2012-08-14 is one
                                whole day.
      --window FROM/TO          Another window for the same request. Repeat
                                for as many as you need.

      --split-days N            Break each window into N-day pieces. One
                                request, many windows — which is what makes
                                a long survey resumable a day at a time
                                instead of all or nothing.

      --pad-minutes N           Move each window start N minutes earlier.
                                Raw files span 30-60 minutes, so the file
                                covering 00:00 usually *starts* before it. A
                                window that begins exactly at 00:00 misses
                                that file, and the data you asked for begins
                                mid-file. The document has no way to say
                                "and the file that spans this edge", so
                                widening the window is how you say it.
                                Default 0: nothing is widened silently.

    Working with an existing document:
      -i, --input PATH          Merge into this document rather than starting
                                empty. New windows join an existing request
                                when vessel, survey and instrument all match;
                                otherwise a new request is appended.
      --check                   Validate and report. Writes nothing. Exit 4
                                if the document is malformed.
      --merge-windows           Combine overlapping or touching windows in the
                                result. Two windows that abut describe one
                                range, and aa-fetch would list the seam twice.

    Output:
      -o, --output_path PATH    Write here and print the path to stdout.
                                Without it, the YAML goes to stdout.
      --json                    Emit the document as JSON instead of YAML.
                                For the Workbench, not for aa-fetch.
      --force                   Overwrite an existing output file.

      -q, --quiet               Warnings and errors only.
      --debug                   Verbose logging.
      -h, --help                This message.

    Exit codes:
      0 ok        1 runtime error    2 usage    4 validation failed

    Examples:
      aa-request --vessel Alaska_Knight --survey CHS12AK --instrument ES60 \\
                 --from 2012-08-13 --to 2012-08-14 -o request.yaml
      aa-request --check request.yaml
      aa-request -i request.yaml --window 2012-08-20/2012-08-21 --merge-windows
      aa-request --vessel Alaska_Knight --survey CHS12AK --instrument ES60 \\
                 --from 2012-08-13 --to 2012-08-20 --split-days 1 | aa-fetch -
    """
    print(help_text)


# --------------------------------------------------------------------------- #
# Quoting
#
# PyYAML is YAML 1.1: `12:30:00` unquoted is the integer 45000 and
# `2012-08-13` unquoted is a datetime.date. A document emitted without quotes
# round-trips into different types than it was written with, and nothing
# raises. So dates and times are carried in a str subclass with a representer
# that always quotes, which makes the correct output structural rather than
# something every call site has to remember.
# --------------------------------------------------------------------------- #
class Quoted(str):
    """A string that always serialises with quotes."""


def _represent_quoted(dumper: yaml.Dumper, data: Quoted):
    return dumper.represent_scalar("tag:yaml.org,2002:str", str(data), style='"')


yaml.add_representer(Quoted, _represent_quoted)


class _Dumper(yaml.Dumper):
    """Block style, insertion order, and block sequences left indentless —
    the dash sitting in the same column as the key it belongs to, which is how
    the existing aa-get documents are written.

    PyYAML does that by default; this subclass exists only to carry the Quoted
    representer without registering it globally, so importing this module
    cannot change how some other part of a program dumps YAML.
    """


_Dumper.add_representer(Quoted, _represent_quoted)


def dump_yaml(document: dict) -> str:
    return yaml.dump(
        document,
        Dumper=_Dumper,
        sort_keys=False,
        default_flow_style=False,
        allow_unicode=True,
        width=100,
    )


# --------------------------------------------------------------------------- #
# Time
# --------------------------------------------------------------------------- #
_DATE_ONLY = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def parse_when(text: str, *, field: str) -> datetime:
    """A date or datetime string -> naive UTC datetime.

    Naive rather than aware because the document has no timezone field and
    NCEI file names are UTC by convention. Inventing an offset here would put
    one in the output that aa-fetch has nowhere to read.
    """
    value = str(text).strip()
    if not value:
        raise ValueError(f"{field} is empty")
    if _DATE_ONLY.match(value):
        return datetime.strptime(value, "%Y-%m-%d")
    normalised = value.replace(" ", "T").rstrip("Z")
    try:
        parsed = datetime.fromisoformat(normalised)
    except ValueError as exc:
        raise ValueError(
            f"{field}: {value!r} is not a date (2012-08-13) or a datetime "
            f"(2012-08-13T06:00:00)"
        ) from exc
    return parsed.replace(tzinfo=None) if parsed.tzinfo else parsed


def window_from(start: datetime, end: datetime) -> dict:
    """A window in the document's own spelling, with every value quoted."""
    return {
        "start-date": Quoted(start.strftime("%Y-%m-%d")),
        "start-time": Quoted(start.strftime("%H:%M:%S")),
        "end-date": Quoted(end.strftime("%Y-%m-%d")),
        "end-time": Quoted(end.strftime("%H:%M:%S")),
    }


def window_bounds(window: dict) -> tuple[datetime, datetime]:
    """Read a window back into datetimes, tolerating the types YAML 1.1 makes.

    A document written by hand may hold a datetime.date where a string was
    meant, or an int where a time was meant. Both are recoverable, and
    refusing to read them would mean the tool cannot check the documents most
    likely to need checking.
    """
    def _date(value: Any, field: str) -> str:
        if hasattr(value, "strftime"):
            return value.strftime("%Y-%m-%d")
        return str(value).strip()

    def _time(value: Any, field: str) -> str:
        if hasattr(value, "strftime"):
            return value.strftime("%H:%M:%S")
        if isinstance(value, int):
            # YAML 1.1 sexagesimal: 45000 was written as 12:30:00.
            hours, rest = divmod(int(value), 3600)
            minutes, seconds = divmod(rest, 60)
            return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        return str(value).strip() or "00:00:00"

    start = parse_when(
        f"{_date(window.get('start-date'), 'start-date')}T"
        f"{_time(window.get('start-time'), 'start-time')}",
        field="start",
    )
    end = parse_when(
        f"{_date(window.get('end-date'), 'end-date')}T"
        f"{_time(window.get('end-time'), 'end-time')}",
        field="end",
    )
    return start, end


def split_window(start: datetime, end: datetime, days: int) -> list[tuple[datetime, datetime]]:
    """Break [start, end) into pieces of at most *days*.

    The last piece is whatever remains rather than a full-length one that
    overshoots — a request that asks for more than was surveyed is not wrong,
    but it makes every downstream count look short.
    """
    if days <= 0 or end <= start:
        return [(start, end)]
    pieces: list[tuple[datetime, datetime]] = []
    cursor = start
    step = timedelta(days=days)
    while cursor < end:
        pieces.append((cursor, min(cursor + step, end)))
        cursor += step
    return pieces


def merge_windows(windows: list[dict]) -> list[dict]:
    """Collapse overlapping or touching windows into one.

    Touching counts: two windows where one ends exactly where the next begins
    describe a single range, and leaving them separate makes aa-fetch list the
    boundary file under both.
    """
    bounds: list[tuple[datetime, datetime]] = []
    for window in windows:
        try:
            bounds.append(window_bounds(window))
        except ValueError:
            # An unreadable window is left exactly as it was: --check is where
            # that gets reported, and silently dropping it here would hide it.
            return windows
    bounds.sort()
    merged: list[list[datetime]] = []
    for start, end in bounds:
        if merged and start <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])
    return [window_from(start, end) for start, end in merged]


# --------------------------------------------------------------------------- #
# Documents
# --------------------------------------------------------------------------- #
def normalise_vessel(name: str) -> str:
    """NCEI folder spelling: "Alaska Knight" -> "Alaska_Knight"."""
    return re.sub(r"\s+", "_", str(name).strip())


def load_document(source: str | Path) -> dict:
    text = Path(source).read_text(encoding="utf-8") if source != "-" else sys.stdin.read()
    document = yaml.safe_load(text)
    if document is None:
        return {ROOT: []}
    if not isinstance(document, dict) or ROOT not in document:
        raise ValueError(f"not a request document: no top-level {ROOT!r} key")
    if not isinstance(document[ROOT], list):
        raise ValueError(f"{ROOT!r} must be a list")
    return document


def add_request(
    document: dict,
    vessel: str,
    survey: str,
    instrument: str,
    windows: list[dict],
) -> tuple[dict, bool]:
    """Add windows, joining an existing request when the three keys match.

    Joining rather than always appending is what keeps a document built up over
    several invocations from growing three identical requests that differ only
    in their windows — which aa-fetch would then list three times.
    """
    requests = document.setdefault(ROOT, [])
    for request in requests:
        if (
            str(request.get("vessel", "")) == vessel
            and str(request.get("survey", "")) == survey
            and str(request.get("instrument", "")) == instrument
        ):
            request.setdefault(WINDOWS, []).extend(windows)
            return document, True
    requests.append(
        {
            "vessel": Quoted(vessel),
            "survey": Quoted(survey),
            "instrument": Quoted(instrument),
            WINDOWS: list(windows),
        }
    )
    return document, False


def normalise_document(document: dict) -> int:
    """Rewrite every readable value through the quoting types.

    Two things this buys. Uniformity: a document built up over several
    invocations otherwise comes out with the entries this tool added quoted
    and the ones it loaded bare, which looks like the quoting means something.
    And repair: a window that was written unquoted loaded as an int and a
    date, and writing it back through `window_from` puts it on disk as the
    strings it was always meant to be. So passing a suspect document through
    aa-request fixes it, rather than merely complaining about it.

    Values that do not parse are left exactly as they are — --check reports
    those, and quietly rewriting something this function does not understand
    is how a validator becomes a corrupter.
    """
    repaired = 0
    for request in document.get(ROOT, []):
        if not isinstance(request, dict):
            continue
        for key in ("vessel", "survey", "instrument"):
            if isinstance(request.get(key), str) and not isinstance(request[key], Quoted):
                request[key] = Quoted(request[key])
        windows = request.get(WINDOWS)
        if not isinstance(windows, list):
            continue
        for index, window in enumerate(windows):
            if not isinstance(window, dict) or WINDOW_KEYS - set(window):
                continue
            try:
                start, end = window_bounds(window)
            except (ValueError, AttributeError):
                continue
            rewritten = window_from(start, end)
            if any(str(window[key]) != str(rewritten[key]) for key in WINDOW_KEYS):
                repaired += 1
            windows[index] = rewritten
    return repaired


def validate(document: dict) -> dict:
    """Everything wrong with a request document, without trying to fix it."""
    problems: list[str] = []
    warnings_: list[str] = []

    requests = document.get(ROOT)
    if not isinstance(requests, list):
        return {"problems": [f"top-level {ROOT!r} is missing or not a list"],
                "warnings": [], "requests": 0, "windows": 0, "ok": False}
    if not requests:
        warnings_.append("document contains no requests")

    total_windows = 0
    for index, request in enumerate(requests):
        label = f"requests[{index}]"
        if not isinstance(request, dict):
            problems.append(f"{label} is not a mapping")
            continue

        for key in ("vessel", "survey", "instrument"):
            value = request.get(key)
            if value in (None, ""):
                problems.append(f"{label}: {key} is missing")
            elif key == "vessel" and re.search(r"\s", str(value)):
                warnings_.append(
                    f"{label}: vessel {value!r} contains a space; NCEI spells it "
                    f"{normalise_vessel(value)!r}"
                )

        # A typo in a key is the failure this document is most prone to,
        # because nothing rejects it: aa-fetch reads the keys it knows and
        # silently ignores the rest, so `time-window` fetches everything.
        unknown = set(request) - REQUEST_KEYS
        if unknown:
            warnings_.append(f"{label}: unrecognised key(s) {sorted(unknown)}")

        windows = request.get(WINDOWS)
        if windows is None:
            problems.append(f"{label}: {WINDOWS} is missing")
            continue
        if not isinstance(windows, list) or not windows:
            problems.append(f"{label}: {WINDOWS} must be a non-empty list")
            continue

        bounds: list[tuple[datetime, datetime, int]] = []
        for window_index, window in enumerate(windows):
            total_windows += 1
            window_label = f"{label}.{WINDOWS}[{window_index}]"
            if not isinstance(window, dict):
                problems.append(f"{window_label} is not a mapping")
                continue
            missing = WINDOW_KEYS - set(window)
            if missing:
                problems.append(f"{window_label}: missing {sorted(missing)}")
                continue
            extra = set(window) - WINDOW_KEYS
            if extra:
                warnings_.append(f"{window_label}: unrecognised key(s) {sorted(extra)}")

            # The YAML 1.1 trap, reported where it can still be fixed.
            for key in ("start-time", "end-time"):
                if isinstance(window.get(key), int):
                    warnings_.append(
                        f"{window_label}: {key} loaded as the integer "
                        f"{window[key]} — it was written unquoted, and YAML 1.1 "
                        "reads 12:30:00 as sexagesimal. Quote it."
                    )
            for key in ("start-date", "end-date"):
                if hasattr(window.get(key), "strftime"):
                    warnings_.append(
                        f"{window_label}: {key} loaded as a date object, not a "
                        "string — it was written unquoted. Quote it."
                    )

            try:
                start, end = window_bounds(window)
            except ValueError as exc:
                problems.append(f"{window_label}: {exc}")
                continue
            if end <= start:
                problems.append(
                    f"{window_label}: ends at or before it starts "
                    f"({start.isoformat()} -> {end.isoformat()})"
                )
                continue
            bounds.append((start, end, window_index))

        bounds.sort()
        for (start_a, end_a, index_a), (start_b, _end_b, index_b) in zip(bounds, bounds[1:]):
            if start_b < end_a:
                warnings_.append(
                    f"{label}: windows {index_a} and {index_b} overlap; aa-fetch "
                    "will list the shared files twice. --merge-windows fixes it"
                )

    return {
        "problems": problems,
        "warnings": warnings_,
        "requests": len(requests) if isinstance(requests, list) else 0,
        "windows": total_windows,
        "ok": not problems,
    }


def summarise(document: dict) -> dict:
    """A compact machine view of the document, for --json and for logging."""
    entries = []
    for request in document.get(ROOT, []):
        if not isinstance(request, dict):
            continue
        spans = []
        for window in request.get(WINDOWS, []) or []:
            try:
                start, end = window_bounds(window)
            except (ValueError, AttributeError):
                continue
            spans.append(
                {
                    "start": start.isoformat() + "Z",
                    "end": end.isoformat() + "Z",
                    "hours": round((end - start).total_seconds() / 3600, 2),
                }
            )
        entries.append(
            {
                "vessel": str(request.get("vessel", "")),
                "survey": str(request.get("survey", "")),
                "instrument": str(request.get("instrument", "")),
                "windows": spans,
                "hours": round(sum(span["hours"] for span in spans), 2),
            }
        )
    return {
        "schema": "aa/request/1",
        "tool": TOOL,
        "version": VERSION,
        "requests": entries,
        "totalWindows": sum(len(entry["windows"]) for entry in entries),
        "totalHours": round(sum(entry["hours"] for entry in entries), 2),
    }


# --------------------------------------------------------------------------- #
# main
# --------------------------------------------------------------------------- #
def main() -> None:
    if "--help" in sys.argv or "-h" in sys.argv:
        print_help()
        sys.exit(0)
    if len(sys.argv) == 1 and sys.stdin.isatty():
        print_help()
        sys.exit(0)

    parser = argparse.ArgumentParser(
        description="Build, merge and validate an aa-fetch request document.",
        add_help=False,
    )
    parser.add_argument("existing", nargs="*", default=[])
    parser.add_argument("-i", "--input", dest="input_path", default=None)
    parser.add_argument("--vessel", default=None)
    parser.add_argument("--survey", default=None)
    parser.add_argument("--instrument", "--sonar_model", dest="instrument", default=None)
    parser.add_argument("--from", dest="start", default=None)
    parser.add_argument("--to", dest="end", default=None)
    parser.add_argument("--window", action="append", default=[], metavar="FROM/TO")
    parser.add_argument("--split-days", "--split_days", dest="split_days", type=int, default=0)
    parser.add_argument("--pad-minutes", "--pad_minutes", dest="pad_minutes", type=int, default=0)
    parser.add_argument("--merge-windows", "--merge_windows", dest="merge_windows",
                        action="store_true")
    parser.add_argument("--check", action="store_true")
    parser.add_argument("-o", "--output_path", "--output", dest="output_path", default=None)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("-q", "--quiet", action="store_true")
    parser.add_argument("--debug", action="store_true")

    # See aa-store: argparse cannot match a variadic positional across an
    # optional, so `--check doc.yaml` loses the filename. Collect leftovers.
    args, leftover = parser.parse_known_args()
    stray = [item for item in leftover if item.startswith("-")]
    if stray:
        logger.error(f"Unknown option(s): {' '.join(stray)}")
        sys.exit(2)
    args.existing = list(args.existing) + [item for item in leftover if not item.startswith("-")]

    if args.debug and args.quiet:
        logger.error("Use --debug OR --quiet, not both.")
        sys.exit(2)
    _configure_logging(args.quiet, args.debug)

    # ---------------------------
    # Resolve the source document
    # ---------------------------
    source = args.input_path or (args.existing[0] if args.existing else None)
    if len(args.existing) > 1:
        logger.error(f"Expected at most one input document, got {len(args.existing)}.")
        sys.exit(2)
    if source is None and not sys.stdin.isatty() and not any(
        [args.vessel, args.survey, args.instrument]
    ):
        source = "-"

    document: dict = {ROOT: []}
    if source is not None:
        if source != "-" and not Path(source).exists():
            logger.error(f"File '{source}' does not exist.")
            sys.exit(1)
        try:
            document = load_document(source)
        except (yaml.YAMLError, ValueError, OSError) as exc:
            logger.error(f"Could not read {source}: {exc}")
            sys.exit(1)
        logger.info(f"Loaded {len(document.get(ROOT, []))} request(s) from {source}")

    # ---------------------------
    # Build new windows
    # ---------------------------
    building = any([args.vessel, args.survey, args.instrument, args.start, args.end, args.window])
    if building:
        missing = [
            name
            for name, value in (
                ("--vessel", args.vessel),
                ("--survey", args.survey),
                ("--instrument", args.instrument),
            )
            if not value
        ]
        if missing:
            logger.error(
                f"Building a request needs {', '.join(missing)}. "
                "A request without all three matches nothing in NCEI."
            )
            sys.exit(2)

        pairs: list[tuple[str, str]] = []
        if args.start or args.end:
            if not (args.start and args.end):
                logger.error("--from and --to go together; give both or neither.")
                sys.exit(2)
            pairs.append((args.start, args.end))
        for spec in args.window:
            if "/" not in spec:
                logger.error(f"--window {spec!r} should be FROM/TO, e.g. 2012-08-13/2012-08-14.")
                sys.exit(2)
            first, second = spec.split("/", 1)
            pairs.append((first, second))
        if not pairs:
            logger.error("No time window given. Use --from/--to or --window FROM/TO.")
            sys.exit(2)

        windows: list[dict] = []
        for first, second in pairs:
            try:
                start = parse_when(first, field="--from")
                end = parse_when(second, field="--to")
            except ValueError as exc:
                logger.error(str(exc))
                sys.exit(2)
            if end <= start:
                logger.error(
                    f"Window ends at or before it starts: {start.isoformat()} -> "
                    f"{end.isoformat()}. A bare date means 00:00:00, so a single "
                    "day is --from 2012-08-13 --to 2012-08-14."
                )
                sys.exit(2)
            pieces = split_window(start, end, args.split_days)
            if args.pad_minutes:
                # Pad the leading edge only, and after splitting rather than
                # before. Padding first drags every internal boundary back by
                # the same amount, so a week split by day comes out as eight
                # windows hinged on 23:00 instead of seven on midnight.
                #
                # Internal boundaries do not need padding anyway: consecutive
                # windows are contiguous, so the file spanning 14T00:00 is
                # already caught by the window *ending* there — it started
                # before the boundary, which is the whole condition. Padding
                # them would only make adjacent windows overlap, and aa-fetch
                # would list the shared file under both.
                first_start, first_end = pieces[0]
                pieces[0] = (first_start - timedelta(minutes=args.pad_minutes), first_end)
                logger.info(
                    f"Widened the leading edge by {args.pad_minutes} min to "
                    f"{pieces[0][0].isoformat()} so the file spanning it is included"
                )
            for piece_start, piece_end in pieces:
                windows.append(window_from(piece_start, piece_end))

        vessel = normalise_vessel(args.vessel)
        if vessel != args.vessel:
            logger.info(f"Vessel normalised to NCEI spelling: {vessel}")
        document, joined = add_request(
            document, vessel, str(args.survey).strip(), str(args.instrument).strip(), windows
        )
        logger.info(
            f"{'Added to existing' if joined else 'Created'} request "
            f"{vessel}/{args.survey}/{args.instrument} with {len(windows)} window(s)"
        )

    if args.merge_windows:
        for request in document.get(ROOT, []):
            if isinstance(request, dict) and isinstance(request.get(WINDOWS), list):
                before = len(request[WINDOWS])
                request[WINDOWS] = merge_windows(request[WINDOWS])
                if len(request[WINDOWS]) != before:
                    logger.info(
                        f"Merged {before} window(s) into {len(request[WINDOWS])} for "
                        f"{request.get('vessel')}/{request.get('survey')}"
                    )

    # ---------------------------
    # Validate
    # ---------------------------
    result = validate(document)
    for warning in result["warnings"]:
        logger.warning(warning)
    for problem in result["problems"]:
        logger.error(problem)

    if args.check:
        logger.info(
            f"{result['requests']} request(s), {result['windows']} window(s): "
            f"{len(result['problems'])} problem(s), {len(result['warnings'])} warning(s)"
        )
        if args.json:
            payload = summarise(document)
            payload["validation"] = result
            print(json.dumps(payload, separators=(",", ":"), default=str))
        sys.exit(0 if result["ok"] and not result["warnings"] else 4)

    if not result["ok"]:
        logger.error("Refusing to write a document with the problems above.")
        sys.exit(4)

    # After validation, never before: --check has to be able to report the
    # unquoted values that a write silently repairs. Reporting and repairing
    # are different jobs and the tool does exactly one of them per run.
    repaired = normalise_document(document)
    if repaired:
        logger.info(f"Rewrote {repaired} window(s) with quoted dates and times")

    # ---------------------------
    # Output
    # ---------------------------
    if args.json:
        rendered = json.dumps(summarise(document), separators=(",", ":"), default=str)
    else:
        rendered = dump_yaml(document)

    if args.output_path:
        output = Path(args.output_path).expanduser()
        if output.exists() and not args.force:
            logger.error(f"{output} exists. Pass --force to replace it.")
            sys.exit(2)
        try:
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(rendered if rendered.endswith("\n") else rendered + "\n",
                              encoding="utf-8")
        except OSError as exc:
            logger.error(f"Could not write {output}: {exc}")
            sys.exit(1)
        logger.success(f"Generated {output.resolve()} with aa-request.")
        # The path, matching every other aa-* tool, so this composes:
        #     aa-request ... -o r.yaml | xargs aa-fetch
        print(output.resolve())
    else:
        # No -o: the document itself is the product, so it goes to stdout and
        # can be piped straight into aa-fetch.
        sys.stdout.write(rendered if rendered.endswith("\n") else rendered + "\n")

    sys.exit(0)


if __name__ == "__main__":
    main()
