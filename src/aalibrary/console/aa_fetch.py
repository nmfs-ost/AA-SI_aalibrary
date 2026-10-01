#!/usr/bin/env python3
"""
aa-fetch

Execute a multi-fetch job defined by a request YAML.

Reads the request (a YAML path from a positional argument or stdin, or the
YAML document itself on stdin), parses it via
aalibrary.utils.multi_fetch_yaml_parser, runs the resulting SQL against the
metadata DB, and downloads matching files into a per-run directory under
--output_root. Beside every downloaded file it writes <file>.aa.json with
the NCEI object it came from.

Pipeline contract:
- stdin  : optional; a YAML path (one line), or the YAML document itself
           ('-', or piped text whose first line is 'requests:')
- stdout : on success, the absolute path of the download directory
           (so aa-fetch can feed aa-ed and the rest of the chain), also
           when nothing matched; empty on failure (non-zero exit)
- stderr : all logs and errors (aalibrary's own prints included)

Typical usage:
    aa-get -n request.yaml | aa-fetch
    aa-fetch ./request.yaml -o ./downloads -n run_001
    aa-request --vessel ... --from ... --to ... | aa-fetch -

    # End-to-end build -> fetch -> convert -> combine -> Sv -> graph:
    aa-get | aa-fetch | aa-ed | aa-combine | aa-sv | aa-graph
"""
from __future__ import annotations

# === Silence noisy library logs BEFORE any heavy imports ===
import logging
import sys
import warnings

logging.disable(logging.CRITICAL)
warnings.filterwarnings("ignore")

from loguru import logger
logger.remove()
# INFO level so the user sees fetch progress / counts / destination on stderr.
# Stdout carries only the download directory, per the pipeline contract above.
logger.add(sys.stderr, level="INFO")

import argparse
import contextlib
import re
from datetime import datetime
from pathlib import Path

from aalibrary.console._core import (
    Help, ToolSpec, record_source, render, show_help, stdio,
)

NCEI_BUCKET = "noaa-wcsd-pds"
# A request document read from stdin is kept here, inside the download
# directory, so the directory records what was asked for.
REQUEST_FILE_NAME = "aa-fetch-request.yaml"

SPEC = ToolSpec(name="aa-fetch", role="source", kind="raw", engines=())

HELP = Help(
    summary="Download every NCEI file a request YAML matches, into one directory.",
    does=(
        "Turns the request document (vessel / survey / instrument / time "
        "windows, as written by aa-request or aa-get) into a query against "
        "aalibrary's cache of NCEI metadata in BigQuery (<project>.metadata."
        "ncei_cache), then downloads every matching object from NCEI's public "
        "bucket noaa-wcsd-pds into a new directory. Files keep their NCEI names."
    ),
    stdin=(
        "The request: a YAML path (argument, or one line on stdin), or the YAML "
        "document itself. '-' reads all of stdin as the document; piped text "
        "whose first line is 'requests:' is read as the document too, so "
        "aa-request ... | aa-fetch works with or without '-'."
    ),
    stdout=(
        "The absolute path of the download directory, one line; also when "
        "nothing matched (the directory is then empty). Empty on failure."
    ),
    metadata=(
        "Writes <file>.aa.json beside every downloaded file: the NCEI object it "
        "came from (s3://noaa-wcsd-pds/data/raw/...), its MD5 and size, ship/"
        "survey/sonar as NCEI spells them, and the request file. aa-nc records "
        "that origin as the source of the .nc it writes (aa-metadata FILE.nc "
        "shows it). The sidecar never changes a hash. A document read from "
        "stdin is kept as <dir>/" + REQUEST_FILE_NAME + "."
    ),
    options=[
        ("YAML_PATH | -", "the request file, or '-' for the document on stdin"),
        ("-o, --output_root DIR", "parent of the download directory (default: current directory)"),
        ("-n, --download_dir_name NAME", "download directory name (default: aa_fetch_<YYYYMMDD_HHMMSS>)"),
    ],
    files=(
        "Reads BigQuery (ggn-nmfs-aa-prod-1.metadata.ncei_cache: aalibrary "
        "selects the production project when imported; needs Google Cloud "
        "credentials) and NCEI's bucket (anonymous). Writes DIR/<file> and DIR/<file>.aa.json "
        "for each match. All files land in one flat directory: two matches with "
        "the same file name overwrite each other; aa-fetch warns and records no "
        "origin for that name. Matches that NCEI does not return are reported "
        "on stderr."
    ),
    pipeline=(
        "Source stage. Feed it from aa-request or aa-get; its directory feeds "
        "aa-ed (directory mode). Exit codes: 0 ok (also for zero matches), "
        "1 unreadable request, query or download error, 2 usage (no request, "
        "empty pipe, bad directory name)."
    ),
    examples=[
        "aa-request --vessel Alaska_Knight --survey CHS12AK --instrument ES60 \\",
        "             --from 2012-08-13 --to 2012-08-14 | aa-fetch - -o ./downloads",
        "aa-fetch request.yaml -o ./downloads -n run_001",
        "aa-get | aa-fetch | aa-ed | aa-combine | aa-sv | aa-graph",
    ],
)


def silence_all_logs():
    """Re-apply suppression in case a library re-enabled logging
    or added its own loguru sink during initialization."""
    logging.disable(logging.CRITICAL)
    for name in [None] + list(logging.root.manager.loggerDict):
        lg = logging.getLogger(name)
        lg.handlers.clear()
        lg.propagate = True
    logger.remove()
    logger.add(sys.stderr, level="INFO")


def print_help() -> None:
    """Curated help (also used by the docs generator)."""
    sys.stdout.write(render(SPEC, HELP, _build_parser()))


def print_help_full() -> None:
    help_text = r"""
aa-fetch — Execute a YAML-driven multi-fetch job

WHAT THIS TOOL DOES
  • Reads a YAML document describing one or more fetch "requests"
  • Parses it via aalibrary.utils.multi_fetch_yaml_parser
  • Builds a SQL query and runs it against the metadata DB (BigQuery)
  • Downloads matching files from NCEI into a per-run directory
  • Writes <file>.aa.json beside each downloaded file: the NCEI object it
    came from (s3://noaa-wcsd-pds/...) and its MD5. aa-nc records that
    origin; see aa-metadata.
  • Logs progress and errors to stderr (loguru)
  • Prints the absolute path of the download directory to stdout on
    success (also when nothing matched), so aa-ed (in directory mode) and
    the rest of the chain can be piped after aa-fetch. On failure stdout
    is empty and the exit code is non-zero.

HOW THE REQUEST IS PROVIDED
  (A) Positional argument:
      aa-fetch /path/to/fetch_request.yaml

  (B) A path piped via stdin (one line):
      aa-get -n req.yaml | aa-fetch

  (C) The YAML document itself on stdin:
      aa-request --vessel ... --from ... --to ... | aa-fetch -
      cat req.yaml | aa-fetch -

  Notes:
    • '-' reads ALL of stdin as the document. Without '-', piped text whose
      first line is 'requests:' is also read as the document; otherwise the
      first line is taken as a path.
    • A document read from stdin is saved in the download directory as
      aa-fetch-request.yaml.
    • Run with no arguments on a terminal, aa-fetch prints help and exits.
      With flags but no request (and nothing piped) it exits 2.
    • An empty pipe is an error (exit 2): the previous stage failed.
    • Flags can be combined with stdin input:
          cat path.txt | aa-fetch -o ./downloads -n run_001

DOWNLOAD DIRECTORY
  -o, --output_root PATH
      Parent directory for the per-run download directory.
      Default: current working directory.

  -n, --download_dir_name NAME
      Name of the per-run download directory under --output_root.
      Default: aa_fetch_<YYYYMMDD_HHMMSS>

EXIT CODES
  0  success (also when nothing matched)
  1  YAML/import/runtime/download error
  2  invalid usage (missing request, bad output dir name)

PIPELINE EXAMPLES
  aa-get | aa-fetch
  aa-get -n request.yaml | aa-fetch -o ./downloads -n run_001
  aa-fetch ./request.yaml
  aa-request --vessel Alaska_Knight --survey CHS12AK --instrument ES60 \
             --from 2012-08-13 --to 2012-08-14 | aa-fetch -

  # Full chain — build a YAML, fetch its files, convert each to .nc,
  # combine into one transect, compute Sv, plot:
  aa-get | aa-fetch | aa-ed | aa-combine | aa-sv | aa-graph

TROUBLESHOOTING
  • "File does not exist" — check the YAML path or your pipeline output.
  • Auth / BigQuery errors — ensure your GCP credentials are configured.
"""
    print(help_text.strip() + "\n")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="aa-fetch",
        description="Execute an aa-fetch YAML job.",
        add_help=False,
    )
    parser.add_argument(
        "yaml_path",
        type=str,
        nargs="?",
        help="Path to YAML file, or '-' for the document on stdin. Optional — falls back to stdin.",
    )
    parser.add_argument(
        "-o", "--output_root",
        type=Path,
        default=None,
        help="Parent directory where the download directory will be created (default: CWD).",
    )
    parser.add_argument(
        "-n", "--download_dir_name",
        type=str,
        default=None,
        help="Download directory name under output_root (default: aa_fetch_<timestamp>).",
    )
    return parser


def _default_download_dir_name() -> str:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"aa_fetch_{stamp}"


# ---------------------------------------------------------------------------
# The request: a path, or the document itself
# ---------------------------------------------------------------------------

_DOCUMENT_START = re.compile(r"^requests\s*:")


def _skippable(line: str) -> bool:
    """Blank lines, comments and YAML directives/markers before the content."""
    s = line.strip()
    return not s or s.startswith("#") or s.startswith("%") or s == "---"


def _read_stdin_request() -> tuple[str | None, str | None]:
    """('document', text), ('path', token) or (None, None) from piped stdin.

    A path is one line (the rest of stdin is not read, as before). A
    document is recognised by its first content line, 'requests:'.
    """
    consumed: list[str] = []
    for line in sys.stdin:
        consumed.append(line)
        if _skippable(line):
            continue
        if _DOCUMENT_START.match(line.strip()):
            return "document", "".join(consumed) + sys.stdin.read()
        return "path", stdio.normalize_token(line)
    return None, None


def _check_document(text: str) -> str | None:
    """None if ``text`` is a request document, else what is wrong with it."""
    import yaml

    try:
        doc = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        return f"not valid YAML: {exc}"
    if not isinstance(doc, dict) or "requests" not in doc:
        return "not a request document (no top-level 'requests:')"
    if not isinstance(doc["requests"], list):
        return "'requests' must be a list"
    return None


# ---------------------------------------------------------------------------
# Where each downloaded file came from
# ---------------------------------------------------------------------------

def _s3_uri(key: str) -> str | None:
    """s3://<bucket>/<key> for a bare NCEI key, an s3:// URI or an S3 object URL.

    None for anything else: an origin we cannot state exactly is not recorded.
    """
    text = str(key or "").strip()
    if not text:
        return None
    if text.startswith("s3://"):
        return text
    m = re.match(r"^https?://([a-z0-9.-]+?)\.s3[a-z0-9.-]*\.amazonaws\.com/(.+)$", text)
    if m:  # virtual-hosted style: https://noaa-wcsd-pds.s3.amazonaws.com/<key>
        return f"s3://{m.group(1)}/{m.group(2)}"
    m = re.match(r"^https?://s3[a-z0-9.-]*\.amazonaws\.com/([^/]+)/(.+)$", text)
    if m:  # path style: https://s3.amazonaws.com/noaa-wcsd-pds/<key>
        return f"s3://{m.group(1)}/{m.group(2)}"
    if "://" in text:
        return None
    return f"s3://{NCEI_BUCKET}/{text.lstrip('/')}"


def _ncei_parts(uri: str | None) -> dict:
    """ship/survey/sonar_model as NCEI spells them, from data/raw/<ship>/<survey>/<sonar>/<file>."""
    if not uri or not uri.startswith("s3://"):
        return {}
    parts = uri[len("s3://"):].split("/")[1:]
    if len(parts) == 6 and parts[0] == "data" and parts[1] == "raw":
        return {"ship": parts[2], "survey": parts[3], "sonar_model": parts[4]}
    return {}


def _file_name(key: str) -> str:
    # The name the library downloads a key to (helpers.get_file_name_from_url).
    return str(key).split("/")[-1]


def _signature(path: Path):
    try:
        st = path.stat()
    except OSError:
        return None
    return (st.st_ino, st.st_size, st.st_mtime_ns)


def _describe(by_name: dict, before: dict, download_dir: Path, request: Path) -> tuple[int, int]:
    """Write <file>.aa.json for every file that arrived in this run.

    Returns (files described, of which with an NCEI origin).

    A file counts only if it changed on disk during the download: the
    library skips objects missing from NCEI without raising, and a file
    left over from an earlier run is not evidence of this one.
    """
    described = with_origin = 0
    missing: list[str] = []
    for name, keys in sorted(by_name.items()):
        path = download_dir / name
        sig = _signature(path)
        if sig is None or sig == before.get(name):
            missing.extend(keys)
            continue
        extra = {"data_source": "NCEI", "request": str(request)}
        if len(keys) == 1:
            origin = _s3_uri(keys[0])
            extra.update(_ncei_parts(origin))
            if origin is None:
                logger.warning(f"{name}: cannot state its NCEI object from {keys[0]!r}; "
                               "sidecar written without an origin")
        else:
            # Same file name under different keys: the downloads overwrote each
            # other in this flat directory, and which one survived cannot be
            # told from here. Record no origin rather than a possibly wrong one.
            origin = None
            extra["candidates"] = [_s3_uri(k) or str(k) for k in keys]
            logger.warning(
                f"{len(keys)} matches share the file name {name}; they were "
                "downloaded to the same path, so only one of them is on disk "
                "and its origin is not recorded:\n  " + "\n  ".join(keys))
        try:
            record_source(path, tool=SPEC.name, origin=origin, extra=extra)
            described += 1
            with_origin += origin is not None
        except OSError as exc:
            logger.warning(f"Could not write {name}.aa.json: {exc}")
    if missing:
        logger.warning(
            f"{len(missing)} match(es) did not arrive (NCEI did not return them):\n  "
            + "\n  ".join(sorted(missing)))
    return described, with_origin


def main() -> int:
    # No request on a terminal: help. (An empty pipe is an error, below.)
    if len(sys.argv) == 1 and not stdio.stdin_is_piped():
        print_help()
        return 0

    parser = _build_parser()
    if show_help(SPEC, HELP, parser, full=print_help_full):
        return 0

    args = parser.parse_args()

    # ---------------------------
    # Resolve the request: positional path > '-' / stdin document > stdin path
    # ---------------------------
    document: str | None = None
    yaml_path: Path | None = None
    if args.yaml_path == "-":
        if not stdio.stdin_is_piped():
            logger.error("'-' reads the request document from stdin, but stdin is a terminal.")
            return 2
        document = sys.stdin.read()
        if not document.strip():
            logger.error("Received nothing on stdin; the previous stage in the pipe "
                         "probably failed (see its error above).")
            return 2
    elif args.yaml_path is not None:
        yaml_path = Path(args.yaml_path)
    else:
        if not stdio.stdin_is_piped():
            logger.error("No request given: pass a YAML path, or pipe a path or the "
                         "YAML document in (see aa-fetch --help).")
            return 2
        kind, value = _read_stdin_request()
        if kind is None:
            logger.error("Received nothing on stdin; the previous stage in the pipe "
                         "probably failed (see its error above).")
            return 2
        if kind == "document":
            document = value
            logger.info("Read the request document from stdin")
        elif not value:
            logger.error("stdin holds neither a YAML path nor a request document "
                         "(see aa-fetch --help).")
            return 2
        else:
            yaml_path = Path(value)
            logger.info(f"Read YAML path from stdin: {yaml_path}")

    if document is not None:
        problem = _check_document(document)
        if problem:
            logger.error(f"stdin: {problem}")
            return 1
    else:
        if not yaml_path.exists():
            logger.error(f"YAML file does not exist: {yaml_path}")
            return 1
        if not yaml_path.is_file():
            logger.error(f"YAML path is not a file: {yaml_path}")
            return 1

    # ---------------------------
    # Resolve & create download directory
    # ---------------------------
    output_root = (args.output_root or Path.cwd()).expanduser().resolve()
    download_dir_name = (args.download_dir_name or _default_download_dir_name()).strip()

    if not download_dir_name:
        logger.error("--download_dir_name cannot be empty.")
        return 2

    try:
        output_root.mkdir(parents=True, exist_ok=True)
        download_dir = (output_root / download_dir_name).resolve()
        download_dir.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        logger.exception(f"Failed to create download directory under '{output_root}': {e}")
        return 2

    logger.info(f"Download directory: {download_dir}")

    if document is not None:
        yaml_path = download_dir / REQUEST_FILE_NAME
        try:
            yaml_path.write_text(document, encoding="utf-8")
        except OSError as e:
            logger.error(f"Could not save the request document to {yaml_path}: {e}")
            return 1
        logger.info(f"Request saved as {yaml_path}")
    yaml_path = yaml_path.resolve()

    # ---------------------------
    # Heavy import (deferred so --help / arg validation is fast).
    # Everything aalibrary prints goes to stderr: stdout carries only the
    # directory, or the next stage would read library messages as paths.
    # ---------------------------
    try:
        with contextlib.redirect_stdout(sys.stderr):
            import aalibrary.utils.multi_fetch_yaml_parser as mf
    except Exception as e:
        logger.exception(f"Failed to import multi_fetch_yaml_parser: {e}")
        return 1

    # ---------------------------
    # Execute fetch
    # ---------------------------
    try:
        with contextlib.redirect_stdout(sys.stderr):
            # YAMLParser always populates sql_query, so the previous hasattr()
            # guard was dead code — log it directly.
            yaml_test = mf.YAMLParser(yaml_file_path=str(yaml_path))
            logger.info(f"SQL query built from YAML:\n{yaml_test.sql_query}")

            results = mf.parse_yaml_and_fetch_results(yaml_file_path=str(yaml_path))

        try:
            n = len(results)
        except TypeError:
            n = "?"
        logger.info(f"Result count: {n}")

        if not results:
            logger.warning("No files matched the YAML criteria — nothing to download.")
            # Still emit the (empty) download directory so the next
            # pipeline stage sees a concrete path rather than empty
            # stdin. aa-ed in directory mode will surface the empty
            # directory as its own clear error — better than aa-ed
            # printing its help screen because it got nothing.
            stdio.emit(download_dir)
            return 0

        # The library downloads every key into this one directory under its
        # file name. Note each file's state first, so that afterwards only
        # files this run actually wrote are described.
        by_name: dict[str, list[str]] = {}
        for key in results:
            by_name.setdefault(_file_name(key), []).append(str(key))
        before = {name: _signature(download_dir / name) for name in by_name}

        # IMPORTANT: do NOT wrap download_results in a "could not log cleanly"
        # except. The previous version did, and it silently swallowed real
        # download failures behind a benign-sounding message. Let download
        # errors surface as their own logged exception.
        failure = None
        try:
            with contextlib.redirect_stdout(sys.stderr):
                mf.download_results(results, str(download_dir))
        except Exception as e:
            failure = e

        # Describe what arrived, also after a failure part-way through.
        described, with_origin = _describe(by_name, before, download_dir, yaml_path)
        logger.info(f"Wrote .aa.json for {described} downloaded file(s), "
                    f"{with_origin} with their NCEI origin")

        if failure is not None:
            logger.opt(exception=failure).error(f"Download failed: {failure}")
            return 1

        logger.success(f"aa-fetch complete. Files in: {download_dir}")
        # Pipeline contract: absolute path of the download directory on
        # stdout. Lets `aa-fetch | aa-ed` (in directory mode) compose
        # cleanly. Flushed so the path arrives at the downstream tool
        # before this process exits.
        stdio.emit(download_dir)
        return 0

    except Exception as e:
        logger.exception(f"aa-fetch failed: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
