#!/usr/bin/env python3
"""
aa-raw

Download a raw echosounder file from NCEI. Pipeline-friendly: prints the
absolute path of the downloaded file to stdout so the rest of the
aa-suite can pick it up.

Pipeline contract (mirrors the rest of the aa-suite):
    input  : flags only — no stdin (this tool is a SOURCE, not a consumer)
    output : .raw file on disk; absolute path printed to stdout (and
             nothing else: library messages are sent to stderr)
    logs   : stderr via loguru
    record : <file>.aa.json beside each downloaded file, with the NCEI
             object it came from (s3://noaa-wcsd-pds/data/raw/...) and its
             MD5. aa-nc records that origin as its input's source.

Typical pipeline usage:
    aa-raw --file_name D20190804-T113723.raw \\
           --ship_name Henry_B._Bigelow \\
           --survey_name HB1907 \\
           --sonar_model EK60 \\
           --file_download_directory ./downloads \\
        | aa-nc --sonar_model EK60 \\
        | aa-sv \\
        | aa-clean \\
        | aa-graph
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

import argparse
import contextlib
import pprint
import re
from pathlib import Path

from aalibrary.console._core import (
    Help, ToolSpec, record_source, render, show_help, stdio,
)

NCEI_BUCKET = "noaa-wcsd-pds"

SPEC = ToolSpec(name="aa-raw", role="source", kind="raw", engines=())

HELP = Help(
    summary="Download one raw echosounder file (.raw, with its .idx/.bot) from NCEI.",
    does=(
        "Finds the file in NCEI's Water Column Sonar Data archive (public S3 "
        "bucket noaa-wcsd-pds, key data/raw/<ship>/<survey>/<sonar>/<file>) and "
        "downloads it into --file_download_directory, together with its .idx "
        "and .bot companions when NCEI has them. The ship name is matched to "
        "NCEI's folder spelling (close matches are accepted). A local file of "
        "the same name is replaced. --upload_to_gcp also copies the files to "
        "the aalibrary GCS bucket."
    ),
    stdin="Nothing. aa-raw is a source: the file is named by the flags, stdin is not read.",
    stdout=(
        "The absolute path of the .raw, one line, and nothing else (aalibrary's "
        "own messages go to stderr, also with --upload_to_gcp). Empty on failure "
        "(exit 1)."
    ),
    metadata=(
        "Writes <file>.aa.json beside the .raw (and beside the .idx/.bot): the "
        "NCEI object it was downloaded from (s3://noaa-wcsd-pds/data/raw/...), "
        "its MD5 and size, and ship/survey/sonar as NCEI spells them. aa-nc "
        "records that origin as the source of the .nc it writes (aa-metadata "
        "FILE.nc shows it; products made from the .nc point back to the .nc). "
        "The sidecar never changes a hash: downstream tools identify the .raw "
        "by its content either way. The .raw itself is not modified."
    ),
    options=[
        ("--file_name NAME", "REQUIRED. File name with extension, e.g. D20190804-T113723.raw"),
        ("--ship_name NAME", "REQUIRED. e.g. Henry_B._Bigelow (spelling is matched to NCEI's)"),
        ("--survey_name NAME", "REQUIRED. e.g. HB1907"),
        ("--sonar_model NAME", "REQUIRED. NCEI's sonar folder, e.g. EK60, EK80"),
        ("--file_download_directory DIR", "where to download (default: current directory; created)"),
        ("--upload_to_gcp", "also upload the files to the aalibrary GCS bucket"),
        ("--quiet / --debug", "fewer / more log messages on stderr"),
    ],
    files=(
        "Reads NCEI's public bucket anonymously. aalibrary also looks up the "
        "file's copy in its GCS bucket (ggn-nmfs-aa-prod-1-data: aalibrary "
        "selects the production project when imported), so Google Cloud "
        "credentials are needed even without --upload_to_gcp. "
        "Writes DIR/<file>.raw, DIR/<stem>.idx, DIR/<stem>.bot and a .aa.json "
        "for each."
    ),
    pipeline=(
        "First stage of a chain: aa-raw ... | aa-nc --sonar_model EK60 | aa-sv | "
        "... It takes no stdin, so nothing can be piped into it; to fetch many "
        "files use aa-request ... | aa-fetch -."
    ),
    examples=[
        "aa-raw --file_name D20190804-T113723.raw --ship_name Henry_B._Bigelow \\",
        "         --survey_name HB1907 --sonar_model EK60 --file_download_directory ./downloads",
        "aa-raw ... | aa-nc --sonar_model EK60 | aa-sv | aa-graph",
        "aa-metadata ./downloads/D20190804-T113723.raw     # origin and MD5 from the sidecar",
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
    """Curated help (also used by the docs generator)."""
    sys.stdout.write(render(SPEC, HELP, _build_parser()))


def print_help_full() -> None:
    help_text = """
    Usage: aa-raw [OPTIONS]

    Required:
      --file_name NAME            Name of the file to download
                                  (e.g. D20190804-T113723.raw).
      --ship_name NAME            Name of the ship (e.g. Henry_B._Bigelow).
                                  Matched to NCEI's folder spelling.
      --survey_name NAME          Name of the survey (e.g. HB1907).
      --sonar_model NAME          Type of echosounder (e.g. EK60, EK80).

    Optional:
      --file_type TYPE            File type passed to aalibrary (default: raw).
                                  aa-raw always fetches the named file and its
                                  .idx/.bot companions.
      --data_source SRC           Data source identifier (default: NCEI).
                                  Currently only 'NCEI' is wired through; other
                                  values log a warning and proceed as NCEI.
      --file_download_directory PATH
                                  Where to download. Default: current directory.
                                  Created if it doesn't exist.
      --upload_to_gcp             Also upload the downloaded file to GCP.
      --debug                     Verbose logging (DEBUG level on stderr).
      --quiet                     Suppress INFO logs; final path still prints.
      -h, --help                  Short help.
      --help-all                  This reference.

    Description:
      Downloads a raw echosounder file (and its .idx/.bot, when NCEI has
      them) from NCEI given (ship, survey, sonar_model, file_name). The
      absolute path of the downloaded .raw is printed on stdout, and
      nothing else, ready for piping into aa-nc and onward.

      Beside each downloaded file aa-raw writes <file>.aa.json with the
      NCEI object it came from (s3://noaa-wcsd-pds/data/raw/...) and its
      MD5. aa-nc records that origin; see aa-metadata.

    Pipeline example:
      aa-raw --file_name D20190804-T113723.raw \\
             --ship_name Henry_B._Bigelow --survey_name HB1907 \\
             --sonar_model EK60 --file_download_directory ./downloads \\
        | aa-nc --sonar_model EK60 | aa-sv | aa-clean

    Direct example:
      aa-raw --file_name D20190804-T113723.raw \\
             --ship_name Henry_B._Bigelow --survey_name HB1907 \\
             --sonar_model EK60 \\
             --file_download_directory Henry_B._Bigelow_HB1907_EK60_NCEI
    """
    print(help_text)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="aa-raw",
        description="Download a raw echosounder file from NCEI.",
        add_help=False,
    )

    parser.add_argument("--file_name", required=True,
                        help="Name of the file to download.")
    parser.add_argument("--file_type", default="raw",
                        help="Type of the file (default: raw).")
    parser.add_argument("--ship_name", required=True,
                        help="Name of the ship.")
    parser.add_argument("--survey_name", required=True,
                        help="Name of the survey.")
    parser.add_argument("--sonar_model", required=True,
                        help="Type of echosounder (e.g. EK60).")
    # The previous version documented --data_source in print_help() but
    # never added it to argparse, so passing the documented flag errored
    # with "unrecognized arguments". Wired up now; logs a warning if
    # the user requests anything other than the only supported source.
    parser.add_argument("--data_source", default="NCEI",
                        help="Data source (default: NCEI).")
    parser.add_argument("--file_download_directory", default=".",
                        help="Directory to download into (default: CWD).")
    parser.add_argument("--upload_to_gcp", action="store_true",
                        help="Also upload the downloaded file to GCP.")
    parser.add_argument("--debug", action="store_true",
                        help="Enable verbose DEBUG-level logging.")
    parser.add_argument("--quiet", action="store_true",
                        help="Suppress INFO logs.")
    return parser


# ---------------------------------------------------------------------------
# Where a downloaded file came from
# ---------------------------------------------------------------------------

def _s3_uri(url: str) -> str | None:
    """s3://<bucket>/<key> for an S3 object URL, an s3:// URI or a bare NCEI key.

    None for anything else: an origin we cannot state exactly is not recorded.
    """
    text = str(url or "").strip()
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


def _signature(path: Path):
    try:
        st = path.stat()
    except OSError:
        return None
    return (st.st_ino, st.st_size, st.st_mtime_ns)


@contextlib.contextmanager
def _record_downloads(module, fetched: dict):
    """Record what the library downloads, as {local file: object URL}.

    download_raw_file_from_ncei builds the NCEI object key itself (after
    correcting the ship name to NCEI's folder spelling) and returns nothing.
    Wrapping its downloader for the duration of the call is how aa-raw
    learns the key that was really used, so the recorded origin is the
    object that was downloaded, not a reconstruction of it. The downloader
    itself runs unchanged. A file counts as downloaded only if it changed on
    disk during the call: the library skips objects that are missing from
    NCEI without raising.
    """
    original = module.download_single_file_from_aws

    def recording(*args, **kwargs):
        url = kwargs.get("file_url", args[0] if args else "")
        location = kwargs.get("download_location", args[1] if len(args) > 1 else "")
        target = Path(str(location) or ".")
        if target.is_dir():
            target = target / str(url).rstrip("/").rsplit("/", 1)[-1]
        before = _signature(target)
        result = original(*args, **kwargs)
        after = _signature(target)
        if after is not None and after != before:
            fetched[target.resolve()] = str(url)
        return result

    module.download_single_file_from_aws = recording
    try:
        yield fetched
    finally:
        module.download_single_file_from_aws = original


def _describe(fetched: dict, args) -> dict:
    """Write <file>.aa.json for every file downloaded in this run."""
    described = {}
    for path, url in fetched.items():
        origin = _s3_uri(url)
        extra = {"ship": args.ship_name, "survey": args.survey_name,
                 "sonar_model": args.sonar_model, "data_source": "NCEI"}
        extra.update(_ncei_parts(origin))
        if origin is None:
            logger.warning(f"{path.name}: cannot state its NCEI object from {url!r}; "
                           "sidecar written without an origin")
        try:
            record_source(path, tool=SPEC.name, origin=origin, extra=extra)
        except OSError as exc:
            logger.warning(f"Could not write {path.name}.aa.json: {exc}")
            continue
        described[path] = origin
        logger.debug(f"Recorded {path.name} <- {origin}")
    return described


def main() -> None:
    # No-args / explicit help short-circuit
    if len(sys.argv) == 1:
        print_help()
        sys.exit(0)

    parser = _build_parser()
    if show_help(SPEC, HELP, parser, full=print_help_full):
        sys.exit(0)

    args = parser.parse_args()

    if args.debug and args.quiet:
        logger.error("Use --debug OR --quiet, not both.")
        sys.exit(2)

    _configure_logging(args.quiet, args.debug)

    if args.data_source.upper() != "NCEI":
        logger.warning(
            f"--data_source='{args.data_source}' requested, but aa-raw currently "
            "only downloads from NCEI. Proceeding as NCEI."
        )

    # Resolve and create the destination directory. The previous version
    # delegated this to download_raw_file_from_ncei; if that function
    # didn't create the directory, the download silently failed and we
    # still printed a non-existent path to stdout, breaking the next
    # pipeline stage with a confusing FileNotFoundError.
    download_dir = Path(args.file_download_directory).expanduser().resolve()
    try:
        download_dir.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        logger.error(f"Could not create download directory '{download_dir}': {e}")
        sys.exit(2)

    # Heavy import deferred so --help is fast and a typo on a required
    # arg fails fast (in argparse) before paying the import cost.
    # Everything aalibrary prints goes to stderr: stdout carries only the
    # path, or the next stage would read library messages as file names.
    try:
        with contextlib.redirect_stdout(sys.stderr):
            import aalibrary.ingestion as ingestion
    except Exception as e:
        logger.exception(f"Failed to import aalibrary.ingestion: {e}")
        sys.exit(1)

    args_summary = {
        "file_name": args.file_name,
        "file_type": args.file_type,
        "ship_name": args.ship_name,
        "survey_name": args.survey_name,
        "sonar_model": args.sonar_model,
        "data_source": args.data_source,
        "file_download_directory": str(download_dir),
        "upload_to_gcp": args.upload_to_gcp,
        "debug": args.debug,
    }
    logger.debug(
        f"Executing aa-raw configured with [OPTIONS]:\n"
        f"{pprint.pformat(args_summary)}"
    )

    fetched: dict = {}
    failure = None
    try:
        logger.info(
            f"Downloading {args.file_name} "
            f"({args.ship_name} / {args.survey_name} / {args.sonar_model}) "
            f"from NCEI -> {download_dir}"
        )
        with contextlib.redirect_stdout(sys.stderr), _record_downloads(ingestion, fetched):
            ingestion.download_raw_file_from_ncei(
                file_name=args.file_name,
                file_type=args.file_type,
                ship_name=args.ship_name,
                survey_name=args.survey_name,
                echosounder=args.sonar_model,
                file_download_directory=str(download_dir),
                upload_to_gcp=args.upload_to_gcp,
                debug=args.debug,
            )
    except Exception as e:
        failure = e

    # Describe every file that arrived, also when a later step (the GCP
    # upload) failed: the downloads themselves are real.
    described = _describe(fetched, args)

    if failure is not None:
        logger.opt(exception=failure).error(f"Download failed: {failure}")
        sys.exit(1)

    downloaded = download_dir / args.file_name

    # Sanity check: the underlying function returns nothing useful, so
    # we only know the download succeeded by checking the file is on
    # disk. Without this, a silent failure would print a non-existent
    # path to stdout and break the next pipeline stage downstream.
    if not downloaded.exists():
        logger.error(
            f"Download appeared to succeed, but '{downloaded}' is not on disk. "
            "Check the ship/survey/sonar/file names (NCEI may not have this "
            "file); rerun with --debug for details."
        )
        sys.exit(1)

    if downloaded.resolve() not in described:
        logger.warning(
            f"{downloaded.name} was not downloaded in this run (NCEI did not "
            "return it); passing on the file already in "
            f"{download_dir}. Its origin was not recorded by this run."
        )
    else:
        logger.info(f"Origin: {described[downloaded.resolve()]}")

    logger.success(
        f"Downloaded {downloaded.name} via aa-raw. "
        "Passing .raw path to stdout..."
    )
    # Pipeline contract: print the absolute path on stdout.
    stdio.emit(downloaded.resolve())


if __name__ == "__main__":
    main()
