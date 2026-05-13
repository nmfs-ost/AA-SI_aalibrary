#!/usr/bin/env python3
"""
aa-ed   (echodata)

Console tool that collapses aa-raw + aa-nc into a single step.

Given only a raw file name, aa-ed looks up the ship, survey, and
echosounder model from the NCEI cache (BigQuery, via
``aalibrary.utils.ncei_cache_utils.get_metadata_from_search_param``),
downloads the .raw from NCEI, converts it to a multi-group NetCDF
EchoData file with echopype, and prints the absolute path of the .nc
file to stdout for the next pipeline stage.

aa-ed is a convenience upgrade over ``aa-raw | aa-nc`` — same output,
fewer keystrokes when the file's metadata can be inferred from the
NCEI cache. It does NOT replace aa-raw or aa-nc; use those when the
file is not in NCEI, or when you need finer control over the download
and conversion stages.

Pipeline contract (mirrors the rest of the aa-suite):
    input  : a raw file name as positional arg or via stdin
    output : .nc file on disk; absolute path printed to stdout
    logs   : stderr via loguru

Typical pipeline usage:
    echo HB1603_L1-D20160703-T183957.raw | aa-ed | aa-sv | aa-graph
    aa-ed HB1603_L1-D20160703-T183957.raw | aa-sv | aa-clean
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
import pprint
import signal
from pathlib import Path
from typing import Optional


# Pipeline tools should die cleanly when the downstream end of the pipe
# closes early (`... | head -n 1`), not throw BrokenPipeError. Guarded
# with hasattr because SIGPIPE doesn't exist on Windows.
if hasattr(signal, "SIGPIPE"):
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)


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
    Usage: aa-ed [OPTIONS] [FILE_NAME]

    Arguments:
      FILE_NAME                   Name of the raw file to fetch and convert
                                  (e.g. HB1603_L1-D20160703-T183957.raw).
                                  Must include the .raw extension. May be
                                  a bare name or a full path — the directory
                                  portion is ignored, only the basename is
                                  used for the NCEI cache lookup. Optional;
                                  falls back to stdin if not provided.

    Optional:
      -o, --output_path PATH      Path to save the converted NetCDF output.
                                  Default: same directory as the downloaded
                                  .raw, with a .nc suffix.

      --file_download_directory PATH
                                  Where to download the .raw to.
                                  Default: current directory. Created if it
                                  doesn't exist.

      --ship_name NAME            Override the ship_name lookup
                                  (e.g. Henry_B._Bigelow).
      --survey_name NAME          Override the survey_name lookup
                                  (e.g. HB1603).
      --sonar_model NAME          Override the echosounder lookup
                                  (e.g. EK60, EK80).

                                  If all three overrides are provided, aa-ed
                                  skips the NCEI cache lookup entirely. Use
                                  this when BigQuery is unreachable or to
                                  disambiguate a file name that collides
                                  across multiple surveys.

      --cleanup-raw               Delete the downloaded .raw after the .nc
                                  is produced. Off by default — the .raw is
                                  source data and is kept so re-running
                                  aa-ed (or aa-nc directly) is free.

      --upload_to_gcp             Also upload the downloaded .raw to GCP
                                  (passed through to aalibrary.ingestion).

      --data_source SRC           Currently only 'NCEI' is wired through;
                                  other values log a warning and proceed
                                  as NCEI.

      --debug                     Verbose logging (DEBUG level on stderr).
      --quiet                     Suppress INFO logs; final path still
                                  prints on stdout.

      -h, --help                  Show this help and exit.

    Description:
      Resolves a raw file's ship/survey/echosounder by querying the NCEI
      BigQuery cache, downloads the .raw from NCEI, and converts it to a
      multi-group NetCDF EchoData file with echopype.open_raw /
      EchoData.to_netcdf. The .nc absolute path is printed on stdout,
      ready for piping into aa-sv and onward.

      Equivalent (in output) to:

          aa-raw --file_name FILE --ship_name S --survey_name SU \\
                 --sonar_model M --file_download_directory DIR \\
            | aa-nc --sonar_model M

      ...but the user only has to supply FILE_NAME.

    Pipeline example:
      echo HB1603_L1-D20160703-T183957.raw | aa-ed | aa-sv | aa-graph

    Direct example:
      aa-ed HB1603_L1-D20160703-T183957.raw \\
            --file_download_directory ./downloads -o ./out/HB1603.nc
    """
    print(help_text)


def main() -> None:
    # Stdin / no-args handling — same shape as aa-sonar so an empty-stdin
    # invocation prints help instead of hanging on readline.
    if len(sys.argv) == 1:
        if not sys.stdin.isatty():
            stdin_data = sys.stdin.readline().strip()
            if stdin_data:
                sys.argv.append(stdin_data)
            else:
                print_help()
                sys.exit(0)
        else:
            print_help()
            sys.exit(0)

    if "--help" in sys.argv or "-h" in sys.argv:
        print_help()
        sys.exit(0)

    parser = argparse.ArgumentParser(
        description="Resolve, download, and convert a raw NCEI file to NetCDF.",
        add_help=False,
    )

    parser.add_argument(
        "file_name",
        type=str,
        nargs="?",
        help="Name of the raw file (with .raw extension).",
    )
    parser.add_argument(
        "-o", "--output_path",
        type=Path,
        default=None,
        help="Path to save the .nc output. Default: alongside the .raw.",
    )
    parser.add_argument(
        "--file_download_directory",
        default=".",
        help="Directory to download the .raw into (default: CWD).",
    )
    parser.add_argument(
        "--ship_name",
        default=None,
        help="Override the ship_name lookup (e.g. Henry_B._Bigelow).",
    )
    parser.add_argument(
        "--survey_name",
        default=None,
        help="Override the survey_name lookup (e.g. HB1603).",
    )
    parser.add_argument(
        "--sonar_model",
        default=None,
        help="Override the echosounder lookup (e.g. EK60, EK80).",
    )
    parser.add_argument(
        "--cleanup-raw", "--cleanup_raw",
        dest="cleanup_raw",
        action="store_true",
        default=False,
        help="Delete the downloaded .raw after producing the .nc.",
    )
    parser.add_argument(
        "--upload_to_gcp",
        action="store_true",
        default=False,
        help="Also upload the downloaded .raw to GCP.",
    )
    parser.add_argument(
        "--data_source",
        default="NCEI",
        help="Data source (default: NCEI; others currently ignored).",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        default=False,
        help="Enable verbose DEBUG-level logging.",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        default=False,
        help="Suppress INFO logs.",
    )

    args = parser.parse_args()

    if args.debug and args.quiet:
        logger.error("Use --debug OR --quiet, not both.")
        sys.exit(2)

    _configure_logging(args.quiet, args.debug)

    # ---------------------------
    # Validate input
    # ---------------------------
    if args.file_name is None:
        if sys.stdin.isatty():
            logger.error("No file name provided and no stdin available.")
            sys.exit(1)
        args.file_name = sys.stdin.readline().strip()
        logger.info(f"Read file name from stdin: {args.file_name}")

    if not args.file_name:
        logger.error("Empty file name.")
        sys.exit(1)

    # Accept either a bare file name or a full path. We only ever need
    # the file name itself: the NCEI cache matches on the file_name
    # column, and the downloaded copy lands under
    # --file_download_directory regardless of where the input pointed.
    # So if the user piped a path (from aa-raw, from a notebook variable
    # holding an absolute path, from `find ... | aa-ed`, etc.) discard
    # the directory portion and keep only the basename.
    _raw_input = args.file_name
    args.file_name = Path(args.file_name).name
    if args.file_name != _raw_input:
        logger.debug(
            f"Stripped directory from input '{_raw_input}'; "
            f"using basename '{args.file_name}'."
        )

    # We deliberately keep this strict: aa-ed is a .raw → .nc tool. Other
    # extensions belong on aa-nc (for already-downloaded files) or aa-sonar
    # (for inspection). Accepting bare stems would also degrade the
    # BigQuery filter from exact-match to LIKE, re-introducing the
    # ambiguity this tool was designed to avoid.
    if not args.file_name.lower().endswith(".raw"):
        logger.error(
            f"'{args.file_name}' is not a .raw file name. aa-ed only operates "
            "on .raw files; include the .raw extension."
        )
        sys.exit(1)

    if args.data_source.upper() != "NCEI":
        logger.warning(
            f"--data_source='{args.data_source}' requested, but aa-ed currently "
            "only resolves and downloads from NCEI. Proceeding as NCEI."
        )

    download_dir = Path(args.file_download_directory).expanduser().resolve()
    try:
        download_dir.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        logger.error(f"Could not create download directory '{download_dir}': {e}")
        sys.exit(2)

    args_summary = {
        "file_name": args.file_name,
        "output_path": args.output_path,
        "file_download_directory": str(download_dir),
        "ship_name": args.ship_name,
        "survey_name": args.survey_name,
        "sonar_model": args.sonar_model,
        "cleanup_raw": args.cleanup_raw,
        "upload_to_gcp": args.upload_to_gcp,
        "data_source": args.data_source,
        "debug": args.debug,
    }
    logger.debug(
        f"Executing aa-ed configured with [OPTIONS]:\n"
        f"{pprint.pformat(args_summary)}"
    )

    # ---------------------------
    # Resolve metadata
    # ---------------------------
    try:
        metadata = resolve_metadata(
            file_name=args.file_name,
            ship_name=args.ship_name,
            survey_name=args.survey_name,
            sonar_model=args.sonar_model,
        )
    except SystemExit:
        # resolve_metadata calls sys.exit with a useful message already.
        raise
    except Exception as e:
        logger.exception(
            f"Could not resolve metadata for '{args.file_name}': {e}\n"
            "If BigQuery is unreachable or you lack credentials, supply "
            "--ship_name, --survey_name, and --sonar_model to skip the lookup."
        )
        sys.exit(1)

    logger.success(
        f"Resolved metadata for {args.file_name}: "
        f"ship='{metadata['ship_name']}', "
        f"survey='{metadata['survey_name']}', "
        f"sonar='{metadata['sonar_model']}'."
    )

    # ---------------------------
    # Resolve output path
    # ---------------------------
    raw_path = download_dir / args.file_name
    if args.output_path is None:
        nc_path = raw_path.with_suffix(".nc")
    else:
        nc_path = args.output_path.expanduser().resolve().with_suffix(".nc")
        nc_path.parent.mkdir(parents=True, exist_ok=True)

    # Same guard aa-nc has — cheap insurance against -o pointing at the .raw.
    if nc_path.resolve() == raw_path.resolve():
        logger.error(f"Refusing to overwrite input file: {raw_path.resolve()}")
        sys.exit(1)

    # ---------------------------
    # Download + convert
    # ---------------------------
    try:
        process_file(
            file_name=args.file_name,
            metadata=metadata,
            raw_path=raw_path,
            nc_path=nc_path,
            download_dir=download_dir,
            upload_to_gcp=args.upload_to_gcp,
            debug=args.debug,
        )
    except SystemExit:
        raise
    except Exception as e:
        logger.exception(f"Error during processing: {e}")
        sys.exit(1)

    # Optional cleanup of the intermediate .raw. Done after the .nc is
    # confirmed on disk inside process_file, so a conversion failure
    # never costs the user their downloaded raw data.
    if args.cleanup_raw:
        try:
            raw_path.unlink(missing_ok=True)
            logger.info(f"Removed intermediate .raw: {raw_path}")
        except Exception as e:
            # Non-fatal — the .nc still exists and gets printed.
            logger.warning(f"Could not delete '{raw_path}': {e}")

    logger.success(
        f"Generated {nc_path.resolve()} with aa-ed. "
        "Passing .nc path to stdout..."
    )
    # Pipeline contract: print the absolute .nc path on stdout.
    print(nc_path.resolve())


def resolve_metadata(
    file_name: str,
    ship_name: Optional[str] = None,
    survey_name: Optional[str] = None,
    sonar_model: Optional[str] = None,
) -> dict:
    """Resolve (ship_name, survey_name, sonar_model) for a given raw file.

    Strategy:
      1. If all three overrides are provided, return them directly and
         skip the BigQuery lookup entirely. Useful when offline / no
         GCP creds, or when the user knows better than the cache.
      2. Otherwise query the NCEI BigQuery cache via
         get_metadata_from_search_param, filter to the exact file_name +
         file_type='raw', then apply any provided overrides as
         additional filters to disambiguate.
      3. Bail out (sys.exit) with a useful message on 0 or >1 matches.

    Returns a dict with keys 'ship_name', 'survey_name', 'sonar_model'.
    The 'ship_name' value is the normalized form (underscores, etc.) as
    accepted by aalibrary.ingestion.download_raw_file_from_ncei.
    """
    # Fast-path: skip the lookup entirely if the user has told us
    # everything we'd otherwise look up.
    if ship_name and survey_name and sonar_model:
        logger.info(
            "All three overrides supplied; skipping NCEI cache lookup."
        )
        return {
            "ship_name": ship_name,
            "survey_name": survey_name,
            "sonar_model": sonar_model,
        }

    # Heavy import deferred until we actually need BigQuery, so --help
    # and the fast-path above stay snappy.
    try:
        from aalibrary.utils.ncei_cache_utils import (
            get_metadata_from_search_param,
        )
    except Exception as e:
        logger.error(
            f"Failed to import aalibrary.utils.ncei_cache_utils: {e}\n"
            "Pass --ship_name, --survey_name, and --sonar_model to skip "
            "the lookup."
        )
        sys.exit(1)

    logger.info(
        f"Querying NCEI BigQuery cache for '{file_name}' "
        "(may take a moment on first call)..."
    )
    df = get_metadata_from_search_param(search_param=file_name)

    if df is None or df.empty:
        logger.error(
            f"No NCEI cache entry matched '{file_name}'. "
            "Check the spelling, or use aa-raw + aa-nc directly with "
            "--ship_name / --survey_name / --sonar_model."
        )
        sys.exit(1)

    # `get_metadata_from_search_param` uses a LIKE %...% on s3_object_key,
    # so a single file name can match many auxiliary rows (idx, bot,
    # metadata files, etc.) or even other files whose name happens to
    # contain this string. Narrow to an exact .raw match here so the
    # rest of the resolution logic has a clean DataFrame to work with.
    df = df[(df["file_name"] == file_name) & (df["file_type"] == "raw")]

    if df.empty:
        logger.error(
            f"NCEI cache returned rows for the search string '{file_name}', "
            "but none of them are a raw file with that exact name. "
            "Double-check the file name (including extension)."
        )
        sys.exit(1)

    # Apply user-supplied overrides as additional filters. These are
    # disambiguators for the rare case where the same file_name lives
    # under multiple surveys — they let the user pin down the right row
    # without having to call aa-raw + aa-nc separately.
    if ship_name:
        df = df[df["ship_name_normalized"] == ship_name]
    if survey_name:
        df = df[df["survey_name"] == survey_name]
    if sonar_model:
        df = df[df["echosounder_name"] == sonar_model]

    if df.empty:
        logger.error(
            f"After applying the provided overrides, no rows remain for "
            f"'{file_name}'. Check that --ship_name / --survey_name / "
            "--sonar_model match the cache exactly."
        )
        sys.exit(1)

    if len(df) > 1:
        # Show the user what we found so they can pick the right one
        # via the override flags. This is the only place where multiple
        # rows surface; we deliberately don't pick one for them.
        choices = df[
            ["ship_name_normalized", "survey_name", "echosounder_name"]
        ].drop_duplicates()
        logger.error(
            f"Multiple NCEI cache entries match '{file_name}':\n"
            f"{choices.to_string(index=False)}\n"
            "Disambiguate with --ship_name / --survey_name / --sonar_model."
        )
        sys.exit(1)

    row = df.iloc[0]
    return {
        "ship_name": row["ship_name_normalized"],
        "survey_name": row["survey_name"],
        "sonar_model": row["echosounder_name"],
    }


def process_file(
    file_name: str,
    metadata: dict,
    raw_path: Path,
    nc_path: Path,
    download_dir: Path,
    upload_to_gcp: bool,
    debug: bool,
) -> None:
    """Download the raw file from NCEI and convert it to NetCDF.

    Mirrors the work of aa-raw and aa-nc respectively, in-process, so
    we don't have to spawn subprocesses or re-parse arguments. The .raw
    is left on disk unless the caller deletes it (see --cleanup-raw).
    """
    # Heavy imports deferred so --help and resolve_metadata errors are
    # fast — echopype in particular is slow to import.
    try:
        from aalibrary.ingestion import download_raw_file_from_ncei
    except Exception as e:
        logger.exception(f"Failed to import aalibrary.ingestion: {e}")
        sys.exit(1)

    try:
        import echopype as ep
    except Exception as e:
        logger.exception(f"Failed to import echopype: {e}")
        sys.exit(1)

    # ---- Download .raw -------------------------------------------
    logger.info(
        f"Downloading {file_name} "
        f"({metadata['ship_name']} / {metadata['survey_name']} / "
        f"{metadata['sonar_model']}) from NCEI -> {download_dir}"
    )
    download_raw_file_from_ncei(
        file_name=file_name,
        file_type="raw",
        ship_name=metadata["ship_name"],
        survey_name=metadata["survey_name"],
        echosounder=metadata["sonar_model"],
        file_download_directory=str(download_dir),
        upload_to_gcp=upload_to_gcp,
        debug=debug,
    )

    # Same sanity check as aa-raw: download_raw_file_from_ncei returns
    # nothing useful, so we only know it worked by checking disk. Without
    # this, a silent failure would let us hand a missing path to echopype
    # and surface a confusing error from the open_raw layer instead.
    if not raw_path.exists():
        logger.error(
            f"Download appeared to succeed, but '{raw_path}' is not on "
            "disk. Rerun with --debug for details."
        )
        sys.exit(1)
    logger.success(f"Downloaded {raw_path.name} ({raw_path.stat().st_size} bytes).")

    # ---- Convert .raw → .nc -------------------------------------
    logger.info(
        f"Loading {raw_path} into EchoData "
        f"(sonar_model={metadata['sonar_model']})"
    )
    ed = ep.open_raw(
        raw_file=raw_path,
        sonar_model=metadata["sonar_model"],
    )

    logger.info(f"Saving EchoData to {nc_path}")
    ed.to_netcdf(save_path=nc_path)

    if not nc_path.exists():
        # Defensive: to_netcdf shouldn't return silently on failure, but
        # if it does, fall through with a clear error rather than printing
        # a missing path to stdout and breaking aa-sv downstream.
        logger.error(
            f"Conversion appeared to succeed, but '{nc_path}' is not on "
            "disk. Rerun with --debug for details."
        )
        sys.exit(1)

    logger.success(f"RAW → NetCDF conversion complete: {nc_path.resolve()}")


if __name__ == "__main__":
    main()