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

Idempotency: if the target .nc already exists, aa-ed prints its path
and exits immediately (no BigQuery lookup, no download, no conversion).
If only the .raw exists, the download is skipped but the conversion
still runs. Pass --force to override both checks.

Input shape: aa-ed auto-detects three modes from the input:

  - Bare filename ("HB1603...raw") -> full NCEI flow: BigQuery
    lookup + download + convert. Output: .nc absolute path on stdout.

  - Path to an existing .raw file ("/abs/path/file.raw") -> fully
    offline single-file mode. Sonar model detected from header (or
    --sonar_model), .nc lands next to the .raw, ZERO network calls.
    Output: .nc absolute path on stdout. --force never overwrites
    a user-provided .raw.

  - Path to an existing directory ("/abs/path/dir/") -> batch mode.
    Globs *.raw (or **/*.raw with --recursive), converts each via
    the same offline path, passes through standalone .nc files,
    keeps going on per-file failures. Output: the DIRECTORY path
    on stdout (not a list of .nc paths) for aa-combine et al.

Idempotency: if the target .nc already exists, aa-ed prints its path
and exits (single-file modes) or counts it as a cache hit (directory
mode). --force overrides.

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
      FILE_NAME                   The raw file (or directory) to process.
                                  THREE shapes accepted, auto-detected:

                                  - Bare filename (e.g.
                                    HB1603_L1-D20160703-T183957.raw)
                                    -> aa-ed queries the NCEI BigQuery
                                    cache for metadata, downloads the
                                    file, and writes the .nc into
                                    --file_download_directory.

                                  - Path to an existing .raw file (e.g.
                                    /home/me/data/HB1603...raw or
                                    ./data/HB1603...raw) -> aa-ed uses
                                    it as-is, detects the sonar model
                                    from the file header (no BigQuery,
                                    no NCEI download, no GCP creds
                                    needed), and writes the .nc
                                    ALONGSIDE the .raw.

                                  - Path to an existing directory (e.g.
                                    /home/me/data/ or ./data/) ->
                                    DIRECTORY BATCH MODE. aa-ed globs
                                    *.raw inside (or **/*.raw with -r),
                                    runs the same offline conversion
                                    on each file, and prints the
                                    DIRECTORY path on stdout (not
                                    individual .nc paths). Standalone
                                    .nc files pass through silently.
                                    Per-file failures are logged but
                                    don't abort the batch; exit code
                                    is non-zero if any failed.

                                  Optional; falls back to stdin if not
                                  provided.

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

      --force, -f                 Re-download and re-convert even when the
                                  .raw / .nc are already on disk. Default
                                  behavior is to treat both as cached: an
                                  existing .nc short-circuits everything
                                  (including the BigQuery lookup), and an
                                  existing .raw skips the NCEI download.
                                  Use this if you suspect a cached file is
                                  stale or corrupt.

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
        "--force", "-f",
        action="store_true",
        default=False,
        help="Re-download and re-convert even if the .raw / .nc are on disk.",
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
        "--recursive", "-r",
        action="store_true",
        default=False,
        help="In directory mode, recursively scan subdirectories for .raw "
             "files. No effect when the input is a single file.",
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

    # Three acceptable input shapes:
    #   1. Bare file name ("HB1603...raw") — aa-ed will download from
    #      NCEI to --file_download_directory. (NCEI single-file mode)
    #   2. Path to an existing .raw file
    #      ("/home/me/data/HB1603...raw" or "./data/HB1603...raw") —
    #      use it as-is, skip NCEI download and BigQuery entirely.
    #      (Local single-file mode)
    #   3. Path to an existing directory ("/home/me/data/" or "./data/")
    #      — convert every .raw inside (skipping cache hits, passing
    #      through standalone .nc files) and print the directory path
    #      on stdout for aa-combine et al. (Directory batch mode)
    #
    # Mode is auto-detected from the input shape — no flag needed.
    # The directory branch dispatches BEFORE args.file_name gets set
    # to a basename (which would be the directory's name, not a useful
    # value) and before the .raw-extension check (irrelevant for dirs).
    _user_input = args.file_name
    _input_path = Path(_user_input).expanduser()
    _has_directory = _user_input != _input_path.name

    user_provided_raw_path: Optional[Path] = None
    if _has_directory:
        _input_path = _input_path.resolve()

        # === Directory mode dispatch ===============================
        if _input_path.is_dir():
            _run_directory_mode(directory=_input_path, args=args)
            return

        if not _input_path.is_file():
            # If the user gave us a path, they expect that path to
            # resolve. Silently falling back to "download to CWD" here
            # would be the surprising behavior we're trying to avoid.
            logger.error(
                f"Path '{_input_path}' does not exist as a file or "
                "directory. If you intended for aa-ed to download from "
                f"NCEI, pass just the filename ('{_input_path.name}') "
                "without a directory component."
            )
            sys.exit(1)

        user_provided_raw_path = _input_path
        logger.info(
            f"Using user-provided .raw at '{user_provided_raw_path}'; "
            "no NCEI download will be performed for this file."
        )

    # The NCEI cache lookup always works on the basename only — even
    # when the user gave us a path, the BigQuery file_name column
    # stores just the filename. We set args.file_name here, AFTER the
    # directory check, so directory mode doesn't see a confusing
    # basename-of-the-directory value.
    args.file_name = _input_path.name

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

    if user_provided_raw_path is not None:
        # The .raw is already on disk at the user-specified location;
        # we never need to create a download directory. Set download_dir
        # to the file's parent for symmetry / logging only — it won't
        # be used to land any downloads. If the user ALSO passed
        # --file_download_directory, the path-form input wins and we
        # warn rather than silently ignoring it. ("." is the parser
        # default, used as a sentinel for "user didn't actually set it".)
        download_dir = user_provided_raw_path.parent
        if args.file_download_directory != ".":
            logger.warning(
                f"--file_download_directory='{args.file_download_directory}' "
                f"is being ignored: the .raw is already on disk at "
                f"'{user_provided_raw_path}'. Drop the directory "
                "component from the input if you want a fresh download."
            )
    else:
        download_dir = Path(args.file_download_directory).expanduser().resolve()
        try:
            download_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            logger.error(f"Could not create download directory '{download_dir}': {e}")
            sys.exit(2)

    args_summary = {
        "file_name": args.file_name,
        "user_provided_raw_path": (
            str(user_provided_raw_path) if user_provided_raw_path else None
        ),
        "output_path": args.output_path,
        "file_download_directory": str(download_dir),
        "ship_name": args.ship_name,
        "survey_name": args.survey_name,
        "sonar_model": args.sonar_model,
        "cleanup_raw": args.cleanup_raw,
        "force": args.force,
        "upload_to_gcp": args.upload_to_gcp,
        "data_source": args.data_source,
        "debug": args.debug,
    }
    logger.debug(
        f"Executing aa-ed configured with [OPTIONS]:\n"
        f"{pprint.pformat(args_summary)}"
    )

    # ---------------------------
    # Resolve output paths
    # ---------------------------
    # Resolved up here (rather than after the metadata lookup) so the
    # cache-skip check below can short-circuit on an existing .nc
    # without ever touching BigQuery.
    #
    # When the user provided the .raw via a path, that path IS the
    # raw_path — we never construct one under download_dir. The .nc
    # then lands next to the user's .raw by default (-o still wins).
    if user_provided_raw_path is not None:
        raw_path = user_provided_raw_path
    else:
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
    # Short-circuit: .nc already on disk
    # ---------------------------
    # Conversion is the expensive step (echopype's open_raw on a multi-
    # hundred-MB .raw dwarfs the NCEI download). If the .nc is already
    # there, the user has nothing to gain from re-running anything —
    # skip the BigQuery lookup, the download, and the conversion, and
    # just hand the existing path to the next pipeline stage.
    # --force overrides this for users who suspect the cached .nc is
    # stale or corrupt.
    logger.debug(
        f"Checking for existing .nc at {nc_path.resolve()}: "
        f"exists={nc_path.exists()}, force={args.force}"
    )
    if nc_path.exists() and not args.force:
        logger.success(
            f".nc already exists; NOT overwriting. Reusing: "
            f"{nc_path.resolve()} (pass --force to regenerate)."
        )
        print(nc_path.resolve())
        return

    # Only log "will be written" once we know we're actually going to
    # write — i.e. past the short-circuit. The previous version logged
    # this unconditionally, which made successful cache hits look like
    # writes in the stderr stream.
    logger.info(f"Output .nc will be written to: {nc_path.resolve()}")

    # ---------------------------
    # Resolve metadata
    # ---------------------------
    # Critical branch: when the user supplied the .raw locally we do
    # NOT touch BigQuery and do NOT touch aalibrary.ingestion at all.
    # We only need sonar_model for echopype.open_raw; ship_name and
    # survey_name exist purely for the NCEI download (which is skipped).
    # Sonar model comes from --sonar_model if given, else is detected
    # from the .raw file's header via aalibrary's sonar_checker — the
    # same logic aa-sonar uses. This keeps the local-file path fully
    # offline, no GCP creds required.
    if user_provided_raw_path is not None:
        if args.sonar_model:
            sonar_model = args.sonar_model
            logger.info(
                f"Using explicit --sonar_model='{sonar_model}' "
                "for user-provided .raw."
            )
        else:
            logger.info(
                f"Detecting sonar model from {raw_path.name} header "
                "(no BigQuery query, no NCEI download)..."
            )
            sonar_model = _detect_sonar_model_from_file(raw_path)
            if sonar_model == "UNKNOWN":
                logger.error(
                    f"Could not auto-detect a sonar model from '{raw_path}'. "
                    "Pass --sonar_model explicitly (e.g. EK60, EK80, AZFP)."
                )
                sys.exit(1)
            logger.info(f"Detected sonar model: {sonar_model}")

        metadata = {
            # ship_name / survey_name are only consumed by the NCEI
            # download path, which we never enter here. Fill placeholders
            # so anything that reads `metadata[...]` keeps working, but
            # make it clear in logs that they're not authoritative.
            "ship_name": args.ship_name or "<local>",
            "survey_name": args.survey_name or "<local>",
            "sonar_model": sonar_model,
        }
    else:
        # Bare-filename input — full NCEI flow with BigQuery lookup.
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
        f"Metadata resolved for {args.file_name}: "
        f"ship='{metadata['ship_name']}', "
        f"survey='{metadata['survey_name']}', "
        f"sonar='{metadata['sonar_model']}'."
    )

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
            force=args.force,
            user_provided_raw=(user_provided_raw_path is not None),
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
        if user_provided_raw_path is not None:
            # The "intermediate" .raw was actually the user's source
            # data — they passed its path explicitly. Refusing here is
            # the right call: --cleanup-raw is meant to clean up
            # aa-ed's own downloads, not the user's source files.
            logger.warning(
                f"--cleanup-raw ignored: '{raw_path}' was provided by "
                "the user, not downloaded by aa-ed. Delete it manually "
                "if you really want it gone."
            )
        else:
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


def _detect_sonar_model_from_file(raw_path: Path) -> str:
    """Detect the sonar model of a local file by inspecting its header.

    Returns an echopype-normalized identifier (EK60, EK80, AZFP,
    AD2CP) or "UNKNOWN" if detection fails.

    Mirrors aa-sonar's detection logic: extension-only fast paths for
    AD2CP/AZFP/AZFP6, header-byte inspection for Simrad .raw via
    aalibrary's sonar_checker. ER60 is normalized to EK60 (echopype
    shares a code path); AZFP6 is normalized to AZFP.

    Used by aa-ed only when the user supplies the .raw locally — saves
    a BigQuery roundtrip when all we need is the sonar_model that
    echopype.open_raw expects, and keeps the whole local-file path
    fully offline (no GCP credentials required).
    """
    try:
        from aalibrary.utils.sonar_checker.sonar_checker import (
            is_AD2CP,
            is_AZFP,
            is_AZFP6,
            is_EK60,
            is_EK80,
            is_ER60,
        )
    except Exception as e:
        # If sonar_checker isn't importable, the user just has to pass
        # --sonar_model explicitly. Don't crash here — let the caller
        # decide what to do with "UNKNOWN".
        logger.debug(f"sonar_checker unavailable: {e}")
        return "UNKNOWN"

    path_str = str(raw_path)
    storage_options: dict = {}  # local file, no fsspec creds needed
    ext = raw_path.suffix.lower()

    # ---- Extension-only fast paths --------------------------------
    if ext == ".ad2cp" or is_AD2CP(path_str):
        return "AD2CP"
    if ext == ".azfp" or is_AZFP6(path_str):
        return "AZFP"

    # ---- AZFP XML sidecar -----------------------------------------
    if ext == ".xml" and is_AZFP(path_str):
        return "AZFP"

    # ---- Simrad .raw header inspection ----------------------------
    if ext == ".raw":
        # EK80 has a 'configuration' block in its config datagram;
        # EK60/ER60 expose 'sounder_name' instead. The two checks
        # don't overlap on real files. ER60 normalizes to EK60.
        try:
            if is_EK80(path_str, storage_options):
                return "EK80"
        except Exception as e:
            logger.debug(f"EK80 check raised on {raw_path}: {e}")
        try:
            if is_EK60(path_str, storage_options):
                return "EK60"
        except Exception as e:
            logger.debug(f"EK60 check raised on {raw_path}: {e}")
        try:
            if is_ER60(path_str, storage_options):
                return "EK60"  # ER60 → EK60 for echopype
        except Exception as e:
            logger.debug(f"ER60 check raised on {raw_path}: {e}")

    return "UNKNOWN"


def process_file(
    file_name: str,
    metadata: dict,
    raw_path: Path,
    nc_path: Path,
    download_dir: Path,
    upload_to_gcp: bool,
    debug: bool,
    force: bool = False,
    user_provided_raw: bool = False,
) -> None:
    """Download the raw file from NCEI and convert it to NetCDF.

    Mirrors the work of aa-raw and aa-nc respectively, in-process, so
    we don't have to spawn subprocesses or re-parse arguments. The .raw
    is left on disk unless the caller deletes it (see --cleanup-raw).

    Idempotency:
      - If user_provided_raw is True, the .raw at raw_path is the
        user's source file. It is never re-downloaded — not even when
        force=True. --force only forces re-conversion of the .nc in
        that case; the user's .raw is preserved verbatim.
      - Otherwise, if raw_path is already on disk and force=False, the
        download is skipped (cache hit). Pass force=True to invalidate.
      - The .nc-already-exists short-circuit lives upstream in main()
        because it also lets us skip the BigQuery lookup; by the time
        we get here, we know the .nc needs (re)building.
    """
    # Echopype is needed in every branch (we always convert). Imported
    # up front. The aalibrary.ingestion download is imported lazily
    # ONLY inside the download branch below — when user_provided_raw is
    # True, the download module is never even loaded, never mind called.
    try:
        import echopype as ep
    except Exception as e:
        logger.exception(f"Failed to import echopype: {e}")
        sys.exit(1)

    # ---- .raw acquisition ----------------------------------------
    if user_provided_raw:
        # User-supplied source file. Belt-and-suspenders existence
        # check — main() already validated this, but a race or an
        # `rm` between validation and now shouldn't produce a
        # confusing echopype error downstream.
        if not raw_path.exists():
            logger.error(
                f"User-provided .raw '{raw_path}' has disappeared "
                "since input validation."
            )
            sys.exit(1)
        logger.info(
            f"Using user-provided .raw: {raw_path} "
            f"({raw_path.stat().st_size} bytes). "
            "NCEI download SKIPPED (no aalibrary.ingestion import, "
            "no network call)."
        )
    elif raw_path.exists() and not force:
        # Cache hit — file from an earlier aa-ed run is still on disk.
        # --force is the escape hatch if the user suspects corruption.
        logger.info(
            f".raw already on disk, skipping NCEI download: {raw_path} "
            f"({raw_path.stat().st_size} bytes). Pass --force to re-download."
        )
    else:
        # Genuine NCEI download path. Import the download function
        # only here — that way the user-provided-raw branch above
        # cannot accidentally trigger it.
        try:
            from aalibrary.ingestion import download_raw_file_from_ncei
        except Exception as e:
            logger.exception(f"Failed to import aalibrary.ingestion: {e}")
            sys.exit(1)

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

        # Sanity check: download_raw_file_from_ncei returns nothing
        # useful, so we only know it worked by checking disk. Without
        # this, a silent failure would let us hand a missing path to
        # echopype and surface a confusing error from open_raw instead.
        if not raw_path.exists():
            logger.error(
                f"Download appeared to succeed, but '{raw_path}' is not on "
                "disk. Rerun with --debug for details."
            )
            sys.exit(1)
        logger.success(
            f"Downloaded {raw_path.name} ({raw_path.stat().st_size} bytes)."
        )

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


def _convert_one_local(
    raw_path: Path,
    nc_path: Path,
    sonar_model_override: Optional[str],
    force: bool,
) -> dict:
    """Convert a single local .raw to .nc, fully offline.

    No NCEI download, no BigQuery — assumes the .raw is already on
    disk at raw_path. Sonar model comes from sonar_model_override if
    given, otherwise auto-detected from the .raw file's header via
    _detect_sonar_model_from_file().

    Idempotent: if nc_path already exists and not force, returns
    early with status="cached" — the existing .nc is treated as
    authoritative.

    Used by _run_directory_mode to process each file in a batch.
    Returns a result dict instead of calling sys.exit so the caller
    can aggregate results across many files (a single bad file at
    file 47/100 shouldn't discard files 1-46).

    Returns a dict shaped like:
        {
            "status": "converted" | "cached" | "failed",
            "raw_path": Path,
            "nc_path": Path,
            "sonar_model": str,   # present when status == "converted"
            "error": str,         # present when status == "failed"
        }
    """
    # Cache check first — same idempotency contract as single-file mode.
    if nc_path.exists() and not force:
        return {
            "status": "cached",
            "raw_path": raw_path,
            "nc_path": nc_path,
        }

    # Resolve sonar model.
    if sonar_model_override:
        sonar_model = sonar_model_override
    else:
        sonar_model = _detect_sonar_model_from_file(raw_path)
        if sonar_model == "UNKNOWN":
            return {
                "status": "failed",
                "raw_path": raw_path,
                "nc_path": nc_path,
                "error": (
                    "could not auto-detect sonar model from header; "
                    "pass --sonar_model to set it explicitly"
                ),
            }

    # Echopype import. Import-once-per-call is wasteful in a loop but
    # the import is cached after the first call, so the cost is paid
    # once across the batch.
    try:
        import echopype as ep
    except Exception as e:
        return {
            "status": "failed",
            "raw_path": raw_path,
            "nc_path": nc_path,
            "error": f"echopype import failed: {e}",
        }

    try:
        # If --force and the .nc already exists, remove it first.
        # echopype's to_netcdf backend behavior on existing files is
        # version-dependent (append vs error); removing up front
        # makes the result predictable.
        if nc_path.exists():
            nc_path.unlink()
        ed = ep.open_raw(raw_file=raw_path, sonar_model=sonar_model)
        ed.to_netcdf(save_path=nc_path)
    except Exception as e:
        return {
            "status": "failed",
            "raw_path": raw_path,
            "nc_path": nc_path,
            "error": f"conversion error: {e}",
        }

    if not nc_path.exists():
        # Defensive: to_netcdf returned without raising but the file
        # isn't there. Treat as a failure rather than reporting
        # spurious success.
        return {
            "status": "failed",
            "raw_path": raw_path,
            "nc_path": nc_path,
            "error": "to_netcdf completed but .nc is not on disk",
        }

    return {
        "status": "converted",
        "raw_path": raw_path,
        "nc_path": nc_path,
        "sonar_model": sonar_model,
    }


def _run_directory_mode(directory: Path, args) -> None:
    """Batch-convert every .raw in `directory` to .nc.

    Dispatched from main() when the user's input path resolves to an
    existing directory. The rest of main() is single-file-only, so
    this function owns the entire directory pipeline end-to-end:
    flag-combination guardrails, globbing, per-file conversion via
    _convert_one_local, result aggregation, and the final stdout
    print.

    Per-file failure policy: keep going, log each failure, exit
    non-zero at the end if any failed. Aborting the whole batch on
    the first bad file would discard hours of completed work in a
    long run; the user can rerun the failures alone afterward.

    The directory path (not a list of .nc paths) is printed on stdout
    so downstream tools like aa-combine — which already accepts a
    directory and globs *.nc inside — can pick up where aa-ed left
    off.
    """
    # ---- Flag-combination guardrails ----------------------------
    if args.output_path is not None:
        # In single-file mode, -o picks the .nc filename. In directory
        # mode it would have to mean "target directory for all the
        # .nc files," which is a different contract and would let
        # users accidentally collide many .nc files into one path.
        # Reject up front rather than guessing.
        logger.error(
            "-o / --output_path is not supported in directory mode. "
            ".nc files always land alongside their source .raw inside "
            "the input directory."
        )
        sys.exit(2)

    if args.cleanup_raw:
        # Single-file --cleanup-raw deletes one downloaded .raw the
        # user has consented to discarding. Directory mode would
        # delete N user-owned files at once — different scale of
        # risk. Refuse outright.
        logger.error(
            "--cleanup-raw is not supported in directory mode. "
            "Deleting many user-owned .raw files at once is too "
            "risky for a single flag; remove them manually after "
            "verifying the .nc outputs."
        )
        sys.exit(2)

    if args.upload_to_gcp:
        # Threading upload through the per-file loop would change
        # the contract of aa-upload and isn't currently wired. Point
        # the user at the clean composition.
        logger.warning(
            "--upload_to_gcp is ignored in directory mode. Pipe the "
            "directory through aa-upload after aa-ed for that "
            "(aa-ed ./dir/ | aa-upload --as-is ...)."
        )

    # ---- Glob inputs --------------------------------------------
    raw_pattern = "**/*.raw" if args.recursive else "*.raw"
    nc_pattern = "**/*.nc" if args.recursive else "*.nc"
    raw_files = sorted(directory.glob(raw_pattern))
    all_nc_files = sorted(directory.glob(nc_pattern))

    if not raw_files and not all_nc_files:
        hint = " Try --recursive." if not args.recursive else ""
        logger.error(
            f"No .raw or .nc files found in '{directory}' "
            f"(pattern: '{raw_pattern}').{hint}"
        )
        sys.exit(1)

    logger.info(
        f"Directory mode: found {len(raw_files)} .raw and "
        f"{len(all_nc_files)} .nc file(s) in '{directory}' "
        f"(recursive={args.recursive})."
    )

    # Identify standalone .nc files — those without a matching .raw
    # in the same directory. These count as already-converted and
    # pass through to the directory output without us touching them.
    # Matching is (parent, stem) to handle the recursive case where
    # files in different subdirs might share a stem.
    raw_stems_by_dir = {(p.parent, p.stem) for p in raw_files}
    standalone_nc = [
        n for n in all_nc_files
        if (n.parent, n.stem) not in raw_stems_by_dir
    ]

    # Sonar model override applies uniformly to every .raw in the dir.
    # Surveys are usually single-echosounder, so this is the common case.
    sonar_override = args.sonar_model
    if sonar_override:
        logger.info(
            f"Applying --sonar_model='{sonar_override}' uniformly to "
            f"all {len(raw_files)} .raw file(s) in the directory."
        )
    elif raw_files:
        logger.info(
            "Auto-detecting sonar model per file from .raw headers."
        )

    if args.force and raw_files:
        # --force in directory mode regenerates EVERY .nc, not just
        # one. Easy to fire by accident with a stale flag from a
        # prior single-file run, so warn loudly with the count.
        existing_nc = sum(
            1 for r in raw_files if r.with_suffix(".nc").exists()
        )
        if existing_nc:
            logger.warning(
                f"--force will regenerate {existing_nc} existing .nc "
                f"file(s) in '{directory}'."
            )

    # ---- Per-file conversion loop -------------------------------
    counts = {
        "converted": 0,
        "cached": 0,
        "passthrough": len(standalone_nc),
        "failed": 0,
    }
    failures: list = []

    for i, raw_path in enumerate(raw_files, start=1):
        nc_path = raw_path.with_suffix(".nc")
        logger.info(f"[{i}/{len(raw_files)}] {raw_path.name}")

        result = _convert_one_local(
            raw_path=raw_path,
            nc_path=nc_path,
            sonar_model_override=sonar_override,
            force=args.force,
        )

        status = result["status"]
        counts[status] = counts.get(status, 0) + 1

        if status == "converted":
            logger.success(
                f"  -> {nc_path.name} "
                f"(sonar={result['sonar_model']}, "
                f"{nc_path.stat().st_size:,} bytes)"
            )
        elif status == "cached":
            logger.info(
                "  .nc already exists, skipping "
                "(--force to override)."
            )
        elif status == "failed":
            logger.error(f"  FAILED: {result['error']}")
            failures.append((raw_path, result["error"]))

    # ---- Summary + stdout contract ------------------------------
    total_outputs = (
        counts["converted"] + counts["cached"] + counts["passthrough"]
    )
    if total_outputs > 0:
        logger.success(
            f"Directory mode complete in '{directory}': "
            f"{counts['converted']} converted, "
            f"{counts['cached']} cached (skipped), "
            f"{counts['passthrough']} pre-existing .nc passed through, "
            f"{counts['failed']} failed."
        )

    if failures:
        logger.error(
            f"--- {len(failures)} file(s) failed during conversion: ---"
        )
        for raw_path, err in failures:
            logger.error(f"  {raw_path}: {err}")

    # Pipeline contract: emit the directory path on stdout so aa-combine
    # (which already accepts a directory and globs *.nc inside) can
    # pick up where aa-ed left off. We print this even on partial
    # failure — successful conversions are real and downstream may
    # still want them. The non-zero exit code below signals "something
    # failed" so a careful shell pipeline can react.
    print(directory.resolve())

    if failures:
        sys.exit(1)


if __name__ == "__main__":
    main()