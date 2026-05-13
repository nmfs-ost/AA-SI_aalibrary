#!/usr/bin/env python3
"""
aa-upload

Console tool for uploading echosounder files or arbitrary folders to a
GCP storage bucket via aalibrary.egress. Pipeline-friendly: prints the
input path back to stdout so aa-upload can sit *between* stages as a
side-effect tee.

Two upload modes (auto-detected, can be forced):

  1. Echosounder mode (default) — wraps
       aalibrary.egress.upload_local_echosounder_files_from_directory_to_gcp_storage_bucket
     Maintains AALibrary's canonical folder structure
     (data/raw/<ship>/<survey>/<echosounder>/...) so the uploaded file
     is retrievable by aa-fetch / aa-raw later. Requires --ship_name,
     --survey_name, --sonar_model. --data_source defaults to "HDD"
     (the convention for files coming off local disk).

     Works on a single file OR a directory. Single files are uploaded
     by symlinking them into a temp directory and pointing the
     directory uploader at that — keeps the path-convention logic
     inside aalibrary where it belongs.

  2. As-is mode (--as-is) — wraps aalibrary.egress.upload_folder_as_is_to_gcp.
     Uploads a directory tree verbatim under --destination_prefix. No
     structure enforcement, no metadata flags. Use this for one-off
     dumps that don't need to be retrievable through aalibrary's
     ship/survey/echosounder views.

Pipeline contract (mirrors the rest of the aa-suite):
    input  : a single file or directory path, positional arg or stdin
    output : the same path printed to stdout (pass-through tee)
    logs   : stderr via loguru

Typical pipeline usage:
    echo file.raw | aa-ed \\
        | aa-upload --ship_name Henry_B._Bigelow \\
                    --survey_name HB1603 --sonar_model EK60 \\
        | aa-sv | aa-graph

    aa-upload ./HB1603/EK60 --ship_name Henry_B._Bigelow \\
              --survey_name HB1603 --sonar_model EK60

    aa-upload ./random_dump --as-is --destination_prefix other/scratch/
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
import os
import pprint
import signal
import tempfile
from pathlib import Path
from typing import Optional


# Pipeline tools should die cleanly when the downstream end of the pipe
# closes early (`... | head -n 1`), not throw BrokenPipeError. Guarded
# with hasattr because SIGPIPE doesn't exist on Windows.
if hasattr(signal, "SIGPIPE"):
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)


# Echosounder mode only uploads files with one of these extensions when
# pointed at a directory. The directory uploader has its own filter
# internally — this list is also used for single-file mode (to refuse
# to "upload as echosounder" something that obviously isn't one) and
# in dry-run preview output. Lowercased; comparison is case-insensitive.
ECHOSOUNDER_EXTENSIONS = {".raw", ".idx", ".bot", ".nc"}


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
    Usage: aa-upload [OPTIONS] [PATH]

    Arguments:
      PATH                        File or directory to upload. May be a
                                  bare name (resolved against CWD), a
                                  relative path, or absolute path.
                                  Optional; falls back to stdin if not
                                  given. Symlinks are followed.

    Echosounder-mode options (used unless --as-is is set):
      --ship_name NAME            Ship name as stored in NCEI / GCP
                                  (normalized form, e.g. Henry_B._Bigelow).
                                  REQUIRED in echosounder mode.
      --survey_name NAME          Survey name (e.g. HB1603).
                                  REQUIRED in echosounder mode.
      --sonar_model NAME          Echosounder model (e.g. EK60, EK80).
                                  REQUIRED in echosounder mode.
      --data_source SRC           Data source tag stored alongside the
                                  file in GCP. Defaults to 'HDD' (the
                                  convention for local-disk uploads).
                                  Other values: NCEI, OMAO, etc.

    As-is mode options:
      --as-is, --as_is            Upload the input directory verbatim
                                  to GCP using upload_folder_as_is_to_gcp.
                                  Requires --destination_prefix. The
                                  input must be a directory; single
                                  files are not supported in this mode.
      --destination_prefix PFX    Bucket-relative prefix to drop the
                                  folder under (e.g. other/scratch/).
                                  REQUIRED in as-is mode.

    GCP environment:
      --gcp_env {prod,dev}        Switch the active aalibrary GCP env
                                  before uploading via
                                  aalibrary.config.use_gcp_prod() or
                                  use_gcp_dev(). If neither this nor
                                  the explicit overrides below are set,
                                  whatever env vars are already exported
                                  in the shell are used.
      --project_id ID             Explicit GCP project id (overrides
                                  --gcp_env).
      --gcp_bucket_name NAME      Explicit GCP bucket name (overrides
                                  --gcp_env).

    Other:
      --dry-run, --dry_run        Resolve mode, validate everything,
                                  set up the GCP bucket object, but do
                                  NOT call the upload functions. Useful
                                  for checking flags before a long run.
      --debug                     Verbose logging (DEBUG level).
      --quiet                     Suppress INFO logs; pass-through path
                                  still prints on stdout.
      -h, --help                  Show this help and exit.

    Description:
      Uploads a single file or a directory to GCP via aalibrary.egress.

      Single-file inputs are handled by symlinking the file into a
      temporary directory and pointing the echosounder-mode uploader at
      that temp directory. This way aa-upload never has to hardcode the
      data/raw/<ship>/<survey>/<echosounder>/<file> path convention —
      whichever convention the directory uploader uses is the one we
      use. Only single files with extensions in {.raw, .idx, .bot, .nc}
      are accepted in echosounder mode.

      The input PATH is printed back to stdout unchanged so aa-upload
      can sit in the middle of a pipeline as a side-effect tee. If
      you're using aa-upload as the last stage, ignore stdout.

    Examples:
      # As a side-effect tee between aa-ed and aa-sv:
      echo HB1603_L1-D20160703-T183957.raw | aa-ed \\
        | aa-upload --ship_name Henry_B._Bigelow \\
                    --survey_name HB1603 --sonar_model EK60 \\
        | aa-sv | aa-graph

      # Upload a whole survey directory under the canonical layout:
      aa-upload ./Henry_B._Bigelow/HB1603/EK60 \\
        --ship_name Henry_B._Bigelow --survey_name HB1603 \\
        --sonar_model EK60 --data_source HDD

      # Dump a folder anywhere in the bucket, ignoring conventions:
      aa-upload ./scratch_data --as-is --destination_prefix other/junk/

      # Dry-run before a long upload:
      aa-upload ./big_dir --ship_name X --survey_name Y \\
        --sonar_model EK80 --dry-run
    """
    print(help_text)


def main() -> None:
    # Stdin / no-args handling — same shape as aa-ed: empty-stdin
    # invocation prints help instead of blocking on readline.
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
        description="Upload a file or directory to GCP via aalibrary.egress.",
        add_help=False,
    )

    parser.add_argument(
        "path",
        type=str,
        nargs="?",
        help="File or directory to upload.",
    )

    # Echosounder-mode metadata
    parser.add_argument("--ship_name", default=None,
                        help="Ship name (normalized form).")
    parser.add_argument("--survey_name", default=None,
                        help="Survey name (e.g. HB1603).")
    parser.add_argument("--sonar_model", default=None,
                        help="Echosounder model (e.g. EK60, EK80).")
    parser.add_argument("--data_source", default="HDD",
                        help="Data source tag (default: HDD).")

    # As-is mode
    parser.add_argument("--as-is", "--as_is", dest="as_is",
                        action="store_true", default=False,
                        help="Upload the directory verbatim instead of "
                             "using the echosounder structure.")
    parser.add_argument("--destination_prefix", default=None,
                        help="Bucket-relative prefix (required for --as-is).")

    # GCP env / overrides
    parser.add_argument("--gcp_env", choices=["prod", "dev"], default=None,
                        help="Switch aalibrary GCP env before upload.")
    parser.add_argument("--project_id", default=None,
                        help="GCP project id (overrides --gcp_env).")
    parser.add_argument("--gcp_bucket_name", default=None,
                        help="GCP bucket name (overrides --gcp_env).")

    # Misc
    parser.add_argument("--dry-run", "--dry_run", dest="dry_run",
                        action="store_true", default=False,
                        help="Validate and set up, but skip the actual upload.")
    parser.add_argument("--debug", action="store_true", default=False,
                        help="Enable verbose DEBUG-level logging.")
    parser.add_argument("--quiet", action="store_true", default=False,
                        help="Suppress INFO logs.")

    args = parser.parse_args()

    if args.debug and args.quiet:
        logger.error("Use --debug OR --quiet, not both.")
        sys.exit(2)

    _configure_logging(args.quiet, args.debug)

    # ---------------------------
    # Resolve input path (stdin fallback, basename behavior NOT applied
    # — unlike aa-ed, here the directory portion IS meaningful: we
    # need the actual filesystem location to read bytes from)
    # ---------------------------
    if args.path is None:
        if sys.stdin.isatty():
            logger.error("No path provided and no stdin available.")
            sys.exit(1)
        args.path = sys.stdin.readline().strip()
        logger.info(f"Read path from stdin: {args.path}")

    if not args.path:
        logger.error("Empty path.")
        sys.exit(1)

    input_path = Path(args.path).expanduser().resolve()
    if not input_path.exists():
        logger.error(f"Path '{input_path}' does not exist.")
        sys.exit(1)

    is_file = input_path.is_file()
    is_dir = input_path.is_dir()
    if not (is_file or is_dir):
        # Sockets, FIFOs, device nodes, broken symlinks that survived
        # the .exists() check above (rare but possible). Nothing useful
        # we can do with these.
        logger.error(
            f"'{input_path}' is neither a regular file nor a directory."
        )
        sys.exit(1)

    # ---------------------------
    # Mode dispatch + validation
    # ---------------------------
    if args.as_is:
        if is_file:
            logger.error(
                "--as-is requires a directory input; "
                f"'{input_path}' is a file. Drop --as-is, or point at a "
                "directory."
            )
            sys.exit(2)
        if not args.destination_prefix:
            logger.error(
                "--as-is requires --destination_prefix "
                "(e.g. --destination_prefix other/scratch/)."
            )
            sys.exit(2)
        mode = "as-is"
    else:
        # Echosounder mode — collect missing flags up front and report
        # them all at once. Better than letting the user fix one, retry,
        # discover the next, retry, etc.
        missing = [
            flag for flag, val in [
                ("--ship_name", args.ship_name),
                ("--survey_name", args.survey_name),
                ("--sonar_model", args.sonar_model),
            ] if not val
        ]
        if missing:
            logger.error(
                f"Echosounder upload requires {', '.join(missing)}. "
                "Pass them explicitly, or use --as-is for a "
                "convention-free upload."
            )
            sys.exit(2)

        if is_file:
            ext = input_path.suffix.lower()
            if ext not in ECHOSOUNDER_EXTENSIONS:
                logger.error(
                    f"'{input_path.name}' has extension '{ext}', which "
                    f"isn't in {sorted(ECHOSOUNDER_EXTENSIONS)}. The "
                    "echosounder uploader would skip it. Use --as-is "
                    "for arbitrary files, or rename if this is a "
                    "mis-extensioned echosounder file."
                )
                sys.exit(2)
        mode = "echosounder"

    args_summary = {
        "path": str(input_path),
        "mode": mode,
        "is_file": is_file,
        "is_dir": is_dir,
        "ship_name": args.ship_name,
        "survey_name": args.survey_name,
        "sonar_model": args.sonar_model,
        "data_source": args.data_source,
        "destination_prefix": args.destination_prefix,
        "gcp_env": args.gcp_env,
        "project_id": args.project_id,
        "gcp_bucket_name": args.gcp_bucket_name,
        "dry_run": args.dry_run,
    }
    logger.debug(
        f"Executing aa-upload configured with [OPTIONS]:\n"
        f"{pprint.pformat(args_summary)}"
    )

    # ---------------------------
    # Resolve GCP bucket
    # ---------------------------
    try:
        gcp_stor_client, gcp_bucket_name, gcp_bucket = _resolve_gcp_bucket(
            gcp_env=args.gcp_env,
            project_id=args.project_id,
            gcp_bucket_name=args.gcp_bucket_name,
        )
    except SystemExit:
        raise
    except Exception as e:
        logger.exception(
            f"Could not set up GCP storage objects: {e}\n"
            "Check that you have run `gcloud auth application-default login` "
            "and have permissions for the target project / bucket."
        )
        sys.exit(1)

    logger.info(f"Targeting GCP bucket '{gcp_bucket_name}'.")

    # ---------------------------
    # Dispatch
    # ---------------------------
    try:
        if mode == "as-is":
            _upload_as_is(
                local_folder=input_path,
                destination_prefix=args.destination_prefix,
                gcp_bucket=gcp_bucket,
                dry_run=args.dry_run,
            )
        else:  # echosounder
            _upload_echosounder(
                input_path=input_path,
                ship_name=args.ship_name,
                survey_name=args.survey_name,
                sonar_model=args.sonar_model,
                data_source=args.data_source,
                gcp_bucket=gcp_bucket,
                debug=args.debug,
                dry_run=args.dry_run,
            )
    except SystemExit:
        raise
    except Exception as e:
        logger.exception(f"Upload failed: {e}")
        sys.exit(1)

    if args.dry_run:
        logger.success(
            f"[dry-run] No bytes transferred. Would have uploaded "
            f"'{input_path}' in {mode} mode."
        )
    else:
        logger.success(
            f"Uploaded '{input_path}' to bucket '{gcp_bucket_name}' "
            f"({mode} mode). Passing input path through to stdout..."
        )

    # Pipeline contract: pass the input path through unchanged so
    # aa-upload can sit between stages as a tee. Downstream tools see
    # the same local path the user gave us.
    print(input_path)


def _resolve_gcp_bucket(
    gcp_env: Optional[str],
    project_id: Optional[str],
    gcp_bucket_name: Optional[str],
):
    """Set up (gcp_stor_client, gcp_bucket_name, gcp_bucket).

    Order of precedence:
      1. If --project_id and/or --gcp_bucket_name are given, pass them
         directly to setup_gcp_storage_objs. These win over --gcp_env.
      2. Else if --gcp_env is given, call
         aalibrary.config.use_gcp_prod() / use_gcp_dev() to set the env
         vars, then setup_gcp_storage_objs() reads them.
      3. Else fall back to whatever AALIBRARY_GCP_* env vars are already
         exported in the shell (the library's normal default behavior).
    """
    # Heavy import deferred until we actually need GCP (keeps --help
    # snappy and lets validation errors above fail fast).
    try:
        from aalibrary.utils.cloud_utils import setup_gcp_storage_objs
    except Exception as e:
        logger.exception(f"Failed to import aalibrary.utils.cloud_utils: {e}")
        sys.exit(1)

    if gcp_env and not (project_id or gcp_bucket_name):
        # Only honor --gcp_env if the user didn't also pass explicit
        # overrides — explicit beats convenience.
        try:
            from aalibrary import config as aalibrary_config
        except Exception as e:
            logger.exception(
                f"Failed to import aalibrary.config for --gcp_env: {e}"
            )
            sys.exit(1)

        if gcp_env == "prod":
            if not hasattr(aalibrary_config, "use_gcp_prod"):
                logger.error(
                    "--gcp_env prod requested but aalibrary.config has no "
                    "use_gcp_prod(). Pass --project_id and --gcp_bucket_name "
                    "explicitly instead."
                )
                sys.exit(1)
            aalibrary_config.use_gcp_prod()
        elif gcp_env == "dev":
            if not hasattr(aalibrary_config, "use_gcp_dev"):
                logger.error(
                    "--gcp_env dev requested but aalibrary.config has no "
                    "use_gcp_dev(). Pass --project_id and --gcp_bucket_name "
                    "explicitly instead."
                )
                sys.exit(1)
            aalibrary_config.use_gcp_dev()
        logger.info(f"Switched aalibrary GCP env to '{gcp_env}'.")

    return setup_gcp_storage_objs(
        project_id=project_id,
        gcp_bucket_name=gcp_bucket_name,
    )


def _upload_echosounder(
    input_path: Path,
    ship_name: str,
    survey_name: str,
    sonar_model: str,
    data_source: str,
    gcp_bucket,
    debug: bool,
    dry_run: bool,
) -> None:
    """Echosounder-mode upload.

    Directory inputs are handed straight to the directory uploader.
    Single-file inputs are wrapped in a temp directory of symlinks
    so we don't have to hardcode the
    data/raw/<ship>/<survey>/<echosounder>/<file> path convention —
    aalibrary owns it.

    The temp directory is cleaned up automatically by the context
    manager whether the upload succeeds or fails. Symlinks (not copies)
    so we don't double-disk multi-GB raw files.
    """
    # Heavy import deferred until we actually need the uploader.
    try:
        from aalibrary.egress import (
            upload_local_echosounder_files_from_directory_to_gcp_storage_bucket,
        )
    except Exception as e:
        logger.exception(f"Failed to import aalibrary.egress: {e}")
        sys.exit(1)

    if input_path.is_dir():
        upload_dir = input_path
        cleanup_temp = None
        logger.info(
            f"Uploading directory '{upload_dir}' as echosounder files "
            f"(ship={ship_name}, survey={survey_name}, "
            f"sonar={sonar_model}, source={data_source})."
        )

        if dry_run:
            # Preview the files the directory uploader would consider.
            _log_dry_run_preview(upload_dir)
            return

        upload_local_echosounder_files_from_directory_to_gcp_storage_bucket(
            local_echosounder_directory_to_upload=str(upload_dir),
            ship_name=ship_name,
            survey_name=survey_name,
            echosounder=sonar_model,
            data_source=data_source,
            gcp_bucket=gcp_bucket,
            debug=debug,
        )
        return

    # ---- Single-file path -----------------------------------------
    # Wrap in a temp directory + symlink and let the directory uploader
    # do its thing. Symlink not copy: avoids double-disking large .raw
    # files. The directory uploader's blob.upload_from_filename ends up
    # calling the OS open() which follows symlinks.
    logger.info(
        f"Uploading single file '{input_path.name}' as echosounder "
        f"(ship={ship_name}, survey={survey_name}, "
        f"sonar={sonar_model}, source={data_source}). "
        "Using a temp directory wrapper so path conventions stay "
        "owned by aalibrary."
    )

    if dry_run:
        logger.info(
            f"[dry-run] Would symlink '{input_path}' into a temp "
            "directory and call "
            "upload_local_echosounder_files_from_directory_to_gcp_storage_bucket."
        )
        return

    with tempfile.TemporaryDirectory(prefix="aa-upload-") as tmp:
        tmp_path = Path(tmp)
        link_path = tmp_path / input_path.name
        try:
            os.symlink(input_path, link_path)
            logger.debug(f"Symlinked {input_path} -> {link_path}")
        except OSError as e:
            # Symlink can fail on some filesystems (Windows w/o admin,
            # some FUSE mounts). Fall back to a hardlink, then copy.
            logger.debug(
                f"symlink failed ({e}); falling back to hardlink/copy."
            )
            try:
                os.link(input_path, link_path)
            except OSError:
                import shutil
                shutil.copy2(input_path, link_path)
                logger.debug(f"Copied {input_path} -> {link_path}")

        upload_local_echosounder_files_from_directory_to_gcp_storage_bucket(
            local_echosounder_directory_to_upload=str(tmp_path),
            ship_name=ship_name,
            survey_name=survey_name,
            echosounder=sonar_model,
            data_source=data_source,
            gcp_bucket=gcp_bucket,
            debug=debug,
        )


def _upload_as_is(
    local_folder: Path,
    destination_prefix: str,
    gcp_bucket,
    dry_run: bool,
) -> None:
    """As-is folder upload via aalibrary.egress.upload_folder_as_is_to_gcp."""
    try:
        from aalibrary.egress import upload_folder_as_is_to_gcp
    except Exception as e:
        logger.exception(f"Failed to import aalibrary.egress: {e}")
        sys.exit(1)

    logger.info(
        f"Uploading folder '{local_folder}' as-is "
        f"under prefix '{destination_prefix}'."
    )

    if dry_run:
        _log_dry_run_preview(local_folder)
        return

    upload_folder_as_is_to_gcp(
        local_folder_path=str(local_folder),
        gcp_bucket=gcp_bucket,
        destination_prefix=destination_prefix,
    )


def _log_dry_run_preview(directory: Path, max_files: int = 20) -> None:
    """List the first N files under `directory` for dry-run feedback.

    Echosounder-extension files are flagged so the user can see what
    the directory uploader's extension filter would catch.
    """
    files = sorted(p for p in directory.rglob("*") if p.is_file())
    total = len(files)
    if total == 0:
        logger.warning(f"[dry-run] '{directory}' contains no files.")
        return

    eligible = [p for p in files if p.suffix.lower() in ECHOSOUNDER_EXTENSIONS]
    logger.info(
        f"[dry-run] Found {total} file(s) under '{directory}'; "
        f"{len(eligible)} match echosounder extensions "
        f"{sorted(ECHOSOUNDER_EXTENSIONS)}."
    )
    for p in files[:max_files]:
        rel = p.relative_to(directory)
        marker = "*" if p.suffix.lower() in ECHOSOUNDER_EXTENSIONS else " "
        logger.info(f"[dry-run] {marker} {rel}")
    if total > max_files:
        logger.info(f"[dry-run]   ... and {total - max_files} more.")


if __name__ == "__main__":
    main()