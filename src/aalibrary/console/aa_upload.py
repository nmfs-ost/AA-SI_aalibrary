#!/usr/bin/env python3
"""
aa-upload

Console tool for uploading products, echosounder files or arbitrary
folders to a GCP storage bucket.

Three upload modes:

  0. gs:// destination (a gs:// URI among the arguments) — uploads each
     input to that URI or prefix with the core's publish(): the
     <file>.aa.json sidecar goes along, the object gets custom metadata
     aa-product-hash / aa-base / aa-tool, and an object that already holds
     the same product (or the same bytes) is not uploaded again. Prints the
     gs:// URI of each object (--tee: the local path instead):

       aa-download gs://b/raw/x.raw | aa-nc --sonar_model EK60 | aa-sv \
         | aa-clean | aa-upload gs://b/derived/me/

  The two original modes, unchanged (they print the input path back to
  stdout so aa-upload can sit *between* stages as a side-effect tee):

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

  2. As-is mode (--as-is) — wraps aalibrary.egress.upload_folder_as_is_to_gcp
     for directories, or aalibrary.utils.cloud_utils.upload_file_to_gcp_bucket
     for single files. Uploads under --destination_prefix verbatim. No
     structure enforcement, no metadata flags. Use this for one-off
     dumps (region files, scratch data, etc.) that don't need to be
     retrievable through aalibrary's ship/survey/echosounder views.

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
import contextlib
import json
import os
import pprint
import signal
import tempfile
from pathlib import Path
from typing import Optional

from aalibrary.console._core import (
    Help, ToolSpec, provenance, identity, render, show_help, stdio, uris,
)

SPEC = ToolSpec(name="aa-upload", role="sink", engines=())

HELP = Help(
    summary="Upload files and products to a GCS bucket.",
    does=(
        "With a gs:// destination (aa-upload [FILE ...] gs://bucket/prefix/): "
        "uploads each input there, with its .aa.json sidecar, stamps the "
        "product hash into the object's metadata, and skips objects that "
        "already hold the same product or the same bytes. Folders (.zarr "
        "stores) are uploaded recursively.\n\n"
        "Without one, the original modes: echosounder mode keeps aalibrary's "
        "data/raw/<ship>/<survey>/<sonar>/ layout (needs --ship_name, "
        "--survey_name, --sonar_model); --as-is uploads under "
        "--destination_prefix in the configured bucket."
    ),
    stdin=("Paths (or gs:// URIs to copy between buckets), one per line, when "
           "no input argument is given. aa/1 JSON handles work too."),
    stdout=("gs:// destination: the gs:// URI of each uploaded (or already "
            "present) object; with --tee the local path instead. Original modes: "
            "the input path, unchanged."),
    options=[
        ("gs://BUCKET/PREFIX/", "destination; ending in / (or several inputs, or a "
                                "folder) means 'put it under this prefix'; a name "
                                "with an extension is the exact object"),
        ("--tee", "gs:// mode: print the local path, so the pipe continues locally"),
        ("--force", "gs:// mode: upload even if the object is identical"),
        ("--dry-run", "show what would be uploaded; upload nothing"),
        ("--ship_name/--survey_name/--sonar_model", "echosounder mode (all three)"),
        ("--as-is --destination_prefix PFX", "as-is mode"),
        ("--gcp_env prod|dev, --project_id, --gcp_bucket_name",
         "which project/bucket the original modes use"),
    ],
    files=(
        "Reads local files and folders (or gs:// objects). Writes gs:// objects "
        "with your Application Default Credentials (billing project: --project_id, "
        "else the project aalibrary is configured for). A .zarr store uploaded over "
        "an older one replaces it completely (objects it no longer has are removed); "
        "other folders only add and update files."
    ),
    pipeline=(
        "The last stage (gs:// mode prints the URIs, which aa-metadata, aa-graph "
        "or aa-download accept), or a tee between stages with --tee or in the "
        "original modes."
    ),
    examples=[
        "aa-nc x.raw --sonar_model EK60 | aa-sv | aa-clean | aa-upload gs://bucket/derived/me/",
        "aa-upload ./HB1603/EK60 --ship_name Henry_B._Bigelow --survey_name HB1603 --sonar_model EK60",
    ],
)


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
    """Curated help (also used by the docs generator)."""
    sys.stdout.write(render(SPEC, HELP, None))


def print_help_full() -> None:
    help_text = """
    Usage: aa-upload [OPTIONS] [PATH ...] gs://BUCKET/PREFIX/   (gs:// destination)
           aa-upload [OPTIONS] [PATH]                         (original modes)

    gs:// destination mode (a gs:// URI among the arguments):
      Each PATH (or each stdin line) is uploaded to the URI: into it when it
      ends in '/', when there are several inputs or the input is a folder;
      as that exact object otherwise (a name with an extension). The
      <file>.aa.json sidecar is uploaded too, and the object gets custom
      metadata aa-product-hash / aa-base / aa-tool from the file's
      provenance. An object that already holds the same product (same
      aa-product-hash) or the same bytes (same MD5) is not uploaded again
      (--force uploads anyway). Prints the gs:// URI of each object, or the
      local path with --tee. --ship_name/--as-is etc. do not apply.

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
      --as-is, --as_is            Upload the input verbatim to GCP under
                                  --destination_prefix. Accepts EITHER
                                  a single file or a directory:
                                    - File      -> blob path is
                                                   <destination_prefix>/<filename>
                                                   (via cloud_utils'
                                                   upload_file_to_gcp_bucket).
                                    - Directory -> uploaded via
                                                   egress.upload_folder_as_is_to_gcp.
                                  No ship/survey/echosounder metadata
                                  required — the prefix you supply IS
                                  the path layout.
      --destination_prefix PFX    Bucket-relative prefix to drop the
                                  file or folder under (e.g. other/scratch/).
                                  REQUIRED in as-is mode. Trailing slash
                                  is normalized.

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
      --tee                       gs:// mode: print the local path instead of
                                  the object URI.
      --force                     gs:// mode: upload even if identical.
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

      # Upload a single arbitrary file (e.g. a region file):
      aa-upload region.evr --as-is \\
        --destination_prefix HDD/Henry_B_Bigelow/HB1603/Echosounder/Data/Evr/

      # Dry-run before a long upload:
      aa-upload ./big_dir --ship_name X --survey_name Y \\
        --sonar_model EK80 --dry-run
    """
    print(help_text)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Upload a file or directory to GCP via aalibrary.egress.",
        add_help=False,
    )

    parser.add_argument(
        "paths",
        type=str,
        nargs="*",
        help="File or directory to upload, and/or a gs:// destination.",
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
    parser.add_argument("--tee", action="store_true", default=False,
                        help="gs:// mode: print the local path instead of the URI.")
    parser.add_argument("--force", action="store_true", default=False,
                        help="gs:// mode: upload even if the object is identical.")
    return parser


def main() -> None:
    # A bare command on a terminal: help. An empty pipe is an error (the
    # previous stage failed); printing help to stdout would feed it onward.
    if len(sys.argv) == 1 and not stdio.stdin_is_piped():
        print_help()
        sys.exit(0)

    parser = _build_parser()
    if show_help(SPEC, HELP, None, full=print_help_full):
        sys.exit(0)

    args = parser.parse_args()

    if args.debug and args.quiet:
        logger.error("Use --debug OR --quiet, not both.")
        sys.exit(2)

    _configure_logging(args.quiet, args.debug)

    if any(uris.is_gcs(p) for p in args.paths):
        # gs:// destination mode (the original modes never took gs:// paths).
        # The destination is the last argument.
        dest = args.paths[-1] if uris.is_gcs(args.paths[-1]) else None
        if dest is None:
            logger.error("Put the gs:// destination last: aa-upload [FILE ...] gs://bucket/prefix/")
            sys.exit(2)
        sources = args.paths[:-1]
        _main_gcs(args, sources, dest)
        return

    if len(args.paths) > 1:
        logger.error("Original modes take one PATH. To upload several files, give a "
                     "gs:// destination: aa-upload FILE ... gs://bucket/prefix/")
        sys.exit(2)
    args.path = args.paths[0] if args.paths else None

    # ---------------------------
    # Resolve input path (stdin fallback, basename behavior NOT applied
    # — unlike aa-ed, here the directory portion IS meaningful: we
    # need the actual filesystem location to read bytes from)
    # ---------------------------
    if args.path is None:
        args.path = stdio.one_input(None, SPEC.name)
        logger.info(f"Read path from stdin: {args.path}")
    else:
        args.path = stdio.normalize_token(args.path) or args.path

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
        # --as-is now accepts either a file or a directory. For a single
        # file the blob path is just `<prefix>/<filename>` — there's no
        # path convention to preserve, since --as-is is by definition
        # convention-free. The dispatch in _upload_as_is handles both.
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
        with contextlib.redirect_stdout(sys.stderr):
            _dispatch(mode, args, input_path, gcp_bucket)
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
    stdio.emit(input_path)


def _dispatch(mode: str, args, input_path: Path, gcp_bucket) -> None:
    """Run the original-mode upload (called with library stdout on stderr)."""
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


# --------------------------------------------------------------------------- #
# gs:// destination mode
# --------------------------------------------------------------------------- #
def _object_uri(dest: str, source_name: str, *, into: bool) -> str:
    return uris.join(dest, source_name) if into else dest


def _looks_like_object(dest: str) -> bool:
    """'gs://b/p/x.nc' names an object; 'gs://b/p/' and 'gs://b/p' a prefix."""
    _, key = uris.parse_gcs(dest)
    last = key.rsplit("/", 1)[-1]
    return bool(last) and not key.endswith("/") and "." in last


def _remote_zarr_hash(uri: str) -> Optional[str]:
    """aa_product_hash in a remote Zarr store's root attributes, if any."""
    bucket, key = uris.parse_gcs(uri)
    for meta in ("zarr.json", ".zattrs"):
        info = uris.backend().stat(bucket, f"{key.rstrip('/')}/{meta}")
        if info is None:
            continue
        with tempfile.TemporaryDirectory(prefix="aa-upload-") as tmp:
            local = Path(tmp) / meta
            uris.backend().download(bucket, info.key, local)
            data = json.loads(local.read_text(encoding="utf-8"))
        attrs = data.get("attributes", data) if meta == "zarr.json" else data
        value = attrs.get(provenance.ATTR_HASH)
        return str(value) if value else None
    return None


def _already_there(local: Path, uri: str, prov: Optional[dict]) -> bool:
    """Does the object already hold this product (or these exact bytes)?"""
    product = (prov or {}).get("product") or {}
    derived = bool(prov) and product.get("role") != "source"
    if local.is_dir():
        return derived and _remote_zarr_hash(uri) == product.get("hash")
    info = uris.stat(uri)
    if info is None:
        return False
    remote_id = identity.gcs_identity(info.md5, info.size)
    if remote_id is not None and remote_id == identity.file_identity(local):
        return True                       # byte-identical
    recorded_md5 = info.metadata.get(uris.META_MD5)
    if recorded_md5 and info.md5 and recorded_md5 != info.md5:
        return False                      # rewritten since it was published
    return derived and info.metadata.get(uris.META_HASH) == product.get("hash")


def _held_product(uri: str) -> Optional[str]:
    """The product hash an existing object claims, if any (for the replace note)."""
    try:
        info = uris.stat(uri)
    except Exception:
        return None
    return (info.metadata.get(uris.META_HASH) if info is not None else None) or None


def _note(args, msg: str) -> None:
    if not args.quiet:
        print(f"aa-upload: {msg}", file=sys.stderr)


def _main_gcs(args, sources: list, dest: str) -> None:
    """Upload each input to the gs:// destination; print the object URIs."""
    if args.as_is or args.destination_prefix or args.ship_name or args.survey_name \
            or args.sonar_model or args.gcp_env or args.gcp_bucket_name:
        logger.error("With a gs:// destination the URI decides the bucket and path; "
                     "--as-is/--destination_prefix/--ship_name/--survey_name/--sonar_model/"
                     "--gcp_env/--gcp_bucket_name do not apply.")
        sys.exit(2)
    if args.project_id:
        os.environ["AALIBRARY_GCP_PROJECT_ID"] = args.project_id
    tokens = stdio.many_inputs(sources, SPEC.name)

    # Resolve every input and its object URI first, so two inputs that would
    # land on the same object are refused before anything is uploaded.
    plan = []
    code = 0
    for token in tokens:
        try:
            if uris.is_gcs(token):
                local = uris.localize(token).path
            else:
                local = Path(os.path.abspath(Path(uris.from_file_uri(token)).expanduser()))
                if not local.exists():
                    raise FileNotFoundError(f"no such file: {token}")
        except FileNotFoundError as exc:
            print(f"aa-upload: {exc}", file=sys.stderr)
            code = 1
            continue
        except Exception as exc:  # credentials, network
            print(f"aa-upload: cannot read {token}: {exc}", file=sys.stderr)
            code = 1
            continue
        into = (len(tokens) > 1 or local.is_dir() or not _looks_like_object(dest))
        name = uris.basename(token) if uris.is_gcs(token) else local.name
        plan.append((token, local, _object_uri(dest, name, into=into)))
    seen: dict = {}
    for token, _, uri in plan:
        if uri in seen:
            stdio.fail(SPEC.name, f"{seen[uri]} and {token} would both be uploaded to {uri}; "
                                  "upload them separately or rename one", 2)
        seen[uri] = token

    for token, local, uri in plan:
        try:
            prov = provenance.read(local)
            product = (prov or {}).get("product") or {}
            derived = bool(prov) and product.get("role") != "source"
            if not args.force and _already_there(local, uri, prov):
                what = (f"product aa:{str(product.get('hash', ''))[:8]}"
                        if derived else "file (same MD5)")
                _note(args, f"reusing {uri} (it already holds this {what}; "
                            "--force uploads again)")
            elif args.dry_run:
                size = (sum(p.stat().st_size for p in local.rglob("*") if p.is_file())
                        if local.is_dir() else local.stat().st_size)
                _note(args, f"[dry-run] would upload {local} -> {uri} ({size:,} bytes)")
            else:
                metadata = None
                if derived:
                    metadata = {
                        uris.META_HASH: product.get("hash", ""),
                        uris.META_BASE: prov.get("base", ""),
                        uris.META_TOOL: ((prov.get("pipeline") or [{}])[-1]).get("tool", ""),
                    }
                    if product.get("recipe"):
                        metadata[uris.META_RECIPE] = product["recipe"]
                if local.is_file():
                    held = _held_product(uri)
                    if held and held != product.get("hash"):
                        _note(args, f"replacing {uri} (it held product aa:{held[:8]})")
                    if metadata is not None:
                        metadata[uris.META_MD5] = identity.md5_b64(local)
                    with contextlib.redirect_stdout(sys.stderr):
                        uris.publish(local, uri, metadata=metadata, keep_in_cache=False)
                    _note(args, f"uploaded {local} -> {uri}")
                else:
                    with contextlib.redirect_stdout(sys.stderr):
                        up, same, gone = uris.publish_tree(local, uri, metadata=metadata)
                    _note(args, f"uploaded {local} -> {uri}: {up} files uploaded, "
                                f"{same} unchanged" + (f", {gone} stale objects removed"
                                                       if gone else ""))
            stdio.emit(local if args.tee else uri)
        except SystemExit:
            raise
        except FileNotFoundError as exc:
            print(f"aa-upload: {exc}", file=sys.stderr)
            code = 1
        except Exception as exc:  # credentials, permissions, network
            print(f"aa-upload: cannot upload {token}: {exc}", file=sys.stderr)
            code = 1
    sys.exit(code)


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
    """As-is upload to GCP. Despite the parameter name (kept for
    backwards compatibility), local_folder may be either a file or a
    directory:

      - Directory -> aalibrary.egress.upload_folder_as_is_to_gcp.
      - File      -> aalibrary.utils.cloud_utils.upload_file_to_gcp_bucket,
                     blob path = `<destination_prefix>/<filename>`.

    The file branch deliberately uses the low-level upload primitive
    rather than wrapping the file in a temp directory: --as-is is
    convention-free, so the blob path is fully determined by the
    user's prefix and there's nothing aalibrary needs to "decide"
    about the layout.
    """
    if local_folder.is_file():
        # ---- Single-file as-is ----------------------------------
        try:
            from aalibrary.utils.cloud_utils import upload_file_to_gcp_bucket
        except Exception as e:
            logger.exception(
                f"Failed to import aalibrary.utils.cloud_utils: {e}"
            )
            sys.exit(1)

        # Normalize trailing slash so "other/scratch" and
        # "other/scratch/" both produce ".../scratch/filename" rather
        # than ".../scratchfilename".
        prefix = destination_prefix.rstrip("/") + "/"
        blob_path = f"{prefix}{local_folder.name}"

        logger.info(
            f"Uploading single file '{local_folder.name}' as-is to "
            f"blob '{blob_path}' ({local_folder.stat().st_size} bytes)."
        )

        if dry_run:
            logger.info(
                f"[dry-run] Would upload '{local_folder}' -> '{blob_path}'."
            )
            return

        upload_file_to_gcp_bucket(
            bucket=gcp_bucket,
            blob_file_path=blob_path,
            local_file_path=str(local_folder),
            debug=False,
        )
        return

    # ---- Directory as-is ----------------------------------------
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