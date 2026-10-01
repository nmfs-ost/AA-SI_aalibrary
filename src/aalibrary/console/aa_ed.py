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
    input  : a raw file name as positional arg or via stdin (a path,
             file:// or gs:// URI, or an aa/1 JSON handle also work)
    output : .nc file on disk; absolute path printed to stdout
    logs   : stderr via loguru

Provenance: the .nc records what it was made from (the .raw's content
identity and, for an NCEI download, the s3://noaa-wcsd-pds/... object it
came from, kept in a <file>.raw.aa.json sidecar beside the download),
the sonar model, the base name, and its product hash. See aa-metadata.

Idempotency: an existing .nc is reused, with no conversion, when
  - it holds the same product (same raw content, same sonar model);
  - in NCEI mode, its recorded origin is the same NCEI object with the
    same sonar model: then there is no BigQuery lookup and no download;
  - it was made before provenance existed (no aa provenance at all):
    it is reused by name, as before, with a note on stderr.
A .nc that holds a different product is converted again. If only the
.raw exists, the download is skipped but the conversion still runs.
Pass --force to override both checks.

Input shape: aa-ed auto-detects three modes from the input:

  - Bare filename ("HB1603...raw") -> full NCEI flow: BigQuery
    lookup + download + convert. Output: .nc absolute path on stdout.

  - Path to an existing .raw file ("/abs/path/file.raw") -> fully
    offline single-file mode. Sonar model detected from header (or
    --sonar_model), .nc lands next to the .raw, ZERO network calls.
    Output: .nc absolute path on stdout. --force never overwrites
    a user-provided .raw. (A gs:// URI of a .raw works the same way:
    read through a gcsfuse mount or the download cache; the .nc then
    lands in the current directory.)

  - Path to an existing directory ("/abs/path/dir/") -> batch mode.
    Globs *.raw (or **/*.raw with --recursive), converts each via
    the same offline path, passes through standalone .nc files,
    keeps going on per-file failures. Output: the DIRECTORY path
    on stdout (not a list of .nc paths) for aa-combine et al.

Idempotency applies per file in directory mode (a reused .nc counts as
a cache hit). --force overrides.

Typical pipeline usage:
    echo HB1603_L1-D20160703-T183957.raw | aa-ed | aa-sv | aa-graph
    aa-ed HB1603_L1-D20160703-T183957.raw | aa-sv | aa-clean

Cloud output & URI caching (opt-in, fully additive):
    With --gcs-uri gs://bucket/path/file.nc (or --gcs-prefix PFX), aa-ed
    treats that object as a cache for the derived .nc:

      - Before doing any work it checks whether the object already exists.
        If it does, the asset is REUSED instead of recomputed — either
        passed straight through as a gs:// URI (--print-uri), moving no
        bytes, or DOWNLOADED to the local .nc so a not-yet-URI-aware
        pipeline keeps working. This skips the BigQuery lookup, the NCEI
        download, and the echopype conversion entirely.
      - On a miss, aa-ed converts locally exactly as it always has, then
        UPLOADS the resulting .nc to that object using the same GCP
        primitive as aa-upload (aalibrary.utils.cloud_utils.
        upload_file_to_gcp_bucket), so aa-ed and aa-upload write objects
        identically. Bucket / credentials resolve the same way too
        (--gcp_env / --project_id / --gcp_bucket_name / ambient env vars).
        The object also gets custom metadata aa-product-hash / aa-base /
        aa-tool from the .nc's provenance.

    NetCDF is HDF5 underneath and needs a seekable local file, so aa-ed
    writes the .nc to disk first and then PUTs the whole object; the
    local copy is kept unless --cloud-only is given. --force bypasses the
    cache check and regenerates.

    None of this activates unless --gcs-uri or --gcs-prefix is passed —
    every existing local, offline, and directory-batch path below is
    unchanged.

    Cloud example:
        aa-ed HB1603_L1-D20160703-T183957.raw \\
              --gcs-prefix derived/nc/ --gcp_bucket_name my-bucket --print-uri
        # -> gs://my-bucket/derived/nc/HB1603_L1-D20160703-T183957.nc
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
import re
import shutil
import signal
from pathlib import Path
from typing import Optional


# Pipeline tools should die cleanly when the downstream end of the pipe
# closes early (`... | head -n 1`), not throw BrokenPipeError. Guarded
# with hasattr because SIGPIPE doesn't exist on Windows.
if hasattr(signal, "SIGPIPE"):
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)

from aalibrary.console._core import (  # noqa: E402 - after the log silencing above
    Help, Run, ToolSpec, add_common_flags, canon, naming, provenance, record_source,
    render, show_help, stdio, uris,
)

SPEC = ToolSpec(
    name="aa-ed",
    role="echodata",
    kind="echodata",
    op="echopype.open_raw",
    op_version=1,
    # The sonar model actually passed to echopype.open_raw: --sonar_model,
    # else the NCEI cache (bare name) or the file header (local .raw).
    params={"sonar_model": canon.choice("upper")},
    # aa-ed runs exactly aa-nc's computation — echopype.open_raw(raw,
    # sonar_model) then to_netcdf(overwrite=True), same op, op_version and
    # canonical sonar_model (echopype upper-cases it too) — so the two tools'
    # products are the same product: `aa-nc x.raw` then `aa-ed x.raw` reuses
    # x.nc instead of converting it again under a different hash. The step
    # still records tool "aa-ed" (and identity_tool "aa-nc").
    identity="aa-nc",
)

# Where NCEI keeps the raw files (the bucket aalibrary.ingestion reads).
NCEI_PREFIX = "s3://noaa-wcsd-pds/"

HELP = Help(
    summary="Raw file name, path or folder -> EchoData NetCDF (aa-raw + aa-nc in one step).",
    does=(
        "A bare NCEI file name is looked up in the NCEI BigQuery cache (ship, survey, "
        "echosounder), downloaded from NCEI and converted with echopype.open_raw. A "
        "local .raw (or gs:// URI) is converted offline, the sonar model read from its "
        "header. A directory: every .raw in it."
    ),
    stdin=(
        "One token (argument or first stdin line): a bare file name, a .raw path, a "
        "directory, a file:// or gs:// URI, or an aa/1 JSON handle. An empty pipe is "
        "an error (exit 1)."
    ),
    stdout=(
        "The .nc's absolute path; its gs:// URI with --print-uri/--cloud-only or "
        "-o/--dest gs://...; the directory itself in directory mode."
    ),
    metadata=(
        "Starts the provenance chain: the .nc records the .raw's identity, the sonar "
        "model and the base name (aa_provenance, aa_product_hash, aa_base). An NCEI "
        "download gets a <file>.raw.aa.json sidecar with its origin "
        "(s3://noaa-wcsd-pds/data/raw/...), which the .nc records too."
    ),
    options=[
        ("FILE_NAME | PATH.raw | DIR", "what to convert (mode is auto-detected)"),
        ("-o, --output_path PATH", "the .nc (suffix forced to .nc); local or gs://"),
        ("--file_download_directory DIR", "where NCEI downloads land (default: .)"),
        ("--ship_name/--survey_name/--sonar_model", "override the lookup; all three "
                                                    "skip BigQuery"),
        ("-r, --recursive", "directory mode: include subfolders"),
        ("--cleanup-raw", "delete the downloaded .raw after converting"),
        ("--gcs-uri URI | --gcs-prefix P", "use a bucket object as a cache of the .nc "
                                           "(see --help-all)"),
        ("-f, --force", "download and convert again"),
    ],
    science={"sonar_model": "Which echopype parser reads the file: --sonar_model, else "
                            "the NCEI cache (bare name) or the .raw header."},
    files=(
        "Writes <raw stem>.nc beside the .raw (NCEI: in --file_download_directory; "
        "gs:// raw: the current directory), or -o / --dest. Reused without converting: "
        "a .nc holding the same product; in NCEI mode one recorded as converted from "
        "the same NCEI object and sonar model (no lookup, no download); one made before "
        "provenance existed (by name, with a note). A different product is converted "
        "again."
    ),
    pipeline=(
        "First stage: aa-ed FILE.raw | aa-sv | aa-clean ...  Directory mode feeds "
        "aa-combine: aa-ed ./raw/ | aa-combine -o survey.zarr"
    ),
    examples=[
        "aa-ed HB1603_L1-D20160703-T183957.raw | aa-sv",
        "aa-ed ./data/D20160703-T060000.raw          # offline, .nc beside the .raw",
        "aa-ed ./raw/ | aa-combine -o HB1603_L1.zarr",
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
                                    ALONGSIDE the .raw. A gs:// URI of a
                                    .raw works the same way (read through
                                    a gcsfuse mount or the download
                                    cache); its .nc goes to the current
                                    directory.

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

                                  Optional; falls back to the first line
                                  of stdin if not provided (a path, a
                                  file:// or gs:// URI, or an aa/1 JSON
                                  handle). An empty pipe is an error
                                  (exit 1): the previous stage failed.

    Optional:
      -o, --output_path PATH      Path to save the converted NetCDF output
                                  (its suffix is forced to .nc). May be a
                                  gs:// URI: written locally, then uploaded.
                                  Default: same directory as the downloaded
                                  .raw, named <raw stem>.nc.

      --base NAME                 Base name of the output (<NAME>.nc) and of
                                  every product derived from it downstream.
      --dest DIR|gs://PREFIX      Write <base>.nc there instead of beside the
                                  .raw. (Not in directory mode.)

      --file_download_directory PATH
                                  Where to download the .raw to.
                                  Default: current directory. Created if it
                                  doesn't exist.

      --ship_name NAME            Override the ship_name lookup
                                  (e.g. Henry_B._Bigelow).
      --survey_name NAME          Override the survey_name lookup
                                  (e.g. HB1603).
      --sonar_model NAME          Override the echosounder lookup
                                  (e.g. EK60, EK80). This is the one option
                                  that changes the product (hash).

                                  If all three overrides are provided, aa-ed
                                  skips the NCEI cache lookup entirely. Use
                                  this when BigQuery is unreachable or to
                                  disambiguate a file name that collides
                                  across multiple surveys.

      --cleanup-raw               Delete the downloaded .raw (and its
                                  .aa.json sidecar) after the .nc is
                                  produced. Off by default — the .raw is
                                  source data and is kept so re-running
                                  aa-ed (or aa-nc directly) is free.

      --force, -f                 Re-download and re-convert even when the
                                  .raw / .nc are already on disk. Default
                                  behavior is to treat both as cached: an
                                  existing .nc that holds the same product
                                  short-circuits everything (in NCEI mode a
                                  .nc recorded as converted from the same
                                  NCEI object and sonar model skips the
                                  BigQuery lookup and the download; a .nc
                                  made before provenance existed is reused
                                  by name, with a note), and an existing
                                  .raw skips the NCEI download. A .nc that
                                  holds a different product is converted
                                  again. Use --force if you suspect a
                                  cached file is stale or corrupt.

      --upload_to_gcp             Also upload the downloaded .raw to GCP
                                  (passed through to aalibrary.ingestion).

      --data_source SRC           Currently only 'NCEI' is wired through;
                                  other values log a warning and proceed
                                  as NCEI.

    Cloud output & URI caching (opt-in; all off by default):
      --gcs-uri URI               Use gs://bucket/path/file.nc as a cache for
                                  the derived .nc. If the object already
                                  exists it is reused instead of recomputed
                                  (downloaded, or passed through with
                                  --print-uri); on a miss the new .nc is
                                  uploaded here after conversion, using the
                                  same GCP primitive as aa-upload, and the
                                  object gets aa-product-hash / aa-base /
                                  aa-tool custom metadata.

      --gcs-prefix PREFIX         Like --gcs-uri, but aa-ed names the object
                                  <prefix>/<stem>.nc; the bucket comes from
                                  --gcp_bucket_name / --gcp_env / env.
                                  Mutually exclusive with --gcs-uri.

      --print-uri                 With a GCS destination set, print the gs://
                                  URI on stdout instead of the local path. On
                                  a cache hit no download happens — the URI
                                  is passed straight through.

      --cloud-only                Delete the local .nc after a successful
                                  upload (keeps local storage minimal).
                                  Implies --print-uri.

      --gcp_env {prod,dev}        Select the aalibrary GCP env for cloud
                                  output (mirrors aa-upload).
      --project_id ID             Explicit GCP project id (overrides --gcp_env).
      --gcp_bucket_name NAME      Explicit GCP bucket (overrides --gcp_env;
                                  ignored if a bucket is given in --gcs-uri).

      --debug                     Verbose logging (DEBUG level on stderr).
      --quiet                     Suppress INFO logs; final path still
                                  prints on stdout.

      -h, --help                  Show the short help and exit.
      --help-all                  Show this reference and exit.

    Description:
      Resolves a raw file's ship/survey/echosounder by querying the NCEI
      BigQuery cache, downloads the .raw from NCEI, and converts it to a
      multi-group NetCDF EchoData file with echopype.open_raw /
      EchoData.to_netcdf. The .nc absolute path is printed on stdout,
      ready for piping into aa-sv and onward. Provenance (the .raw's
      identity and NCEI origin, sonar model, base name, product hash) is
      embedded in the .nc; see aa-metadata.

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

    Cloud example (reuse-if-exists, else convert-and-upload):
      aa-ed HB1603_L1-D20160703-T183957.raw \\
            --gcs-prefix derived/nc/ --gcp_bucket_name my-bucket --print-uri
      # hit  -> prints gs://my-bucket/derived/nc/HB1603...nc (no work done)
      # miss -> converts locally, uploads, prints the same gs:// URI
    """
    print(help_text)


def _build_parser() -> argparse.ArgumentParser:
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
        # str, not Path: a gs:// URI must survive parsing.
        type=str,
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

    # ------------------------------------------------------------------
    # Cloud output & URI caching (additive). All off by default; when
    # none of these are set, aa-ed behaves exactly as before — local .nc
    # written to disk, local path printed on stdout, zero GCP imports.
    # ------------------------------------------------------------------
    parser.add_argument(
        "--gcs-uri", "--gcs_uri",
        dest="gcs_uri",
        default=None,
        help="Full gs://bucket/path/file.nc destination for the derived "
             ".nc, used as a cache. If the object already exists it is "
             "reused (downloaded, or passed through with --print-uri) "
             "instead of recomputed; on a miss the freshly-made .nc is "
             "uploaded here after conversion.",
    )
    parser.add_argument(
        "--gcs-prefix", "--gcs_prefix",
        dest="gcs_prefix",
        default=None,
        help="Bucket-relative prefix; the derived object becomes "
             "<prefix>/<stem>.nc. The bucket comes from --gcp_bucket_name "
             "/ --gcp_env / ambient env. Mutually exclusive with --gcs-uri.",
    )
    parser.add_argument(
        "--print-uri", "--print_uri",
        dest="print_uri",
        action="store_true",
        default=False,
        help="When a GCS destination is set, print the gs:// URI on stdout "
             "instead of the local path. On a cache hit this moves no bytes "
             "— the URI is passed straight through.",
    )
    parser.add_argument(
        "--cloud-only", "--cloud_only",
        dest="cloud_only",
        action="store_true",
        default=False,
        help="After a successful upload, delete the local .nc so the "
             "workstation keeps minimal storage. Implies --print-uri "
             "(no local file is left to hand downstream).",
    )
    parser.add_argument(
        "--gcp_env",
        choices=["prod", "dev"],
        default=None,
        help="Switch the aalibrary GCP env before any cloud operation "
             "(mirrors aa-upload).",
    )
    parser.add_argument(
        "--project_id",
        default=None,
        help="Explicit GCP project id for cloud output (overrides --gcp_env).",
    )
    parser.add_argument(
        "--gcp_bucket_name",
        default=None,
        help="Explicit GCP bucket name for cloud output (overrides "
             "--gcp_env; ignored if a bucket is given inside --gcs-uri).",
    )
    # --base / --dest (aa-ed already has its own --force).
    add_common_flags(parser)
    return parser


# ============================================================
# Provenance, naming and reuse (shared core)
# ============================================================

def _canon_sonar(value):
    """The hash's spelling of a sonar model (EK60 == ek60)."""
    return SPEC.params["sonar_model"](value)


def _note(args, message: str) -> None:
    """One line on stderr, like the core's "reusing ..." line (not under --quiet)."""
    if not getattr(args, "quiet", False):
        print(f"{SPEC.name}: {message}", file=sys.stderr)


def _explicit_nc(args) -> Optional[str]:
    """-o as it always worked: the suffix forced to .nc. May be a gs:// URI."""
    if not args.output_path:
        return None
    return naming.with_ext(uris.from_file_uri(str(args.output_path)), ".nc")


def _planned_target(args, default_dir: Path, file_name: str) -> str:
    """Where the .nc goes, decided before the .raw is at hand.

    The rule Run.plan applies — -o, else --dest, else the standard name
    <base>.nc beside the .raw (AA_NAMING=legacy: <raw stem>.nc) — computed
    without the input, so an existing .nc can be checked before any lookup
    or download. It is then handed to plan() as the explicit target, so the
    two can never disagree.
    """
    explicit = _explicit_nc(args)
    if explicit is not None:
        if uris.is_remote(explicit):
            return explicit
        return str(Path(explicit).expanduser().resolve())
    base = naming.sanitize_base(args.base) if args.base else naming.base_of(file_name)
    name = naming.echodata_name(base, ".nc")
    if args.dest:
        if uris.is_remote(args.dest):
            return uris.join(args.dest, name)
        return str((Path(args.dest).expanduser() / name).resolve())
    if naming.mode() == "legacy":
        return str((Path(default_dir) / file_name).with_suffix(".nc").resolve())
    return str((Path(default_dir) / name).resolve())


def _default_nc_path(raw_path: Path) -> Path:
    """The .nc a .raw converts to by default: <base>.nc beside it, where the
    base is the raw file's stem (AA_NAMING=legacy: <raw stem>.nc). The same
    rule Run.plan applies; directory mode passes it as the explicit target so
    its collision check and its conversions use one list of names."""
    if naming.mode() == "legacy":
        return raw_path.with_suffix(".nc")
    return raw_path.parent / naming.echodata_name(naming.base_of(raw_path.name), ".nc")


def _recorded_sonar(doc: dict) -> Optional[str]:
    """The sonar model an existing .nc was converted with (from its provenance)."""
    step = (doc.get("pipeline") or [{}])[-1]
    if step.get("op") != SPEC.op:
        return None
    return (step.get("params") or {}).get("sonar_model")


def _ship_key(name) -> str:
    return re.sub(r"[^a-z0-9]", "", str(name or "").lower())


def _ncei_origin_match(doc: dict, args) -> Optional[str]:
    """The NCEI origin an existing .nc records, when it is the object this run
    would download and the .nc was converted with the same sonar model.

    Same object: the key ends in this file name, and the ship / survey /
    echosounder in it agree with any overrides given. Same sonar model:
    --sonar_model when given, else the echosounder in the recorded key (which
    is what the BigQuery lookup would return). None when they do not match.
    """
    recorded = _recorded_sonar(doc)
    if recorded is None:
        return None
    for item in doc.get("inputs") or []:
        origin = str(item.get("origin") or "")
        if not origin.startswith(NCEI_PREFIX):
            continue
        parts = origin[len(NCEI_PREFIX):].split("/")
        if parts[-1] != args.file_name or len(parts) < 4:
            continue
        ship, survey, echosounder = parts[-4], parts[-3], parts[-2]
        if args.ship_name and _ship_key(ship) != _ship_key(args.ship_name):
            continue
        if args.survey_name and survey != args.survey_name:
            continue
        if _canon_sonar(args.sonar_model or echosounder) != recorded:
            continue
        return origin
    return None


def _product_run(args, raw_path: Path, sonar_model: str, target: str, *, stage: bool = True):
    """Run + planned output for converting raw_path with sonar_model."""
    run = Run(SPEC, args, params={"sonar_model": _canon_sonar(sonar_model)})
    run.input(str(raw_path))
    return run, run.plan(ext=".nc", explicit=target, stage=stage)


def _ncei_object_key(file_name: str, metadata: dict) -> str:
    """The NCEI object a download came from: the BigQuery row's
    s3_object_key when the lookup ran, else the key aalibrary.ingestion
    builds (data/raw/<NCEI ship>/<survey>/<echosounder>/<file>)."""
    key = metadata.get("s3_object_key")
    if key:
        return str(key).lstrip("/")
    ship = metadata["ship_name"]
    try:
        from aalibrary.utils.helpers import normalize_ship_name
        from aalibrary.utils.ncei_utils import get_closest_ncei_formatted_ship_name

        ship = get_closest_ncei_formatted_ship_name(ship_name=normalize_ship_name(ship)) or ship
    except Exception as exc:  # noqa: BLE001 - the given name is the best fallback
        logger.debug(f"Could not resolve the NCEI ship folder for '{ship}': {exc}")
    return f"data/raw/{ship}/{metadata['survey_name']}/{metadata['sonar_model']}/{file_name}"


def _object_metadata(nc_path: Path) -> dict:
    """aa-product-hash / aa-base / aa-tool for a bucket object, from the .nc."""
    doc = provenance.read(nc_path) if nc_path.exists() else None
    if not doc:
        return {}
    return {
        uris.META_HASH: doc["product"]["hash"],
        uris.META_BASE: doc.get("base", ""),
        uris.META_TOOL: (doc.get("pipeline") or [{}])[-1].get("tool", SPEC.name),
        **({uris.META_RECIPE: doc["product"]["recipe"]}
           if doc["product"].get("recipe") else {}),
    }


def _deliver(args, target: str, cloud: Optional[dict], *, existing: bool = False) -> None:
    """Hand the result to the next stage, exactly as before.

    No cloud cache: print the .nc's absolute path (or its gs:// URI when -o /
    --dest was one). Cloud cache (--gcs-uri / --gcs-prefix): upload the local
    .nc to the object (registering an existing one), then print the gs:// URI
    with --print-uri / --cloud-only, else the local path.
    """
    if cloud is None:
        stdio.emit(target)
        return
    nc_path = Path(target)
    _gcs_upload_nc(cloud["bucket"], cloud["blob"], nc_path, args.debug,
                   metadata=_object_metadata(nc_path))
    if existing:
        logger.success(f"Registered existing .nc in the bucket: {cloud['uri']}")
    else:
        logger.success(f"Uploaded derived .nc to {cloud['uri']}")
    if args.cloud_only:
        _maybe_remove_local(nc_path)
    logger.info("Passing reference to stdout...")
    print(cloud["uri"] if (args.print_uri or args.cloud_only) else nc_path.resolve())


def _cleanup_raw(args, raw_path: Path, user_owned: bool) -> None:
    """--cleanup-raw: delete aa-ed's own download (and its .aa.json sidecar).
    Done after the .nc is confirmed, so a conversion failure never costs the
    user their downloaded raw data."""
    if not args.cleanup_raw:
        return
    if user_owned:
        # The "intermediate" .raw was actually the user's source data (a
        # path they gave, or a gs:// object read through a mount/the cache).
        # --cleanup-raw is meant to clean up aa-ed's own downloads only.
        logger.warning(
            f"--cleanup-raw ignored: '{raw_path}' was provided by "
            "the user, not downloaded by aa-ed. Delete it manually "
            "if you really want it gone."
        )
        return
    try:
        raw_path.unlink(missing_ok=True)
        provenance.sidecar_path(raw_path).unlink(missing_ok=True)
        logger.info(f"Removed intermediate .raw: {raw_path}")
    except Exception as e:
        # Non-fatal — the .nc still exists and gets printed.
        logger.warning(f"Could not delete '{raw_path}': {e}")


def main() -> None:
    # No args on a terminal: help. (An empty pipe is an error: see below.)
    if len(sys.argv) == 1 and not stdio.stdin_is_piped():
        print_help()
        sys.exit(0)

    parser = _build_parser()
    # -h/--help: the short, curated help. --help-all: the full reference.
    if show_help(SPEC, HELP, parser, full=print_help_full):
        sys.exit(0)

    args = parser.parse_args()

    if args.debug and args.quiet:
        logger.error("Use --debug OR --quiet, not both.")
        sys.exit(2)

    _configure_logging(args.quiet, args.debug)

    # ---------------------------
    # Validate input
    # ---------------------------
    # Positional, else the first stdin line (a bare name, a path, a file://
    # or gs:// URI, or an aa/1 JSON handle). An empty pipe exits 1: it means
    # the previous stage failed, and help on stdout would feed the next one.
    from_stdin = stdio.normalize_token(args.file_name) is None
    args.file_name = stdio.one_input(args.file_name, SPEC.name)
    if from_stdin:
        logger.info(f"Read file name from stdin: {args.file_name}")

    # Three acceptable input shapes:
    #   1. Bare file name ("HB1603...raw") — aa-ed will download from
    #      NCEI to --file_download_directory. (NCEI single-file mode)
    #   2. Path to an existing .raw file
    #      ("/home/me/data/HB1603...raw" or "./data/HB1603...raw") —
    #      use it as-is, skip NCEI download and BigQuery entirely.
    #      (Local single-file mode; a gs:// URI of a .raw is read the same
    #      way, through a gcsfuse mount or the download cache.)
    #   3. Path to an existing directory ("/home/me/data/" or "./data/")
    #      — convert every .raw inside (skipping cache hits, passing
    #      through standalone .nc files) and print the directory path
    #      on stdout for aa-combine et al. (Directory batch mode)
    #
    # Mode is auto-detected from the input shape — no flag needed.
    # The directory branch dispatches BEFORE args.file_name gets set
    # to a basename (which would be the directory's name, not a useful
    # value) and before the .raw-extension check (irrelevant for dirs).
    gcs_raw: Optional[str] = None
    user_provided_raw_path: Optional[Path] = None
    if uris.is_gcs(args.file_name):
        gcs_raw = args.file_name
        args.file_name = uris.basename(gcs_raw)
    else:
        _user_input = args.file_name
        _input_path = Path(_user_input).expanduser()
        _has_directory = _user_input != _input_path.name

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
        # stores just the filename.
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

    local_source = user_provided_raw_path is not None or gcs_raw is not None
    if user_provided_raw_path is not None:
        # The .raw is already on disk at the user-specified location;
        # we never need to create a download directory. If the user ALSO
        # passed --file_download_directory, the path-form input wins and
        # we warn rather than silently ignoring it. ("." is the parser
        # default, used as a sentinel for "user didn't actually set it".)
        download_dir = user_provided_raw_path.parent
        if args.file_download_directory != ".":
            logger.warning(
                f"--file_download_directory='{args.file_download_directory}' "
                f"is being ignored: the .raw is already on disk at "
                f"'{user_provided_raw_path}'. Drop the directory "
                "component from the input if you want a fresh download."
            )
    elif gcs_raw is not None:
        # Read through a gcsfuse mount or the download cache; the .nc goes
        # to the current directory (or -o / --dest), as for every aa-* tool
        # whose input came from gs://.
        download_dir = Path.cwd()
        if args.file_download_directory != ".":
            logger.warning(
                f"--file_download_directory='{args.file_download_directory}' "
                f"is being ignored for a gs:// input ({gcs_raw}): it is read "
                "through a gcsfuse mount or the download cache."
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
        "gcs_raw": gcs_raw,
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
        "gcs_uri": args.gcs_uri,
        "gcs_prefix": args.gcs_prefix,
        "print_uri": args.print_uri,
        "cloud_only": args.cloud_only,
        "gcp_env": args.gcp_env,
        "project_id": args.project_id,
        "gcp_bucket_name": args.gcp_bucket_name,
        "base": args.base,
        "dest": args.dest,
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
        raw_path = download_dir / args.file_name  # gs:// input: set once read

    target = _planned_target(args, download_dir, args.file_name)
    remote_target = uris.is_remote(target)
    nc_path: Optional[Path] = None if remote_target else Path(target)

    # Same guard aa-nc has — cheap insurance against -o pointing at the .raw.
    if nc_path is not None and gcs_raw is None and nc_path.resolve() == raw_path.resolve():
        logger.error(f"Refusing to overwrite input file: {raw_path.resolve()}")
        sys.exit(1)

    # ---------------------------
    # Cloud output / URI-cache setup (additive; inert without a
    # --gcs-uri / --gcs-prefix destination)
    # ---------------------------
    # When no cloud destination is set, cloud_target stays None and every
    # branch below is skipped — aa-ed's original local behavior is
    # untouched, and no GCP module is imported.
    if remote_target and (args.gcs_uri or args.gcs_prefix):
        logger.error(
            "-o/--dest gs://... and --gcs-uri/--gcs-prefix both name a bucket "
            "destination for the .nc; use one of them."
        )
        sys.exit(2)
    cloud_target = _resolve_cloud_target(args, nc_path) if nc_path is not None else None
    cloud: Optional[dict] = None
    if cloud_target is not None:
        _bucket_name, blob_path, gcs_uri = cloud_target
        gcp_bucket, _resolved_bucket = _resolve_gcp_bucket(
            gcp_env=args.gcp_env,
            project_id=args.project_id,
            gcp_bucket_name=_bucket_name,
        )
        # --gcs-prefix doesn't carry a bucket, so fold the resolved name
        # back into the URI we print/log.
        if gcs_uri.startswith("gs://<bucket>/"):
            gcs_uri = f"gs://{_resolved_bucket}/{blob_path}"
        cloud = {"bucket": gcp_bucket, "blob": blob_path, "uri": gcs_uri}

        # --- URI cache check: found -> reuse instead of recompute -------
        # This is the whole point of the feature: if someone already made
        # this asset, discover it and reuse it rather than re-running the
        # (expensive) download + conversion. --force bypasses the check.
        if not args.force and _gcs_blob_exists(gcp_bucket, blob_path):
            logger.success(
                f"Cache hit: {gcs_uri} already exists. Skipping BigQuery "
                "lookup, NCEI download, and conversion (pass --force to "
                "regenerate)."
            )
            if args.print_uri or args.cloud_only:
                # URI-aware downstream: hand back the URI, move no bytes.
                print(gcs_uri)
                return
            # Otherwise honor the request literally — fetch the existing
            # asset and continue with a local path so the current
            # (not-yet-URI-aware) pipeline keeps working.
            _gcs_download_blob(gcp_bucket, blob_path, nc_path)
            print(nc_path.resolve())
            return
    elif (args.print_uri or args.cloud_only or args.gcp_env
          or args.project_id or args.gcp_bucket_name):
        # Cloud-ish flags with no destination to act on — warn rather than
        # silently ignoring them, then fall through to local behavior.
        logger.warning(
            "Cloud flags were given but neither --gcs-uri nor --gcs-prefix "
            "was set; cloud output is disabled and aa-ed will behave "
            "locally."
        )

    # ---------------------------
    # Short-circuit: .nc already on disk
    # ---------------------------
    # Conversion is the expensive step (echopype's open_raw on a multi-
    # hundred-MB .raw dwarfs the NCEI download). An existing .nc is handed
    # on without redoing anything when we can tell it is this product:
    #   - it carries no aa provenance (made before provenance existed):
    #     reused by name, exactly as before, with a note;
    #   - NCEI mode: its recorded origin is the same NCEI object with the
    #     same sonar model — no BigQuery lookup, no download;
    #   - NCEI mode with the .raw already on disk: same product hash.
    # Local and gs:// inputs get the product-hash check below, once the
    # sonar model is known (reading a header is cheap and offline).
    # --force overrides all of this.
    logger.debug(
        f"Checking for existing .nc at {target}: "
        f"exists={nc_path.exists() if nc_path is not None else 'remote'}, force={args.force}"
    )
    existing_doc = None
    if nc_path is not None and nc_path.exists() and not args.force:
        existing_doc = provenance.read(nc_path)
        reuse = False
        if existing_doc is None:
            _note(args, f"{nc_path} has no aa provenance (it was made before provenance "
                        "was recorded); reusing it by name, as before, without checking "
                        "what it was made from. --force converts it again and records "
                        "provenance.")
            reuse = True
        elif not local_source:
            origin = _ncei_origin_match(existing_doc, args)
            if origin:
                _note(args, f"reusing {nc_path} (converted from the same NCEI object "
                            f"{origin}, sonar_model {_recorded_sonar(existing_doc)}; no "
                            "lookup or download needed; --force converts again)")
                reuse = True
            elif raw_path.exists():
                sonar = args.sonar_model or _recorded_sonar(existing_doc)
                if sonar:
                    probe, planned = _product_run(args, raw_path, sonar, target, stage=False)
                    reuse = probe.reusable(planned)
        if reuse:
            logger.success(
                f".nc already exists; NOT overwriting. Reusing: "
                f"{nc_path.resolve()} (pass --force to regenerate)."
            )
            _deliver(args, str(nc_path.resolve()), cloud, existing=True)
            return

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
    run = Run(SPEC, args)
    if gcs_raw is not None:
        try:
            raw_path = run.input(gcs_raw).local
        except Exception as e:  # noqa: BLE001
            logger.error(f"Could not read {gcs_raw}: {e}")
            sys.exit(1)
    elif user_provided_raw_path is not None:
        run.input(str(raw_path))

    if local_source:
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
    # Download (NCEI mode) and record where the .raw came from
    # ---------------------------
    if not local_source:
        try:
            downloaded = _acquire_raw(
                file_name=args.file_name,
                metadata=metadata,
                raw_path=raw_path,
                download_dir=download_dir,
                upload_to_gcp=args.upload_to_gcp,
                debug=args.debug,
                force=args.force,
            )
        except SystemExit:
            raise
        except Exception as e:
            logger.exception(f"Error during processing: {e}")
            sys.exit(1)
        if downloaded:
            # <file>.raw.aa.json: the NCEI object and its checksum. The .nc
            # records the origin; the identity (content md5) is unchanged,
            # so the sidecar never changes a hash.
            origin = NCEI_PREFIX + _ncei_object_key(args.file_name, metadata)
            record_source(raw_path, tool=SPEC.name, origin=origin, extra={
                "ship": metadata["ship_name"],
                "survey": metadata["survey_name"],
                "sonar": metadata["sonar_model"],
            })
        run.input(str(raw_path))

    # ---------------------------
    # Plan (hash) and reuse
    # ---------------------------
    run.params["sonar_model"] = _canon_sonar(metadata["sonar_model"])
    out = run.plan(ext=".nc", explicit=target)
    user_owned = local_source
    if run.reusable(out):
        run.finish(out, emit=False)
        _cleanup_raw(args, raw_path, user_owned)
        _deliver(args, out.target, cloud, existing=True)
        return
    if nc_path is not None and nc_path.exists():
        if args.force:
            logger.info(f"--force: converting {nc_path.name} again.")
        elif existing_doc is not None:
            logger.info(
                f"{nc_path.name} holds a different product "
                f"(aa:{str(existing_doc.get('product', {}).get('hash', ''))[:8]}, "
                f"this one is aa:{out.short}); converting again."
            )

    # Only log "will be written" once we know we're actually going to
    # write — i.e. past the short-circuit.
    logger.info(f"Output .nc will be written to: {out.target}")

    # ---------------------------
    # Convert
    # ---------------------------
    def _drop_staging() -> None:
        # A gs:// target is written to a local staging folder first.
        if out.staging is not None:
            shutil.rmtree(out.staging, ignore_errors=True)
            out.staging = None

    try:
        _convert(raw_path=raw_path, nc_path=out.local, sonar_model=metadata["sonar_model"])
    except SystemExit:
        _drop_staging()
        raise
    except Exception as e:
        logger.exception(f"Error during processing: {e}")
        _drop_staging()
        sys.exit(1)

    # Provenance into the .nc; a gs:// -o/--dest target is uploaded here,
    # with aa-product-hash / aa-base / aa-tool object metadata.
    try:
        run.finish(out, emit=False)
    except Exception as e:  # noqa: BLE001 - an upload that failed
        logger.exception(f"Could not publish {out.target}: {e}")
        _drop_staging()
        sys.exit(1)

    # Optional cleanup of the intermediate .raw. Done after the .nc is
    # confirmed on disk, so a conversion failure never costs the user
    # their downloaded raw data.
    _cleanup_raw(args, raw_path, user_owned)

    logger.success(f"Generated {out.target} with aa-ed.")

    # Cloud output (additive): on a cache miss we've just built the .nc
    # locally; upload it (registering it for the next person), then emit
    # either the gs:// URI or the local path. Without a cloud target this
    # is exactly the original behavior — print the local .nc path.
    if cloud is None:
        logger.info("Passing .nc path to stdout...")
    _deliver(args, out.target, cloud)


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
        # The exact NCEI object (recorded as the download's origin).
        "s3_object_key": row.get("s3_object_key"),
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


# ============================================================
# Cloud output & URI caching helpers (additive).
#
# Everything here is INERT unless the user passes --gcs-uri or
# --gcs-prefix. Uploads deliberately route through the SAME
# aalibrary primitive aa-upload uses, so the two tools write
# objects identically; existence-check and download use the
# standard google-cloud-storage Bucket/Blob API on the object
# setup_gcp_storage_objs returns (the same object aa-upload
# calls blob.upload_from_filename on).
# ============================================================

def _parse_gcs_uri(uri: str):
    """Split 'gs://bucket/path/to/obj.nc' into ('bucket', 'path/to/obj.nc').

    Raises ValueError on a malformed URI so the caller can emit a clean
    error and exit.
    """
    if not uri.startswith("gs://"):
        raise ValueError(f"Not a gs:// URI: {uri!r}")
    rest = uri[len("gs://"):]
    if "/" not in rest:
        raise ValueError(
            f"gs:// URI {uri!r} is missing an object path "
            "(expected gs://<bucket>/<path>.nc)."
        )
    bucket_name, blob_path = rest.split("/", 1)
    if not bucket_name or not blob_path:
        raise ValueError(
            f"gs:// URI {uri!r} has an empty bucket or object path."
        )
    return bucket_name, blob_path


def _resolve_cloud_target(args, nc_path: Path):
    """Decide where the derived .nc should live in GCS, if anywhere.

    Returns None when neither --gcs-uri nor --gcs-prefix is set (the
    normal local-only path). Otherwise returns
    (bucket_name, blob_path, gs_uri):

      --gcs-uri gs://bucket/a/b/file.nc  -> exact object; bucket parsed
                                            from the URI.
      --gcs-prefix a/b/                  -> object is '<prefix>/<stem>.nc';
                                            bucket comes from
                                            --gcp_bucket_name/--gcp_env/env
                                            (bucket_name may be None here
                                            and is filled in by the bucket
                                            resolver).

    --gcs-uri and --gcs-prefix are mutually exclusive.

    Note on the cache key: this is the simple "deterministic URI" scheme
    — the object path IS the key. aa-ed's conversion has no tunable
    parameters beyond sonar_model (which the file's own header pins
    down), so <stem>.nc is a stable name for "the EchoData NetCDF of this
    raw file." If you want finer cache invalidation (e.g. per echopype
    version), bake that into the prefix; --force always bypasses the
    check.
    """
    if args.gcs_uri and args.gcs_prefix:
        logger.error("Use --gcs-uri OR --gcs-prefix, not both.")
        sys.exit(2)

    if args.gcs_uri:
        try:
            bucket_name, blob_path = _parse_gcs_uri(args.gcs_uri)
        except ValueError as e:
            logger.error(str(e))
            sys.exit(2)
        # Explicit URI bucket wins over --gcp_bucket_name; warn on conflict
        # rather than silently ignoring the flag.
        if args.gcp_bucket_name and args.gcp_bucket_name != bucket_name:
            logger.warning(
                f"--gcp_bucket_name='{args.gcp_bucket_name}' disagrees with "
                f"the bucket in --gcs-uri ('{bucket_name}'); using the URI's "
                "bucket."
            )
        return bucket_name, blob_path, f"gs://{bucket_name}/{blob_path}"

    if args.gcs_prefix:
        prefix = args.gcs_prefix.strip().lstrip("/")
        prefix = (prefix.rstrip("/") + "/") if prefix else ""
        blob_path = f"{prefix}{nc_path.stem}.nc"
        bucket_name = args.gcp_bucket_name  # may be None -> env default
        gs_uri = (
            f"gs://{bucket_name}/{blob_path}" if bucket_name
            else f"gs://<bucket>/{blob_path}"
        )
        return bucket_name, blob_path, gs_uri

    return None


def _resolve_gcp_bucket(gcp_env, project_id, gcp_bucket_name):
    """Set up and return (bucket, resolved_bucket_name), mirroring aa-upload.

    Precedence matches aa-upload's resolver:
      1. Explicit --project_id / --gcp_bucket_name win.
      2. Else --gcp_env switches the aalibrary env
         (use_gcp_prod() / use_gcp_dev()).
      3. Else whatever AALIBRARY_GCP_* env vars are already exported.

    The bucket object comes from
    aalibrary.utils.cloud_utils.setup_gcp_storage_objs — the same call
    aa-upload makes.
    """
    try:
        from aalibrary.utils.cloud_utils import setup_gcp_storage_objs
    except Exception as e:
        logger.exception(f"Failed to import aalibrary.utils.cloud_utils: {e}")
        sys.exit(1)

    if gcp_env and (project_id or gcp_bucket_name):
        # Explicit project/bucket already pins the target; the env switch
        # would be a no-op at best and confusing at worst. Say so.
        logger.warning(
            f"--gcp_env '{gcp_env}' is ignored because an explicit "
            "--project_id / --gcp_bucket_name (or a --gcs-uri bucket) was "
            "provided."
        )
    elif gcp_env:
        try:
            from aalibrary import config as aalibrary_config
        except Exception as e:
            logger.exception(
                f"Failed to import aalibrary.config for --gcp_env: {e}"
            )
            sys.exit(1)
        if gcp_env == "prod" and hasattr(aalibrary_config, "use_gcp_prod"):
            aalibrary_config.use_gcp_prod()
        elif gcp_env == "dev" and hasattr(aalibrary_config, "use_gcp_dev"):
            aalibrary_config.use_gcp_dev()
        else:
            logger.error(
                f"--gcp_env {gcp_env} requested but aalibrary.config lacks "
                "the corresponding switch. Pass --project_id / "
                "--gcp_bucket_name instead."
            )
            sys.exit(1)
        logger.info(f"Switched aalibrary GCP env to '{gcp_env}'.")

    _client, resolved_name, bucket = setup_gcp_storage_objs(
        project_id=project_id,
        gcp_bucket_name=gcp_bucket_name,
    )
    logger.info(f"Targeting GCP bucket '{resolved_name}'.")
    return bucket, resolved_name


def _gcs_blob_exists(gcp_bucket, blob_path: str) -> bool:
    """True if `blob_path` already exists in the bucket (the cache check).

    A failed existence check must NOT masquerade as a hit — that would
    skip a needed conversion — so on error we warn and treat it as a
    miss.
    """
    try:
        return gcp_bucket.blob(blob_path).exists()
    except Exception as e:
        logger.warning(
            f"Could not check whether '{blob_path}' exists in the bucket "
            f"({e}); proceeding as a cache miss."
        )
        return False


def _gcs_download_blob(gcp_bucket, blob_path: str, dest: Path) -> None:
    """Download an existing bucket object to `dest` (found-and-downloaded)."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Downloading cached asset gs://.../{blob_path} -> {dest}")
    try:
        gcp_bucket.blob(blob_path).download_to_filename(str(dest))
    except Exception as e:
        logger.exception(f"Failed to download cached asset '{blob_path}': {e}")
        sys.exit(1)
    if not dest.exists():
        logger.error(
            f"Download of '{blob_path}' reported success but '{dest}' is "
            "not on disk."
        )
        sys.exit(1)
    logger.success(f"Reused cached asset -> {dest.resolve()}")


def _gcs_upload_nc(gcp_bucket, blob_path: str, local_nc: Path,
                   debug: bool, metadata: Optional[dict] = None) -> None:
    """Upload the local .nc to the bucket via aa-upload's primitive.

    Routes through aalibrary.utils.cloud_utils.upload_file_to_gcp_bucket —
    the same single-file primitive aa-upload's --as-is mode uses — so
    aa-ed and aa-upload write objects the same way. NetCDF is HDF5 (needs
    a seekable local file), so we upload the already-written local .nc as
    a whole object rather than streaming.

    ``metadata`` (aa-product-hash / aa-base / aa-tool) is then set as the
    object's custom metadata, which is how the shared core recognizes an
    identical product in a bucket. Best effort: the upload itself stands.
    """
    try:
        from aalibrary.utils.cloud_utils import upload_file_to_gcp_bucket
    except Exception as e:
        logger.exception(f"Failed to import aalibrary.utils.cloud_utils: {e}")
        sys.exit(1)
    logger.info(f"Uploading {local_nc.name} -> gs://.../{blob_path}")
    upload_file_to_gcp_bucket(
        bucket=gcp_bucket,
        blob_file_path=blob_path,
        local_file_path=str(local_nc),
        debug=debug,
    )
    if metadata:
        try:
            blob = gcp_bucket.blob(blob_path)
            blob.metadata = {key: str(value) for key, value in metadata.items()}
            blob.patch()
        except Exception as e:  # noqa: BLE001
            logger.warning(f"Uploaded, but could not set aa-* metadata on "
                           f"'{blob_path}': {e}")


def _maybe_remove_local(nc_path: Path) -> None:
    """Delete the local .nc after a successful upload (--cloud-only)."""
    try:
        nc_path.unlink(missing_ok=True)
        logger.info(
            f"Removed local .nc after upload (--cloud-only): {nc_path}"
        )
    except Exception as e:
        logger.warning(f"Could not remove local .nc '{nc_path}': {e}")


def _acquire_raw(
    file_name: str,
    metadata: dict,
    raw_path: Path,
    download_dir: Path,
    upload_to_gcp: bool,
    debug: bool,
    force: bool = False,
) -> bool:
    """Make sure the NCEI .raw is at raw_path. True if it was downloaded now.

    If raw_path is already on disk and force=False, the download is skipped
    (cache hit). Pass force=True to invalidate. The aalibrary.ingestion
    download is imported only here, so the local-file modes never load it.
    """
    if raw_path.exists() and not force:
        # Cache hit — file from an earlier aa-ed run is still on disk.
        # --force is the escape hatch if the user suspects corruption.
        logger.info(
            f".raw already on disk, skipping NCEI download: {raw_path} "
            f"({raw_path.stat().st_size} bytes). Pass --force to re-download."
        )
        return False

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
    return True


def _convert(raw_path: Path, nc_path: Path, sonar_model: str) -> None:
    """Convert raw_path to an EchoData NetCDF at nc_path (echopype.open_raw)."""
    try:
        import echopype as ep
    except Exception as e:
        logger.exception(f"Failed to import echopype: {e}")
        sys.exit(1)

    logger.info(
        f"Loading {raw_path} into EchoData "
        f"(sonar_model={sonar_model})"
    )
    ed = ep.open_raw(
        raw_file=raw_path,
        sonar_model=sonar_model,
    )

    logger.info(f"Saving EchoData to {nc_path}")
    Path(nc_path).parent.mkdir(parents=True, exist_ok=True)
    # overwrite=True: we only get here when the existing file (if any) is
    # not this product, or --force was given. echopype's default (False)
    # kept the old file and returned as if it had written — which is why
    # `aa-ed FILE --force` used to leave a stale .nc behind.
    ed.to_netcdf(save_path=nc_path, overwrite=True)

    if not Path(nc_path).exists():
        # Defensive: to_netcdf shouldn't return silently on failure, but
        # if it does, fall through with a clear error rather than printing
        # a missing path to stdout and breaking aa-sv downstream.
        logger.error(
            f"Conversion appeared to succeed, but '{nc_path}' is not on "
            "disk. Rerun with --debug for details."
        )
        sys.exit(1)

    logger.success(f"RAW → NetCDF conversion complete: {Path(nc_path).resolve()}")


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

    Kept for callers of this module; main() runs the two halves itself
    (_acquire_raw, then _convert) so it can record the download's origin
    and the .nc's provenance in between. No provenance is embedded here.

    Idempotency:
      - If user_provided_raw is True, the .raw at raw_path is the
        user's source file. It is never re-downloaded — not even when
        force=True.
      - Otherwise, if raw_path is already on disk and force=False, the
        download is skipped (cache hit). Pass force=True to invalidate.
    """
    if user_provided_raw:
        if not raw_path.exists():
            logger.error(
                f"User-provided .raw '{raw_path}' has disappeared "
                "since input validation."
            )
            sys.exit(1)
    else:
        _acquire_raw(file_name, metadata, raw_path, download_dir, upload_to_gcp, debug, force)
    _convert(raw_path, nc_path, metadata["sonar_model"])


def _convert_one_local(
    raw_path: Path,
    nc_path: Optional[Path] = None,
    sonar_model_override: Optional[str] = None,
    force: bool = False,
    args=None,
) -> dict:
    """Convert a single local .raw to .nc, fully offline, with provenance.

    No NCEI download, no BigQuery — assumes the .raw is already on
    disk at raw_path. Sonar model comes from sonar_model_override if
    given, otherwise auto-detected from the .raw file's header via
    _detect_sonar_model_from_file().

    The .nc goes to nc_path when given, else the standard name beside the
    .raw (<raw stem>.nc; AA_NAMING=legacy gives the same for plain names).

    Idempotent: an existing .nc is "cached" (not converted again) when it
    holds the same product, or when it carries no aa provenance at all (made
    before provenance existed; reused by name, as before, and flagged with
    "legacy": True). A .nc holding a different product is converted again.

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
            "product": str,       # short product hash, when known
            "legacy": bool,       # cached without provenance
            "error": str,         # present when status == "failed"
        }
    """
    run = Run(SPEC, args)
    run.force = bool(force) or run.force
    explicit = str(nc_path) if nc_path is not None else None
    try:
        run.input(str(raw_path))
        where = run.plan(ext=".nc", explicit=explicit,
                         legacy=lambda: raw_path.with_suffix(".nc"), stage=False)
    except (Exception, SystemExit) as e:  # noqa: BLE001 - one file must not end the batch
        return {"status": "failed", "raw_path": raw_path,
                "nc_path": nc_path or raw_path.with_suffix(".nc"),
                "error": f"could not read the .raw: {e}"}
    nc_path = Path(where.target)

    # A .nc made before provenance existed: reused by name, exactly as
    # before (and before reading any header).
    if nc_path.exists() and not run.force and provenance.read(nc_path) is None:
        return {"status": "cached", "raw_path": raw_path, "nc_path": nc_path, "legacy": True}

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

    run.params["sonar_model"] = _canon_sonar(sonar_model)
    out = run.plan(ext=".nc", explicit=str(nc_path), stage=False)
    if not run.force and nc_path.exists() and run.existing_hash(out) == out.hash:
        return {"status": "cached", "raw_path": raw_path, "nc_path": nc_path,
                "product": out.short, "legacy": False}

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
        # If the .nc exists (--force, or a different product), remove it
        # first so a half-written file is never mistaken for the old one;
        # overwrite=True as well, since echopype's default silently keeps
        # an existing file.
        if nc_path.exists():
            nc_path.unlink()
        ed = ep.open_raw(raw_file=raw_path, sonar_model=sonar_model)
        ed.to_netcdf(save_path=nc_path, overwrite=True)
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

    run.finish(out, emit=False)  # embed the provenance
    return {
        "status": "converted",
        "raw_path": raw_path,
        "nc_path": nc_path,
        "sonar_model": sonar_model,
        "product": out.short,
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

    if getattr(args, "dest", None) or getattr(args, "base", None):
        # Same reason as -o: the directory printed on stdout is where the
        # .nc files are, and one base name cannot name many files.
        logger.error(
            "--dest / --base are not supported in directory mode. "
            ".nc files always land alongside their source .raw, named "
            "after it."
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

    if getattr(args, "gcs_uri", None) or getattr(args, "gcs_prefix", None):
        # Cloud output / URI caching is single-file only: a directory
        # would need N distinct object paths, which is exactly what
        # aa-upload is for. Keep the contracts separate and point the
        # user at the clean composition rather than guessing a layout.
        logger.warning(
            "--gcs-uri / --gcs-prefix are ignored in directory mode. "
            "Convert locally with aa-ed, then pipe the directory through "
            "aa-upload (aa-ed ./dir/ | aa-upload ...)."
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

    # ---- Output-name collisions ---------------------------------
    # Two .raw files must never write the same .nc: the second conversion
    # would silently replace the first. Checked for the whole batch before
    # anything is converted.
    planned: dict[str, list[Path]] = {}
    for raw_path in raw_files:
        planned.setdefault(os.path.normcase(str(_default_nc_path(raw_path))), []).append(raw_path)
    collisions = {name: raws for name, raws in planned.items() if len(raws) > 1}
    if collisions:
        for name, raws in sorted(collisions.items()):
            logger.error(
                f"{len(raws)} .raw files would all be converted to {name}: "
                + ", ".join(f"'{raw.name}'" for raw in raws)
            )
        logger.error(
            "Nothing was converted. Rename the files so their names differ, "
            "or convert them one at a time with -o."
        )
        sys.exit(1)

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
            1 for r in raw_files if _default_nc_path(r).exists()
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
    legacy_reused = 0

    for i, raw_path in enumerate(raw_files, start=1):
        logger.info(f"[{i}/{len(raw_files)}] {raw_path.name}")

        result = _convert_one_local(
            raw_path=raw_path,
            nc_path=_default_nc_path(raw_path),
            sonar_model_override=sonar_override,
            force=args.force,
            args=args,
        )

        status = result["status"]
        counts[status] = counts.get(status, 0) + 1
        nc_path = result["nc_path"]

        if status == "converted":
            logger.success(
                f"  -> {nc_path.name} "
                f"(sonar={result['sonar_model']}, "
                f"{nc_path.stat().st_size:,} bytes, aa:{result['product']})"
            )
        elif status == "cached":
            if result.get("legacy"):
                legacy_reused += 1
                logger.info(
                    "  .nc already exists (no aa provenance), skipping "
                    "(--force to override)."
                )
            else:
                logger.info(
                    f"  .nc already exists (identical product aa:{result['product']}), "
                    "skipping (--force to override)."
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

    if legacy_reused:
        _note(args, f"{legacy_reused} existing .nc file(s) in {directory} have no aa "
                    "provenance (made before provenance was recorded) and were reused by "
                    "name, as before. --force converts them again with provenance.")

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
