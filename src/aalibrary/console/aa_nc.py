#!/usr/bin/env python3
"""
aa-nc

Console tool for converting raw echosounder files (.raw) to NetCDF using
Echopype's open_raw / EchoData.to_netcdf. Produces a multi-group NetCDF
EchoData file suitable as input to aa-sv.

This tool does NOT compute Sv and does NOT remove background noise — it
is purely the RAW → NetCDF conversion stage of the pipeline.

It starts the provenance chain: the output records the raw file's
identity (and its NCEI/GCS origin when aa-raw or aa-download left a
sidecar), the sonar model, and the base name every later product keeps.

Pipeline-friendly: writes the output path (or gs:// URI) to stdout, all
logs to stderr.

Typical pipeline usage:
    aa-nc --sonar_model EK60 input.raw | aa-sv | aa-clean
"""

# === Silence logs BEFORE any heavy imports ===
import logging
import sys
import warnings

logging.disable(logging.CRITICAL)
warnings.filterwarnings("ignore")

from loguru import logger
logger.remove()
# Default sink: WARNING+ to stderr so real errors aren't swallowed.
logger.add(sys.stderr, level="WARNING")

import argparse
import pprint
from pathlib import Path

from aalibrary.console._core import (
    Help, Run, ToolSpec, add_common_flags, canon, naming, render, show_help, stdio,
)

SPEC = ToolSpec(
    name="aa-nc",
    role="echodata",
    kind="echodata",
    op="echopype.open_raw",
    op_version=1,
    params={"sonar_model": canon.choice("upper")},
)

HELP = Help(
    summary="Convert a raw echosounder file (.raw) to an EchoData NetCDF.",
    does=(
        "Parses the .raw with echopype.open_raw and writes the multi-group "
        "EchoData NetCDF that aa-sv calibrates. No Sv, no noise removal: this "
        "is only the conversion stage. The .raw is never modified."
    ),
    stdin="One .raw path or gs:// URI (argument, or one line on stdin).",
    stdout="The absolute path of the .nc (or its gs:// URI with -o/--dest gs://...).",
    options=[
        ("--sonar_model MODEL", "REQUIRED. EK60, EK80, AZFP, EA640, ..."),
        ("-o, --output_path PATH", "Where to write; the extension is forced to .nc. "
                                   "Local path or gs:// URI."),
    ],
    science={"sonar_model": "Which echopype parser reads the file."},
    files=(
        "Reads a local .raw, or gs://.../x.raw (through your gcsfuse mount when "
        "it covers the path, otherwise downloaded once to the cache). Writes "
        "<base>.nc beside the input, where <base> is the raw file's stem (or "
        "--base NAME); with a gs:// input the default is the current directory. "
        "If <base>.nc already exists from the same raw file and options, it is "
        "reused instead of converted again."
    ),
    pipeline=(
        "First stage after the data source. Feed it from aa-raw or aa-download, "
        "pipe its output into aa-sv."
    ),
    examples=[
        "aa-nc D20160703-T060000.raw --sonar_model EK60",
        "aa-raw ... | aa-nc --sonar_model EK60 | aa-sv | aa-clean",
        "aa-nc gs://bucket/raw/D20160703-T060000.raw --sonar_model EK60 --dest gs://bucket/nc/",
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


def print_help():
    """Curated help (also used by the docs generator)."""
    sys.stdout.write(render(SPEC, HELP, _build_parser()))


def print_help_full():
    help_text = """
    Usage: aa-nc [OPTIONS] INPUT_PATH

    Arguments:
    INPUT_PATH                  Path (or gs:// URI) to the input .raw file.
                                (Required, may also be supplied via stdin.)

    Options:
    -o, --output_path           Path (or gs:// URI) to save the converted
                                NetCDF output; the suffix is forced to .nc.
                                Default: same directory as input, named
                                <base>.nc where <base> is the .raw stem.

    --sonar_model               Sonar model identifier (REQUIRED).
                                Examples: EK60, EK80, AZFP, EA640.

    --base NAME                 Base name for the output (and for every
                                product derived from it downstream).
    --dest DIR|gs://PREFIX      Write <base>.nc into DIR or a gs:// prefix.
    --force                     Convert again even if an identical <base>.nc
                                (same raw file, same options) already exists.

    Description:
    Converts a raw echosounder file (.raw) into a multi-group NetCDF
    EchoData file using echopype.open_raw. The output is the input to
    the next pipeline stage (aa-sv), which is what actually computes Sv.

    Provenance (raw file identity and origin, sonar model, base name) is
    embedded in the output's global attributes; see aa-metadata.

    The input .raw file is never modified.

    Example:
    aa-nc /path/to/input.raw --sonar_model EK60 -o /path/to/output.nc
    """
    print(help_text)


def _build_parser():
    parser = argparse.ArgumentParser(
        description="Convert .raw files to NetCDF EchoData with Echopype.",
        add_help=False,
    )
    parser.add_argument(
        "input_path",
        type=str,
        nargs="?",
        help="Path or gs:// URI of the .raw file.",
    )
    parser.add_argument(
        "-o", "--output_path",
        type=str,
        help="Path or gs:// URI to save processed output. Default: <base>.nc beside the input.",
    )
    parser.add_argument(
        "--sonar_model",
        type=str,
        required=True,
        help="Sonar model identifier (e.g., EK60, EK80, AZFP, EA640).",
    )
    add_common_flags(parser)
    return parser


def main():
    # No args on a terminal: help. (An empty pipe is an error: see stdio.)
    if len(sys.argv) == 1 and not stdio.stdin_is_piped():
        print_help()
        sys.exit(0)

    parser = _build_parser()
    if show_help(SPEC, HELP, parser, full=print_help_full):
        sys.exit(0)

    args = parser.parse_args()

    # ---------------------------
    # Validate input
    # ---------------------------
    token = stdio.one_input(args.input_path, SPEC.name)
    run = Run(SPEC, args)
    src = run.input(token)

    allowed_extensions = {".raw"}
    ext = src.local.suffix.lower()
    if ext not in allowed_extensions:
        logger.error(
            f"'{src.name}' is not a supported file type. "
            f"Allowed: {', '.join(sorted(allowed_extensions))}"
        )
        sys.exit(1)

    # ---------------------------
    # Resolve output path
    # ---------------------------
    explicit = naming.with_ext(args.output_path, ".nc") if args.output_path else None
    out = run.plan(
        ext=".nc",
        explicit=explicit,
        legacy=lambda: src.local.with_suffix(".nc"),
    )

    # Guard against clobbering the input. With the .raw → .nc extension
    # split this is essentially impossible, but cheap insurance against
    # someone passing -o pointing at the source file.
    if not out.remote and Path(out.target).resolve() == src.local.resolve():
        logger.error(f"Refusing to overwrite input file: {src.local.resolve()}")
        sys.exit(1)

    if run.reusable(out):
        run.finish(out)
        return

    # ---------------------------
    # Process file
    # ---------------------------
    try:
        args_summary = {
            "input": token,
            "output": out.target,
            "sonar_model": args.sonar_model,
            "product": out.hash,
        }
        logger.debug(
            f"Executing aa-nc configured with [OPTIONS]:\n"
            f"{pprint.pformat(args_summary)}"
        )

        process_file(
            input_path=src.local,
            output_path=out.local,
            sonar_model=args.sonar_model,
        )

        logger.success(f"Generated {out.target} with aa-nc. Passing it to stdout...")
        # Pipe the output path (or URI) to stdout for the next tool
        run.finish(out)

    except Exception as e:
        logger.exception(f"Error during processing: {e}")
        sys.exit(1)


def process_file(input_path: Path, output_path: Path, sonar_model: str):
    """Load a raw file as EchoData and save it as a multi-group NetCDF.

    No Sv computation, no noise removal — those happen downstream in
    aa-sv and aa-clean. This is just the conversion stage.
    """
    import echopype as ep  # deferred so --help stays fast

    logger.info(f"Loading {input_path} into EchoData (sonar_model={sonar_model})")
    ed = ep.open_raw(raw_file=str(input_path), sonar_model=sonar_model)

    logger.info(f"Saving EchoData to {output_path}")
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    # overwrite=True: we only get here when the existing file (if any) is not
    # the same product, so replacing it is correct. echopype's default
    # (False) used to keep a stale file while reporting success.
    ed.to_netcdf(save_path=str(output_path), overwrite=True)
    logger.success(f"RAW → NetCDF conversion complete: {output_path}")


if __name__ == "__main__":
    main()
