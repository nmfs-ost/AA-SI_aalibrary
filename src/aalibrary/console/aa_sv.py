#!/usr/bin/env python3
"""
aa-sv

Console tool for computing Sv (volume backscattering strength) from a .nc
EchoData file (typically the output of aa-nc) using Echopype, and saving
back to NetCDF.

Pipeline-friendly: reads input path (or gs:// URI) from positional arg or
stdin, writes output path to stdout, all logs to stderr. The output
carries the input's provenance plus this calibration step.
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
# Without this, any exception in process_file disappears silently and
# the pipeline downstream gets no input — a confusing failure mode.
logger.add(sys.stderr, level="WARNING")

import argparse
import pprint
from pathlib import Path

from aalibrary.console._core import (
    Help, Run, ToolSpec, add_common_flags, canon, naming, render, show_help, stdio,
)


def _waveform(value):
    # echopype treats FM as BB, so they are the same computation.
    if value is None:
        return None
    v = str(value).strip().upper()
    return "BB" if v in {"BB", "FM"} else v


SPEC = ToolSpec(
    name="aa-sv",
    role="transform",
    kind="sv",
    op="echopype.calibrate.compute_Sv",
    op_version=1,
    params={"waveform_mode": _waveform, "encode_mode": canon.choice("lower")},
)

HELP = Help(
    summary="Calibrate EchoData to volume backscattering strength (Sv).",
    does=(
        "Runs echopype.calibrate.compute_Sv on a converted EchoData file and "
        "writes a flat Sv dataset (Sv, echo_range, sound_absorption, ... on "
        "channel x ping_time x range_sample)."
    ),
    stdin="One EchoData .nc/.zarr path or gs:// URI, from aa-nc, aa-ed or aa-combine.",
    stdout="The Sv file's absolute path (or gs:// URI).",
    options=[
        ("-o, --output_path PATH", "Explicit output; '_Sv' is appended to its stem, as "
                                   "always. Local path or gs:// URI."),
        ("--waveform_mode CW|BB|FM", "EK80 only. Omit for EK60."),
        ("--encode_mode complex|power", "EK80 only. Omit for EK60."),
    ],
    science={
        "waveform_mode": "EK80 waveform. FM and BB are the same computation.",
        "encode_mode": "EK80 encoding.",
    },
    files=(
        "Reads EchoData .nc or .zarr, local or gs://. Writes <base>_<hash>.nc "
        "beside the input (current directory for gs:// input), or -o, or --dest. "
        "An identical earlier result is reused."
    ),
    pipeline=("Second stage: aa-nc | aa-sv | aa-graph, or aa-nc | aa-sv | aa-depth | "
              "... (see aa-guide). Note: aa-clean writes the cleaned values to "
              "Sv_corrected; aa-mvbs, aa-nasc and aa-graph read Sv."),
    examples=[
        "aa-nc D20160703-T060000.raw --sonar_model EK60 | aa-sv",
        "aa-sv file.nc --waveform_mode BB --encode_mode complex   # EK80",
    ],
    notes=["EK80 needs both --waveform_mode and --encode_mode; echopype refuses "
           "EK80 data without them."],
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
    Usage: aa-sv [OPTIONS] [INPUT_PATH]

    Arguments:
    INPUT_PATH                  Path (or gs:// URI) to the .nc / .zarr EchoData
                                file. Optional. Defaults to stdin if not provided.

    Options:
    -o, --output_path           Path (or gs:// URI) to save processed output.
                                '_Sv' is appended to its stem and a .nc
                                suffix forced. Default: <base>_<hash>.nc beside
                                the input (AA_NAMING=legacy: <stem>_Sv.nc).

    --waveform_mode             For EK80 echosounders ONLY: waveform mode.
                                Choices: CW, BB, FM
                                Default: not passed. EK60 needs neither flag;
                                EK80 needs both.

    --encode_mode               For EK80 echosounders ONLY: encoding mode.
                                Choices: complex, power
                                Default: not passed.

    --base NAME                 Base name for the output.
    --dest DIR|gs://PREFIX      Write the default-named output there.
    --force                     Recompute even if an identical product exists.

    Description:
    This tool computes Sv (volume backscattering strength) from a previously-
    converted NetCDF EchoData file using echopype.calibrate.compute_Sv, and
    saves the result to a new .nc file. The output path is printed to stdout
    for piping into the next stage of the pipeline. Provenance (the input's
    chain plus this step) is embedded in the output; see aa-metadata.

    For visualization, pipe the output into aa-graph or aa-plot:
        aa-nc --sonar_model EK60 input.raw | aa-sv | aa-graph

    Example:
        aa-sv /path/to/input.nc --waveform_mode BB --encode_mode complex \\
              -o /path/to/output.nc          # EK80 broadband
        aa-sv /path/to/input.nc --waveform_mode CW --encode_mode power    # EK80 CW
    """
    print(help_text)


def _build_parser():
    parser = argparse.ArgumentParser(
        description="Compute Sv from a NetCDF EchoData file with Echopype.",
        add_help=False,
    )
    parser.add_argument(
        "input_path",
        type=str,
        nargs="?",
        help="Path or gs:// URI of the .nc / .zarr EchoData file.",
    )
    parser.add_argument(
        "-o", "--output_path",
        type=str,
        help="Path to save processed output. '_Sv' is appended to the stem.",
    )
    parser.add_argument(
        "--waveform_mode",
        type=str,
        default=None,
        choices=["CW", "BB", "FM"],
        help="For EK80 Echosounders ONLY: waveform mode. Omit for EK60.",
    )
    parser.add_argument(
        "--encode_mode",
        type=str,
        default=None,
        choices=["complex", "power"],
        help="For EK80 Echosounders ONLY: encoding mode. Omit for EK60.",
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

    allowed_extensions = {".netcdf4", ".nc", ".zarr"}
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
    explicit = (naming.with_stem_suffix(args.output_path, "_Sv", ".nc")
                if args.output_path else None)
    out = run.plan(
        ext=".nc",
        explicit=explicit,
        legacy=lambda: naming.with_stem_suffix(src.local, "_Sv", ".nc"),
    )

    # Guard against clobbering the input
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
            "waveform_mode": args.waveform_mode,
            "encode_mode": args.encode_mode,
            "product": out.hash,
        }
        logger.debug(
            f"Executing aa-sv configured with [OPTIONS]:\n"
            f"{pprint.pformat(args_summary)}"
        )

        process_file(
            input_path=src.local,
            output_path=out.local,
            waveform_mode=args.waveform_mode,
            encode_mode=args.encode_mode,
        )

        logger.success(f"Generated {out.target} with aa-sv. Passing it to stdout...")
        run.finish(out)

    except Exception as e:
        logger.exception(f"Error during processing: {e}")
        sys.exit(1)


def clean_attrs(ds):
    """Replace None-valued attrs with 'NA' so the dataset is NetCDF-safe.
    NetCDF attrs cannot be None — to_netcdf will raise on serialization."""
    for k, v in ds.attrs.items():
        if v is None:
            ds.attrs[k] = "NA"
    for var in ds.data_vars:
        for k, v in ds[var].attrs.items():
            if v is None:
                ds[var].attrs[k] = "NA"
    return ds


def process_file(
    input_path: Path,
    output_path: Path,
    waveform_mode=None,
    encode_mode=None,
):
    """Load EchoData from NetCDF, compute Sv, and save to NetCDF."""
    import echopype as ep  # deferred so --help stays fast

    logger.info(f"Loading EchoData from {input_path}")
    ed = ep.open_converted(str(input_path))

    # Build kwargs lazily — only pass waveform_mode / encode_mode when the
    # user explicitly provided them. echopype's compute_Sv treats these as
    # EK80-only; passing CW/complex unconditionally to an EK60 dataset
    # raises an error. The previous version of this script always passed
    # them, which is why EK60 pipelines silently failed.
    compute_kwargs = {}
    if waveform_mode is not None:
        compute_kwargs["waveform_mode"] = waveform_mode
    if encode_mode is not None:
        compute_kwargs["encode_mode"] = encode_mode

    if compute_kwargs:
        logger.info(f"Computing Sv (EK80 mode: {compute_kwargs})")
    else:
        logger.info("Computing Sv (using echopype defaults for this sonar)")

    ds_Sv = ep.calibrate.compute_Sv(ed, **compute_kwargs)
    ds_Sv = clean_attrs(ds_Sv)

    output_path = Path(output_path).with_suffix(".nc")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Saving Sv dataset to {output_path}")
    ds_Sv.to_netcdf(output_path)
    logger.success(f"Sv computation complete: {output_path}")


if __name__ == "__main__":
    main()
