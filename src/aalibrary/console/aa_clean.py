#!/usr/bin/env python3
"""
aa-clean

Console tool for removing background noise from a Sv (volume backscattering
strength) NetCDF dataset using echopype.clean.remove_background_noise, and
saving the cleaned result back to NetCDF.

Pipeline-friendly: reads input path (or gs:// URI) from positional arg or
stdin, writes output path to stdout, all logs to stderr. The output
carries the input's provenance plus this cleaning step.

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
# Without this, any exception in process_file disappears silently and
# the pipeline downstream gets no input — a confusing failure mode.
logger.add(sys.stderr, level="WARNING")

import argparse
import pprint
from pathlib import Path

from aalibrary.console._core import (
    Help, Run, ToolSpec, add_common_flags, canon, naming, render, show_help, stdio,
)

SPEC = ToolSpec(
    name="aa-clean",
    role="transform",
    kind="sv",
    op="echopype.clean.remove_background_noise",
    op_version=1,
    params={
        "ping_num": canon.integer,
        "range_sample_num": canon.integer,
        "background_noise_max": canon.quantity("dB"),
        "snr_threshold": canon.number,
    },
)

HELP = Help(
    summary="Remove background noise from Sv (De Robertis & Higginbottom 2007).",
    does=(
        "Runs echopype.clean.remove_background_noise on a flat Sv dataset. Noise "
        "is estimated from blocks of --ping_num pings x --range_sample_num "
        "samples; samples whose noise-corrected Sv is not more than "
        "--snr_threshold dB above the noise become NaN. The output is the input "
        "dataset plus two variables: Sv_noise (the noise estimate) and "
        "Sv_corrected (the cleaned Sv). The variable Sv itself is NOT changed."
    ),
    stdin=(
        "One flat Sv .nc/.netcdf4 path or gs:// URI, from aa-sv. It must contain "
        "Sv, echo_range and sound_absorption (not the EchoData file from aa-nc)."
    ),
    stdout="The cleaned file's absolute path (or gs:// URI).",
    options=[
        ("-o, --output_path PATH", "Explicit output; '_clean' is ALWAYS appended to its "
                                   "stem and .nc forced (-o out.nc writes out_clean.nc). "
                                   "Local path or gs:// URI."),
        ("--ping_num N", "pings per noise-estimation block (default: 20)"),
        ("--range_sample_num N", "range samples per block (default: 20)"),
        ("--background_noise_max=VALdB", "cap on the noise estimate, e.g. "
                                         "--background_noise_max=-125dB (write '=' before a "
                                         "negative value); default: no cap"),
        ("--snr_threshold DB", "minimum signal-to-noise ratio, a plain number in dB "
                               "(default: 3.0)"),
    ],
    science={
        "ping_num": "Pings per noise-estimation block.",
        "range_sample_num": "Range samples per noise-estimation block.",
        "background_noise_max": "Upper limit on the noise estimate; '-125dB' and "
                                "'-125.0dB' are the same value.",
        "snr_threshold": "Minimum SNR in dB; 3 and 3.0 are the same value.",
    },
    files=(
        "Reads a flat Sv NetCDF, local or gs://. Writes <base>_<hash8>.nc beside "
        "the input (current directory for gs:// input), or in --dest DIR|gs://PREFIX, "
        "or at -o (+'_clean'). AA_NAMING=legacy restores the old default "
        "<input stem>_clean.nc. An identical earlier result is reused, not recomputed."
    ),
    pipeline=(
        "After aa-sv: aa-nc | aa-sv | aa-clean | ... Tools downstream that read the "
        "variable Sv (aa-mvbs, aa-mvbs-index, aa-nasc, and aa-graph by default) still "
        "see the uncorrected Sv; the cleaned values are only in Sv_corrected "
        "(aa-graph --var Sv_corrected draws them)."
    ),
    examples=[
        "aa-nc D20160703-T060000.raw --sonar_model EK60 | aa-sv | aa-clean",
        "aa-clean sv.nc --ping_num 40 --range_sample_num 100 \\",
        "    --background_noise_max=-125dB --snr_threshold 5",
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
    Usage: aa-clean [OPTIONS] [INPUT_PATH]

    Arguments:
    INPUT_PATH                  Path (or gs:// URI) to a Sv .nc / .netcdf4 file
                                (typically the output of aa-sv).
                                Optional. Defaults to stdin if not provided.

    Options:
    -o, --output_path           Path (or gs:// URI) to save processed output.
                                '_clean' is ALWAYS appended to its stem and a
                                .nc suffix forced (-o out.nc writes
                                out_clean.nc), so the input file is never
                                silently overwritten.
                                Default: <base>_<hash>.nc beside the input
                                (AA_NAMING=legacy: <stem>_clean.nc).

    --ping_num                  Number of pings to use for background
                                noise estimation.
                                Default: 20

    --range_sample_num          Number of range samples to use for background
                                noise estimation.
                                Default: 20

    --background_noise_max      Optional upper bound on the estimated
                                background noise, e.g. "-125dB". Pass with
                                the dB unit suffix, and with '=' because the
                                value starts with '-':
                                --background_noise_max=-125dB
                                Default: None (no cap).

    --snr_threshold             SNR threshold as a number in dB. The 'dB'
                                unit suffix is appended automatically before
                                handing off to echopype.
                                Default: 3.0

    --base NAME                 Base name for the output.
    --dest DIR|gs://PREFIX      Write the default-named output there.
    --force                     Recompute even if an identical product exists.

    Description:
    Removes background noise from a Sv NetCDF using
    echopype.clean.remove_background_noise. The expected input is the
    output of aa-sv (a flat NetCDF Sv dataset, NOT a multi-group EchoData
    file from aa-nc).

    The output contains every variable of the input, unchanged (including
    Sv), plus Sv_noise (the noise estimate) and Sv_corrected (the cleaned
    Sv). Provenance (the input's chain plus this step) is embedded in the
    output; see aa-metadata.

    Pipeline example:
        aa-nc --sonar_model EK60 input.raw | aa-sv | aa-clean

    Direct example (writes /path/to/output_clean.nc):
        aa-clean /path/to/input_Sv.nc \\
                 --ping_num 50 --range_sample_num 200 \\
                 --snr_threshold 5.0 -o /path/to/output.nc
    """
    print(help_text)


def _build_parser():
    parser = argparse.ArgumentParser(
        description="Remove background noise from a Sv NetCDF file with Echopype.",
        add_help=False,
    )
    parser.add_argument(
        "input_path",
        type=str,
        nargs="?",
        help="Path or gs:// URI of the Sv .nc / .netcdf4 file.",
    )
    parser.add_argument(
        "-o", "--output_path",
        type=str,
        help="Path to save processed output. '_clean' is appended to the stem.",
    )
    parser.add_argument(
        "--ping_num",
        type=int,
        default=20,
        help="Number of pings to use for background noise estimation.",
    )
    parser.add_argument(
        "--range_sample_num",
        type=int,
        default=20,
        help="Number of range samples to use for background noise estimation.",
    )
    parser.add_argument(
        "--background_noise_max",
        type=str,
        default=None,
        help='Optional upper bound for background noise (e.g. "-125dB").',
    )
    parser.add_argument(
        "--snr_threshold",
        type=float,
        default=3.0,
        help="SNR threshold in dB (default: 3.0). 'dB' suffix added automatically.",
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

    allowed_extensions = {".netcdf4", ".nc"}
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
    # '-o' keeps its old rule: '_clean' is always appended and .nc forced.
    explicit = (naming.with_stem_suffix(args.output_path, "_clean", ".nc")
                if args.output_path else None)
    out = run.plan(
        ext=".nc",
        explicit=explicit,
        legacy=lambda: naming.with_stem_suffix(src.local, "_clean", ".nc"),
    )

    # Guard against clobbering the input — refuse rather than silently
    # corrupt the source file.
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
            "ping_num": args.ping_num,
            "range_sample_num": args.range_sample_num,
            "background_noise_max": args.background_noise_max,
            "snr_threshold": args.snr_threshold,
            "product": out.hash,
        }
        logger.debug(
            f"Executing aa-clean configured with [OPTIONS]:\n"
            f"{pprint.pformat(args_summary)}"
        )

        process_file(
            input_path=src.local,
            output_path=out.local,
            ping_num=args.ping_num,
            range_sample_num=args.range_sample_num,
            background_noise_max=args.background_noise_max,
            snr_threshold=args.snr_threshold,
        )

        logger.success(
            f"Generated {out.target} with aa-clean. "
            "Passing .nc path to stdout..."
        )
        # Pipe the output path (or URI) to stdout for the next tool
        run.finish(out)

    except Exception as e:
        logger.exception(f"Error during processing: {e}")
        sys.exit(1)


def clean_attrs(ds):
    """Replace None-valued attrs with 'NA' so the dataset is NetCDF-safe.
    NetCDF attrs cannot be None — to_netcdf will raise on serialization.
    Using 'NA' (matching aa-sv) rather than 'NaN' to avoid implying the
    attribute was a missing numeric value."""
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
    ping_num: int,
    range_sample_num: int,
    background_noise_max: str = None,
    snr_threshold: float = 3.0,
):
    """Load a Sv NetCDF, remove background noise, and save the result."""
    import xarray as xr  # deferred so --help stays fast
    from echopype.clean import remove_background_noise

    logger.info(f"Loading Sv dataset from {input_path}")
    # Note: this expects a flat Sv dataset (output of aa-sv via
    # ds_Sv.to_netcdf), NOT a multi-group EchoData file (output of
    # aa-nc via ed.to_netcdf). xr.open_dataset would only see the root
    # group of the latter and remove_background_noise would fail.
    ds_Sv = xr.open_dataset(input_path)

    try:
        logger.info(
            f"Removing background noise "
            f"(ping_num={ping_num}, range_sample_num={range_sample_num}, "
            f"background_noise_max={background_noise_max}, "
            f"SNR_threshold={snr_threshold}dB)"
        )
        ds_Sv_clean = remove_background_noise(
            ds_Sv,
            ping_num=ping_num,
            range_sample_num=range_sample_num,
            background_noise_max=background_noise_max,
            SNR_threshold=f"{snr_threshold}dB",
        )

        ds_Sv_clean = clean_attrs(ds_Sv_clean)

        # Force-load before writing so we don't depend on the source
        # file handle still being open during to_netcdf's lazy compute.
        ds_Sv_clean.load()
    finally:
        ds_Sv.close()

    output_path = Path(output_path).with_suffix(".nc")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Saving cleaned Sv dataset to {output_path}")
    ds_Sv_clean.to_netcdf(output_path)
    logger.success(f"Background noise removal complete: {output_path.resolve()}")


if __name__ == "__main__":
    main()
