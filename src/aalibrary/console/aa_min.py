#!/usr/bin/env python3
"""
aa-min

Console tool for creating an impulse-noise mask using
echopype.clean.mask_impulse_noise and saving the mask to NetCDF. The same
computation as aa-impulse, with underscore option names and the mask
stored under the variable name 'Sv' (see --help).

Usage examples:
  aa-min /path/to/input.nc --depth_bin 5m --num_side_pings 2 -o /path/to/output.nc
  echo /path/to/input.nc | aa-min --impulse_noise_threshold 12.0dB

Pipeline-friendly: reads the input path (or gs:// URI) from a positional
arg or stdin, writes the mask's path to stdout, all logs to stderr. The
output carries the input's provenance plus this step.
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

SUFFIX = "_mask-impulse-noise"

SPEC = ToolSpec(
    name="aa-min",
    role="transform",
    kind="mask",
    op="echopype.clean.mask_impulse_noise",
    op_version=1,
    params={
        "depth_bin": canon.quantity("m"),
        "num_side_pings": canon.integer,
        "impulse_noise_threshold": canon.quantity("dB"),
        "range_var": canon.choice(),
        "use_index_binning": canon.boolean,
    },
)

HELP = Help(
    summary="Impulse-noise mask, stored under the variable name 'Sv'.",
    does=(
        "The same computation as aa-impulse: Sv is averaged in --depth_bin bins "
        "and a sample is impulse noise when it is more than "
        "--impulse_noise_threshold above BOTH the ping --num_side_pings before "
        "it and the ping --num_side_pings after it (Ryan et al. 2015).\n\n"
        "The output holds one variable NAMED 'Sv' that is the MASK, not Sv: "
        "float64, 1 = impulse noise, 0 = keep, dims (channel, range_sample, "
        "ping_time), with Sv's long_name/units attributes copied by echopype. "
        "The name is kept for compatibility; aa-impulse writes the same mask "
        "as 'impulse_mask'."
    ),
    stdin=(
        "One Sv NetCDF (.nc/.netcdf4) path or gs:// URI that has the --range_var "
        "variable: depth (add it with aa-depth) or echo_range."
    ),
    stdout="The mask's absolute path (or gs:// URI), one line.",
    options=[
        ("-o, --output_path PATH", "Explicit output; '_mask-impulse-noise' is "
                                   "appended to its stem and .nc forced, as always. "
                                   "Local path or gs:// URI."),
    ],
    science={
        "depth_bin": "Vertical averaging bin before the comparison, e.g. 5m.",
        "num_side_pings": "Compare each ping with the ping this many pings before "
                          "and after it.",
        "impulse_noise_threshold": "dB above both neighbours that counts as impulse "
                                   "noise, e.g. 10dB.",
        "range_var": "Vertical variable: depth or echo_range.",
        "use_index_binning": "Bin by range_sample index (assumes uniform sample "
                             "spacing per channel). Faster.",
    },
    files=(
        "Reads NetCDF, local or gs://. Writes <base>_<hash>.nc beside the input "
        "(current directory for gs:// input), or -o, or --dest. AA_NAMING=legacy: "
        "<stem>_mask-impulse-noise.nc beside the input. An identical earlier "
        "result is reused."
    ),
    pipeline=(
        "After aa-sv and aa-depth: ... | aa-sv | aa-depth | aa-min | aa-graph. "
        "Downstream tools that look for a variable called Sv will find this mask."
    ),
    examples=[
        "aa-nc x.raw --sonar_model EK60 | aa-sv | aa-depth | aa-min --use_index_binning",
        "aa-min sv_depth.nc --impulse_noise_threshold 12dB --use_index_binning "
        "-o masks/run1.nc   # -> masks/run1_mask-impulse-noise.nc",
    ],
    notes=[
        "--use_index_binning is needed when the channels cover different depth "
        "ranges (e.g. EK60 38 + 120 kHz): echopype 0.11.1's default depth "
        "binning then fails with \"conflicting sizes for dimension 'depth_bins'\".",
    ],
)


def print_help():
    """Curated help (also used by the docs generator)."""
    sys.stdout.write(render(SPEC, HELP, _build_parser()))


def print_help_full():
    help_text = """
    Usage: aa-min [OPTIONS] [INPUT_PATH]

    Arguments:
    INPUT_PATH                  Path (or gs:// URI) to the .nc / .netcdf4 Sv
                                file. Optional. Defaults to stdin if not
                                provided (an empty stdin is an error, exit 1).

    Options:
    -o, --output_path           Path (or gs:// URI) to save processed output
                                (NetCDF). "_mask-impulse-noise" is appended to
                                its stem and a .nc suffix forced.
                                Default: <base>_<hash>.nc beside the input
                                (AA_NAMING=legacy: input file with
                                "_mask-impulse-noise" appended to the stem).

    --depth_bin                 Downsampling vertical bin size (default: 5m)
    --num_side_pings            Number of side pings for two-sided comparison (default: 2)
    --impulse_noise_threshold   Threshold (dB) for impulse detection (default: "10.0dB")
    --range_var                 Range coordinate: "depth" or "echo_range" (default: depth)
    --use_index_binning         Use index-based binning for speed (default: False)

    --base NAME                 Base name for the output.
    --dest DIR|gs://PREFIX      Write the default-named output there.
    --force                     Recompute even if an identical product exists.

    Output:
    The mask is written as a single variable named "Sv" (float64,
    1 = impulse noise, 0 = keep, dims channel x range_sample x ping_time).
    Despite its name it is the mask, not Sv. Provenance (the input's chain
    plus this step) is embedded; see aa-metadata.

    Example:
    aa-min /path/to/input.nc --depth_bin 5m --num_side_pings 3 \\
        --impulse_noise_threshold "12.0dB" --use_index_binning -o /path/to/output_mask.nc
    """
    print(help_text)


def _build_parser():
    parser = argparse.ArgumentParser(
        description="Create an impulse-noise mask using echopype.clean.mask_impulse_noise.",
        add_help=False,
    )

    parser.add_argument("input_path", type=str, nargs="?",
                        help="Path or gs:// URI of the .netcdf4 file.")
    parser.add_argument("-o", "--output_path", type=str,
                        help="Path to save processed output ('_mask-impulse-noise' is appended).")

    parser.add_argument("--depth_bin", type=str, default="5m",
                        help="Downsampling bin size along vertical range variable (default: 5m).")
    parser.add_argument("--num_side_pings", type=int, default=2,
                        help="Number of side pings for two-sided comparison (default: 2).")
    parser.add_argument("--impulse_noise_threshold", type=str, default="10.0dB",
                        help='Impulse noise threshold, as a string with units (default: "10.0dB").')
    parser.add_argument("--range_var", type=str, choices=["depth", "echo_range"], default="depth",
                        help='Vertical axis range variable: "depth" or "echo_range" (default: depth).')
    parser.add_argument("--use_index_binning", action="store_true",
                        help="Use index-based binning for speed (default: False).")
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

    # Input: positional > stdin; local path or gs:// URI
    token = stdio.one_input(args.input_path, SPEC.name)
    run = Run(SPEC, args)
    src = run.input(token)

    allowed_extensions = {".netcdf4": "netcdf", ".nc": "netcdf"}
    ext = src.local.suffix.lower()
    if ext not in allowed_extensions:
        logger.error(
            f"'{src.name}' is not a supported file type. "
            f"Allowed: {', '.join(allowed_extensions.keys())}"
        )
        sys.exit(1)

    # -o: append _mask-impulse-noise to the stem and force .nc, as always.
    explicit = (naming.with_stem_suffix(args.output_path, SUFFIX, ".nc")
                if args.output_path else None)
    out = run.plan(
        ext=".nc",
        explicit=explicit,
        legacy=lambda: naming.with_stem_suffix(src.local, SUFFIX, ".nc"),
    )
    logger.info(f"Output path set to: {out.target}")

    # Guard against clobbering the input
    if not out.remote and Path(out.target).resolve() == src.local.resolve():
        logger.error(f"Refusing to overwrite input file: {src.local.resolve()}")
        sys.exit(1)

    if run.reusable(out):
        run.finish(out)
        return

    # Pretty-print args
    pretty_args = pprint.pformat(vars(args) | {"output": out.target, "product": out.hash})
    logger.debug(
        f"Executing aa-min configured with [OPTIONS]:\n{pretty_args}\n"
        "* ( Each aa-min associated option_name may be overridden using --option_name value )"
    )
    # Call processor
    try:
        process_file(
            input_path=src.local,
            output_path=out.local,
            depth_bin=args.depth_bin,
            num_side_pings=args.num_side_pings,
            impulse_noise_threshold=args.impulse_noise_threshold,
            range_var=args.range_var,
            use_index_binning=args.use_index_binning,
        )
        logger.success(f"Generated {out.target} with aa-min.")
        # Emit the output path (or URI) to stdout for piping
        run.finish(out)

    except Exception as e:
        logger.exception(f"Error during processing: {e}")
        sys.exit(1)


def process_file(
    input_path: Path,
    output_path: Path,
    depth_bin: str = "5m",
    num_side_pings: int = 2,
    impulse_noise_threshold: str = "10.0dB",
    range_var: str = "depth",
    use_index_binning: bool = False
):
    """
    Load the Sv dataset, compute the impulse-noise mask, and save the mask
    DataArray to NetCDF. echopype names that DataArray 'Sv', so the file's
    single variable is 'Sv' (1 = impulse noise); kept as is for compatibility.
    """
    import xarray as xr  # deferred so --help stays fast
    from echopype.clean import mask_impulse_noise

    logger.info(f"Loading NetCDF file {input_path} into xarray dataset")
    ds_Sv = xr.open_dataset(input_path)

    da_mask = mask_impulse_noise(
        ds_Sv,
        depth_bin=depth_bin,
        num_side_pings=num_side_pings,
        impulse_noise_threshold=impulse_noise_threshold,
        range_var=range_var,
        use_index_binning=use_index_binning,
    )

    # Save to NetCDF
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Saving the impulse noise mask to {output_path} ...")
    da_mask.to_netcdf(output_path, mode="w", format="NETCDF4")


if __name__ == "__main__":
    main()
