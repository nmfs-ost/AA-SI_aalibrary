#!/usr/bin/env python3
"""
Console tool for computing TS (target strength) from a .nc EchoData file
using Echopype, and saving back to NetCDF.

Pipeline-friendly: reads input path from positional arg or stdin, writes
output path to stdout, all logs to stderr.
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

# Now the heavy imports - anything they log gets squashed
import argparse
import pprint
from pathlib import Path

import echopype as ep


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
    help_text = """
    Usage: aa-ts [OPTIONS] [INPUT_PATH]

    Arguments:
    INPUT_PATH                  Path to the .nc / .netcdf4 EchoData file.
                                Optional. Defaults to stdin if not provided.

    Options:
    -o, --output_path           Path to save processed output.
                                Default: same directory as input, with '_ts'
                                appended to the stem and a .nc suffix.

    --env-param KEY=VALUE       Environmental parameter override (repeatable).
                                Example: --env-param sound_speed=1500
                                         --env-param temperature=10.5

    --cal-param KEY=VALUE       Calibration parameter override (repeatable).
                                Example: --cal-param gain_correction=1.0

    --waveform_mode             For EK80 echosounders: waveform mode.
                                Choices: CW, BB, FM   (default: CW)

    --encode_mode               For EK80 echosounders: encoding mode.
                                Choices: complex, power   (default: complex)

    Description:
    This tool computes TS (target strength) from a previously-converted
    NetCDF EchoData file using echopype.calibrate.compute_TS, and saves
    the result to a new .nc file. The output path is printed to stdout
    for piping into the next stage of the pipeline.

    Example:
    aa-ts /path/to/input.nc --env-param sound_speed=1500 \\
        --cal-param gain_correction=1.0 -o /path/to/input_ts.nc
    """
    print(help_text)


def parse_kv_pairs(pair_list):
    """Parse a list of key=value strings into a dict of floats.

    Returns None when the input is None or empty so we can pass it straight
    through to echopype (which treats None as 'use defaults').
    """
    if not pair_list:
        return None
    out = {}
    for pair in pair_list:
        if "=" not in pair:
            raise argparse.ArgumentTypeError(
                f"Invalid key=value pair: {pair!r} (expected e.g. sound_speed=1500)"
            )
        key, value = pair.split("=", 1)
        try:
            out[key.strip()] = float(value)
        except ValueError:
            raise argparse.ArgumentTypeError(
                f"Could not parse value for {key!r}: {value!r} is not a number"
            )
    return out


def main():
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
        description="Compute TS from a NetCDF EchoData file with Echopype.",
        add_help=False,  # we handle help ourselves
    )

    parser.add_argument(
        "input_path",
        type=Path,
        nargs="?",
        help="Path to the .nc / .netcdf4 EchoData file.",
    )
    parser.add_argument(
        "-o", "--output_path",
        type=Path,
        help="Path to save processed output. Default appends '_ts' to the input stem.",
    )
    parser.add_argument(
        "--env-param",
        dest="env_params",
        action="append",
        default=None,
        metavar="KEY=VALUE",
        help="Environmental parameter override (repeatable). Example: sound_speed=1500",
    )
    parser.add_argument(
        "--cal-param",
        dest="cal_params",
        action="append",
        default=None,
        metavar="KEY=VALUE",
        help="Calibration parameter override (repeatable). Example: gain_correction=1.0",
    )
    parser.add_argument(
        "--waveform_mode",
        type=str,
        default="CW",
        choices=["CW", "BB", "FM"],
        help="For EK80 Echosounders: waveform mode (default: CW).",
    )
    parser.add_argument(
        "--encode_mode",
        type=str,
        default="complex",
        choices=["complex", "power"],
        help="For EK80 Echosounders: encoding mode (default: complex).",
    )

    args = parser.parse_args()

    # ---------------------------
    # Parse env / cal kv pairs
    # ---------------------------
    try:
        env_params = parse_kv_pairs(args.env_params)
        cal_params = parse_kv_pairs(args.cal_params)
    except argparse.ArgumentTypeError as exc:
        logger.error(str(exc))
        sys.exit(2)

    # ---------------------------
    # Validate input
    # ---------------------------
    if args.input_path is None:
        if sys.stdin.isatty():
            logger.error("No input path provided and no stdin available.")
            sys.exit(1)
        args.input_path = Path(sys.stdin.readline().strip())
        logger.info(f"Read input path from stdin: {args.input_path}")

    if not args.input_path.exists():
        logger.error(f"File '{args.input_path}' does not exist.")
        sys.exit(1)

    allowed_extensions = {".netcdf4": "netcdf", ".nc": "netcdf"}
    ext = args.input_path.suffix.lower()
    if ext not in allowed_extensions:
        logger.error(
            f"'{args.input_path.name}' is not a supported file type. "
            f"Allowed: {', '.join(allowed_extensions.keys())}"
        )
        sys.exit(1)

    # ---------------------------
    # Resolve output path
    # ---------------------------
    if args.output_path is None:
        args.output_path = args.input_path

    args.output_path = args.output_path.with_stem(args.output_path.stem + "_ts")
    args.output_path = args.output_path.with_suffix(".nc")

    # Guard against clobbering the input
    if args.output_path.resolve() == args.input_path.resolve():
        logger.error(f"Refusing to overwrite input file: {args.input_path.resolve()}")
        sys.exit(1)

    # ---------------------------
    # Process file
    # ---------------------------
    try:
        args_summary = {
            "input_path": args.input_path,
            "output_path": args.output_path,
            "env_params": env_params,
            "cal_params": cal_params,
            "waveform_mode": args.waveform_mode,
            "encode_mode": args.encode_mode,
        }
        logger.debug(
            f"Executing aa-ts configured with [OPTIONS]:\n"
            f"{pprint.pformat(args_summary)}"
        )

        process_file(
            input_path=args.input_path,
            output_path=args.output_path,
            env_params=env_params,
            cal_params=cal_params,
            waveform_mode=args.waveform_mode,
            encode_mode=args.encode_mode,
        )

        logger.success(f"Generated {args.output_path.resolve()} with aa-ts.")
        print(args.output_path.resolve())

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
    env_params=None,
    cal_params=None,
    waveform_mode: str = "CW",
    encode_mode: str = "complex",
):
    """Load EchoData from NetCDF, compute TS, and save to NetCDF."""

    logger.info(f"Generating EchoData from NetCDF file:\n  {input_path}")
    ed = ep.open_converted(input_path)

    logger.info(
        f"Computing TS from EchoData "
        f"(waveform_mode={waveform_mode}, encode_mode={encode_mode})"
    )

    # Build kwargs lazily so we only pass overrides the user actually provided.
    # echopype's compute_TS treats None as 'use defaults', so omitting the
    # kwarg vs passing None is equivalent — being explicit keeps the call site
    # readable and avoids feeding None into a future version that gets stricter.
    compute_kwargs = {
        "waveform_mode": waveform_mode,
        "encode_mode": encode_mode,
    }
    if env_params is not None:
        compute_kwargs["env_params"] = env_params
    if cal_params is not None:
        compute_kwargs["cal_params"] = cal_params

    ds_TS = ep.calibrate.compute_TS(ed, **compute_kwargs)
    ds_TS = clean_attrs(ds_TS)

    output_path = output_path.with_suffix(".nc")
    logger.info(f"Saving TS dataset to {output_path}")
    ds_TS.to_netcdf(output_path)

    logger.success(f"TS computation complete: {output_path.resolve()}")


if __name__ == "__main__":
    main()