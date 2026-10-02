#!/usr/bin/env python3
"""
aa-ts

Console tool for computing TS (target strength) from a .nc EchoData file
using Echopype, and saving back to NetCDF.

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
    name="aa-ts",
    role="transform",
    kind="ts",
    op="echopype.calibrate.compute_TS",
    op_version=1,
    params={
        # The parsed float dicts are passed to Run() explicitly (see main);
        # these canonicalizers give the same result and feed --help.
        "env_params": canon.kv(canon.number),
        "cal_params": canon.kv(canon.number),
        "waveform_mode": _waveform,
        "encode_mode": canon.choice("lower"),
    },
)

HELP = Help(
    summary="Calibrate EchoData to target strength (TS).",
    does=(
        "Runs echopype.calibrate.compute_TS on a converted EchoData file and "
        "writes a flat TS dataset (TS, echo_range, ... on channel x ping_time x "
        "range_sample). Environmental and calibration values stored in the file "
        "are used unless you override them with --env-param / --cal-param."
    ),
    stdin="One EchoData .nc/.netcdf4 path or gs:// URI, from aa-nc, aa-ed or aa-combine.",
    stdout="The TS file's absolute path (or gs:// URI).",
    options=[
        ("-o, --output_path PATH", "Explicit output; '_ts' is ALWAYS appended to its stem "
                                   "and .nc forced (-o out.nc writes out_ts.nc). Local "
                                   "path or gs:// URI."),
        ("--env-param KEY=VALUE", "override an environmental value; repeatable, "
                                  "e.g. --env-param sound_speed=1500"),
        ("--cal-param KEY=VALUE", "override a calibration value; repeatable, "
                                  "e.g. --cal-param gain_correction=25.9"),
        ("--waveform_mode CW|BB|FM", "EK80 waveform (default: CW). EK60/AZFP: always CW."),
        ("--encode_mode complex|power", "EK80 encoding (default: complex). EK60/AZFP: "
                                        "always power."),
    ],
    science={
        "env_params": "Environmental overrides (--env-param), as numbers; order and "
                      "1500 vs 1500.0 do not matter.",
        "cal_params": "Calibration overrides (--cal-param), as numbers.",
        "waveform_mode": "EK80 waveform. FM and BB are the same computation.",
        "encode_mode": "EK80 encoding.",
    },
    files=(
        "Reads EchoData .nc/.netcdf4, local or gs://. Writes <base>_<hash8>.nc "
        "beside the input (current directory for gs:// input), or in --dest "
        "DIR|gs://PREFIX, or at -o (+'_ts'). AA_NAMING=legacy restores the old "
        "default <input stem>_ts.nc. An identical earlier result is reused."
    ),
    pipeline=(
        "Parallel to aa-sv, on the same EchoData: aa-nc x.raw --sonar_model EK60 | aa-ts. "
        "Its output holds TS, not Sv, so it does not feed aa-clean, aa-mvbs or aa-nasc."
    ),
    examples=[
        "aa-nc D20160703-T060000.raw --sonar_model EK60 | aa-ts",
        "aa-ts file.nc --env-param sound_speed=1500 --env-param temperature=10.5",
    ],
    notes=["For EK60 and AZFP data echopype ignores --waveform_mode and --encode_mode "
           "(it always uses CW power samples), but they are still recorded."],
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
    Usage: aa-ts [OPTIONS] [INPUT_PATH]

    Arguments:
    INPUT_PATH                  Path (or gs:// URI) to the .nc / .netcdf4
                                EchoData file.
                                Optional. Defaults to stdin if not provided.

    Options:
    -o, --output_path           Path (or gs:// URI) to save processed output.
                                '_ts' is appended to its stem and a .nc
                                suffix forced (-o out.nc writes out_ts.nc).
                                Default: <base>_<hash>.nc beside the input
                                (AA_NAMING=legacy: <stem>_ts.nc).

    --env-param KEY=VALUE       Environmental parameter override (repeatable).
                                Example: --env-param sound_speed=1500
                                         --env-param temperature=10.5

    --cal-param KEY=VALUE       Calibration parameter override (repeatable).
                                Example: --cal-param gain_correction=1.0

    --waveform_mode             For EK80 echosounders: waveform mode.
                                Choices: CW, BB, FM   (default: CW)

    --encode_mode               For EK80 echosounders: encoding mode.
                                Choices: complex, power   (default: complex)

    --base NAME                 Base name for the output.
    --dest DIR|gs://PREFIX      Write the default-named output there.
    --force                     Recompute even if an identical product exists.

    Description:
    This tool computes TS (target strength) from a previously-converted
    NetCDF EchoData file using echopype.calibrate.compute_TS, and saves
    the result to a new .nc file. The output path is printed to stdout
    for piping into the next stage of the pipeline. Provenance (the input's
    chain plus this step) is embedded in the output; see aa-metadata.

    Example (writes /path/to/input_ts.nc):
    aa-ts /path/to/input.nc --env-param sound_speed=1500 \\
        --cal-param gain_correction=1.0 -o /path/to/input.nc
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


def _build_parser():
    parser = argparse.ArgumentParser(
        description="Compute TS from a NetCDF EchoData file with Echopype.",
        add_help=False,  # we handle help ourselves
    )
    parser.add_argument(
        "input_path",
        type=str,
        nargs="?",
        help="Path or gs:// URI of the .nc / .netcdf4 EchoData file.",
    )
    parser.add_argument(
        "-o", "--output_path",
        type=str,
        help="Path to save processed output. '_ts' is appended to the stem.",
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
    token = stdio.one_input(args.input_path, SPEC.name)
    # Hash exactly the dicts echopype receives.
    run = Run(SPEC, args, params={"env_params": env_params, "cal_params": cal_params})
    src = run.input(token)

    allowed_extensions = {".netcdf4": "netcdf", ".nc": "netcdf"}
    ext = src.local.suffix.lower()
    if ext not in allowed_extensions:
        logger.error(
            f"'{src.name}' is not a supported file type. "
            f"Allowed: {', '.join(allowed_extensions.keys())}"
        )
        sys.exit(1)

    # ---------------------------
    # Resolve output path
    # ---------------------------
    # '-o' keeps its old rule: '_ts' is always appended and .nc forced.
    explicit = (naming.with_stem_suffix(args.output_path, "_ts", ".nc")
                if args.output_path else None)
    out = run.plan(
        ext=".nc",
        explicit=explicit,
        legacy=lambda: naming.with_stem_suffix(src.local, "_ts", ".nc"),
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
            "env_params": env_params,
            "cal_params": cal_params,
            "waveform_mode": args.waveform_mode,
            "encode_mode": args.encode_mode,
            "product": out.hash,
        }
        logger.debug(
            f"Executing aa-ts configured with [OPTIONS]:\n"
            f"{pprint.pformat(args_summary)}"
        )

        process_file(
            input_path=src.local,
            output_path=out.local,
            env_params=env_params,
            cal_params=cal_params,
            waveform_mode=args.waveform_mode,
            encode_mode=args.encode_mode,
        )

        logger.success(f"Generated {out.target} with aa-ts.")
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
    env_params=None,
    cal_params=None,
    waveform_mode: str = "CW",
    encode_mode: str = "complex",
):
    """Load EchoData from NetCDF, compute TS, and save to NetCDF."""
    import echopype as ep  # deferred so --help stays fast

    logger.info(f"Generating EchoData from NetCDF file:\n  {input_path}")
    ed = ep.open_converted(str(input_path))

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

    output_path = Path(output_path).with_suffix(".nc")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Saving TS dataset to {output_path}")
    ds_TS.to_netcdf(output_path)

    logger.success(f"TS computation complete: {output_path.resolve()}")


if __name__ == "__main__":
    main()
