#!/usr/bin/env python3
"""
aa-freqdiff

Console tool to compute a frequency-differencing mask using Echopype.

This wraps:
  echopype.mask.frequency_differencing(
      source_Sv, storage_options={}, freqABEq=None, chanABEq=None
  )

The mask is True where Sv at one frequency (or channel) minus Sv at another
satisfies a dB criterion, e.g. 38kHz - 120kHz >= 10dB.

Pipeline-friendly: reads the input path (or gs:// URI) from a positional
arg or stdin, writes the mask's path to stdout, logs to stderr. The output
carries the input's provenance plus this step.
"""

import argparse
import io
import pprint
import re
import sys
from contextlib import redirect_stdout
from pathlib import Path

from loguru import logger

from aalibrary.console._core import (
    Help, Run, ToolSpec, add_common_flags, canon, render, show_help, stdio,
)

_QUOTED = re.compile(r'("[^"]*")')


def _ab_equation(value):
    """Canonical freqABEq / chanABEq text for the hash.

    Whitespace and number spelling outside double quotes do not matter
    (canon.expression: '38.0kHz - 120kHz >= 10.0dB' == '38kHz-120kHz>=10dB').
    Quoted channel names are kept verbatim: echopype matches them exactly,
    and canon.expression would squeeze their internal spaces and zeros.
    """
    if value is None:
        return None
    parts = _QUOTED.split(str(value))
    return "".join(p if len(p) >= 2 and p[0] == p[-1] == '"' else canon.expression(p)
                   for p in parts)


SPEC = ToolSpec(
    name="aa-freqdiff",
    role="transform",
    kind="mask",
    op="echopype.mask.frequency_differencing",
    op_version=1,
    params={"freqABEq": _ab_equation, "chanABEq": _ab_equation},
)

HELP = Help(
    summary="Frequency-differencing mask: Sv(A) - Sv(B) <op> N dB.",
    does=(
        "Runs echopype.mask.frequency_differencing with one criterion, "
        "'A - B <op> N dB' (op one of > < >= <= ==), A and B given as nominal "
        "frequencies (--freqABEq) or channel names (--chanABEq). Useful for "
        "separating scatterers by frequency response (e.g. krill).\n\n"
        "The mask file holds one variable, freqdiff_mask: boolean, True = the "
        "criterion holds, False = it does not or either Sv is NaN, dims "
        "(ping_time, range_sample) -- no channel dimension."
    ),
    stdin=(
        "One Sv NetCDF (or Zarr) path or gs:// URI with a channel coordinate and "
        "frequency_nominal, e.g. from aa-sv."
    ),
    stdout="The mask's absolute path (or gs:// URI), one line.",
    options=[
        ("--freqABEq 'A - B op NdB'", "Frequencies WITHOUT quotes, in Hz with an "
                                      "optional k/M/G prefix, matching "
                                      "frequency_nominal: '38kHz - 120kHz >= 10dB'."),
        ("--chanABEq '\"A\" - \"B\" op NdB'", "Channel names in double quotes, exactly "
                                            "as in the channel coordinate."),
        ("-o, --output_path PATH", "Used exactly as given (no extension added). Local "
                                   "path or gs:// URI."),
        ("--quiet", "Only warnings and errors on stderr."),
    ],
    science={
        "freqABEq": "Criterion by frequency. Give exactly one of the two.",
        "chanABEq": "Criterion by channel name. Give exactly one of the two.",
    },
    files=(
        "Reads NetCDF or Zarr, local or gs://. Writes <base>_<hash>.nc beside the "
        "input (current directory for gs:// input), or -o, or --dest. "
        "AA_NAMING=legacy: <stem>_freqdiff.nc beside the input. An identical "
        "earlier result is reused."
    ),
    pipeline="After aa-sv (or aa-clean): ... | aa-sv | aa-freqdiff --freqABEq '...' | aa-graph",
    examples=[
        "aa-nc x.raw --sonar_model EK60 | aa-sv | aa-freqdiff --freqABEq '120kHz - 38kHz > 2dB'",
        "aa-freqdiff sv.nc -o krill_mask.nc --chanABEq \\",
        "    '\"GPT  120 kHz 00907205c002-1 ES120-7C\" - \"GPT   38 kHz 00907205c001-1 ES38B\" <= 5dB'",
    ],
    notes=[
        "echopype 0.11.1 accepts only a non-negative number of dB: 'A - B < -5dB' "
        "is rejected ('Invalid operator!'). Swap the operands instead: "
        "'B - A > 5dB'. Quoted frequencies ('\"38kHz\" - ...') are rejected too "
        "('Invalid freqAB Equation!').",
    ],
)


def print_help():
    """Curated help (also used by the docs generator)."""
    sys.stdout.write(render(SPEC, HELP, _build_parser()))


def print_help_full():
    """The complete reference (--help-all)."""
    help_text = """
    Usage: aa-freqdiff [OPTIONS] [INPUT_PATH]

    Arguments:
      INPUT_PATH                Path (or gs:// URI) to a NetCDF/Zarr file
                                containing Sv with a `channel` dimension and
                                `frequency_nominal`. Optional — defaults to stdin
                                if not provided (an empty stdin is an error,
                                exit 1).

    Options:
      -o, --output_path PATH    Where to write the mask NetCDF, used exactly as
                                given; local path or gs:// URI. Default:
                                <base>_<hash>.nc beside the input
                                (AA_NAMING=legacy: <stem>_freqdiff.nc).
      --freqABEq STR            Frequency differencing expression, frequencies
                                without quotes, e.g. '38.0kHz - 120.0kHz>=10.0dB'.
      --chanABEq STR            Channel-based differencing expression, channel
                                names in double quotes, e.g. '"chan1" - "chan2"<5dB'.
                                Exactly one of --freqABEq / --chanABEq.
                                Operators: > < >= <= ==. The dB value must be
                                non-negative (swap A and B for a negative one).
      --quiet                   Suppress logger info (only warnings/errors on
                                stderr); stdout carries only the output path
                                either way.
      --base NAME               Base name for the output.
      --dest DIR|gs://PREFIX    Write the default-named output there.
      --force                   Recompute even if an identical product exists.
      -h, --help                Short help. --help-all: this text.

    Description:
      Computes a boolean mask of Sv data where one frequency minus another
      meets a user-specified threshold/difference. Useful for identifying
      scatterers with different frequency responses (for example krill).
      The mask variable is 'freqdiff_mask', True where the criterion holds,
      dims (ping_time, range_sample). Provenance (the input's chain plus
      this step) is embedded; see aa-metadata.

    Examples:
      aa-freqdiff data.nc --freqABEq '38.0kHz - 120.0kHz>=12.0dB' -o out_mask.nc
      aa-freqdiff data.nc --chanABEq '"chan1" - "chan2"<5dB'
    """
    print(help_text)


def _add_basic_attrs(ds) -> None:
    """Replace None attrs with strings to avoid NetCDF writer issues."""
    for k, v in list(ds.attrs.items()):
        if v is None:
            ds.attrs[k] = "NA"
    for var in ds.data_vars:
        for k, v in list(ds[var].attrs.items()):
            if v is None:
                ds[var].attrs[k] = "NA"


def _build_parser():
    parser = argparse.ArgumentParser(
        description="Compute a frequency-differencing mask (Sv differences) using Echopype.",
        add_help=False,
    )
    parser.add_argument(
        "input_path",
        type=str,
        nargs="?",
        help="Path or gs:// URI of a dataset (NetCDF or Zarr) containing Sv with "
             "channel/frequency_nominal.",
    )
    parser.add_argument(
        "-o", "--output_path",
        type=str,
        help="Output path for mask NetCDF, used as given (default: <base>_<hash>.nc).",
    )
    parser.add_argument(
        "--freqABEq",
        dest="freqABEq",
        default=None,
        help="Expression for differencing by frequency, e.g. '38.0kHz - 120.0kHz>=10.0dB'."
    )
    parser.add_argument(
        "--chanABEq",
        dest="chanABEq",
        default=None,
        help="Expression for differencing by channel names, e.g. '\"chan1\" - \"chan2\"<5dB'."
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress informational logging; only print output path."
    )
    add_common_flags(parser)
    return parser


def main():
    """Entry point for the aa-freqdiff CLI."""
    # No args on a terminal: help. (An empty pipe is an error: see stdio.)
    if len(sys.argv) == 1 and not stdio.stdin_is_piped():
        print_help()
        sys.exit(0)

    parser = _build_parser()
    if show_help(SPEC, HELP, parser, full=print_help_full):
        sys.exit(0)

    args = parser.parse_args()

    # --quiet: warnings and errors only. Without it, loguru's default sink
    # (every level, to stderr) stays, as before.
    if args.quiet:
        logger.remove()
        logger.add(sys.stderr, level="WARNING")

    if args.freqABEq is None and args.chanABEq is None:
        logger.error("Either --freqABEq or --chanABEq must be provided (not both).")
        sys.exit(1)
    if args.freqABEq is not None and args.chanABEq is not None:
        logger.error("Only one of --freqABEq or --chanABEq may be provided.")
        sys.exit(1)

    # Input: positional > stdin; local path or gs:// URI
    token = stdio.one_input(args.input_path, SPEC.name)
    run = Run(SPEC, args)
    src = run.input(token)

    # -o used verbatim, as always.
    out = run.plan(
        ext=".nc",
        explicit=args.output_path or None,
        legacy=lambda: src.local.with_stem(src.local.stem + "_freqdiff").with_suffix(".nc"),
    )

    # Guard against clobbering the input
    if not out.remote and Path(out.target).resolve() == src.local.resolve():
        logger.error(f"Refusing to overwrite input file: {src.local.resolve()}")
        sys.exit(1)

    if run.reusable(out):
        run.finish(out)
        return

    try:
        import xarray as xr  # deferred so --help stays fast
        from echopype.mask import frequency_differencing

        # Load dataset quietly (suppress chatter)
        f = io.StringIO()
        with redirect_stdout(f):
            ds = xr.open_dataset(src.local)

        logger.info("Computing frequency-differencing mask...")
        mask = frequency_differencing(
            source_Sv=ds,
            storage_options={},   # no special options here
            freqABEq=args.freqABEq,
            chanABEq=args.chanABEq
        )

        # Wrap DataArray into Dataset for writing
        mask_ds = mask.to_dataset(name="freqdiff_mask")
        _add_basic_attrs(mask_ds)

        out.local.parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"Saving mask to {out.target} ...")
        mask_ds.to_netcdf(out.local, mode="w", format="NETCDF4")

        logger.debug(f"\naa-freqdiff args:\n{pprint.pformat(vars(args))}")
        run.finish(out)
        logger.info("Frequency differencing mask complete.")

    except Exception as e:
        logger.exception(f"Error during frequency differencing: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
