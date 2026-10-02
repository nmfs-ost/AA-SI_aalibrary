#!/usr/bin/env python3
"""
aa-swap-freq

Console tool to swap the 'channel' dimension with 'frequency_nominal' using
Echopype, so frequency becomes the primary dimension/coordinate.

Pipeline-friendly: reads the input path (or gs:// URI) from the positional
argument or stdin, writes the output path to stdout, all logs to stderr.
The output carries the input's provenance plus this step.
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
import io
import pprint
from contextlib import redirect_stdout
from pathlib import Path

from aalibrary.console._core import (
    Help, Run, ToolSpec, add_common_flags, render, show_help, stdio, uris,
)

# numpy / xarray / echopype are imported inside the functions that use them
# so --help stays fast.

# A structural change with no options: the op alone gives the product its
# own hash.
SPEC = ToolSpec(
    name="aa-swap-freq",
    role="transform",
    kind="sv",
    op="echopype.consolidate.swap_dims_channel_frequency",
    op_version=1,
    params={},
)

HELP = Help(
    summary="Index a dataset by frequency_nominal instead of channel.",
    does=(
        "Runs echopype.consolidate.swap_dims_channel_frequency: every variable "
        "on the 'channel' dimension is re-indexed by 'frequency_nominal' (Hz, e.g. "
        "38000., 120000.), so you can select with .sel(frequency_nominal=38000). "
        "Values are not changed. Needs unique nominal frequencies. The product "
        "kind is the input's (sv, mvbs, mask, ...)."
    ),
    stdin="One NetCDF path or gs:// URI with a 'channel' dimension and 'frequency_nominal' (Sv, MVBS, ...).",
    stdout="The output file's absolute path (or gs:// URI).",
    options=[
        ("--check-unique", "fail early (exit 1) if frequency_nominal is missing or has "
                           "duplicates"),
        ("--no-overwrite", "exit 1 instead of replacing a different existing output"),
        ("-o, --output_path PATH", "Explicit output, used exactly as given. Local path or "
                                   "gs:// URI."),
    ],
    files=(
        "Reads NetCDF, local or gs://. Writes <base>_<hash>.nc beside the input "
        "(current directory for gs:// input), or -o, or --dest. An identical "
        "earlier result is reused. AA_NAMING=legacy: <stem>_freqswap.nc."
    ),
    pipeline=(
        "Usually last before analysis in Python: ... | aa-sv | aa-swap-freq. Tools "
        "that select by 'channel' will not work on its output."
    ),
    examples=[
        "aa-nc D20160703-T060000.raw --sonar_model EK60 | aa-sv | aa-swap-freq --check-unique",
        "aa-swap-freq MVBS.nc -o MVBS_by_freq.nc",
    ],
)


def print_help():
    """Curated help (also used by the docs generator)."""
    sys.stdout.write(render(SPEC, HELP, _build_parser()))


def print_help_full():
    help_text = """
    Usage: aa-swap-freq [OPTIONS] [INPUT_PATH]

    Arguments:
      INPUT_PATH                   Path (or gs:// URI) to a NetCDF file (.nc) with a
                                   'channel' dimension and a 'frequency_nominal'
                                   variable/coordinate. Optional. Defaults to stdin
                                   if not provided.

    Options:
      -o, --output_path PATH       Where to write the swapped dataset (NetCDF), used
                                   exactly as given (local path or gs:// URI).
                                   Default: <base>_<hash>.nc beside the input
                                   (AA_NAMING=legacy: <stem>_freqswap.nc).
      --check-unique               Fail early if duplicate frequency_nominal values exist.
      --no-overwrite               Do not overwrite an existing, different output file
                                   (exit 1). An identical product is reused.
      --base NAME                  Base name for the output.
      --dest DIR|gs://PREFIX       Write the default-named output there.
      --force                      Recompute even if an identical product exists.

      -h, --help                   Show the short help; --help-all shows this text.

    Description:
      Replaces the 'channel' dimension with the 'frequency_nominal' coordinate so that
      data are indexed by nominal transducer frequency (e.g., 18000., 38000., 120000.).
      Operation requires unique frequencies. Provenance (the input's chain plus
      this step) is embedded in the output; see aa-metadata.
    """
    print(help_text)


def _build_parser():
    parser = argparse.ArgumentParser(
        description="Swap 'channel' dimension with 'frequency_nominal' so frequency "
                    "becomes the primary dimension.",
        add_help=False,  # help is handled by show_help()
    )

    # ---------------------------
    # Positional/IO args
    # ---------------------------
    parser.add_argument(
        "input_path",
        type=str,
        nargs="?",
        help="Path or gs:// URI of a NetCDF file containing 'channel' and 'frequency_nominal'.",
    )
    parser.add_argument(
        "-o", "--output_path",
        type=str,
        help="Output path for the swapped NetCDF, used as given.",
    )
    parser.add_argument(
        "--check-unique",
        action="store_true",
        help="Fail early if duplicate frequency_nominal values exist.",
    )
    parser.add_argument(
        "--no-overwrite",
        action="store_true",
        help="Do not overwrite an existing output file.",
    )
    add_common_flags(parser)
    return parser


def _inherited_kind(src) -> str:
    """The input's product kind (sv, mvbs, mask ...): this step keeps what the data are.

    Falls back to SPEC.kind ("sv") for inputs without provenance, and for
    EchoData or fetched source files, whose kinds describe a different
    representation.
    """
    kind = (((src.prov or {}).get("product") or {}).get("kind") or "").strip()
    return kind if kind and kind not in {"echodata", "source"} else SPEC.kind


def _add_basic_attrs(ds) -> None:
    """Replace None attrs with strings to avoid NetCDF writer issues."""
    for k, v in list(ds.attrs.items()):
        if v is None:
            ds.attrs[k] = "NA"
    for var in ds.data_vars:
        for k, v in list(ds[var].attrs.items()):
            if v is None:
                ds[var].attrs[k] = "NA"


def _assert_unique_frequencies(ds) -> None:
    """
    If requested, verify that 'frequency_nominal' exists and is unique.
    Raises SystemExit(1) with an error message if not.
    """
    import numpy as np

    if "frequency_nominal" not in ds and "frequency_nominal" not in ds.coords:
        logger.error("Dataset lacks 'frequency_nominal' variable/coord required for swapping.")
        sys.exit(1)

    # frequency_nominal could be data var or coord; access safely:
    freq = ds["frequency_nominal"]
    vals = np.asarray(freq.values).ravel()
    # Remove NaNs before uniqueness check
    vals = vals[~np.isnan(vals)]
    if len(vals) != len(np.unique(vals)):
        logger.error("Duplicate values found in 'frequency_nominal'; cannot swap dims.")
        sys.exit(1)


def _exists(target: str) -> bool:
    return uris.stat(target) is not None if uris.is_gcs(target) else Path(target).exists()


def main():
    """Entry point for the aa-swap-freq CLI."""
    # No args on a terminal: help. (An empty pipe is an error: see stdio.)
    if len(sys.argv) == 1 and not stdio.stdin_is_piped():
        print_help()
        sys.exit(0)

    parser = _build_parser()
    if show_help(SPEC, HELP, parser, full=print_help_full):
        sys.exit(0)

    args = parser.parse_args()

    # ---------------------------
    # Resolve/validate input
    # ---------------------------
    token = stdio.one_input(args.input_path, SPEC.name)
    run = Run(SPEC, args)
    src = run.input(token)

    out = run.plan(
        ext=".nc",
        kind=_inherited_kind(src),
        explicit=args.output_path or None,
        legacy=lambda: src.local.with_stem(src.local.stem + "_freqswap").with_suffix(".nc"),
    )

    # Guard against clobbering the input.
    if not out.remote and Path(out.target).resolve() == src.local.resolve():
        logger.error(f"Refusing to overwrite input file: {src.local.resolve()}")
        sys.exit(1)

    # An identical product is never an "overwrite": reuse it first.
    if run.reusable(out):
        run.finish(out)
        return

    if args.no_overwrite and _exists(out.target):
        logger.error(f"Output file '{out.target}' exists and --no-overwrite was set.")
        sys.exit(1)

    try:
        import xarray as xr
        from echopype.consolidate import swap_dims_channel_frequency

        # Suppress any library chatter to stdout so pipelines remain clean.
        f = io.StringIO()
        with redirect_stdout(f):
            ds = xr.open_dataset(src.local)

        # Optional: early uniqueness check (the swap itself will fail if duplicates exist)
        if args.check_unique:
            _assert_unique_frequencies(ds)

        logger.info("Swapping 'channel' dimension with 'frequency_nominal' ...")
        ds_swapped = swap_dims_channel_frequency(ds)

        # Clean attributes to avoid None in NetCDF
        _add_basic_attrs(ds_swapped)

        logger.info(f"Saving swapped dataset to {out.target} ...")
        out.local.parent.mkdir(parents=True, exist_ok=True)
        ds_swapped.to_netcdf(out.local, mode="w", format="NETCDF4")

        logger.debug(f"\naa-swap-freq args:\n"
                     f"{pprint.pformat(dict(vars(args), output=out.target, product=out.hash))}")
        logger.info("Frequency/channel dimension swap complete.")
        run.finish(out)

    except Exception as e:
        logger.exception(f"Error during frequency/channel swap: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
