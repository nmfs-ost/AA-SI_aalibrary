#!/usr/bin/env python3
"""
aa-absorption

Console tool to compute seawater absorption (dB/m) using Echopype.

Wraps:
  echopype.utils.uwa.calc_absorption(
      frequency, temperature=27, salinity=35, pressure=10, pH=8.1, sound_speed=None, formula_source='AM'
  )

Accepts a frequency (or comma-separated list) in Hz and prints the
absorption coefficient(s), or with -o writes a NetCDF file with the result
(with provenance) and prints its path; the same options and -o again reuse
that file.
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
# _configure_logging() below replaces this once --quiet is parsed.
logger.add(sys.stderr, level="WARNING")

import argparse
import pprint

from aalibrary.console._core import (
    Help, Run, ToolSpec, add_common_flags, canon, naming, render, show_help, stdio,
)

# numpy, xarray and echopype are imported where used so --help stays fast.

SPEC = ToolSpec(
    name="aa-absorption",
    role="transform",
    kind="absorption",
    op="echopype.utils.uwa.calc_absorption",
    op_version=1,
    params={
        "frequency": canon.csv_numbers,
        "temperature": canon.number,
        "salinity": canon.number,
        "pressure": canon.number,
        "pH": canon.number,
        "formula_source": canon.choice(),
    },
)

HELP = Help(
    summary="Seawater absorption coefficient (dB/m) at one or more frequencies.",
    does=(
        "Evaluates echopype.utils.uwa.calc_absorption: Ainslie & McColm (AM, default), "
        "Francois & Garrison (FG) or the AZFP formula, from frequency, temperature, "
        "salinity, pressure and pH. Reads no file. Without -o it only prints the "
        "result; with -o it writes a NetCDF product."
    ),
    stdin="Nothing. All inputs are options.",
    stdout=(
        "Without -o: one frequency prints a bare number (e.g. 0.006275527046960815); "
        "a list prints numpy's array text (e.g. [0.00627553 0.04699969], rounded for "
        "display), the same text as always. With -o: the NetCDF's absolute path (or "
        "gs:// URI), whose values are full precision."
    ),
    metadata=(
        "Without -o nothing is written and there is no provenance. With -o the "
        "NetCDF holds 'absorption' (units dB m-1) on a 'frequency' coordinate (Hz) "
        "and the global attributes temperature_degC, salinity_psu, pressure_dbar, "
        "pH, formula_source, tool, plus aa provenance (this step with its canonical "
        "options, no inputs; see aa-metadata). Its base name is --base or the -o "
        "file's stem."
    ),
    options=[
        ("--frequency HZ[,HZ...]", "REQUIRED. e.g. 38000 or 38000,120000"),
        ("--temperature DEGC", "temperature in deg C (default 27)"),
        ("--salinity PSU", "salinity in PSU (default 35)"),
        ("--pressure DBAR", "pressure in dbar (default 10)"),
        ("--pH PH", "seawater pH (default 8.1)"),
        ("--formula-source NAME", "AM (default), FG or AZFP"),
        ("-o, --output_path PATH", "write a NetCDF instead of printing; .nc is forced. "
                                   "Local path or gs:// URI."),
        ("--quiet", "warnings and errors only on stderr"),
        ("--force", "with -o: recompute even if an identical file is already there"),
        ("--base NAME", "with -o: base name recorded in the provenance"),
    ],
    science={
        "frequency": "Hz; list order is kept (it is the output's coordinate order).",
        "temperature": "deg C.",
        "salinity": "PSU.",
        "pressure": "dbar.",
        "pH": "seawater pH.",
        "formula_source": "AM, FG or AZFP.",
    },
    files="Writes only with -o: exactly that path with its extension forced to .nc.",
    pipeline=(
        "A starting point, not a filter: use the number in shell substitution, e.g. "
        "a=$(aa-absorption --frequency 38000 --temperature 4 --quiet)."
    ),
    examples=[
        "aa-absorption --frequency 38000 --temperature 4 --salinity 34 --pressure 50",
        "aa-absorption --frequency 18000,38000,120000 -o alpha.nc",
    ],
    common=False,   # --dest does not apply: the only output is -o
)


def _configure_logging(quiet: bool) -> None:
    """Replace the default suppression sink with a user-visible one."""
    logger.remove()
    logger.add(sys.stderr, level="WARNING" if quiet else "INFO")


def print_help():
    """Curated help (also used by the docs generator)."""
    sys.stdout.write(render(SPEC, HELP, _build_parser()))


def print_help_full():
    help_text = """
    Usage: aa-absorption [OPTIONS]

    Options:
      --frequency FLOAT_OR_LIST  Frequency in Hz (e.g., 38000) or comma-separated list (e.g., 38000,120000). Required.
      --temperature FLOAT         Temperature in °C. Default: 27
      --salinity FLOAT            Salinity in PSU. Default: 35
      --pressure FLOAT            Pressure in dbar. Default: 10
      --pH FLOAT                  pH of seawater. Default: 8.1
      --formula-source STR        Formula source: 'AM', 'FG', or 'AZFP'. Default: AM
      -o, --output_path PATH      Optional NetCDF output path or gs:// URI; the suffix
                                  is forced to .nc (default: none).
      --quiet                     Warnings and errors only on stderr (stdout is the
                                  value(s), or the -o path, either way).
      --force                     With -o: recompute even if an identical file exists.
      --base NAME                 With -o: base name recorded in the provenance
                                  (default: the -o file's stem).
      -h, --help                  Curated help. --help-all: this text.

    Description:
      Computes seawater absorption in dB/m for given frequency(ies) and parameters.
      A single frequency prints a number; a list prints numpy's array text.
      With -o, writes 'absorption' on a 'frequency' coordinate to NetCDF, with
      the parameters as attributes and aa provenance (see aa-metadata), and
      prints its path instead. Running again with the same parameters and -o
      reuses the file.
    """
    print(help_text)


def _build_parser():
    parser = argparse.ArgumentParser(
        description="Compute seawater absorption (dB/m) using Echopype.",
        add_help=False,
    )
    parser.add_argument("--frequency", required=True,
                        help="Frequency in Hz, or comma-separated list.")
    parser.add_argument("--temperature", type=float, default=27.0,
                        help="Temperature in °C (default: 27).")
    parser.add_argument("--salinity", type=float, default=35.0,
                        help="Salinity in PSU (default: 35).")
    parser.add_argument("--pressure", type=float, default=10.0,
                        help="Pressure in dbar (default: 10).")
    parser.add_argument("--pH", type=float, default=8.1,
                        help="pH of seawater (default: 8.1).")
    parser.add_argument("--formula-source", dest="formula_source",
                        choices=["AM", "FG", "AZFP"], default="AM",
                        help="Formula source (default: AM).")
    parser.add_argument("-o", "--output_path", type=str,
                        help="Optional NetCDF output path.")
    parser.add_argument("--quiet", action="store_true",
                        help="Print only numeric result(s).")
    # --dest is left out: without -o nothing is written.
    add_common_flags(parser, dest=False)
    return parser


def _add_basic_attrs(ds) -> None:
    for k, v in list(ds.attrs.items()):
        if v is None:
            ds.attrs[k] = "NA"
    for var in ds.data_vars:
        for k, v in list(ds[var].attrs.items()):
            if v is None:
                ds[var].attrs[k] = "NA"


def main():
    if len(sys.argv) == 1 and not stdio.stdin_is_piped():
        print_help()
        sys.exit(0)

    parser = _build_parser()
    if show_help(SPEC, HELP, parser, full=print_help_full):
        sys.exit(0)

    args = parser.parse_args()
    _configure_logging(args.quiet)

    import numpy as np

    # parse frequency list
    try:
        freqs = [float(f) for f in args.frequency.split(",")]
        if len(freqs) == 1:
            freqs = freqs[0]
        else:
            freqs = np.array(freqs, dtype=float)
    except Exception:
        logger.error(f"Invalid --frequency value: {args.frequency}")
        sys.exit(1)

    try:
        if args.output_path:
            _write_product(args, freqs)
        else:
            abs_val = _absorption(args, freqs)
            # Printed exactly as always: a float, or numpy's array text.
            logger.info("Computed seawater absorption (dB/m):")
            print(abs_val)

        logger.debug(f"\naa-absorption args:\n{pprint.pformat(vars(args))}")

    except Exception as e:
        logger.exception(f"Error computing absorption: {e}")
        sys.exit(1)


def _absorption(args, freqs):
    from echopype.utils.uwa import calc_absorption

    return calc_absorption(
        frequency=freqs,
        temperature=args.temperature,
        salinity=args.salinity,
        pressure=args.pressure,
        pH=args.pH,
        formula_source=args.formula_source,
    )


def _write_product(args, freqs) -> None:
    """-o: a small NetCDF with provenance; its path goes to stdout."""
    import numpy as np
    import xarray as xr

    explicit = naming.with_ext(args.output_path, ".nc")   # suffix forced, as always
    # No input to take a base name from: --base, else the -o file's stem.
    run = Run(SPEC, args, base=naming.base_of(explicit))
    out = run.plan(ext=".nc", explicit=explicit)
    if run.reusable(out):
        run.finish(out)
        return

    abs_val = _absorption(args, freqs)
    ds = xr.Dataset(
        data_vars=dict(
            absorption=(["frequency"],
                        np.atleast_1d(abs_val),
                        {"units": "dB m-1", "long_name": "Seawater absorption coefficient"})
        ),
        coords=dict(
            frequency=(["frequency"], np.atleast_1d(freqs), {"units": "Hz"})
        ),
        attrs=dict(
            temperature_degC=args.temperature,
            salinity_psu=args.salinity,
            pressure_dbar=args.pressure,
            pH=args.pH,
            formula_source=args.formula_source,
            tool="aa-absorption"
        )
    )
    _add_basic_attrs(ds)
    out.local.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Saving absorption to {out.target} ...")
    ds.to_netcdf(out.local, mode="w", format="NETCDF4")
    # Print path (or URI) for piping
    run.finish(out)


if __name__ == "__main__":
    main()
