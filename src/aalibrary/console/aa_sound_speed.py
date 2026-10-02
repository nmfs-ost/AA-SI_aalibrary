#!/usr/bin/env python3
"""
aa-sound-speed

Console tool to compute seawater sound speed (m/s) using Echopype.

Wraps:
  echopype.utils.uwa.calc_sound_speed(
      temperature=27, salinity=35, pressure=10, formula_source='Mackenzie'
  )

No input file. Prints the number on stdout. With -o it instead writes a
small NetCDF (scalar 'sound_speed' plus the inputs as global attributes),
with provenance, and prints its path; the same options and -o again reuse
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

# xarray and echopype are imported where used so --help stays fast.

SPEC = ToolSpec(
    name="aa-sound-speed",
    role="transform",
    kind="sound_speed",
    op="echopype.utils.uwa.calc_sound_speed",
    op_version=1,
    params={
        "temperature": canon.number,
        "salinity": canon.number,
        "pressure": canon.number,
        "formula_source": canon.choice(),
    },
)

HELP = Help(
    summary="Seawater sound speed (m/s) from temperature, salinity and pressure.",
    does=(
        "Evaluates echopype.utils.uwa.calc_sound_speed: Mackenzie (1981) by default, "
        "or the AZFP formula. Reads no file. Without -o it only prints the number; "
        "with -o it writes a NetCDF product."
    ),
    stdin="Nothing. All inputs are options.",
    stdout=(
        "Without -o: the sound speed as a bare number, e.g. 1539.0866009307247 (the "
        "same text as always). With -o: the NetCDF's absolute path (or gs:// URI)."
    ),
    metadata=(
        "Without -o nothing is written and there is no provenance. With -o the "
        "NetCDF holds scalar 'sound_speed' (units m s-1) and the global attributes "
        "temperature_degC, salinity_psu, pressure_dbar, formula_source, tool, plus "
        "aa provenance (this step with its canonical options, no inputs; see "
        "aa-metadata). Its base name is --base or the -o file's stem."
    ),
    options=[
        ("--temperature DEGC", "temperature in deg C (default 27)"),
        ("--salinity PSU", "salinity in PSU / ppt (default 35)"),
        ("--pressure DBAR", "pressure in dbar (default 10)"),
        ("--formula-source NAME", "Mackenzie (default) or AZFP"),
        ("-o, --output_path PATH", "write a NetCDF instead of printing the number; "
                                   ".nc is forced. Local path or gs:// URI."),
        ("--quiet", "warnings and errors only on stderr"),
        ("--force", "with -o: recompute even if an identical file is already there"),
        ("--base NAME", "with -o: base name recorded in the provenance"),
    ],
    science={
        "temperature": "deg C.",
        "salinity": "PSU / ppt.",
        "pressure": "dbar.",
        "formula_source": "Mackenzie or AZFP.",
    },
    files="Writes only with -o: exactly that path with its extension forced to .nc.",
    pipeline=(
        "A starting point, not a filter: use the number in shell substitution, e.g. "
        "c=$(aa-sound-speed --temperature 4 --salinity 34 --quiet)."
    ),
    examples=[
        "aa-sound-speed --temperature 10 --salinity 33 --pressure 5",
        "aa-sound-speed --temperature 2 --salinity 35 --pressure 1000 -o ssp.nc",
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
    Usage: aa-sound-speed [OPTIONS]

    Options:
      --temperature FLOAT     Temperature in deg C (default: 27)
      --salinity FLOAT        Salinity in PSU / ppt (default: 35)
      --pressure FLOAT        Pressure in dbar (default: 10)
      --formula-source STR    'Mackenzie' (default) or 'AZFP'
      -o, --output_path PATH  Optional NetCDF output, local path or gs:// URI;
                              the suffix is forced to .nc (default: none)
      --quiet                 Warnings and errors only on stderr (stdout is the
                              number, or the -o path, either way)
      --force                 With -o: recompute even if an identical file exists
      --base NAME             With -o: base name recorded in the provenance
                              (default: the -o file's stem)
      -h, --help              Curated help. --help-all: this text.

    Description:
      Computes seawater sound speed in m/s using Echopype's utilities.
      If an output path is provided, writes a small NetCDF with a scalar
      variable 'sound_speed' and the input parameters as attributes, plus
      aa provenance (see aa-metadata), and prints its path instead of the
      number. Running again with the same parameters and -o reuses the file.

    Examples:
      aa-sound-speed --temperature 10 --salinity 33 --pressure 5
      aa-sound-speed --temperature 2 --salinity 35 --pressure 1000 --formula-source Mackenzie -o ssp.nc
    """
    print(help_text)


def _build_parser():
    parser = argparse.ArgumentParser(
        description="Compute seawater sound speed (m/s) using Echopype.",
        add_help=False,
    )

    # Parameters for calc_sound_speed
    parser.add_argument("--temperature", type=float, default=27.0,
                        help="Temperature in deg C (default: 27).")
    parser.add_argument("--salinity", type=float, default=35.0,
                        help="Salinity in PSU/ppt (default: 35).")
    parser.add_argument("--pressure", type=float, default=10.0,
                        help="Pressure in dbar (default: 10).")
    parser.add_argument("--formula-source", dest="formula_source",
                        choices=["Mackenzie", "AZFP"], default="Mackenzie",
                        help="Formula source (default: Mackenzie).")

    # IO / behavior
    parser.add_argument("-o", "--output_path", type=str,
                        help="Optional NetCDF output path (default: none).")
    parser.add_argument("--quiet", action="store_true",
                        help="Print only the numeric value.")
    # --dest is left out: without -o nothing is written.
    add_common_flags(parser, dest=False)
    return parser


def _add_basic_attrs(ds) -> None:
    """Replace None attrs with strings to avoid NetCDF writer issues."""
    for k, v in list(ds.attrs.items()):
        if v is None:
            ds.attrs[k] = "NA"
    for var in ds.data_vars:
        for k, v in list(ds[var].attrs.items()):
            if v is None:
                ds[var].attrs[k] = "NA"


def _sound_speed(args) -> float:
    from echopype.utils.uwa import calc_sound_speed

    return float(calc_sound_speed(
        temperature=args.temperature,
        salinity=args.salinity,
        pressure=args.pressure,
        formula_source=args.formula_source,
    ))


def main():
    """Entry point for the aa-sound-speed CLI."""
    # If invoked with no args on a TTY, show help and exit (no stdin protocol needed here).
    if len(sys.argv) == 1 and not stdio.stdin_is_piped():
        print_help()
        sys.exit(0)

    parser = _build_parser()
    if show_help(SPEC, HELP, parser, full=print_help_full):
        sys.exit(0)

    args = parser.parse_args()
    _configure_logging(args.quiet)

    try:
        if args.output_path is None:
            # Compute sound speed (m/s) and print the number, as always.
            c = _sound_speed(args)
            logger.info("Computed seawater sound speed (m/s):")
            print(c)
        else:
            _write_product(args)

        # Debug dump of args
        logger.debug(f"\naa-sound-speed args:\n{pprint.pformat(vars(args))}")

    except Exception as e:
        logger.exception(f"Error computing sound speed: {e}")
        sys.exit(1)


def _write_product(args) -> None:
    """-o: a tiny NetCDF with provenance; its path goes to stdout."""
    import xarray as xr

    explicit = naming.with_ext(args.output_path, ".nc")   # suffix forced, as always
    # No input to take a base name from: --base, else the -o file's stem.
    run = Run(SPEC, args, base=naming.base_of(explicit))
    out = run.plan(ext=".nc", explicit=explicit)
    if run.reusable(out):
        run.finish(out)
        return

    c = _sound_speed(args)
    ds = xr.Dataset(
        data_vars=dict(
            # A scalar: dims () with a 0-d value. (This used to pass [c], a
            # 1-element list, which xarray rejects, so -o always failed.)
            sound_speed=((), c, {"units": "m s-1", "long_name": "Seawater sound speed"})
        ),
        attrs=dict(
            temperature_degC=args.temperature,
            salinity_psu=args.salinity,
            pressure_dbar=args.pressure,
            formula_source=args.formula_source,
            tool="aa-sound-speed",
        ),
    )
    _add_basic_attrs(ds)
    out.local.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Saving sound speed to {out.target} ...")
    ds.to_netcdf(out.local, mode="w", format="NETCDF4")
    # Print path (or URI) for piping
    run.finish(out)


if __name__ == "__main__":
    main()
