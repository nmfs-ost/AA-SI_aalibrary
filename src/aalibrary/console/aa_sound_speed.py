#!/usr/bin/env python3
"""
Console tool to compute seawater sound speed (m/s) using Echopype.

Pattern matches your suite:
- simple argparse wrapper
- human-readable comments
- optional NetCDF output
- quiet mode for piping

Wraps:
  echopype.utils.uwa.calc_sound_speed(
      temperature=27, salinity=35, pressure=10, formula_source='Mackenzie'
  )
"""

import argparse
import sys
from pathlib import Path
import xarray as xr
from loguru import logger
from echopype.utils.uwa import calc_sound_speed
import pprint


def print_help():
    """Standalone help text for zero-arg, tty invocation."""
    help_text = """
    Usage: aa-sound-speed [OPTIONS]

    Options:
      --temperature FLOAT     Temperature in deg C (default: 27)
      --salinity FLOAT        Salinity in PSU / ppt (default: 35)
      --pressure FLOAT        Pressure in dbar (default: 10)
      --formula-source STR    'Mackenzie' (default) or 'AZFP'
      -o, --output_path PATH  Optional NetCDF output (default: none)
      --quiet                 Print only the numeric value
      -h, --help              Show this help message and exit

    Description:
      Computes seawater sound speed in m/s using Echopype’s utilities.
      If an output path is provided, writes a small NetCDF with a scalar
      variable 'sound_speed' and the input parameters as attributes.

    Examples:
      aa-sound-speed --temperature 10 --salinity 33 --pressure 5
      aa-sound-speed --temperature 2 --salinity 35 --pressure 1000 --formula-source Mackenzie -o ssp.nc
    """
    print(help_text)


def _add_basic_attrs(ds: xr.Dataset) -> None:
    """Replace None attrs with strings to avoid NetCDF writer issues."""
    for k, v in list(ds.attrs.items()):
        if v is None:
            ds.attrs[k] = "NA"
    for var in ds.data_vars:
        for k, v in list(ds[var].attrs.items()):
            if v is None:
                ds[var].attrs[k] = "NA"


def main():
    """Entry point for the aa-sound-speed CLI."""
    # If invoked with no args on a TTY, show help and exit (no stdin protocol needed here).
    if len(sys.argv) == 1 and sys.stdin.isatty():
        print_help()
        sys.exit(0)

    parser = argparse.ArgumentParser(
        description="Compute seawater sound speed (m/s) using Echopype."
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
    parser.add_argument("-o", "--output_path", type=Path,
                        help="Optional NetCDF output path (default: none).")
    parser.add_argument("--quiet", action="store_true",
                        help="Print only the numeric value.")

    args = parser.parse_args()

    try:
        # Compute sound speed (m/s)
        c = float(calc_sound_speed(
            temperature=args.temperature,
            salinity=args.salinity,
            pressure=args.pressure,
            formula_source=args.formula_source,
        ))

        # If writing NetCDF, package into a tiny Dataset
        if args.output_path is not None:
            ds = xr.Dataset(
                data_vars=dict(
                    sound_speed=([], [c], {"units": "m s-1", "long_name": "Seawater sound speed"})
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
            # Ensure suffix
            out = args.output_path.with_suffix(".nc")
            logger.info(f"Saving sound speed to {out} ...") if not args.quiet else None
            ds.to_netcdf(out, mode="w", format="NETCDF4")
            # Print path for piping
            print(out.resolve())
        else:
            # Print numeric result (quiet mode prints only the number)
            if args.quiet:
                print(c)
            else:
                logger.info("Computed seawater sound speed (m/s):")
                print(c)

        # Debug dump of args if not quiet
        if not args.quiet:
            pretty_args = pprint.pformat(vars(args))
            logger.debug(f"\naa-sound-speed args:\n{pretty_args}")

    except Exception as e:
        logger.exception(f"Error computing sound speed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
