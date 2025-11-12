#!/usr/bin/env python3
"""
Console tool to compute seawater absorption (dB/m) using Echopype.

Wraps:
  echopype.utils.uwa.calc_absorption(
      frequency, temperature=27, salinity=35, pressure=10, pH=8.1, sound_speed=None, formula_source='AM'
  )

Accepts a frequency (or array) in Hz and writes the absorption coefficient or
optionally a NetCDF file with the result.
"""

import argparse
import sys
from pathlib import Path
import numpy as np
import xarray as xr
from loguru import logger
from echopype.utils.uwa import calc_absorption
import pprint

def print_help():
    help_text = """
    Usage: aa-absorption [OPTIONS]

    Options:
      --frequency FLOAT_OR_LIST  Frequency in Hz (e.g., 38000) or comma-separated list (e.g., 38000,120000). Required.
      --temperature FLOAT         Temperature in °C. Default: 27
      --salinity FLOAT            Salinity in PSU. Default: 35
      --pressure FLOAT            Pressure in dbar. Default: 10
      --pH FLOAT                  pH of seawater. Default: 8.1
      --formula-source STR        Formula source: 'AM', 'FG', or 'AZFP'. Default: AM
      -o, --output_path PATH      Optional NetCDF output path (default: none).
      --quiet                     Print only numeric values (or array).
      -h, --help                  Show this help message and exit.

    Description:
      Computes seawater absorption in dB/m for given frequency(ies) and parameters.
    """
    print(help_text)

def _add_basic_attrs(ds: xr.Dataset) -> None:
    for k, v in list(ds.attrs.items()):
        if v is None:
            ds.attrs[k] = "NA"
    for var in ds.data_vars:
        for k, v in list(ds[var].attrs.items()):
            if v is None:
                ds[var].attrs[k] = "NA"

def main():
    if len(sys.argv) == 1 and sys.stdin.isatty():
        print_help()
        sys.exit(0)

    parser = argparse.ArgumentParser(
        description="Compute seawater absorption (dB/m) using Echopype."
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
                        choices=["AM","FG","AZFP"], default="AM",
                        help="Formula source (default: AM).")
    parser.add_argument("-o", "--output_path", type=Path,
                        help="Optional NetCDF output path.")
    parser.add_argument("--quiet", action="store_true",
                        help="Print only numeric result(s).")

    args = parser.parse_args()

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
        # compute absorption
        abs_val = calc_absorption(
            frequency=freqs,
            temperature=args.temperature,
            salinity=args.salinity,
            pressure=args.pressure,
            pH=args.pH,
            formula_source=args.formula_source
        )

        if args.output_path:
            # build a Dataset
            ds = xr.Dataset(
                data_vars=dict(
                    absorption=(["frequency"], 
                                np.atleast_1d(abs_val),
                                {"units":"dB m-1", "long_name": "Seawater absorption coefficient"})
                ),
                coords=dict(
                    frequency=(["frequency"], np.atleast_1d(freqs), {"units":"Hz"})
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
            out = args.output_path.with_suffix(".nc")
            logger.info(f"Saving absorption to {out} ...") if not args.quiet else None
            ds.to_netcdf(out, mode="w", format="NETCDF4")
            print(out.resolve())

        else:
            if args.quiet:
                print(abs_val)
            else:
                logger.info("Computed seawater absorption (dB/m):")
                print(abs_val)

    except Exception as e:
        logger.exception(f"Error computing absorption: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
