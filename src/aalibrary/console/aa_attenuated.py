#!/usr/bin/env python3
"""
aa-attenuated

Console tool for locating attenuated signal in calibrated Sv data with
Echopype (echopype.clean.mask_attenuated_signal) and saving an
attenuated-signal mask (and, with --apply, a copy of the Sv data with the
flagged pings set to NaN).

Pipeline-friendly: reads the input path (or gs:// URI) from a positional
arg or stdin, writes the MASK's path to stdout, all logs to stderr. Both
outputs carry the input's provenance plus this step.

Typical pipeline usage:
    aa-nc --sonar_model EK60 input.raw | aa-sv | aa-depth | aa-attenuated --apply
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
import re
from contextlib import redirect_stdout
from pathlib import Path

from aalibrary.console._core import (
    Help, Run, ToolSpec, add_common_flags, canon, naming, render, show_help, stdio,
)

SPEC = ToolSpec(
    name="aa-attenuated",
    role="transform",
    kind="mask",
    op="echopype.clean.mask_attenuated_signal",
    op_version=1,
    params={
        "upper_limit_sl": canon.quantity("m"),
        "lower_limit_sl": canon.quantity("m"),
        "num_side_pings": canon.integer,
        "attenuation_signal_threshold": canon.quantity("dB"),
        "range_var": canon.choice(),
    },
)

IMPLICIT_SV = {"implicit_step": "echopype.calibrate.compute_Sv (defaults)"}

HELP = Help(
    summary="Attenuated-ping mask for Sv; --apply also writes cleaned Sv.",
    does=(
        "Runs echopype.clean.mask_attenuated_signal (Ryan et al. 2015). For each "
        "ping, the median Sv between --upper-limit-sl and --lower-limit-sl is "
        "compared with the median over the block of +/- --num-side-pings pings; "
        "the whole ping is flagged when (ping median - block median) is BELOW "
        "--attenuation-threshold. Pings within --num-side-pings of either end "
        "are never flagged.\n\n"
        "The mask file holds one variable, attenuated_mask: boolean, True = "
        "attenuated (the whole column), False = keep, dims (channel, ping_time, "
        "range_sample). If the limits lie outside the data it is all False."
    ),
    stdin=(
        "One Sv NetCDF path or gs:// URI that has the --range-var variable: depth "
        "(add it with aa-depth) or echo_range. An EchoData file without Sv is "
        "calibrated first with compute_Sv defaults (recorded in the provenance "
        "as an implicit step)."
    ),
    stdout=(
        "The MASK's absolute path (or gs:// URI), one line. With --apply the "
        "cleaned Sv's path goes to stderr instead, as "
        "'aa-attenuated: cleaned Sv: PATH' (also when it is reused)."
    ),
    metadata=(
        "Reads the input's provenance, appends this step with its canonical "
        "scientific options, computes the product hash, and embeds it all in "
        "the mask (NetCDF attributes aa_provenance, aa_product_hash, aa_base, "
        "aa_tool, history). The --apply file is its own product: same step, "
        "variant 'apply', kind sv, its own hash. Inspect with: aa-metadata FILE"
    ),
    options=[
        ("-o, --output_path PATH", "The mask file, used exactly as given (no "
                                   "extension added). Local path or gs:// URI. "
                                   "Does not move the --apply file."),
        ("--apply", "Also write the input's Sv with attenuated pings set to NaN "
                    "(all other variables copied)."),
    ],
    science={
        "upper_limit_sl": "Top of the comparison layer, e.g. 400m.",
        "lower_limit_sl": "Bottom of the comparison layer, e.g. 500m.",
        "num_side_pings": "Pings on each side in the comparison block.",
        "attenuation_signal_threshold": "Flag a ping when ping median - block "
                                        "median is below this. Negative values "
                                        "need '=': --attenuation-threshold=-6dB.",
        "range_var": "Vertical variable: depth or echo_range.",
    },
    files=(
        "Reads NetCDF, local or gs://. Writes the mask to <base>_<hash>.nc beside "
        "the input (current directory for gs:// input), or -o, or --dest. With "
        "--apply a second <base>_<hash>.nc (a different hash) goes beside the "
        "input or into --dest, never to -o. AA_NAMING=legacy: "
        "<stem>_attenuated_mask.nc and <stem>_attenuated_cleaned.nc beside the "
        "input. Identical earlier results are reused."
    ),
    pipeline=(
        "After aa-sv and aa-depth: ... | aa-sv | aa-depth | aa-attenuated | "
        "aa-graph (draws the mask). Only the mask travels down the pipe."
    ),
    examples=[
        "aa-nc x.raw --sonar_model EK60 | aa-sv | aa-depth | aa-attenuated --apply",
        "aa-attenuated sv_depth.nc --upper-limit-sl 50m --lower-limit-sl 100m "
        "--attenuation-threshold=-6dB",
    ],
    notes=[
        "Sign of the threshold: an attenuated ping is weaker than its block, so "
        "its difference is negative. A negative threshold (e.g. -6dB) flags "
        "pings more than that much weaker; the default +8.0dB flags nearly every "
        "ping in the comparable range.",
        "echopype 0.11.1 checks upper < lower by comparing the two limits as "
        "text, which would refuse 20m vs 100m. The tool therefore passes both "
        "written with the same width and decimals (20m, 100m -> 020m, 100m), so "
        "text order is numeric order and any spelling works. The upper limit "
        "must be shallower: a reversed pair is refused ('Minimum range has to be "
        "shorter than maximum range'), equal limits flag nothing. A channel "
        "whose depth contains NaN (a shorter-range channel) is never flagged.",
    ],
)


def print_help():
    """Curated help (also used by the docs generator)."""
    sys.stdout.write(render(SPEC, HELP, _build_parser()))


def print_help_full():
    """The complete reference (--help-all)."""
    help_text = """
    Usage: aa-attenuated [OPTIONS] [INPUT_PATH]

    Arguments:
      INPUT_PATH                   Path (or gs:// URI) to the calibrated .nc
                                   (NetCDF) file containing Sv (preferred), or a
                                   converted Echopype file that can be
                                   calibrated to Sv. Optional. Defaults to stdin
                                   if not provided (an empty stdin is an error,
                                   exit 1).

    Options:
      -o, --output_path PATH       Where to write the attenuated-signal mask
                                   (NetCDF), used exactly as given; local path or
                                   gs:// URI. Default: <base>_<hash>.nc beside the
                                   input (AA_NAMING=legacy:
                                   <stem>_attenuated_mask.nc).
      --apply                      Also apply the mask to Sv (attenuated pings set
                                   to NaN) and write the result as a second file:
                                   <base>_<hash>.nc beside the input or in --dest
                                   (AA_NAMING=legacy: <stem>_attenuated_cleaned.nc
                                   beside the input). -o never moves this file.
                                   stdout still gets only the mask path; this
                                   file's path goes to stderr ('aa-attenuated: cleaned Sv: PATH').

      # mask_attenuated_signal parameters
      --upper-limit-sl STR         Upper limit of deep scattering layer line, e.g. '400.0m'.
                                   Default: 400.0m
      --lower-limit-sl STR         Lower limit of deep scattering layer line, e.g. '500.0m'.
                                   Default: 500.0m
                                   The upper limit must be shallower than the
                                   lower one. (echopype compares the two as text;
                                   the tool passes both with the same width and
                                   decimals, e.g. 20m 100m -> 020m 100m, so any
                                   spelling works.)
      --num-side-pings INT         Pings on each side defining the comparison block.
                                   Default: 15
      --attenuation-threshold STR  A ping is flagged when (ping median - block
                                   median) is below this, e.g. '-6dB' (write
                                   --attenuation-threshold=-6dB: with a space,
                                   argparse reads -6dB as an option).
                                   Default: 8.0dB (flags nearly every ping)
      --range-var STR              Name of the range/depth coordinate: depth or
                                   echo_range. Default: depth

      --base NAME                  Base name for the outputs.
      --dest DIR|gs://PREFIX       Write the default-named outputs there.
      --force                      Recompute even if identical products exist.
      -h, --help                   Short help. --help-all: this text.

    Description:
      Creates a boolean mask marking likely attenuated-signal pings based on
      comparisons across neighboring ping blocks between two depth limits.
      The mask variable is 'attenuated_mask', True = attenuated (whole ping),
      dims (channel, ping_time, range_sample). Optionally applies the mask to
      Sv to produce a cleaned Sv dataset. Provenance (the input's chain plus
      this step) is embedded in both files; see aa-metadata.

      If the input has no 'Sv' variable it is calibrated with
      echopype.calibrate.compute_Sv (defaults) first.

    Examples:
      aa-attenuated data.nc --upper-limit-sl 350m --lower-limit-sl 480m --num-side-pings 17
      aa-attenuated data.nc --apply -o out_mask.nc
    """
    print(help_text)


def _add_basic_attrs(ds) -> None:
    """Replace None attrs with strings to avoid NetCDF writer issues."""
    # Dataset-level attrs
    for k, v in list(ds.attrs.items()):
        if v is None:
            ds.attrs[k] = "NA"
    # Variable-level attrs
    for var in ds.data_vars:
        for k, v in list(ds[var].attrs.items()):
            if v is None:
                ds[var].attrs[k] = "NA"


def _build_parser():
    parser = argparse.ArgumentParser(
        description="Create an attenuated-signal mask from Sv and (optionally) write "
                    "Sv cleaned with that mask.",
        add_help=False,
    )

    # ---------------------------
    # Positional/IO args
    # ---------------------------
    parser.add_argument(
        "input_path",
        type=str,
        nargs="?",
        help="Path or gs:// URI of a NetCDF file containing Sv (preferred) or a "
             "converted file that can be calibrated to Sv.",
    )
    parser.add_argument(
        "-o", "--output_path",
        type=str,
        help="Output path for the mask NetCDF, used as given (default: <base>_<hash>.nc).",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Also write Sv cleaned by the attenuated-signal mask as a second product.",
    )

    # ---------------------------
    # mask_attenuated_signal parameters
    # ---------------------------
    parser.add_argument("--upper-limit-sl", dest="upper_limit_sl", default="400.0m",
                        help="Upper limit of deep scattering layer line, e.g., '400.0m' (default: 400.0m).")
    parser.add_argument("--lower-limit-sl", dest="lower_limit_sl", default="500.0m",
                        help="Lower limit of deep scattering layer line, e.g., '500.0m' (default: 500.0m).")
    parser.add_argument("--num-side-pings", dest="num_side_pings", type=int, default=15,
                        help="Number of side pings for comparison block (default: 15).")
    parser.add_argument("--attenuation-threshold", dest="attenuation_signal_threshold", default="8.0dB",
                        help="Attenuation threshold, e.g. --attenuation-threshold=-6dB "
                             "(default: 8.0dB).")
    parser.add_argument("--range-var", dest="range_var", default="depth",
                        help="Range/depth variable name (default: depth).")
    add_common_flags(parser)
    return parser


def _metres(value):
    m = re.match(r"^\s*([\d.]+)\s*m\s*$", str(value), flags=re.IGNORECASE)
    try:
        return float(m.group(1)) if m else None
    except ValueError:
        return None


def _decimals(value) -> int:
    m = re.match(r"^\s*\d*\.(\d*)", str(value))
    return len(m.group(1)) if m else 0


def _limits_for_echopype(upper: str, lower: str) -> tuple[str, str]:
    """The two layer limits, spelled so that text order == numeric order.

    echopype 0.11.1 refuses ``upper_limit_sl > lower_limit_sl`` comparing the
    two *strings*, so '20m' vs '100m' is refused although 20 < 100, and
    '100m' vs '20m' is accepted and flags nothing. Written with the same
    width and decimals ('020m', '100m') the text comparison is the numeric
    one. echopype parses both back to the same floats, so the mask (and the
    hash, which is taken from the canonical values) does not change.
    Values the tool cannot parse are passed unchanged for echopype to judge.
    """
    up, lo = _metres(upper), _metres(lower)
    if up is None or lo is None:
        return upper, lower
    d = max(_decimals(upper), _decimals(lower))
    w = max(len(f"{up:.{d}f}"), len(f"{lo:.{d}f}"))
    new_upper, new_lower = f"{up:0{w}.{d}f}m", f"{lo:0{w}.{d}f}m"
    if up > lo:
        logger.warning(
            f"--upper-limit-sl {upper} is deeper than --lower-limit-sl {lower}; echopype "
            "refuses a reversed pair ('Minimum range has to be shorter than maximum "
            "range')."
        )
    elif up == lo:
        logger.warning(
            f"--upper-limit-sl {upper} equals --lower-limit-sl {lower}: echopype "
            "accepts this but flags nothing."
        )
    return new_upper, new_lower


def _report_side(side):
    """The --apply file is not on stdout (the mask is); say where it is."""
    print(f"{SPEC.name}: cleaned Sv: {side.target}", file=sys.stderr)


def main():
    """Entry point for the aa-attenuated CLI."""
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

    # What echopype gets: same values, spelled so its text comparison works.
    upper_sl, lower_sl = _limits_for_echopype(args.upper_limit_sl, args.lower_limit_sl)

    # ---------------------------
    # Resolve output paths
    # ---------------------------
    # The mask: -o used verbatim, as always.
    out = run.plan(
        ext=".nc",
        explicit=args.output_path or None,
        legacy=lambda: naming.with_stem_suffix(src.local, "_attenuated_mask", ".nc"),
    )
    # The --apply side output: never moved by -o (legacy: beside the input).
    side = None
    if args.apply:
        side = run.plan(
            ext=".nc",
            variant="apply",
            kind="sv",
            legacy=lambda: naming.with_stem_suffix(src.local, "_attenuated_cleaned", ".nc"),
        )

    # Guard against clobbering the input (or one output with the other)
    for o in (out, side):
        if o is not None and not o.remote and Path(o.target).resolve() == src.local.resolve():
            logger.error(f"Refusing to overwrite input file: {src.local.resolve()}")
            sys.exit(1)
    if side is not None and side.target == out.target:
        logger.error(f"-o names the same file as the --apply output: {out.target}")
        sys.exit(1)

    mask_done = run.reusable(out)
    side_done = side is None or run.reusable(side)
    if mask_done and side_done:
        if side is not None:
            run.finish(side, emit=False)
            _report_side(side)
        run.finish(out)
        return

    try:
        logger.debug(
            "\naa-attenuated args:\n"
            + pprint.pformat(vars(args) | {"passed_limits": (upper_sl, lower_sl),
                                           "mask": out.target,
                                           "cleaned": side.target if side else None,
                                           "product": out.hash})
        )

        ds, mask, calibrated = compute_mask(
            input_path=src.local,
            upper_limit_sl=upper_sl,
            lower_limit_sl=lower_sl,
            num_side_pings=args.num_side_pings,
            attenuation_signal_threshold=args.attenuation_signal_threshold,
            range_var=args.range_var,
        )
        extra = IMPLICIT_SV if calibrated else None

        if not mask_done:
            write_mask(mask, out.local)
        run.finish(out, extra=extra, emit=False)

        # Optionally write a cleaned Sv file with attenuated-signal samples set to NaN
        if side is not None:
            if not side_done:
                write_cleaned(ds, mask, side.local)
            run.finish(side, extra=extra, emit=False)
            _report_side(side)

        # Echo the primary output (mask path) to stdout for piping
        stdio.emit(out.target)
        logger.info("Attenuated-signal masking complete.")

    except Exception as e:
        logger.exception(f"Error during attenuated-signal masking: {e}")
        sys.exit(1)


def compute_mask(
    input_path: Path,
    upper_limit_sl: str = "400.0m",
    lower_limit_sl: str = "500.0m",
    num_side_pings: int = 15,
    attenuation_signal_threshold: str = "8.0dB",
    range_var: str = "depth",
):
    """Load Sv (calibrating if needed) and compute the attenuated-signal mask.

    Returns (ds_Sv, mask, calibrated) where ``calibrated`` says whether the
    input had no Sv and compute_Sv was run on it.
    """
    import echopype as ep  # deferred so --help stays fast
    import xarray as xr
    from echopype.clean import mask_attenuated_signal

    # Suppress any library chatter to stdout so pipelines remain clean.
    f = io.StringIO()
    with redirect_stdout(f):
        ds = xr.open_dataset(input_path)

    # ---------------------------
    # Ensure we have calibrated Sv
    # ---------------------------
    calibrated = False
    if "Sv" not in ds.data_vars:
        logger.info("No 'Sv' variable found; attempting to calibrate to Sv via Echopype...")
        ed = ep.open_converted(str(input_path))
        ds = ep.calibrate.compute_Sv(ed)
        calibrated = True

    logger.info("Computing attenuated-signal mask...")
    mask = mask_attenuated_signal(
        ds_Sv=ds,
        upper_limit_sl=upper_limit_sl,
        lower_limit_sl=lower_limit_sl,
        num_side_pings=num_side_pings,
        attenuation_signal_threshold=attenuation_signal_threshold,
        range_var=range_var,
    )
    return ds, mask, calibrated


def write_mask(mask, output_path: Path):
    """Save the mask as variable 'attenuated_mask' (True = attenuated, as echopype emits it)."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Saving attenuated-signal mask to {output_path} ...")
    # Wrap DataArray into a Dataset for clearer NetCDF structure
    mask_ds = mask.to_dataset(name="attenuated_mask")
    _add_basic_attrs(mask_ds)
    mask_ds.to_netcdf(output_path, mode="w", format="NETCDF4")


def write_cleaned(ds, mask, output_path: Path):
    """Write Sv with attenuated pings set to NaN; every other variable as is."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Applying mask to Sv and writing cleaned Sv to {output_path} ...")
    ds_clean = ds.copy()
    noise = mask if mask.dtype == bool else mask.fillna(0) != 0
    # Keep values where NOT attenuated signal
    ds_clean["Sv"] = (
        ds_clean["Sv"].where(~noise, other=float("nan")).transpose(*ds["Sv"].dims)
    )
    _add_basic_attrs(ds_clean)
    ds_clean.to_netcdf(output_path, mode="w", format="NETCDF4")


if __name__ == "__main__":
    main()
