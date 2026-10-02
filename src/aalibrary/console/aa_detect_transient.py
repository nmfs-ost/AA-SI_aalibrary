#!/usr/bin/env python3
"""
aa-detect-transient

Console tool for detecting transient noise in Sv using Echopype's dispatcher
`echopype.clean.detect_transient(ds, method, params)` and saving the boolean
mask it returns (True = VALID, False = transient noise), and, with --apply,
a copy of the Sv data with the transient samples set to NaN.

Pipeline-friendly: reads the input path (or gs:// URI) from a positional
arg or stdin, writes the MASK's path to stdout, all logs to stderr. Both
outputs carry the input's provenance plus this step.

Typical pipeline usage:
    aa-sv x.nc | aa-depth | aa-detect-transient --method matecho --apply
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
import ast
import io
import pprint
from contextlib import redirect_stdout
from pathlib import Path

from aalibrary.console._core import (
    Help, Run, ToolSpec, add_common_flags, naming, render, show_help, stdio,
)

# The scientific options are the method and its complete parameter set: the
# --param dict (after --range-var is merged in) with echopype's defaults for
# every key not given, read from the method's signature. main() hands both to
# Run(params=...), so an explicit default hashes like an omitted one. They are
# listed in HELP.science for the help text.
SPEC = ToolSpec(
    name="aa-detect-transient",
    role="transform",
    kind="mask",
    op="echopype.clean.detect_transient",
    op_version=1,
    params={},
)

IMPLICIT_SV = {"implicit_step": "echopype.calibrate.compute_Sv (defaults)"}

HELP = Help(
    summary="Transient mask, fielding or matecho (True = VALID).",
    does=(
        "Runs echopype.clean.detect_transient(ds, method, params). echopype "
        "0.11.1 has two methods; both flag pings that are louder than their "
        "neighbours in a deep window:\n\n"
        "  fielding  a ping whose median Sv in r0..r1 m exceeds the median of\n"
        "            +/- n pings by more than thr[0] (and whose 75th percentile\n"
        "            is below maxts) is flagged from where that excess drops\n"
        "            below thr[1] (searched upward in `jumps` m steps, not above\n"
        "            roff m) down to the end of the column.\n"
        "  matecho   a ping whose mean Sv in start_depth..start_depth+window_meter\n"
        "            m exceeds by more than delta_db the `percentile` of all Sv\n"
        "            in that window over the window_ping pings around it is\n"
        "            flagged over the whole column.\n\n"
        "The mask file holds one variable, transient_detect_mask: boolean, True = "
        "VALID (keep), False = transient noise -- the OPPOSITE of aa-transient. "
        "Its attribute 'meaning' says so. Dims (channel, ping_time, range_sample)."
    ),
    stdin=(
        "One Sv NetCDF path or gs:// URI that has the range variable (depth by "
        "default; add it with aa-depth). An EchoData file without Sv is "
        "calibrated first with compute_Sv defaults (recorded in the provenance "
        "as an implicit step)."
    ),
    stdout=(
        "The MASK's absolute path (or gs:// URI), one line. With --apply the "
        "cleaned Sv's path goes to stderr instead, as "
        "'aa-detect-transient: cleaned Sv: PATH' (also when it is reused)."
    ),
    metadata=(
        "Reads the input's provenance, appends this step (the method and all its "
        "parameters: yours, plus echopype's defaults for the rest), computes the "
        "product hash, and "
        "embeds it all in the mask (NetCDF attributes aa_provenance, "
        "aa_product_hash, aa_base, aa_tool, history). The --apply file is its "
        "own product: same step, variant 'apply', kind sv, its own hash. "
        "Inspect with: aa-metadata FILE"
    ),
    options=[
        ("--method fielding|matecho", "REQUIRED. The detector."),
        ("--param KEY=VAL ...", "Method parameters, parsed as Python literals. "
                                "Plain numbers in m, dB or pings (r0=900, not "
                                "r0=900m). Omitted keys take echopype's defaults. "
                                "Put INPUT_PATH before --param (or end the list "
                                "with --)."),
        ("-o, --output_path PATH", "The mask file, used exactly as given (no "
                                   "extension added). Local path or gs:// URI. "
                                   "Does not move the --apply file."),
        ("--apply", "Also write the input's Sv with transient samples (mask False) "
                    "set to NaN; valid samples are kept unchanged."),
    ],
    science={
        "method": "fielding or matecho.",
        "param": (
            "fielding: r0, r1 (m, default 900, 1000), n (pings, 30), "
            "thr (dB pair, (3, 1)), roff (m, 20), jumps (m, 5), maxts (dB, -35), "
            "start (pings, 0). matecho: start_depth (m, 220), window_meter (m, "
            "450), window_ping (pings, 100), percentile (25), delta_db (dB, 12), "
            "extend_ping (pings, 0), min_window (m, 20). Both: var_name (Sv). "
            "Omitted keys are recorded and hashed with echopype's default, so "
            "n=30 and leaving n out give the same hash. Unknown keys are refused."
        ),
        "range_var": "Vertical variable, passed as params['range_var'] unless "
                     "--param range_var=... is given.",
    },
    files=(
        "Reads NetCDF, local or gs://. Writes the mask to <base>_<hash>.nc beside "
        "the input (current directory for gs:// input), or -o, or --dest. With "
        "--apply a second <base>_<hash>.nc (a different hash) goes beside the "
        "input or into --dest, never to -o. AA_NAMING=legacy: "
        "<stem>_detect_transient_mask.nc and <stem>_detect_transient_cleaned.nc "
        "beside the input. Identical earlier results are reused."
    ),
    pipeline=(
        "After aa-sv and aa-depth: ... | aa-depth | aa-detect-transient --method "
        "matecho | aa-graph (draws the mask). Only the mask travels down the pipe."
    ),
    examples=[
        "aa-detect-transient sv_depth.nc --method matecho --param start_depth=220 "
        "window_meter=450 window_ping=100 percentile=25 delta_db=12",
        "aa-detect-transient sv_depth.nc --apply --method fielding --param r0=900 "
        "r1=1000 n=30 \"thr=(3, 1)\" roff=20 jumps=5 maxts=-35",
    ],
    notes=[
        "The defaults look deep (fielding 900-1000 m, matecho from 220 m). If the "
        "window is outside the data nothing is flagged: the mask is all True.",
    ],
)


def print_help():
    """Curated help (also used by the docs generator)."""
    sys.stdout.write(render(SPEC, HELP, _build_parser()))


def print_help_full():
    """The complete reference (--help-all)."""
    help_text = """
    Usage: aa-detect-transient [OPTIONS] [INPUT_PATH]

    Arguments:
      INPUT_PATH                 Path (or gs:// URI) to a calibrated Sv NetCDF
                                 (.nc), or a converted Echopype file that can be
                                 calibrated. Optional. Defaults to stdin if not
                                 provided (an empty stdin is an error, exit 1).

    Options:
      -o, --output_path PATH     Where to write the transient-noise mask (NetCDF),
                                 used exactly as given; local path or gs:// URI.
                                 Default: <base>_<hash>.nc beside the input
                                 (AA_NAMING=legacy: <stem>_detect_transient_mask.nc).
      --apply                    Also apply the mask to Sv and write a cleaned Sv
                                 file: <base>_<hash>.nc beside the input or in
                                 --dest (AA_NAMING=legacy:
                                 <stem>_detect_transient_cleaned.nc beside the
                                 input). Samples where the mask is False
                                 (transient noise) become NaN; valid samples are
                                 kept. -o never moves this file. stdout still
                                 gets only the mask path; this file's path goes
                                 to stderr ('aa-detect-transient: cleaned Sv: PATH').

      # detect_transient parameters
      --method STR               Transient detection method name (dispatcher key).
                                 REQUIRED. echopype 0.11.1: 'fielding' or 'matecho'.
      --param KEY=VAL [...]      Parameters for the chosen method as key=value pairs.
                                 Values are safely parsed (int/float/bool/None/
                                 tuple/str). Numbers carry no units.
                                 fielding: var_name, range_var, r0, r1, n, thr,
                                           roff, jumps, maxts, start
                                 matecho:  var_name, range_var, time_var,
                                           bottom_var, start_depth, window_meter,
                                           window_ping, percentile, delta_db,
                                           extend_ping, min_window
                                 --param takes every following word: give
                                 INPUT_PATH before it or end the list with '--'
                                 (a trailing word without '=' is taken as
                                 INPUT_PATH when none was given).

      --range-var STR            Name of the range/depth coordinate (passed to the
                                 method as range_var unless --param range_var=...
                                 is given). Default: depth

      --base NAME                Base name for the outputs.
      --dest DIR|gs://PREFIX     Write the default-named outputs there.
      --force                    Recompute even if identical products exist.
      -h, --help                 Short help. --help-all: this text.

    Description:
      Dispatches transient-noise detection to a selected method via Echopype's
      `detect_transient(ds, method, params)`. The mask it returns is saved as
      variable 'transient_detect_mask' with echopype's polarity:
      True = VALID (keep), False = transient noise (see its 'meaning'
      attribute). Optionally applies the mask to Sv to produce a cleaned Sv
      dataset. Provenance (the input's chain plus this step, with every
      method parameter: those given, plus echopype's defaults for the rest,
      so an explicit default hashes like an omitted one) is embedded in both
      files; see aa-metadata.

    Examples:
      aa-detect-transient data.nc --method matecho --param start_depth=220 window_meter=450 window_ping=100 percentile=25 delta_db=12
      aa-detect-transient data.nc --apply -o out_mask.nc --method fielding --param r0=900 r1=1000 n=30 "thr=(3, 1)" roff=20
    """
    print(help_text)


def _parse_kv_pairs(pairs):
    """
    Convert a list of KEY=VAL strings into a dict with safe literal parsing.
    - Tries int/float/bool/None via ast.literal_eval when possible.
    - Leaves plain strings as-is (so '10m' or '12.0dB' remain strings).
    """
    out = {}
    for p in pairs or []:
        if "=" not in p:
            raise argparse.ArgumentTypeError(f"Invalid key=value pair: {p}")
        k, v = p.split("=", 1)
        k = k.strip()
        v = v.strip()
        # Try safe literal -> if it fails, keep original string (good for '10m', '12.0dB')
        try:
            out[k] = ast.literal_eval(v)
        except Exception:
            out[k] = v
    return out


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
        description="Detect transient noise in Sv using Echopype's detect_transient dispatcher.",
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
        help="Also write Sv cleaned by the transient mask as a second product.",
    )

    # ---------------------------
    # detect_transient parameters
    # ---------------------------
    parser.add_argument(
        "--method",
        required=True,
        help="Transient detection method name: fielding or matecho.",
    )
    parser.add_argument(
        "--param",
        nargs="*",
        help="Method parameters as key=value pairs (e.g., r0=900 r1=1000 n=30).",
    )

    # Optional convenience if a method expects a range var name
    parser.add_argument("--range-var", dest="range_var", default="depth",
                        help="Range/depth variable name (default: depth).")
    add_common_flags(parser)
    return parser


def _method_defaults(method: str):
    """echopype's default for every parameter --param can set, or None.

    Read from the method's signature (the dispatcher calls it as
    ``method(ds, **params)``). None when that cannot be done safely: an
    unknown method, *args/**kwargs, or a parameter without a default.
    """
    try:
        import inspect

        from echopype.clean.api import METHODS_TRANSIENT
        sig = inspect.signature(METHODS_TRANSIENT[method])
    except Exception:
        return None
    out = {}
    for p in list(sig.parameters.values())[1:]:   # [0] is the dataset
        if p.kind not in (p.POSITIONAL_OR_KEYWORD, p.KEYWORD_ONLY) or p.default is p.empty:
            return None
        out[p.name] = p.default
    return out


def _known_methods():
    """The dispatcher's method names, or None if echopype hides them."""
    try:
        from echopype.clean.api import METHODS_TRANSIENT
        return sorted(METHODS_TRANSIENT)
    except Exception:
        return None


def _report_side(side):
    """The --apply file is not on stdout (the mask is); say where it is."""
    print(f"{SPEC.name}: cleaned Sv: {side.target}", file=sys.stderr)


def main():
    """Entry point for the aa-detect-transient CLI."""
    # No args on a terminal: help. (An empty pipe is an error: see stdio.)
    if len(sys.argv) == 1 and not stdio.stdin_is_piped():
        print_help()
        sys.exit(0)

    parser = _build_parser()
    if show_help(SPEC, HELP, parser, full=print_help_full):
        sys.exit(0)

    args = parser.parse_args()

    # --param takes every following word, so `--param n=10 in.nc` swallows
    # the input. Give a trailing word without '=' back to INPUT_PATH when
    # no input was given otherwise.
    if args.param and args.input_path is None and "=" not in args.param[-1]:
        args.input_path = args.param.pop()

    # ---------------------------
    # Build params for dispatcher (fail before any heavy work)
    # ---------------------------
    try:
        params = _parse_kv_pairs(args.param)
    except argparse.ArgumentTypeError as e:
        logger.error(str(e))
        sys.exit(1)
    # Provide range_var default via params only if user didn't already pass it.
    params.setdefault("range_var", args.range_var)

    known = _known_methods()
    if known is not None and args.method not in known:
        logger.error(f"Unsupported --method {args.method!r}; echopype offers: "
                     f"{', '.join(known)}")
        sys.exit(1)

    # The hash (and the provenance) get the complete parameter set: echopype's
    # defaults, overridden by what was given. echopype itself gets `params`.
    defaults = _method_defaults(args.method)
    if defaults is None:
        logger.warning(f"Could not read echopype's defaults for method {args.method!r}; "
                       "an explicit default will hash differently from an omitted one.")
        hashed = dict(params)
    else:
        unknown = sorted(set(params) - set(defaults))
        if unknown:
            logger.error(f"Unknown --param key(s) for {args.method}: {', '.join(unknown)}. "
                         f"Accepted: {', '.join(defaults)}")
            sys.exit(1)
        hashed = {**defaults, **params}

    # ---------------------------
    # Resolve/validate input
    # ---------------------------
    token = stdio.one_input(args.input_path, SPEC.name)
    run = Run(SPEC, args, params={"method": args.method, "params": hashed})
    src = run.input(token)

    # ---------------------------
    # Resolve output paths
    # ---------------------------
    # The mask: -o used verbatim, as always.
    out = run.plan(
        ext=".nc",
        explicit=args.output_path or None,
        legacy=lambda: naming.with_stem_suffix(src.local, "_detect_transient_mask", ".nc"),
    )
    # The --apply side output: never moved by -o (legacy: beside the input).
    side = None
    if args.apply:
        side = run.plan(
            ext=".nc",
            variant="apply",
            kind="sv",
            legacy=lambda: naming.with_stem_suffix(src.local, "_detect_transient_cleaned", ".nc"),
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
            "\naa-detect-transient args:\n"
            + pprint.pformat(vars(args) | {"params": params, "mask": out.target,
                                           "cleaned": side.target if side else None,
                                           "product": out.hash})
        )

        ds, mask, calibrated = compute_mask(src.local, args.method, params)
        extra = IMPLICIT_SV if calibrated else None

        if not mask_done:
            write_mask(mask, out.local)
        run.finish(out, extra=extra, emit=False)

        # Optionally apply mask to Sv and save a cleaned file
        if side is not None:
            if not side_done:
                write_cleaned(ds, mask, side.local)
            run.finish(side, extra=extra, emit=False)
            _report_side(side)

        # Echo the primary output (mask path) to stdout for piping
        stdio.emit(out.target)
        logger.info("Transient detection complete.")

    except Exception as e:
        logger.exception(f"Error during transient detection: {e}")
        sys.exit(1)


def compute_mask(input_path: Path, method: str, params: dict):
    """Load Sv (calibrating if needed) and run echopype's detect_transient.

    Returns (ds_Sv, mask, calibrated). The mask is echopype's: True = VALID.
    """
    import echopype as ep  # deferred so --help stays fast
    import xarray as xr
    from echopype.clean import detect_transient

    # Load dataset quietly
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

    logger.info(f"Detecting transient noise with method='{method}' and params={params} ...")
    mask = detect_transient(
        ds=ds,
        method=method,
        params=dict(params),
    )
    return ds, mask, calibrated


def write_mask(mask, output_path: Path):
    """Save the mask as 'transient_detect_mask' with echopype's polarity (True = VALID)."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Saving transient-detection mask to {output_path} ...")
    mask_ds = mask.to_dataset(name="transient_detect_mask")
    _add_basic_attrs(mask_ds)
    mask_ds.to_netcdf(output_path, mode="w", format="NETCDF4")


def _valid(mask):
    """Boolean 'keep' array from echopype's mask.

    detect_transient documents True = VALID (keep), False = transient noise,
    and its methods say so in the 'meaning' attribute. Refuse to guess if a
    method ever declares something else.
    """
    meaning = str(mask.attrs.get("meaning", ""))
    if meaning and "True = VALID" not in meaning:
        raise ValueError(f"unexpected mask polarity (meaning={meaning!r}); not applying it")
    return mask if mask.dtype == bool else mask.fillna(0) != 0


def write_cleaned(ds, mask, output_path: Path):
    """Write Sv with transient samples (mask False) set to NaN; valid samples kept."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Applying mask to Sv and writing cleaned Sv to {output_path} ...")
    ds_clean = ds.copy()
    # Keep values where the mask says VALID (True); NaN where transient (False).
    ds_clean["Sv"] = (
        ds_clean["Sv"].where(_valid(mask), other=float("nan")).transpose(*ds["Sv"].dims)
    )
    _add_basic_attrs(ds_clean)
    ds_clean.to_netcdf(output_path, mode="w", format="NETCDF4")


if __name__ == "__main__":
    main()
