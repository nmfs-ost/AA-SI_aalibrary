#!/usr/bin/env python3
"""
aa-detect-shoal

Console tool to detect shoals in Sv using Echopype's dispatcher
`echopype.mask.detect_shoal(ds, method, params)` and save a 2D boolean mask
(optionally also write an Sv file with the mask applied: only the Sv inside
the shoals is kept).

Pipeline-friendly: reads the input path (or gs:// URI) from the positional
argument or stdin, writes the mask's path to stdout, all logs to stderr.
Every output carries the input's provenance plus this step.
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
import ast
import io
import pprint
from contextlib import redirect_stdout
from pathlib import Path

from aalibrary.console._core import (
    Help, Run, ToolSpec, add_common_flags, canon, render, show_help, stdio, uris,
)

# xarray / echopype are imported inside process_file so --help stays fast.

SPEC = ToolSpec(
    name="aa-detect-shoal",
    role="transform",
    kind="mask",
    op="echopype.mask.detect_shoal",
    op_version=1,
    # 'param' is replaced by the tool's own parsed dict (see main()).
    params={"method": canon.choice(), "param": canon.kv()},
)

# Recorded in the provenance when the input had no Sv and was calibrated here.
IMPLICIT_SV = {"implicit_step": "echopype.calibrate.compute_Sv (defaults)"}

HELP = Help(
    summary="Detect shoals in Sv and write a shoal mask; optionally the Sv inside the shoals.",
    does=(
        "Runs echopype.mask.detect_shoal with the chosen method on one channel "
        "and writes 'shoal_mask' (ping_time x range_sample, True = inside a "
        "shoal). --apply also writes a copy of the Sv that KEEPS ONLY the "
        "samples inside the shoals (everything else set to NaN), i.e. the "
        "school echoes, not a school-free echogram."
    ),
    stdin="One Sv .nc path or gs:// URI (aa-sv, aa-clean ... output). A file without "
          "'Sv' is calibrated as EchoData first (compute_Sv defaults, recorded in the "
          "provenance as an implicit step).",
    stdout=("The mask file's absolute path (or gs:// URI). The --apply file is not "
            "printed there; its path goes to stderr."),
    options=[
        ("--method weill|echoview", "REQUIRED. Detector. echoview also needs idim/jdim "
                                    "arrays and is not usable from the command line."),
        ("--param KEY=VALUE ...", "Detector arguments. weill: var_name=Sv (required), "
                                  "channel=<id from the 'channel' coordinate; quote it> "
                                  "(required for multi-channel data), thr (-70 dB), "
                                  "maxvgap (5), maxhgap (0), minvlen (0), minhlen (0)."),
        ("--apply", "also write the Sv inside the shoals (kind sv)"),
        ("--no-overwrite", "exit 1, before writing anything, if the mask or the --apply "
                           "output exists and is not the identical product"),
        ("--quiet", "only warnings and errors on stderr"),
        ("-o, --output_path PATH", "Explicit mask output, used exactly as given. Local "
                                   "path or gs:// URI. Does not move the --apply output."),
    ],
    science={
        "method": "Detector (echopype dispatcher key): weill or echoview.",
        "param": ("Detector arguments, parsed as Python literals ('12dB' stays text). "
                  "Arguments left out are hashed with the method's own defaults (read "
                  "from the installed echopype), so writing a default out, key order, "
                  "and 5 vs 5.0 give the same hash."),
    },
    files=(
        "Reads Sv .nc, local or gs://. Writes <base>_<hash>.nc for the mask and "
        "for the --apply output (each has its own hash) beside the input (current "
        "directory for gs:// input), or --dest; -o names only the mask. Identical "
        "earlier results are reused. AA_NAMING=legacy: <stem>_detect_shoal_mask.nc "
        "and <stem>_detect_shoal_cleaned.nc, the latter always beside the input."
    ),
    pipeline=(
        "After aa-sv/aa-clean: ... | aa-sv | aa-detect-shoal ... | aa-graph draws "
        "the mask. The next stage receives the mask."
    ),
    examples=[
        "aa-nc D20160703-T060000.raw --sonar_model EK60 | aa-sv | aa-detect-shoal \\",
        "    --method weill --param var_name=Sv \"channel=GPT   38 kHz 00907205c001-1 ES38B\" "
        "thr=-60 --apply",
    ],
)


def print_help():
    """Curated help (also used by the docs generator)."""
    sys.stdout.write(render(SPEC, HELP, _build_parser()))


def print_help_full():
    help_text = """
    Usage: aa-detect-shoal [OPTIONS] [INPUT_PATH]

    Arguments:
      INPUT_PATH                 Path (or gs:// URI) to a calibrated Sv NetCDF (.nc),
                                 or a converted Echopype file that can be calibrated.
                                 Optional; defaults to stdin if not provided.

    Options:
      -o, --output_path PATH     Where to write the shoal mask (NetCDF), used
                                 exactly as given (local path or gs:// URI).
                                 Default: <base>_<hash>.nc beside the input
                                 (AA_NAMING=legacy: <stem>_detect_shoal_mask.nc).
      --apply                    Also apply the mask to Sv and write the Sv INSIDE
                                 the shoals (samples outside set to NaN). Default
                                 name <base>_<hash>.nc beside the input
                                 (AA_NAMING=legacy: <stem>_detect_shoal_cleaned.nc).
                                 Its path is printed to stderr.

      # detect_shoal parameters
      --method STR               Shoal detection method (dispatcher key), 'weill' or
                                 'echoview'. (required) echoview needs idim/jdim
                                 numpy arrays and cannot be driven from here.
      --param KEY=VAL [...]      Parameters for the chosen method as key=value pairs.
                                 Values are safely parsed (int/float/bool/None) when possible;
                                 strings like '10m' or '12.0dB' remain strings.
                                 weill: var_name (required, e.g. Sv), channel (an id
                                 from the 'channel' coordinate; required for
                                 multi-channel data; quote it), thr (-70),
                                 maxvgap (5), maxhgap (0), minvlen (0), minhlen (0).
                                 List the channel ids with:
                                   python -c "import xarray as xr; print(*xr.open_dataset('Sv.nc').channel.values, sep=chr(10))"

      --no-overwrite             Do not overwrite an existing, different output file:
                                 exit 1 before anything is written if the mask or the
                                 --apply output exists and is not the identical product.
      --quiet                    Suppress info logs; print only the final output path.
      --base NAME                Base name for the outputs.
      --dest DIR|gs://PREFIX     Write the default-named outputs there.
      --force                    Recompute even if identical products exist.
      -h, --help                 Show the short help; --help-all shows this text.

    Description:
      Dispatches shoal detection to the chosen method via Echopype's
      `detect_shoal(ds, method, params)` and returns a 2D boolean mask
      (True = inside shoal). Optionally applies the mask to Sv and writes
      the in-shoal Sv. Provenance is embedded in every output; see aa-metadata.

    Example:
      aa-detect-shoal Sv.nc --method weill --param var_name=Sv \\
          "channel=GPT   38 kHz 00907205c001-1 ES38B" thr=-60 --apply
    """
    print(help_text)


def _build_parser():
    parser = argparse.ArgumentParser(
        description="Detect shoals in Sv using Echopype's detect_shoal dispatcher.",
        add_help=False,  # help is handled by show_help()
    )

    # IO
    parser.add_argument(
        "input_path",
        type=str,
        nargs="?",
        help="Path or gs:// URI of a NetCDF file containing Sv (preferred) or a "
             "converted file to calibrate.",
    )
    parser.add_argument(
        "-o", "--output_path",
        type=str,
        help="Output path for the shoal mask NetCDF, used as given.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Also write the Sv inside the shoals (mask applied).",
    )
    parser.add_argument(
        "--no-overwrite",
        action="store_true",
        help="Do not overwrite an existing output file.",
    )
    parser.add_argument("--quiet", action="store_true", help="Suppress logs; print only output path.")

    # detect_shoal params
    parser.add_argument("--method", required=True,
                        help="Shoal detection method name (dispatcher key), e.g., 'echoview', 'weill'.")
    parser.add_argument("--param", nargs="*",
                        help="Additional method parameters as key=value pairs.")
    add_common_flags(parser)
    return parser


def _configure_logging(quiet: bool) -> None:
    """INFO progress on stderr by default (as this tool always did); --quiet: warnings only."""
    logger.remove()
    logger.add(sys.stderr, level="WARNING" if quiet else "INFO")


def _parse_kv_pairs(pairs):
    """
    Convert KEY=VAL strings into a dict with safe literal parsing.
    Leaves units-bearing strings (e.g., '10m', '12.0dB') as-is.
    """
    out = {}
    for p in pairs or []:
        if "=" not in p:
            raise argparse.ArgumentTypeError(f"Invalid key=value pair: {p}")
        k, v = p.split("=", 1)
        k = k.strip()
        v = v.strip()
        try:
            out[k] = ast.literal_eval(v)
        except Exception:
            out[k] = v
    return out


def _add_basic_attrs(ds) -> None:
    """Replace None attrs with strings to avoid NetCDF writer issues."""
    for k, v in list(ds.attrs.items()):
        if v is None:
            ds.attrs[k] = "NA"
    for var in ds.data_vars:
        for k, v in list(ds[var].attrs.items()):
            if v is None:
                ds[var].attrs[k] = "NA"


def _exists(target: str) -> bool:
    return uris.stat(target) is not None if uris.is_gcs(target) else Path(target).exists()


def main():
    """Entry point for the aa-detect-shoal CLI."""
    # No args on a terminal: help. (An empty pipe is an error: see stdio.)
    if len(sys.argv) == 1 and not stdio.stdin_is_piped():
        print_help()
        sys.exit(0)

    parser = _build_parser()
    if show_help(SPEC, HELP, parser, full=print_help_full):
        sys.exit(0)

    args = parser.parse_args()
    _configure_logging(args.quiet)
    # Used stripped, exactly as hashed (echopype matches the key exactly).
    args.method = args.method.strip()

    # Build dispatcher params early: a bad pair fails before any work, and the
    # parsed dict is what enters the hash.
    try:
        params = _parse_kv_pairs(args.param)
    except argparse.ArgumentTypeError as e:
        logger.error(str(e))
        sys.exit(1)

    # Resolve/validate
    token = stdio.one_input(args.input_path, SPEC.name)
    # The hash sees the method's defaults filled in, so an argument written
    # out at its default value is the same product as one left out.
    run = Run(SPEC, args, params={"param": _with_method_defaults(args.method, params)})
    src = run.input(token)

    out = run.plan(
        ext=".nc",
        explicit=args.output_path or None,
        legacy=lambda: src.local.with_stem(
            src.local.stem + "_detect_shoal_mask").with_suffix(".nc"),
    )
    cleaned_out = None
    if args.apply:
        cleaned_out = run.plan(
            ext=".nc", variant="apply", kind="sv",
            legacy=lambda: src.local.with_stem(
                src.local.stem + "_detect_shoal_cleaned").with_suffix(".nc"),
        )
    outputs = [o for o in (out, cleaned_out) if o is not None]

    # Guard against clobbering the input.
    for o in outputs:
        if not o.remote and Path(o.target).resolve() == src.local.resolve():
            logger.error(f"Refusing to overwrite input file: {src.local.resolve()}")
            sys.exit(1)

    # Identical products are reused (never an "overwrite").
    for o in outputs:
        run.reusable(o)
    if all(o.reused for o in outputs):
        _finish_all(run, out, cleaned_out, extra=None)
        return

    # --no-overwrite: check every requested output before writing anything.
    if args.no_overwrite:
        if not out.reused and _exists(out.target):
            logger.error(f"Output file '{out.target}' exists and --no-overwrite was set.")
            sys.exit(1)
        if cleaned_out is not None and not cleaned_out.reused and _exists(cleaned_out.target):
            logger.error(
                f"Cleaned output '{cleaned_out.target}' exists and --no-overwrite was set.")
            sys.exit(1)

    try:
        logger.debug(f"\naa-detect-shoal args:\n"
                     f"{pprint.pformat(dict(vars(args), params=params, product=out.hash))}")

        calibrated = process_file(
            input_path=src.local,
            mask_path=None if out.reused else out.local,
            cleaned_path=(cleaned_out.local
                          if cleaned_out is not None and not cleaned_out.reused else None),
            method=args.method,
            params=params,
        )

        _finish_all(run, out, cleaned_out, extra=IMPLICIT_SV if calibrated else None)
        logger.info("Shoal detection complete.")

    except Exception as e:
        logger.exception(f"Error during shoal detection: {e}")
        sys.exit(1)


def _finish_all(run, out, cleaned_out, extra=None):
    """The --apply output first (path to stderr), then the mask to stdout."""
    if cleaned_out is not None:
        run.finish(cleaned_out, emit=False, extra=extra)
        print(f"{SPEC.name}: in-shoal Sv: {cleaned_out.target}", file=sys.stderr)
    run.finish(out, extra=extra)


def _with_method_defaults(method: str, params: dict) -> dict:
    """The --param dict as the hash sees it: the method's defaults, then the user's values.

    echopype's detect_shoal passes ``params`` straight to the method as
    keyword arguments, so an argument left out takes the method's signature
    default; writing that default out computes the same product. Unknown
    methods (which fail later anyway) are hashed as given.
    """
    import inspect

    merged = {}
    try:
        from echopype.mask.api import METHODS_SHOAL

        fn = METHODS_SHOAL.get(method)
        sig = inspect.signature(fn) if fn is not None else None
    except Exception:
        sig = None
    if sig is not None:
        for name in list(sig.parameters)[1:]:          # the first is the dataset
            p = sig.parameters[name]
            if (p.kind in (p.POSITIONAL_OR_KEYWORD, p.KEYWORD_ONLY)
                    and p.default is not p.empty):
                merged[name] = p.default
    merged.update(params)
    return merged


def process_file(input_path: Path, mask_path: Path | None, cleaned_path: Path | None,
                 method: str, params: dict):
    """Detect shoals; write the mask (unless ``mask_path`` is None: already
    there) and, if ``cleaned_path`` is given, the Sv inside the shoals.

    Returns True when the input had no Sv and was calibrated here."""
    import echopype as ep
    import xarray as xr
    from echopype.mask import detect_shoal, apply_mask

    # Load dataset quietly (keep stdout clean for piping)
    f = io.StringIO()
    with redirect_stdout(f):
        ds = xr.open_dataset(input_path)

    # Ensure we have calibrated Sv; some files may be converted but not calibrated.
    calibrated = False
    if "Sv" not in ds.data_vars:
        logger.info("No 'Sv' variable found; attempting to calibrate to Sv via Echopype...")
        ed = ep.open_converted(str(input_path))
        ds = ep.calibrate.compute_Sv(ed)
        calibrated = True

    logger.info(f"Detecting shoals with method='{method}' and params={params} ...")
    mask = detect_shoal(ds=ds, method=method, params=params)

    # Save mask (wrap DA -> DS for NetCDF structure)
    if mask_path is not None:
        mask_ds = mask.to_dataset(name="shoal_mask")
        _add_basic_attrs(mask_ds)
        Path(mask_path).parent.mkdir(parents=True, exist_ok=True)
        logger.info("Saving shoal mask ...")
        mask_ds.to_netcdf(mask_path, mode="w", format="NETCDF4")

    # Optionally apply the mask to Sv: apply_mask keeps the samples where the
    # mask is True, i.e. the Sv inside the shoals.
    if cleaned_path is not None:
        logger.info("Applying shoal mask to Sv and saving the in-shoal Sv ...")
        ds_clean = apply_mask(source_ds=ds, mask=mask, var_name="Sv")
        _add_basic_attrs(ds_clean)
        Path(cleaned_path).parent.mkdir(parents=True, exist_ok=True)
        ds_clean.to_netcdf(cleaned_path, mode="w", format="NETCDF4")

    return calibrated


if __name__ == "__main__":
    main()
