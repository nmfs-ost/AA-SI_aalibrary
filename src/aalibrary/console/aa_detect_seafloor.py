#!/usr/bin/env python3
"""
aa-detect-seafloor

Console tool to detect the seafloor (bottom line) using Echopype's
dispatcher echopype.mask.detect_seafloor(ds, method, params) and save
the result. Optionally emits a 2D below-bottom mask and/or writes a copy
of Sv with the sub-bottom samples removed.

Pipeline-friendly: reads input path (or gs:// URI) from positional arg or
stdin, writes the bottom-line file's path to stdout, all logs to stderr.
Every output carries the input's provenance plus this step.

Typical pipeline usage:
    aa-nc --sonar_model EK60 input.raw | aa-sv | aa-depth | \\
        aa-detect-seafloor --method basic \\
            --param var_name=Sv "channel=<channel id>" "threshold=(-30,10)" --apply
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
# Without this, any exception in process_file disappears silently and
# the pipeline downstream gets no input — a confusing failure mode.
logger.add(sys.stderr, level="WARNING")

import argparse
import ast
import pprint
from pathlib import Path

from aalibrary.console._core import (
    Help, Run, ToolSpec, add_common_flags, canon, naming, render, show_help, stdio, uris,
)

# xarray / echopype are imported inside process_file so --help stays fast.

SPEC = ToolSpec(
    name="aa-detect-seafloor",
    role="transform",
    kind="seafloor",
    op="echopype.mask.detect_seafloor",
    op_version=1,
    # 'param' is replaced by the tool's own parsed dict (see main()).
    # --range-label only shapes the mask/cleaned outputs, so it enters only
    # their hashes (extra_params on those plan() calls).
    params={"method": canon.choice(), "param": canon.kv()},
)

# Recorded in the provenance when the input had no Sv and was calibrated here.
IMPLICIT_SV = {"implicit_step": "echopype.calibrate.compute_Sv (defaults)"}

HELP = Help(
    summary="Detect the seafloor line; optionally write a below-bottom mask and Sv without sub-bottom samples.",
    does=(
        "Runs echopype.mask.detect_seafloor with the chosen method on one "
        "channel and writes the bottom depth per ping ('seafloor', m, over "
        "ping_time). --emit-mask also writes 'seafloor_mask' (True = below the "
        "bottom), from comparing --range-label (default echo_range) with the "
        "bottom line. --apply also writes a copy of the Sv with the samples "
        "below the bottom set to NaN: the water column is kept."
    ),
    stdin=(
        "One Sv .nc/.netcdf4 path or gs:// URI that has a 'depth' variable "
        "(aa-depth output); 'blackwell' also needs angle_alongship/athwartship "
        "(aa-splitbeam-angle). A file without 'Sv' is calibrated as EchoData first "
        "(compute_Sv defaults, recorded in the provenance as an implicit step)."
    ),
    stdout=(
        "The bottom-line file's absolute path (or gs:// URI). The --emit-mask and "
        "--apply files are not printed there; their paths go to stderr."
    ),
    options=[
        ("--method basic|blackwell", "REQUIRED. Detector."),
        ("--param KEY=VALUE ...", "Detector arguments. Both methods need var_name=Sv and "
                                  "channel=<an id from the 'channel' coordinate; quote "
                                  "it, it contains spaces>. basic: threshold (-50 = "
                                  "window -50..-40 dB; or (min,max)), offset_m (0.5), "
                                  "bin_skip_from_surface (200). blackwell: threshold "
                                  "(-75 or (Sv,theta,phi)), offset, r0, r1, wtheta, wphi."),
        ("--emit-mask", "also write the below-bottom mask (kind mask)"),
        ("--apply", "also write Sv with sub-bottom samples removed (kind sv)"),
        ("--range-label NAME", "variable compared with the bottom line (a depth) to "
                               "build the mask. Default echo_range, which equals depth "
                               "only when aa-depth applied no transducer depth offset, "
                               "tilt, or Platform/Beam offsets or angles; otherwise use "
                               "'depth' (the tool warns when they differ)."),
        ("--no-overwrite", "exit 1, before writing anything, if any requested output "
                           "exists and is not the identical product"),
        ("-o, --output_path PATH", "Explicit bottom-line output; the extension is forced "
                                   "to .nc. Local path or gs:// URI. Does not move the "
                                   "mask/cleaned outputs."),
    ],
    science={
        "method": "Detector (echopype dispatcher key), e.g. basic, blackwell.",
        "param": ("Detector arguments, parsed as Python literals ('10m' stays text). "
                  "Arguments left out are hashed with the method's own defaults (read "
                  "from the installed echopype), so writing a default out, key order, "
                  "and 5 vs 5.0 give the same hash; pass integers where echopype wants "
                  "them (bin_skip_from_surface=200)."),
        "range_label": ("Variable compared with the bottom line (depth). Changes only "
                        "the --emit-mask and --apply outputs."),
    },
    files=(
        "Reads Sv .nc/.netcdf4, local or gs://. Writes <base>_<hash>.nc for each "
        "output (bottom line, mask, cleaned Sv; each has its own hash) beside the "
        "input (current directory for gs:// input), or --dest. -o names only the "
        "bottom line. Identical earlier results are reused. AA_NAMING=legacy: "
        "<stem>_seafloor.nc, <stem>_seafloor_mask.nc, <stem>_seafloor_cleaned.nc, "
        "the last two always beside the input."
    ),
    pipeline=(
        "After aa-depth: aa-nc | aa-sv | aa-depth | aa-detect-seafloor ... The next "
        "stage receives the bottom line. For the cleaned Sv, run the chain in "
        "steps and take its path from stderr, or use AA_NAMING=legacy."
    ),
    examples=[
        "SV=$(aa-nc D20160703-T060000.raw --sonar_model EK60 | aa-sv | aa-depth)",
        "aa-detect-seafloor \"$SV\" --method basic --param var_name=Sv \\",
        "    \"channel=GPT   38 kHz 00907205c001-1 ES38B\" \"threshold=(-30,10)\" --emit-mask --apply",
    ],
    notes=[
        "Before this version --apply kept only the sub-bottom samples (the mask "
        "was applied as 'keep where below bottom'). It now keeps the water column. "
        "The mask file itself is unchanged: True = below the bottom."
    ],
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
    Usage: aa-detect-seafloor [OPTIONS] [INPUT_PATH]

    Arguments:
    INPUT_PATH                  Path (or gs:// URI) to the calibrated Sv .nc /
                                .netcdf4 file with a 'depth' variable (aa-depth
                                output), or a converted Echopype file that
                                can be calibrated to Sv (it then has no depth,
                                which both methods need).
                                Optional. Defaults to stdin if not provided.

    Options:
    -o, --output_path           Path (or gs:// URI) to save the bottom-line
                                dataset; the suffix is forced to .nc.
                                Default: <base>_<hash>.nc beside the input
                                (AA_NAMING=legacy: same directory as input,
                                with '_seafloor' appended to the stem and a
                                .nc suffix). Does not affect the mask or
                                cleaned outputs.

    --method                    Seafloor detection method (dispatcher key),
                                'basic' or 'blackwell'. (REQUIRED)
    --param KEY=VAL [...]       Parameters for the chosen method as
                                key=value pairs. Values are safely parsed
                                (int / float / bool / None / tuple) when
                                possible; strings like '10m' remain strings.
                                Both methods require var_name (e.g. Sv) and
                                channel (an id from the 'channel' coordinate;
                                quote the pair, the id contains spaces). List
                                the ids with:
                                  python -c "import xarray as xr; print(*xr.open_dataset('Sv.nc').channel.values, sep=chr(10))"
                                basic:     threshold (default -50: the window
                                           -50..-40 dB; or a (min, max) tuple),
                                           offset_m (0.5),
                                           bin_skip_from_surface (200).
                                blackwell: threshold (-75, or (Sv, theta, phi)),
                                           offset (0.3), r0 (0), r1 (500),
                                           wtheta (28), wphi (52); needs the
                                           split-beam angles (aa-splitbeam-angle).

    --emit-mask                 Also compute and save a 2D boolean mask of
                                samples below the bottom line
                                (True = below bottom). Default name
                                <base>_<hash>.nc beside the input
                                (AA_NAMING=legacy: suffix '_seafloor_mask').
    --range-label               Range/depth variable name used to build the
                                mask. Default: echo_range. The bottom line is
                                a depth, so echo_range is right only when
                                depth == echo_range, i.e. aa-depth applied no
                                transducer depth offset, tilt, or Platform/Beam
                                offsets or angles. Otherwise pass 'depth'; the
                                tool warns on stderr when they differ.
    --apply                    Write a cleaned copy of Sv with the samples
                                below the bottom set to NaN (the water column
                                is kept). Default name <base>_<hash>.nc beside
                                the input (AA_NAMING=legacy: suffix
                                '_seafloor_cleaned'). Implies mask construction.

    --no-overwrite              Do not overwrite existing output files: exit 1,
                                before writing anything, if any requested
                                output exists and is not the identical product
                                (identical products are reused).

    --base NAME                 Base name for the outputs.
    --dest DIR|gs://PREFIX      Write the default-named outputs there.
    --force                     Recompute even if identical products exist.

    Description:
    Dispatches to detect_seafloor(ds, method, params) and returns a 1-D
    bottom line (per ping). With --emit-mask, builds a 2D mask by
    comparing range to the bottom line (True below bottom). With
    --apply, applies the inverse of that mask to Sv via
    echopype.mask.apply_mask, so the water column is kept and the
    sub-bottom samples become NaN. The bottom-line path is printed to
    stdout for piping into the next stage of the pipeline; the mask and
    cleaned paths are printed to stderr. Provenance is embedded in every
    output; see aa-metadata.

    Examples:
        aa-detect-seafloor input_Sv.nc --method basic --param var_name=Sv \\
              "channel=GPT   38 kHz 00907205c001-1 ES38B" --emit-mask
        aa-sv input.nc | aa-depth | aa-detect-seafloor --method basic \\
              --param var_name=Sv "channel=GPT   38 kHz 00907205c001-1 ES38B" \\
              "threshold=(-30,10)" --apply
    """
    print(help_text)


def _build_parser():
    parser = argparse.ArgumentParser(
        description="Detect the seafloor with Echopype's detect_seafloor dispatcher.",
        add_help=False,
    )

    # I/O
    parser.add_argument(
        "input_path",
        type=str,
        nargs="?",
        help="Path or gs:// URI of the .nc / .netcdf4 file containing Sv.",
    )
    parser.add_argument(
        "-o", "--output_path",
        type=str,
        help="Path to save the bottom line (suffix forced to .nc).",
    )
    parser.add_argument(
        "--no-overwrite",
        dest="no_overwrite",
        action="store_true",
        help="Do not overwrite existing outputs.",
    )

    # detect_seafloor params
    parser.add_argument(
        "--method",
        required=True,
        help="Seafloor detection method key (e.g., 'basic', 'blackwell').",
    )
    parser.add_argument(
        "--param",
        nargs="*",
        default=None,
        help="Method parameters as key=value pairs.",
    )

    # mask / apply
    parser.add_argument(
        "--emit-mask",
        dest="emit_mask",
        action="store_true",
        help="Also compute/save a 2D mask (True = below bottom).",
    )
    parser.add_argument(
        "--range-label",
        dest="range_label",
        default="echo_range",
        help="Range/depth variable name used to build mask (default: echo_range).",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write Sv with the samples below the bottom removed.",
    )
    add_common_flags(parser)
    return parser


def _exists(target: str) -> bool:
    return uris.stat(target) is not None if uris.is_gcs(target) else Path(target).exists()


def main():
    # No args on a terminal: help. (An empty pipe is an error: see stdio.)
    if len(sys.argv) == 1 and not stdio.stdin_is_piped():
        print_help()
        sys.exit(0)

    parser = _build_parser()
    if show_help(SPEC, HELP, parser, full=print_help_full):
        sys.exit(0)

    args = parser.parse_args()
    # Used stripped, exactly as hashed (echopype matches these names exactly).
    args.method = args.method.strip()
    args.range_label = args.range_label.strip()

    # Parse --param key=value pairs early so a bad pair fails before
    # we touch the dataset. The parsed dict is what enters the hash.
    try:
        params = _parse_kv_pairs(args.param)
    except argparse.ArgumentTypeError as e:
        logger.error(str(e))
        sys.exit(1)

    # ---------------------------
    # Validate input
    # ---------------------------
    token = stdio.one_input(args.input_path, SPEC.name)
    # The hash sees the method's defaults filled in, so an argument written
    # out at its default value is the same product as one left out.
    run = Run(SPEC, args, params={"param": _with_method_defaults(args.method, params)})
    src = run.input(token)

    allowed_extensions = {".netcdf4", ".nc"}
    ext = src.local.suffix.lower()
    if ext not in allowed_extensions:
        logger.error(
            f"'{src.name}' is not a supported file type. "
            f"Allowed: {', '.join(sorted(allowed_extensions))}"
        )
        sys.exit(1)

    # ---------------------------
    # Resolve output paths
    # ---------------------------
    explicit = naming.with_ext(args.output_path, ".nc") if args.output_path else None
    out = run.plan(
        ext=".nc",
        explicit=explicit,
        legacy=lambda: src.local.with_stem(src.local.stem + "_seafloor").with_suffix(".nc"),
    )
    side_params = {"range_label": canon.text(args.range_label)}
    mask_out = None
    if args.emit_mask:
        mask_out = run.plan(
            ext=".nc", variant="mask", kind="mask", extra_params=side_params,
            legacy=lambda: src.local.with_stem(
                src.local.stem + "_seafloor_mask").with_suffix(".nc"),
        )
    cleaned_out = None
    if args.apply:
        cleaned_out = run.plan(
            ext=".nc", variant="apply", kind="sv", extra_params=side_params,
            legacy=lambda: src.local.with_stem(
                src.local.stem + "_seafloor_cleaned").with_suffix(".nc"),
        )
    outputs = [o for o in (out, mask_out, cleaned_out) if o is not None]

    # Guard against clobbering the input, for every output.
    input_resolved = src.local.resolve()
    for o in outputs:
        if not o.remote and Path(o.target).resolve() == input_resolved:
            logger.error(f"Refusing to overwrite input file: {input_resolved}")
            sys.exit(1)

    # Identical products are reused (never an "overwrite").
    for o in outputs:
        run.reusable(o)
    if all(o.reused for o in outputs):
        _finish_all(run, out, mask_out, cleaned_out, extra=None)
        return

    # Optional: refuse to overwrite existing, different outputs. All are
    # checked before anything is written.
    if args.no_overwrite:
        if not out.reused and _exists(out.target):
            logger.error(f"Output '{out.target}' exists and --no-overwrite was set.")
            sys.exit(1)
        if mask_out is not None and not mask_out.reused and _exists(mask_out.target):
            logger.error(f"Mask output '{mask_out.target}' exists and --no-overwrite was set.")
            sys.exit(1)
        if cleaned_out is not None and not cleaned_out.reused and _exists(cleaned_out.target):
            logger.error(
                f"Cleaned output '{cleaned_out.target}' exists and --no-overwrite was set.")
            sys.exit(1)

    # ---------------------------
    # Process file
    # ---------------------------
    try:
        args_summary = {
            "input_path": token,
            "output_path": out.target,
            "mask_path": mask_out.target if mask_out else None,
            "cleaned_path": cleaned_out.target if cleaned_out else None,
            "method": args.method,
            "params": params,
            "emit_mask": args.emit_mask,
            "apply": args.apply,
            "range_label": args.range_label,
            "no_overwrite": args.no_overwrite,
            "product": out.hash,
        }
        logger.debug(
            f"Executing aa-detect-seafloor configured with [OPTIONS]:\n"
            f"{pprint.pformat(args_summary)}"
        )

        # Outputs that are being reused are not written again.
        calibrated = process_file(
            input_path=src.local,
            output_path=None if out.reused else out.local,
            method=args.method,
            params=params,
            emit_mask=mask_out is not None and not mask_out.reused,
            apply_mask=cleaned_out is not None and not cleaned_out.reused,
            range_label=args.range_label,
            mask_path=mask_out.local if mask_out is not None else None,
            cleaned_path=cleaned_out.local if cleaned_out is not None else None,
        )

        logger.success(
            f"Generated {out.target} with aa-detect-seafloor. Passing .nc path to stdout..."
        )
        _finish_all(run, out, mask_out, cleaned_out,
                    extra=IMPLICIT_SV if calibrated else None)

    except Exception as e:
        logger.exception(f"Error during processing: {e}")
        sys.exit(1)


def _finish_all(run, out, mask_out, cleaned_out, extra=None):
    """Side outputs first (paths to stderr), then the bottom line to stdout."""
    if mask_out is not None:
        run.finish(mask_out, emit=False, extra=extra)
        print(f"{SPEC.name}: mask: {mask_out.target}", file=sys.stderr)
    if cleaned_out is not None:
        run.finish(cleaned_out, emit=False, extra=extra)
        print(f"{SPEC.name}: cleaned Sv: {cleaned_out.target}", file=sys.stderr)
    # Pipe the bottom-line path to stdout for the next tool
    run.finish(out, extra=extra)


def _with_method_defaults(method: str, params: dict) -> dict:
    """The --param dict as the hash sees it: the method's defaults, then the user's values.

    echopype's detect_seafloor passes ``params`` straight to the method as
    keyword arguments, so an argument left out takes the method's signature
    default; writing that default out computes the same product. Unknown
    methods (which fail later anyway) are hashed as given.
    """
    import inspect

    merged = {}
    try:
        from echopype.mask.api import METHODS_BOTTOM

        fn = METHODS_BOTTOM.get(method)
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


def _parse_kv_pairs(pairs):
    """Convert KEY=VAL strings to dict; keep units strings ('10m','12dB')
    as strings, but coerce ints/floats/bools/None via ast.literal_eval."""
    out = {}
    for p in pairs or []:
        if "=" not in p:
            raise argparse.ArgumentTypeError(f"Invalid key=value pair: {p}")
        k, v = p.split("=", 1)
        k = k.strip()
        v = v.strip()
        try:
            out[k] = ast.literal_eval(v)
        except (ValueError, SyntaxError):
            out[k] = v
    return out


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
    output_path: Path | None,
    method: str,
    params: dict,
    emit_mask: bool = False,
    apply_mask: bool = False,
    range_label: str = "echo_range",
    mask_path: Path = None,
    cleaned_path: Path = None,
):
    """Load Sv from NetCDF, dispatch detect_seafloor, and save the bottom
    line (unless ``output_path`` is None: already there). Optionally save
    the 2D below-bottom mask, and optionally a copy of Sv with the
    sub-bottom samples removed.

    Returns True when the input had no Sv and was calibrated here."""
    import echopype as ep
    import numpy as np
    import xarray as xr
    from echopype.mask import detect_seafloor, apply_mask as ep_apply_mask

    logger.info(f"Loading dataset from {input_path}")
    ds = xr.open_dataset(input_path)

    # If 'Sv' isn't present, fall back to calibrating from a converted
    # Echopype file. Mirrors aa-sv's contract so this tool can sit
    # anywhere downstream of aa-nc in a pipeline.
    calibrated = False
    if "Sv" not in ds.data_vars:
        logger.info("No 'Sv' variable found; calibrating to Sv via Echopype")
        ed = ep.open_converted(str(input_path))
        ds = ep.calibrate.compute_Sv(ed)
        calibrated = True

    logger.info(f"Detecting seafloor with method='{method}' and params={params}")
    bottom = detect_seafloor(ds=ds, method=method, params=params)

    # Save the 1-D bottom line
    if output_path is not None:
        bottom_ds = bottom.to_dataset(name="seafloor")
        bottom_ds["seafloor"].attrs.setdefault("long_name", "Seafloor bottom line")
        bottom_ds["seafloor"].attrs.setdefault("units", "m")
        bottom_ds = clean_attrs(bottom_ds)

        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        logger.info("Saving seafloor bottom line")
        bottom_ds.to_netcdf(output_path, mode="w", format="NETCDF4")

    # The 2D mask is needed for either --emit-mask or --apply, so build
    # once and reuse.
    if emit_mask or apply_mask:
        # Pull the range/depth variable that the mask is defined against
        if (range_label not in ds
                and range_label not in ds.coords
                and range_label not in ds.data_vars):
            raise KeyError(
                f"Range variable '{range_label}' not found; cannot build bottom mask."
            )

        rng = ds[range_label]

        # The bottom line is a depth. Comparing it with echo_range is only
        # right while depth == echo_range (no transducer offset, tilt or
        # Platform/Beam values applied by aa-depth). The default stays
        # echo_range (as always); say so when it matters for this file.
        if range_label != "depth" and "depth" in ds.variables:
            try:
                gap = float(abs(ds["depth"] - rng).max(skipna=True))   # aligned by dim
            except Exception:
                gap = float("nan")
            if np.isfinite(gap) and gap > 1e-3:
                logger.warning(
                    f"the bottom line is a depth, but the mask compares it with "
                    f"'{range_label}', which differs from 'depth' by up to {gap:.2f} m "
                    "in this file (transducer depth offset, tilt, or Platform/Beam "
                    "offsets or angles from aa-depth). The mask and the --apply output "
                    "are off by that much; use --range-label depth to compare depth "
                    "with depth."
                )
        # echo_range is typically 2D (ping x range_sample); bottom is 1-D
        # over the ping/time axis. Broadcast bottom across the range
        # dimension so the comparison produces a 2D mask.
        try:
            bottom_b = (
                bottom.broadcast_like(rng)
                if set(bottom.dims) <= set(rng.dims)
                else bottom
            )
            mask2d = rng > bottom_b
        except Exception:
            # Fallback: align along the first shared dimension. This path
            # rarely fires but is cheap insurance for unusual coord names.
            shared = [d for d in bottom.dims if d in rng.dims]
            if not shared:
                raise RuntimeError(
                    "Cannot align bottom line with range variable to form a mask."
                )
            bottom_b = bottom.transpose(*shared).broadcast_like(rng)
            mask2d = rng > bottom_b

        if emit_mask:
            # The mask file keeps its meaning: True = below the bottom.
            mask_ds = mask2d.to_dataset(name="seafloor_mask")
            mask_ds = clean_attrs(mask_ds)
            Path(mask_path).parent.mkdir(parents=True, exist_ok=True)
            logger.info("Saving bottom mask")
            mask_ds.to_netcdf(mask_path, mode="w", format="NETCDF4")

        if apply_mask:
            # echopype.mask.apply_mask KEEPS the samples where the mask is
            # True and sets the rest to NaN. mask2d is True below the
            # bottom, so applying it as-is would keep only the sub-bottom
            # echoes (the bug this tool had). Apply its inverse: keep the
            # water column, remove everything below the bottom. Samples
            # with no range value (NaN padding) compare False in mask2d and
            # are kept; their Sv is already NaN.
            logger.info("Removing sub-bottom Sv and saving the cleaned copy")
            ds_clean = ep_apply_mask(source_ds=ds, mask=~mask2d, var_name="Sv")
            ds_clean = clean_attrs(ds_clean)
            Path(cleaned_path).parent.mkdir(parents=True, exist_ok=True)
            ds_clean.to_netcdf(cleaned_path, mode="w", format="NETCDF4")

    logger.success("Seafloor detection complete.")
    return calibrated


if __name__ == "__main__":
    main()
