#!/usr/bin/env python3
"""
aa-sv

Console tool for computing Sv (volume backscattering strength) from an
EchoData file (typically the output of aa-nc or aa-ed) using Echopype, and
saving it as NetCDF (default) or a Zarr store (--format zarr).

Pipeline-friendly: reads input path (or gs:// URI) from positional arg or
stdin, writes output path to stdout, all logs to stderr. The output
carries the input's provenance plus this calibration step.

Several inputs (the per-file EchoData stores of a range, say) are calibrated
one at a time, each by a fresh aa-sv, into --dest: each file's Sv on its
own, so nothing ever holds more than one file's worth. aa-combine then joins
them into the range's Sv (see aa-combine: joining Sv).
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
import pprint
from pathlib import Path

from aalibrary.console._core import (
    Help, Run, ToolSpec, add_common_flags, canon, naming, render, show_help, stdio, uris,
)


def _waveform(value):
    # echopype treats FM as BB, so they are the same computation.
    if value is None:
        return None
    v = str(value).strip().upper()
    return "BB" if v in {"BB", "FM"} else v


SPEC = ToolSpec(
    name="aa-sv",
    role="transform",
    kind="sv",
    op="echopype.calibrate.compute_Sv",
    op_version=1,
    params={
        "waveform_mode": _waveform,
        "encode_mode": canon.choice("lower"),
        # Added later: hashed only when given, so plain runs keep their hash.
        # The parsed dicts are passed to Run() (see main); a per-channel key
        # is spelled name@<Hz>. The ECS file is an input (role
        # "calibration"): its content enters the hash.
        "env_params": canon.kv(),
        "cal_params": canon.kv(),
    },
    optional=frozenset({"env_params", "cal_params"}),
)

HELP = Help(
    summary="Calibrate EchoData to volume backscattering strength (Sv).",
    does=(
        "Runs echopype.calibrate.compute_Sv on a converted EchoData file and "
        "writes a flat Sv dataset (Sv, echo_range, sound_absorption, ... on "
        "channel x ping_time x range_sample)."
    ),
    stdin="EchoData .nc/.zarr paths or gs:// URIs, from aa-nc, aa-ed or aa-combine.",
    stdout="The Sv file's absolute path (or gs:// URI); with several inputs, one per "
           "input, as each is made.",
    options=[
        ("-o, --output_path PATH", "Explicit output; '_Sv' is appended to its stem, as "
                                   "always. Local path or gs:// URI."),
        ("--format nc|zarr", ("NetCDF (default) or a Zarr store. A gs:// store is "
                              "written here, then uploaded with its root metadata last.")),
        ("INPUT INPUT ... --dest DIR|gs://PREFIX", ("Several inputs: each file's Sv in "
                                                    "turn, by a fresh aa-sv, into --dest.")),
        ("--progress", "NDJSON progress events on stderr (several inputs)."),
        ("--ecs FILE", "An Echoview calibration supplement (.ecs), local or gs://: "
                       "per-transducer values, matched to channels by frequency "
                       "(aa-ecs writes and shows them)."),
        ("--env-param KEY=VALUE", "Override an environmental value (sound_speed, "
                                  "sound_absorption, temperature, salinity, pressure, "
                                  "pH); KEY@38kHz=VALUE for one channel. Repeatable."),
        ("--cal-param KEY=VALUE", "Override a calibration value (gain_correction, "
                                  "sa_correction, equivalent_beam_angle, ...); "
                                  "KEY@38kHz=VALUE for one channel. Repeatable."),
        ("--waveform_mode CW|BB|FM", "EK80 only. Omit for EK60."),
        ("--encode_mode complex|power", "EK80 only. Omit for EK60."),
    ],
    science={
        "waveform_mode": "EK80 waveform. FM and BB are the same computation.",
        "encode_mode": "EK80 encoding.",
        "env_params": "Environmental overrides (--env-param), as numbers. Hashed only "
                      "when given.",
        "cal_params": "Calibration overrides (--cal-param), as numbers. Hashed only "
                      "when given.",
    },
    files=(
        "Reads EchoData .nc or .zarr, local or gs:// (a gs:// .zarr is read where it is, "
        "not copied). Writes <base>_<hash>.nc (or .zarr) beside the input (current "
        "directory for gs:// input), or -o, or --dest. An identical earlier result is "
        "reused."
    ),
    pipeline=("Second stage: aa-nc | aa-sv | aa-graph, or aa-nc | aa-sv | aa-depth | "
              "... (see aa-guide). Note: aa-clean writes the cleaned values to "
              "Sv_corrected; aa-mvbs, aa-nasc and aa-graph read Sv."),
    examples=[
        "aa-nc D20160703-T060000.raw --sonar_model EK60 | aa-sv",
        "aa-sv file.nc --waveform_mode BB --encode_mode complex   # EK80",
        "aa-sv file.nc --ecs gs://bucket/cal/HB1603.ecs",
        "aa-sv file.nc --cal-param gain_correction@38kHz=26.12 --env-param sound_speed=1490",
        ("aa-sv gs://b/RL2107/EK80/*.zarr --format zarr --dest gs://b/RL2107/EK80/ "
         "--waveform_mode CW --encode_mode power   # each file's Sv"),
    ],
    notes=["EK80 needs both --waveform_mode and --encode_mode; echopype refuses "
           "EK80 data without them.",
           "An ECS file and --env-param/--cal-param cannot be combined: echopype "
           "ignores the overrides when it has an ECS. Put the values in the ECS "
           "(aa-ecs write) instead."],
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
    Usage: aa-sv [OPTIONS] [INPUT_PATH ...]

    Arguments:
    INPUT_PATH                  Path (or gs:// URI) to the .nc / .zarr EchoData
                                file. Optional. Defaults to stdin if not provided.
                                Several: each file's Sv in turn, by a fresh
                                aa-sv, into --dest (required), each printed as
                                it is made. A file whose Sv is already there is
                                reused.

    Options:
    -o, --output_path           Path (or gs:// URI) to save processed output.
                                '_Sv' is appended to its stem and the format's
                                suffix forced. Default: <base>_<hash>.nc beside
                                the input (AA_NAMING=legacy: <stem>_Sv.nc).

    --format nc|zarr            NetCDF (default) or a Zarr store (.zarr). A
                                gs:// target is written here first, then
                                uploaded object by object with its root
                                metadata last, so an unfinished upload is never
                                taken for the product.

    --progress                  NDJSON progress events on stderr (several
                                inputs: one per file).

    --waveform_mode             For EK80 echosounders ONLY: waveform mode.
                                Choices: CW, BB, FM
                                Default: not passed. EK60 needs neither flag;
                                EK80 needs both.

    --encode_mode               For EK80 echosounders ONLY: encoding mode.
                                Choices: complex, power
                                Default: not passed.

    --base NAME                 Base name for the output.
    --dest DIR|gs://PREFIX      Write the default-named output there.
    --force                     Recompute even if an identical product exists.

    Description:
    This tool computes Sv (volume backscattering strength) from a previously-
    converted NetCDF EchoData file using echopype.calibrate.compute_Sv, and
    saves the result to a new .nc file. The output path is printed to stdout
    for piping into the next stage of the pipeline. Provenance (the input's
    chain plus this step) is embedded in the output; see aa-metadata.

    For visualization, pipe the output into aa-graph or aa-plot:
        aa-nc --sonar_model EK60 input.raw | aa-sv | aa-graph

    Example:
        aa-sv /path/to/input.nc --waveform_mode BB --encode_mode complex \\
              -o /path/to/output.nc          # EK80 broadband
        aa-sv /path/to/input.nc --waveform_mode CW --encode_mode power    # EK80 CW
    """
    print(help_text)


def _build_parser():
    parser = argparse.ArgumentParser(
        description="Compute Sv from a NetCDF EchoData file with Echopype.",
        add_help=False,
    )
    parser.add_argument(
        "inputs",
        type=str,
        nargs="*",
        default=[],
        help="Paths or gs:// URIs of .nc / .zarr EchoData files (default: stdin).",
    )
    parser.add_argument(
        "-o", "--output_path",
        type=str,
        help="Path to save processed output. '_Sv' is appended to the stem.",
    )
    parser.add_argument(
        "--ecs",
        type=str,
        default=None,
        metavar="FILE",
        help="Echoview calibration supplement (.ecs), local path or gs:// URI.",
    )
    parser.add_argument(
        "--env-param",
        dest="env_params",
        action="append",
        default=None,
        metavar="KEY=VALUE",
        help="Environmental override (repeatable), e.g. sound_speed=1490; "
             "KEY@38kHz=VALUE for one channel.",
    )
    parser.add_argument(
        "--cal-param",
        dest="cal_params",
        action="append",
        default=None,
        metavar="KEY=VALUE",
        help="Calibration override (repeatable), e.g. gain_correction@38kHz=26.12.",
    )
    parser.add_argument(
        "--waveform_mode",
        type=str,
        default=None,
        choices=["CW", "BB", "FM"],
        help="For EK80 Echosounders ONLY: waveform mode. Omit for EK60.",
    )
    parser.add_argument(
        "--encode_mode",
        type=str,
        default=None,
        choices=["complex", "power"],
        help="For EK80 Echosounders ONLY: encoding mode. Omit for EK60.",
    )
    parser.add_argument(
        "--format",
        dest="fmt",
        choices=["nc", "zarr"],
        default="nc",
        help="Output format: NetCDF (default) or a Zarr store.",
    )
    parser.add_argument(
        "--progress",
        action="store_true",
        help="NDJSON progress events on stderr.",
    )
    add_common_flags(parser)
    return parser


def main():
    # No args on a terminal: help. (An empty pipe is an error: see stdio.)
    if len(sys.argv) == 1 and not stdio.stdin_is_piped():
        print_help()
        sys.exit(0)

    parser = _build_parser()
    if show_help(SPEC, HELP, parser, full=print_help_full):
        sys.exit(0)

    # argparse cannot match a variadic positional across an optional
    # (`a.zarr --dest D b.zarr`): collect the leftovers, as aa-combine does.
    args, leftover = parser.parse_known_args()
    stray = [item for item in leftover if item.startswith("-")]
    if stray:
        logger.error(f"aa-sv: unknown option(s): {' '.join(stray)}")
        sys.exit(2)
    args.inputs = list(args.inputs) + leftover

    # ---------------------------
    # Validate input
    # ---------------------------
    from aalibrary.console import _calibration as calib

    try:
        env_kv = calib.parse_kv(args.env_params, text_keys=calib.TEXT_ENV)
        cal_kv = calib.parse_kv(args.cal_params)
    except ValueError as exc:
        logger.error(f"aa-sv: {exc}")
        sys.exit(2)
    if args.ecs and (env_kv or cal_kv):
        logger.error("aa-sv: --ecs cannot be combined with --env-param/--cal-param "
                     "(echopype ignores the overrides when it has an ECS); put the "
                     "values in the ECS instead (aa-ecs write).")
        sys.exit(2)

    tokens = stdio.many_inputs(args.inputs, SPEC.name)
    if len(tokens) > 1:
        _run_list_mode(tokens, args)
        return
    token = tokens[0]
    ext_out = "." + args.fmt
    run = Run(SPEC, args, params={"env_params": env_kv, "cal_params": cal_kv})
    # A gs:// Zarr store is read where it is (lazily, through a gcsfuse mount
    # or fsspec), never copied here; anything else through the cache.
    src = run.input(token, fetch=not (uris.is_gcs(token) and uris.is_store(token)))
    ecs = run.param_file(args.ecs, role="calibration") if args.ecs else None

    allowed_extensions = {".netcdf4", ".nc", ".zarr"}
    ext = Path(src.name).suffix.lower() if src.local is None else src.local.suffix.lower()
    if ext not in allowed_extensions:
        logger.error(
            f"'{src.name}' is not a supported file type. "
            f"Allowed: {', '.join(sorted(allowed_extensions))}"
        )
        sys.exit(1)

    # ---------------------------
    # Resolve output path
    # ---------------------------
    explicit = (naming.with_stem_suffix(args.output_path, "_Sv", ext_out)
                if args.output_path else None)
    out = run.plan(
        ext=ext_out,
        explicit=explicit,
        legacy=lambda: naming.with_stem_suffix(src.local or src.name, "_Sv", ext_out),
    )

    # Guard against clobbering the input
    if (not out.remote and src.local is not None
            and Path(out.target).resolve() == src.local.resolve()):
        logger.error(f"Refusing to overwrite input file: {src.local.resolve()}")
        sys.exit(1)

    if run.reusable(out):
        run.finish(out)
        return

    # ---------------------------
    # Process file
    # ---------------------------
    try:
        args_summary = {
            "input": token,
            "output": out.target,
            "waveform_mode": args.waveform_mode,
            "encode_mode": args.encode_mode,
            "product": out.hash,
        }
        logger.debug(
            f"Executing aa-sv configured with [OPTIONS]:\n"
            f"{pprint.pformat(args_summary)}"
        )

        process_file(
            input_path=src.read_at,
            output_path=out.local,
            waveform_mode=args.waveform_mode,
            encode_mode=args.encode_mode,
            ecs_file=ecs.local if ecs else None,
            env_kv=env_kv,
            cal_kv=cal_kv,
            fmt=ext_out,
        )

        logger.success(f"Generated {out.target} with aa-sv. Passing it to stdout...")
        run.finish(out)

    except Exception as e:
        logger.exception(f"Error during processing: {e}")
        run.discard(out)
        sys.exit(1)


#: A list stops after this many files in a row have failed: the same cause
#: (credentials, the wrong EK80 modes) would fail the rest.
MAX_FAILURES_IN_A_ROW = 3


def _progress(enabled: bool, event: str, **fields) -> None:
    """One flat NDJSON event on stderr (the console tools' --progress format)."""
    if not enabled:
        return
    import json
    from datetime import UTC, datetime

    payload = {"t": datetime.now(UTC).isoformat(timespec="seconds").replace(
        "+00:00", "Z"), "stage": SPEC.name, "event": event, **fields}
    sys.stderr.write(json.dumps(payload, separators=(",", ":"), default=str) + "\n")
    sys.stderr.flush()


def _run_list_mode(inputs: list, args) -> None:
    """Each input's Sv in turn, each by a fresh aa-sv (as aa-ed does with
    several files): memory is given back between files, one bad file cannot
    leave the next in a strange state, and each goes through exactly the
    single-file path (its reuse check, its provenance, its upload). Each Sv
    goes to --dest under its standard name and is printed as it is made."""
    import signal
    import subprocess

    if args.output_path or args.base:
        logger.error("aa-sv: -o/--base: not with several inputs, which each get their own "
                     "name in --dest.")
        sys.exit(2)
    if not args.dest:
        logger.error("aa-sv: several inputs need --dest: the folder (or gs:// prefix) "
                     "their Sv goes to, each under its own name.")
        sys.exit(2)
    passthrough = ["--dest", args.dest, "--format", args.fmt]
    for flag, value in (("--waveform_mode", args.waveform_mode),
                        ("--encode_mode", args.encode_mode), ("--ecs", args.ecs)):
        if value:
            passthrough += [flag, value]
    for flag, values in (("--env-param", args.env_params), ("--cal-param", args.cal_params)):
        for value in values or []:
            passthrough += [flag, value]
    if args.force:
        passthrough.append("--force")

    total = len(inputs)
    made: list = []
    failed: list = []
    in_a_row = 0
    current: dict = {}

    def _stop(signum, _frame):
        child = current.get("child")
        if child is not None and child.poll() is None:
            child.terminate()
        logger.warning(f"aa-sv: stopped (signal {signum}) after {len(made)} of {total} files.")
        _progress(args.progress, "done", exit=3)
        sys.exit(3)

    for signum in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(signum, _stop)
        except (ValueError, OSError):  # pragma: no cover - not the main thread
            pass

    _progress(args.progress, "start", inputs=total)
    for index, item in enumerate(inputs, start=1):
        _progress(args.progress, "progress", done=index - 1, total=total, unit="files")
        print(f"aa-sv: [{index}/{total}] {item}", file=sys.stderr, flush=True)
        child = subprocess.Popen(
            [sys.executable, "-m", "aalibrary.console.aa_sv", item, *passthrough],
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, text=True)
        current["child"] = child
        out, _ = child.communicate()
        current["child"] = None
        lines = [line.strip() for line in (out or "").splitlines() if line.strip()]
        if child.returncode == 0 and lines:
            made.append(lines[-1])
            stdio.emit(lines[-1])
            sys.stdout.flush()
            in_a_row = 0
            continue
        failed.append(item)
        if child.returncode == -9:
            logger.error(f"aa-sv: [{index}/{total}] {item} was killed by the system "
                         "(signal 9), almost certainly for running out of memory.")
        elif child.returncode < 0:
            logger.error(f"aa-sv: [{index}/{total}] {item} was stopped by signal "
                         f"{-child.returncode}.")
        else:
            logger.error(f"aa-sv: [{index}/{total}] {item} failed (exit {child.returncode}); "
                         "its error is above.")
        in_a_row += 1
        if in_a_row >= MAX_FAILURES_IN_A_ROW and index < total:
            logger.error(f"aa-sv: {in_a_row} files in a row failed: stopping, {total - index} "
                         "not tried. Fix the cause and run again; finished files are reused.")
            break
    _progress(args.progress, "progress", done=total, total=total, unit="files")
    if len(made) < total:
        logger.error(f"aa-sv: {len(made)} of {total} calibrated; {len(failed)} failed"
                     + (f", {total - len(made) - len(failed)} not tried"
                        if len(made) + len(failed) < total else "")
                     + ": " + ", ".join(uris.basename(str(f)) for f in failed[:5])
                     + ("…" if len(failed) > 5 else ""))
        _progress(args.progress, "done", exit=1)
        sys.exit(1)
    print(f"aa-sv: {total} files calibrated into {args.dest}.", file=sys.stderr)
    _progress(args.progress, "done", exit=0)


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
    output_path: Path,
    waveform_mode=None,
    encode_mode=None,
    ecs_file=None,
    env_kv=None,
    cal_kv=None,
    fmt=".nc",
):
    """Load EchoData, compute Sv, and save it as NetCDF or (fmt ".zarr") Zarr."""
    import echopype as ep  # deferred so --help stays fast

    logger.info(f"Loading EchoData from {input_path}")
    # Memory must not grow with the length of the survey (see aa-combine):
    #  * chunks={}: dask arrays in the file's own chunking, so compute_Sv
    #    builds a graph and to_netcdf below streams it. Opened plainly, a
    #    combined survey is read whole and Sv needs several times its size.
    #  * A 1 MB HDF5 chunk cache per variable instead of netCDF-C's 64 MB:
    #    every read is of whole chunks, so a bigger cache only holds memory.
    _small_read_cache()
    if uris.is_remote(str(input_path)):
        # A store read in place: lazily, through fsspec.
        ed = ep.open_converted(str(input_path), chunks={}, storage_options={})
    else:
        ed = ep.open_converted(str(input_path), chunks={})
    align_ping_chunks(ed)

    # Build kwargs lazily — only pass waveform_mode / encode_mode when the
    # user explicitly provided them. echopype's compute_Sv treats these as
    # EK80-only; passing CW/complex unconditionally to an EK60 dataset
    # raises an error. The previous version of this script always passed
    # them, which is why EK60 pipelines silently failed.
    compute_kwargs = {}
    if waveform_mode is not None:
        compute_kwargs["waveform_mode"] = waveform_mode
    if encode_mode is not None:
        compute_kwargs["encode_mode"] = encode_mode

    if compute_kwargs:
        logger.info(f"Computing Sv (EK80 mode: {compute_kwargs})")
    else:
        logger.info("Computing Sv (using echopype defaults for this sonar)")

    if ecs_file is not None:
        logger.info(f"Calibration from ECS {ecs_file}")
        compute_kwargs["ecs_file"] = str(ecs_file)
    if env_kv or cal_kv:
        from aalibrary.console import _calibration as calib

        needs_base = any("@" in k for k in {**(env_kv or {}), **(cal_kv or {})})
        base = (calib.summarize(calib.calibrator(ed, waveform_mode=waveform_mode,
                                                 encode_mode=encode_mode))
                if needs_base else None)
        env = calib.overrides_for_echopype(ed, env_kv, kind="env", base=base)
        cal = calib.overrides_for_echopype(ed, cal_kv, kind="cal", base=base)
        if env:
            compute_kwargs["env_params"] = env
        if cal:
            compute_kwargs["cal_params"] = cal

    ds_Sv = ep.calibrate.compute_Sv(ed, **compute_kwargs)
    ds_Sv = clean_attrs(ds_Sv)

    output_path = Path(output_path).with_suffix(fmt)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Saving Sv dataset to {output_path}")
    if fmt == ".zarr":
        _write_zarr_streaming(ds_Sv, output_path)
    else:
        _write_streaming(ds_Sv, output_path)
    logger.success(f"Sv computation complete: {output_path}")


def align_ping_chunks(ed) -> None:
    """Chunk every per-ping value of a beam group as its backscatter is.

    A file's per-ping values (pulse length, transmit power, sample interval,
    ...) are small and stored in one chunk that spans every ping. compute_Sv
    broadcasts them against range, so that one chunk becomes the size of the
    whole file's backscatter, computed whole before it is split: 4.6 GB for
    one 1 GB EK60 file, for any part of its Sv. Chunked like the backscatter
    along ping_time, each chunk of Sv needs only its own pings (0.8 GB).
    The values are the same either way.
    """
    for group in getattr(ed, "group_paths", ()) or ():
        if not str(group).startswith("Sonar/Beam_group"):
            continue
        try:
            dataset = ed[group]
        except Exception as exc:  # noqa: BLE001 - a group this sonar does not have
            logger.debug(f"No {group}: {exc}")
            continue
        if dataset is None or "ping_time" not in dataset.dims:
            continue
        reference = next((dataset[name] for name in ("backscatter_r", "backscatter_i")
                          if name in dataset and getattr(dataset[name].data, "chunks", None)),
                         None)
        if reference is None:
            continue
        chunks = reference.chunksizes.get("ping_time")
        if not chunks or len(chunks) < 2:
            continue
        try:
            ed[group] = dataset.chunk({"ping_time": chunks})
        except Exception as exc:  # noqa: BLE001 - as it was: correct, only larger
            logger.debug(f"Could not rechunk {group}: {exc}")


def _write_streaming(ds, output_path: Path) -> None:
    """Write *ds* to NetCDF in memory that does not grow with its length.

    * Each HDF5 chunk is one dask chunk, so every write is a whole chunk,
      never read back and rewritten.
    * One chunk is computed at a time: with threads, computed chunks queued
      in memory for the locked writer.
    * The large arrays are written one per pass. Sv is computed from
      echo_range, so written together dask keeps every echo_range chunk it
      made for Sv until echo_range's own turn comes: one whole
      channel x ping x sample array (0.8 GB for 48,000 EK60 pings; 30 GB
      or more for a long survey). Apart, echo_range is computed twice, and
      memory stays flat.
    """
    import dask

    for variable in ds.variables.values():
        chunks = getattr(variable.data, "chunksize", None)
        if chunks and variable.ndim and variable.dtype.kind in "biufcmM":
            variable.encoding["chunksizes"] = tuple(int(c) for c in chunks)
            variable.encoding.pop("contiguous", None)

    large = [
        name for name, variable in ds.data_vars.items()
        if getattr(variable.data, "npartitions", 1) > 1
    ]
    with dask.config.set(scheduler="synchronous"):
        ds.drop_vars(large).to_netcdf(output_path)
        for name in large:
            ds[[name]].to_netcdf(output_path, mode="a")


def _write_zarr_streaming(ds, output_path: Path) -> None:
    """Write *ds* as a Zarr store in memory that does not grow with its length.

    As _write_streaming: one chunk computed at a time, and the large arrays
    one per pass (echo_range would otherwise be kept for Sv's sake). The
    store is consolidated, so it opens with one read wherever it ends up.
    """
    import dask

    from aalibrary.console._core import zarr_io

    zarr_io.clean_encoding(ds)
    # The Sv's own run of pings per chunk (its EchoData's), when it has one:
    # each chunk written is then one chunk computed, split by channel only.
    native = ds["Sv"].chunksizes.get("ping_time") if "Sv" in ds else None
    ping_chunk = (int(native[0]) if native and len(set(native[:-1])) <= 1
                  else zarr_io.ping_chunk_for(ds))
    chunked = {}
    for name, variable in ds.variables.items():
        if variable.ndim and hasattr(variable.data, "chunks"):
            chunked[name] = {dim: (ping_chunk if dim == "ping_time" else
                                   (1 if dim == "channel" else -1)) for dim in variable.dims}
    for name, chunks in chunked.items():
        if name in ds.data_vars:
            ds[name] = ds[name].chunk(chunks)
    encoding = zarr_io.chunked_encoding(ds, ping_chunk)
    large = [
        name for name, variable in ds.data_vars.items()
        if getattr(variable.data, "npartitions", 1) > 1
    ]
    store = str(output_path)
    with dask.config.set(scheduler="synchronous"):
        small = ds.drop_vars(large)
        small.to_zarr(store, mode="w", consolidated=False,
                      encoding={k: v for k, v in encoding.items() if k in small.variables})
        for name in large:
            ds[[name]].to_zarr(store, mode="a", consolidated=False,
                               encoding={name: encoding[name]} if name in encoding else None)
    zarr_io.consolidate_store(store)


def _small_read_cache(size: int = 1 << 20) -> None:
    """Give NetCDF files opened from now on an HDF5 chunk cache of *size*."""
    try:
        import netCDF4
    except ImportError:  # pragma: no cover - h5netcdf-only installs
        return
    _, nelems, preemption = netCDF4.get_chunk_cache()
    netCDF4.set_chunk_cache(size, nelems, preemption)


if __name__ == "__main__":
    main()
