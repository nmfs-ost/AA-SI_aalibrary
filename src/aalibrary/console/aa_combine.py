#!/usr/bin/env python3
"""
aa-combine

Console tool to combine multiple EchoData .nc files into a single
combined .nc via echopype.combine_echodata.

This is an N-in / 1-out tool, so the input shape is more flexible than
the rest of the aa-suite. Three ways to feed it (mix freely):

  1. Shell glob in positional args:
        aa-combine *.nc -o combined.nc
     The shell does the expansion; aa-combine sees a list of filenames.

  2. Directory positional arg(s):
        aa-combine ./nc_dir/ -o combined.nc
     aa-combine globs '*.nc' inside (or '**/*.nc' with --recursive).

  3. Stdin, one path per line:
        find /data -name '*.nc' -newer ref.nc | aa-combine -o combined.nc
     The escape hatch when shell globbing isn't expressive enough.

All three accumulate into a single list, get deduplicated (preserving
first occurrence), and are sorted by name by default — which gives
chronological order for the standard echosounder naming convention
(D{YYYYMMDD}-T{HHMMSS}.nc). echopype.combine_echodata REQUIRES
chronological order and will raise if files aren't ordered correctly;
pass --no-sort if you've already ordered the input yourself.

Pipeline contract:
    input  : multiple .nc paths (argv, stdin, directories)
    output : combined .nc on disk; absolute path printed to stdout
    logs   : stderr via loguru

Typical pipeline usage:
    aa-combine *.nc -o all.nc | aa-sv | aa-clean | aa-graph
    find /survey -name '*.nc' | aa-combine -o /survey/combined.nc
"""
from __future__ import annotations

# === Silence logs BEFORE any heavy imports ===
import logging
import sys
import warnings

logging.disable(logging.CRITICAL)
warnings.filterwarnings("ignore")

from loguru import logger
logger.remove()
# Default sink: WARNING+ to stderr so real errors aren't swallowed.
# _configure_logging() below replaces this once --quiet / --debug are parsed.
logger.add(sys.stderr, level="WARNING")

# Now the heavy imports — anything they log gets squashed.
import argparse
import os
import pprint
import signal
from pathlib import Path
from typing import List, Optional


# Pipeline tools should die cleanly when the downstream end of the pipe
# closes early (`... | head -n 1`), not throw BrokenPipeError. Guarded
# with hasattr because SIGPIPE doesn't exist on Windows.
if hasattr(signal, "SIGPIPE"):
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)


# Only .nc is supported in v1. .zarr is plausibly addable later, but
# .zarr is a directory not a file, which makes the "is this input a
# bag of files or a directory to glob" decision ambiguous. Worth its
# own pass if/when the team needs it.
ALLOWED_EXTENSIONS = {".nc"}


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


def _configure_logging(quiet: bool, debug: bool) -> None:
    """Replace the suppression sink with one at the user's chosen level.
    --debug wins over --quiet (mutually-exclusive check happens in main)."""
    logger.remove()
    if debug:
        logger.add(sys.stderr, level="DEBUG", backtrace=True, diagnose=False)
    elif quiet:
        logger.add(sys.stderr, level="WARNING", backtrace=False, diagnose=False)
    else:
        logger.add(sys.stderr, level="INFO", backtrace=True, diagnose=False)


def print_help() -> None:
    help_text = """
    Usage: aa-combine [OPTIONS] [PATHS...]

    Arguments:
      PATHS                       Zero or more paths. Each may be a .nc
                                  file (e.g. from shell-expanded
                                  `*.nc`) or a directory (globbed for
                                  *.nc inside; **/*.nc with --recursive).
                                  Additional paths can be supplied on
                                  stdin, one per line — argv and stdin
                                  are concatenated. If neither is
                                  given, prints help.

    Options:
      -o, --output_path PATH      Path to save the combined .nc.
                                  Default: 'combined.nc' in the common
                                  parent directory of the inputs if
                                  they all share one, else CWD.
                                  Always normalized to a .nc extension.

      --recursive, -r             For directory inputs, glob '**/*.nc'
                                  instead of '*.nc'. Has no effect on
                                  file inputs.

      --no-sort, --no_sort        Preserve input order. Default: sort
                                  by filename (alphabetical), which
                                  produces chronological order for the
                                  standard D{YYYYMMDD}-T{HHMMSS}.nc
                                  naming convention. echopype REQUIRES
                                  chronological order; --no-sort is
                                  for the case where you've already
                                  ordered the input yourself.

      --channels CH1,CH2,...      Restrict the combined output to the
                                  named channels. Maps to
                                  combine_echodata(channel_selection=
                                  [...]). Use the channel names as
                                  they appear in the source files.
                                  Comma-separated. Optional.

      --force, -f                 Overwrite the output .nc if it
                                  already exists. Default: refuse.

      --debug                     Verbose logging (DEBUG level).
      --quiet                     Suppress INFO logs; output path still
                                  prints on stdout.

      -h, --help                  Show this help and exit.

    Description:
      Loads each input file as an EchoData object via
      echopype.open_converted, then calls
      echopype.combine_echodata(echodata_list=[...]) to produce a
      single combined EchoData and saves it as .nc.

      Combine constraints (enforced by echopype, surfaced here on
      failure with a useful hint):
        - Minimum 2 input files.
        - All files must share the same sonar_model.
        - Files must be in chronological order (see --no-sort).
        - File paths must be unique (we deduplicate, so this is
          automatic for accidental duplicates).
        - Same channel set across files (or use --channels to subset).

      The combined .nc absolute path is printed on stdout for piping
      into aa-sv / aa-clean / aa-graph.

    Examples:
      # Shell glob — most natural for "everything matching":
      aa-combine *.nc -o combined.nc

      # Directory globbing:
      aa-combine ./HB1603/EK60/ -o ./HB1603/combined.nc

      # Recursive directory:
      aa-combine ./survey_root/ -r -o ./survey_root/all.nc

      # Stdin from find for non-trivial filtering:
      find /data/HB1603 -name '*.nc' -newer ref.nc \\
        | aa-combine -o /data/HB1603/recent.nc

      # Mix argv and stdin:
      echo extra.nc | aa-combine *.nc -o combined.nc

      # Channel subset:
      aa-combine *.nc --channels "GPT 38 kHz,GPT 120 kHz" -o combined.nc

      # Pipeline:
      aa-combine ./nc_files/ -o combined.nc | aa-sv | aa-graph
    """
    print(help_text)


def main() -> None:
    # Help short-circuit (must come before argparse because argparse
    # will choke on unrecognized -h positions or empty input).
    if "--help" in sys.argv or "-h" in sys.argv:
        print_help()
        sys.exit(0)

    # If no positional args AND nothing on stdin, print help. Don't
    # block forever waiting for stdin from an interactive terminal.
    if len(sys.argv) == 1 and sys.stdin.isatty():
        print_help()
        sys.exit(0)

    parser = argparse.ArgumentParser(
        description="Combine multiple .nc EchoData files via echopype.combine_echodata.",
        add_help=False,
    )

    parser.add_argument(
        "paths",
        nargs="*",
        type=str,
        help="Input .nc files or directories.",
    )
    parser.add_argument(
        "-o", "--output_path",
        type=Path,
        default=None,
        help="Path to save the combined .nc.",
    )
    parser.add_argument(
        "--recursive", "-r",
        action="store_true",
        default=False,
        help="Recursively glob directories for *.nc.",
    )
    parser.add_argument(
        "--no-sort", "--no_sort",
        dest="no_sort",
        action="store_true",
        default=False,
        help="Preserve input order. Default: alphabetical sort.",
    )
    parser.add_argument(
        "--channels",
        type=str,
        default=None,
        help="Comma-separated list of channel names for "
             "channel_selection. Optional.",
    )
    parser.add_argument(
        "--force", "-f",
        action="store_true",
        default=False,
        help="Overwrite existing output .nc.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        default=False,
        help="Enable verbose DEBUG-level logging.",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        default=False,
        help="Suppress INFO logs.",
    )

    args = parser.parse_args()

    if args.debug and args.quiet:
        logger.error("Use --debug OR --quiet, not both.")
        sys.exit(2)

    _configure_logging(args.quiet, args.debug)

    # ---------------------------
    # Collect input paths
    # ---------------------------
    # Argv + stdin both feed the same list. Directories get expanded
    # to their .nc contents. We don't reorder argv-vs-stdin: argv
    # comes first, then stdin lines. After dedup + (optional) sort,
    # we end up with a final ordered list.
    try:
        input_paths = _collect_input_paths(
            positional=args.paths,
            recursive=args.recursive,
        )
    except SystemExit:
        raise
    except Exception as e:
        logger.exception(f"Failed to collect input paths: {e}")
        sys.exit(1)

    if not args.no_sort:
        # Sort by the basename, not the full path: if files come from
        # different directories but share a temporal naming scheme,
        # we want them ordered by time, not by which folder they live
        # in. Stable sort preserves dedup order for ties.
        input_paths.sort(key=lambda p: p.name)
        logger.debug("Sorted inputs by filename (alphabetical).")
    else:
        logger.debug("Preserving input order (--no-sort).")

    if len(input_paths) < 2:
        # combine_echodata isn't meaningful for one file (no-op) or
        # zero (typo). Surface clearly rather than letting echopype
        # raise an opaque error.
        logger.error(
            f"Need at least 2 .nc files to combine; got {len(input_paths)}. "
            "Check your glob, directory contents, or stdin."
        )
        sys.exit(1)

    # ---------------------------
    # Resolve output path
    # ---------------------------
    if args.output_path is None:
        # If every input shares a parent directory, put the combined
        # file there alongside its components. Otherwise CWD.
        parents = {p.parent for p in input_paths}
        if len(parents) == 1:
            out_dir = parents.pop()
        else:
            out_dir = Path.cwd().resolve()
            logger.info(
                f"Inputs span {len(parents)} directories; defaulting "
                f"output to CWD ({out_dir}). Pass -o to override."
            )
        output_path = (out_dir / "combined.nc").resolve()
    else:
        output_path = args.output_path.expanduser().resolve().with_suffix(".nc")
        output_path.parent.mkdir(parents=True, exist_ok=True)

    # Refuse to clobber any input file. echopype doesn't check this,
    # and silently overwriting a source .nc mid-combine would be a
    # data-loss bug. Compare resolved paths to catch symlinks / aliases.
    resolved_inputs = {p.resolve() for p in input_paths}
    if output_path.resolve() in resolved_inputs:
        logger.error(
            f"Refusing to overwrite an input file: {output_path}. "
            "Pick a different -o."
        )
        sys.exit(1)

    if output_path.exists() and not args.force:
        logger.error(
            f"Output already exists: {output_path}. "
            "Pass --force to overwrite, or pick a different -o."
        )
        sys.exit(1)

    # ---------------------------
    # Parse channel_selection
    # ---------------------------
    channel_selection: Optional[List[str]] = None
    if args.channels:
        # echopype accepts either a flat list (applied across all
        # channel-dim'd groups) or a {beam_group: [channels]} dict.
        # CLI restricts us to the flat-list form — the dict form
        # would need a more elaborate syntax than comma-split.
        channel_selection = [c.strip() for c in args.channels.split(",") if c.strip()]
        logger.info(f"Restricting to channels: {channel_selection}")

    # ---------------------------
    # Summary log
    # ---------------------------
    args_summary = {
        "input_count": len(input_paths),
        "first_input": str(input_paths[0]),
        "last_input": str(input_paths[-1]),
        "output_path": str(output_path),
        "sorted": not args.no_sort,
        "recursive": args.recursive,
        "channels": channel_selection,
        "force": args.force,
    }
    logger.debug(
        f"Executing aa-combine configured with [OPTIONS]:\n"
        f"{pprint.pformat(args_summary)}"
    )
    logger.info(
        f"Combining {len(input_paths)} .nc files into {output_path}"
    )
    if args.debug:
        # In debug mode, dump the full ordered file list so the user
        # can see exactly what order combine_echodata will see them.
        for i, p in enumerate(input_paths):
            logger.debug(f"  [{i}] {p}")

    # ---------------------------
    # Run the combine
    # ---------------------------
    try:
        _run_combine(
            input_paths=input_paths,
            output_path=output_path,
            channel_selection=channel_selection,
        )
    except SystemExit:
        raise
    except Exception as e:
        logger.exception(f"Combine failed: {e}")
        sys.exit(1)

    if not output_path.exists():
        # Defensive: to_netcdf shouldn't return silently on failure,
        # but if it does, surface it now instead of printing a missing
        # path to stdout and breaking aa-sv downstream.
        logger.error(
            f"Combine appeared to succeed, but '{output_path}' is "
            "not on disk. Rerun with --debug for details."
        )
        sys.exit(1)

    logger.success(
        f"Combined .nc written to {output_path} "
        f"({output_path.stat().st_size:,} bytes). "
        "Passing combined .nc path to stdout..."
    )

    # Pipeline contract: combined .nc absolute path on stdout.
    print(output_path)


def _collect_input_paths(
    positional: List[str],
    recursive: bool,
) -> List[Path]:
    """Resolve positional args + stdin lines into a deduplicated list
    of existing .nc Path objects.

    - Argv strings come first; stdin (if non-tty) is appended after.
    - Strings that resolve to directories are globbed for *.nc
      (or **/*.nc with --recursive).
    - Strings that resolve to files are validated as .nc.
    - Missing paths or unsupported extensions are fatal.
    - Order is preserved through dedup (first occurrence wins) so the
      user's --no-sort intent works downstream.
    """
    # Accumulate raw strings: argv first, stdin lines second.
    raw: List[str] = list(positional)
    if not sys.stdin.isatty():
        for line in sys.stdin:
            s = line.strip()
            if s:
                raw.append(s)

    if not raw:
        logger.error(
            "No input paths supplied. Pass .nc files / directories as "
            "arguments, or pipe paths in on stdin."
        )
        sys.exit(1)

    # Walk each input, expanding directories and validating files.
    collected: List[Path] = []
    for s in raw:
        p = Path(s).expanduser()
        if not p.exists():
            logger.error(f"Input path does not exist: {p}")
            sys.exit(1)

        if p.is_dir():
            pattern = "**/*.nc" if recursive else "*.nc"
            found = sorted(p.glob(pattern))
            if not found:
                logger.warning(
                    f"No .nc files found in {p} "
                    f"(pattern: '{pattern}')."
                )
                continue
            logger.debug(
                f"Directory {p} expanded to {len(found)} files "
                f"(pattern: '{pattern}')."
            )
            collected.extend(f.resolve() for f in found)

        elif p.is_file():
            ext = p.suffix.lower()
            if ext not in ALLOWED_EXTENSIONS:
                logger.error(
                    f"Unsupported extension '{ext}' for input '{p}'. "
                    f"aa-combine only handles {sorted(ALLOWED_EXTENSIONS)}."
                )
                sys.exit(1)
            collected.append(p.resolve())

        else:
            # Sockets, FIFOs, broken-symlink-following-other-paths,
            # etc. Skip with a clear error rather than silently.
            logger.error(
                f"'{p}' is neither a regular file nor a directory; "
                "cannot use as input."
            )
            sys.exit(1)

    # Dedup, preserving first-occurrence order. echopype requires
    # unique filenames; this also catches the case where the user
    # globbed and also piped the same path in.
    seen: set = set()
    deduped: List[Path] = []
    duplicates: List[Path] = []
    for p in collected:
        if p in seen:
            duplicates.append(p)
            continue
        seen.add(p)
        deduped.append(p)

    if duplicates:
        logger.warning(
            f"Removed {len(duplicates)} duplicate input(s): "
            f"{[str(d) for d in duplicates[:5]]}"
            f"{'...' if len(duplicates) > 5 else ''}"
        )

    return deduped


def _run_combine(
    input_paths: List[Path],
    output_path: Path,
    channel_selection: Optional[List[str]],
) -> None:
    """Load each input with ep.open_converted, hand the list to
    ep.combine_echodata, save the combined result as .nc.

    combine_echodata returns a lazy EchoData; to_netcdf materializes
    on write, so memory pressure is bounded by xarray/dask's chunking
    rather than the cumulative size of the inputs.

    Errors from echopype are surfaced as-is with a hint pointing back
    at the most common cause (chronological-order requirement), since
    that's the failure mode aa-combine users hit most often.
    """
    # Heavy import deferred so --help is fast and arg-parse / path
    # validation errors fail before paying echopype's import cost.
    try:
        import echopype as ep
    except Exception as e:
        logger.exception(f"Failed to import echopype: {e}")
        sys.exit(1)

    # Open each .nc as EchoData. Lazy by default — xarray + dask.
    ed_list = []
    for i, p in enumerate(input_paths):
        try:
            ed = ep.open_converted(str(p))
        except Exception as e:
            logger.error(
                f"Failed to open '{p}' as EchoData (input {i+1}/"
                f"{len(input_paths)}): {e}"
            )
            sys.exit(1)
        ed_list.append(ed)
    logger.info(f"Opened {len(ed_list)} EchoData objects.")

    # Combine.
    try:
        combined = ep.combine_echodata(
            echodata_list=ed_list,
            channel_selection=channel_selection,
        )
    except RuntimeError as e:
        # echopype raises RuntimeError for chronological-order
        # violations and channel/dimension mismatches. The order
        # issue is by far the most common — surface it with a hint.
        msg = str(e).lower()
        if "time" in msg or "first time" in msg:
            logger.error(
                f"echopype rejected the input order: {e}\n"
                "Hint: combine_echodata requires files in chronological "
                "order. Default sort (--no-sort off) sorts by filename, "
                "which gives chronological order for D{YYYYMMDD}-"
                "T{HHMMSS}.nc files. If your filenames don't follow "
                "that convention, pre-sort the input and pass --no-sort."
            )
        else:
            logger.error(f"echopype combine_echodata failed: {e}")
        sys.exit(1)
    except ValueError as e:
        # Bad inputs: missing sonar_model, missing file paths,
        # duplicate filenames, etc.
        logger.error(f"echopype rejected the input list: {e}")
        sys.exit(1)

    # Save. If --force and the output already existed, unlink first —
    # ed.to_netcdf appends-or-fails on some backends if the file's
    # there, depending on the engine.
    if output_path.exists():
        try:
            output_path.unlink()
            logger.debug(f"Removed pre-existing {output_path} (--force).")
        except Exception as e:
            logger.error(f"Could not remove existing output: {e}")
            sys.exit(1)

    logger.info(f"Saving combined EchoData to {output_path} ...")
    combined.to_netcdf(save_path=output_path)


if __name__ == "__main__":
    main()