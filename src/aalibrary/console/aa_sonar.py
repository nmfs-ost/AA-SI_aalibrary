#!/usr/bin/env python3
"""
aa-sonar

Console tool for detecting the sonar model of a raw echosounder file.

Given a path to a raw file (.raw, .azfp, .ad2cp, or an AZFP sidecar
.xml), prints the detected sonar model identifier to stdout. The output
is normalized to the value expected by echopype's ``sonar_model``
parameter, so it can be fed straight into ``aa-nc --sonar_model``.

Pipeline-friendly: reads input path from positional arg or stdin,
writes the model identifier to stdout, all logs to stderr.

Typical usage:
    aa-sonar input.raw
    echo input.raw | aa-sonar
    aa-nc --sonar_model "$(aa-sonar input.raw)" input.raw
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

# Now the heavy imports — anything they log gets squashed
import argparse
import pprint
import signal
from pathlib import Path

from aalibrary.utils.sonar_checker.sonar_checker import (
    is_AD2CP,
    is_AZFP,
    is_AZFP6,
    is_EK60,
    is_EK80,
    is_ER60,
)


# Pipeline tools should die cleanly when the downstream end of the pipe
# closes early (`... | head -n 1`), not throw BrokenPipeError. Guarded
# with hasattr because SIGPIPE doesn't exist on Windows.
if hasattr(signal, "SIGPIPE"):
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)


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
    help_text = """
    Usage: aa-sonar [OPTIONS] [INPUT_PATH]

    Arguments:
    INPUT_PATH                  Path to a raw echosounder file.
                                Supported extensions: .raw, .azfp, .ad2cp,
                                .xml (AZFP sidecar). Optional; falls back
                                to stdin if not provided.

    Options:
    --strict                    Exit with non-zero status if the sonar
                                model cannot be determined. Default
                                behavior is to print 'UNKNOWN' and exit 0.

    --raw-name                  Emit the literal detection result instead
                                of the normalized echopype identifier.
                                Distinguishes ER60 from EK60 and AZFP6
                                from AZFP. Off by default.

    Description:
    Detects the sonar model of a raw echosounder file. For .ad2cp and
    .azfp files the check is extension-based; for AZFP XML sidecars the
    InstrumentType element is inspected; for Simrad .raw files the
    config datagram header is read via aalibrary's sonar_checker.

    By default the output is normalized to a value accepted by echopype's
    `sonar_model` parameter, so it can be piped directly into aa-nc:

        aa-nc --sonar_model "$(aa-sonar input.raw)" input.raw

    Normalization:
        ER60        -> EK60
        AZFP6       -> AZFP
        (others pass through unchanged)

    Pass --raw-name to disable normalization.

    The input file is never modified. All logs go to stderr; only the
    model identifier goes to stdout.

    Examples:
    aa-sonar /path/to/input.raw
    echo /path/to/input.raw | aa-sonar
    aa-sonar --raw-name /path/to/input.raw
    """
    print(help_text)


# Echopype sonar_model normalization. Maps detector outputs to the
# identifier echopype accepts in `sonar_model=...`. ER60 and EK60 share
# the same echopype path; AZFP and AZFP6 both go through AZFP.
_NORMALIZE = {
    "ER60": "EK60",
    "AZFP6": "AZFP",
}


def main():
    # Stdin / no-args handling — mirrors aa-mvbs so an empty-stdin
    # invocation prints help instead of hanging on readline.
    if len(sys.argv) == 1:
        if not sys.stdin.isatty():
            stdin_data = sys.stdin.readline().strip()
            if stdin_data:
                sys.argv.append(stdin_data)
            else:
                print_help()
                sys.exit(0)
        else:
            print_help()
            sys.exit(0)

    if "--help" in sys.argv or "-h" in sys.argv:
        print_help()
        sys.exit(0)

    parser = argparse.ArgumentParser(
        description="Detect the sonar model of a raw echosounder file.",
        add_help=False,
    )

    parser.add_argument(
        "input_path",
        type=Path,
        nargs="?",
        help="Path to the raw file.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        default=False,
        help="Exit non-zero if the sonar model cannot be determined.",
    )
    parser.add_argument(
        "--raw-name", "--raw_name",
        dest="raw_name",
        action="store_true",
        default=False,
        help="Emit the literal detector result without echopype normalization.",
    )

    args = parser.parse_args()

    # ---------------------------
    # Validate input
    # ---------------------------
    if args.input_path is None:
        if sys.stdin.isatty():
            logger.error("No input path provided and no stdin available.")
            sys.exit(1)
        args.input_path = Path(sys.stdin.readline().strip())
        logger.info(f"Read input path from stdin: {args.input_path}")

    if not args.input_path.exists():
        logger.error(f"File '{args.input_path}' does not exist.")
        sys.exit(1)

    allowed_extensions = {".raw", ".azfp", ".ad2cp", ".xml"}
    ext = args.input_path.suffix.lower()
    if ext not in allowed_extensions:
        logger.error(
            f"'{args.input_path.name}' is not a supported file type. "
            f"Allowed: {', '.join(sorted(allowed_extensions))}"
        )
        sys.exit(1)

    # ---------------------------
    # Detect sonar model
    # ---------------------------
    try:
        args_summary = {
            "input_path": args.input_path,
            "strict": args.strict,
            "raw_name": args.raw_name,
        }
        logger.debug(
            f"Executing aa-sonar configured with [OPTIONS]:\n"
            f"{pprint.pformat(args_summary)}"
        )

        raw_model = detect_sonar_model(args.input_path)
        model = raw_model if args.raw_name else _NORMALIZE.get(raw_model, raw_model)

        if model == "UNKNOWN":
            logger.warning(
                f"Could not determine sonar model for {args.input_path.resolve()}"
            )
            if args.strict:
                sys.exit(1)
        else:
            logger.success(
                f"Detected sonar model '{model}' for {args.input_path.resolve()}. "
                "Passing model identifier to stdout..."
            )

        # Pipe the model identifier to stdout for the next tool
        print(model)

    except Exception as e:
        logger.exception(f"Error during processing: {e}")
        sys.exit(1)


def detect_sonar_model(input_path: Path) -> str:
    """Identify the sonar model of a raw file.

    Strategy (cheapest checks first):

    1. ``.ad2cp`` extension -> AD2CP.
    2. ``.azfp`` extension  -> AZFP6.
    3. ``.xml`` sidecar with ``InstrumentType string="AZFP"`` -> AZFP.
    4. ``.raw`` files are inspected via sonar_checker, which reads the
       leading config datagram:
         - EK80 if the datagram contains a ``configuration`` block.
         - EK60 if ``sounder_name`` is ``EK60`` or ``ER60``.
         - ER60 falls under the same check and is reported separately
           only when --raw-name is requested upstream.

    Returns one of: ``EK80``, ``EK60``, ``ER60``, ``AZFP``, ``AZFP6``,
    ``AD2CP``, or ``UNKNOWN`` if no check matches.

    The input file is opened read-only and not modified.
    """
    path_str = str(input_path)
    storage_options: dict = {}  # local file: no fsspec credentials needed
    ext = input_path.suffix.lower()

    # ---- Extension-only fast paths --------------------------------
    # is_AD2CP / is_AZFP6 are pure string checks, so calling them is
    # equivalent to inspecting the suffix. We do both for symmetry with
    # sonar_checker's public surface, in case its logic evolves.
    if ext == ".ad2cp" or is_AD2CP(path_str):
        return "AD2CP"

    if ext == ".azfp" or is_AZFP6(path_str):
        return "AZFP6"

    # ---- AZFP sidecar XML -----------------------------------------
    if ext == ".xml":
        if is_AZFP(path_str):
            return "AZFP"
        return "UNKNOWN"

    # ---- Simrad .raw header inspection ----------------------------
    if ext == ".raw":
        # EK80 first: distinguished by a 'configuration' block in the
        # config datagram. EK60/ER60 expose 'sounder_name' instead, so
        # the two checks don't overlap on real files.
        try:
            if is_EK80(path_str, storage_options):
                return "EK80"
        except Exception as e:
            logger.debug(f"EK80 check raised on {input_path}: {e}")

        # is_EK60 and is_ER60 currently match the same sounder_name set
        # ({'ER60','EK60'}). We try EK60 first (more common) and only
        # fall back to ER60 if EK60 raised — keeps behavior deterministic
        # even if the underlying checks diverge later.
        try:
            if is_EK60(path_str, storage_options):
                return "EK60"
        except Exception as e:
            logger.debug(f"EK60 check raised on {input_path}: {e}")

        try:
            if is_ER60(path_str, storage_options):
                return "ER60"
        except Exception as e:
            logger.debug(f"ER60 check raised on {input_path}: {e}")

    return "UNKNOWN"


if __name__ == "__main__":
    main()