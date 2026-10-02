#!/usr/bin/env python3
"""
aa-show

Console tool that prints a quick summary of a NetCDF file: the xarray
representation of its root group (dimensions, coordinates, data
variables, attributes), exactly as ``print(xr.open_dataset(path))``.

Read-only. Accepts a local path or gs:// URI, as an argument or one line
on stdin. stdout is only the xarray repr; hints go to stderr:
    - for a multi-group EchoData file (whose root group holds no data
      variables) the names of its groups,
    - the full channel names (the repr truncates them), when the file or
      one of its groups has a 'channel' coordinate,
    - for a file written by an aa-* tool, one line naming the product
      (see aa-metadata for the full provenance).
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
import pprint
from pathlib import Path

from aalibrary.console._core import Help, ToolSpec, provenance, render, show_help, stdio, uris

# xarray and netCDF4 are imported inside process_file() so --help stays fast.

SPEC = ToolSpec(name="aa-show", role="inspector", engines=())

HELP = Help(
    summary="Print a NetCDF file's contents summary (the xarray repr).",
    does=(
        "Opens the file with xarray and prints its root group: dimensions, "
        "coordinates, data variables and global attributes. Nothing is computed "
        "and nothing is written."
    ),
    stdin="One .nc/.netcdf4 path or gs:// URI (argument, or one line on stdin).",
    stdout=(
        "The xarray repr of the root group, exactly print(xr.open_dataset(path)). "
        "For a multi-group EchoData file (e.g. from aa-nc) the root holds only "
        "attributes, so the group names are listed on stderr. The full channel "
        "names (the repr cuts them off) go to stderr under 'channels:', repr-quoted "
        "with frequency_nominal: use them as \"channel=<name>\" in aa-detect-shoal "
        "and aa-detect-seafloor. When the file was written by an aa-* tool, one line "
        "'aa: <kind> <hash8> base=<base>' also goes to stderr. A reader that stops "
        "early (| head) is not an error."
    ),
    files=(
        "Reads .nc / .netcdf4, local or gs:// (through a gcsfuse mount when one "
        "covers it, otherwise downloaded once to the cache, AA_CACHE_DIR)."
    ),
    pipeline="End of a pipe, for a human: aa-nc x.raw --sonar_model EK60 | aa-sv | aa-show",
    examples=[
        "aa-show D20160703-T060000_35e8864f.nc",
        "aa-nc x.raw --sonar_model EK60 | aa-sv | aa-show",
        "aa-show gs://bucket/derived/x_35e8864f.nc",
    ],
)


def print_help():
    """Curated help (also used by the docs generator)."""
    sys.stdout.write(render(SPEC, HELP, _build_parser()))


def print_help_full():
    help_text = """
    Usage: aa-show [INPUT_PATH]

    Arguments:
    INPUT_PATH                  Path (or gs:// URI) to a .nc or .netcdf4 file.
                                Optional; defaults to one line on stdin.

    Options:
    -h, --help                  Curated help. --help-all: this text.

    Description:
    Prints the xarray summary of the file's root group (dimensions,
    coordinates, data variables, attributes) to stdout. Read-only: no
    output file is written and nothing is computed.

    A multi-group EchoData file (from aa-nc, aa-ed, aa-combine) keeps its
    data in groups (Environment, Platform, Sonar/Beam_group1, ...); its
    root group has attributes only, so aa-show also lists the groups on
    stderr. The repr truncates long channel names, so every channel
    name found (in the file, or in its groups) is also printed in full on
    stderr under "channels:", repr-quoted, with frequency_nominal; pass
    one as a single argument "channel=<name>" to aa-detect-shoal or
    aa-detect-seafloor. A file written by an aa-* tool gets one stderr
    line naming its product kind, hash and base name; aa-metadata shows
    the rest.

    gs:// URIs are read through a gcsfuse mount when one covers them,
    otherwise downloaded once to the cache (AA_CACHE_DIR). An empty stdin
    (a failed previous stage) is an error (exit 1).

    Example:
    aa-show /path/to/file_Sv.nc
    aa-nc input.raw --sonar_model EK60 | aa-sv | aa-show
    """
    print(help_text)


def _build_parser():
    parser = argparse.ArgumentParser(
        description="Reveals data within nc files.",
        add_help=False,
    )

    # ---------------------------
    # File argument (positional > stdin)
    # ---------------------------
    parser.add_argument(
        "input_path", type=str, nargs="?",
        help="Path or gs:// URI of the .nc or .netcdf4 file.",
    )
    return parser


_ALLOWED = {".netcdf4": "netcdf", ".nc": "netcdf"}


def _unsupported(name: str) -> None:
    logger.error(
        f"'{name}' is not a supported file type. "
        f"Allowed: {', '.join(_ALLOWED.keys())}"
    )
    sys.exit(1)


def main():
    # No args on a terminal: help. (An empty pipe is an error: see stdio.)
    if len(sys.argv) == 1 and not stdio.stdin_is_piped():
        print_help()
        sys.exit(0)

    parser = _build_parser()
    if show_help(SPEC, HELP, parser, full=print_help_full):
        sys.exit(0)

    args = parser.parse_args()

    # ---------------------------
    # Validate input
    # ---------------------------
    token = stdio.one_input(args.input_path, SPEC.name)

    if uris.is_gcs(token):
        # Check the name first: never download a file we would refuse.
        name = uris.basename(token)
        if Path(name).suffix.lower() not in _ALLOWED:
            _unsupported(name)
        try:
            input_path = uris.localize(token).path
        except FileNotFoundError:
            logger.error(f"File '{token}' does not exist.")
            sys.exit(1)
    else:
        input_path = Path(token).expanduser()
        if not input_path.exists():
            logger.error(f"File '{token}' does not exist.")
            sys.exit(1)
        if input_path.suffix.lower() not in _ALLOWED:
            _unsupported(input_path.name)

    # ---------------------------
    # Process file
    # ---------------------------
    try:
        logger.debug(f"\naa-show args:\n{pprint.pformat(vars(args))}")
        process_file(input_path=input_path)

    except Exception as e:
        logger.exception(f"Error during processing: {e}")
        sys.exit(1)


def process_file(input_path: Path):
    """Print the xarray summary of ``input_path``; hints go to stderr."""
    import xarray as xr

    logger.info(f"Loading NetCDF file {input_path} into xarray dataset")
    with xr.open_dataset(input_path) as ds:
        # Print a summary (variables, coordinates, attributes): exactly what
        # print(ds) wrote, but a reader that has gone (| head) is not an error.
        stdio.write(str(ds) + "\n")
        empty_root = len(ds.data_vars) == 0
        channels = _channels_of(ds)

    rows: list = []
    if empty_root or not channels:
        rows, group_channels = _scan_groups(input_path)
        channels = channels or group_channels
    if empty_root:
        _print_groups(rows)
    _print_channels(channels)
    _print_provenance(input_path)


def _number(value, units=None) -> str:
    """38000.0 -> '38000 Hz' (the variable's own units, when it has them)."""
    try:
        x = float(value)
        text = str(int(x)) if x.is_integer() else repr(x)
    except (TypeError, ValueError):
        text = str(value)
    return f"{text} {units}" if units else text


def _channels_of(ds) -> list[tuple[str, str | None]]:
    """(full channel name, nominal frequency) from a flat dataset's coordinate."""
    if "channel" not in ds.coords:
        return []
    names = [str(v) for v in ds["channel"].values.ravel()]
    freqs: list = [None] * len(names)
    fn = ds.variables.get("frequency_nominal")
    if fn is not None and fn.dims == ("channel",):
        freqs = [_number(v, fn.attrs.get("units")) for v in fn.values]
    return list(zip(names, freqs))


def _scan_groups(input_path: Path):
    """Groups of a multi-group file (EchoData) and the channels they hold.

    Returns (rows, channels): rows are (group path, variable count, dims);
    channels are (full name, nominal frequency or None), each name once, in
    the order first seen.
    """
    rows: list = []
    channels: dict[str, str | None] = {}
    try:
        import netCDF4
    except ImportError:
        return rows, []

    def text(v) -> str:
        return v.decode("utf-8", "replace") if isinstance(v, bytes) else str(v)

    def walk(group, prefix=""):
        for name, sub in group.groups.items():
            path = f"{prefix}{name}"
            dims = ", ".join(f"{d}: {len(v)}" for d, v in sub.dimensions.items())
            rows.append((path, len(sub.variables), dims))
            var = sub.variables.get("channel")
            if var is not None and var.dimensions == ("channel",):
                names = [text(v) for v in var[:].tolist()]
                freqs = [None] * len(names)
                fn = sub.variables.get("frequency_nominal")
                if fn is not None and fn.dimensions == ("channel",):
                    units = getattr(fn, "units", None)
                    freqs = [_number(v, units) for v in fn[:].tolist()]
                for n, f in zip(names, freqs):
                    if channels.get(n) is None:
                        channels[n] = f
            walk(sub, path + "/")

    try:
        with netCDF4.Dataset(str(input_path), "r") as nc:
            walk(nc)
    except Exception as e:
        logger.debug(f"could not list groups of {input_path}: {e}")
    return rows, list(channels.items())


def _print_groups(rows) -> None:
    """List the groups of a multi-group file (EchoData) on stderr."""
    if not rows:
        return
    width = max(len(r[0]) for r in rows) + 2
    lines = ["aa-show: the root group has no data variables; the data is in these groups:"]
    for path, nvars, dims in rows:
        lines.append(f"  {path.ljust(width)}{nvars:3d} variables  ({dims})" if dims
                     else f"  {path.ljust(width)}{nvars:3d} variables")
    example = next((r[0] for r in rows if "Beam_group" in r[0]), rows[0][0])
    lines.append(f"  (Python: xr.open_dataset(path, group=\"{example}\"))")
    print("\n".join(lines), file=sys.stderr)


def _print_channels(channels) -> None:
    """Full channel names on stderr: the repr truncates them, and tools such
    as aa-detect-shoal need the exact name ("channel=<name>", one argument)."""
    if not channels:
        return
    lines = ["channels:"]
    width = max(len(repr(n)) for n, _ in channels) + 2
    for name, freq in channels:
        lines.append(f"  {repr(name).ljust(width)}frequency_nominal={freq}" if freq
                     else f"  {repr(name)}")
    print("\n".join(lines), file=sys.stderr)


def _print_provenance(input_path: Path) -> None:
    """One stderr line naming the aa product, when the file has provenance."""
    doc = provenance.read(input_path)
    if not doc:
        return
    prod = doc.get("product", {}) or {}
    short = prod.get("short") or str(prod.get("hash", ""))[:8]
    print(f"aa: {prod.get('kind') or '?'} {short} base={doc.get('base', '')} "
          f"(aa-metadata for details)", file=sys.stderr)


if __name__ == "__main__":
    main()
