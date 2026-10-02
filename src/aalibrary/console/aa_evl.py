#!/usr/bin/env python3
"""
aa-evl

Mask echogram NetCDF (.nc/.netcdf4) using Echoview line files (.evl) via echoregions Lines2D.

AA-style pipeline behavior:
- Reads input NetCDF paths (or gs:// URIs) from stdin (newline-delimited) when piped
  OR accepts positional inputs.
- Produces a NEW NetCDF output per input.
- Emits output path(s) to stdout (one per line, in input order) for downstream piping.
- Logs go to stderr.

Core behavior:
- --evl accepts one or more .evl paths or gs:// URIs (argparse nargs="+").
- Loads all EVLs and builds a per-ping depth threshold for each, then unions them
  into a single composite line (the shallowest or deepest, depending on --keep).
- Applies the line mask to all variables containing (time_dim, depth_dim): the
  side that is masked becomes NaN.

Provenance and naming (shared console core):
- Each output is a scientific product named <base>_<hash8>.nc. The hash covers
  the input's product hash, the CONTENT of the EVL files (not their paths),
  --keep, --depth-offset, the resolved variable / dimension names, the channel
  index (when a channel dimension is involved) and --write-line. For
  --keep above/below the order of the EVL files and duplicates don't matter
  (per-ping min/max); for --keep between the order is kept (see below).
- -o PATH and an explicit --suffix TEXT keep the old explicit names;
  AA_NAMING=legacy restores the old default <stem>_evl.nc.
- An identical earlier product is reused; a different existing file is only
  replaced with --overwrite.

EVL semantics:
  An EVL file is a time-series of (datetime, depth_metres) points defining a
  boundary line across the echogram (e.g. seafloor, surface, bottom exclusion
  zone).  Unlike EVR (closed polygons), an EVL is an open line; there is no
  "inside" - only above vs. below.  Depth is positive downward.

  --keep above   Keep data ABOVE the union line (default); mask everything below.
                 Typical use: mask out seafloor / bottom noise.
  --keep below   Keep data BELOW the union line; mask everything above.
                 Typical use: mask out near-surface noise, keep deep water data.
  --keep between Requires exactly two EVL files; keeps data between the two lines.

  --depth-offset METRES
                 Shift the line up (negative) or down (positive) before masking.
                 Useful for adding a safety buffer above the seafloor, e.g. -5.0
                 to keep data more than 5 m above the detected bottom.

Design notes:
- Union line for "above" mode:  per-ping MINIMUM depth across all EVLs
  (the shallowest of all lines; anything above all of them is safe).
- Union line for "below" mode:  per-ping MAXIMUM depth across all EVLs
  (the deepest of all lines).
- "Between" mode requires exactly 2 EVLs: upper_line <= depth <= lower_line.
  The files are given upper then lower; if the first line's median depth is
  deeper than the second's, the two are swapped. When the medians are equal the
  given order decides, so for "between" the file order is part of the hash.
- Lines are interpolated to every ping_time in the echogram using linear
  interpolation; pings outside the EVL time range use the nearest boundary
  value (forward/back fill).  Both time axes are converted to nanoseconds first
  (under pandas 3 the EVL times parse as datetime64[us] while ping_time is
  datetime64[ns]; mixing the two used to clamp every ping to the last EVL point).
- Line points with sentinel depths (|depth| >= 9000, e.g. Echoview's -10000.99
  "no data") are dropped, so the line is interpolated across them from the
  neighbouring valid points; the remaining depths are clipped to the echogram
  depth range.
- Coordinate-label mismatch is avoided by positional masking (same fix as aa-evr).
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
# _configure_logging() below replaces this once --debug is parsed.
logger.add(sys.stderr, level="WARNING")

# Now the heavy imports - anything they log gets squashed
import argparse
import pprint
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd
import xarray as xr

import echoregions as er

from aalibrary.console._core import (
    Help, Run, ToolSpec, add_common_flags, canon, naming, render, show_help, stdio, uris,
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


# ---------------------------------------------------------------------------
# Workaround: echoregions.lines.lines_parser.parse_evl crashes on pandas 2.x
# when string[pyarrow] is the default string dtype.
#
# Upstream code does:
#     df = pd.DataFrame([{"time": "20090916 1247393820", ...}])  # str column
#     df.loc[:, "time"] = df.loc[:, "time"].apply(parse_time)    # <-- DatetimeArray
#
# The .apply returns datetime objects but the column is dtyped `string[pyarrow]`,
# which refuses datetime values:
#     TypeError: Invalid value '<DatetimeArray>...' for dtype 'str'
#
# Old pandas silently upgraded the column dtype; new pandas does not.
# We patch parse_evl to convert the string column to datetime BEFORE
# writing it back, sidestepping the dtype clash entirely.
#
# Re-applies upstream's own logic; only the assignment mechanic changes.
# ---------------------------------------------------------------------------
def _patch_echoregions_parse_evl() -> None:
    try:
        import os as _os
        from echoregions.lines import lines_parser as _lp
        from echoregions.utils.io import check_file as _check_file
        from echoregions.utils.time import parse_time as _parse_time
    except Exception as _exc:
        logger.debug(f"Could not patch echoregions.parse_evl: {_exc}")
        return

    def parse_evl(input_file: str):
        _check_file(input_file, "EVL")
        with open(input_file, encoding="utf-8-sig") as fid:
            file_lines = fid.readlines()
        file_type, file_format_number, ev_version = file_lines[0].strip().split()
        file_metadata = {
            "file_name": (
                _os.path.splitext(_os.path.basename(input_file))[0]
                + _os.path.splitext(_os.path.basename(input_file))[1]
            ),
            "file_type": file_type,
            "evl_file_format_version": file_format_number,
            "echoview_version": ev_version,
        }
        n_points = int(file_lines[1].strip())
        if (len(file_lines) - 2) != n_points:
            raise ValueError(
                "There exists a mismatch between the expected number of lines "
                f"in the file and the actual number of points. There should be "
                f"2 less lines in the file than the number of points, however "
                f"we have {len(file_lines)} number of lines in the file and "
                f"{n_points} number of points."
            )
        points = []
        for i in range(n_points):
            date, time, depth, status = file_lines[i + 2].strip().split()
            points.append(
                {
                    # Pre-parse to datetime so the column is born as the right
                    # dtype — no string-to-datetime reassignment needed.
                    "time": _parse_time(f"{date} {time}"),
                    "depth": float(depth),
                    "status": status,
                }
            )
        df = pd.DataFrame(points)
        df = df.assign(**file_metadata)
        order = list(file_metadata.keys()) + ["time", "depth", "status"]
        return df[order]

    _lp.parse_evl = parse_evl
    # The Lines class imports parse_evl by name into its own module
    # namespace, so we have to rebind the reference there too — patching
    # only lines_parser leaves Lines.__init__ calling the unpatched copy.
    try:
        from echoregions.lines import lines as _lines_mod
        _lines_mod.parse_evl = parse_evl
    except Exception as _exc:
        logger.debug(f"Could not rebind parse_evl in echoregions.lines.lines: {_exc}")
    logger.debug("Applied echoregions.parse_evl PyArrow-string-dtype patch.")


_patch_echoregions_parse_evl()


# ---------------------------
# Tool identity
# ---------------------------

SPEC = ToolSpec(
    name="aa-evl",
    role="transform",
    kind="sv",   # the default; an output keeps its input's kind (masked MVBS is still mvbs)
    op="aa_evl.line_mask",
    op_version=1,
    engines=("echoregions",),
    # The EVL files are scientific too: they are registered as inputs
    # (role "regions") so their CONTENT enters the hash. var / time_dim /
    # depth_dim / channel_index are replaced by the values actually used
    # (see _resolve_masking) before the hash is computed.
    params={
        "keep": canon.choice(),
        "depth_offset": canon.number,
        "var": canon.text,
        "time_dim": canon.text,
        "depth_dim": canon.text,
        "channel_index": canon.integer,
        "write_line": canon.boolean,
    },
)

DEFAULT_SUFFIX = "_evl"   # the old default name, <stem>_evl.nc (AA_NAMING=legacy)

HELP = Help(
    summary="Mask an echogram above, below or between Echoview lines (.evl).",
    does=(
        "Reads one or more Echoview line files (time, depth points), interpolates "
        "each line linearly to every ping (pings before the first / after the "
        "last point take that point's depth), and sets every cell of every "
        "(time, depth) variable on the unwanted side of the line to NaN. Axis "
        "variables (echo_range, depth, ...) are left intact.\n\n"
        "--keep above (default): keep cells at or above the line (depth <= line); "
        "with several lines, the per-ping shallowest one. Typical: remove the "
        "seafloor and everything below it.\n\n"
        "--keep below: keep cells at or below the line (depth >= line); with "
        "several lines, the per-ping deepest one. Typical: remove near-surface "
        "noise.\n\n"
        "--keep between: exactly two lines, given upper then lower; keep "
        "upper <= depth <= lower. If the first line's median depth is deeper, "
        "the two are swapped.\n\n"
        "Depth is positive downward. --depth-offset METRES is added to the line "
        "(to both lines for between) before masking: negative moves the line up "
        "(shallower), positive moves it down (deeper). So --keep above "
        "--depth-offset -5 on a seafloor line keeps only data more than 5 m above "
        "the bottom. Line depths are clipped to the echogram's depth range. Line "
        "points with sentinel depths (|depth| >= 9000, e.g. -10000.99) are dropped "
        "and the line is interpolated across them."
    ),
    stdin=(
        "Flat NetCDF paths (.nc/.netcdf4) or gs:// URIs, one per line or as "
        "arguments: Sv, cleaned Sv, MVBS, ... with a (ping_time|time) x "
        "(depth|range_sample|range_bin|echo_range) variable (--var, default Sv)."
    ),
    stdout=(
        "One line per input, in input order: the output's absolute path (or gs:// "
        "URI). An input that fails prints nothing and the exit status is 1 at "
        "the end."
    ),
    metadata=(
        "Reads the input's provenance, appends this step with its canonical "
        "scientific options and embeds it (NetCDF attributes aa_provenance, "
        "aa_product_hash, aa_base, aa_tool, history). The EVL files are recorded "
        "as inputs with role 'regions' and identified by content; the attributes "
        "aa_evl_files, aa_evl_keep and aa_evl_depth_offset are kept. The base "
        "name is carried through. Inspect with: aa-metadata FILE"
    ),
    options=[
        ("--evl EVL [EVL ...]", "REQUIRED. Line files, local or gs://."),
        ("--keep above|below|between", "which side of the line(s) to keep "
                                       "(default above)"),
        ("--depth-offset METRES", "shift the line(s) before masking; negative = up "
                                  "(shallower), positive = down (default 0)"),
        ("-o, --output-path PATH", "exact output path (one input only); local or gs://"),
        ("--out-dir DIR", "write the outputs here instead of beside each input"),
        ("--suffix TEXT", "use the old naming <input stem><TEXT>.nc instead of "
                          "<base>_<hash8>.nc"),
        ("--overwrite", "replace an existing output that is a different product"),
        ("--var NAME", "variable whose dimensions define the mask (default Sv)"),
        ("--time-dim / --depth-dim NAME", "dimension names (default: ping_time|time; "
                                          "depth|range_sample|range_bin|echo_range)"),
        ("--channel-index N", "channel whose echo_range/depth gives the depth axis "
                              "(default 0)"),
        ("--write-line", "also write the line used as evl_line_depth (per ping; "
                         "for between: the upper line)"),
        ("--fail-empty", "fail an input whose mask keeps nothing instead of "
                         "writing all-NaN"),
        ("--debug", "verbose diagnostics on stderr"),
    ],
    science={
        "evl": "The line files' CONTENT (not their paths or names). For above/below "
               "their order and duplicates don't matter; for between the order is "
               "kept (it decides upper/lower when the medians are equal).",
        "keep": "Which side of the line is kept.",
        "depth_offset": "Metres added to the line depth (negative = shallower; "
                        "default 0).",
        "var": "Variable whose dimensions define the mask.",
        "time_dim": "Time dimension used, as resolved.",
        "depth_dim": "Depth dimension used, as resolved.",
        "channel_index": "Channel whose echo_range/depth gives the depth axis. "
                         "Recorded only when --var or that axis has a channel "
                         "dimension.",
        "write_line": "Adds the evl_line_depth variable to the file.",
    },
    files=(
        "Reads flat NetCDF, local or gs://; EVL files local or gs:// (read through "
        "the cache, AA_CACHE_DIR). Writes <base>_<hash8>.nc beside each input "
        "(current directory for gs:// input), in --out-dir, or under --dest "
        "DIR|gs://PREFIX. -o and --suffix keep the old explicit names; "
        "AA_NAMING=legacy restores the old default <stem>_evl.nc. An identical "
        "earlier product is reused; a different existing file is replaced only "
        "with --overwrite."
    ),
    pipeline=(
        "After aa-sv / aa-clean / aa-depth, before aa-evr, aa-graph, aa-mvbs, ... "
        "Every stdin line is one input and gives one output line."
    ),
    examples=[
        "aa-nc x.raw --sonar_model EK60 | aa-sv | aa-evl --evl seafloor.evl "
        "--depth-offset -5 | aa-graph",
        "aa-evl a.nc b.nc --evl surface.evl --keep below --out-dir masked/",
        "aa-evl x_Sv.nc --evl upper.evl lower.evl --keep between --write-line",
    ],
    notes=[
        "Line depths are compared with echo_range of --channel-index at the first "
        "ping (range from the transducer) when the file has it, otherwise with "
        "depth, otherwise with sample indices (with a warning).",
    ],
)


# ---------------------------
# Help / logging
# ---------------------------

def print_help() -> None:
    """Curated help (also used by the docs generator)."""
    sys.stdout.write(render(SPEC, HELP, _build_parser()))


def print_help_full() -> None:
    print(
        """
aa-evl - apply Echoview line(s) (.evl) to an echogram NetCDF (.nc/.netcdf4)

USAGE
  echo input.nc | aa-evl --evl seafloor.evl | aa-plot --all
  aa-evl input.nc --evl top.evl bottom.evl --keep between
  echo input.nc | aa-evl --evl seafloor.evl --depth-offset -5.0 --overwrite

REQUIRED
  --evl EVL [EVL ...]     One or more .evl paths (accepts wildcards via shell)
                          or gs:// URIs (read through the aalibrary cache).

INPUT
  INPUT_PATH [INPUT_PATH ...]
    Optional positional .nc paths or gs:// URIs. If omitted, reads
    newline-delimited .nc paths from stdin. An empty stdin is an error
    (exit 1); a bare `aa-evl` on a terminal prints the short help.

OUTPUT
  -o, --output-path PATH  Only valid when processing exactly 1 input. Used as
                          given; may be a gs:// URI.
  --out-dir DIR           Output directory for pipelines / multiple inputs.
  --suffix TEXT           Name outputs <input stem><TEXT>.nc (in --out-dir, or
                          beside the input). Default: no suffix, outputs are
                          named <base>_<hash8>.nc (AA_NAMING=legacy: the old
                          default suffix _evl).
  --overwrite             Replace an existing output that is a different
                          product. An identical product (same input, same EVL
                          content, same options) is reused.
  --base NAME             Base name for the default output name.
  --dest DIR|gs://PREFIX  Write the default-named output there.
  --force                 Recompute even if an identical product exists.

MASKING
  --keep {above,below,between}
                          Which side of the line(s) to KEEP (default: above).
                            above   : keep data shallower than the line; mask
                                      deeper data.  Union = per-ping MIN depth.
                                      Typical use: exclude seafloor / bottom.
                            below   : keep data deeper than the line; mask
                                      shallower data.  Union = per-ping MAX depth.
                                      Typical use: exclude near-surface noise.
                            between : keep data BETWEEN two lines.  Requires
                                      exactly 2 EVL files (upper then lower;
                                      swapped if the first has the deeper
                                      median depth).
                          Cells exactly on a line are kept.
  --depth-offset METRES   Shift the composite line by this many metres before
                          masking.  Negative = shift up (shallower); positive =
                          shift down (deeper).  Default: 0.0.  With --keep
                          between, both lines are shifted.
                          Example: --depth-offset -5  removes 5 m above seafloor.
  --var NAME              Variable to mask (default: Sv).
  --time-dim NAME         Time dimension name (default: infer ping_time or time).
  --depth-dim NAME        Depth dimension name (default: infer depth, range_sample,
                          range_bin, or echo_range as in aa-mvbs output).
  --channel-index INT     Channel used to resolve metre-depth coordinates when the
                          variable has a 'channel' dim (default: 0).
  --write-line            Write the interpolated composite line as a variable
                          'evl_line_depth' in the output NetCDF (for --keep
                          between: the upper line).
  --fail-empty            Exit non-zero if the line mask keeps zero cells.
  --debug                 Verbose diagnostics to stderr.

LINE HANDLING
  Each line is interpolated linearly to every ping; pings outside the EVL time
  range take the depth of the nearest end point.  Points whose depth is a
  sentinel (|depth| >= 9000, e.g. Echoview's -10000.99) are dropped and the
  line is interpolated across them; remaining depths are clipped to the
  echogram depth range.  Depths are compared with echo_range (else depth) of
  --channel-index at the first ping.

PROVENANCE
  Each output embeds its provenance (aa-metadata FILE): the input's chain, this
  step with its scientific options, and the EVL files as inputs with role
  "regions", identified by content.  The attributes aa_tool, aa_evl_files,
  aa_evl_keep and aa_evl_depth_offset are kept.

EXAMPLES
  # Mask everything below the seafloor line, with a 5 m safety buffer above it:
  aa-evl input.nc --evl seafloor.evl --keep above --depth-offset -5.0

  # Mask near-surface noise (everything above the surface exclusion line):
  aa-evl input.nc --evl surface.evl --keep below

  # Keep only data between two operator-drawn lines:
  aa-evl input.nc --evl upper.evl lower.evl --keep between

  # Full pipeline:
  echo D20090916-T132105.raw | aa-nc --sonar_model EK60 | aa-sv | aa-depth \\
    | aa-evl --evl seafloor.evl --depth-offset -5.0 --overwrite | aa-plot --all

NOTE
  EVL files are Echoview line export files containing (datetime, depth_metres)
  pairs that define a boundary line across the echogram.  They differ from EVR
  (region) files, which contain closed polygons.
"""
    )


def _configure_logging(debug: bool) -> None:
    """Replace the default suppression sink with a user-visible one.
    Keeps standard logging fully disabled."""
    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if debug else "INFO")


# ---------------------------
# EVL files (file-valued scientific option)
# ---------------------------

@dataclass
class _LineFile:
    """One --evl source, resolved once for the whole batch."""
    raw: str      # as given on the command line
    token: str    # what the core reads: a local path or a gs:// URI
    local: Path   # readable local copy
    id: str       # content identity (enters the hash)

    @property
    def shown(self) -> str:
        """How the file is listed in the aa_evl_files attribute: the resolved
        local path, as before, or the gs:// URI."""
        return self.raw if uris.is_gcs(self.raw) else str(self.local)


def _resolve_line_files(sources: List[str]) -> Tuple[Optional[List["_LineFile"]], str]:
    """Resolve --evl sources to local files and content identities, in the
    order given. Returns (files, "") or (None, error message).

    Local paths and file:// URIs are read in place; gs:// URIs through the
    aalibrary cache (a gcsfuse mount or one download per object version).
    """
    probe = Run(SPEC)   # only to compute identities; never plans anything
    out: List[_LineFile] = []
    for raw in sources:
        if uris.is_gcs(raw):
            # Checked here so a missing object exits 2 like a missing local
            # file (the core would exit 1). A gcsfuse mount needs no API call.
            if uris.mounted_path(raw) is None:
                try:
                    found = uris.stat(raw) is not None
                except Exception as exc:      # credentials, network, permissions
                    return None, f"EVL file not readable: {raw} ({exc})"
                if not found:
                    return None, f"EVL file not found: {raw}"
            token = raw
        else:
            local = Path(uris.from_file_uri(raw)).expanduser().resolve()
            if not local.exists():
                return None, f"EVL file not found: {local}"
            token = str(local)
        try:
            inp = probe.param_file(token, role="regions")
        except (Exception, SystemExit) as exc:   # the core exits on unreadable input
            return None, f"EVL file not readable: {raw} ({exc})"
        if inp.local.suffix.lower() != ".evl":
            logger.warning(f"Unexpected extension for EVL file: {inp.local.name}")
        out.append(_LineFile(raw, token, inp.local, inp.id))
    return out, ""


def _canonical_lines(files: List["_LineFile"], keep: str) -> List["_LineFile"]:
    """The order used for the hash AND the computation.

    above / below: per-ping min / max over the lines, so order and duplicates
    don't matter: sort by content identity, one file per identity.
    between: the order as given (upper, lower). The median swap makes it
    irrelevant unless the two medians are equal, and then it decides.
    """
    if keep == "between":
        return list(files)
    seen = {}
    for f in files:
        seen.setdefault(f.id, f)
    return [seen[k] for k in sorted(seen)]


def _register_lines(run: Run, files: List["_LineFile"]) -> None:
    for f in files:
        run.param_file(f.token, role="regions")


# ---------------------------
# Input handling
# ---------------------------

_ALLOWED_EXT = {".nc", ".netcdf4"}


def _validate_inputs(tokens: List[str]) -> None:
    """Up-front checks, as before: a missing local input or a wrong extension
    stops the whole batch with exit 1. (A gs:// input is checked when read.)"""
    for tok in tokens:
        name = uris.basename(tok)
        if not uris.is_gcs(tok):
            p = Path(uris.from_file_uri(tok)).expanduser().resolve()
            if not p.exists():
                logger.error(f"Input file not found: {p}")
                sys.exit(1)
            name = p.name
        if Path(name).suffix.lower() not in _ALLOWED_EXT:
            logger.error(
                f"Unsupported input extension: {name} "
                f"(allowed: {', '.join(sorted(_ALLOWED_EXT))})"
            )
            sys.exit(1)


# ---------------------------
# Dimension helpers
# ---------------------------

def _infer_dims(
    ds: xr.Dataset,
    var: str,
    time_dim: Optional[str],
    depth_dim: Optional[str],
) -> Tuple[str, str]:
    if var not in ds.data_vars:
        raise ValueError(
            f"Variable '{var}' not found. Available: {list(ds.data_vars.keys())}"
        )
    da = ds[var]

    if time_dim:
        tdim = time_dim
    else:
        tdim = (
            "ping_time" if "ping_time" in da.dims
            else ("time" if "time" in da.dims else None)
        )
        if tdim is None:
            raise ValueError(
                f"Could not infer time dim for '{var}'. Provide --time-dim."
            )

    if depth_dim:
        ddim = depth_dim
    else:
        if "depth" in da.dims:
            ddim = "depth"
        elif "range_sample" in da.dims:
            ddim = "range_sample"
        elif "range_bin" in da.dims:
            ddim = "range_bin"
        elif "echo_range" in da.dims:
            # aa-mvbs output: Sv on (channel, ping_time, echo_range). Checked
            # last, so files that have depth/range_sample/range_bin infer the
            # same dimension as before.
            ddim = "echo_range"
        else:
            raise ValueError(
                f"Could not infer depth dim for '{var}'. Provide --depth-dim."
            )

    if tdim not in da.dims:
        raise ValueError(f"time dim '{tdim}' not in {var}.dims={da.dims}")
    if ddim not in da.dims:
        raise ValueError(f"depth dim '{ddim}' not in {var}.dims={da.dims}")

    return tdim, ddim


def _get_depth_coord_metres(
    ds: xr.Dataset,
    var_da: xr.DataArray,
    time_dim: str,
    depth_dim: str,
    channel_index: int,
) -> Optional[np.ndarray]:
    """
    Return a 1-D numpy array of depth values in metres aligned to depth_dim,
    or None if no usable metres coordinate is found.
    Tries echo_range then depth; strips channel if present; takes first ping
    if 2-D.
    """
    def _pick_ch(x):
        if x is None:
            return None
        if "channel" in x.dims:
            try:
                x = x.isel(channel=channel_index)
                if "channel" in x.coords:
                    x = x.drop_vars("channel")
            except Exception:
                return None
        return x

    for cname in ("echo_range", "depth"):
        x = _pick_ch(ds[cname]) if cname in ds else None
        if x is None and cname in var_da.coords:
            x = _pick_ch(var_da.coords[cname])
        if x is None:
            continue
        if depth_dim in x.dims and time_dim not in x.dims:
            return np.asarray(x.values, dtype=float)
        if time_dim in x.dims and depth_dim in x.dims:
            return np.asarray(x.isel({time_dim: 0}).values, dtype=float)

    return None


# ---------------------------
# EVL reading + line building
# ---------------------------

def _read_evl_to_series(
    evl_path: Path,
    ech_depth_min: float,
    ech_depth_max: float,
    debug: bool,
) -> pd.Series:
    """
    Read a single EVL file and return a pd.Series of depth values in metres,
    indexed by (tz-naive) datetime64. The index unit is whatever pandas parsed:
    [us] under pandas 3; _interpolate_line_to_pings converts it to [ns].

    Handles:
    - Sentinel depth values (|depth| >= 9000, e.g. Echoview's -10000.99
      "no data") -> dropped (not replaced), so the line is interpolated across
      them from the neighbouring valid points.
    - Remaining depths -> clipped to the echogram depth range.
    - NaN / bad depth values -> dropped.
    - The echoregions Lines2D dataframe layout (columns depend on version).
    """
    lines2d = er.read_evl(str(evl_path))

    # --- get underlying dataframe (version-resilient) ---
    df = None
    if hasattr(lines2d, "to_dataframe"):
        try:
            df = lines2d.to_dataframe()
        except Exception:
            pass
    if df is None and hasattr(lines2d, "data"):
        try:
            df = lines2d.data
        except Exception:
            pass
    if df is None:
        raise RuntimeError(
            f"Could not access Lines2D dataframe for {evl_path}"
        )

    df = df.copy()

    if debug:
        logger.debug(
            f"{evl_path.name}: Lines2D dataframe columns={list(df.columns)}, "
            f"shape={df.shape}"
        )

    # --- locate time and depth columns ---
    # Common column name variations across echoregions versions:
    #   time / ping_time / datetime
    #   depth / depth_meters / Depth
    time_col = None
    for candidate in ("ping_time", "time", "datetime", "Time"):
        if candidate in df.columns:
            time_col = candidate
            break

    depth_col = None
    for candidate in ("depth", "depth_meters", "Depth", "range"):
        if candidate in df.columns:
            depth_col = candidate
            break

    # If the dataframe has a DatetimeIndex, use that for time
    if time_col is None and isinstance(df.index, pd.DatetimeIndex):
        df = df.reset_index().rename(columns={"index": "ping_time"})
        time_col = "ping_time"

    if time_col is None or depth_col is None:
        raise RuntimeError(
            f"{evl_path.name}: Cannot locate time/depth columns in EVL dataframe. "
            f"Available columns: {list(df.columns)}"
        )

    # --- coerce time to datetime64 (unit as parsed; converted to ns later) ---
    t_raw = pd.to_datetime(df[time_col], errors="coerce", utc=False)
    # Strip timezone so we can compare with naive NetCDF timestamps
    if hasattr(t_raw, "dt") and t_raw.dt.tz is not None:
        t_raw = t_raw.dt.tz_localize(None)

    # --- coerce depth to float, drop sentinels, clip ---
    d_raw = pd.to_numeric(df[depth_col], errors="coerce").astype(float)
    d_raw = d_raw.where(d_raw.abs() < 9000, other=np.nan)  # drop sentinels
    d_raw = d_raw.clip(lower=ech_depth_min, upper=ech_depth_max)

    # --- build series, drop NaT / NaN rows, sort by time ---
    s = pd.Series(d_raw.values, index=t_raw, name="depth")
    s = s[s.index.notna() & s.notna()]
    s = s.sort_index()

    if len(s) == 0:
        raise RuntimeError(
            f"{evl_path.name}: No valid (time, depth) points after cleaning. "
            "Check that the EVL time range overlaps the echogram."
        )

    if debug:
        logger.debug(
            f"{evl_path.name}: {len(s)} valid line points, "
            f"depth range {s.min():.2f}-{s.max():.2f} m, "
            f"time range {s.index.min()} -> {s.index.max()}"
        )

    return s


def _as_float_ns(times) -> np.ndarray:
    """Datetimes (any datetime64 unit, DatetimeIndex, or parseable values) as
    float64 nanoseconds since the epoch. tz-aware values lose their zone, as
    the EVL times do in _read_evl_to_series."""
    idx = pd.DatetimeIndex(pd.to_datetime(times))
    if idx.tz is not None:
        idx = idx.tz_localize(None)
    return idx.values.astype("datetime64[ns]").astype(np.int64).astype(float)


def _interpolate_line_to_pings(
    line_series: pd.Series,
    ping_times: np.ndarray,
    fill_value_min: float,
    fill_value_max: float,
) -> np.ndarray:
    """
    Linearly interpolate a (time -> depth) line Series to every ping_time in the
    echogram.  Pings outside the EVL time range are filled with the nearest
    boundary value (clamp extrapolation rather than NaN-fill).

    Returns a 1-D float64 array of shape (n_pings,).
    """
    # Both time axes as float64 nanoseconds. The unit must be converted
    # explicitly: under pandas 3 the EVL index is datetime64[us] while ping_time
    # is datetime64[ns], and a bare .astype(np.int64) returned microseconds for
    # one and nanoseconds for the other, so np.interp clamped every ping to the
    # last EVL point.
    evl_t_ns = _as_float_ns(line_series.index)
    evl_d = line_series.values.astype(float)

    ping_t_ns = _as_float_ns(ping_times)

    # Use numpy interp (clamps at boundaries automatically)
    interp_depth = np.interp(ping_t_ns, evl_t_ns, evl_d)

    # Apply explicit clamp to echogram depth range
    interp_depth = np.clip(interp_depth, fill_value_min, fill_value_max)

    return interp_depth


# ---------------------------
# Mask building + application
# ---------------------------

def _build_line_mask(
    ds: xr.Dataset,
    evl_files: List[Path],
    var: str,
    time_dim: str,
    depth_dim: str,
    channel_index: int,
    keep: str,
    depth_offset: float,
    debug: bool,
) -> Tuple[xr.DataArray, np.ndarray]:
    """
    Build a boolean mask (True = keep) aligned to ds[var] on (time_dim, depth_dim).

    Returns
    -------
    mask : xr.DataArray
        Boolean, dims (time_dim, depth_dim), positional coordinates.
    composite_line : np.ndarray
        1-D float array of shape (n_pings,): the interpolated line depth after
        offset, in metres (or depth-index units if no metre coord found).
    """

    var_da = ds[var]

    # --- channel stripping ---
    if "channel" in var_da.dims:
        if channel_index < 0 or channel_index >= var_da.sizes["channel"]:
            raise ValueError(
                f"--channel-index {channel_index} out of range "
                f"(size={var_da.sizes['channel']})"
            )
        da = var_da.isel(channel=channel_index)
        if "channel" in da.coords:
            try:
                da = da.drop_vars("channel")
            except Exception:
                pass
    else:
        da = var_da

    # --- original coordinate arrays (positional labels for output mask) ---
    orig_time_vals = np.asarray(
        ds[time_dim].values if time_dim in ds.coords else da.coords[time_dim].values
    )
    orig_depth_vals = (
        np.asarray(da.coords[depth_dim].values)
        if depth_dim in da.coords
        else np.arange(da.sizes[depth_dim])
    )

    # --- resolve metre-valued depth axis ---
    depth_vals_m = _get_depth_coord_metres(
        ds, var_da, time_dim, depth_dim, channel_index
    )

    if depth_vals_m is not None:
        ech_depth_min = float(np.nanmin(depth_vals_m))
        ech_depth_max = float(np.nanmax(depth_vals_m))
    else:
        ech_depth_min = float(np.nanmin(orig_depth_vals))
        ech_depth_max = float(np.nanmax(orig_depth_vals))
        logger.warning(
            "No metre-valued depth coordinate found; using range_sample indices "
            "as depth proxy. Masking may be inaccurate if the depth axis is not "
            "in metres. Provide echo_range or depth in the dataset for best results."
        )

    if debug:
        logger.debug(
            f"Echogram depth range: {ech_depth_min:.2f} -> {ech_depth_max:.2f} m"
        )
        logger.debug(
            f"Ping time range: {orig_time_vals.min()} -> {orig_time_vals.max()}"
        )

    # --- read and interpolate each EVL ---
    per_file_lines: List[np.ndarray] = []
    for evl_path in evl_files:
        s = _read_evl_to_series(evl_path, ech_depth_min, ech_depth_max, debug)
        interp = _interpolate_line_to_pings(
            s, orig_time_vals, ech_depth_min, ech_depth_max
        )
        per_file_lines.append(interp)
        if debug:
            logger.debug(
                f"{evl_path.name}: interpolated line depth "
                f"min={interp.min():.2f} max={interp.max():.2f} m"
            )

    # --- build composite line ---
    composite_line: Optional[np.ndarray] = None
    upper_line: Optional[np.ndarray] = None
    lower_line: Optional[np.ndarray] = None

    if keep == "above":
        # Shallowest of all lines -> keep anything above even the shallowest
        composite_line = np.min(np.stack(per_file_lines, axis=0), axis=0)
    elif keep == "below":
        # Deepest of all lines -> keep anything below even the deepest
        composite_line = np.max(np.stack(per_file_lines, axis=0), axis=0)
    else:  # between - validated to have exactly 2
        upper_line = per_file_lines[0]
        lower_line = per_file_lines[1]
        # Ensure correct ordering (upper should be shallower)
        if np.median(upper_line) > np.median(lower_line):
            logger.warning(
                "The first EVL appears deeper than the second for --keep between. "
                "Swapping so the shallower line is treated as upper."
            )
            upper_line, lower_line = lower_line, upper_line

    # --- apply depth offset ---
    if keep == "between":
        upper_line = np.clip(upper_line + depth_offset, ech_depth_min, ech_depth_max)
        lower_line = np.clip(lower_line + depth_offset, ech_depth_min, ech_depth_max)
    else:
        composite_line = np.clip(
            composite_line + depth_offset, ech_depth_min, ech_depth_max
        )

    if debug and composite_line is not None:
        logger.debug(
            f"Composite line after offset: min={composite_line.min():.2f} "
            f"max={composite_line.max():.2f} m"
        )

    # --- depth axis in metres for comparison ---
    depth_axis = depth_vals_m if depth_vals_m is not None else orig_depth_vals.astype(float)

    # --- build boolean mask (n_pings, n_depth) ---
    # depth_axis: shape (n_depth,)
    # composite/upper/lower: shape (n_pings,)
    # We need a (n_pings, n_depth) boolean array.
    # Expand dims for broadcasting:
    #   depth_axis_2d : (1, n_depth)
    #   line_2d       : (n_pings, 1)

    depth_2d = depth_axis[np.newaxis, :]  # (1, n_depth)

    if keep == "above":
        # Keep cells where depth <= line threshold (shallower than / on the line)
        line_2d = composite_line[:, np.newaxis]  # (n_pings, 1)
        mask_np = depth_2d <= line_2d

    elif keep == "below":
        # Keep cells where depth >= line threshold (deeper than / on the line)
        line_2d = composite_line[:, np.newaxis]
        mask_np = depth_2d >= line_2d

    else:  # between
        upper_2d = upper_line[:, np.newaxis]
        lower_2d = lower_line[:, np.newaxis]
        mask_np = (depth_2d >= upper_2d) & (depth_2d <= lower_2d)

    inside = int(mask_np.sum())
    total = mask_np.size
    logger.info(
        f"Line mask coverage: {inside}/{total} cells kept "
        f"({100.0 * inside / max(total, 1):.1f}%)"
    )

    # --- wrap into DataArray with original coordinate labels ---
    mask_da = xr.DataArray(
        mask_np,
        dims=(time_dim, depth_dim),
        coords={
            time_dim: orig_time_vals,
            depth_dim: orig_depth_vals,
        },
    )

    # For "between" mode there is no single composite line; return the upper
    # one so callers that want to write a representative line still get something.
    returned_line = composite_line if keep != "between" else upper_line
    return mask_da, returned_line


def _apply_mask(
    ds: xr.Dataset,
    mask: xr.DataArray,
    time_dim: str,
    depth_dim: str,
    write_line: bool,
    composite_line: np.ndarray,
) -> xr.Dataset:
    """
    Apply boolean mask to all variables containing (time_dim, depth_dim).
    Uses positional masking (coordinate-stripped) to avoid the silent
    coordinate-mismatch / NaN-fill bug from aa-evr.

    Variables that represent the depth/range *axis* (echo_range, depth,
    range, range_meter, etc.) are intentionally NOT masked. They define
    the coordinate system and need to remain valid for downstream tools
    (especially aa-plot's auto-range — masking them creates a NaN-filled
    axis that visibly breaks the y-axis scale).
    """
    AXIS_VARS = frozenset({
        "echo_range", "depth", "range", "range_m", "range_meter", "range_sample",
    })

    ds_out = ds.copy(deep=False)

    # Pre-compute positional numpy mask (n_time, n_depth)
    mask_np = np.asarray(mask.transpose(time_dim, depth_dim).values, dtype=bool)

    for name, da in list(ds_out.data_vars.items()):
        if name in AXIS_VARS:
            continue  # Skip axis variables — masking them breaks the coord system
        if (time_dim in da.dims) and (depth_dim in da.dims):
            m_bare = xr.DataArray(mask_np, dims=(time_dim, depth_dim))
            m_broadcast = m_bare.broadcast_like(da)
            ds_out[name] = xr.where(m_broadcast, da, np.nan)

    if write_line and composite_line is not None:
        # Store the composite line depth per ping
        time_vals = np.asarray(
            ds[time_dim].values if time_dim in ds.coords
            else ds_out[time_dim].values
        )
        ds_out["evl_line_depth"] = xr.DataArray(
            composite_line,
            dims=(time_dim,),
            coords={time_dim: time_vals},
        )
        ds_out["evl_line_depth"].attrs["long_name"] = (
            "Composite EVL line depth (metres) after offset"
        )
        ds_out["evl_line_depth"].attrs["units"] = "m"

    return ds_out


# ---------------------------
# Scientific parameters actually used
# ---------------------------

def _channel_used(ds: xr.Dataset, var: str) -> bool:
    """Whether --channel-index can affect the result.

    It selects the channel of --var (when it has one) and, independently, the
    channel of the echo_range / depth axis (see _get_depth_coord_metres). If
    neither has a channel dimension the index is unused. Conservative: any
    channel dimension on those candidates counts.
    """
    da = ds[var]
    if "channel" in da.dims:
        return True
    for cname in ("echo_range", "depth"):
        x = ds[cname] if cname in ds else (da.coords[cname] if cname in da.coords else None)
        if x is not None and "channel" in x.dims:
            return True
    return False


def _resolve_masking(nc_path: Path, var: str, time_dim: Optional[str],
                     depth_dim: Optional[str], channel_index: int) -> dict:
    """The variable, dimensions and channel the mask will actually use.

    Reads only the file's metadata. These resolved values (not the flags as
    typed) enter the hash, so an explicit `--time-dim ping_time` and the
    auto-detected ping_time are the same product.
    """
    with xr.open_dataset(nc_path) as ds:
        tdim, ddim = _infer_dims(ds, var=var, time_dim=time_dim, depth_dim=depth_dim)
        used = _channel_used(ds, var)
    return {
        "var": var,
        "time_dim": tdim,
        "depth_dim": ddim,
        "channel_index": channel_index if used else None,
    }


def _kind_of(src) -> str:
    """Masking keeps what the data is: masked Sv is sv, masked MVBS is mvbs."""
    kind = (((src.prov or {}).get("product") or {}).get("kind") or "").strip()
    return kind if kind and kind not in {"echodata", "source"} else SPEC.kind


# ---------------------------
# Output path resolution
# ---------------------------

def _resolve_output_path(
    input_path: Path,
    output_path: Optional[Path],
    out_dir: Optional[Path],
    suffix: str,
) -> Path:
    """The old naming rule: -o as given, else (out_dir or input dir)/<stem><suffix>.nc."""
    if output_path is not None:
        return output_path
    out_name = input_path.with_suffix("").name + suffix + ".nc"
    return (out_dir or input_path.parent) / out_name


def _input_dir_and_path(src) -> Tuple[Path, Path]:
    """(folder, path) the old rule names outputs after: the resolved local
    input, or the current directory for a gs:// input."""
    if src.via == "local":
        p = src.local.resolve()
        return p.parent, p
    return Path.cwd(), Path(src.name)


def _explicit_output(args, src) -> Optional[str]:
    """Names the user chose keep their old meaning.

    -o PATH is used as given (local or gs://). An explicitly given --suffix
    keeps the old <stem><suffix>.nc name, in --dest, --out-dir or beside the
    input. Otherwise None: the standard <base>_<hash8>.nc name.
    """
    if args.output_path:
        return str(args.output_path)
    if args.suffix is None:
        return None
    folder, in_path = _input_dir_and_path(src)
    name = in_path.with_suffix("").name + args.suffix + ".nc"
    where = args.dest or args.out_dir
    if where and uris.is_remote(str(where)):
        return uris.join(str(where), name)
    if where:
        return str(Path(where).expanduser().resolve() / name)
    return str(folder / name)


def _legacy_output(args, src, suffix: str) -> str:
    """The old default name, <stem><suffix>.nc in --out-dir or beside the input."""
    folder, in_path = _input_dir_and_path(src)
    if args.out_dir and uris.is_remote(str(args.out_dir)):
        return uris.join(str(args.out_dir), in_path.with_suffix("").name + suffix + ".nc")
    out_dir = Path(args.out_dir).expanduser().resolve() if args.out_dir else None
    return str(_resolve_output_path(in_path, None, out_dir or folder, suffix))


# ---------------------------
# Per-file processing
# ---------------------------

def _process_file(
    input_path: Path,
    evl_files: List[Path],
    output_path: Path,
    var: str,
    time_dim: Optional[str],
    depth_dim: Optional[str],
    channel_index: int,
    keep: str,
    depth_offset: float,
    write_line: bool,
    fail_empty: bool,
    debug: bool,
    evl_sources: Optional[List[str]] = None,
) -> Optional[Path]:
    """Mask one file and write it to output_path. None: nothing written.

    Output-exists / --overwrite and the refuse-to-overwrite-the-input guard are
    decided by the caller, before this runs (reuse comes first).
    """
    # Read into memory and release the file handle so we can write into the
    # same directory without xarray holding a read lock.
    with xr.open_dataset(input_path) as ds_in:
        ds = ds_in.load()

    tdim, ddim = _infer_dims(ds, var=var, time_dim=time_dim, depth_dim=depth_dim)
    if debug:
        logger.debug(
            f"Using dims: time_dim='{tdim}', depth_dim='{ddim}', "
            f"{var}.dims={ds[var].dims}, shape={ds[var].shape}"
        )

    mask, composite_line = _build_line_mask(
        ds=ds,
        evl_files=evl_files,
        var=var,
        time_dim=tdim,
        depth_dim=ddim,
        channel_index=channel_index,
        keep=keep,
        depth_offset=depth_offset,
        debug=debug,
    )

    inside = int(mask.values.sum())
    total = int(mask.size)

    if inside == 0:
        logger.error(
            "Line mask is EMPTY - 0 cells would be kept; output will be all-NaN.\n"
            "Possible causes:\n"
            "  - EVL time range does not overlap the NetCDF ping_time range\n"
            "  - --keep direction is inverted for this line\n"
            "  - Sentinel depth values were not resolved correctly\n"
            "Run with --debug to inspect time/depth ranges."
        )
        if fail_empty:
            return None

    if inside == total:
        logger.warning(
            "Line mask is FULL - all cells are kept; no data will be masked.\n"
            "Check that the EVL line is within the echogram depth range and that\n"
            "--keep direction is correct."
        )

    ds_out = _apply_mask(
        ds,
        mask=mask,
        time_dim=tdim,
        depth_dim=ddim,
        write_line=write_line,
        composite_line=composite_line,
    )
    ds_out.attrs["aa_tool"] = "aa-evl"
    ds_out.attrs["aa_evl_files"] = ",".join(
        str(p) for p in (evl_sources if evl_sources is not None else evl_files)
    )
    ds_out.attrs["aa_evl_keep"] = keep
    ds_out.attrs["aa_evl_depth_offset"] = str(depth_offset)

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    ds_out.to_netcdf(output_path)

    return output_path


def _run_one(token: str, args, lines: List["_LineFile"],
             evl_sources: List[str]) -> Optional[str]:
    """One input -> one product. Returns the printed target, or None on failure."""
    run = Run(SPEC, args)
    src = run.input(token)
    _register_lines(run, lines)

    resolved = _resolve_masking(src.local, args.var, args.time_dim, args.depth_dim,
                                args.channel_index)
    if args.debug:
        logger.debug(f"{src.name}: resolved {resolved}")

    hash_naming = naming.mode() != "legacy"
    out = run.plan(
        ext=".nc",
        explicit=_explicit_output(args, src),
        legacy=lambda: _legacy_output(args, src, DEFAULT_SUFFIX),
        directory=args.out_dir if hash_naming else None,
        kind=_kind_of(src),
        extra_params=resolved,
    )

    # Guard against clobbering the input
    if not out.remote and Path(out.target).resolve() == src.local.resolve():
        logger.error(f"Refusing to overwrite input file: {src.local.resolve()}")
        run.discard(out)
        return None

    # Reuse comes first: an identical product is never an "overwrite".
    if run.reusable(out):
        return run.finish(out)

    # A different product already there (an identical one, e.g. with --force,
    # is not an "overwrite").
    if not args.overwrite and run.conflicts(out):
        logger.error(f"Output exists (use --overwrite): {out.target}")
        run.discard(out)
        return None

    try:
        produced = _process_file(
            input_path=src.local,
            evl_files=[f.local for f in lines],
            output_path=out.local,
            var=resolved["var"],
            time_dim=resolved["time_dim"],
            depth_dim=resolved["depth_dim"],
            channel_index=args.channel_index,
            keep=args.keep,
            depth_offset=args.depth_offset,
            write_line=args.write_line,
            fail_empty=args.fail_empty,
            debug=args.debug,
            evl_sources=evl_sources,
        )
    except BaseException:
        run.discard(out)
        raise
    if not produced:
        run.discard(out)
        return None

    logger.success(f"Saved masked NetCDF:\n\t{out.target}")
    logger.success("Piping saved .nc path to stdout ⟶")
    return run.finish(out)


# ---------------------------
# Entry point
# ---------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="aa-evl",
        description="Mask echogram NetCDF (.nc) using Echoview EVL line files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        add_help=False,
    )
    parser.add_argument(
        "input_paths", nargs="*", type=str,
        help="Input .nc/.netcdf4 paths or gs:// URIs (or read from stdin).",
    )
    parser.add_argument(
        "--evl", required=True, nargs="+", type=str, metavar="EVL",
        help="One or more .evl paths or gs:// URIs.",
    )
    parser.add_argument(
        "-o", "--output-path", dest="output_path", type=str,
        help="Output path or gs:// URI (only valid for a single input file).",
    )
    parser.add_argument(
        "--out-dir", type=str,
        help="Output directory (for pipelines / multiple inputs).",
    )
    parser.add_argument(
        "--suffix", type=str, default=None,
        help=("Name outputs <input stem><SUFFIX>.nc. Default: <base>_<hash8>.nc; "
              "AA_NAMING=legacy: suffix _evl."),
    )
    parser.add_argument(
        "--overwrite", action="store_true",
        help="Replace an existing output that is a different product.",
    )
    parser.add_argument(
        "--keep", choices=["above", "below", "between"], default="above",
        help=(
            "Which side of the line to KEEP (default: above).  "
            "'above' keeps shallower data (masks seafloor/bottom).  "
            "'below' keeps deeper data (masks surface).  "
            "'between' requires exactly 2 EVL files."
        ),
    )
    parser.add_argument(
        "--depth-offset", type=float, default=0.0, metavar="METRES",
        help=(
            "Shift the composite line by this many metres before masking.  "
            "Negative = shift line up (shallower boundary).  "
            "Default: 0.0."
        ),
    )
    parser.add_argument(
        "--var", type=str, default="Sv",
        help="Variable to mask (default: Sv).",
    )
    parser.add_argument(
        "--time-dim", type=str, default=None,
        help="Time dimension name (default: auto-detect).",
    )
    parser.add_argument(
        "--depth-dim", type=str, default=None,
        help="Depth dimension name (default: auto-detect).",
    )
    parser.add_argument(
        "--channel-index", type=int, default=0,
        help="Channel index for metre-depth resolution (default: 0).",
    )
    parser.add_argument(
        "--write-line", action="store_true",
        help="Write interpolated composite line as 'evl_line_depth' in output.",
    )
    parser.add_argument(
        "--fail-empty", action="store_true",
        help="Exit non-zero if the line mask keeps zero cells.",
    )
    parser.add_argument(
        "--debug", action="store_true",
        help="Verbose diagnostics to stderr.",
    )
    add_common_flags(parser)
    return parser


def main() -> int:
    # No args on a terminal: help. (An empty pipe is an error: see stdio.)
    if len(sys.argv) == 1 and not stdio.stdin_is_piped():
        print_help()
        return 0

    parser = _build_parser()
    if show_help(SPEC, HELP, parser, full=print_help_full):
        return 0
    args = parser.parse_args()

    _configure_logging(args.debug)

    tokens = stdio.many_inputs(args.input_paths, SPEC.name)   # empty stdin: exit 1

    if args.keep == "between" and len(args.evl) != 2:
        logger.error(
            f"--keep between requires exactly 2 EVL files; got {len(args.evl)}."
        )
        return 2

    # --evl may be a local path or a gs:// URI. Resolved once for the whole
    # batch; each file's CONTENT identifies it.
    lines_given, err = _resolve_line_files([str(s) for s in args.evl])
    if lines_given is None:
        logger.error(err)
        return 2
    lines = _canonical_lines(lines_given, args.keep)
    # Attribute written into the output: the files as given (user order).
    evl_sources = [f.shown for f in lines_given]

    _validate_inputs(tokens)

    if args.output_path is not None and len(tokens) != 1:
        logger.error(
            "--output-path is only valid when processing exactly 1 input. "
            "Use --out-dir for multiple files."
        )
        return 2

    if args.out_dir and not uris.is_remote(str(args.out_dir)):
        Path(args.out_dir).expanduser().resolve().mkdir(parents=True, exist_ok=True)

    if args.debug:
        logger.debug(f"\naa-evl args:\n{pprint.pformat(vars(args))}")
        logger.debug(f"EVL files (order used): {[f.raw for f in lines]}")

    any_fail = False
    for token in tokens:
        try:
            if not _run_one(token, args, lines, evl_sources):
                any_fail = True
        except SystemExit:
            # The core stops on this input (e.g. a missing gs:// object) after
            # printing why; the rest of the batch still runs.
            any_fail = True
        except Exception as e:
            any_fail = True
            logger.exception(f"Error processing {token}: {e}")

    return 1 if any_fail else 0


if __name__ == "__main__":
    raise SystemExit(main())
