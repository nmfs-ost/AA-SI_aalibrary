#!/usr/bin/env python3
"""
aa-evr

Mask echogram NetCDF (.nc/.netcdf4) using Echoview region files (.evr) via echoregions Regions2D.

AA-style pipeline behavior:
- Reads input NetCDF paths (or gs:// URIs) from stdin (newline-delimited) when piped
  OR accepts positional inputs.
- Produces a NEW NetCDF output per input.
- Emits output path(s) to stdout (one per line, in input order) for downstream piping.
- Logs go to stderr.

Core behavior:
- --evr accepts one or more .evr sources (argparse nargs="+"): local paths,
  gs:// URIs (read through the aalibrary cache), or other fsspec URIs.
- Loads all EVRs and unions all regions across them.
- Applies union mask to all variables containing (time_dim, depth_dim): outside -> NaN.

Provenance and naming (shared console core):
- Each output is a scientific product named <base>_<hash8>.nc. The hash covers
  the input's product hash, the CONTENT of the region files (not their paths;
  order and duplicates don't matter, the union is order-free), the resolved
  variable / dimension names, the channel index (when the variable has a
  channel dimension) and --write-mask.
- -o PATH and an explicit --suffix TEXT keep the old explicit names;
  AA_NAMING=legacy restores the old default <stem>_evr.nc.
- An identical earlier product is reused; a different existing file is only
  replaced with --overwrite.

Drawing mode (--evr omitted):
- Launched when --evr is NOT provided.
- Reads one NC path from stdin (or positional arg).
- Opens an interactive Bokeh browser app showing the echogram.
- User draws freehand ROI polygons directly on the echogram.
- On save, produces:
    1. An .evr file (--name, default: <stem>_regions.evr) recording the drawing.
       echoregions cannot currently parse this file, so it cannot be passed
       back to --evr.
    2. A masked NetCDF (<stem>_evr.nc) with only drawn regions kept.
- The masked NetCDF carries provenance (variant "draw"); a drawing is never
  reused, and the drawn .evr (when written) is recorded as its regions input.
- Output NC path is emitted to stdout for downstream piping.

  Example:
    cat D20191001-T003423_Sv_depth.nc | aa-evr --name my_school.evr
    cat D20191001-T003423_Sv_depth.nc | aa-evr --name school.evr | aa-plot --all

Known fixes in this version:
  1. Zero-region crash: when echoregions returns mask_3d with region_id dimension of
     size 0 (no polygons overlap the echogram time window), .max("region_id") previously
     raised "zero-size array to reduction operation maximum which has no identity".
     Now detected and handled as an all-False mask with a diagnostic warning.
     This manifests when running aa-evr on output that has already been masked by
     aa-evl (the surviving time window may no longer overlap all EVR polygons).

  2. Coordinate label mismatch: _build_union_region_mask assigned metre-based depth
     coordinate labels to the mask (needed for geometry), but _apply_mask then tried to
     reindex those metre labels against the original range_sample integer indices.
     No labels matched -> full NaN fill -> bool(NaN)==True -> nothing became NaN ->
     echogram looked completely untouched with zero errors raised.
     Fixed by (a) rebuilding the returned mask with original dataset coordinate labels,
     and (b) using positional (coordinate-stripped) masking in _apply_mask.

  3. Robust region_mask return parsing: handles variable name variations (mask_3d,
     mask) and dimension name variations (region_id, region) across echoregions versions.

  4. Depth bounds: er.read_evr() was called without min_depth/max_depth, so
     echoregions' defaults (0 m, 1000 m) made Regions2D.region_mask() silently
     drop every region with a vertex deeper than 1000 m. The bounds are now
     wide open (see _ER_DEPTH_BOUNDS); aa-evr does its own depth handling.
"""

# === Silence logs BEFORE any heavy imports ===
import atexit
import logging
import os
import re
import shutil
import sys
import tempfile
import uuid
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


# ---------------------------
# Tool identity
# ---------------------------

SPEC = ToolSpec(
    name="aa-evr",
    role="transform",
    kind="sv",   # the default; an output keeps its input's kind (masked MVBS is still mvbs)
    op="echoregions.Regions2D.region_mask",
    op_version=1,
    engines=("echoregions", "regionmask"),
    # The region files are scientific too: they are registered as inputs
    # (role "regions") so their CONTENT enters the hash. var / time_dim /
    # depth_dim / channel_index are replaced by the values actually used
    # (see _resolve_masking) before the hash is computed.
    params={
        "var": canon.text,
        "time_dim": canon.text,
        "depth_dim": canon.text,
        "channel_index": canon.integer,
        "write_mask": canon.boolean,
    },
)

# Draw mode is interactive: the polygons come from a person, and the mask is
# rasterized with matplotlib.path from the drawn coordinates (region_draw.py),
# not with echoregions. Its step says so.
DRAW_SPEC = ToolSpec(
    name="aa-evr",
    role="transform",
    kind="sv",
    op="aalibrary.utils.region_draw.polygon_mask",
    op_version=1,
    engines=("matplotlib", "bokeh"),
    # --write-mask does not apply to draw mode (region_draw never writes it).
    params={k: v for k, v in SPEC.params.items() if k != "write_mask"},
)

DEFAULT_SUFFIX = "_evr"   # the old default name, <stem>_evr.nc (AA_NAMING=legacy)

HELP = Help(
    summary="Keep only the data inside Echoview regions (.evr); set the rest to NaN.",
    does=(
        "Two modes.\n\n"
        "EVR mode (--evr given): reads one or more Echoview region files with "
        "echoregions, takes the union of all their regions, and sets every cell "
        "of every (time, depth) variable that lies outside the union to NaN. "
        "Axis variables (echo_range, depth, ...) are left intact. The mask is "
        "built on --var at --channel-index and applied to all channels. Echoview's -9999.99 / 9999.99 depths (surface / bottom) become "
        "the echogram's top / bottom; regions without usable depths (GPS or "
        "track regions) keep whole pings inside their time span.\n\n"
        "Draw mode (no --evr): opens the echogram of one input in your browser "
        "(Bokeh). Draw regions freehand, click Save, and the tool writes the "
        "masked NetCDF <stem>_evr.nc plus an .evr of the drawn polygons (--name)."
    ),
    stdin=(
        "EVR mode: flat NetCDF paths (.nc/.netcdf4) or gs:// URIs, one per line "
        "or as arguments: Sv, cleaned Sv, MVBS, ... with a (ping_time|time) x "
        "(depth|range_sample|range_bin|echo_range) variable. Draw mode: one path (only the "
        "first input is used)."
    ),
    stdout=(
        "EVR mode: one line per input, in input order: the output's absolute path "
        "(or gs:// URI). An input that fails prints nothing and the exit status is "
        "1 at the end. Draw mode: the masked NetCDF's path."
    ),
    metadata=(
        "Reads the input's provenance, appends this step with its canonical "
        "scientific options and embeds it (NetCDF attributes aa_provenance, "
        "aa_product_hash, aa_base, aa_tool, history). The region files are "
        "recorded as inputs with role 'regions' and identified by content, and "
        "the aa_evr_files attribute lists them as given. The base name is "
        "carried through. Draw mode records variant 'draw' and the drawn .evr. "
        "Inspect with: aa-metadata FILE"
    ),
    options=[
        ("--evr EVR [EVR ...]", "EVR mode: region files, local, gs://, or another "
                                "fsspec URI (s3://, https://). All regions are unioned."),
        ("-o, --output-path PATH", "exact output path (one input only); local or gs://"),
        ("--out-dir DIR", "write the outputs here instead of beside each input"),
        ("--suffix TEXT", "use the old naming <input stem><TEXT>.nc instead of "
                          "<base>_<hash8>.nc"),
        ("--overwrite", "replace an existing output that is a different product"),
        ("--var NAME", "variable the mask is built on (default: first of Sv, "
                       "Sv_clean, MVBS, TS, NASC)"),
        ("--time-dim / --depth-dim NAME", "dimension names (default: ping_time|time; "
                                          "depth|range_sample|range_bin|echo_range)"),
        ("--channel-index N", "channel whose depth axis builds the mask (default 0)"),
        ("--write-mask", "also write the union mask as int8 variable region_mask"),
        ("--fail-empty", "fail an input whose mask is empty instead of writing all-NaN"),
        ("--name FILE", "draw mode: .evr file name (default <stem>_regions.evr)"),
        ("--port N", "draw mode: Bokeh server port (default 5006, next free one)"),
        ("--debug", "verbose diagnostics on stderr"),
    ],
    science={
        "evr": "The region files' CONTENT (not their paths or names). Order and "
               "duplicates don't matter: the union is the same.",
        "var": "Variable the mask is built on, as resolved (an auto-detected name "
               "hashes the same as the same name given explicitly).",
        "time_dim": "Time dimension used, as resolved.",
        "depth_dim": "Depth dimension used, as resolved.",
        "channel_index": "Channel whose depth axis builds the mask. Recorded only "
                         "when --var has a channel dimension.",
        "write_mask": "Adds the region_mask variable to the file.",
    },
    files=(
        "Reads flat NetCDF, local or gs://. Region files may be local, gs:// (read "
        "through the cache, AA_CACHE_DIR) or another fsspec URI. Writes "
        "<base>_<hash8>.nc beside each input (current directory for gs:// input), "
        "in --out-dir, or under --dest DIR|gs://PREFIX. -o and --suffix keep the "
        "old explicit names; AA_NAMING=legacy restores the old default "
        "<stem>_evr.nc. An identical earlier product is reused; a different "
        "existing file is replaced only with --overwrite. Draw mode writes "
        "<stem>_evr.nc and the .evr beside the input or in --out-dir."
    ),
    pipeline=(
        "After aa-sv / aa-clean / aa-depth / aa-evl, before aa-graph, aa-plot, "
        "aa-mvbs, ... Every stdin line is one input and gives one output line. "
        "Draw mode blocks until you click Save in the browser."
    ),
    examples=[
        "aa-nc x.raw --sonar_model EK60 | aa-sv | aa-evr --evr school.evr | aa-graph",
        "aa-evr a.nc b.nc --evr gs://bucket/regions/leg1.evr --out-dir masked/",
        "aa-evl x_Sv.nc --evl bottom.evl | aa-evr --evr school.evr --write-mask",
        "aa-evr x_Sv.nc --name school.evr        # draw mode",
    ],
    notes=[
        "Region depths are compared with echo_range of --channel-index at the "
        "first ping (range from the transducer) when the file has it, otherwise "
        "with depth, otherwise with sample indices.",
        "Regions that don't overlap the echogram's time range give an empty mask: "
        "a warning and an all-NaN output (or a failure with --fail-empty).",
        "The .evr written by draw mode is a record of the drawing; echoregions "
        "cannot read it back, so don't pass it to --evr.",
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
aa-evr - apply Echoview region(s) (.evr) to an echogram NetCDF (.nc/.netcdf4)

USAGE
  # Apply existing EVR file(s):
  aa-sv input.raw | aa-depth | aa-evr --evr regions/*.evr | aa-plot --all
  aa-sv input.raw | aa-depth | aa-evl --evl bottom.evl | aa-evr --evr school.evr | aa-plot --all
  aa-evr input.nc --evr a.evr b.evr

  # Draw new regions interactively (--evr omitted):
  cat input.nc | aa-evr --name my_school.evr
  cat input.nc | aa-evr --name my_school.evr | aa-plot --all

MODES
  EVR mode (--evr provided)   Apply existing .evr file(s) to echogram.
  Draw mode (--evr omitted)   Open an interactive Bokeh browser app to draw
                              freehand ROI regions directly on the echogram.
                              Saves both a new .evr and a masked .nc. The .evr
                              records the drawing; echoregions cannot read it
                              back, so it cannot be passed to --evr.

REQUIRED (EVR mode only)
  --evr EVR [EVR ...]     One or more .evr sources. Each may be a local path
                          or a remote URI. gs://bucket/regions.evr is read
                          through the aalibrary cache (AA_CACHE_DIR); other
                          schemes (s3://, http(s)://) need fsspec + the
                          matching backend (e.g. s3fs for s3://).
                          The regions of all files are unioned, so their
                          order and any duplicates do not matter.

INPUT
  INPUT_PATH [INPUT_PATH ...]
    Optional positional .nc paths or gs:// URIs. If omitted, reads
    newline-delimited .nc paths from stdin. An empty stdin is an error
    (exit 1); a bare `aa-evr` on a terminal prints the short help.

OUTPUT
  -o, --output-path PATH  Only valid when processing exactly 1 input. Used as
                          given; may be a gs:// URI.
  --out-dir DIR           Output directory for pipelines / multiple inputs.
  --suffix TEXT           Name outputs <input stem><TEXT>.nc (in --out-dir, or
                          beside the input). Default: no suffix, outputs are
                          named <base>_<hash8>.nc (AA_NAMING=legacy: the old
                          default suffix _evr).
  --overwrite             Replace an existing output that is a different
                          product. An identical product (same input, same
                          region-file content, same options) is reused.
  --base NAME             Base name for the default output name.
  --dest DIR|gs://PREFIX  Write the default-named output there.
  --force                 Recompute even if an identical product exists.

DRAWING MODE OPTIONS
  --name TEXT             EVR output filename (default: <input_stem>_regions.evr).
                          May include or omit the .evr extension.
  --port INT              Port for the Bokeh drawing server (default: 5006;
                          auto-increments if occupied).
  Draw mode writes <input_stem>_evr.nc and the .evr beside the input (or in
  --out-dir); -o, --suffix, --base and --dest apply to EVR mode only.

MASKING (both modes)
  --var NAME              Variable to mask (default: auto-detect - first of
                          Sv, Sv_clean, MVBS, TS, NASC found in the file).
                          Pass explicitly to override.
  --time-dim NAME         Time dimension name (default: infer ping_time else time).
  --depth-dim NAME        Depth dimension name (default: infer depth, range_sample,
                          range_bin, or echo_range as in aa-mvbs output).
  --channel-index INT     Channel used to build mask when var has 'channel' dim
                          (default: 0).
  --write-mask            Write union mask as int8 variable 'region_mask' in output.
  --fail-empty            Exit non-zero if union mask is empty (0 cells inside).
  --debug                 Verbose diagnostics to stderr.

PROVENANCE
  Each output embeds its provenance (aa-metadata FILE): the input's chain, this
  step with its scientific options, and the region files as inputs with role
  "regions", identified by content. The attributes aa_tool and aa_evr_files
  (the region files as given) are kept.

EXAMPLES
  # Mask to EVR regions only:
  aa-evr input.nc --evr school.evr --overwrite

  # Pipeline: EVL bottom line first, then EVR school regions:
  echo D20090916-T132105.raw | aa-nc --sonar_model EK60 | aa-sv | aa-depth \\
    | aa-evl --evl seafloor.evl --depth-offset -5.0 \\
    | aa-evr --evr d20090916_t124739-t132105.evr --overwrite \\
    | aa-plot --all

  # Draw new regions interactively, then plot:
  cat D20191001-T003423_Sv_depth.nc | aa-evr --name school_regions.evr | aa-plot --all

NOTE
  If the EVR polygon time range does not fully overlap the echogram ping_time range
  (e.g. because an upstream aa-evl has already masked the data), echoregions may
  return zero matching regions for some files. This is treated as an all-False mask
  for that file (nothing is kept from it) rather than a crash, and a warning is
  emitted. Run with --debug to compare time ranges.
"""
    )


def _configure_logging(debug: bool) -> None:
    """Replace the default suppression sink with a user-visible one.
    Keeps standard logging fully disabled."""
    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if debug else "INFO")


# ---------------------------
# Remote EVR / URI handling
# ---------------------------

# Matches a leading URI scheme like "gs://", "s3://", "http://".  A bare local
# path, a relative path, a Windows drive path (C:\...) or a UNC path
# (\\server\share) has no "<scheme>://" prefix and so is left untouched.
_URI_RE = re.compile(r"^[A-Za-z][A-Za-z0-9+.\-]*://")


def _looks_like_uri(s: str) -> bool:
    """True if *s* carries a URI scheme (gs://, s3://, http(s)://, ...)."""
    return bool(_URI_RE.match(s))


def _safe_unlink(path: str) -> None:
    try:
        os.remove(path)
    except OSError:
        pass


def _download_to_temp(uri: str) -> Path:
    """Download a remote EVR *uri* to a local temp .evr file and return its Path.

    echoregions.read_evr only reads local files, so a remote object has to be
    materialised first.  fsspec handles s3://, http(s)://, etc.; the matching
    backend (e.g. s3fs for s3://) must be installed.  gs:// does not come here:
    it is read through the aalibrary cache (see _resolve_region_files).  The
    temp file is removed automatically when the process exits.
    """
    try:
        import fsspec
    except ImportError as exc:
        raise RuntimeError(
            f"Reading a remote EVR ({uri}) needs fsspec plus the backend for its "
            f"scheme. Install, e.g.:\n"
            f"    pip install fsspec s3fs    # for s3://\n"
            f"(import error: {exc})"
        ) from exc

    # Preserve the original extension (strip any ?query/#fragment first).
    clean = uri.split("?", 1)[0].split("#", 1)[0]
    suffix = os.path.splitext(clean)[1] or ".evr"

    fd, tmp = tempfile.mkstemp(prefix="aa_evr_", suffix=suffix)
    os.close(fd)
    atexit.register(_safe_unlink, tmp)

    try:
        with fsspec.open(uri, "rb") as remote, open(tmp, "wb") as out:
            shutil.copyfileobj(remote, out)
    except Exception as exc:
        _safe_unlink(tmp)
        raise RuntimeError(f"Failed to download EVR {uri}: {exc}") from exc

    logger.debug(f"Downloaded remote EVR {uri} -> {tmp}")
    return Path(tmp)


@dataclass
class _RegionFile:
    """One --evr source, resolved once for the whole batch."""
    raw: str                        # as given on the command line
    token: str                      # what the core reads: a local path or a gs:// URI
    local: Path                     # readable local copy
    id: str                         # content identity (enters the hash)
    remote_uri: Optional[str] = None  # a non-gs:// URI fetched with fsspec

    @property
    def shown(self) -> str:
        """How the file is listed in the aa_evr_files attribute (unchanged rule:
        the URI for remote sources, the resolved local path otherwise)."""
        if _looks_like_uri(self.raw) and not self.raw.lower().startswith("file://"):
            return self.raw
        return str(self.local)


def _resolve_region_files(sources: List[str]) -> Tuple[Optional[List["_RegionFile"]], str]:
    """Resolve --evr sources to local files and content identities, in the
    order given. Returns (files, "") or (None, error message).

    Local paths and file:// URIs are read in place; gs:// URIs through the
    aalibrary cache (a gcsfuse mount or one download per object version);
    any other scheme is downloaded with fsspec to a temp file.
    """
    probe = Run(SPEC)   # only to compute identities; never plans anything
    out: List[_RegionFile] = []
    for raw in sources:
        remote_uri = None
        if uris.is_gcs(raw):
            # Checked here so a missing object exits 2 like a missing local
            # file (the core would exit 1). A gcsfuse mount needs no API call.
            if uris.mounted_path(raw) is None:
                try:
                    found = uris.stat(raw) is not None
                except Exception as exc:      # credentials, network, permissions
                    return None, f"EVR file not readable: {raw} ({exc})"
                if not found:
                    return None, f"EVR file not found: {raw}"
            token = raw
        elif _looks_like_uri(raw) and not raw.lower().startswith("file://"):
            try:
                token = str(_download_to_temp(raw))
            except Exception as exc:
                return None, str(exc)
            remote_uri = raw
        else:
            if raw.lower().startswith("file://"):
                from urllib.parse import unquote, urlparse
                local = Path(unquote(urlparse(raw).path))
            else:
                local = Path(raw)
            local = local.expanduser().resolve()
            if not local.exists():
                return None, f"EVR file not found: {local}"
            token = str(local)
        try:
            inp = probe.param_file(token, role="regions")
        except (Exception, SystemExit) as exc:   # the core exits on unreadable input
            return None, f"EVR file not readable: {raw} ({exc})"
        out.append(_RegionFile(raw, token, inp.local, inp.id, remote_uri))
    return out, ""


def _canonical_regions(files: List["_RegionFile"]) -> List["_RegionFile"]:
    """The union is order-free and idempotent: sort by content identity and
    keep one file per identity. This order is used for the hash AND for the
    computation."""
    seen = {}
    for f in files:
        seen.setdefault(f.id, f)
    return [seen[k] for k in sorted(seen)]


def _register_regions(run: Run, files: List["_RegionFile"]) -> None:
    for f in files:
        # A file fetched with fsspec is recorded by its URI, not the temp copy.
        run.param_file(f.token, role="regions", uri=f.remote_uri)


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
# Dataset helpers
# ---------------------------

# Default-variable search order when --var is not given.  First hit wins.
# Mirrors aa_graph.py / aa_plot.py / kmeans_core.ACOUSTIC_VARIABLES so all
# four tools agree on which variable a multi-var file targets by default.
# Add new entries here when new acoustic variables come up — one line each.
_DEFAULT_VAR_CANDIDATES = (
    "Sv", "Sv_clean", "MVBS", "TS", "NASC",
)


def _resolve_var(ds: xr.Dataset, var: Optional[str]) -> str:
    """Pick a variable to mask.

    If *var* is given, validate it exists and return it.  Otherwise walk
    _DEFAULT_VAR_CANDIDATES and return the first one present in the
    dataset.  As a last resort, fall back to the first data variable in
    the file (with a warning, since auto-picking an unknown variable is
    a guess).
    """
    if var is not None:
        if var not in ds.data_vars:
            raise ValueError(
                f"Variable '{var}' not found. "
                f"Available: {list(ds.data_vars.keys())}"
            )
        return var

    for cand in _DEFAULT_VAR_CANDIDATES:
        if cand in ds.data_vars:
            return cand

    if not ds.data_vars:
        raise ValueError("Dataset has no data variables to mask.")

    fallback = list(ds.data_vars)[0]
    logger.warning(
        f"No standard acoustic variable "
        f"({', '.join(_DEFAULT_VAR_CANDIDATES)}) found in dataset. "
        f"Falling back to first data variable: '{fallback}'. "
        f"Pass --var explicitly to override."
    )
    return fallback


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


# ---------------------------
# Mask building + application
# ---------------------------

def _extract_region_mask_union(
    region_mask_result,
    ping_time_size: int,
    depth_size: int,
    debug: bool,
) -> xr.DataArray:
    """
    Robustly extract a 2-D boolean union mask (ping_time x depth) from the
    return value of Regions2D.region_mask(), regardless of echoregions version.

    Handles:
      - Tuple return  -> (dataset_or_da, region_ids)
      - Dataset       -> look for 'mask_3d', then any variable
      - DataArray     -> use directly
      - ZERO regions  -> region_id dimension of size 0; returns all-False mask
                         instead of crashing on numpy max() of empty array.
        This happens when an EVR's polygon time range does not overlap the
        echogram after an upstream step (e.g. aa-evl) has trimmed the data.
    """
    # --- unwrap tuple if needed ---
    if isinstance(region_mask_result, tuple):
        raw = region_mask_result[0]
    else:
        raw = region_mask_result

    # --- extract DataArray from Dataset ---
    if isinstance(raw, xr.Dataset):
        for name in ("mask_3d", "mask", "Mask"):
            if name in raw:
                da = raw[name]
                break
        else:
            varnames = list(raw.data_vars)
            if not varnames:
                raise RuntimeError("region_mask returned an empty Dataset with no variables")
            da = raw[varnames[0]]
            if debug:
                logger.debug(
                    f"region_mask Dataset did not contain 'mask_3d'; "
                    f"using variable '{varnames[0]}' instead"
                )
    elif isinstance(raw, xr.DataArray):
        da = raw
    else:
        raise RuntimeError(
            f"region_mask returned unexpected type {type(raw)}; "
            "expected xr.Dataset or xr.DataArray"
        )

    # --- collapse region dimension ---
    for rdim in ("region_id", "region", "regions"):
        if rdim in da.dims:
            if da.sizes[rdim] == 0:
                # FIX: zero-region crash.
                # echoregions returned no matching polygons for this time window.
                # This is valid - it means no EVR polygons overlap the echogram,
                # commonly because aa-evl upstream has already trimmed the time range.
                # Return all-False (nothing inside any region) instead of crashing.
                logger.warning(
                    f"EVR returned 0 matching regions for this echogram "
                    f"(region_id dimension is empty). This typically means the EVR "
                    f"polygon time range does not overlap the current echogram ping_time "
                    f"range - which can happen when aa-evl has already masked the data. "
                    f"Treating as all-False (nothing inside any region). "
                    f"Run with --debug to compare time ranges."
                )
                other_dims = [d for d in da.dims if d != rdim]
                return xr.DataArray(
                    np.zeros([da.sizes[d] for d in other_dims], dtype=bool),
                    dims=other_dims,
                )
            else:
                da = (da.max(rdim) > 0)
            break

    return da.astype(bool)


# Depth bounds handed to er.read_evr(). echoregions' Regions2D.region_mask()
# drops every region that has a vertex outside [max(0, min_depth), max_depth]
# (read_evr defaults: 0 and 1000 m), so with the defaults any region reaching
# deeper than 1000 m silently disappeared from the mask. aa-evr handles depth
# itself before calling region_mask (Echoview's +/-9999.99 sentinels become the
# echogram's top/bottom, vertices are clamped to the echogram's depth range),
# so echoregions' filter must never fire: these bounds lie beyond any ocean
# depth. (echoregions floors the lower bound at 0 m regardless.) The bounds are
# also what echoregions' replace_nan_depth() would use, which aa-evr never calls.
_ER_DEPTH_BOUNDS = {"min_depth": -1.0e7, "max_depth": 1.0e7}


def _build_union_region_mask(
    ds: xr.Dataset,
    evr_files: List[Path],
    var: str,
    time_dim: str,
    depth_dim: str,
    channel_index: int,
    debug: bool,
) -> xr.DataArray:
    """
    Build a boolean union mask aligned to ds[var] on (time_dim, depth_dim).
    True  = inside a region  (values are KEPT).
    False = outside all regions (values become NaN).

    Two modes (auto-detected per EVR file):
      1) Normal echogram EVR (depth+time polygons): echoregions Regions2D.region_mask()
      2) GPS/alongtrack EVR (no usable depth): TIME-ONLY fallback - keep all depths
         for pings whose ping_time falls within any region time span.

    KEY: The returned mask uses the original dataset coordinate labels (not the
    metre-based labels used internally for geometry), so _apply_mask cannot
    produce a silent coordinate mismatch.
    """

    # --- channel stripping ---
    var_da = ds[var]
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

    # --- original coordinate labels (used to rebuild the mask at the end) ---
    orig_time_vals = np.asarray(
        ds[time_dim].values if time_dim in ds.coords else da.coords[time_dim].values
    )
    orig_depth_vals = (
        np.asarray(da.coords[depth_dim].values)
        if depth_dim in da.coords
        else np.arange(da.sizes[depth_dim])
    )

    # --- build surface for echoregions: rename dims to ("ping_time", "depth") ---
    da_for_mask = (
        da.rename({time_dim: "ping_time", depth_dim: "depth"})
        .transpose("ping_time", "depth", ...)
    )

    # Force ping_time to a clean 1-D coordinate vector
    da_for_mask = da_for_mask.assign_coords(ping_time=("ping_time", orig_time_vals))

    # --- resolve metre-valued depth coordinate for geometry ---
    depth_vals_m = None

    def _pick_channel(x):
        if x is None:
            return None
        if "channel" in x.dims and "channel" in var_da.dims:
            try:
                x = x.isel(channel=channel_index)
                if "channel" in x.coords:
                    try:
                        x = x.drop_vars("channel")
                    except Exception:
                        pass
            except Exception:
                return None
        return x

    for cname in ("echo_range", "depth"):
        x = _pick_channel(ds[cname]) if cname in ds else None
        if x is None and cname in var_da.coords:
            x = _pick_channel(var_da.coords[cname])
        if x is None:
            continue
        if depth_dim in x.dims and time_dim not in x.dims:
            depth_vals_m = np.asarray(x.values)
            break
        if time_dim in x.dims and depth_dim in x.dims:
            depth_vals_m = np.asarray(x.isel({time_dim: 0}).values)
            break

    if depth_vals_m is not None:
        da_for_mask = da_for_mask.assign_coords(depth=("depth", depth_vals_m))
        ech_depth_min = float(np.nanmin(depth_vals_m))
        ech_depth_max = float(np.nanmax(depth_vals_m))
    else:
        dtmp = np.asarray(da_for_mask["depth"].values)
        ech_depth_min = float(np.nanmin(dtmp))
        ech_depth_max = float(np.nanmax(dtmp))

    if debug:
        logger.debug(f"Echogram depth range: {ech_depth_min:.2f} -> {ech_depth_max:.2f} m")
        logger.debug(f"Echogram time range:  {orig_time_vals.min()} -> {orig_time_vals.max()}")
        logger.debug(f"da_for_mask shape: {da_for_mask.shape}, dims: {da_for_mask.dims}")

    # dask safety
    try:
        da_for_mask = da_for_mask.compute()
    except Exception:
        pass

    union_mask = None

    for evr_path in evr_files:
        # Wide-open depth bounds: echoregions must not filter regions by depth
        # (see _ER_DEPTH_BOUNDS); sentinels and clamping are handled below.
        regions2d = er.read_evr(str(evr_path), **_ER_DEPTH_BOUNDS)

        # --- get underlying dataframe ---
        df = None
        if hasattr(regions2d, "to_dataframe"):
            try:
                df = regions2d.to_dataframe()
            except Exception:
                pass
        if df is None and hasattr(regions2d, "data"):
            try:
                df = regions2d.data
            except Exception:
                pass
        if df is None:
            raise RuntimeError(
                f"Could not access Regions2D dataframe for {evr_path}"
            )

        df = df.copy()

        if debug:
            logger.debug(
                f"{evr_path.name}: {len(df)} regions, columns={list(df.columns)}"
            )

        # --- normalise TIME lists to datetime64 ---
        if "time" in df.columns:
            new_time = []
            for t_list in df["time"]:
                try:
                    t = pd.to_datetime(list(t_list), errors="coerce").dropna()
                    new_time.append([np.datetime64(x) for x in t])
                except Exception:
                    new_time.append([])
            df["time"] = new_time

        # --- normalise DEPTH lists: replace sentinels, clip to echogram range ---
        if "depth" in df.columns:
            # Capture raw depth statistics *before* clamping so we can detect
            # the case where the polygon was drawn against an integer-index
            # axis (e.g. range_sample) and gets squashed to a single point.
            raw_depth_min = float("inf")
            raw_depth_max = float("-inf")
            raw_depth_count = 0
            for d_list in df["depth"]:
                for d in list(d_list):
                    try:
                        x = float(d)
                        if np.isfinite(x) and abs(x) < 9000:
                            raw_depth_count += 1
                            if x < raw_depth_min: raw_depth_min = x
                            if x > raw_depth_max: raw_depth_max = x
                    except Exception:
                        pass

            # Detect catastrophic clamp: polygon depths entirely outside
            # the echogram metre range. Almost always means the EVR was
            # drawn against a range_sample (integer index) axis. In that
            # case, DO NOT clamp — leave the polygon as-is so echoregions
            # filters it out cleanly and we can emit a clear diagnostic.
            # Clamping would silently collapse all vertices onto a single
            # edge value, producing a degenerate horizontal line that
            # masks nothing but reports no error.
            polygon_outside_echogram = (
                raw_depth_count > 0
                and (raw_depth_min > ech_depth_max or raw_depth_max < ech_depth_min)
            )

            new_depth = []
            for d_list in df["depth"]:
                fixed = []
                for d in list(d_list):
                    try:
                        x = float(d)
                    except Exception:
                        fixed.append(np.nan)
                        continue
                    if np.isfinite(x):
                        # Always replace Echoview sentinel values (±9999.99)
                        # which represent "to the surface" / "to the seafloor"
                        # boundaries — those genuinely should clamp.
                        if x <= -9000:
                            x = ech_depth_min
                        elif x >= 9000:
                            x = ech_depth_max
                        # For NORMAL polygon vertices, only clamp if the
                        # polygon as a whole is at least partly inside the
                        # echogram. If it's entirely outside, preserve the
                        # original values so the failure is visible.
                        elif not polygon_outside_echogram:
                            x = min(max(x, ech_depth_min), ech_depth_max)
                    fixed.append(x)
                new_depth.append(fixed)
            df["depth"] = new_depth

            # --- diagnostic logging ---
            if raw_depth_count > 0:
                if debug:
                    logger.debug(
                        f"{evr_path.name}: EVR depth range (raw): "
                        f"{raw_depth_min:.2f} -> {raw_depth_max:.2f} "
                        f"(echogram range: {ech_depth_min:.2f} -> {ech_depth_max:.2f} m)"
                    )
                if polygon_outside_echogram:
                    logger.warning(
                        f"{evr_path.name}: polygon depths ({raw_depth_min:.2f} -> "
                        f"{raw_depth_max:.2f}) are entirely OUTSIDE the echogram "
                        f"depth range ({ech_depth_min:.2f} -> {ech_depth_max:.2f} m). "
                        f"This EVR cannot mask any cells in this echogram.\n"
                        f"  Most common cause: the EVR was created from an aa-plot "
                        f"HTML whose y-axis was 'range_sample' (integer indices, 0 "
                        f"to ~N) instead of metre-valued depth. The polygon depths "
                        f"in the EVR are sample indices, not metres.\n"
                        f"  Fix: regenerate the aa-plot HTML against an input file "
                        f"that contains a 'depth' or 'echo_range' axis (run aa-depth "
                        f"first), or pass --y echo_range / --y depth explicitly to "
                        f"aa-plot. Then redraw and re-export the EVR."
                    )

        # --- close polygons ---
        if "time" in df.columns and "depth" in df.columns:
            closed_t, closed_d = [], []
            for t_list, d_list in zip(df["time"], df["depth"]):
                try:
                    if len(t_list) and len(d_list) and (
                        t_list[0] != t_list[-1] or d_list[0] != d_list[-1]
                    ):
                        t_list = list(t_list) + [t_list[0]]
                        d_list = list(d_list) + [d_list[0]]
                except Exception:
                    pass
                closed_t.append(t_list)
                closed_d.append(d_list)
            df["time"] = closed_t
            df["depth"] = closed_d

        try:
            regions2d.data = df
        except Exception:
            pass

        # --- detect whether EVR has any usable depth points ---
        has_depth = False
        if "depth" in df.columns:
            for d_list in df["depth"]:
                if any(
                    isinstance(x, (int, float, np.floating)) and np.isfinite(x)
                    for x in d_list
                ):
                    has_depth = True
                    break

        # -------------------------------------------------------
        # GPS / alongtrack fallback: TIME-ONLY mask
        # -------------------------------------------------------
        if not has_depth and "time" in df.columns:
            mask_time = np.zeros(orig_time_vals.shape, dtype=bool)
            for t_list in df["time"]:
                if not t_list:
                    continue
                try:
                    tmin = np.min(t_list)
                    tmax = np.max(t_list)
                    mask_time |= (orig_time_vals >= tmin) & (orig_time_vals <= tmax)
                except Exception:
                    continue

            file_union = xr.DataArray(
                np.broadcast_to(
                    mask_time[:, np.newaxis],
                    (len(orig_time_vals), da.sizes[depth_dim]),
                ).copy(),
                dims=("ping_time", "depth"),
            )
            if debug:
                logger.debug(
                    f"{evr_path.name}: time-only mask - "
                    f"inside pings={int(mask_time.sum())}/{mask_time.size}"
                )

        # -------------------------------------------------------
        # Normal echogram EVR: depth-time polygon mask
        # -------------------------------------------------------
        else:
            # Pad region endpoint times slightly to avoid boundary misses
            pad = pd.Timedelta(seconds=1)
            new_times = []
            for t_list in df["time"]:
                t = pd.to_datetime(list(t_list), errors="coerce").dropna()
                if len(t) == 0:
                    new_times.append(list(t_list))
                    continue
                tmin, tmax = t.min(), t.max()
                out = []
                for ti in t:
                    if ti == tmin:
                        out.append(np.datetime64(ti - pad))
                    elif ti == tmax:
                        out.append(np.datetime64(ti + pad))
                    else:
                        out.append(np.datetime64(ti))
                new_times.append(out)
            df["time"] = new_times
            try:
                regions2d.data = df
            except Exception:
                pass

            if debug:
                all_evr_times = []
                for t_list in df["time"]:
                    all_evr_times.extend(t_list)
                if all_evr_times:
                    logger.debug(
                        f"{evr_path.name}: EVR time range  "
                        f"{np.min(all_evr_times)} -> {np.max(all_evr_times)}"
                    )

            region_mask_result = regions2d.region_mask(da_for_mask, collapse_to_2d=False)

            file_union = _extract_region_mask_union(
                region_mask_result,
                ping_time_size=len(orig_time_vals),
                depth_size=da.sizes[depth_dim],
                debug=debug,
            )

            if set(file_union.dims) >= {"ping_time", "depth"}:
                file_union = file_union.transpose("ping_time", "depth")

            if debug:
                n_in = int(file_union.values.sum())
                logger.debug(
                    f"{evr_path.name}: polygon mask - "
                    f"inside={n_in}/{file_union.size} cells "
                    f"({100 * n_in / max(file_union.size, 1):.1f}%)"
                )

        union_mask = file_union if union_mask is None else (union_mask | file_union)

    if union_mask is None:
        raise RuntimeError("No region mask produced.")

    # --- rebuild mask with original dataset coordinate labels ---
    # Prevents the silent coordinate-mismatch bug where metre-based depth labels
    # on the mask fail to reindex against range_sample integer indices, producing
    # a full-NaN mask that bool-evaluates to all-True and keeps everything.
    mask_values = np.asarray(union_mask.values, dtype=bool)
    if mask_values.shape != (len(orig_time_vals), da.sizes[depth_dim]):
        mask_values = mask_values.reshape(len(orig_time_vals), da.sizes[depth_dim])

    return xr.DataArray(
        mask_values,
        dims=(time_dim, depth_dim),
        coords={
            time_dim: orig_time_vals,
            depth_dim: orig_depth_vals,
        },
    )


def _apply_mask(
    ds: xr.Dataset,
    mask: xr.DataArray,
    time_dim: str,
    depth_dim: str,
    write_mask: bool,
) -> xr.Dataset:
    """
    Apply boolean mask to all data variables containing (time_dim, depth_dim).

    Uses positional (coordinate-stripped) masking as the sole strategy.
    By construction the mask has the same shape as the data along (time_dim,
    depth_dim), so stripping coordinate labels before broadcasting guarantees
    no reindex mismatch can silently fill the mask with NaN - which in numpy
    evaluates as True and causes xr.where to keep all original values unchanged.

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

    mask_np = np.asarray(mask.transpose(time_dim, depth_dim).values, dtype=bool)

    for name, da in list(ds_out.data_vars.items()):
        if name in AXIS_VARS:
            continue  # Skip axis variables — masking them breaks the coord system
        if (time_dim in da.dims) and (depth_dim in da.dims):
            m_bare = xr.DataArray(mask_np, dims=(time_dim, depth_dim))
            m_broadcast = m_bare.broadcast_like(da)
            ds_out[name] = xr.where(m_broadcast, da, np.nan)

    if write_mask:
        ds_out["region_mask"] = mask.astype("int8")
        ds_out["region_mask"].attrs["long_name"] = (
            "Union region mask from EVR files (1=inside, 0=outside)"
        )

    return ds_out


# ---------------------------
# Scientific parameters actually used
# ---------------------------

def _resolve_masking(nc_path: Path, var: Optional[str], time_dim: Optional[str],
                     depth_dim: Optional[str], channel_index: int) -> dict:
    """The variable, dimensions and channel the mask will actually use.

    Reads only the file's metadata. These resolved values (not the flags as
    typed) enter the hash, so `--var Sv` and an auto-detected Sv are the same
    product. The channel index only matters when the variable has a channel
    dimension (see _build_union_region_mask); otherwise it is recorded as None.
    """
    with xr.open_dataset(nc_path) as ds:
        rvar = _resolve_var(ds, var)
        tdim, ddim = _infer_dims(ds, var=rvar, time_dim=time_dim, depth_dim=depth_dim)
        has_channel = "channel" in ds[rvar].dims
    return {
        "var": rvar,
        "time_dim": tdim,
        "depth_dim": ddim,
        "channel_index": channel_index if has_channel else None,
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
    evr_files: List[Path],
    output_path: Path,
    var: str,
    time_dim: Optional[str],
    depth_dim: Optional[str],
    channel_index: int,
    write_mask: bool,
    fail_empty: bool,
    debug: bool,
    evr_sources: Optional[List[str]] = None,
) -> Optional[Path]:
    """Mask one file and write it to output_path. None: nothing written.

    Output-exists / --overwrite and the refuse-to-overwrite-the-input guard are
    decided by the caller, before this runs (reuse comes first).
    """
    # Read into memory and release the file handle so we can write into the
    # same directory without xarray holding a read lock.
    with xr.open_dataset(input_path) as ds_in:
        ds = ds_in.load()

    # Resolve --var: explicit value if given, else auto-detect from a
    # candidate list (Sv, Sv_clean, MVBS, TS, NASC) that mirrors aa-graph
    # and aa-plot.  Lets `aa-evr file_TS.nc --evr ...` work without
    # forcing the user to add --var TS by hand.
    var = _resolve_var(ds, var)
    if debug:
        logger.debug(f"Resolved variable: '{var}'")

    tdim, ddim = _infer_dims(ds, var=var, time_dim=time_dim, depth_dim=depth_dim)
    if debug:
        logger.debug(
            f"Using dims: time_dim='{tdim}', depth_dim='{ddim}', "
            f"{var}.dims={ds[var].dims}, shape={ds[var].shape}"
        )

    mask = _build_union_region_mask(
        ds=ds,
        evr_files=evr_files,
        var=var,
        time_dim=tdim,
        depth_dim=ddim,
        channel_index=channel_index,
        debug=debug,
    )

    inside = int(mask.values.sum())
    total = int(mask.size)
    pct = 100.0 * inside / max(total, 1)
    logger.info(
        f"Union mask coverage: {inside}/{total} cells inside regions ({pct:.1f}%)"
    )

    if inside == 0:
        logger.error(
            "Union mask is EMPTY (0 cells inside). Output will be all-NaN.\n"
            "Possible causes:\n"
            "  - EVR time range does not overlap the NetCDF ping_time range\n"
            "  - EVR depth polygons are outside the echogram depth range\n"
            "  - The EVR is a GPS/track file, not an echogram region export\n"
            "Run with --debug to see time/depth ranges for diagnosis."
        )
        if fail_empty:
            return None

    if inside == total:
        logger.warning(
            "Union mask is FULL (all cells inside). Output will be identical to "
            "input - no data will be masked out. Check EVR polygon boundaries."
        )

    ds_out = _apply_mask(
        ds, mask=mask, time_dim=tdim, depth_dim=ddim, write_mask=write_mask
    )
    ds_out.attrs["aa_tool"] = "aa-evr"
    ds_out.attrs["aa_evr_files"] = ",".join(
        str(s) for s in (evr_sources if evr_sources is not None else evr_files)
    )

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    ds_out.to_netcdf(output_path)

    return output_path


def _run_one(token: str, args, regions: List["_RegionFile"],
             evr_sources: List[str]) -> Optional[str]:
    """One input -> one product. Returns the printed target, or None on failure."""
    run = Run(SPEC, args)
    src = run.input(token)
    _register_regions(run, regions)

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
            evr_files=[r.local for r in regions],
            output_path=out.local,
            var=resolved["var"],
            time_dim=resolved["time_dim"],
            depth_dim=resolved["depth_dim"],
            channel_index=args.channel_index,
            write_mask=args.write_mask,
            fail_empty=args.fail_empty,
            debug=args.debug,
            evr_sources=evr_sources,
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


def _evr_mode(args) -> int:
    tokens = stdio.many_inputs(args.input_paths, SPEC.name)   # empty stdin: exit 1

    # --evr may be a local path OR a remote URI (gs://, s3://, http(s)://).
    # Resolved once for the whole batch; each file's CONTENT identifies it.
    regions_given, err = _resolve_region_files([str(s) for s in args.evr])
    if regions_given is None:
        logger.error(err)
        return 2
    regions = _canonical_regions(regions_given)

    # Provenance attribute written into the output NetCDF: the sources as given
    # (remote URI, or the resolved local path), unchanged from before.
    evr_sources = [r.shown for r in regions_given]

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
        logger.debug(f"\naa-evr args:\n{pprint.pformat(vars(args))}")
        logger.debug(f"Region files (canonical order): {[r.raw for r in regions]}")

    any_fail = False
    for token in tokens:
        try:
            if not _run_one(token, args, regions, evr_sources):
                any_fail = True
        except SystemExit:
            # The core stops on this input (e.g. a missing gs:// object) after
            # printing why; the rest of the batch still runs.
            any_fail = True
        except Exception as e:
            any_fail = True
            logger.exception(f"Error processing {token}: {e}")

    return 1 if any_fail else 0


# ---------------------------
# Drawing mode
# ---------------------------

def _set_nc_attrs(path: Path, attrs: dict) -> None:
    import netCDF4

    with netCDF4.Dataset(str(path), "a") as nc:
        for k, v in attrs.items():
            nc.setncattr(k, v)


def _draw_mode(args) -> int:
    try:
        from aalibrary.utils.region_draw import run_drawing_mode
    except ImportError as exc:
        logger.error(
            f"Could not import region_draw (drawing mode): {exc}\n"
            "Ensure region_draw.py is in the same directory as aa_evr.py, "
            "or on PYTHONPATH."
        )
        return 2

    tokens = stdio.many_inputs(args.input_paths, SPEC.name)   # empty stdin: exit 1
    if len(tokens) > 1:
        logger.warning(
            f"Drawing mode processes one file at a time; "
            f"using first input: {tokens[0]}"
        )
    token = tokens[0]
    if not uris.is_gcs(token):
        p = Path(uris.from_file_uri(token)).expanduser().resolve()
        if not p.exists():
            logger.error(f"Input file not found: {p}")
            return 1
        token = str(p)

    ignored = [flag for flag, value in (("-o", args.output_path), ("--suffix", args.suffix),
                                        ("--dest", args.dest), ("--base", args.base))
               if value]
    if ignored:
        logger.warning(f"Drawing mode ignores {', '.join(ignored)} (EVR mode only).")

    # A drawing is never reused: nothing identifies it before it is drawn.
    run = Run(DRAW_SPEC, args)
    src = run.input(token)
    nc_path = src.local

    # Resolve --var: explicit value if given, else auto-detect by
    # opening the file to inspect data_vars.  Mirrors the EVR-mode
    # path so `aa-evr file_TS.nc --name foo.evr` works without
    # forcing the user to add --var TS by hand.
    try:
        resolved = _resolve_masking(nc_path, args.var, args.time_dim, args.depth_dim,
                                    args.channel_index)
    except Exception as exc:
        logger.error(f"Could not resolve variable in {nc_path}: {exc}")
        return 1
    resolved_var = resolved["var"]
    # region_draw also uses the index to pick a channel of echo_range/depth,
    # whether or not --var has a channel dimension: record it as given.
    resolved["channel_index"] = args.channel_index

    evr_name = args.name or (Path(src.name).with_suffix("").name + "_regions.evr")
    if args.out_dir:
        out_dir = Path(args.out_dir).expanduser().resolve()
    elif src.via != "local":
        out_dir = Path.cwd()        # never write beside a cached gs:// copy
    else:
        out_dir = None

    if args.debug:
        logger.debug(
            f"\naa-evr drawing mode args:\n"
            f"  nc_path={nc_path}\n"
            f"  evr_name={evr_name}\n"
            f"  out_dir={out_dir}\n"
            f"  var={resolved_var}, port={args.port}"
        )

    result = run_drawing_mode(
        nc_path=nc_path,
        evr_name=evr_name,
        out_dir=out_dir,
        var=resolved_var,
        time_dim=resolved["time_dim"],     # same inference as region_draw's,
        depth_dim=resolved["depth_dim"],   # plus echo_range (aa-mvbs output)
        channel_index=args.channel_index,
        overwrite=args.overwrite,
        port=args.port,
        debug=args.debug,
    )
    if not result:
        return 1

    # Provenance for the drawn product. The drawn .evr (when written) is its
    # regions input; a one-off drawing id keeps two different drawings from
    # ever sharing a hash (the .evr stores whole seconds, and may be missing).
    result = Path(result)
    with xr.open_dataset(result) as ds_done:
        written = str(ds_done.attrs.get("aa_evr_files", "not_written"))
        tool_attr = str(ds_done.attrs.get("aa_tool", "aa-evr-draw"))
    if written != "not_written" and Path(written).is_file():
        run.param_file(written, role="regions")
    # stage=False: region_draw has already written the target itself, so the
    # core embeds into it rather than expecting a temp file to rename.
    out = run.plan(ext=".nc", explicit=str(result), variant="draw", kind=_kind_of(src),
                   extra_params=dict(resolved, drawing=uuid.uuid4().hex), stage=False)
    run.finish(out, emit=False)
    # The core stamps aa_tool with the step's tool; keep the attribute
    # region_draw has always written for drawn products.
    _set_nc_attrs(result, {"aa_tool": tool_attr})

    logger.success("Piping saved .nc path to stdout ⟶")
    stdio.emit(str(result))
    return 0


# ---------------------------
# Entry point
# ---------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="aa-evr",
        description=(
            "Mask echogram NetCDF (.nc) using Echoview EVR region files, "
            "or draw new regions interactively (omit --evr)."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        add_help=False,
    )
    parser.add_argument(
        "input_paths", nargs="*", type=str,
        help="Input .nc/.netcdf4 paths or gs:// URIs (or read from stdin).",
    )

    # -- EVR mode --
    parser.add_argument(
        "--evr", required=False, default=None, nargs="+", type=str, metavar="EVR",
        help=(
            "One or more .evr sources: local paths and/or remote URIs such as "
            "gs://bucket/regions.evr (omit to enter interactive drawing mode)."
        ),
    )

    # -- Drawing mode --
    parser.add_argument(
        "--name", type=str, default=None, dest="name",
        help=(
            "Drawing mode: EVR output filename "
            "(default: <input_stem>_regions.evr).  "
            "May include or omit the .evr extension."
        ),
    )
    parser.add_argument(
        "--port", type=int, default=5006,
        help="Drawing mode: Bokeh server port (default: 5006; auto-increments if busy).",
    )

    # -- Shared output options --
    parser.add_argument(
        "-o", "--output-path", dest="output_path", type=str,
        help="Output path or gs:// URI (only valid for a single input file, EVR mode only).",
    )
    parser.add_argument(
        "--out-dir", type=str,
        help="Output directory (for pipelines / multiple inputs).",
    )
    parser.add_argument(
        "--suffix", type=str, default=None,
        help=("Name outputs <input stem><SUFFIX>.nc (EVR mode only). Default: "
              "<base>_<hash8>.nc; AA_NAMING=legacy: suffix _evr."),
    )
    parser.add_argument(
        "--overwrite", action="store_true",
        help="Replace an existing output that is a different product.",
    )

    # -- Masking options (shared) --
    parser.add_argument(
        "--var", type=str, default=None,
        help=(
            "Variable to mask (default: auto-detect — first of "
            "Sv, Sv_clean, MVBS, TS, NASC found in the file). "
            "Pass explicitly to override."
        ),
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
        help="Channel index for mask building (default: 0).",
    )
    parser.add_argument(
        "--write-mask", action="store_true",
        help="Write 'region_mask' variable to output NetCDF (EVR mode only).",
    )
    parser.add_argument(
        "--fail-empty", action="store_true",
        help="Exit non-zero if union mask is empty (EVR mode only).",
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

    # ==============================================================
    # Route: DRAWING MODE  (--evr not provided)
    # ==============================================================
    if not args.evr:
        return _draw_mode(args)

    # ==============================================================
    # Route: EVR MODE  (--evr provided)
    # ==============================================================
    return _evr_mode(args)


if __name__ == "__main__":
    raise SystemExit(main())
