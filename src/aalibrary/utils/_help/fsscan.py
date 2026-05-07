"""Filesystem discovery: walk the user's home directory and collect names of
acoustic-relevant files (.raw, .nc, .netcdf4, .evr, .evl). The list is
injected into the planner's system prompt so it can refer to real filenames
when constructing pipelines.

Design notes:
  - Names ONLY. We never read file contents. This keeps prompts short and
    sidesteps any privacy concerns about feeding YAMLs/notes/etc to Vertex.
  - Walks the whole home directory by default, BUT prunes aggressively:
    hidden dirs (.git, .cache, .venv...), build artifacts (node_modules,
    __pycache__, site-packages), and anything in the user's exclude list.
  - Cached. Walking is fast (1-2s on a typical home dir) but doing it on
    every query is wasteful. Cache lives at config_dir/file_index.json with
    a TTL controlled by `file_index_ttl_seconds` in the toml.
  - Caps at MAX_FILES per category to prevent the prompt from ballooning
    on really busy disks.
  - Always includes CWD even if it's outside the scan root, since that's
    the highest-value context.
"""
from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path
from typing import Iterable, Optional


# Acoustic-relevant file types. Everything else gets ignored.
RAW_EXTS = {".raw"}
NC_EXTS = {".nc", ".netcdf4"}
ECHOVIEW_EXTS = {".evr", ".evl"}
ALL_EXTS = RAW_EXTS | NC_EXTS | ECHOVIEW_EXTS

# Directory names to skip during the walk. These are the universally-noisy
# folders -- hidden dotdirs (incl. .git/.cache/.venv), build artifacts,
# package caches, and known never-acoustic content.
PRUNE_DIRS = frozenset({
    "node_modules",
    "__pycache__",
    "site-packages",
    "venv", ".venv", "env",
    "build", "dist", ".tox",
    ".git", ".hg", ".svn",
    ".cache", ".local", ".config",
    ".npm", ".cargo", ".rustup",
    ".gradle", ".m2",
    "Library",        # macOS user Library
    "AppData",        # Windows
    "Trash", ".Trash",
    "snap",
})

# Per-category caps. The whole-home walk on a NOAA workstation can hit
# thousands of .raw files; we don't want them all in the prompt.
MAX_FILES_PER_EXT = 100
DEFAULT_TTL = 300   # seconds


def _should_skip_dir(name: str) -> bool:
    if name.startswith("."):
        return True
    return name in PRUNE_DIRS


def _walk(root: Path, extra_excludes: list[str]) -> dict[str, list[str]]:
    """Walk `root`, returning {extension: [absolute paths]} for matching files.

    extra_excludes is a list of directory NAMES (not paths) that the user
    wants skipped on top of PRUNE_DIRS.
    """
    out: dict[str, list[str]] = {ext: [] for ext in ALL_EXTS}
    skip = PRUNE_DIRS | set(extra_excludes)
    caps_hit: set[str] = set()

    # Use os.walk; it's the fastest pure-Python tree traversal and lets us
    # mutate dirnames in-place to prune.
    for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
        # Prune in-place. Dotdirs and known-noise dirs are skipped.
        dirnames[:] = [
            d for d in dirnames
            if not d.startswith(".") and d not in skip
        ]
        for f in filenames:
            ext = os.path.splitext(f)[1].lower()
            if ext not in ALL_EXTS or ext in caps_hit:
                continue
            if len(out[ext]) >= MAX_FILES_PER_EXT:
                caps_hit.add(ext)
                continue
            out[ext].append(os.path.join(dirpath, f))
        if len(caps_hit) == len(ALL_EXTS):
            break  # all categories full
    return out


# -- caching ----------------------------------------------------------------

def _cache_path(config_dir: Path) -> Path:
    return config_dir / "file_index.json"


def _load_cache(config_dir: Path) -> Optional[dict]:
    p = _cache_path(config_dir)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _save_cache(config_dir: Path, payload: dict) -> None:
    config_dir.mkdir(parents=True, exist_ok=True)
    try:
        _cache_path(config_dir).write_text(json.dumps(payload), encoding="utf-8")
    except OSError:
        pass


def _is_fresh(cache: dict, ttl: int) -> bool:
    ts = cache.get("timestamp", 0)
    return (time.time() - ts) < ttl


# -- public API -------------------------------------------------------------

def build_index(scan_root: Path, config_dir: Path,
                exclude_dirs: list[str]) -> dict[str, list[str]]:
    """Walk scan_root, save to cache, return the index."""
    files_by_ext = _walk(scan_root, exclude_dirs)
    payload = {
        "timestamp": time.time(),
        "scan_root": str(scan_root),
        "files_by_ext": files_by_ext,
    }
    _save_cache(config_dir, payload)
    return files_by_ext


def get_index(scan_root: Path, config_dir: Path,
              exclude_dirs: list[str], ttl: int = DEFAULT_TTL,
              ) -> dict[str, list[str]]:
    """Return the cached index if fresh, otherwise rebuild it."""
    cache = _load_cache(config_dir)
    if (cache
            and cache.get("scan_root") == str(scan_root)
            and _is_fresh(cache, ttl)):
        return cache["files_by_ext"]
    return build_index(scan_root, config_dir, exclude_dirs)


# -- prompt section ---------------------------------------------------------

def _extract_paths_from_question(question: str) -> list[Path]:
    """Best-effort filesystem-path extraction. We just need to know if the
    user mentioned a directory so we can prioritize it in the listing.
    """
    candidates: set[str] = set()
    for m in re.finditer(r"(~/|/|[A-Za-z]:\\\\)\S+", question):
        candidates.add(m.group(0).rstrip(".,;:!?'\")"))
    ext_pattern = "|".join(re.escape(e[1:]) for e in ALL_EXTS)
    for m in re.finditer(rf"[\w./~-]+\.(?:{ext_pattern})\b", question):
        candidates.add(m.group(0))
    paths: list[Path] = []
    seen: set[Path] = set()
    for c in candidates:
        try:
            p = Path(c).expanduser()
        except (ValueError, OSError):
            continue
        if p in seen:
            continue
        seen.add(p)
        paths.append(p)
    return paths


def _format_section(label: str, paths: list[str], cap: int = 30) -> str:
    if not paths:
        return ""
    lines = [f"  {label} ({len(paths)} found):"]
    for p in paths[:cap]:
        lines.append(f"    - {p}")
    if len(paths) > cap:
        lines.append(f"    (+{len(paths) - cap} more, not listed)")
    return "\n".join(lines) + "\n"


def scan_for_planner(question: str, config_dir: Path,
                     scan_root: Optional[Path] = None,
                     exclude_dirs: Optional[list[str]] = None,
                     ttl: int = DEFAULT_TTL) -> str:
    """Build a string block for the planner's system prompt.

    Strategy:
      1. CWD listing (always, fresh, cheap).
      2. Cached home-tree index (full home dir, names only, scoped by ext).
      3. If the user mentioned a directory, list its acoustic files inline.
    """
    scan_root = scan_root or Path.home()
    exclude_dirs = exclude_dirs or []
    sections: list[str] = []

    # 1. CWD: list acoustic files in the user's current directory. We do this
    #    without the cache because CWD can change between calls.
    cwd = Path.cwd()
    cwd_files: dict[str, list[str]] = {ext: [] for ext in ALL_EXTS}
    try:
        for entry in os.listdir(cwd):
            ext = os.path.splitext(entry)[1].lower()
            if ext in ALL_EXTS:
                cwd_files[ext].append(entry)  # bare name in CWD context
    except (OSError, PermissionError):
        pass

    cwd_lines: list[str] = []
    cwd_lines.append(_format_section("raw acoustic files",
                                     sorted(cwd_files[".raw"])))
    cwd_lines.append(_format_section(
        "netcdf files",
        sorted(cwd_files[".nc"] + cwd_files[".netcdf4"])))
    cwd_lines.append(_format_section(
        "echoview files",
        sorted(cwd_files[".evr"] + cwd_files[".evl"])))
    cwd_body = "".join(cwd_lines)
    if cwd_body:
        sections.append(f"# current working directory: {cwd}\n{cwd_body}")

    # 2. Whole-home index (cached). We only show this when the user is NOT
    #    in their home dir or a subdir of it -- if they're already there,
    #    section 1 above would double-count. Otherwise show a deduped list.
    try:
        index = get_index(scan_root, config_dir, exclude_dirs, ttl)
    except Exception as e:
        index = {ext: [] for ext in ALL_EXTS}
        # Don't fail the call -- discovery is opportunistic.
        import sys
        sys.stderr.write(f"aa-help: file discovery failed: {e}\n")

    # Filter out anything already in CWD (those are listed by bare name above).
    cwd_str = str(cwd)
    home_lines: list[str] = []
    home_lines.append(_format_section(
        "raw files elsewhere",
        sorted(p for p in index.get(".raw", []) if not p.startswith(cwd_str))))
    home_lines.append(_format_section(
        "netcdf files elsewhere",
        sorted(p for p in index.get(".nc", []) + index.get(".netcdf4", [])
               if not p.startswith(cwd_str))))
    home_lines.append(_format_section(
        "echoview files elsewhere",
        sorted(p for p in index.get(".evr", []) + index.get(".evl", [])
               if not p.startswith(cwd_str))))
    home_body = "".join(home_lines)
    if home_body:
        sections.append(f"# home directory ({scan_root}, cached):\n{home_body}")

    # 3. If the question mentions a path, surface it specifically. We don't
    #    re-walk -- we just filter the existing index for matches.
    mentioned = _extract_paths_from_question(question)
    if mentioned:
        hit_lines: list[str] = []
        all_paths = (index.get(".raw", [])
                     + index.get(".nc", []) + index.get(".netcdf4", [])
                     + index.get(".evr", []) + index.get(".evl", []))
        for ref in mentioned:
            try:
                ref_str = str(ref.expanduser().resolve())
            except (OSError, ValueError):
                continue
            matches = [p for p in all_paths if ref_str in p]
            if matches:
                hit_lines.append(_format_section(
                    f"matches for '{ref}'", sorted(matches), cap=20))
        if hit_lines:
            sections.append(f"# user-mentioned paths\n{''.join(hit_lines)}")

    if not sections:
        return ""

    header = (
        "\n\n=== FILE DISCOVERY ===\n"
        "Acoustic data files visible to the user. Reference real filenames\n"
        "by their exact path when constructing pipelines. The 'cached' note\n"
        "means this listing may be slightly stale (TTL-based refresh).\n\n"
    )
    return header + "".join(sections)


def stats(config_dir: Path) -> dict:
    """Summary of the cached file index, used by `aa-help --files-stats`."""
    cache = _load_cache(config_dir)
    if not cache:
        return {"cached": False}
    fbe = cache.get("files_by_ext", {})
    return {
        "cached": True,
        "scan_root": cache.get("scan_root"),
        "age_seconds": int(time.time() - cache.get("timestamp", 0)),
        "raw": len(fbe.get(".raw", [])),
        "nc": len(fbe.get(".nc", [])) + len(fbe.get(".netcdf4", [])),
        "echoview": len(fbe.get(".evr", [])) + len(fbe.get(".evl", [])),
    }
