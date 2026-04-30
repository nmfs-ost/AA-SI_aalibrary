"""Assemble the system prompt from a base prompt + library docs + knowledge dirs.

This is the "training" surface. The model sees:
  1. The hand-written base prompt at prompts/system.md
  2. Every supported file under the installed `aalibrary` package (READMEs, modules)
  3. Every supported file under any `knowledge_dirs` paths from the user's config
  4. Free-form `extra_system_prompt` text from the config

To teach aa-help something new, you either edit prompts/system.md (for behavior
rules) or drop reference files into a knowledge_dir (for facts and examples).
"""
from __future__ import annotations

from importlib import resources
from pathlib import Path

from .config import Settings


_KNOWLEDGE_EXTS = {".md", ".txt", ".rst", ".py", ".toml"}
_MAX_FILE_BYTES = 200_000        # cap per file
_MAX_TOTAL_BYTES = 1_500_000     # ~few hundred K tokens; safe for 1M context models


def _read_base_prompt() -> str:
    try:
        return (resources.files(__package__)
                .joinpath("prompts/system.md")
                .read_text(encoding="utf-8"))
    except (FileNotFoundError, ModuleNotFoundError, AttributeError):
        return "You are aa-help, an assistant for the aalibrary active-acoustics toolkit."


def _gather_files(roots: list[Path]) -> list[Path]:
    seen: set[Path] = set()
    out: list[Path] = []
    for root in roots:
        if not root.exists():
            continue
        if root.is_file():
            if root.suffix.lower() in _KNOWLEDGE_EXTS and root not in seen:
                seen.add(root)
                out.append(root)
            continue
        for p in sorted(root.rglob("*")):
            if (p.is_file()
                    and p.suffix.lower() in _KNOWLEDGE_EXTS
                    and p not in seen):
                seen.add(p)
                out.append(p)
    return out


def _bundle_files(files: list[Path]) -> str:
    parts: list[str] = []
    total = 0
    for f in files:
        try:
            text = f.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        if len(text) > _MAX_FILE_BYTES:
            text = text[:_MAX_FILE_BYTES] + f"\n\n[...truncated, {len(text):,} bytes total]"
        block = f"\n\n----- FILE: {f} -----\n{text}\n"
        if total + len(block) > _MAX_TOTAL_BYTES:
            parts.append(f"\n\n[Knowledge bundle truncated at {_MAX_TOTAL_BYTES:,} bytes]\n")
            break
        parts.append(block)
        total += len(block)
    return "".join(parts)


def _library_root() -> Path | None:
    try:
        root = resources.files("aalibrary")
        return Path(str(root))
    except (ModuleNotFoundError, TypeError, AttributeError):
        return None


def build_system_prompt(settings: Settings) -> str:
    sections: list[str] = [_read_base_prompt()]

    roots: list[Path] = []
    lib_root = _library_root()
    if lib_root is not None:
        roots.append(lib_root)
    roots.extend(Path(p).expanduser() for p in settings.knowledge_dirs)

    bundled = _bundle_files(_gather_files(roots))
    if bundled:
        sections.append("\n\n=== KNOWLEDGE BASE ===\n" + bundled)

    if settings.extra_system_prompt.strip():
        sections.append("\n\n=== USER NOTES ===\n"
                        + settings.extra_system_prompt.strip())

    return "".join(sections)