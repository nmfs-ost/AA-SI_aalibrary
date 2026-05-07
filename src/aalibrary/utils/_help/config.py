"""Config loading for aa-help.

Lives at ~/.config/aalibrary/aa_help.toml (or $XDG_CONFIG_HOME/aalibrary/...).
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib  # type: ignore

try:
    import tomli_w
    _HAVE_TOMLI_W = True
except ModuleNotFoundError:
    _HAVE_TOMLI_W = False


APP_NAME = "aalibrary"
DEFAULT_MODEL = "gemini-2.5-pro"
DEFAULT_LOCATION = "us-central1"


@dataclass
class Settings:
    project_id: str = ""
    location: str = DEFAULT_LOCATION
    model: str = DEFAULT_MODEL
    temperature: float = 0.2          # lower default; planner output
    max_output_tokens: int = 4096
    knowledge_dirs: list[str] = field(default_factory=list)
    extra_system_prompt: str = ""
    rag_top_k: int = 8                # how many chunks to retrieve per query
    rag_max_chars: int = 60_000       # hard cap on retrieved-context size

    # File discovery (acoustic-file index across the home directory).
    file_scan_root: str = ""          # default: ~ (home dir)
    file_scan_exclude: list[str] = field(default_factory=list)
    file_index_ttl_seconds: int = 300

    def is_complete(self) -> bool:
        return bool(self.project_id and self.location and self.model)


def config_dir() -> Path:
    xdg = os.environ.get("XDG_CONFIG_HOME")
    base = Path(xdg) if xdg else Path.home() / ".config"
    return base / APP_NAME


def config_path() -> Path:
    return config_dir() / "aa_help.toml"


def _ensure_dir() -> None:
    config_dir().mkdir(parents=True, exist_ok=True)


def load_config() -> Settings:
    p = config_path()
    if not p.exists():
        return Settings()
    with p.open("rb") as f:
        data = tomllib.load(f)
    aa = data.get("aa_help", {})
    return Settings(
        project_id=aa.get("project_id", ""),
        location=aa.get("location", DEFAULT_LOCATION),
        model=aa.get("model", DEFAULT_MODEL),
        temperature=float(aa.get("temperature", 0.2)),
        max_output_tokens=int(aa.get("max_output_tokens", 4096)),
        knowledge_dirs=list(aa.get("knowledge_dirs", [])),
        extra_system_prompt=aa.get("extra_system_prompt", ""),
        rag_top_k=int(aa.get("rag_top_k", 8)),
        rag_max_chars=int(aa.get("rag_max_chars", 60_000)),
        file_scan_root=aa.get("file_scan_root", ""),
        file_scan_exclude=list(aa.get("file_scan_exclude", [])),
        file_index_ttl_seconds=int(aa.get("file_index_ttl_seconds", 300)),
    )


def save_config(settings: Settings) -> None:
    _ensure_dir()
    payload: dict[str, Any] = {"aa_help": asdict(settings)}
    if _HAVE_TOMLI_W:
        with config_path().open("wb") as f:
            tomli_w.dump(payload, f)
    else:
        config_path().write_text(_dump_toml(payload), encoding="utf-8")


def _dump_toml(payload: dict[str, Any]) -> str:
    lines = ["[aa_help]"]
    for k, v in payload["aa_help"].items():
        if isinstance(v, str):
            esc = v.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
            lines.append(f'{k} = "{esc}"')
        elif isinstance(v, bool):
            lines.append(f"{k} = {'true' if v else 'false'}")
        elif isinstance(v, (int, float)):
            lines.append(f"{k} = {v}")
        elif isinstance(v, list):
            joined = ", ".join(f'"{s}"' for s in v)
            lines.append(f"{k} = [{joined}]")
    return "\n".join(lines) + "\n"


def run_setup_wizard() -> Settings:
    print("=== aa-help setup ===")
    print(f"Config will be written to: {config_path()}\n")
    cur = load_config()

    project = input(f"GCP project ID [{cur.project_id}]: ").strip() or cur.project_id
    while not project:
        project = input("GCP project ID is required: ").strip()
    location = (input(f"Vertex AI location [{cur.location or DEFAULT_LOCATION}]: ").strip()
                or cur.location or DEFAULT_LOCATION)
    model = (input(f"Model [{cur.model or DEFAULT_MODEL}]: ").strip()
             or cur.model or DEFAULT_MODEL)

    s = Settings(
        project_id=project, location=location, model=model,
        temperature=cur.temperature, max_output_tokens=cur.max_output_tokens,
        knowledge_dirs=cur.knowledge_dirs,
        extra_system_prompt=cur.extra_system_prompt,
        rag_top_k=cur.rag_top_k, rag_max_chars=cur.rag_max_chars,
    )
    save_config(s)
    print(f"\nSaved config to {config_path()}")
    print("Next: add docs to knowledge_dirs in the toml, then run:")
    print("    aa-help --reindex")
    return s


def edit_config() -> None:
    _ensure_dir()
    p = config_path()
    if not p.exists():
        save_config(Settings())
    editor = os.environ.get("EDITOR") or os.environ.get("VISUAL")
    if not editor:
        for cand in ("nano", "vim", "vi", "notepad"):
            if shutil.which(cand):
                editor = cand
                break
    if not editor:
        print(f"No $EDITOR set. Open {p} manually.", file=sys.stderr)
        return
    subprocess.call([editor, str(p)])
