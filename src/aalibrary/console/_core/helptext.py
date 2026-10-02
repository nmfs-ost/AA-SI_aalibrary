"""Curated --help, the same shape for every tool.

Each tool fills in a ``Help`` record; ``render`` lays it out in a fixed
order that answers, in this order:

    1. What does this tool do?                       (summary / does)
    2. What does it accept through stdin?            INPUT
    3. What does it produce on stdout?               OUTPUT
    4. What happens to metadata?                     METADATA
    5. What options actually matter?                 OPTIONS
    6. Which options affect the scientific hash?     SCIENTIFIC OPTIONS
    7. What files/URIs can it read or write?         FILES & URIs
    8. How does it behave in a pipeline?             IN A PIPELINE
    9. One or two useful examples.                   EXAMPLES

SCIENTIFIC OPTIONS is generated from the tool's ToolSpec, i.e. from the
same list the hash is computed from, so it cannot drift out of date.
The previous long-form help stays available as ``--help-all``.
"""

from __future__ import annotations

import textwrap
from dataclasses import dataclass, field

WIDTH = 78

_METADATA = {
    "source": (
        "Writes a <file>.aa.json sidecar beside each file it fetches: where it "
        "came from and its checksum. Tools downstream record that origin."
    ),
    "echodata": (
        "Starts the provenance chain. Records the source file's identity (and "
        "its NCEI/GCS origin when known) and the conversion options inside the "
        "output (NetCDF attributes aa_provenance, aa_product_hash, aa_base). "
        "The base name comes from the source file's stem unless you set it."
    ),
    "transform": (
        "Reads the input's provenance, appends this step with its canonical "
        "scientific options, and embeds it all in the output (NetCDF attributes "
        "aa_provenance, aa_recipe, aa_product_hash, aa_base, aa_tool, history). "
        "Two hashes: the recipe (this step and every step before it, without the "
        "data: the <hash8> in the name, the same for any data processed this way) "
        "and the product hash (this recipe applied to this input: decides reuse). "
        "The base name is carried through unchanged. Inspect with: aa-metadata FILE"
    ),
    "representation": (
        "Copies the provenance of the product it shows into the output (PNG "
        "text chunks, or a JSON block in the HTML head) and adds a "
        "non-scientific rendering step. The output is named after the product "
        "it shows, with its own extension."
    ),
    "sink": (
        "Never changes the file. Uploads it with its .aa.json sidecar and "
        "stamps aa-product-hash / aa-base / aa-tool into the object's custom "
        "metadata, so the bucket can answer 'is this product already here?'."
    ),
    "inspector": "Read-only: reads files and metadata, writes nothing.",
    "utility": "Produces no scientific product and records no provenance.",
    "interactive": "Produces no scientific product and records no provenance.",
}

_COMMON = [
    ("--force", "recompute even if an identical product already exists"),
    ("--base NAME", "name outputs after NAME instead of the input's base name"),
    ("--dest DIR|gs://PREFIX",
     "write the default-named output there instead of beside the input"),
    ("--help-all", "the complete reference, every option"),
]

_ROLE_LABEL = {
    "source": "source (fetches data)",
    "echodata": "EchoData builder (starts the chain)",
    "transform": "scientific transform (hashed)",
    "representation": "representation (renders a product)",
    "sink": "sink (stores products)",
    "inspector": "inspector (read-only)",
    "utility": "utility",
    "interactive": "interactive",
}


@dataclass
class Help:
    summary: str
    does: str = ""
    stdin: str = ""
    stdout: str = ""
    metadata: str = ""                     # overrides the role's standard text
    options: list[tuple[str, str]] = field(default_factory=list)
    science: dict[str, str] = field(default_factory=dict)  # dest -> description
    files: str = ""
    pipeline: str = ""
    examples: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    common: bool | None = None             # show --force/--base/--dest (default: product roles)
    hash_note: str | None = None           # replaces the "flag order ..." line; "" drops it


# Never split tool names (aa-metadata), flags (--refresh-files) or URLs.
_FILL = {"break_on_hyphens": False, "break_long_words": False}


def _wrap(text: str, indent: int = 2) -> str:
    pad = " " * indent
    out = []
    for para in text.strip().split("\n\n"):
        lines = para.splitlines()
        if any(line.startswith("  ") for line in lines):  # preformatted
            out.append("\n".join(pad + line for line in lines))
        else:
            out.append(textwrap.fill(" ".join(line.strip() for line in lines), WIDTH,
                                     initial_indent=pad, subsequent_indent=pad, **_FILL))
    return "\n\n".join(out)


def _rows(rows: list[tuple[str, str]], indent: int = 2) -> str:
    if not rows:
        return ""
    col = min(max(len(r[0]) for r in rows) + 2, 30)
    pad = " " * indent
    out = []
    for flag, desc in rows:
        body = textwrap.wrap(desc, WIDTH - indent - col, **_FILL) or [""]
        if len(flag) + 2 > col:
            out.append(pad + flag)
            out.extend(pad + " " * col + line for line in body)
        else:
            out.append(pad + flag.ljust(col) + body[0])
            out.extend(pad + " " * col + line for line in body[1:])
    return "\n".join(out)


def _science_rows(spec, help_: Help, parser) -> list[tuple[str, str]]:
    rows = []
    actions: dict[str, list] = {}
    if parser is not None:
        for action in getattr(parser, "_actions", []):
            actions.setdefault(action.dest, []).append(action)
    names = list(getattr(spec, "params", {}) or {}) + list(help_.science.keys())
    seen = set()
    for dest in names:
        if dest in seen:
            continue
        seen.add(dest)
        group = actions.get(dest) or []
        # Paired flags (--skipna / --no_skipna) share a dest: list them all.
        strings = [s for a in group for s in a.option_strings]
        flag = ", ".join(dict.fromkeys(strings)) if strings else f"--{dest}"
        first = group[0] if group else None
        desc = help_.science.get(dest) or next((a.help for a in group if a.help), "") or ""
        if first is not None and first.default not in (None, False, [], "") and "default" not in desc:
            desc = f"{desc} (default: {first.default})".strip()
        rows.append((flag, desc))
    return rows


def _registered(parser) -> set[str]:
    return {s for a in getattr(parser, "_actions", []) for s in a.option_strings} if parser else set()


def render(spec, help_: Help, parser=None) -> str:
    role = getattr(spec, "role", "utility")
    name = getattr(spec, "name", "")
    parts = [textwrap.fill(f"{name} — {help_.summary}".rstrip(), WIDTH,
                           subsequent_indent="  ", **_FILL)]
    tag = _ROLE_LABEL.get(role, role)
    if getattr(spec, "kind", ""):
        tag += f" · product: {spec.kind}"
    parts.append(f"  [{tag}]")

    def section(title: str, body: str) -> None:
        if body and body.strip():
            parts.append(f"\n{title}\n{body}")

    section("WHAT IT DOES", _wrap(help_.does) if help_.does else "")
    section("INPUT (argument or stdin)", _wrap(help_.stdin))
    section("OUTPUT (stdout)", _wrap(help_.stdout))
    section("METADATA", _wrap(help_.metadata or _METADATA.get(role, "")))
    section("OPTIONS", _rows(help_.options))
    if role in {"echodata", "transform", "representation", "source"}:
        rows = _science_rows(spec, help_, parser)
        if role == "representation":
            label = "RENDERING OPTIONS (identify this rendering; the science shown is the input's)"
        elif role == "source":
            label = "SCIENTIFIC OPTIONS"
        else:
            label = "SCIENTIFIC OPTIONS (change the product hash)"
        if rows:
            body = _rows(rows)
        elif role == "source":
            body = ("  None. A fetched file is identified by its content (MD5), not by the\n"
                    "  options that selected it, so the same file always has the same identity.")
        else:
            body = "  None. Every option is I/O or display and leaves the hash alone."
        note = help_.hash_note
        if note is None and rows and role != "representation":
            note = "Flag order, alias spellings and explicit defaults do not change the hash."
        if rows and note:
            body += "\n" + _wrap(note)
        section(label, body)
    section("FILES & URIs", _wrap(help_.files))
    section("IN A PIPELINE", _wrap(help_.pipeline))
    if help_.examples:
        section("EXAMPLES", "\n".join("  " + e if not e.startswith("  ") else e
                                      for e in help_.examples))
    for note in help_.notes:
        section("NOTE", _wrap(note))
    show_common = help_.common if help_.common is not None else role in {
        "echodata", "transform", "representation"}
    if show_common:
        have = _registered(parser)
        rows = [r for r in _COMMON
                if r[0] == "--help-all" or parser is None or r[0].split()[0] in have]
        section("COMMON OPTIONS", _rows(rows))
    else:
        section("MORE", _rows([("--help-all", "the complete reference, every option")]))
    return "\n".join(parts) + "\n"


__all__ = ["Help", "render", "WIDTH"]
