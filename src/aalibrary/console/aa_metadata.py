#!/usr/bin/env python3
"""
aa-metadata

Show the provenance embedded in an aa-* product: what made it, from what,
with which scientific options, and its product hash. Works on local files
and gs:// URIs, for every format the tools write (.nc, .zarr, .png,
.html, and <file>.aa.json sidecars for everything else).

    aa-metadata FILE [FILE ...]          human-readable summary
    aa-metadata FILE --json              the provenance document (one line per file)
    aa-metadata FILE --hash              just the product hash
    aa-metadata FILE --verify            recompute the hash from the recorded step
    ... | aa-metadata --tee | aa-next    summary to stderr, path passed through

Exit codes: 0 ok, 1 unreadable input, 3 no provenance, 4 --verify mismatch.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

from aalibrary.console._core import (
    Help, ToolSpec, canon, identity, provenance, render, show_help, stdio, uris,
)

SPEC = ToolSpec(name="aa-metadata", role="inspector", engines=())

HELP = Help(
    summary="Show what made a product: inputs, pipeline, options, hash.",
    does=(
        "Reads the provenance every aa-* tool embeds in its outputs and prints "
        "it: base name, recipe (the processing, which is the <hash8> in the file "
        "name and is the same for any data processed the same way), product hash "
        "(this processing of this data), the inputs and raw sources (with their "
        "origin, e.g. the NCEI object a .raw came from), every scientific step in "
        "order with its canonical options, and the software versions. --verify "
        "recomputes both hashes from the recorded step and checks that the hash "
        "in the file name is the recorded recipe."
    ),
    stdin="Paths or gs:// URIs, one per line (or as arguments). aa/1 JSON handles work too.",
    stdout=("A summary per file; with --json the provenance document; with --hash the "
            "hash; with --tee the input path unchanged (summary goes to stderr)."),
    options=[
        ("--json", "print the provenance document (compact, one line per file)"),
        ("--hash", "print only the product hash (--full for all 64 hex digits)"),
        ("--verify", "recompute the hash from the recorded step; exit 4 on mismatch"),
        ("--tee", "pass the path through on stdout; summary to stderr"),
    ],
    files=(
        "Local files, directories (.zarr) and gs:// URIs. For gs:// the file is "
        "read through a gcsfuse mount when one covers it, otherwise downloaded "
        "once to the cache (AA_CACHE_DIR)."
    ),
    pipeline="An inspector: at the end of a pipe, or in the middle with --tee.",
    examples=[
        "aa-metadata HB1603_EK60_20160703T060000-20160703T120000_71957ca9.nc",
        "aa-nc x.raw --sonar_model EK60 | aa-sv | aa-metadata --tee | aa-clean",
        "aa-metadata gs://bucket/derived/x_71957ca9.nc --verify",
    ],
)


def print_help():
    sys.stdout.write(render(SPEC, HELP, _build_parser()))


def print_help_full():
    print(__doc__)


def _build_parser():
    p = argparse.ArgumentParser(prog="aa-metadata", add_help=False)
    p.add_argument("paths", nargs="*")
    p.add_argument("--json", action="store_true")
    p.add_argument("--hash", action="store_true")
    p.add_argument("--full", action="store_true")
    p.add_argument("--verify", action="store_true")
    p.add_argument("--tee", action="store_true")
    return p


def _params(params: dict | None) -> str:
    if not params:
        return "(defaults)"
    parts = []
    for k, v in sorted(params.items()):
        if v is None:
            continue
        parts.append(f"{k}={v if isinstance(v, str) else canon.dumps(v)}")
    return " ".join(parts) or "(defaults)"


def summarize(name: str, doc: dict) -> str:
    prod = doc.get("product", {})
    lines = [name]
    kind = prod.get("kind") or "?"
    role = prod.get("role") or "?"
    recipe = prod.get("recipe")
    if recipe and role not in ("echodata", "source", "representation"):
        lines.append(f"  recipe    {str(recipe)[:8]}  the processing (the <hash8> in the name; the "
                     "same for any data processed this way)")
    lines.append(f"  product   {prod.get('short', '')}  {kind} ({role})   {prod.get('hash', '')}")
    sci = prod.get("scientific_hash")
    if sci and sci != prod.get("hash"):
        lines.append(f"  shows     {str(prod.get('scientific_recipe') or sci)[:8]}  "
                     "(the recipe of the product this renders)")
    lines.append(f"  base      {doc.get('base', '')}")
    for i, inp in enumerate(doc.get("inputs", []) or []):
        label = "inputs   " if i == 0 else "         "
        where = inp.get("origin") or inp.get("uri") or ""
        lines.append(f"  {label} {inp.get('role', ''):8s} {inp.get('name', '') or ''}  "
                     f"{str(inp.get('id', ''))[:24]}  {where}".rstrip())
    pipe = doc.get("pipeline", []) or []
    if doc.get("pipeline_truncated"):
        lines.append(f"  pipeline  ({doc['pipeline_truncated']} earlier steps in the .aa.json sidecar)")
    for i, step in enumerate(pipe, 1):
        label = "pipeline " if i == 1 else "         "
        tag = "" if step.get("scientific", True) else "  [rendering]"
        count = f"  x{step['count']}" if step.get("count") else ""
        variant = f" ({step['variant']})" if step.get("variant") else ""
        lines.append(f"  {label} {i}. {step.get('tool', '')}{variant}  {step.get('op', '')}"
                     f"  {_params(step.get('params'))}{count}{tag}")
    extra = doc.get("extra") or {}
    if extra.get("origin"):
        lines.append(f"  origin    {extra['origin']}")
    sources = doc.get("sources") or []
    if sources and prod.get("role") != "source":
        shown = sources[:5]
        for i, src in enumerate(shown):
            label = "sources   " if i == 0 else "          "
            head = f"({len(sources)}) " if i == 0 and len(sources) > 1 else ""
            where = src.get("origin") or ""
            lines.append(f"  {label}{head}{src.get('name') or ''}  {where}".rstrip())
        if len(sources) > len(shown):
            lines.append(f"            ... and {len(sources) - len(shown)} more (--json lists all)")
    elif doc.get("sources_truncated"):
        lines.append(f"  sources   {doc['sources_truncated']} (listed in the .aa.json sidecar)")
    for key, value in sorted(extra.items()):
        if key in ("origin",):
            continue
        text = value if isinstance(value, str) else canon.dumps(value)
        if len(text) > 100:
            text = text[:97] + "..."
        lines.append(f"  note      {key}: {text}")
    sw = doc.get("software", {}) or {}
    if sw:
        order = ["aalibrary", "echopype", "xarray", "numpy", "python"]
        lines.append("  software  " + " · ".join(f"{k} {sw[k]}" for k in order if k in sw))
    created = doc.get("created", {}) or {}
    if created:
        who = f" by {created.get('user')}@{created.get('host')}" if created.get("user") else ""
        lines.append(f"  created   {created.get('at', '')}{who}")
    return "\n".join(lines)


_NAME_HASH = re.compile(r"_([0-9a-f]{8})(?:\.[^/]*)?$")


def verify(doc: dict, name: str = "") -> tuple[bool, str]:
    """Recompute the product hash from the last recorded step, and check that
    a hash in the file name (<base>_<hash8>.<ext>) is the one recorded."""
    prod = doc.get("product", {})
    m = (_NAME_HASH.search(uris.basename(name))
         if name and prod.get("role") in ("transform", "representation") else None)
    if prod.get("role") == "representation":
        expected = str(prod.get("scientific_recipe") or prod.get("scientific_hash") or "")[:8]
    else:
        expected = str(prod.get("recipe") or prod.get("hash") or "")[:8]
    if m and expected and m.group(1) != expected:
        return False, (f"MISMATCH: the file name says {m.group(1)}, the file records "
                       f"recipe {expected} (renamed, or a different product copied over it)")
    if prod.get("role") == "source":
        return True, "source file: identified by content (nothing to recompute)"
    steps = doc.get("pipeline") or []
    if not steps:
        return False, "no pipeline recorded"
    step = steps[-1]
    inputs = [{"role": i.get("role", "source"), "id": i.get("id")} for i in doc.get("inputs", [])]
    ident = identity.identity_document(
        tool=step.get("identity_tool") or step.get("tool", ""), op=step.get("op", ""),
        op_version=step.get("op_version", 1),
        params=step.get("params") or {}, engine=step.get("engine") or {},
        inputs=inputs, variant=step.get("variant"))
    got = identity.product_hash(ident)
    if got != prod.get("hash"):
        return False, f"MISMATCH: recorded {prod.get('hash')}, recomputed {got}"
    if prod.get("recipe") and "recipe_inputs" in step:
        rdoc = identity.recipe_document(
            tool=step.get("identity_tool") or step.get("tool", ""), op=step.get("op", ""),
            op_version=step.get("op_version", 1), params=step.get("params") or {},
            engine=step.get("engine") or {}, inputs=step.get("recipe_inputs") or [],
            variant=step.get("variant"))
        r = identity.recipe_hash(rdoc)
        if r != prod.get("recipe"):
            return False, f"MISMATCH: recorded recipe {prod.get('recipe')}, recomputed {r}"
    return True, "hash verified"


def _remote_doc(token: str) -> dict | None:
    """Provenance of a gs:// product without downloading it, when possible.

    Products published by the tools have a small ``<key>.aa.json`` beside
    them; a Zarr store keeps it in its root metadata. Either is kilobytes,
    where the product may be gigabytes. The sidecar is trusted only when it
    names the product hash the object itself carries.
    """
    import json
    import tempfile

    bucket, key = uris.parse_gcs(token)
    key = key.rstrip("/")
    backend = uris.backend()
    info = backend.stat(bucket, key) if key else None
    side = backend.stat(bucket, key + uris.SIDECAR_SUFFIX) if key else None
    with tempfile.TemporaryDirectory(prefix="aa-metadata-") as tmp:
        if side is not None:
            local = Path(tmp) / "side.json"
            backend.download(bucket, side.key, local)
            doc = json.loads(local.read_text(encoding="utf-8"))
            claimed = (info.metadata.get(uris.META_HASH) if info else None)
            if doc.get("schema") == provenance.SCHEMA and (
                    claimed is None or claimed == doc.get("product", {}).get("hash")):
                return doc
        if info is None:
            # A prefix: read a store's root metadata and its seal, nothing else.
            store = Path(tmp) / "store.zarr"
            got = False
            for rel in ("zarr.json", ".zattrs", ".zgroup",
                        f"{provenance.SEAL_GROUP}/zarr.json",
                        f"{provenance.SEAL_GROUP}/.zattrs"):
                obj = backend.stat(bucket, f"{key}/{rel}")
                if obj is not None:
                    target = store / rel
                    target.parent.mkdir(parents=True, exist_ok=True)
                    backend.download(bucket, obj.key, target)
                    got = True
            if got:
                return provenance.read(store)
    return None


def _load(token: str) -> tuple[dict | None, str]:
    if uris.is_gcs(token):
        try:
            doc = _remote_doc(token)
        except Exception:  # noqa: BLE001 - fall back to reading the product
            doc = None
        if doc is not None:
            return doc, token
        loc = uris.localize(token)
        return provenance.read(loc.path), token
    p = Path(uris.from_file_uri(token)).expanduser()
    if not p.exists():
        raise FileNotFoundError(f"no such file: {token}")
    return provenance.read(p), str(p.resolve())


def main():
    if len(sys.argv) == 1 and not stdio.stdin_is_piped():
        print_help()
        sys.exit(0)
    parser = _build_parser()
    if show_help(SPEC, HELP, parser, full=print_help_full):
        sys.exit(0)
    args = parser.parse_args()
    tokens = stdio.many_inputs(args.paths, SPEC.name)
    out = sys.stderr if args.tee else sys.stdout
    code = 0
    try:
        code = _report(tokens, args, out)
    except BrokenPipeError:              # `aa-metadata FILE | head`
        stdio.quiet_broken_pipe()
    sys.exit(code)


def _report(tokens: list[str], args, out) -> int:
    code = 0
    for token in tokens:
        try:
            doc, shown = _load(token)
        except Exception as exc:
            msg = str(exc) if isinstance(exc, FileNotFoundError) and str(exc).startswith(
                "no such file") else f"cannot read {token}: {exc}"
            print(f"aa-metadata: {msg}", file=sys.stderr)
            code = max(code, 1)
            continue
        if doc is None:
            print(f"aa-metadata: {token}: no aa provenance (not written by an aa-* tool, "
                  "or written before provenance existed)", file=sys.stderr)
            code = max(code, 3)
            if args.tee:
                stdio.emit(token)
            continue
        if args.json:
            print(json.dumps(doc, separators=(",", ":"), sort_keys=True), file=out)
        elif args.hash:
            h = doc.get("product", {}).get("hash", "")
            print(h if args.full else h[:8], file=out)
        else:
            print(summarize(shown, doc), file=out)
        if args.verify:
            ok, msg = verify(doc, shown)
            print(f"  verify    {msg}", file=out if not args.json else sys.stderr)
            if not ok:
                code = max(code, 4)
        if args.tee:
            stdio.emit(token)
    return code


if __name__ == "__main__":
    main()
