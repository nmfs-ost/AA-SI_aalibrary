"""One run of one tool: inputs -> hash -> reuse or compute -> provenance -> output.

Every product-writing tool follows the same five calls::

    run = Run(SPEC, args)                       # canonical params from argparse
    src = run.input(token)                      # local path, provenance, base
    out = run.plan(ext=".nc", explicit=...)     # name + hash, before computing
    if run.reusable(out):                       # identical product exists?
        return run.finish(out)                  #   -> print it, done
    compute(src.local, out.local)               # the tool's own science
    run.finish(out)                             # embed provenance, upload, print

``ToolSpec`` declares which argparse options are scientific (and how to
canonicalize them). Nothing else reaches the hash.

Output location, in order:
    1. an explicit output from the user (-o), exactly as the tool always
       interpreted it; may be gs://
    2. --dest DIR or gs://PREFIX, with the standard name
    3. AA_NAMING=legacy: the tool's old default name (moved to the current
       directory when it would land beside a cached or mounted gs:// input)
    4. the tool's own output folder (e.g. aa-evr --out-dir), standard name
    5. the standard name, beside the input (or in the current directory
       when the input came from gs://)

Base name: --base, else (EchoData builders only) the stem of an explicit
-o, else the primary input's recorded base, else its file stem.
"""

from __future__ import annotations

import argparse
import atexit
import os
import shutil
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from . import canon, identity, naming, provenance, stdio, uris

ROLES = {"source", "echodata", "transform", "representation", "sink",
         "inspector", "utility", "interactive"}


@dataclass(frozen=True)
class ToolSpec:
    name: str
    role: str
    kind: str = ""
    op: str = ""
    op_version: int = 1
    version: str = ""
    params: dict[str, Callable[[Any], Any]] = field(default_factory=dict)
    engines: tuple[str, ...] = ("echopype",)
    ext: str = ".nc"
    # The tool name used in the product hash. Two tools that run exactly the
    # same computation (aa-ed and aa-nc both run echopype.open_raw and write
    # the same file) share one, so their products are interchangeable.
    identity: str = ""

    def __post_init__(self):
        if self.role not in ROLES:
            raise ValueError(f"unknown role {self.role!r}")

    @property
    def tool_version(self) -> str:
        if self.version:
            return self.version
        try:
            from importlib.metadata import version

            return version("aalibrary")
        except Exception:
            return "unknown"


@dataclass
class Input:
    token: str               # as given (path or URI)
    local: Path              # readable local path
    uri: str                 # file:// or gs:// URI recorded in provenance
    prov: dict | None
    id: str                  # identity used in the hash
    base: str
    role: str = "source"
    via: str = "local"       # local | mount | cache | download
    recipe: str = identity.DATA   # what this input adds to a recipe (see identity)

    @property
    def name(self) -> str:
        return uris.basename(self.token)

    def record(self) -> dict:
        rec = {"role": self.role, "uri": self.uri, "id": self.id, "name": self.name}
        origin = ((self.prov or {}).get("extra") or {}).get("origin")
        if origin:
            rec["origin"] = origin
        return rec


@dataclass
class Output:
    target: str              # final location: local path string or gs:// URI
    local: Path              # where the tool writes
    hash: str                # this file's product hash
    scientific_hash: str     # the science it carries (= hash, except renderings)
    base: str
    kind: str
    variant: str | None
    step: dict
    reused: bool = False
    recipe: str = ""              # the processing, without the data: <hash8> in names
    scientific_recipe: str = ""   # = recipe, except renderings (the shown product's)
    staging: Path | None = None   # temp dir for a gs:// target, removed by finish()
    atomic: bool = False          # local is a temp sibling renamed onto target by finish()
    planned_at: float = 0.0

    @property
    def remote(self) -> bool:
        return uris.is_remote(self.target)

    @property
    def short(self) -> str:
        return self.hash[: naming.HASH_LEN]

    @property
    def recipe_short(self) -> str:
        return self.recipe[: naming.HASH_LEN]


def add_common_flags(parser: argparse.ArgumentParser, *, force: bool = True,
                     base: bool = True, dest: bool = True) -> None:
    """--force / --base / --dest, unless the tool already defines them."""
    existing = {s for a in parser._actions for s in a.option_strings}
    if force and "--force" not in existing:
        parser.add_argument("--force", action="store_true", default=False,
                            help="Recompute even if an identical product already exists.")
    if base and "--base" not in existing:
        parser.add_argument("--base", default=None, metavar="NAME",
                            help="Base name for outputs (default: the input's base name).")
    if dest and "--dest" not in existing:
        parser.add_argument("--dest", default=None, metavar="DIR|gs://PREFIX",
                            help="Write the default-named output here instead of beside the input.")


# Staging folders for gs:// targets not yet finished: removed at exit, so a
# failure between plan() and finish() leaves nothing behind in the cache.
_PENDING_STAGING: set[Path] = set()


def _remove(path: Path) -> None:
    if path.is_dir() and not path.is_symlink():
        shutil.rmtree(path, ignore_errors=True)
    else:
        try:
            path.unlink()
        except OSError:
            pass


@atexit.register
def _cleanup_staging() -> None:
    for d in list(_PENDING_STAGING):
        _remove(d)
        side = provenance.sidecar_path(d)
        if side.exists():
            _remove(side)
    _PENDING_STAGING.clear()


def _temp_sibling(target: Path) -> Path:
    """Hidden temp name beside target, keeping its extension (tools and
    libraries pick the format from it)."""
    import uuid

    return target.with_name(f".{target.stem}.aa-{uuid.uuid4().hex[:8]}{target.suffix}")


def _replace(src: Path, dst: Path) -> None:
    """Move a finished file or folder onto dst in one step (as far as the OS allows)."""
    if src.is_dir():
        if dst.exists() or dst.is_symlink():
            old = dst.with_name(f".{dst.name}.aa-old-{os.getpid()}")
            os.replace(dst, old)
            os.replace(src, dst)
            _remove(old)
        else:
            os.replace(src, dst)
    else:
        if dst.is_dir() and not dst.is_symlink():
            shutil.rmtree(dst)
        os.replace(src, dst)


def _source_sidecar_is_current(local: Path, prov: dict) -> bool:
    """A fetched file's sidecar describes the file that is there now.

    The sidecar records the content identity (md5:<hex>:<size>) when the
    file was fetched. A different size, or a file modified after its
    sidecar was written whose MD5 no longer matches, means the file was
    replaced and the sidecar (and its origin) no longer applies.
    """
    content_id = (prov.get("product") or {}).get("content_id") or ""
    parts = content_id.split(":")
    if len(parts) != 3 or parts[0] != "md5":
        return True
    try:
        if int(parts[2]) != local.stat().st_size:
            return False
        # Always by content: a same-size file copied in with an older mtime
        # (rsync -a, fixed-size raw chunks) must not inherit another's
        # identity. file_identity is memoized, so this hashes a file once.
        return identity.file_identity(local) == content_id
    except (OSError, ValueError):
        return True


def canonical_params(spec: ToolSpec, args: Any) -> dict:
    out = {}
    for dest, fn in (spec.params or {}).items():
        out[dest] = fn(getattr(args, dest, None)) if args is not None else None
    return out


class Run:
    def __init__(self, spec: ToolSpec, args: Any = None, *, params: dict | None = None,
                 base: str | None = None):
        """``base``: the base name to use when there is no input to take it
        from (aa-sound-speed -o, aa-absorption -o); --base still wins."""
        self.spec = spec
        self.args = args
        base_params = canonical_params(spec, args)
        if params:
            base_params.update({k: canon.normalize(v) for k, v in params.items()})
        self.params = base_params
        self.inputs: list[Input] = []
        self.force = bool(getattr(args, "force", False)) or os.getenv("AA_REUSE", "1") == "0"
        self.base_override = getattr(args, "base", None)
        self.default_base = base
        self.dest = getattr(args, "dest", None)

    # -- inputs ------------------------------------------------------------

    def input(self, token: str | os.PathLike, *, role: str = "source",
              must_exist: bool = True) -> Input:
        token = str(token)
        via = "local"
        info = None
        if uris.is_gcs(token):
            try:
                loc = uris.localize(token)
            except FileNotFoundError:
                stdio.fail(self.spec.name, f"input not found: {token}")
            except Exception as exc:  # credentials, network, permissions
                stdio.fail(self.spec.name, f"cannot read {token}: {exc}")
            local, uri, info, via = loc.path, token, loc.info, loc.via
        elif uris.is_remote(token):
            stdio.fail(self.spec.name, f"unsupported URI scheme (use gs:// or a local path): {token}")
        else:
            local = Path(uris.from_file_uri(token)).expanduser()
            if must_exist and not local.exists():
                stdio.fail(self.spec.name, f"input not found: {token}")
            uri = uris.to_uri(local) if local.exists() else token
        prov, status = provenance.inspect(local) if local.exists() else (None, "none")
        if status == "copied":
            print(f"{self.spec.name}: note: {local.name} carries aa provenance attributes "
                  "copied from another product (it was saved by another program); treating "
                  "it as a plain file, identified by its content", file=sys.stderr)
        elif status == "stale":
            print(f"{self.spec.name}: note: {local.name}{provenance.SIDECAR_SUFFIX} describes "
                  "an earlier version of the file; ignoring it", file=sys.stderr)
        if (prov and (prov.get("product") or {}).get("role") == "source"
                and not local.is_dir() and not _source_sidecar_is_current(local, prov)):
            print(f"{self.spec.name}: warning: {local.name} has changed since its "
                  f"{provenance.SIDECAR_SUFFIX} sidecar was written; ignoring the sidecar "
                  "(origin unknown)", file=sys.stderr)
            prov = None
        ident = identity.of_provenance(prov)
        if ident is None and info is not None:
            ident = identity.gcs_identity(info.md5, info.size)
        if ident is None:
            ident = identity.content_identity(local) if local.exists() else f"uri:{token}"
        recorded = (prov or {}).get("base")
        base = (naming.sanitize_base(recorded) if recorded
                else naming.base_of(local.name if local.name else token))
        inp = Input(token, local, uri, prov, ident, base, role, via,
                    identity.input_recipe(role, prov, ident))
        self.inputs.append(inp)
        return inp

    def param_file(self, token: str | os.PathLike, *, role: str,
                   uri: str | None = None) -> Input:
        """A file-valued scientific option (EVR/EVL regions, a reference file):
        its content identity enters the hash, not its path.

        ``uri``: the location to record when the tool fetched the file
        itself (e.g. s3:// through fsspec) and passes the local copy.
        """
        inp = self.input(token, role=role)
        if uri:
            inp.uri = uri
            inp.token = uri
        return inp

    @property
    def primary(self) -> Input | None:
        for inp in self.inputs:
            if inp.role == "source":
                return inp
        return self.inputs[0] if self.inputs else None

    # -- naming and hashing ------------------------------------------------

    def base(self) -> str:
        if self.base_override:
            return naming.sanitize_base(self.base_override)
        p = self.primary
        if p is not None:
            return p.base
        return naming.sanitize_base(self.default_base) if self.default_base else "product"

    def identity_doc(self, variant: str | None = None, extra_params: dict | None = None) -> dict:
        params = dict(self.params)
        if extra_params:
            params.update(canon.normalize(extra_params))
        return identity.identity_document(
            tool=self.spec.identity or self.spec.name, op=self.spec.op or self.spec.name,
            op_version=self.spec.op_version, params=params,
            engine=identity.engine_versions(self.spec.engines),
            inputs=[{"role": i.role, "id": i.id} for i in self.inputs],
            variant=variant)

    def recipe_doc(self, variant: str | None = None, extra_params: dict | None = None) -> dict:
        """The recipe: this step and (through its inputs) every earlier step,
        without the identity of the data."""
        params = dict(self.params)
        if extra_params:
            params.update(canon.normalize(extra_params))
        return identity.recipe_document(
            tool=self.spec.identity or self.spec.name, op=self.spec.op or self.spec.name,
            op_version=self.spec.op_version, params=params,
            engine=identity.engine_versions(self.spec.engines),
            inputs=identity.recipe_inputs([(i.role, i.recipe) for i in self.inputs]),
            variant=variant)

    def plan(self, *, ext: str | None = None, explicit: str | os.PathLike | None = None,
             legacy: Callable[[], str | os.PathLike] | None = None,
             variant: str | None = None, kind: str | None = None,
             extra_params: dict | None = None, name: str | None = None,
             directory: str | os.PathLike | None = None, stage: bool = True) -> Output:
        """Decide where the output goes and what its hash is, before computing.

        ``directory``: put the standard name in this folder (a tool's own
        --out-dir); ``--dest`` still wins over it. ``stage=False``: the tool
        writes a gs:// target itself (e.g. Zarr through fsspec), so no local
        staging file is made; call ``finish(out, publish=False)``.
        """
        ext = ext if ext is not None else self.spec.ext
        role = self.spec.role
        doc = self.identity_doc(variant, extra_params)
        h = identity.product_hash(doc)
        rdoc = self.recipe_doc(variant, extra_params)
        recipe = identity.recipe_hash(rdoc)
        base = self.base()
        if (role == "echodata" and not self.base_override
                and explicit is not None and str(explicit) != ""):
            # Naming the EchoData names everything made from it.
            base = naming.base_of(uris.basename(str(explicit)))
        primary = self.primary

        if role == "representation":
            shown = (primary.prov or {}).get("product", {}) if primary else {}
            scientific_hash = shown.get("hash") or (primary.id if primary else h)
            scientific_recipe = shown.get("recipe") or recipe
        else:
            scientific_hash = h
            scientific_recipe = recipe

        if name is None:
            if role == "echodata":
                name = naming.echodata_name(base, ext)
            elif role == "representation":
                # Named after the product it shows: <that product's name>.<ext>
                if self.base_override or primary is None:
                    name = naming.echodata_name(base, ext)
                else:
                    name = naming.representation_name(primary.name, ext)
            else:
                # <base> says which data, <hash8> which processing (the recipe).
                name = naming.derived_name(base, recipe, ext)

        if explicit is not None and str(explicit) != "" and str(explicit).endswith("/"):
            # "-o DIR/" names a folder: the standard name goes inside it.
            target = (uris.join(str(explicit), name) if uris.is_remote(str(explicit))
                      else str(Path(str(explicit)).expanduser() / name))
        elif explicit is not None and str(explicit) != "":
            target = str(explicit)
        elif self.dest:
            target = (uris.join(self.dest, name) if uris.is_remote(self.dest)
                      else str(Path(self.dest).expanduser() / name))
        elif legacy is not None and naming.mode() == "legacy":
            target = str(legacy())
            if (not uris.is_remote(target) and primary is not None and primary.via != "local"
                    and Path(os.path.abspath(Path(target).expanduser())).parent
                    == Path(os.path.abspath(primary.local)).parent):
                # Beside a cached or mounted gs:// input is the cache or a
                # (read-only) mount: use the current directory instead.
                target = str(Path.cwd() / Path(target).name)
        elif directory is not None:
            target = (uris.join(str(directory), name) if uris.is_remote(str(directory))
                      else str(Path(directory).expanduser() / name))
        else:
            if primary is not None and primary.via == "local":
                # Beside the input as given (a symlink's folder, not its target's).
                target = str(Path(os.path.abspath(primary.local)).parent / name)
            else:
                target = str(Path.cwd() / name)

        self._refuse_input_as_output(target)

        staging = None
        atomic = False
        if uris.is_remote(target) and not stage:
            local = Path(target)          # placeholder; the tool writes the URI itself
        elif uris.is_gcs(target):
            local = uris.stage(target)
            staging = local.parent
            _PENDING_STAGING.add(staging)
        elif uris.is_remote(target):
            stdio.fail(self.spec.name, f"cannot write {target}: only gs:// URIs and local "
                                       "paths are supported as outputs")
        else:
            target = os.path.abspath(Path(target).expanduser())
            local = Path(target)
            if stage and local.suffix:
                # Write beside the target under a hidden temp name; finish()
                # renames it into place, so a reader (or a second run of the
                # same pipeline) never sees a half-written file.
                local = _temp_sibling(local)
                atomic = True
                _PENDING_STAGING.add(local)

        step = {
            "tool": self.spec.name,
            **({"identity_tool": self.spec.identity}
               if self.spec.identity and self.spec.identity != self.spec.name else {}),
            "tool_version": self.spec.tool_version,
            "op": self.spec.op or self.spec.name,
            "op_version": self.spec.op_version,
            "params": doc["params"],
            "engine": doc["engine"],
            "inputs": [i.id for i in self.inputs],
            "product": h,
            "recipe": recipe,
            "recipe_inputs": rdoc["inputs"],
            "variant": variant,
            "scientific": role not in {"representation"},
        }
        return Output(target, local, h, scientific_hash, base, kind or self.spec.kind,
                      variant, step, recipe=recipe, scientific_recipe=scientific_recipe,
                      staging=staging, atomic=atomic, planned_at=time.time())

    def _refuse_input_as_output(self, target: str) -> None:
        """Never write over an input (e.g. -o gs://b/x.nc with input gs://b/x.nc)."""
        for inp in self.inputs:
            if uris.is_remote(target):
                same = target in (inp.uri, inp.token)
            else:
                try:
                    same = (not uris.is_remote(inp.token)
                            and Path(os.path.abspath(Path(target).expanduser())).resolve()
                            == inp.local.resolve())
                except OSError:
                    same = False
            if same:
                stdio.fail(self.spec.name, f"refusing to overwrite an input: {target}")

    # -- reuse ---------------------------------------------------------------

    def existing_hash(self, out: Output) -> str | None:
        """The product hash of what is already at the target, if trustworthy."""
        if out.remote:
            if not uris.is_gcs(out.target):
                return None
            info = uris.stat(out.target)
            if info is None:
                return None
            recorded_md5 = info.metadata.get(uris.META_MD5)
            if recorded_md5 and info.md5 and recorded_md5 != info.md5:
                return None   # the object was rewritten after the tool published it
            return info.metadata.get(uris.META_HASH)
        if not Path(out.target).exists():
            return None
        doc = provenance.read(out.target)   # unsealed (copied) provenance reads as None
        return (doc or {}).get("product", {}).get("hash")

    def exists(self, out: Output) -> bool:
        """Is there anything at the planned target (local file/dir or gs:// object)?"""
        if out.remote:
            if not uris.is_gcs(out.target):
                return False
            try:
                return uris.stat(out.target) is not None
            except Exception:
                return False
        return Path(out.target).exists()

    def conflicts(self, out: Output) -> bool:
        """Something different already sits at the target (for --no-overwrite)."""
        if not self.exists(out):
            return False
        try:
            return self.existing_hash(out) != out.hash
        except Exception:
            return True

    def reusable(self, out: Output, *, quiet: bool = False) -> bool:
        """True when the planned output already exists with the same hash."""
        if self.force:
            return False
        try:
            same = self.existing_hash(out) == out.hash
        except Exception:
            return False
        if same:
            out.reused = True
            if not quiet:
                print(f"{self.spec.name}: reusing {out.target} "
                      f"(identical product aa:{out.short} already exists; --force recomputes)",
                      file=sys.stderr)
        return same

    def discard(self, out: Output) -> None:
        """Drop a planned output that won't be written (removes its staging)."""
        if out.staging is not None:
            shutil.rmtree(out.staging, ignore_errors=True)
            _PENDING_STAGING.discard(out.staging)
            out.staging = None
        if out.atomic:
            if out.local.exists() or out.local.is_symlink():
                _remove(out.local)
            side = provenance.sidecar_path(out.local)
            if side.exists():
                _remove(side)
            _PENDING_STAGING.discard(out.local)

    # -- finishing -----------------------------------------------------------

    def document(self, out: Output, *, extra: dict | None = None) -> dict:
        doc = provenance.build(
            base=out.base, product_hash=out.hash, kind=out.kind, role=self.spec.role,
            name=uris.basename(out.target), step=out.step,
            inputs=[i.record() for i in self.inputs],
            parents=[i.prov for i in self.inputs],
            variant=out.variant, extra=extra, engines=self.spec.engines)
        doc["product"]["scientific_hash"] = out.scientific_hash
        doc["product"]["recipe"] = out.recipe
        doc["product"]["scientific_recipe"] = out.scientific_recipe
        return doc

    def finish(self, out: Output, *, extra: dict | None = None, embed: bool = True,
               emit: bool = True, publish: bool = True) -> str:
        """Embed provenance, upload if the target is gs://, print the result."""
        if not out.reused:
            written = out.local
            if out.atomic and not out.local.exists() and Path(out.target).exists() \
                    and os.stat(out.target).st_mtime >= out.planned_at - 2:
                written = Path(out.target)   # the tool wrote the target itself, just now
            if (out.staging is not None or not out.remote) and not written.exists():
                self.discard(out)
                stdio.fail(self.spec.name, f"no output was written for {out.target}")
            doc = None
            if embed and (out.staging is not None or not out.remote):
                doc = self.document(out, extra=extra)
                provenance.write(written, doc)
                got = (provenance.read(written) or {}).get("product", {}).get("hash")
                if got != out.hash:
                    self.discard(out)
                    stdio.fail(self.spec.name, f"could not record provenance in {out.target} "
                                               "(see the error above); output not kept")
            if out.atomic and written == out.local:
                target = Path(out.target)
                _replace(out.local, target)
                side = provenance.sidecar_path(out.local)
                if side.exists():
                    os.replace(side, provenance.sidecar_path(target))
                elif provenance.sidecar_path(target).exists():
                    _remove(provenance.sidecar_path(target))   # from an earlier product
                _PENDING_STAGING.discard(out.local)
            if out.remote and publish and out.staging is not None:
                metadata = {uris.META_HASH: out.hash, uris.META_BASE: out.base,
                            uris.META_TOOL: self.spec.name, uris.META_RECIPE: out.recipe}
                if out.local.is_file():
                    metadata[uris.META_MD5] = identity.md5_b64(out.local)
                side = provenance.sidecar_path(out.local)
                if doc is not None and not side.exists():
                    # A bucket object's provenance is embedded, but reading it
                    # means downloading the product. The small .aa.json beside
                    # it lets aa-metadata (and anyone browsing the bucket) read
                    # the record without that.
                    provenance.write_sidecar(out.local, doc)
                uris.publish(out.local, out.target, metadata=metadata)
        self.discard(out)
        if emit:
            stdio.emit(out.target)
        return out.target


def record_source(path: str | os.PathLike, *, tool: str, origin: str | None = None,
                  extra: dict | None = None) -> dict:
    """Describe a fetched source file (a .raw from NCEI, an object from GCS).

    Writes ``<file>.aa.json``: where the file came from and its content
    identity. Downstream tools record ``origin`` as the input's source;
    the identity is the same md5 a file without a sidecar would get, so
    the sidecar never changes any hash.
    """
    path = Path(path)
    content_id = identity.content_identity(path)
    digest = content_id.split(":")[1] if ":" in content_id else content_id
    info = dict(extra or {})
    if origin:
        info["origin"] = origin
    doc = {
        "schema": provenance.SCHEMA,
        "base": naming.base_of(path.name),
        "product": {"hash": digest, "short": digest[:naming.HASH_LEN], "kind": "source",
                    "role": "source", "name": path.name, "variant": None,
                    "content_id": content_id, "scientific_hash": digest},
        "inputs": [{"role": "origin", "uri": origin, "id": content_id}] if origin else [],
        "pipeline": [],
        "software": provenance.software_versions(),
        "created": {"at": provenance.now()},
        "extra": info | {"fetched_by": tool},
    }
    provenance.sidecar_path(path).write_text(
        __import__("json").dumps(doc, indent=2, sort_keys=True), encoding="utf-8")
    return doc


__all__ = ["ToolSpec", "Input", "Output", "Run", "add_common_flags", "canonical_params",
           "record_source", "ROLES"]
