# Writing and converting aa-* console tools

Every `aa-*` tool uses the shared core in `aalibrary/console/_core/`. It gives
all tools the same rules for the scientific hash, file names, provenance,
`gs://` URIs, reuse and `--help` (the user-facing rules are in
[Products, names and provenance](console_provenance.md)). This page is the
recipe. The reference implementations are:

| Role | Example | Shows |
|---|---|---|
| EchoData builder | `aa_nc.py` | starting the chain, `-o` with a forced extension |
| scientific transform | `aa_sv.py` | the standard transform; legacy `-o` suffix rule |
| representation | `aa_graph.py` | an image named after the product it shows |

## 1. Classify the tool

`ToolSpec.role` is one of:

- `source`: fetches data (aa-raw, aa-fetch). It writes a `<file>.aa.json`
  sidecar with `record_source()`.
- `echodata`: creates EchoData and starts the chain (aa-nc, aa-ed,
  aa-combine). Output is `<base>.nc` / `<base>.zarr`.
- `transform`: produces a new scientific product. Output is
  `<base>_<hash8>.<ext>`.
- `representation`: renders a product (aa-graph, aa-plot). Output is
  `<product name>.<ext>`.
- `sink`, `inspector`, `utility`, `interactive`: no product. These get the
  curated help only.

`kind` is a short product label: `sv`, `ts`, `mask`, `mvbs`, `nasc`,
`noise`, `depth`, `echometric`, `echodata`, `echogram`, `html` ...

## 2. Declare the scientific options

`ToolSpec.params` maps **argparse `dest`** to a canonicalizer from
`_core.canon`. Only these options enter the hash, so declare an option
when changing it can change the output values:

| Value | Canonicalizer |
|---|---|
| integer / float | `canon.integer`, `canon.number` |
| `"10dB"`, `"5m"`, `"0.5nmi"`, `"20s"` | `canon.quantity("dB")`, `canon.quantity("m")`, `canon.quantity()` |
| enumerated text | `canon.choice()`, `canon.choice("upper")`, `canon.choice("lower")` |
| flag | `canon.boolean` |
| `KEY=VALUE` list | `canon.kv()` |
| order-free list | `canon.set_of()` |
| order-significant list | `canon.ordered()` |
| comma-separated numbers | `canon.csv_numbers` |
| expression text | `canon.expression` |

**Never** declare paths, `-o`, `--quiet`, `--debug`, `--json`,
`--overwrite`/`--no-overwrite`, `--force`, compression or chunking, or
dask/flox performance knobs (`method`, `engine`, `chunk`). An option that
only matters when another is set is still declared; the hash stays
conservative, so it may miss a reuse but never reuses the wrong product.

When the tool parses a value itself (for example `--param k=v` through
`ast.literal_eval`), pass the parsed result instead:
`Run(SPEC, args, params={"param": parsed_dict})`.

A **file-valued** scientific option (EVR/EVL region files, `--echodata`)
is registered as an input, so its *content* enters the hash:
`run.param_file(path, role="regions")` or
`run.input(path, role="echodata")`.

`op` names the science call, e.g. `"echopype.clean.remove_background_noise"`.
`op_version` starts at 1; **bump it whenever the tool's own logic changes
the numbers.** `engines` lists the libraries whose version enters the hash
(default `("echopype",)`; add `"flox"` for binned reductions; `()` for
renderings). Two tools that run exactly the same computation can share
products with `ToolSpec(identity="aa-nc")` (aa-ed does this).

If the hashed dict comes from a parsed option (`--param`), fill in the
library's defaults for the hash (`inspect.signature`) so an explicit
default equals an omitted one; otherwise set `Help(hash_note=...)`, because
the generated help promises that explicit defaults don't change the hash.

## 3. The main() skeleton

```python
from aalibrary.console._core import (
    Help, Run, ToolSpec, add_common_flags, canon, naming, render, show_help, stdio,
)

SPEC = ToolSpec(name="aa-x", role="transform", kind="sv", op="echopype.x", op_version=1,
                params={"ping_num": canon.integer, "threshold": canon.quantity("dB")})
HELP = Help(summary=..., does=..., stdin=..., stdout=..., options=[...],
            science={...}, files=..., pipeline=..., examples=[...])

def print_help():                    # curated; the docs generator imports this name
    sys.stdout.write(render(SPEC, HELP, _build_parser()))

def print_help_full():               # the previous help text, kept for --help-all
    ...

def _build_parser():
    p = argparse.ArgumentParser(add_help=False)
    p.add_argument("input_path", type=str, nargs="?")      # str, not Path: gs:// must survive
    p.add_argument("-o", "--output_path", type=str)
    ...tool options unchanged...
    add_common_flags(p)                                      # --force --base --dest
    return p

def main():
    if len(sys.argv) == 1 and not stdio.stdin_is_piped():   # bare command on a terminal
        print_help(); sys.exit(0)
    parser = _build_parser()
    if show_help(SPEC, HELP, parser, full=print_help_full):  # -h/--help, --help-all
        sys.exit(0)
    args = parser.parse_args()
    token = stdio.one_input(args.input_path, SPEC.name)      # positional > stdin
    run = Run(SPEC, args)
    src = run.input(token)                                   # local path, provenance, base
    ...extension checks on src.local.suffix...
    explicit = <the tool's EXISTING -o rule applied to args.output_path, or None>
    out = run.plan(ext=".nc", explicit=explicit,
                   legacy=lambda: <the tool's EXISTING default path, from src.local>)
    if not out.remote and Path(out.target).resolve() == src.local.resolve():
        ...refuse, as before...
    if run.reusable(out):                                    # same product already there
        run.finish(out); return
    ...existing overwrite policy (e.g. --no-overwrite) here...
    compute(src.local, out.local)                            # write to out.local
    run.finish(out)                                          # provenance, upload, print
```

Rules the skeleton encodes:

- **`-o` behaves exactly as before.** Tools that appended their suffix to
  `-o` still do (`naming.with_stem_suffix(args.output_path, "_clean", ".nc")`).
  Tools that forced `.nc` still do (`naming.with_ext(...)`). Tools that
  used `-o` verbatim still do. `-o` may now be a `gs://` URI.
- **`legacy=`** is the tool's old default path. It is used only when
  `AA_NAMING=legacy`. Otherwise the default is the standard name beside the
  input, or the current directory when the input came from gs://.
- **Reuse comes before overwrite checks.** An identical product is never
  an "overwrite". `--no-overwrite` / `--overwrite` keep their meaning for a
  *different* existing file.
- **Write only to `out.local`** and never to `out.target`, and never derive
  other paths from `out.local`: for a local target it is a hidden temp
  sibling (`.<name>.aa-XXXX<ext>`) that `finish()` renames into place once
  provenance is embedded; for a gs:// target it is a staging file that
  `finish()` uploads. Use `out.target` in messages. A tool that must write
  the target itself (a Zarr store through fsspec, a file another library
  wrote) plans with `stage=False`.
- `finish()` exits 1 if nothing was written or the provenance can't be
  read back; `run.discard(out)` drops a planned output that won't be
  written. `run.exists(out)` / `run.conflicts(out)` answer "is something
  there?" / "is something *different* there?" for `--no-overwrite`.
- The core refuses a target equal to any registered input, and writes
  defaults beside the input as given (a symlink's folder, not its target's).
- **stdout contract:** `run.finish(out)` prints `out.target`, the same line
  the tool printed before (an absolute path, or now a gs:// URI). A tool
  whose stdout was something else (a number, a token, a repr) keeps it.
  Call `run.finish(out, emit=False)` and print as before.
- **Side outputs** (`--apply` masked copies, seafloor masks): plan each
  with `variant="apply"` (or another short name) and its own
  `kind`/`legacy`, then call `run.finish(side, emit=False)` and announce it
  on stderr (`aa-x: cleaned Sv: <side.target>`). The primary output is still
  the one printed.
- **Zero-input products** (aa-sound-speed `-o`): `Run(SPEC, args, base=NAME)`.
- **Remote Zarr written through fsspec**: embed with
  `provenance.write_zarr_group(group, run.document(out))` (attributes plus
  the `aa_seal` subgroup), consolidate, then `run.finish(out, embed=False,
  publish=False)`.
- **Batch tools** (several inputs, one output each): make a new `Run` per
  input inside the loop.
- **An empty piped stdin is an error.** `stdio.one_input` handles it: it
  prints to stderr and exits 1. Do not print help in that case; help on
  stdout would feed the next stage.

## 4. Curated help

`Help` answers, in this order: what the tool does, input, output,
metadata, options that matter, scientific options (generated from
`SPEC.params`, so it cannot drift), files and URIs, pipeline behavior,
and examples. Keep each section short and concrete. Name real defaults
and real variable names. The previous full text moves to
`print_help_full()`, shown by `--help-all`. Correct any statements in it
that the change made untrue, such as output names.

## 5. Checking a conversion

```bash
python -m aalibrary.utils.ek60_synth /tmp/w/D20160703-T060000.raw
aa-nc /tmp/w/D20160703-T060000.raw --sonar_model EK60 | aa-sv | aa-x ...
aa-x ... again with the flags reordered         # -> "reusing ..." on stderr
aa-metadata <output> --verify                   # chain, params, hash verified
AA_NAMING=legacy aa-x ...                       # old default name
AA_GCS_FAKE_ROOT=/tmp/w/gcs aa-x ... --dest gs://b/p/   # prints gs://b/p/<name>
aa-x --help ; aa-x --help-all
```

Then add the tool to `[project.scripts]` in `pyproject.toml`, to
`KNOWN_TOOLS` in `aalibrary/utils/_help/safety.py`, and to aa-guide's
index, and regenerate `docs/documentation/console_tools.md` with
`other/scripts/generate_console_tools_docs.ipynb`. `pytest tests/console`
and `aa-test` must pass.
