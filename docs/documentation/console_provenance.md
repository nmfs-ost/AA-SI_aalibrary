# Console tools: products, names, hashes and provenance

Every `aa-*` console tool follows the same rules for what it names its
outputs, when it can reuse an earlier result, and what it records inside
each file. The rules live in one place, `aalibrary/console/_core/`, so
they are the same for all tools. This page explains them for users;
[Writing console tools](console_tool_authoring.md) explains them for
developers.

## In one paragraph

A file name has two parts: the **base** says *which data*
(`HB1603_EK60_20160703T060000-20160703T120000`), and the 8-character
**recipe hash** says *which processing*: the chain of scientific steps and
their options, written in a canonical form, without the data. So the same
processing applied to two surveys gives `HB1603_…_71957ca9.nc` and
`HB1701_…_71957ca9.nc`, and changing a scientific option gives a new hash.
Inside the file a second hash, the **product hash** (recipe + the identity
of the input data), decides whether an existing file can be reused.
`aa-metadata FILE` shows both and the full record.

## Base names

The base name says *which data* a product is about. It is set once, where
the EchoData is made, and every later product keeps it:

| Where | Base |
|---|---|
| `aa-nc x.raw`, `aa-ed x.raw` | the raw file's stem, verbatim (`D20160703-T060000`) |
| `aa-nc x.raw -o NAME.nc`, `aa-combine -o NAME.zarr` | `NAME` |
| `--base NAME` on any tool | `NAME`, from that tool on |
| `aa-combine` with neither | `combined` |

The base is stored in each product's provenance (`base`), so it survives
renames, copies and gs:// round trips. Tools never change it on their own.
A file without provenance contributes its stem.

## File names

The file name identifies the product; the extension identifies its
representation.

| Product | Name | Example |
|---|---|---|
| EchoData (aa-nc, aa-ed, aa-combine) | `<base>.nc` / `<base>.zarr` | `HB1603_EK60_…-…120000.nc` |
| scientific products (Sv, masks, MVBS, NASC, …) | `<base>_<recipe8>.<ext>` | `HB1603_…_71957ca9.nc` |
| renderings (aa-graph, aa-plot) | the name of the product shown | `HB1603_…_71957ca9.png` |
| explicit `-o PATH` | exactly as each tool always did | `aa-sv -o out.nc` → `out_Sv.nc` |

Defaults are written beside the input (the current directory when the
input came from gs://), or under `--dest DIR|gs://PREFIX/`.
`AA_NAMING=legacy` restores the old suffix names (`_Sv`, `_clean`,
`_mvbs`, …) for scripts that depend on them; provenance is embedded
either way.

## The two hashes

```
recipe  = sha256( canonical JSON of {
    tool, op, op_version,            # which computation (and its version)
    params,                          # scientific options, canonical form
    engine,                          # e.g. {"echopype": "0.11.1", "flox": "0.11.2"}
    variant,                         # e.g. "apply" for a masked copy
    inputs: [{role, recipe}]         # the recipe of each input; raw data is just "data"
} )
product = sha256( the same, but inputs: [{role, id}] )   # id = the input's actual identity
```

The **recipe** is in the file name. It chains: the recipe of the cleaned
Sv covers aa-nc → aa-sv → aa-clean with their options, so "clean after
calibration" and "clean alone" differ, but it never depends on which raw
file went in. Combining 3 or 300 files converted the same way is the same
recipe. Parameter files that define the query itself (EVR regions, EVL
lines) do count, by content.

The **product** hash adds the identity of the inputs. It is stored in the
file and in the object's metadata, and it is what reuse compares, so two
different datasets never share a product even when they share a recipe.

**Only scientific options count.** Each tool declares which options can
change its numbers (they are listed under *SCIENTIFIC OPTIONS* in
`aa-x --help`); paths, verbosity, compression, chunking and performance
switches never enter the hash.

**Spelling doesn't matter.** Values are canonicalized before hashing, so
these pairs give the same product:

| | |
|---|---|
| `--ping_num 20 --snr_threshold 3` | `--snr_threshold=3.0 --ping_num 20` |
| `--background_noise_max -125dB` | `--background_noise_max=-125.0dB` |
| `--range_bin 20m` | `--range_bin "20.0 m"` |
| option omitted | the same option given with its default value |
| `--waveform_mode FM` | `--waveform_mode BB` (echopype computes them identically) |

**Input identities (product hash).** An input that is itself a product is
identified by its product hash (`aa:<hash>`), which already covers
everything upstream. A file without provenance (a `.raw`) is identified by
its MD5 and size, the same value GCS reports for the object, so a file has
one identity whether it is read locally, through a mount, or from the
bucket. Region and line files (`--evr`, `--evl`) and `--echodata` files are
inputs too: their *content* counts, not their path.

Finding everything processed one way across surveys is then a file-name
search: `gsutil ls 'gs://bucket/derived_products/**_71957ca9.*'`.

**Library versions count.** echopype and other 0.x science libraries are
hashed by full version (patch releases have changed results); 1.x and
later by major.minor.

## Reuse

Before computing, each tool works out the output's name and both hashes.
If a file with that name already holds a product with that product hash,
the tool
prints `reusing … (identical product aa:xxxxxxxx already exists)` on
stderr, prints the path on stdout as usual, and does nothing else. A
pipeline re-run after a failure therefore only computes what is missing.

- `--force` (any tool) or `AA_REUSE=0` (whole pipeline) recomputes.
- A *different* existing file keeps each tool's old rule
  (`--no-overwrite`, `--overwrite`).
- Reuse works for gs:// targets too: the object's custom metadata carries
  `aa-product-hash`.

Guards against reusing the wrong thing:

- **Re-saved copies.** xarray copies attributes when a dataset is saved,
  so a product edited in a notebook would still claim the original hash.
  NetCDF and Zarr products therefore carry a small `aa_seal` group that
  xarray does not copy; provenance without a matching seal is ignored and
  the file is treated as a plain file (a note says so on stderr).
- **Rewritten objects.** Objects record the MD5 they were published with
  (`aa-content-md5`); an object rewritten afterwards is not reused.
- **Replaced raw files.** A `.raw` whose `.aa.json` sidecar describes
  different bytes loses the sidecar's identity and origin.
- **Half-written files.** Local outputs are written under a hidden
  temporary name and renamed into place when complete, so neither a
  reader nor a second copy of the same pipeline ever sees a partial file.

## Provenance inside the file

Every product carries a document like this (abridged):

```json
{
  "schema": "aa-provenance/1",
  "base": "HB1603_EK60_20160703T060000-20160703T120000",
  "product": {"hash": "71957ca9…", "kind": "sv", "role": "transform",
              "name": "HB1603_…_71957ca9.nc"},
  "inputs": [{"role": "source", "id": "aa:4be1…", "uri": "gs://…/HB1603_….nc"}],
  "sources": [{"name": "D20160703-T060000.raw",
               "origin": "s3://noaa-wcsd-pds/data/raw/Henry_B._Bigelow/HB1603/EK60/D20160703-T060000.raw"}],
  "pipeline": [
    {"tool": "aa-ed", "op": "echopype.open_raw", "params": {"sonar_model": "EK60"}, "count": 24},
    {"tool": "aa-combine", "op": "echopype.combine_echodata", "params": {}},
    {"tool": "aa-sv", "op": "echopype.calibrate.compute_Sv", "params": {}}
  ],
  "software": {"aalibrary": "…", "echopype": "0.11.1", "xarray": "…", "python": "…"},
  "created": {"at": "2026-09-30T16:01:02Z", "user": "…", "host": "…"}
}
```

The pipeline lists the scientific steps in order, with their canonical
options (not the literal command line, so flag order never shows).
`sources` lists the raw files at the root of the chain and where they came
from, carried forward through every step.

| Format | Where the document is |
|---|---|
| `.nc` | root attributes `aa_provenance` (plus `aa_recipe`, `aa_product_hash`, `aa_base`, `aa_tool`, `aa_product_kind`, a `history` line) and the `aa_seal` group |
| `.zarr` | root attributes, same names, plus `aa_seal`; consolidated metadata is refreshed |
| `.png` | text chunks |
| `.html` | a `<script type="application/json" id="aa-provenance">` block at the top of `<head>` |
| anything else | a `<file>.aa.json` sidecar |

Very long N:1 records (hundreds of combined files) are trimmed inside
the file and kept complete in a `.aa.json` sidecar.

Inspect with:

```bash
aa-metadata FILE                 # summary: product, base, inputs, sources, pipeline
aa-metadata FILE --verify        # recompute the hash; check it matches the file name
aa-metadata FILE --json          # the document
aa-metadata gs://bucket/x.nc     # works on objects too
... | aa-metadata --tee | ...    # in the middle of a pipe
```

## gs:// URIs

Every tool accepts `gs://bucket/key` wherever it accepts a path, and
writes to gs:// with `-o gs://…` or `--dest gs://PREFIX/`. The science
still runs on local files; the core moves bytes at the edges:

- **Reading** uses, in order: a fresh copy in the download cache, a
  gcsfuse mount that shows the object (e.g. `~/ggn-nmfs-aa-prod-1-data`),
  or a download into the cache (checked against the object's MD5).
- **Writing** stages the file in the cache, uploads it with its sidecar,
  stamps `aa-recipe`, `aa-product-hash`, `aa-base`, `aa-tool`, `aa-content-md5`
  and `aa-kind` (what the product is: one tool can write several kinds, as
  aa-annotate writes line and region files) into the object's custom metadata,
  and keeps the staged copy in the cache so
  the next stage reads it without downloading. Every product published to
  gs:// gets a `<object>.aa.json` sidecar beside it (a store: `<store>.aa.json`),
  so its provenance can be read without downloading the product.
- **`aa-metadata gs://…`** reads that sidecar first and trusts it only when it
  names the product hash the object itself carries; for a store without one it
  reads only the root metadata and the seal; only then does it download.
- `AA_GCS_FAKE_ROOT=/dir` makes a local directory play the object store
  (tests and rehearsals): nothing reaches GCS, stores included.
- `AA_CACHE_DIR` sets the cache (default `~/.cache/aalibrary`). Point it at
  a tmpfs to keep a whole pipeline in RAM.
- `AA_GCS_MOUNTS="bucket=/path"` declares mounts that `/proc/mounts`
  doesn't show.
- `aa-download` copies objects or prefixes to local files; `aa-upload FILE…
  gs://bucket/prefix/` uploads with the same metadata. Re-uploading a
  `.zarr` store replaces it completely.

## The pipe contract

- stdout carries results only: one path or URI per line (a few tools print
  a number or a token, as they always did). Everything else goes to stderr.
- An empty stdin from a pipe is an error (exit 1): it means the previous
  stage failed, and printing help into the pipe would feed it onward.
- `aa-x --help` is a curated summary in a fixed order (what it does,
  input, output, metadata, options, scientific options, files and URIs,
  pipeline, examples); `aa-x --help-all` is the full reference.

## Known limits

- Editing a product *in place* (opening it with netCDF4 in append mode
  and changing data) keeps its seal and hash. Save edits to a new file.
- Some spellings a library would reject (`20S`, `-125` without `dB`)
  canonicalize to a valid value; if that product already exists it is
  reused rather than the run failing. It can never return a different
  product.
- Spellings of the same quantity in different units (`1min` vs `60s`,
  `0.5nmi` vs `926m`) are different products: echopype stores the string
  itself in the file.
