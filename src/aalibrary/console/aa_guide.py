#!/usr/bin/env python3
"""
aa-guide

Print the field guide to the aa-* console suite: how the pipes work, the
file-naming rule, provenance, reuse, gs:// URIs, every installed tool,
worked pipelines, and the common mistakes.

    aa-guide              the whole guide on stdout
    aa-guide | less       page through it
    aa-guide --help       what this tool does (curated help)

Reads no input and writes no files. Imports nothing heavy, so it works
offline and starts instantly.
"""

from __future__ import annotations

import sys

from aalibrary.console._core import Help, ToolSpec, render, show_help

SPEC = ToolSpec(name="aa-guide", role="utility", engines=())

HELP = Help(
    summary="Print the field guide to the aa-* tools and how to pipe them.",
    does=(
        "Prints one plain-text reference: how aa-* pipes pass file names, the "
        "file-naming rule (<base>.nc, <base>_<hash8>.<ext>), where provenance "
        "lives and how aa-metadata reads it, reuse and --force, gs:// URIs and "
        "the download cache, an index of every installed aa-* tool by stage, "
        "worked pipelines, and the mistakes that silently give wrong science."
    ),
    stdin="Nothing. aa-guide reads no input.",
    stdout="The guide text, about 350 lines. Page it or search it.",
    options=[
        ("(no options)", "aa-guide always prints the whole guide"),
    ],
    files="Reads and writes no files. Works offline.",
    pipeline=(
        "Not a pipeline stage. Pipe its output to a pager or to grep, never "
        "into another aa-* tool."
    ),
    examples=[
        "aa-guide | less",
        "aa-guide | grep -A4 'OUTPUT NAMING'",
        "aa-guide | grep aa-nasc",
    ],
    notes=["For one tool in detail run <tool> --help (what matters) or "
           "<tool> --help-all (every option)."],
)


GUIDE = r"""
================================================================================
 aa-* console suite: field guide and piping playbook
================================================================================
 One tool in detail:    <tool> --help       what matters, in a fixed order
                        <tool> --help-all   every option
 Ask in plain English:  aa-help "..."
 Check your install:    aa-test             (offline, about a minute)

HOW THE PIPES WORK
------------------
  * A pipe passes file NAMES (local paths or gs:// URIs), one per line,
    never the data itself.
  * Each stage takes its input as an argument or from stdin, writes a new
    file, and prints that file's location on stdout. Logs go to stderr.
  * A tool run with no arguments on a terminal prints its help, except
    these, which start working: aa-setup and aa-refresh (they reinstall!),
    aa-test, aa-guide, and the menu tools aa-find, aa-get, aa-help and
    aa-cruisepack.
  * A stage that receives an EMPTY pipe stops with an error (exit 1;
    aa-fetch exits 2): the stage before it failed, and that stage's error is
    printed above. Help text is never passed downstream as a file name.
    Two exceptions: aa-request reads an empty pipe as an empty request
    (prints "requests: []", exit 0), and aa-combine falls back to the
    EchoData files in --workdir (default: the current directory).

    aa-nc D20160703-T060000.raw --sonar_model EK60 | aa-sv | aa-graph

  Keep an intermediate product in a shell variable and branch from it:

    SV=$(aa-nc D20160703-T060000.raw --sonar_model EK60 | aa-sv)
    aa-mvbs "$SV" --range_bin 10m --ping_time_bin 60s
    aa-graph "$SV" --vmin -80 --vmax -30

OUTPUT NAMING
-------------
  The file name identifies the product; the extension says how it is stored.

  Base name  Set once, at the source, and carried inside every product's
             provenance. No tool renames it.
               - the raw file's stem (NCEI file names are kept verbatim), or
               - your choice: -o or --base on aa-nc, aa-ed or aa-combine (the
                 tools that create EchoData), or --base NAME on any tool.

  Names      EchoData (aa-nc, aa-ed, aa-combine)  <base>.nc | <base>.zarr
             scientific products                  <base>_<hash8>.<ext>
             renderings (aa-graph, aa-plot)       named after the product
                                                  they show: <base>_<hash8>.png
             explicit -o PATH                     as each tool always did

  <hash8> is the first 8 hex digits of the recipe: the whole chain of
  scientific steps and their options, but not the data. The base says which
  data, the hash says which processing: the same processing on two surveys
  gives HB1603_..._71957ca9.nc and HB1701_..._71957ca9.nc, and different
  processing of the same data never overwrites it. A second hash inside the
  file (the product hash: recipe + data) decides reuse. aa-metadata FILE
  shows both.

  Where      Beside the input by default; in the current directory when the
             input came from gs://. --dest DIR or --dest gs://PREFIX/ puts
             the standard name somewhere else.

  Example, one raw file through four stages (your hashes will differ):
    D20160703-T060000.raw             the source file
    D20160703-T060000.nc              aa-nc     EchoData
    D20160703-T060000_35e8864f.nc     aa-sv     Sv
    D20160703-T060000_41b27617.nc     aa-clean  Sv, Sv_noise, Sv_corrected
    D20160703-T060000_41b27617.png    aa-graph  image of the aa-clean product

  -o means what it always meant for each tool. Some add their suffix to it
  (aa-sv -o x.nc writes x_Sv.nc, aa-clean -o x.nc writes x_clean.nc), aa-nc
  forces .nc, others use it verbatim; see <tool> --help. -o may be gs://.

  AA_NAMING=legacy restores the old default names (x_Sv.nc, x_Sv_clean.nc,
  x_Sv_clean_mvbs.nc, ...) for scripts that depend on them. Provenance is
  embedded either way.

PROVENANCE
----------
  Every product records what made it: the source file (and its NCEI or GCS
  origin), each scientific step with its options in canonical form, the
  software versions, and the product hash. The record travels in the file:
    .nc, .zarr     attributes aa_provenance, aa_product_hash, aa_base, ...
    .png           PNG text chunks
    .html          <script type="application/json" id="aa-provenance">
    anything else  a <file>.aa.json sidecar beside it (aa-raw, aa-fetch and
                   aa-download write one for each plain file they download,
                   e.g. a .raw: its origin and checksum)
  Read it with aa-metadata:
    aa-metadata FILE               base, hash, inputs, every step and option
    aa-metadata FILE --verify      recompute the hash from the record
                                   (exit 4 if it does not match)
    aa-metadata FILE --json        the whole record, one line of JSON
    ... | aa-metadata --tee | ...  summary on stderr, path passed through

REUSE
-----
  Before computing, a tool works out the hash of the product it is about to
  make. If that product already exists at the output location it prints
  "<tool>: reusing <path> (identical product aa:<hash8> already exists ...)"
  on stderr and passes the path on without recomputing. Re-running a
  pipeline therefore only computes the stages whose input or options
  changed, and the stages after them. Flag order, alias spellings and
  defaults typed out explicitly do not change the hash.
    --force      recompute this stage anyway
    AA_REUSE=0   recompute in every tool run from this shell
  Reuse also works against a bucket: the hash is stamped into each uploaded
  object's metadata (aa-product-hash). aa-upload skips an object that
  already holds the same product or the same bytes; aa-download skips a
  local file with the same content (GCS MD5).

gs:// URIS
----------
  Anywhere a tool reads a path it also reads gs://bucket/key, as an argument
  or on stdin. Anywhere it writes a file it can write to gs://: -o
  gs://bucket/x.nc, or --dest gs://bucket/prefix/.
  Reading   Through a gcsfuse mount when one covers the object (mounts in
            /proc/mounts are found automatically; AA_GCS_MOUNTS=bucket=/path
            names others). Otherwise the object is downloaded once into the
            cache and reused there until the object changes.
  Writing   The tool writes a local file, uploads it with its provenance,
            and keeps a copy in the cache, so the next stage does not
            download it again.
  AA_CACHE_DIR   the download cache (default ~/.cache/aalibrary). Put it on
                 fast local disk.
  Credentials    Google Application Default Credentials:
                 gcloud auth application-default login  (aa-setup runs it)

    aa-nc gs://my-bucket/raw/D20160703-T060000.raw --sonar_model EK60 \
      | aa-sv --dest gs://my-bucket/derived/ \
      | aa-graph --dest gs://my-bucket/figures/

TOOL INDEX
----------
  "A -> B": reads A (argument or stdin) and prints the location of B.
  [start]  begins a pipe; reads no file path
  [end]    prints text that is not a file path; last in a pipe
  [menu]   interactive; run it on its own, never inside a pipe
  [alone]  setup or maintenance; run it on its own, never inside a pipe

FIND AND FETCH DATA
  aa-find             [menu] browse NCEI: vessel > survey > sonar > .raw;
                      downloads (via aa-raw) or plots a file for you
  aa-raw              [start] download one .raw from NCEI -> its path
  aa-request          [start] write or --check a fetch request (vessel,
                      survey, instrument, time windows) -> the YAML on
                      stdout, or its path with -o
  aa-get              [start] build a fetch request in terminal menus (the
                      choices come from BigQuery) -> its path;
                      aa-get | aa-fetch (the menus go to the terminal)
  aa-fetch            fetch request YAML -> downloads the matching files ->
                      the download directory
  aa-download         gs:// objects -> local files -> one local path per
                      input. A prefix ending in / is copied as a folder and
                      printed once (--pattern '*.raw' filters it). A file
                      already there with the same MD5 is kept. Sidecars come
                      along; a plain file (a .raw) gets one recording its
                      gs:// origin.
  aa-sonar            [end] .raw -> its sonar model (EK60, EK80, ...), e.g.
                      aa-nc x.raw --sonar_model "$(aa-sonar x.raw)"

MAKE ECHODATA (starts the provenance chain)
  aa-nc               .raw -> EchoData <base>.nc  (--sonar_model required)
  aa-ed               a .raw file name (looked up and downloaded from NCEI),
                      a local .raw, or a directory of .raw -> EchoData .nc
                      (directory in, same directory out)
  aa-combine          several EchoData files or a directory -> one
                      <base>.zarr (or .nc); run --check first for QC

CALIBRATE AND ADD COORDINATES
  aa-sv               EchoData -> Sv (--ecs FILE, or --cal-param /
                      --env-param KEY[@38kHz]=VALUE overrides)
  aa-ts               EchoData -> TS (target strength)
  aa-ecs              [end] EchoData -> the calibration values echopype will
                      use; --write: an Echoview .ecs file of them (with
                      --cal-param/--env-param/--values changes)
  aa-depth            Sv -> Sv with a depth coordinate
  aa-location         Sv -> Sv with latitude/longitude (--echodata FILE)
  aa-splitbeam-angle  Sv -> Sv with split-beam angles (--echodata FILE
                      --waveform-mode CW|BB --encode-mode complex|power)
  aa-swap-freq        dataset -> same data indexed by frequency_nominal
                      instead of channel
  aa-coerce-time      dataset -> same data with ping_time strictly
                      increasing (--report says whether it was reversed)

NOISE, MASKS AND LINES
  aa-clean            Sv -> Sv plus Sv_noise and Sv_corrected (background
                      noise removed). The variable Sv is NOT changed.
  aa-noise-est        Sv -> background-noise estimate (Sv_noise)
  aa-impulse          Sv -> impulse-noise mask
  aa-min              Sv -> impulse-noise mask, stored under the variable
                      name Sv (minimal variant, no --apply)
  aa-transient        Sv -> transient-noise mask
  aa-attenuated       Sv -> attenuated-signal mask
  aa-detect-transient Sv -> transient-noise mask (--method fielding|matecho
                      --param k=v ...)
  aa-detect-shoal     Sv -> shoal mask (--method weill --param k=v ...)
  aa-detect-seafloor  Sv -> bottom line (--method basic|blackwell --param
                      k=v ...; --emit-mask, --apply)
  aa-freqdiff         Sv -> frequency-differencing mask
                      (--freqABEq '38kHz - 120kHz>=12dB')
  aa-evl              Sv -> Sv masked above, below or between Echoview
                      .evl lines (--evl)
  aa-evr              Sv -> Sv kept only inside Echoview .evr regions
                      (--evr); [menu] without --evr: draw regions in a browser
  aa-mask             Sv -> Sv with masks applied (--remove FILE: drop where
                      the mask is True; --keep FILE: keep only there;
                      --mask FILE: the aa-* mask's own meaning)
  aa-threshold        Sv -> Sv with --min DB (empty water) / --max DB
  aa-crop             Sv/MVBS/mask -> a time, range, ping or channel window
  aa-annotate         drawn shapes (.json) or a detected bottom -> Echoview
                      .evl/.evr product; --json reads them back
  Mask tools print the MASK. With --apply they also write a masked copy of
  Sv as a side file; the next stage in the pipe still receives the mask.
  aa-impulse, aa-min, aa-transient and aa-attenuated need depth: put
  aa-depth before them (see MISTAKES).

GRID AND INTEGRATE
  aa-mvbs             Sv -> MVBS on a range x time grid
  aa-mvbs-index       Sv -> MVBS on a sample x ping grid
  aa-nasc             Sv -> NASC in range x distance bins (the input needs
                      depth, latitude and longitude; see example 3)
  aa-integrate        Sv -> Echoview-style integration CSV: cells
                      (--interval 0.5nmi|5min|100, --layer 10), exclusion
                      lines (--surface, --bottom), bad-data regions (--bad),
                      thresholds; --by regions|region-cells (PRC_NASC)

ECHOMETRICS (echopype.metrics along echo_range; Sv -> one value per ping
             and channel)
  aa-abundance        area backscattering strength Sa (dB re 1 m2 m-2)
  aa-aggregation      index of aggregation IA (m-1)
  aa-center-of-mass   center of mass CM (m): sv-weighted mean range
  aa-dispersion       inertia I (m2): spread about the center of mass
  aa-evenness         equivalent area EA (m)

SEAWATER (no input file)
  aa-sound-speed      [end] --temperature --salinity --pressure -> m/s
  aa-absorption       [end] --frequency HZ[,HZ...] (+ T, S, P, pH) -> dB/m

LOOK AND CHECK
  aa-graph            product -> echogram PNG named after the product
  aa-plot             product -> interactive HTML echogram
  aa-tiles            product -> echogram tile pack (.tiles) for viewers
                      (the Workbench's Echogram panel)
  aa-show             [end] print a file's xarray summary (dimensions,
                      coordinates, variables); the full channel names follow
                      on stderr, under "channels:"
  aa-metadata         [end] show or --verify a product's provenance
                      (with --tee it passes the path through instead)
  aa-store            [end] describe (info) or check (verify) a .zarr store

STORE AND SHARE
  aa-upload           files -> GCS. ... | aa-upload gs://bucket/prefix/
                      uploads each file with its sidecar, stamps the product
                      hash into the object's metadata, skips objects that
                      already hold the same product or bytes, and prints the
                      gs:// URIs (--tee: the local paths, so the pipe goes
                      on). Older modes: the survey layout (--ship_name
                      --survey_name --sonar_model) or --as-is; they print
                      the input path unchanged.
  aa-cruisepack       [menu] find CruisePack SQLite databases on this
                      computer and upload them to GCS

SET UP AND HELP
  aa-guide            [end] this guide
  aa-help             [menu] ask in plain English; plans (and can run)
                      aa-* pipelines. Uses Vertex AI.
  aa-setup            [alone] (re)install the AA-SI GCP workstation
                      environment and log in to gcloud
  aa-refresh          [alone] reinstall aalibrary and AA-SI-KMEANS from
                      GitHub main; this is how new tools appear on your PATH
  aa-test             [end] offline self-test on synthetic EK60 data

EXAMPLES
--------
1) Raw file -> Sv -> echogram, then grid it
     SV=$(aa-nc D20160703-T060000.raw --sonar_model EK60 | aa-sv)
     aa-graph "$SV" --vmin -80 --vmax -30
     aa-mvbs "$SV" --range_bin 10m --ping_time_bin 60s | aa-graph

2) Background-noise removal, and the echogram of the cleaned values
     aa-nc D20160703-T060000.raw --sonar_model EK60 | aa-sv | aa-clean \
       | aa-graph --var Sv_corrected --vmin -80 --vmax -30

3) Position, depth, NASC (aa-location reads the NMEA fixes in the EchoData)
     ED=$(aa-nc D20160703-T060000.raw --sonar_model EK60)
     SV=$(aa-sv "$ED")
     aa-location "$SV" --echodata "$ED" | aa-depth \
       | aa-nasc --range_bin 20m --dist_bin 0.5nmi
     aa-splitbeam-angle "$SV" --echodata "$ED" --waveform-mode CW \
       --encode-mode power

4) EK80 data (both EK80 flags are required by aa-sv)
     aa-nc D20230801-T120000.raw --sonar_model EK80 \
       | aa-sv --waveform_mode BB --encode_mode complex | aa-graph

5) Noise masks, frequency differencing, shoals, seafloor
     aa-depth "$SV" | aa-transient --apply
     aa-freqdiff "$SV" --freqABEq '38kHz - 120kHz>=12dB'
     aa-show "$SV"      # full channel names on stderr, after the summary
     aa-detect-shoal "$SV" --method weill \
       --param var_name=Sv "channel=<a channel name>" thr=-70 minvlen=5 \
       minhlen=3 --apply
     aa-depth "$SV" | aa-detect-seafloor --method basic \
       --param var_name=Sv "channel=<a channel name>" threshold=-50

6) Repair reversed ping times before gridding
     aa-nc D20160703-T060000.raw --sonar_model EK60 | aa-sv \
       | aa-coerce-time --report | aa-mvbs --range_bin 10m --ping_time_bin 20s

7) Several raw files -> one transect (aa-combine -o sets the base name)
     aa-ed ./raw/ | aa-combine -o HB1603_T1.zarr | aa-sv | aa-graph
     # HB1603_T1.zarr, then HB1603_T1_<hash8>.nc and HB1603_T1_<hash8>.png

8) From NCEI, by file name or by time window
     echo HB1603_L1-D20160703-T183957.raw | aa-ed | aa-sv | aa-graph
     aa-request --vessel Henry_B._Bigelow --survey HB1603 --instrument EK60 \
         --from 2016-07-03 --to 2016-07-04 -o request.yaml \
       | aa-fetch | aa-ed | aa-combine | aa-sv | aa-graph

9) Products in a bucket, and local copies back
     aa-nc D20160703-T060000.raw --sonar_model EK60 \
       | aa-sv --dest gs://my-bucket/derived/ \
       | aa-graph --dest gs://my-bucket/figures/
     aa-nc D20160703-T060000.raw --sonar_model EK60 | aa-sv | aa-clean \
       | aa-upload gs://my-bucket/derived/       # prints the gs:// URI
     aa-sv D20160703-T060000.nc | aa-upload gs://my-bucket/derived/ --tee \
       | aa-graph                                # continues locally
     aa-download gs://my-bucket/derived/D20160703-T060000_35e8864f.nc \
       --dest copy/
     aa-download gs://my-bucket/raw/HB1603/ --pattern '*.raw' --dest data/ \
       | aa-ed | aa-combine -o HB1603_T1.zarr

10) Where did this file come from?
     aa-metadata D20160703-T060000_41b27617.png
     aa-metadata D20160703-T060000_41b27617.nc --verify
     aa-nc x.raw --sonar_model EK60 | aa-sv | aa-metadata --tee | aa-graph

MISTAKES THAT GIVE WRONG SCIENCE OR NO OUTPUT
---------------------------------------------
  * aa-clean does not overwrite Sv. The cleaned values are in Sv_corrected,
    and aa-mvbs, aa-mvbs-index and aa-nasc average the variable Sv, so
    "aa-clean | aa-mvbs" gives the same MVBS as "aa-sv | aa-mvbs". aa-graph
    also draws Sv unless you pass --var Sv_corrected.
  * aa-clean, aa-mvbs, aa-nasc, the masks and the metrics need calibrated
    Sv (aa-sv output), not the EchoData file from aa-nc.
  * aa-nasc needs depth, latitude and longitude in its input:
    aa-location --echodata ... | aa-depth | aa-nasc.
  * aa-location and aa-splitbeam-angle need --echodata: the Sv file alone
    has no navigation or beam data.
  * EK80: aa-sv needs both --waveform_mode and --encode_mode. EK60 ignores
    them (the Sv is identical), but don't pass them: they still change the
    product hash, so the file gets a new name and is not reused.
  * aa-nc needs --sonar_model. aa-sonar reads it from the file.
  * Mask tools print the mask, not masked Sv (see NOISE, MASKS AND LINES).
  * aa-impulse, aa-min, aa-transient and aa-attenuated default to the depth
    axis and fail on plain aa-sv output ("requires `depth`"). Run aa-depth
    first: ... | aa-sv | aa-depth | aa-transient. aa-transient and
    aa-attenuated also accept --range-var echo_range; aa-impulse and aa-min
    do not (echopype fails on 'depth_bins'), and they also fail when the
    channels have different numbers of samples.
  * aa-freqdiff frequencies are unquoted: '38kHz - 120kHz>=12dB'.
  * aa-detect-shoal --method weill and aa-detect-seafloor --method basic
    need --param var_name=Sv "channel=<name>" (aa-show prints the full
    names on stderr). aa-detect-seafloor also needs depth: aa-depth first.

TIPS
----
  * Quote paths that contain spaces: aa-mvbs "My Cruise 2024_35e8864f.nc"
  * The last line printed is the final product: OUT=$( ... | aa-graph )
  * Re-running is cheap: unchanged stages are reused, not recomputed.
  * A new tool is "command not found"? Run aa-refresh (or, in a git clone,
    pip install -e .) so its entry point is installed.
  * Something odd? aa-test checks the whole chain offline in about a minute.

================================================================================
"""


def print_console_tools_reference():
    """Print the guide (kept under its old name for existing callers)."""
    sys.stdout.write(GUIDE.lstrip("\n"))


def print_help():
    """Curated help (also used by the docs generator)."""
    sys.stdout.write(render(SPEC, HELP))


def print_help_full():
    """--help-all: aa-guide has no options, so its complete reference is the guide."""
    print_console_tools_reference()


def main():
    if show_help(SPEC, HELP, None, full=print_help_full):
        sys.exit(0)
    extra = [a for a in sys.argv[1:] if a != "--"]
    if extra:
        print(f"aa-guide: takes no arguments (got {' '.join(extra)}); see aa-guide --help",
              file=sys.stderr)
        sys.exit(2)
    try:
        print_console_tools_reference()
    except BrokenPipeError:          # aa-guide | head
        sys.stderr.close()
    sys.exit(0)


if __name__ == "__main__":
    main()
