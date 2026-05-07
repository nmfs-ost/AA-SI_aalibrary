from aalibrary import quick_test
import sys


def print_console_tools_reference():
    reference = r"""
================================================================================
 Active Acoustics Console Suite — Field Guide & Piping Playbook
================================================================================

(For details on any one tool:  <tool> --help)

This suite is designed for **pipeline-style** work. Each tool:
  • **Reads** a single NetCDF path (".nc") either as a positional arg or from **stdin**.
  • **Writes** a new NetCDF file to disk (never just to stdout).
  • **Prints** the absolute path of its primary output to **stdout** so the next tool
    can pick it up. This means our “pipes” pass **filenames**, not raw data.

PIPING MODEL (IMPORTANT)
------------------------
1) Chain tools with pipes; each tool emits one path, which becomes the next tool’s input:
     aa-nc RAW.raw --sonar_model EK60 \
       | aa-sv \
       | aa-clean \
       | aa-mvbs

2) You can capture intermediate outputs explicitly:
     CLEAN=$( aa-nc RAW.raw --sonar_model EK60 | aa-sv | aa-clean )
     aa-mvbs "$CLEAN"

3) If you need to fan out to multiple tools, use `tee` and subshells:
     NC=$( aa-nc RAW.raw --sonar_model EK60 )
     ( aa-sv   "$NC" | aa-clean | aa-nasc   ) &
     ( aa-sv   "$NC" | aa-clean | aa-mvbs   ) &
     wait

OUTPUT NAMING & ON-DISK BEHAVIOR
--------------------------------
• If you don’t pass -o/--output_path, tools create sensible defaults by **suffixing**
  the input stem (e.g., “input.nc” → “input_clean.nc”, “input_mvbs.nc”, etc.).
• Tools **always write NetCDF to disk**, then emit the resulting path to stdout.
• Most tools accept stdin. If you run a tool with no args and no stdin, it prints
  a friendly help page and exits.

-------------------------------------------------------------------------------
 TOOL INDEX (A–Z by category)
-------------------------------------------------------------------------------

INGEST & CONVERSION
  aa-raw             : Manage raw-file logistics (download/upload/metadata helpers).
  aa-nc              : Convert RAW → NetCDF (choose sonar model, etc.).
  aa-swap-freq       : Swap 'channel' dimension with 'frequency_nominal'.

CALIBRATION & CORE DERIVATIVES
  aa-sv              : Compute calibrated Sv and write Sv dataset.
  aa-ts              : Compute target strength (TS) dataset.
  aa-depth           : Add depth coordinate(s) (if applicable to your stack).
  aa-location        : Add geographic location (lat/lon) from EchoData into Sv.
  aa-splitbeam-angle : Add alongship/athwartship split-beam angles to Sv.

CLEANING & MASKING
  aa-clean           : Remove background noise (ping/range windows, SNR threshold).
  aa-impulse         : Mask impulse noise.
  aa-transient       : Mask transient noise.
  aa-attenuated      : Mask attenuated signal.
  aa-detect-transient: Detect transient noise (dispatcher; emits mask; optional apply).
  aa-detect-shoal    : Detect shoals (dispatcher; emits mask; optional apply).
  aa-detect-seafloor : Detect seafloor bottom line; optional bottom mask & apply.
  aa-freqdiff        : Frequency differencing mask (e.g., “38kHz − 120kHz ≥ 12 dB”).
  aa-min             : (Your existing minimal/masking helper, if present.)
  aa-mask            : (If present in your stack; generic masking helper.)

GRIDDING & SUMMARIES
  aa-mvbs            : Compute MVBS (physical bins).
  aa-mvbs-index      : Compute MVBS using index binning (range_sample / ping).
  aa-nasc            : Compute NASC (integral of Sv over range & distance).

QC (TIME CONSISTENCY & REPORTS)
  aa-exist-reversed  : Check if a time coord (e.g., ping_time) has reversals.
  aa-coerce-time     : Force time to be strictly increasing (repairs reversals).
  aa-show            : Quick inspect / summarize (if present in your stack).
  aa-plot            : Plot/visual QC (if present).

METRICS (Echopype metrics.* over echo_range)
  aa-abundance       : Abundance metric.
  aa-aggregation     : Aggregation metric.
  aa-center-of-mass  : Center of mass (COM).
  aa-dispersion      : Dispersion (inertia).
  aa-evenness        : Evenness (Equivalent Area, EA).

UTILITIES (Seawater & acoustics)
  aa-sound-speed     : Seawater sound speed (m/s) via Echopype UWA utils.
  aa-absorption      : Seawater absorption (dB/m) vs frequency & conditions.

DISCOVERY, HELPERS & SETUP
  aa-find            : Find datasets / convenience discovery.
  aa-help            : Print this reference and tool tips.
  aa-setup           : Prepare AA-SI environments (e.g., GCP Workstations).
  aa-test            : Self-tests / sanity checks for the suite.

-------------------------------------------------------------------------------
 EXAMPLES — PRACTICAL PIPELINES
-------------------------------------------------------------------------------

1) RAW → NC → Sv → Clean → MVBS
   (One-liner with sensible defaults; no intermediate filenames required.)
     aa-nc cruise.raw --sonar_model EK60 \
       | aa-sv \
       | aa-clean \
       | aa-mvbs

   Resulting files (typical):
     cruise.nc               # from aa-nc
     cruise_sv.nc            # from aa-sv
     cruise_sv_clean.nc      # from aa-clean
     cruise_sv_clean_mvbs.nc # from aa-mvbs
   The **last line printed** to your terminal is the path to the MVBS file.

2) Add geolocation, angles, and NASC
     SV=$( aa-nc raw.raw --sonar_model EK80 | aa-sv )
     LOC=$( aa-location "$SV" )
     ANG=$( aa-splitbeam-angle "$LOC" --waveform-mode BB --encode-mode complex )
     aa-nasc "$ANG" --range-bin 20m --dist-bin 0.5nmi

3) Frequency differencing followed by shoal detection and masked Sv export
     SV=$( aa-nc file.raw --sonar_model EK80 | aa-sv | aa-clean )
     MASK=$( aa-freqdiff "$SV" --freqABEq '"38.0kHz" - "120.0kHz">=12.0dB' )
     aa-detect-shoal "$SV" --method echoview --param threshold= -60dB min_len=5 \
       --apply  # writes *_detect_shoal_cleaned.nc

4) Time QC & repair, then MVBS
     NC=$( aa-nc raw.raw --sonar_model EK60 )
     REV=$( aa-exist-reversed "$NC" --time-name ping_time )
     [ "$REV" = "True" ] && NC=$( aa-coerce-time "$NC" --time-name ping_time )
     aa-mvbs "$NC" --range-bin 10m --dist-bin 1nmi

-------------------------------------------------------------------------------
 TIPS & GOTCHAS
-------------------------------------------------------------------------------
• Our “pipes” pass **filenames** (paths), not bytes. Every tool writes a file, then
  prints the new path. That keeps memory usage tiny and makes workflows restartable.
• If your path contains spaces, **quote it**:  aa-mvbs "My Cruise 2024.nc"
• Need the final path in a shell variable?  OUT=$( aa-clean input.nc )
• Want absolute paths for downstream tools? That’s what we print by default.
• Overwrite behavior:
    – With no -o: new files are created with suffixed stems.
    – With -o path: the tool will write to that exact path (and still print it).
• Most cleaning/detection tools support **--apply** to emit a cleaned Sv file in
  addition to their mask/line product.

================================================================================
"""
    print(reference)



def main():

    # Call the function to display the reference
    print_console_tools_reference()
    sys.exit(0)


if __name__ == "__main__":
    main()
