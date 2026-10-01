# Console Tools Documentation

## aa_absorption

```bash
aa-absorption — Seawater absorption coefficient (dB/m) at one or more
  frequencies.
  [scientific transform (hashed) · product: absorption]

WHAT IT DOES
  Evaluates echopype.utils.uwa.calc_absorption: Ainslie & McColm (AM,
  default), Francois & Garrison (FG) or the AZFP formula, from frequency,
  temperature, salinity, pressure and pH. Reads no file. Without -o it only
  prints the result; with -o it writes a NetCDF product.

INPUT (argument or stdin)
  Nothing. All inputs are options.

OUTPUT (stdout)
  Without -o: one frequency prints a bare number (e.g. 0.006275527046960815);
  a list prints numpy's array text (e.g. [0.00627553 0.04699969], rounded for
  display), the same text as always. With -o: the NetCDF's absolute path (or
  gs:// URI), whose values are full precision.

METADATA
  Without -o nothing is written and there is no provenance. With -o the NetCDF
  holds 'absorption' (units dB m-1) on a 'frequency' coordinate (Hz) and the
  global attributes temperature_degC, salinity_psu, pressure_dbar, pH,
  formula_source, tool, plus aa provenance (this step with its canonical
  options, no inputs; see aa-metadata). Its base name is --base or the -o
  file's stem.

OPTIONS
  --frequency HZ[,HZ...]  REQUIRED. e.g. 38000 or 38000,120000
  --temperature DEGC	  temperature in deg C (default 27)
  --salinity PSU		  salinity in PSU (default 35)
  --pressure DBAR		 pressure in dbar (default 10)
  --pH PH				 seawater pH (default 8.1)
  --formula-source NAME   AM (default), FG or AZFP
  -o, --output_path PATH  write a NetCDF instead of printing; .nc is forced.
						  Local path or gs:// URI.
  --quiet				 warnings and errors only on stderr
  --force				 with -o: recompute even if an identical file is
						  already there
  --base NAME			 with -o: base name recorded in the provenance

SCIENTIFIC OPTIONS (change the product hash)
  --frequency	   Hz; list order is kept (it is the output's coordinate
					order).
  --temperature	 deg C. (default: 27.0)
  --salinity		PSU. (default: 35.0)
  --pressure		dbar. (default: 10.0)
  --pH			  seawater pH. (default: 8.1)
  --formula-source  AM, FG or AZFP. (default: AM)
  Flag order, alias spellings and explicit defaults do not change the hash.

FILES & URIs
  Writes only with -o: exactly that path with its extension forced to .nc.

IN A PIPELINE
  A starting point, not a filter: use the number in shell substitution, e.g.
  a=$(aa-absorption --frequency 38000 --temperature 4 --quiet).

EXAMPLES
  aa-absorption --frequency 38000 --temperature 4 --salinity 34 --pressure 50
  aa-absorption --frequency 18000,38000,120000 -o alpha.nc

MORE
  --help-all  the complete reference, every option
```

## aa_abundance

```bash
aa-abundance — Area backscattering strength (Sa, dB re 1 m2 m-2) of each ping.
  [scientific transform (hashed) · product: echometric]

WHAT IT DOES
  Runs echopype.metrics.abundance on calibrated Sv: Sa = 10*log10(sum sv*dz)
  over range_sample, with sv = 10^(Sv/10) and dz the spacing of the range
  variable (Urmy et al. 2012). Every range sample in the file is integrated;
  trim or mask the Sv first to exclude surface or seafloor. Writes one
  variable, 'abundance' (units dB re 1 m2 m-2), on channel x ping_time. Not
  NASC: no 4*pi*1852^2 factor (use aa-nasc for that).

INPUT (argument or stdin)
  One Sv NetCDF path or gs:// URI (argument, or one line on stdin), e.g. from
  aa-sv or aa-clean. It must contain 'Sv' (dB) on a range_sample dimension and
  the range variable named by --range-label (default echo_range).

OUTPUT (stdout)
  The output file's absolute path (or gs:// URI).

METADATA
  Reads the input's provenance, appends this step with its canonical
  scientific options, and embeds it all in the output (NetCDF attributes
  aa_provenance, aa_recipe, aa_product_hash, aa_base, aa_tool, history). Two
  hashes: the recipe (this step and every step before it, without the data:
  the <hash8> in the name, the same for any data processed this way) and the
  product hash (this recipe applied to this input: decides reuse). The base
  name is carried through unchanged. Inspect with: aa-metadata FILE

OPTIONS
  -o, --output_path PATH  Explicit output, used exactly as given (no suffix
						  added). Local path or gs:// URI.
  --range-label NAME	  variable holding range in metres (default:
						  echo_range)
  --try-calibrate		 if that variable is missing, open the input as
						  EchoData and compute Sv first (echopype defaults; no
						  EK80 modes)
  --no-overwrite		  exit 1 if the output exists and is a different
						  product (an identical one is reused)
  --quiet				 warnings and errors only on stderr

SCIENTIFIC OPTIONS (change the product hash)
  --range-label	Variable used as range (m) for dz and the integral.
				   (default: echo_range)
  --try-calibrate  Compute Sv from EchoData first when the range variable is
				   missing; changes what is analysed.
  Flag order, alias spellings and explicit defaults do not change the hash.

FILES & URIs
  Reads a flat Sv NetCDF, local or gs:// (through a gcsfuse mount when one
  covers it, otherwise downloaded once to the cache). Writes <base>_<hash8>.nc
  beside the input (current directory for gs:// input), or -o, or --dest.
  AA_NAMING=legacy restores <input stem>_abundance.nc. An identical earlier
  result is reused.

IN A PIPELINE
  After calibration: aa-nc | aa-sv | aa-abundance. The output is a 2-D metric
  (channel x ping_time), not Sv, so it ends the Sv chain.

EXAMPLES
  aa-nc D20160703-T060000.raw --sonar_model EK60 | aa-sv | aa-abundance
  aa-abundance sv.nc -o sa.nc --no-overwrite

COMMON OPTIONS
  --force				 recompute even if an identical product already
						  exists
  --base NAME			 name outputs after NAME instead of the input's base
						  name
  --dest DIR|gs://PREFIX  write the default-named output there instead of
						  beside the input
  --help-all			  the complete reference, every option
```

## aa_aggregation

```bash
aa-aggregation — Index of aggregation (IA, m^-1) of backscatter along range,
  per ping.
  [scientific transform (hashed) · product: echometric]

WHAT IT DOES
  Runs echopype.metrics.aggregation on calibrated Sv: IA = 1/EA = sum(sv^2*dz)
  / (sum sv*dz)^2 over range_sample, with sv = 10^(Sv/10) and dz the spacing
  of the range variable (Urmy et al. 2012). IA is high when a small part of
  the water column is much denser than the rest. Writes one variable,
  'aggregation' (units m-1), on channel x ping_time.

INPUT (argument or stdin)
  One Sv NetCDF path or gs:// URI (argument, or one line on stdin), e.g. from
  aa-sv or aa-clean. It must contain 'Sv' (dB) on a range_sample dimension and
  the range variable named by --range-label (default echo_range).

OUTPUT (stdout)
  The output file's absolute path (or gs:// URI).

METADATA
  Reads the input's provenance, appends this step with its canonical
  scientific options, and embeds it all in the output (NetCDF attributes
  aa_provenance, aa_recipe, aa_product_hash, aa_base, aa_tool, history). Two
  hashes: the recipe (this step and every step before it, without the data:
  the <hash8> in the name, the same for any data processed this way) and the
  product hash (this recipe applied to this input: decides reuse). The base
  name is carried through unchanged. Inspect with: aa-metadata FILE

OPTIONS
  -o, --output_path PATH  Explicit output, used exactly as given (no suffix
						  added). Local path or gs:// URI.
  --range-label NAME	  variable holding range in metres (default:
						  echo_range)
  --no-overwrite		  exit 1 if the output exists and is a different
						  product (an identical one is reused)
  --quiet				 warnings and errors only on stderr

SCIENTIFIC OPTIONS (change the product hash)
  --range-label  Variable used as range (m) for dz and the integral. (default:
				 echo_range)
  Flag order, alias spellings and explicit defaults do not change the hash.

FILES & URIs
  Reads a flat Sv NetCDF, local or gs:// (through a gcsfuse mount when one
  covers it, otherwise downloaded once to the cache). Writes <base>_<hash8>.nc
  beside the input (current directory for gs:// input), or -o, or --dest.
  AA_NAMING=legacy restores <input stem>_aggregation.nc. An identical earlier
  result is reused.

IN A PIPELINE
  After calibration: aa-nc | aa-sv | aa-aggregation. The output is a 2-D
  metric (channel x ping_time), not Sv, so it ends the Sv chain.

EXAMPLES
  aa-nc D20160703-T060000.raw --sonar_model EK60 | aa-sv | aa-aggregation
  aa-aggregation sv.nc -o ia.nc --no-overwrite

COMMON OPTIONS
  --force				 recompute even if an identical product already
						  exists
  --base NAME			 name outputs after NAME instead of the input's base
						  name
  --dest DIR|gs://PREFIX  write the default-named output there instead of
						  beside the input
  --help-all			  the complete reference, every option
```

## aa_attenuated

```bash
aa-attenuated — Attenuated-ping mask for Sv; --apply also writes cleaned Sv.
  [scientific transform (hashed) · product: mask]

WHAT IT DOES
  Runs echopype.clean.mask_attenuated_signal (Ryan et al. 2015). For each
  ping, the median Sv between --upper-limit-sl and --lower-limit-sl is
  compared with the median over the block of +/- --num-side-pings pings; the
  whole ping is flagged when (ping median - block median) is BELOW
  --attenuation-threshold. Pings within --num-side-pings of either end are
  never flagged.

  The mask file holds one variable, attenuated_mask: boolean, True =
  attenuated (the whole column), False = keep, dims (channel, ping_time,
  range_sample). If the limits lie outside the data it is all False.

INPUT (argument or stdin)
  One Sv NetCDF path or gs:// URI that has the --range-var variable: depth
  (add it with aa-depth) or echo_range. An EchoData file without Sv is
  calibrated first with compute_Sv defaults (recorded in the provenance as an
  implicit step).

OUTPUT (stdout)
  The MASK's absolute path (or gs:// URI), one line. With --apply the cleaned
  Sv's path goes to stderr instead, as 'aa-attenuated: cleaned Sv: PATH' (also
  when it is reused).

METADATA
  Reads the input's provenance, appends this step with its canonical
  scientific options, computes the product hash, and embeds it all in the mask
  (NetCDF attributes aa_provenance, aa_product_hash, aa_base, aa_tool,
  history). The --apply file is its own product: same step, variant 'apply',
  kind sv, its own hash. Inspect with: aa-metadata FILE

OPTIONS
  -o, --output_path PATH  The mask file, used exactly as given (no extension
						  added). Local path or gs:// URI. Does not move the
						  --apply file.
  --apply				 Also write the input's Sv with attenuated pings set
						  to NaN (all other variables copied).

SCIENTIFIC OPTIONS (change the product hash)
  --upper-limit-sl		 Top of the comparison layer, e.g. 400m. (default:
						   400.0m)
  --lower-limit-sl		 Bottom of the comparison layer, e.g. 500m.
						   (default: 500.0m)
  --num-side-pings		 Pings on each side in the comparison block.
						   (default: 15)
  --attenuation-threshold  Flag a ping when ping median - block median is
						   below this. Negative values need '=':
						   --attenuation-threshold=-6dB. (default: 8.0dB)
  --range-var			  Vertical variable: depth or echo_range. (default:
						   depth)
  Flag order, alias spellings and explicit defaults do not change the hash.

FILES & URIs
  Reads NetCDF, local or gs://. Writes the mask to <base>_<hash>.nc beside the
  input (current directory for gs:// input), or -o, or --dest. With --apply a
  second <base>_<hash>.nc (a different hash) goes beside the input or into
  --dest, never to -o. AA_NAMING=legacy: <stem>_attenuated_mask.nc and
  <stem>_attenuated_cleaned.nc beside the input. Identical earlier results are
  reused.

IN A PIPELINE
  After aa-sv and aa-depth: ... | aa-sv | aa-depth | aa-attenuated | aa-graph
  (draws the mask). Only the mask travels down the pipe.

EXAMPLES
  aa-nc x.raw --sonar_model EK60 | aa-sv | aa-depth | aa-attenuated --apply
  aa-attenuated sv_depth.nc --upper-limit-sl 50m --lower-limit-sl 100m --attenuation-threshold=-6dB

NOTE
  Sign of the threshold: an attenuated ping is weaker than its block, so its
  difference is negative. A negative threshold (e.g. -6dB) flags pings more
  than that much weaker; the default +8.0dB flags nearly every ping in the
  comparable range.

NOTE
  echopype 0.11.1 checks upper < lower by comparing the two limits as text,
  which would refuse 20m vs 100m. The tool therefore passes both written with
  the same width and decimals (20m, 100m -> 020m, 100m), so text order is
  numeric order and any spelling works. The upper limit must be shallower: a
  reversed pair is refused ('Minimum range has to be shorter than maximum
  range'), equal limits flag nothing. A channel whose depth contains NaN (a
  shorter-range channel) is never flagged.

COMMON OPTIONS
  --force				 recompute even if an identical product already
						  exists
  --base NAME			 name outputs after NAME instead of the input's base
						  name
  --dest DIR|gs://PREFIX  write the default-named output there instead of
						  beside the input
  --help-all			  the complete reference, every option
```

## aa_center_of_mass

```bash
aa-center-of-mass — Center of mass (CM, m): the sv-weighted mean range of
  backscatter.
  [scientific transform (hashed) · product: echometric]

WHAT IT DOES
  Runs echopype.metrics.center_of_mass on calibrated Sv: CM = sum(r * sv*dz) /
  sum(sv*dz) over range_sample, with sv = 10^(Sv/10), r the range variable and
  dz its spacing (Urmy et al. 2012). Writes one variable, 'center_of_mass'
  (units m, the range variable's reference: transducer range for echo_range),
  on channel x ping_time.

INPUT (argument or stdin)
  One Sv NetCDF path or gs:// URI (argument, or one line on stdin), e.g. from
  aa-sv or aa-clean. It must contain 'Sv' (dB) on a range_sample dimension and
  the range variable named by --range-label (default echo_range).

OUTPUT (stdout)
  The output file's absolute path (or gs:// URI).

METADATA
  Reads the input's provenance, appends this step with its canonical
  scientific options, and embeds it all in the output (NetCDF attributes
  aa_provenance, aa_recipe, aa_product_hash, aa_base, aa_tool, history). Two
  hashes: the recipe (this step and every step before it, without the data:
  the <hash8> in the name, the same for any data processed this way) and the
  product hash (this recipe applied to this input: decides reuse). The base
  name is carried through unchanged. Inspect with: aa-metadata FILE

OPTIONS
  -o, --output_path PATH  Explicit output, used exactly as given (no suffix
						  added). Local path or gs:// URI.
  --range-label NAME	  variable holding range in metres (default:
						  echo_range)
  --try-calibrate		 if that variable is missing, open the input as
						  EchoData and compute Sv first (echopype defaults; no
						  EK80 modes)
  --no-overwrite		  exit 1 if the output exists and is a different
						  product (an identical one is reused)
  --quiet				 warnings and errors only on stderr

SCIENTIFIC OPTIONS (change the product hash)
  --range-label	Variable used as range (m): the weighted values, dz and the
				   integral. (default: echo_range)
  --try-calibrate  Compute Sv from EchoData first when the range variable is
				   missing; changes what is analysed.
  Flag order, alias spellings and explicit defaults do not change the hash.

FILES & URIs
  Reads a flat Sv NetCDF, local or gs:// (through a gcsfuse mount when one
  covers it, otherwise downloaded once to the cache). Writes <base>_<hash8>.nc
  beside the input (current directory for gs:// input), or -o, or --dest.
  AA_NAMING=legacy restores <input stem>_com.nc. An identical earlier result
  is reused.

IN A PIPELINE
  After calibration: aa-nc | aa-sv | aa-center-of-mass. The output is a 2-D
  metric (channel x ping_time), not Sv, so it ends the Sv chain.

EXAMPLES
  aa-nc D20160703-T060000.raw --sonar_model EK60 | aa-sv | aa-center-of-mass
  aa-center-of-mass sv.nc -o cm.nc --no-overwrite

COMMON OPTIONS
  --force				 recompute even if an identical product already
						  exists
  --base NAME			 name outputs after NAME instead of the input's base
						  name
  --dest DIR|gs://PREFIX  write the default-named output there instead of
						  beside the input
  --help-all			  the complete reference, every option
```

## aa_clean

```bash
aa-clean — Remove background noise from Sv (De Robertis & Higginbottom 2007).
  [scientific transform (hashed) · product: sv]

WHAT IT DOES
  Runs echopype.clean.remove_background_noise on a flat Sv dataset. Noise is
  estimated from blocks of --ping_num pings x --range_sample_num samples;
  samples whose noise-corrected Sv is not more than --snr_threshold dB above
  the noise become NaN. The output is the input dataset plus two variables:
  Sv_noise (the noise estimate) and Sv_corrected (the cleaned Sv). The
  variable Sv itself is NOT changed.

INPUT (argument or stdin)
  One flat Sv .nc/.netcdf4 path or gs:// URI, from aa-sv. It must contain Sv,
  echo_range and sound_absorption (not the EchoData file from aa-nc).

OUTPUT (stdout)
  The cleaned file's absolute path (or gs:// URI).

METADATA
  Reads the input's provenance, appends this step with its canonical
  scientific options, and embeds it all in the output (NetCDF attributes
  aa_provenance, aa_recipe, aa_product_hash, aa_base, aa_tool, history). Two
  hashes: the recipe (this step and every step before it, without the data:
  the <hash8> in the name, the same for any data processed this way) and the
  product hash (this recipe applied to this input: decides reuse). The base
  name is carried through unchanged. Inspect with: aa-metadata FILE

OPTIONS
  -o, --output_path PATH		Explicit output; '_clean' is ALWAYS appended
								to its stem and .nc forced (-o out.nc writes
								out_clean.nc). Local path or gs:// URI.
  --ping_num N				  pings per noise-estimation block (default: 20)
  --range_sample_num N		  range samples per block (default: 20)
  --background_noise_max=VALdB  cap on the noise estimate, e.g.
								--background_noise_max=-125dB (write '='
								before a negative value); default: no cap
  --snr_threshold DB			minimum signal-to-noise ratio, a plain number
								in dB (default: 3.0)

SCIENTIFIC OPTIONS (change the product hash)
  --ping_num			  Pings per noise-estimation block. (default: 20)
  --range_sample_num	  Range samples per noise-estimation block. (default:
						  20)
  --background_noise_max  Upper limit on the noise estimate; '-125dB' and
						  '-125.0dB' are the same value.
  --snr_threshold		 Minimum SNR in dB; 3 and 3.0 are the same value.
						  (default: 3.0)
  Flag order, alias spellings and explicit defaults do not change the hash.

FILES & URIs
  Reads a flat Sv NetCDF, local or gs://. Writes <base>_<hash8>.nc beside the
  input (current directory for gs:// input), or in --dest DIR|gs://PREFIX, or
  at -o (+'_clean'). AA_NAMING=legacy restores the old default <input
  stem>_clean.nc. An identical earlier result is reused, not recomputed.

IN A PIPELINE
  After aa-sv: aa-nc | aa-sv | aa-clean | ... Tools downstream that read the
  variable Sv (aa-mvbs, aa-mvbs-index, aa-nasc, and aa-graph by default) still
  see the uncorrected Sv; the cleaned values are only in Sv_corrected
  (aa-graph --var Sv_corrected draws them).

EXAMPLES
  aa-nc D20160703-T060000.raw --sonar_model EK60 | aa-sv | aa-clean
  aa-clean sv.nc --ping_num 40 --range_sample_num 100 \
	--background_noise_max=-125dB --snr_threshold 5

COMMON OPTIONS
  --force				 recompute even if an identical product already
						  exists
  --base NAME			 name outputs after NAME instead of the input's base
						  name
  --dest DIR|gs://PREFIX  write the default-named output there instead of
						  beside the input
  --help-all			  the complete reference, every option
```

## aa_coerce_time

```bash
aa-coerce-time — Make a time coordinate strictly increasing
  (echopype.qc.coerce_increasing_time).
  [scientific transform (hashed) · product: sv]

WHAT IT DOES
  Finds pings whose timestamp jumps backwards and replaces each backward step
  with the median ping interval of the preceding --win-len pings; later
  intervals are kept, so the times after a reversal shift forward by the same
  amount. Only the time coordinate changes; no data values move. A file
  without reversals is written unchanged. The product kind is the input's (sv,
  mvbs, ...).

INPUT (argument or stdin)
  One NetCDF path or gs:// URI with the time coordinate (Sv, MVBS, ...).

OUTPUT (stdout)
  The output file's absolute path (or gs:// URI).

METADATA
  Reads the input's provenance, appends this step with its canonical
  scientific options, and embeds it all in the output (NetCDF attributes
  aa_provenance, aa_recipe, aa_product_hash, aa_base, aa_tool, history). Two
  hashes: the recipe (this step and every step before it, without the data:
  the <hash8> in the name, the same for any data processed this way) and the
  product hash (this recipe applied to this input: decides reuse). The base
  name is carried through unchanged. Inspect with: aa-metadata FILE

OPTIONS
  --time-name NAME		the time coordinate to fix (default ping_time)
  --win-len N			 pings before a reversal used for the median interval
						  (default 100)
  --report				say on stderr whether reversals existed before/after
  --no-overwrite		  exit 1 instead of replacing a different existing
						  output (replacing is the default)
  -o, --output_path PATH  Explicit output, used exactly as given. Local path
						  or gs:// URI.

SCIENTIFIC OPTIONS (change the product hash)
  --time-name  Time coordinate to coerce. (default: ping_time)
  --win-len	Window (pings) for the median ping interval. (default: 100)
  Flag order, alias spellings and explicit defaults do not change the hash.

FILES & URIs
  Reads NetCDF, local or gs://. Writes <base>_<hash>.nc beside the input
  (current directory for gs:// input), or -o, or --dest. An identical earlier
  result is reused. AA_NAMING=legacy: <stem>_timefix.nc.

IN A PIPELINE
  Anywhere after aa-sv, typically before aa-mvbs: ... | aa-sv | aa-coerce-time
  | aa-mvbs ...

EXAMPLES
  aa-nc D20160703-T060000.raw --sonar_model EK60 | aa-sv | aa-coerce-time --report
  aa-coerce-time Sv.nc --time-name ping_time --win-len 120 -o Sv_timefix.nc

COMMON OPTIONS
  --force				 recompute even if an identical product already
						  exists
  --base NAME			 name outputs after NAME instead of the input's base
						  name
  --dest DIR|gs://PREFIX  write the default-named output there instead of
						  beside the input
  --help-all			  the complete reference, every option
```

## aa_combine

```bash
aa-combine — Combine converted EchoData files into one L1 Zarr store (or a .nc
  export).
  [EchoData builder (starts the chain) · product: echodata]

WHAT IT DOES
  QC-checks the inputs first (sonar model, file names, channels, ping order,
  overlaps, and seams: transit gaps that would make MVBS average across water
  the ship was not in), then runs echopype.combine_echodata in time order and
  writes one store with an unbroken ping axis. A QC report is written beside
  it.

INPUT (argument or stdin)
  Used only when no INPUTS and no --workdir are given: paths, directories or
  gs:// URIs, one per line (bare paths or aa/1 JSON handles; '#' lines
  ignored). An empty pipe falls back to the current directory.

OUTPUT (stdout)
  The output's absolute path (or URI). With --json one aa/1 handle: schema,
  kind (l1 | netcdf), uri, provenance {tool, version, parents, at}, time,
  report, and product (hash), base, reused.

METADATA
  Root attributes keep aa_kind=l1, provenance {tool, version, parents, at},
  report, time_coverage_* and the aa_write marker, and add the aa provenance
  (aa_provenance, aa_product_hash, aa_base): each input's chain, identical
  steps grouped (aa-nc x3), then this step. The hash covers the inputs, in
  canonical order, and --channels. Base: --base, else the -o stem, else
  'combined'.

OPTIONS
  INPUTS | --workdir DIR		files/directories to combine (default: stdin,
								else .)
  -o, --output_path PATH		.zarr store or .nc export; local, gs:// or
								s3:// (default: <base>.zarr in --workdir or .)
  --channels A,B				channels to keep; needed when inputs differ
  --check | --plan			  QC only | estimate only (exit 4 on findings)
  --strict					  block on seams, overlaps and duplicate pings
  --chunk-pings N, --compression C
								store layout; not scientific
  --json						print an aa/1 handle instead of the path
  --overwrite				   replace an existing output (always rewrites)
  --force					   rebuild even when the identical output exists
  --base NAME				   product base name (default: the -o stem, else
								'combined')
  --dest DIR|gs://PREFIX		write <base>.zarr there instead of --workdir/.

SCIENTIFIC OPTIONS (change the product hash)
  --channels  Channels kept, in this order (sets the output channel order).
  Flag order, alias spellings and explicit defaults do not change the hash.

FILES & URIs
  Reads .nc/.zarr EchoData, local or gs:// (an object, or a folder of .nc).
  Writes <base>.zarr in --workdir or ., or -o / --dest; a .zarr goes straight
  to gs:// or s3://, a gs:// .nc is staged and uploaded. An existing output
  holding the identical product in the same layout is reused once QC passes;
  anything else needs --overwrite (else exit 2). QC report: named after the
  output (<output stem>.qc.json), beside it (in the bucket when remote).

IN A PIPELINE
  N:1 stage after aa-nc / aa-ed: aa-ed ./raw/ | aa-combine -o HB1603_L1.zarr |
  aa-sv. Exit codes: 0 ok, 1 error, 2 usage or existing output, 3 interrupted
  (store marked incomplete), 4 QC failed.

EXAMPLES
  aa-combine ./converted/ --check
  aa-combine *.nc -o HB1603_L1.zarr --chunk-pings 500
  aa-ed ./raw/ | aa-combine -o gs://bucket/HB1603_L1.zarr --json | aa-store verify --json

MORE
  --help-all  the complete reference, every option
```

## aa_crop

```bash
aa-crop — PLACEHOLDER: copies EchoData unchanged. Not installed as a command.
  [utility]

WHAT IT DOES
  Nothing scientific yet. It is meant to crop an echogram / EchoData to a
  (ping, range) window. Today it loads the input with echopype, applies
  transform_echo_data(), which returns the EchoData unchanged, and writes it
  to NetCDF. --ping_num, --range_sample_num, --background_noise_max and
  --snr_threshold are parsed but not used (left over from the background-noise
  template the file was copied from).

INPUT (argument or stdin)
  Does not read stdin. One local input path as the argument (.raw, .nc or
  .netcdf4).

OUTPUT (stdout)
  The output path.

METADATA
  Records no provenance and computes no product hash.

OPTIONS
  INPUT_PATH				a converted EchoData .nc/.netcdf4 (or .raw; see
							NOTE)
  -o, --output_path PATH	output file (default <input stem>_processed.nc)
  --ping_num N			  required, unused
  --range_sample_num N	  required, unused
  --background_noise_max X  unused
  --snr_threshold DB		unused (default 3.0)

FILES & URIs
  Local files only. Writes beside the input unless -o is given.

IN A PIPELINE
  Not installed as a command (there is no aa-crop entry point in
  pyproject.toml), so it is not part of any pipeline. Run it as python -m
  aalibrary.console.aa_crop.

EXAMPLES
  python -m aalibrary.console.aa_crop x.nc --ping_num 1 --range_sample_num 1

NOTE
  .raw input fails: echopype.open_raw is called without a sonar model.
  Re-saving a converted .nc can also fail with recent xarray versions
  ("unexpected encoding parameters for 'netCDF4' backend").

MORE
  --help-all  the complete reference, every option
```

## aa_cruisepack

```bash
aa-cruisepack — Find CruisePack SQLite databases on this computer and upload
  them to GCS.
  [interactive]

WHAT IT DOES
  Asks for your name, the GCP project (dev or prod) and your science center,
  then searches the whole disk (from /, or C:\ on Windows) for folders named
  cruise_pack_*, CruisePack_* or packager_*. Every cruiseData.sqlite and
  packageData.sqlite in a folder below them whose name contains 'database' is
  uploaded to

	gs://ggn-nmfs-aa-dev-1-data/cruisepack/<CENTER>/<CENTER>_<name>_<n>_<file>
	gs://ggn-nmfs-aa-prod-1-data/...   (prod project)

  Nothing is changed locally.

INPUT (argument or stdin)
  Nothing: the answers come from the prompts.

OUTPUT (stdout)
  Prompts and progress messages. Not a pipeline stage.

METADATA
  Produces no scientific product and records no provenance.

OPTIONS
  (none)  aa-cruisepack takes no arguments; anything other than
		  -h/--help/--help-all is refused

FILES & URIs
  Reads every directory it can list, starting at the filesystem root; this can
  take a long time. Writes objects to the chosen GCS bucket (an object with
  the same name is replaced). Needs Google Cloud credentials that may write to
  that bucket (gcloud auth application-default login). Any of these packages
  that cannot be imported is first pip-installed into the running Python:

	google-cloud-storage  google-api-python-client  inquirerpy

IN A PIPELINE
  Interactive only; not a pipeline stage.

EXAMPLES
  aa-cruisepack

MORE
  --help-all  the complete reference, every option
```

## aa_depth

```bash
aa-depth — Add a depth variable to an Sv dataset
  (echopype.consolidate.add_depth).
  [scientific transform (hashed) · product: sv]

WHAT IT DOES
  Adds 'depth' (m; channel x ping_time x range_sample) to the Sv dataset:
  depth = transducer depth + echo_range x cos(tilt); with --no-downward the
  echo_range term is subtracted instead (transducer depth - echo_range x
  cos(tilt)). Transducer depth is --depth-offset, else the Platform vertical
  offsets of --echodata (with --use-platform-vertical-offsets), else 0. The
  tilt is --tilt, else the Platform or Beam angles of --echodata
  (--use-platform-angles / --use-beam-angles), else 0. An explicit
  --depth-offset or --tilt always wins over the corresponding --use-* flag.
  Every input variable is kept unchanged; the product kind is the input's (sv,
  mvbs, ...).

INPUT (argument or stdin)
  One Sv .nc/.netcdf4 path or gs:// URI (aa-sv output, or anything with
  echo_range).

OUTPUT (stdout)
  The output file's absolute path (or gs:// URI).

METADATA
  Reads the input's provenance, appends this step with its canonical
  scientific options, and embeds it all in the output (NetCDF attributes
  aa_provenance, aa_recipe, aa_product_hash, aa_base, aa_tool, history). Two
  hashes: the recipe (this step and every step before it, without the data:
  the <hash8> in the name, the same for any data processed this way) and the
  product hash (this recipe applied to this input: decides reuse). The base
  name is carried through unchanged. Inspect with: aa-metadata FILE

OPTIONS
  -o, --output_path PATH		Explicit output, used as given; '.nc' is added
								only when it has no suffix. Local path or
								gs:// URI.
  --depth-offset M			  transducer depth below the surface, in metres
  --tilt DEG					transducer tilt from vertical, in degrees
  --no-downward				 upward-looking transducers (default: downward)
  --echodata ED.nc			  the EchoData (aa-nc output) the Sv came from;
								needed by the --use-* options. Its content
								enters the product hash.
  --use-platform-vertical-offsets
								transducer depth from Platform (EK60/EK80)
  --use-platform-angles | --use-beam-angles
								tilt from Platform or Beam angles (EK60/EK80;
								not both)

SCIENTIFIC OPTIONS (change the product hash)
  --depth-offset				Transducer depth (m). Overrides the Platform
								vertical offsets.
  --tilt						Tilt from vertical (degrees). Overrides
								Platform/Beam angles.
  --downward, --no-downward	 Downward-looking by default; --no-downward
								(upward-looking) subtracts the echo_range term
								from the transducer depth.
  --use-platform-vertical-offsets
								Transducer depth from the EchoData Platform
								group.
  --use-platform-angles		 Tilt from the EchoData Platform group angles.
  --use-beam-angles			 Tilt from the EchoData Beam group angles.
  --echodata					EchoData file: its content identity (not its
								path) enters the hash.
  Flag order, alias spellings and explicit defaults do not change the hash.

FILES & URIs
  Reads Sv .nc/.netcdf4 and an optional EchoData .nc/.netcdf4/.zarr, local or
  gs://. Writes <base>_<hash>.nc beside the input (current directory for gs://
  input), or -o, or --dest. An identical earlier result is reused.
  AA_NAMING=legacy: <stem>_depth.nc. Refuses to overwrite the input or the
  --echodata file.

IN A PIPELINE
  After aa-sv, before tools that need depth (aa-detect-seafloor): aa-nc |
  aa-sv | aa-depth | ...

EXAMPLES
  aa-nc D20160703-T060000.raw --sonar_model EK60 | aa-sv | aa-depth --depth-offset 5
  aa-depth Sv.nc --echodata D20160703-T060000.nc --use-platform-vertical-offsets

COMMON OPTIONS
  --force				 recompute even if an identical product already
						  exists
  --base NAME			 name outputs after NAME instead of the input's base
						  name
  --dest DIR|gs://PREFIX  write the default-named output there instead of
						  beside the input
  --help-all			  the complete reference, every option
```

## aa_detect_seafloor

```bash
aa-detect-seafloor — Detect the seafloor line; optionally write a below-bottom
  mask and Sv without sub-bottom samples.
  [scientific transform (hashed) · product: seafloor]

WHAT IT DOES
  Runs echopype.mask.detect_seafloor with the chosen method on one channel and
  writes the bottom depth per ping ('seafloor', m, over ping_time).
  --emit-mask also writes 'seafloor_mask' (True = below the bottom), from
  comparing --range-label (default echo_range) with the bottom line. --apply
  also writes a copy of the Sv with the samples below the bottom set to NaN:
  the water column is kept.

INPUT (argument or stdin)
  One Sv .nc/.netcdf4 path or gs:// URI that has a 'depth' variable (aa-depth
  output); 'blackwell' also needs angle_alongship/athwartship
  (aa-splitbeam-angle). A file without 'Sv' is calibrated as EchoData first
  (compute_Sv defaults, recorded in the provenance as an implicit step).

OUTPUT (stdout)
  The bottom-line file's absolute path (or gs:// URI). The --emit-mask and
  --apply files are not printed there; their paths go to stderr.

METADATA
  Reads the input's provenance, appends this step with its canonical
  scientific options, and embeds it all in the output (NetCDF attributes
  aa_provenance, aa_recipe, aa_product_hash, aa_base, aa_tool, history). Two
  hashes: the recipe (this step and every step before it, without the data:
  the <hash8> in the name, the same for any data processed this way) and the
  product hash (this recipe applied to this input: decides reuse). The base
  name is carried through unchanged. Inspect with: aa-metadata FILE

OPTIONS
  --method basic|blackwell  REQUIRED. Detector.
  --param KEY=VALUE ...	 Detector arguments. Both methods need var_name=Sv
							and channel=<an id from the 'channel' coordinate;
							quote it, it contains spaces>. basic: threshold
							(-50 = window -50..-40 dB; or (min,max)), offset_m
							(0.5), bin_skip_from_surface (200). blackwell:
							threshold (-75 or (Sv,theta,phi)), offset, r0, r1,
							wtheta, wphi.
  --emit-mask			   also write the below-bottom mask (kind mask)
  --apply				   also write Sv with sub-bottom samples removed
							(kind sv)
  --range-label NAME		variable compared with the bottom line (a depth)
							to build the mask. Default echo_range, which
							equals depth only when aa-depth applied no
							transducer depth offset, tilt, or Platform/Beam
							offsets or angles; otherwise use 'depth' (the tool
							warns when they differ).
  --no-overwrite			exit 1, before writing anything, if any requested
							output exists and is not the identical product
  -o, --output_path PATH	Explicit bottom-line output; the extension is
							forced to .nc. Local path or gs:// URI. Does not
							move the mask/cleaned outputs.

SCIENTIFIC OPTIONS (change the product hash)
  --method	   Detector (echopype dispatcher key), e.g. basic, blackwell.
  --param		Detector arguments, parsed as Python literals ('10m' stays
				 text). Arguments left out are hashed with the method's own
				 defaults (read from the installed echopype), so writing a
				 default out, key order, and 5 vs 5.0 give the same hash; pass
				 integers where echopype wants them
				 (bin_skip_from_surface=200).
  --range-label  Variable compared with the bottom line (depth). Changes only
				 the --emit-mask and --apply outputs. (default: echo_range)
  Flag order, alias spellings and explicit defaults do not change the hash.

FILES & URIs
  Reads Sv .nc/.netcdf4, local or gs://. Writes <base>_<hash>.nc for each
  output (bottom line, mask, cleaned Sv; each has its own hash) beside the
  input (current directory for gs:// input), or --dest. -o names only the
  bottom line. Identical earlier results are reused. AA_NAMING=legacy:
  <stem>_seafloor.nc, <stem>_seafloor_mask.nc, <stem>_seafloor_cleaned.nc, the
  last two always beside the input.

IN A PIPELINE
  After aa-depth: aa-nc | aa-sv | aa-depth | aa-detect-seafloor ... The next
  stage receives the bottom line. For the cleaned Sv, run the chain in steps
  and take its path from stderr, or use AA_NAMING=legacy.

EXAMPLES
  SV=$(aa-nc D20160703-T060000.raw --sonar_model EK60 | aa-sv | aa-depth)
  aa-detect-seafloor "$SV" --method basic --param var_name=Sv \
	"channel=GPT   38 kHz 00907205c001-1 ES38B" "threshold=(-30,10)" --emit-mask --apply

NOTE
  Before this version --apply kept only the sub-bottom samples (the mask was
  applied as 'keep where below bottom'). It now keeps the water column. The
  mask file itself is unchanged: True = below the bottom.

COMMON OPTIONS
  --force				 recompute even if an identical product already
						  exists
  --base NAME			 name outputs after NAME instead of the input's base
						  name
  --dest DIR|gs://PREFIX  write the default-named output there instead of
						  beside the input
  --help-all			  the complete reference, every option
```

## aa_detect_shoal

```bash
aa-detect-shoal — Detect shoals in Sv and write a shoal mask; optionally the
  Sv inside the shoals.
  [scientific transform (hashed) · product: mask]

WHAT IT DOES
  Runs echopype.mask.detect_shoal with the chosen method on one channel and
  writes 'shoal_mask' (ping_time x range_sample, True = inside a shoal).
  --apply also writes a copy of the Sv that KEEPS ONLY the samples inside the
  shoals (everything else set to NaN), i.e. the school echoes, not a
  school-free echogram.

INPUT (argument or stdin)
  One Sv .nc path or gs:// URI (aa-sv, aa-clean ... output). A file without
  'Sv' is calibrated as EchoData first (compute_Sv defaults, recorded in the
  provenance as an implicit step).

OUTPUT (stdout)
  The mask file's absolute path (or gs:// URI). The --apply file is not
  printed there; its path goes to stderr.

METADATA
  Reads the input's provenance, appends this step with its canonical
  scientific options, and embeds it all in the output (NetCDF attributes
  aa_provenance, aa_recipe, aa_product_hash, aa_base, aa_tool, history). Two
  hashes: the recipe (this step and every step before it, without the data:
  the <hash8> in the name, the same for any data processed this way) and the
  product hash (this recipe applied to this input: decides reuse). The base
  name is carried through unchanged. Inspect with: aa-metadata FILE

OPTIONS
  --method weill|echoview  REQUIRED. Detector. echoview also needs idim/jdim
						   arrays and is not usable from the command line.
  --param KEY=VALUE ...	Detector arguments. weill: var_name=Sv (required),
						   channel=<id from the 'channel' coordinate; quote
						   it> (required for multi-channel data), thr (-70
						   dB), maxvgap (5), maxhgap (0), minvlen (0), minhlen
						   (0).
  --apply				  also write the Sv inside the shoals (kind sv)
  --no-overwrite		   exit 1, before writing anything, if the mask or the
						   --apply output exists and is not the identical
						   product
  --quiet				  only warnings and errors on stderr
  -o, --output_path PATH   Explicit mask output, used exactly as given. Local
						   path or gs:// URI. Does not move the --apply
						   output.

SCIENTIFIC OPTIONS (change the product hash)
  --method  Detector (echopype dispatcher key): weill or echoview.
  --param   Detector arguments, parsed as Python literals ('12dB' stays text).
			Arguments left out are hashed with the method's own defaults (read
			from the installed echopype), so writing a default out, key order,
			and 5 vs 5.0 give the same hash.
  Flag order, alias spellings and explicit defaults do not change the hash.

FILES & URIs
  Reads Sv .nc, local or gs://. Writes <base>_<hash>.nc for the mask and for
  the --apply output (each has its own hash) beside the input (current
  directory for gs:// input), or --dest; -o names only the mask. Identical
  earlier results are reused. AA_NAMING=legacy: <stem>_detect_shoal_mask.nc
  and <stem>_detect_shoal_cleaned.nc, the latter always beside the input.

IN A PIPELINE
  After aa-sv/aa-clean: ... | aa-sv | aa-detect-shoal ... | aa-graph draws the
  mask. The next stage receives the mask.

EXAMPLES
  aa-nc D20160703-T060000.raw --sonar_model EK60 | aa-sv | aa-detect-shoal \
	--method weill --param var_name=Sv "channel=GPT   38 kHz 00907205c001-1 ES38B" thr=-60 --apply

COMMON OPTIONS
  --force				 recompute even if an identical product already
						  exists
  --base NAME			 name outputs after NAME instead of the input's base
						  name
  --dest DIR|gs://PREFIX  write the default-named output there instead of
						  beside the input
  --help-all			  the complete reference, every option
```

## aa_detect_transient

```bash
aa-detect-transient — Transient mask, fielding or matecho (True = VALID).
  [scientific transform (hashed) · product: mask]

WHAT IT DOES
  Runs echopype.clean.detect_transient(ds, method, params). echopype 0.11.1
  has two methods; both flag pings that are louder than their neighbours in a
  deep window:

	fielding  a ping whose median Sv in r0..r1 m exceeds the median of
			  +/- n pings by more than thr[0] (and whose 75th percentile
			  is below maxts) is flagged from where that excess drops
			  below thr[1] (searched upward in `jumps` m steps, not above
			  roff m) down to the end of the column.
	matecho   a ping whose mean Sv in start_depth..start_depth+window_meter
			  m exceeds by more than delta_db the `percentile` of all Sv
			  in that window over the window_ping pings around it is
			  flagged over the whole column.

  The mask file holds one variable, transient_detect_mask: boolean, True =
  VALID (keep), False = transient noise -- the OPPOSITE of aa-transient. Its
  attribute 'meaning' says so. Dims (channel, ping_time, range_sample).

INPUT (argument or stdin)
  One Sv NetCDF path or gs:// URI that has the range variable (depth by
  default; add it with aa-depth). An EchoData file without Sv is calibrated
  first with compute_Sv defaults (recorded in the provenance as an implicit
  step).

OUTPUT (stdout)
  The MASK's absolute path (or gs:// URI), one line. With --apply the cleaned
  Sv's path goes to stderr instead, as 'aa-detect-transient: cleaned Sv: PATH'
  (also when it is reused).

METADATA
  Reads the input's provenance, appends this step (the method and all its
  parameters: yours, plus echopype's defaults for the rest), computes the
  product hash, and embeds it all in the mask (NetCDF attributes
  aa_provenance, aa_product_hash, aa_base, aa_tool, history). The --apply file
  is its own product: same step, variant 'apply', kind sv, its own hash.
  Inspect with: aa-metadata FILE

OPTIONS
  --method fielding|matecho  REQUIRED. The detector.
  --param KEY=VAL ...		Method parameters, parsed as Python literals.
							 Plain numbers in m, dB or pings (r0=900, not
							 r0=900m). Omitted keys take echopype's defaults.
							 Put INPUT_PATH before --param (or end the list
							 with --).
  -o, --output_path PATH	 The mask file, used exactly as given (no
							 extension added). Local path or gs:// URI. Does
							 not move the --apply file.
  --apply					Also write the input's Sv with transient samples
							 (mask False) set to NaN; valid samples are kept
							 unchanged.

SCIENTIFIC OPTIONS (change the product hash)
  --method	 fielding or matecho.
  --param	  fielding: r0, r1 (m, default 900, 1000), n (pings, 30), thr (dB
			   pair, (3, 1)), roff (m, 20), jumps (m, 5), maxts (dB, -35),
			   start (pings, 0). matecho: start_depth (m, 220), window_meter
			   (m, 450), window_ping (pings, 100), percentile (25), delta_db
			   (dB, 12), extend_ping (pings, 0), min_window (m, 20). Both:
			   var_name (Sv). Omitted keys are recorded and hashed with
			   echopype's default, so n=30 and leaving n out give the same
			   hash. Unknown keys are refused.
  --range-var  Vertical variable, passed as params['range_var'] unless --param
			   range_var=... is given. (default: depth)
  Flag order, alias spellings and explicit defaults do not change the hash.

FILES & URIs
  Reads NetCDF, local or gs://. Writes the mask to <base>_<hash>.nc beside the
  input (current directory for gs:// input), or -o, or --dest. With --apply a
  second <base>_<hash>.nc (a different hash) goes beside the input or into
  --dest, never to -o. AA_NAMING=legacy: <stem>_detect_transient_mask.nc and
  <stem>_detect_transient_cleaned.nc beside the input. Identical earlier
  results are reused.

IN A PIPELINE
  After aa-sv and aa-depth: ... | aa-depth | aa-detect-transient --method
  matecho | aa-graph (draws the mask). Only the mask travels down the pipe.

EXAMPLES
  aa-detect-transient sv_depth.nc --method matecho --param start_depth=220 window_meter=450 window_ping=100 percentile=25 delta_db=12
  aa-detect-transient sv_depth.nc --apply --method fielding --param r0=900 r1=1000 n=30 "thr=(3, 1)" roff=20 jumps=5 maxts=-35

NOTE
  The defaults look deep (fielding 900-1000 m, matecho from 220 m). If the
  window is outside the data nothing is flagged: the mask is all True.

COMMON OPTIONS
  --force				 recompute even if an identical product already
						  exists
  --base NAME			 name outputs after NAME instead of the input's base
						  name
  --dest DIR|gs://PREFIX  write the default-named output there instead of
						  beside the input
  --help-all			  the complete reference, every option
```

## aa_dispersion

```bash
aa-dispersion — Inertia (I, m^2): spread of backscatter about its center of
  mass.
  [scientific transform (hashed) · product: echometric]

WHAT IT DOES
  Runs echopype.metrics.dispersion on calibrated Sv: I = sum((r - CM)^2 *
  sv*dz) / sum(sv*dz) over range_sample, with sv = 10^(Sv/10), r the range
  variable, dz its spacing and CM the center of mass (Urmy et al. 2012). I is
  a sv-weighted variance of range. Writes one variable, 'dispersion' (units
  m2), on channel x ping_time.

INPUT (argument or stdin)
  One Sv NetCDF path or gs:// URI (argument, or one line on stdin), e.g. from
  aa-sv or aa-clean. It must contain 'Sv' (dB) on a range_sample dimension and
  the range variable named by --range-label (default echo_range).

OUTPUT (stdout)
  The output file's absolute path (or gs:// URI).

METADATA
  Reads the input's provenance, appends this step with its canonical
  scientific options, and embeds it all in the output (NetCDF attributes
  aa_provenance, aa_recipe, aa_product_hash, aa_base, aa_tool, history). Two
  hashes: the recipe (this step and every step before it, without the data:
  the <hash8> in the name, the same for any data processed this way) and the
  product hash (this recipe applied to this input: decides reuse). The base
  name is carried through unchanged. Inspect with: aa-metadata FILE

OPTIONS
  -o, --output_path PATH  Explicit output, used exactly as given (no suffix
						  added). Local path or gs:// URI.
  --range-label NAME	  variable holding range in metres (default:
						  echo_range)
  --no-overwrite		  exit 1 if the output exists and is a different
						  product (an identical one is reused)
  --quiet				 warnings and errors only on stderr

SCIENTIFIC OPTIONS (change the product hash)
  --range-label  Variable used as range (m) for dz and the integral. (default:
				 echo_range)
  Flag order, alias spellings and explicit defaults do not change the hash.

FILES & URIs
  Reads a flat Sv NetCDF, local or gs:// (through a gcsfuse mount when one
  covers it, otherwise downloaded once to the cache). Writes <base>_<hash8>.nc
  beside the input (current directory for gs:// input), or -o, or --dest.
  AA_NAMING=legacy restores <input stem>_dispersion.nc. An identical earlier
  result is reused.

IN A PIPELINE
  After calibration: aa-nc | aa-sv | aa-dispersion. The output is a 2-D metric
  (channel x ping_time), not Sv, so it ends the Sv chain.

EXAMPLES
  aa-nc D20160703-T060000.raw --sonar_model EK60 | aa-sv | aa-dispersion
  aa-dispersion sv.nc -o inertia.nc --no-overwrite

NOTE
  echopype 0.11 computes the center of mass inside dispersion from
  'echo_range' whatever --range-label says. With another --range-label the
  deviations and the center of mass come from different variables, which adds
  (CM of label - CM of echo_range)^2 to every value (+25 m2 for a depth that
  is echo_range + 5 m), and it fails when the file has no echo_range. Keep the
  default; the tool warns on stderr otherwise.

COMMON OPTIONS
  --force				 recompute even if an identical product already
						  exists
  --base NAME			 name outputs after NAME instead of the input's base
						  name
  --dest DIR|gs://PREFIX  write the default-named output there instead of
						  beside the input
  --help-all			  the complete reference, every option
```

## aa_download

```bash
aa-download — Copy gs:// objects to local files (skips identical ones).
  [source (fetches data)]

WHAT IT DOES
  Downloads each gs:// object to a local file and prints its path. A file
  already there with the same content (GCS MD5) is kept, not downloaded again.
  A prefix ending in / is copied as a folder (a .zarr store, or a folder of
  .raw files, optionally filtered with --pattern) and the folder is printed
  once. When a gcsfuse mount shows the object, it is copied from the mount.

INPUT (argument or stdin)
  gs:// URIs, one per line (or as arguments). aa/1 JSON handles work too.

OUTPUT (stdout)
  One local absolute path per input (a folder for a prefix).

METADATA
  Provenance travels with the file. Products carry it inside (NetCDF
  attributes, PNG text, ...); a <key>.aa.json sidecar in the bucket is copied
  along; a plain file without either (e.g. a .raw) gets a new <file>.aa.json
  recording its gs:// origin and MD5, so aa-nc lists where the data came from.
  Nothing about the bytes changes, so hashes are the same as reading the gs://
  URI directly.

OPTIONS
  --dest DIR		 download into DIR (default: the current directory)
  -o, --output PATH  exact local path; one input only
  --pattern GLOB	 for a prefix: only names matching GLOB, e.g. '*.raw'
  --no-copy		  if a gcsfuse mount shows the object, print that path
					 instead of copying (no disk used)
  --force			download even if an identical local file exists
  --dry-run		  print what would be downloaded; download nothing

SCIENTIFIC OPTIONS
  None. A fetched file is identified by its content (MD5), not by the
  options that selected it, so the same file always has the same identity.

FILES & URIs
  Reads gs:// objects with your Application Default Credentials (the project
  aalibrary is configured for). gcsfuse mounts (e.g.
  ~/ggn-nmfs-aa-prod-1-data) are detected from /proc/mounts or AA_GCS_MOUNTS.
  Writes into --dest, -o, or the current directory.

IN A PIPELINE
  A first stage: aa-download gs://.../x.raw | aa-nc --sonar_model EK60 |
  aa-sv. You rarely need it for .nc/.zarr inputs: every tool accepts gs://
  URIs directly.

EXAMPLES
  aa-download gs://ggn-nmfs-aa-prod-1-data/raw/D20160703-T060000.raw | aa-nc --sonar_model EK60
  aa-download gs://bucket/raw/HB1603/ --pattern '*.raw' --dest data/ | aa-ed --sonar_model EK60

MORE
  --help-all  the complete reference, every option
```

## aa_ed

```bash
aa-ed — Raw file name, path or folder -> EchoData NetCDF (aa-raw + aa-nc in
  one step).
  [EchoData builder (starts the chain) · product: echodata]

WHAT IT DOES
  A bare NCEI file name is looked up in the NCEI BigQuery cache (ship, survey,
  echosounder), downloaded from NCEI and converted with echopype.open_raw. A
  local .raw (or gs:// URI) is converted offline, the sonar model read from
  its header. A directory: every .raw in it.

INPUT (argument or stdin)
  One token (argument or first stdin line): a bare file name, a .raw path, a
  directory, a file:// or gs:// URI, or an aa/1 JSON handle. An empty pipe is
  an error (exit 1).

OUTPUT (stdout)
  The .nc's absolute path; its gs:// URI with --print-uri/--cloud-only or
  -o/--dest gs://...; the directory itself in directory mode.

METADATA
  Starts the provenance chain: the .nc records the .raw's identity, the sonar
  model and the base name (aa_provenance, aa_product_hash, aa_base). An NCEI
  download gets a <file>.raw.aa.json sidecar with its origin
  (s3://noaa-wcsd-pds/data/raw/...), which the .nc records too.

OPTIONS
  FILE_NAME | PATH.raw | DIR	what to convert (mode is auto-detected)
  -o, --output_path PATH		the .nc (suffix forced to .nc); local or gs://
  --file_download_directory DIR
								where NCEI downloads land (default: .)
  --ship_name/--survey_name/--sonar_model
								override the lookup; all three skip BigQuery
  -r, --recursive			   directory mode: include subfolders
  --cleanup-raw				 delete the downloaded .raw after converting
  --gcs-uri URI | --gcs-prefix P
								use a bucket object as a cache of the .nc (see
								--help-all)
  -f, --force				   download and convert again

SCIENTIFIC OPTIONS (change the product hash)
  --sonar_model  Which echopype parser reads the file: --sonar_model, else the
				 NCEI cache (bare name) or the .raw header.
  Flag order, alias spellings and explicit defaults do not change the hash.

FILES & URIs
  Writes <raw stem>.nc beside the .raw (NCEI: in --file_download_directory;
  gs:// raw: the current directory), or -o / --dest. Reused without
  converting: a .nc holding the same product; in NCEI mode one recorded as
  converted from the same NCEI object and sonar model (no lookup, no
  download); one made before provenance existed (by name, with a note). A
  different product is converted again.

IN A PIPELINE
  First stage: aa-ed FILE.raw | aa-sv | aa-clean ...  Directory mode feeds
  aa-combine: aa-ed ./raw/ | aa-combine -o survey.zarr

EXAMPLES
  aa-ed HB1603_L1-D20160703-T183957.raw | aa-sv
  aa-ed ./data/D20160703-T060000.raw		  # offline, .nc beside the .raw
  aa-ed ./raw/ | aa-combine -o HB1603_L1.zarr

COMMON OPTIONS
  --force				 recompute even if an identical product already
						  exists
  --base NAME			 name outputs after NAME instead of the input's base
						  name
  --dest DIR|gs://PREFIX  write the default-named output there instead of
						  beside the input
  --help-all			  the complete reference, every option
```

## aa_evenness

```bash
aa-evenness — Equivalent area (EA, m) of backscatter along range, per channel
  and ping.
  [scientific transform (hashed) · product: echometric]

WHAT IT DOES
  Runs echopype.metrics.evenness on calibrated Sv: EA = (sum sv*dz)^2 /
  sum(sv^2*dz) over range_sample, with sv = 10^(Sv/10) and dz the spacing of
  the range variable. EA is the range extent the backscatter would fill if
  every sample held the mean density (Urmy et al. 2012). Writes one variable,
  'evenness' (units m), on channel x ping_time.

INPUT (argument or stdin)
  One Sv NetCDF path or gs:// URI (argument, or one line on stdin), e.g. from
  aa-sv or aa-clean. It must contain 'Sv' (dB) on a range_sample dimension and
  the range variable named by --range-label (default echo_range).

OUTPUT (stdout)
  The output file's absolute path (or gs:// URI).

METADATA
  Reads the input's provenance, appends this step with its canonical
  scientific options, and embeds it all in the output (NetCDF attributes
  aa_provenance, aa_recipe, aa_product_hash, aa_base, aa_tool, history). Two
  hashes: the recipe (this step and every step before it, without the data:
  the <hash8> in the name, the same for any data processed this way) and the
  product hash (this recipe applied to this input: decides reuse). The base
  name is carried through unchanged. Inspect with: aa-metadata FILE

OPTIONS
  -o, --output_path PATH  Explicit output, used exactly as given (no suffix
						  added). Local path or gs:// URI.
  --range-label NAME	  variable holding range in metres (default:
						  echo_range)
  --try-calibrate		 if that variable is missing, open the input as
						  EchoData and compute Sv first (echopype defaults; no
						  EK80 modes)
  --no-overwrite		  exit 1 if the output exists and is a different
						  product (an identical one is reused)
  --quiet				 warnings and errors only on stderr

SCIENTIFIC OPTIONS (change the product hash)
  --range-label	Variable used as range (m) for dz and the integral.
				   (default: echo_range)
  --try-calibrate  Compute Sv from EchoData first when the range variable is
				   missing; changes what is analysed.
  Flag order, alias spellings and explicit defaults do not change the hash.

FILES & URIs
  Reads a flat Sv NetCDF, local or gs:// (through a gcsfuse mount when one
  covers it, otherwise downloaded once to the cache). Writes <base>_<hash8>.nc
  beside the input (current directory for gs:// input), or -o, or --dest.
  AA_NAMING=legacy restores <input stem>_evenness.nc. An identical earlier
  result is reused.

IN A PIPELINE
  After calibration: aa-nc | aa-sv | aa-evenness. The output is a 2-D metric
  (channel x ping_time), not Sv, so it ends the Sv chain.

EXAMPLES
  aa-nc D20160703-T060000.raw --sonar_model EK60 | aa-sv | aa-evenness
  aa-evenness sv.nc -o ea.nc --no-overwrite

COMMON OPTIONS
  --force				 recompute even if an identical product already
						  exists
  --base NAME			 name outputs after NAME instead of the input's base
						  name
  --dest DIR|gs://PREFIX  write the default-named output there instead of
						  beside the input
  --help-all			  the complete reference, every option
```

## aa_evl

```bash
aa-evl — Mask an echogram above, below or between Echoview lines (.evl).
  [scientific transform (hashed) · product: sv]

WHAT IT DOES
  Reads one or more Echoview line files (time, depth points), interpolates
  each line linearly to every ping (pings before the first / after the last
  point take that point's depth), and sets every cell of every (time, depth)
  variable on the unwanted side of the line to NaN. Axis variables
  (echo_range, depth, ...) are left intact.

  --keep above (default): keep cells at or above the line (depth <= line);
  with several lines, the per-ping shallowest one. Typical: remove the
  seafloor and everything below it.

  --keep below: keep cells at or below the line (depth >= line); with several
  lines, the per-ping deepest one. Typical: remove near-surface noise.

  --keep between: exactly two lines, given upper then lower; keep upper <=
  depth <= lower. If the first line's median depth is deeper, the two are
  swapped.

  Depth is positive downward. --depth-offset METRES is added to the line (to
  both lines for between) before masking: negative moves the line up
  (shallower), positive moves it down (deeper). So --keep above --depth-offset
  -5 on a seafloor line keeps only data more than 5 m above the bottom. Line
  depths are clipped to the echogram's depth range. Line points with sentinel
  depths (|depth| >= 9000, e.g. -10000.99) are dropped and the line is
  interpolated across them.

INPUT (argument or stdin)
  Flat NetCDF paths (.nc/.netcdf4) or gs:// URIs, one per line or as
  arguments: Sv, cleaned Sv, MVBS, ... with a (ping_time|time) x
  (depth|range_sample|range_bin|echo_range) variable (--var, default Sv).

OUTPUT (stdout)
  One line per input, in input order: the output's absolute path (or gs://
  URI). An input that fails prints nothing and the exit status is 1 at the
  end.

METADATA
  Reads the input's provenance, appends this step with its canonical
  scientific options and embeds it (NetCDF attributes aa_provenance,
  aa_product_hash, aa_base, aa_tool, history). The EVL files are recorded as
  inputs with role 'regions' and identified by content; the attributes
  aa_evl_files, aa_evl_keep and aa_evl_depth_offset are kept. The base name is
  carried through. Inspect with: aa-metadata FILE

OPTIONS
  --evl EVL [EVL ...]		   REQUIRED. Line files, local or gs://.
  --keep above|below|between	which side of the line(s) to keep (default
								above)
  --depth-offset METRES		 shift the line(s) before masking; negative =
								up (shallower), positive = down (default 0)
  -o, --output-path PATH		exact output path (one input only); local or
								gs://
  --out-dir DIR				 write the outputs here instead of beside each
								input
  --suffix TEXT				 use the old naming <input stem><TEXT>.nc
								instead of <base>_<hash8>.nc
  --overwrite				   replace an existing output that is a different
								product
  --var NAME					variable whose dimensions define the mask
								(default Sv)
  --time-dim / --depth-dim NAME
								dimension names (default: ping_time|time;
								depth|range_sample|range_bin|echo_range)
  --channel-index N			 channel whose echo_range/depth gives the depth
								axis (default 0)
  --write-line				  also write the line used as evl_line_depth
								(per ping; for between: the upper line)
  --fail-empty				  fail an input whose mask keeps nothing instead
								of writing all-NaN
  --debug					   verbose diagnostics on stderr

SCIENTIFIC OPTIONS (change the product hash)
  --keep		   Which side of the line is kept. (default: above)
  --depth-offset   Metres added to the line depth (negative = shallower;
				   default 0).
  --var			Variable whose dimensions define the mask. (default: Sv)
  --time-dim	   Time dimension used, as resolved.
  --depth-dim	  Depth dimension used, as resolved.
  --channel-index  Channel whose echo_range/depth gives the depth axis.
				   Recorded only when --var or that axis has a channel
				   dimension.
  --write-line	 Adds the evl_line_depth variable to the file.
  --evl			The line files' CONTENT (not their paths or names). For
				   above/below their order and duplicates don't matter; for
				   between the order is kept (it decides upper/lower when the
				   medians are equal).
  Flag order, alias spellings and explicit defaults do not change the hash.

FILES & URIs
  Reads flat NetCDF, local or gs://; EVL files local or gs:// (read through
  the cache, AA_CACHE_DIR). Writes <base>_<hash8>.nc beside each input
  (current directory for gs:// input), in --out-dir, or under --dest
  DIR|gs://PREFIX. -o and --suffix keep the old explicit names;
  AA_NAMING=legacy restores the old default <stem>_evl.nc. An identical
  earlier product is reused; a different existing file is replaced only with
  --overwrite.

IN A PIPELINE
  After aa-sv / aa-clean / aa-depth, before aa-evr, aa-graph, aa-mvbs, ...
  Every stdin line is one input and gives one output line.

EXAMPLES
  aa-nc x.raw --sonar_model EK60 | aa-sv | aa-evl --evl seafloor.evl --depth-offset -5 | aa-graph
  aa-evl a.nc b.nc --evl surface.evl --keep below --out-dir masked/
  aa-evl x_Sv.nc --evl upper.evl lower.evl --keep between --write-line

NOTE
  Line depths are compared with echo_range of --channel-index at the first
  ping (range from the transducer) when the file has it, otherwise with depth,
  otherwise with sample indices (with a warning).

COMMON OPTIONS
  --force				 recompute even if an identical product already
						  exists
  --base NAME			 name outputs after NAME instead of the input's base
						  name
  --dest DIR|gs://PREFIX  write the default-named output there instead of
						  beside the input
  --help-all			  the complete reference, every option
```

## aa_evr

```bash
aa-evr — Keep only the data inside Echoview regions (.evr); set the rest to
  NaN.
  [scientific transform (hashed) · product: sv]

WHAT IT DOES
  Two modes.

  EVR mode (--evr given): reads one or more Echoview region files with
  echoregions, takes the union of all their regions, and sets every cell of
  every (time, depth) variable that lies outside the union to NaN. Axis
  variables (echo_range, depth, ...) are left intact. The mask is built on
  --var at --channel-index and applied to all channels. Echoview's -9999.99 /
  9999.99 depths (surface / bottom) become the echogram's top / bottom;
  regions without usable depths (GPS or track regions) keep whole pings inside
  their time span.

  Draw mode (no --evr): opens the echogram of one input in your browser
  (Bokeh). Draw regions freehand, click Save, and the tool writes the masked
  NetCDF <stem>_evr.nc plus an .evr of the drawn polygons (--name).

INPUT (argument or stdin)
  EVR mode: flat NetCDF paths (.nc/.netcdf4) or gs:// URIs, one per line or as
  arguments: Sv, cleaned Sv, MVBS, ... with a (ping_time|time) x
  (depth|range_sample|range_bin|echo_range) variable. Draw mode: one path
  (only the first input is used).

OUTPUT (stdout)
  EVR mode: one line per input, in input order: the output's absolute path (or
  gs:// URI). An input that fails prints nothing and the exit status is 1 at
  the end. Draw mode: the masked NetCDF's path.

METADATA
  Reads the input's provenance, appends this step with its canonical
  scientific options and embeds it (NetCDF attributes aa_provenance,
  aa_product_hash, aa_base, aa_tool, history). The region files are recorded
  as inputs with role 'regions' and identified by content, and the
  aa_evr_files attribute lists them as given. The base name is carried
  through. Draw mode records variant 'draw' and the drawn .evr. Inspect with:
  aa-metadata FILE

OPTIONS
  --evr EVR [EVR ...]		   EVR mode: region files, local, gs://, or
								another fsspec URI (s3://, https://). All
								regions are unioned.
  -o, --output-path PATH		exact output path (one input only); local or
								gs://
  --out-dir DIR				 write the outputs here instead of beside each
								input
  --suffix TEXT				 use the old naming <input stem><TEXT>.nc
								instead of <base>_<hash8>.nc
  --overwrite				   replace an existing output that is a different
								product
  --var NAME					variable the mask is built on (default: first
								of Sv, Sv_clean, MVBS, TS, NASC)
  --time-dim / --depth-dim NAME
								dimension names (default: ping_time|time;
								depth|range_sample|range_bin|echo_range)
  --channel-index N			 channel whose depth axis builds the mask
								(default 0)
  --write-mask				  also write the union mask as int8 variable
								region_mask
  --fail-empty				  fail an input whose mask is empty instead of
								writing all-NaN
  --name FILE				   draw mode: .evr file name (default
								<stem>_regions.evr)
  --port N					  draw mode: Bokeh server port (default 5006,
								next free one)
  --debug					   verbose diagnostics on stderr

SCIENTIFIC OPTIONS (change the product hash)
  --var			Variable the mask is built on, as resolved (an
				   auto-detected name hashes the same as the same name given
				   explicitly).
  --time-dim	   Time dimension used, as resolved.
  --depth-dim	  Depth dimension used, as resolved.
  --channel-index  Channel whose depth axis builds the mask. Recorded only
				   when --var has a channel dimension.
  --write-mask	 Adds the region_mask variable to the file.
  --evr			The region files' CONTENT (not their paths or names). Order
				   and duplicates don't matter: the union is the same.
  Flag order, alias spellings and explicit defaults do not change the hash.

FILES & URIs
  Reads flat NetCDF, local or gs://. Region files may be local, gs:// (read
  through the cache, AA_CACHE_DIR) or another fsspec URI. Writes
  <base>_<hash8>.nc beside each input (current directory for gs:// input), in
  --out-dir, or under --dest DIR|gs://PREFIX. -o and --suffix keep the old
  explicit names; AA_NAMING=legacy restores the old default <stem>_evr.nc. An
  identical earlier product is reused; a different existing file is replaced
  only with --overwrite. Draw mode writes <stem>_evr.nc and the .evr beside
  the input or in --out-dir.

IN A PIPELINE
  After aa-sv / aa-clean / aa-depth / aa-evl, before aa-graph, aa-plot,
  aa-mvbs, ... Every stdin line is one input and gives one output line. Draw
  mode blocks until you click Save in the browser.

EXAMPLES
  aa-nc x.raw --sonar_model EK60 | aa-sv | aa-evr --evr school.evr | aa-graph
  aa-evr a.nc b.nc --evr gs://bucket/regions/leg1.evr --out-dir masked/
  aa-evl x_Sv.nc --evl bottom.evl | aa-evr --evr school.evr --write-mask
  aa-evr x_Sv.nc --name school.evr		# draw mode

NOTE
  Region depths are compared with echo_range of --channel-index at the first
  ping (range from the transducer) when the file has it, otherwise with depth,
  otherwise with sample indices.

NOTE
  Regions that don't overlap the echogram's time range give an empty mask: a
  warning and an all-NaN output (or a failure with --fail-empty).

NOTE
  The .evr written by draw mode is a record of the drawing; echoregions cannot
  read it back, so don't pass it to --evr.

COMMON OPTIONS
  --force				 recompute even if an identical product already
						  exists
  --base NAME			 name outputs after NAME instead of the input's base
						  name
  --dest DIR|gs://PREFIX  write the default-named output there instead of
						  beside the input
  --help-all			  the complete reference, every option
```

## aa_fetch

```bash
aa-fetch — Download every NCEI file a request YAML matches, into one
  directory.
  [source (fetches data) · product: raw]

WHAT IT DOES
  Turns the request document (vessel / survey / instrument / time windows, as
  written by aa-request or aa-get) into a query against aalibrary's cache of
  NCEI metadata in BigQuery (<project>.metadata.ncei_cache), then downloads
  every matching object from NCEI's public bucket noaa-wcsd-pds into a new
  directory. Files keep their NCEI names.

INPUT (argument or stdin)
  The request: a YAML path (argument, or one line on stdin), or the YAML
  document itself. '-' reads all of stdin as the document; piped text whose
  first line is 'requests:' is read as the document too, so aa-request ... |
  aa-fetch works with or without '-'.

OUTPUT (stdout)
  The absolute path of the download directory, one line; also when nothing
  matched (the directory is then empty). Empty on failure.

METADATA
  Writes <file>.aa.json beside every downloaded file: the NCEI object it came
  from (s3://noaa-wcsd-pds/data/raw/...), its MD5 and size, ship/survey/sonar
  as NCEI spells them, and the request file. aa-nc records that origin as the
  source of the .nc it writes (aa-metadata FILE.nc shows it). The sidecar
  never changes a hash. A document read from stdin is kept as
  <dir>/aa-fetch-request.yaml.

OPTIONS
  YAML_PATH | -				 the request file, or '-' for the document on
								stdin
  -o, --output_root DIR		 parent of the download directory (default:
								current directory)
  -n, --download_dir_name NAME  download directory name (default:
								aa_fetch_<YYYYMMDD_HHMMSS>)

SCIENTIFIC OPTIONS
  None. A fetched file is identified by its content (MD5), not by the
  options that selected it, so the same file always has the same identity.

FILES & URIs
  Reads BigQuery (ggn-nmfs-aa-prod-1.metadata.ncei_cache: aalibrary selects
  the production project when imported; needs Google Cloud credentials) and
  NCEI's bucket (anonymous). Writes DIR/<file> and DIR/<file>.aa.json for each
  match. All files land in one flat directory: two matches with the same file
  name overwrite each other; aa-fetch warns and records no origin for that
  name. Matches that NCEI does not return are reported on stderr.

IN A PIPELINE
  Source stage. Feed it from aa-request or aa-get; its directory feeds aa-ed
  (directory mode). Exit codes: 0 ok (also for zero matches), 1 unreadable
  request, query or download error, 2 usage (no request, empty pipe, bad
  directory name).

EXAMPLES
  aa-request --vessel Alaska_Knight --survey CHS12AK --instrument ES60 \
			 --from 2012-08-13 --to 2012-08-14 | aa-fetch - -o ./downloads
  aa-fetch request.yaml -o ./downloads -n run_001
  aa-get | aa-fetch | aa-ed | aa-combine | aa-sv | aa-graph

MORE
  --help-all  the complete reference, every option
```

## aa_find

```bash
aa-find — Browse NCEI's echosounder archive in a terminal menu; download or
  plot a file.
  [interactive]

WHAT IT DOES
  Drill down vessel -> survey -> sonar model -> .raw file in NCEI's Water
  Column Sonar archive (public S3 bucket noaa-wcsd-pds). On a file:

	Download .raw		  runs aa-raw
	Plot Echogram(s)	   runs aa-raw | aa-nc | aa-sv | aa-graph
	Check File Disk Usage  size of the object on S3

  Also: the survey's size, Google Cloud sign-in (gcloud auth login,
  application-default login, project ggn-nmfs-aa-dev-1) and documentation
  links. OMAO search, NetCDF download and KMeans/DBScan are placeholders.

INPUT (argument or stdin)
  Nothing: keyboard only (arrows, Enter, type to filter, Ctrl-C).

OUTPUT (stdout)
  The menus. aa-find is not a pipeline stage.

METADATA
  Produces no scientific product and records no provenance.

OPTIONS
  --no-color, --plain  plain text: no colors, panels or spinners
  --version			print the aalibrary version

FILES & URIs
  Writes into ./<ship>_<survey>_<sonar>_NCEI/ in the current directory: the
  .raw with its .idx/.bot and their .aa.json sidecars (from aa-raw); for a
  plot also <stem>.nc (aa-nc), the Sv file (aa-sv) and <stem>.png (aa-graph).
  Browsing reads NCEI's bucket anonymously; downloading needs Google Cloud
  credentials (see aa-raw --help).

IN A PIPELINE
  Interactive only. The tools it runs must be on PATH (the same environment as
  aa-find). For EK80 and ES80 data, Plot first asks how the file was recorded
  (CW or broadband, power or complex): aa-sv cannot calibrate those without
  it.

EXAMPLES
  aa-find
  aa-find --plain

MORE
  --help-all  the complete reference, every option
```

## aa_freqdiff

```bash
aa-freqdiff — Frequency-differencing mask: Sv(A) - Sv(B) <op> N dB.
  [scientific transform (hashed) · product: mask]

WHAT IT DOES
  Runs echopype.mask.frequency_differencing with one criterion, 'A - B <op> N
  dB' (op one of > < >= <= ==), A and B given as nominal frequencies
  (--freqABEq) or channel names (--chanABEq). Useful for separating scatterers
  by frequency response (e.g. krill).

  The mask file holds one variable, freqdiff_mask: boolean, True = the
  criterion holds, False = it does not or either Sv is NaN, dims (ping_time,
  range_sample) -- no channel dimension.

INPUT (argument or stdin)
  One Sv NetCDF (or Zarr) path or gs:// URI with a channel coordinate and
  frequency_nominal, e.g. from aa-sv.

OUTPUT (stdout)
  The mask's absolute path (or gs:// URI), one line.

METADATA
  Reads the input's provenance, appends this step with its canonical
  scientific options, and embeds it all in the output (NetCDF attributes
  aa_provenance, aa_recipe, aa_product_hash, aa_base, aa_tool, history). Two
  hashes: the recipe (this step and every step before it, without the data:
  the <hash8> in the name, the same for any data processed this way) and the
  product hash (this recipe applied to this input: decides reuse). The base
  name is carried through unchanged. Inspect with: aa-metadata FILE

OPTIONS
  --freqABEq 'A - B op NdB'	 Frequencies WITHOUT quotes, in Hz with an
								optional k/M/G prefix, matching
								frequency_nominal: '38kHz - 120kHz >= 10dB'.
  --chanABEq '"A" - "B" op NdB'
								Channel names in double quotes, exactly as in
								the channel coordinate.
  -o, --output_path PATH		Used exactly as given (no extension added).
								Local path or gs:// URI.
  --quiet					   Only warnings and errors on stderr.

SCIENTIFIC OPTIONS (change the product hash)
  --freqABEq  Criterion by frequency. Give exactly one of the two.
  --chanABEq  Criterion by channel name. Give exactly one of the two.
  Flag order, alias spellings and explicit defaults do not change the hash.

FILES & URIs
  Reads NetCDF or Zarr, local or gs://. Writes <base>_<hash>.nc beside the
  input (current directory for gs:// input), or -o, or --dest.
  AA_NAMING=legacy: <stem>_freqdiff.nc beside the input. An identical earlier
  result is reused.

IN A PIPELINE
  After aa-sv (or aa-clean): ... | aa-sv | aa-freqdiff --freqABEq '...' |
  aa-graph

EXAMPLES
  aa-nc x.raw --sonar_model EK60 | aa-sv | aa-freqdiff --freqABEq '120kHz - 38kHz > 2dB'
  aa-freqdiff sv.nc -o krill_mask.nc --chanABEq \
	'"GPT  120 kHz 00907205c002-1 ES120-7C" - "GPT   38 kHz 00907205c001-1 ES38B" <= 5dB'

NOTE
  echopype 0.11.1 accepts only a non-negative number of dB: 'A - B < -5dB' is
  rejected ('Invalid operator!'). Swap the operands instead: 'B - A > 5dB'.
  Quoted frequencies ('"38kHz" - ...') are rejected too ('Invalid freqAB
  Equation!').

COMMON OPTIONS
  --force				 recompute even if an identical product already
						  exists
  --base NAME			 name outputs after NAME instead of the input's base
						  name
  --dest DIR|gs://PREFIX  write the default-named output there instead of
						  beside the input
  --help-all			  the complete reference, every option
```

## aa_get

```bash
aa-get — Build a fetch-request YAML in a terminal menu; print its path.
  [interactive]

WHAT IT DOES
  Asks for vessel, survey, instrument and time windows (the choices come from
  aalibrary's NCEI metadata cache in BigQuery), shows the result, and on
  confirmation writes the request document that aa-fetch reads. aa-request
  writes the same document from flags, for scripts and jobs.

INPUT (argument or stdin)
  Nothing, except with OUTPUT_DIR '-': then one line naming the output
  directory.

OUTPUT (stdout)
  The saved YAML's absolute path, one line, last. The menus are drawn on the
  terminal (sent to stderr when stdout is piped, so aa-get | aa-fetch works).
  Nothing if you decline to save (exit 1) or press Ctrl-C (exit 130).

METADATA
  Produces no scientific product and records no provenance.

OPTIONS
  OUTPUT_DIR | -		directory to save into (default: current directory);
						'-' reads it from stdin
  -d, --output_dir DIR  same as OUTPUT_DIR; wins over it
  -n, --file_name NAME  file name; .yaml is added if missing (default:
						fetch_request_<YYYYMMDD_HHMMSS>.yaml)

FILES & URIs
  Writes OUTPUT_DIR/NAME.yaml (asks before replacing an existing file). Needs
  a terminal on stdout or stderr for the menus, and Google Cloud credentials
  for the BigQuery lookups.

IN A PIPELINE
  aa-get | aa-fetch: the saved path is the next stage's input.

EXAMPLES
  aa-get -n request.yaml | aa-fetch -o ./downloads -n run_001
  aa-get -d ./schedules -n test.yaml

MORE
  --help-all  the complete reference, every option
```

## aa_graph

```bash
aa-graph — Draw an echogram PNG of a NetCDF product (Sv, MVBS, masks,
  clusters).
  [representation (renders a product) · product: echogram]

WHAT IT DOES
  Plots one variable, one panel per channel, with a pie row showing the
  distribution of values or cluster labels. Categorical data (masks, cluster
  labels) gets a discrete palette automatically.

INPUT (argument or stdin)
  One flat NetCDF path or gs:// URI (Sv, MVBS, NASC, a mask, ...). After
  aa-clean the file holds both Sv (unchanged) and Sv_corrected (cleaned): the
  default draws Sv; add --var Sv_corrected to see the cleaned data.

OUTPUT (stdout)
  The PNG's absolute path (or gs:// URI). The last stdout line is the image,
  which aa_show() in notebooks relies on.

METADATA
  Copies the provenance of the product it shows into the output (PNG text
  chunks, or a JSON block in the HTML head) and adds a non-scientific
  rendering step. The output is named after the product it shows, with its own
  extension.

OPTIONS
  --var NAME					variable to plot (default: Sv, then other
								common names)
  --channel N | --frequency HZ | --single
								plot one channel only
  --vmin DB --vmax DB		   colour limits (defaults per variable, e.g. Sv
								-80/-30)
  --decimate N				  plot every Nth ping (large files)
  --ymin M --ymax M			 depth window
  -o, --output_path PATH		explicit output (.png, .svg or .pdf); local or
								gs://
  --dpi N					   resolution (default 100)

RENDERING OPTIONS (identify this rendering; the science shown is the input's)
  --var		 variable drawn (default: Sv, then other common names)
  --channel	 draw only channel index N
  --frequency   draw only the channel nearest F Hz
  --single	  draw only the first channel
  --vmin		lower colour limit (default per variable, Sv -80 dB)
  --vmax		upper colour limit (default per variable, Sv -30 dB)
  --cmap		matplotlib colour map (default: viridis)
  --figwidth	figure width, inches (default: 10)
  --rowheight   height of each channel row, inches (default: 3)
  --no-flip	 don't put depth increasing downwards
  --no-pie	  no distribution (pie) row
  --pie-height  height of the pie row, inches (default: 2.6)
  --decimate	draw every Nth ping (default: 1)
  --ymin		top of the depth window
  --ymax		bottom of the depth window
  --dpi		 resolution (default: 100)

FILES & URIs
  Writes <name of the input product>.png beside the input: the image of
  HB1603_..._35e8864f.nc is HB1603_..._35e8864f.png. Re-drawing with the same
  options reuses the existing PNG; different options replace it (use -o to
  keep several renderings).

IN A PIPELINE
  Last stage: ... | aa-sv | aa-graph, or ... | aa-clean | aa-graph --var
  Sv_corrected.

EXAMPLES
  aa-nc x.raw --sonar_model EK60 | aa-sv | aa-graph --vmin -80 --vmax -30
  ... | aa-clean | aa-graph --var Sv_corrected
  aa-graph gs://bucket/derived/x_35e8864f.nc --frequency 38000 --dest gs://bucket/figs/

COMMON OPTIONS
  --force				 recompute even if an identical product already
						  exists
  --base NAME			 name outputs after NAME instead of the input's base
						  name
  --dest DIR|gs://PREFIX  write the default-named output there instead of
						  beside the input
  --help-all			  the complete reference, every option
```

## aa_guide

```bash
aa-guide — Print the field guide to the aa-* tools and how to pipe them.
  [utility]

WHAT IT DOES
  Prints one plain-text reference: how aa-* pipes pass file names, the
  file-naming rule (<base>.nc, <base>_<hash8>.<ext>), where provenance lives
  and how aa-metadata reads it, reuse and --force, gs:// URIs and the download
  cache, an index of every installed aa-* tool by stage, worked pipelines, and
  the mistakes that silently give wrong science.

INPUT (argument or stdin)
  Nothing. aa-guide reads no input.

OUTPUT (stdout)
  The guide text, about 350 lines. Page it or search it.

METADATA
  Produces no scientific product and records no provenance.

OPTIONS
  (no options)  aa-guide always prints the whole guide

FILES & URIs
  Reads and writes no files. Works offline.

IN A PIPELINE
  Not a pipeline stage. Pipe its output to a pager or to grep, never into
  another aa-* tool.

EXAMPLES
  aa-guide | less
  aa-guide | grep -A4 'OUTPUT NAMING'
  aa-guide | grep aa-nasc

NOTE
  For one tool in detail run <tool> --help (what matters) or <tool> --help-all
  (every option).

MORE
  --help-all  the complete reference, every option
```

## aa_help

```bash
aa-help — Ask in plain English; get an aa-* pipeline, and run it if you
  choose.
  [interactive]

WHAT IT DOES
  Sends your question to a Gemini model on Vertex AI together with the
  matching pages of your indexed documentation and the paths of acoustic files
  (.raw, .nc, .evr, .evl) in the current directory and under your home
  directory. The model answers, asks one multiple-choice question, or proposes
  a pipeline of aa-* commands. Every proposed stage is checked against the
  installed aa-* tools and rejected if it contains shell syntax. You then
  pick: run it, copy it, show it as a one-liner, or cancel. A pipeline runs
  without a shell: each stage's stdout feeds the next stage's stdin, as in a |
  b | c.

INPUT (argument or stdin)
  Nothing. The question is the argument; with no question aa-help starts an
  interactive prompt (aa-help>).

OUTPUT (stdout)
  The plan (summary, commands, expected output, risks) or the answer,
  formatted for the terminal. When you run a pipeline, its last stage's output
  follows.

METADATA
  aa-help itself records nothing. The pipelines it runs are ordinary aa-*
  runs: their outputs carry provenance, are named <base>_<hash8>.<ext>, and
  are reused when an identical product exists.

OPTIONS
  QUESTION ...	   one shot: plan (and offer to run) this, then exit
  --no-execute	   plan only, never run anything (default: offer to run)
  --model NAME	   Vertex AI model for this run (default from the config:
					 gemini-2.5-pro)
  --setup			configuration wizard: project, location, model
  --config / --edit  print the config path / open it in $EDITOR
  --reindex		  rebuild the documentation index (Vertex AI embeddings)
  --refresh-index	index only changed documentation files
  --index-stats	  how many files and chunks are indexed
  --refresh-files	rescan for acoustic files now
  --files-stats	  what the acoustic-file index holds
  --version		  aalibrary version

FILES & URIs
  Config ~/.config/aalibrary/aa_help.toml ($XDG_CONFIG_HOME is honoured);
  beside it knowledge.db (the documentation index, built from the
  knowledge_dirs in the config) and file_index.json (acoustic files under
  file_scan_root, default your home directory). Planning and indexing need
  network access to Vertex AI and Application Default Credentials (gcloud auth
  application-default login). --help, --help-all, --version, --config, --edit
  and --index-stats need neither a configuration nor the network;
  --refresh-files and --files-stats work offline once aa-help is configured.

IN A PIPELINE
  Not a pipeline stage; it builds pipelines. Stages must be installed aa-*
  tools. A plan that uses the network (aa-raw, aa-fetch, aa-ed, aa-find,
  aa-get, aa-upload, aa-download, aa-cruisepack, aa-setup, aa-refresh, or any
  gs:// / s3:// / http(s):// argument) asks for an extra confirmation before
  it runs.

EXAMPLES
  aa-help
  aa-help "convert D20160703-T060000.raw to Sv and draw an echogram"
  aa-help --no-execute "compute NASC for D20160703-T060000.raw"
  aa-help "what is the difference between MVBS and NASC?"

NOTE
  Menus: up/down arrows move, Enter selects, Ctrl-C cancels the current plan.
  Ctrl-D, /exit, /quit or :q leave aa-help; /help or ? at the prompt shows the
  keys.

NOTE
  Read a plan before running it. aa-help only runs installed aa-* tools, but
  the model can still pick the wrong tool, flag or file.

MORE
  --help-all  the complete reference, every option
```

## aa_impulse

```bash
aa-impulse — Impulse-noise mask for Sv; --apply also writes cleaned Sv.
  [scientific transform (hashed) · product: mask]

WHAT IT DOES
  Runs echopype.clean.mask_impulse_noise (Ryan et al. 2015). Sv is first
  averaged in --depth-bin bins; a sample is impulse noise when it is more than
  --impulse-threshold above BOTH the ping --num-side-pings before it and the
  ping --num-side-pings after it.

  The mask file holds one variable, impulse_mask: float64, 1 = impulse noise,
  0 = keep, dims (channel, range_sample, ping_time). Samples whose binned Sv
  is NaN (e.g. beyond a channel's range) also come out as 1. echopype copies
  Sv's long_name/units attributes onto the mask; they do not describe it.

INPUT (argument or stdin)
  One Sv NetCDF (.nc/.netcdf4) path or gs:// URI that has the --range-var
  variable: depth (add it with aa-depth) or echo_range. An EchoData file
  without Sv is calibrated first with compute_Sv defaults (recorded in the
  provenance as an implicit step).

OUTPUT (stdout)
  The MASK's absolute path (or gs:// URI), one line. With --apply the cleaned
  Sv's path goes to stderr instead, as 'aa-impulse: cleaned Sv: PATH' (also
  when it is reused).

METADATA
  Reads the input's provenance, appends this step with its canonical
  scientific options, computes the product hash, and embeds it all in the mask
  (NetCDF attributes aa_provenance, aa_product_hash, aa_base, aa_tool,
  history). The --apply file is its own product: same step, variant 'apply',
  kind sv, its own hash. Inspect with: aa-metadata FILE

OPTIONS
  -o, --output_path PATH  The mask file, used as given with the extension
						  forced to .nc. Local path or gs:// URI. Does not
						  move the --apply file.
  --apply				 Also write the input's Sv with impulse samples set
						  to NaN (all other variables copied).

SCIENTIFIC OPTIONS (change the product hash)
  --depth-bin		  Vertical averaging bin before the comparison, e.g. 5m.
					   (default: 5m)
  --num-side-pings	 Compare each ping with the ping this many pings before
					   and after it. (default: 2)
  --impulse-threshold  dB above both neighbours that counts as impulse noise,
					   e.g. 10dB. (default: 10.0dB)
  --range-var		  Vertical variable: depth or echo_range. (default:
					   depth)
  --use-index-binning  Bin by range_sample index (assumes uniform sample
					   spacing per channel). Faster.
  Flag order, alias spellings and explicit defaults do not change the hash.

FILES & URIs
  Reads NetCDF, local or gs://. Writes the mask to <base>_<hash>.nc beside the
  input (current directory for gs:// input), or -o, or --dest. With --apply a
  second <base>_<hash>.nc (a different hash) goes beside the input or into
  --dest, never to -o. AA_NAMING=legacy: <stem>_impulse_mask.nc and
  <stem>_impulse_cleaned.nc beside the input. Identical earlier results are
  reused.

IN A PIPELINE
  After aa-sv and aa-depth: ... | aa-sv | aa-depth | aa-impulse | aa-graph
  (draws the mask). Only the mask travels down the pipe.

EXAMPLES
  aa-nc x.raw --sonar_model EK60 | aa-sv | aa-depth | aa-impulse --apply --use-index-binning
  aa-impulse sv_depth.nc --impulse-threshold 12dB --use-index-binning

NOTE
  --use-index-binning is needed when the channels cover different depth ranges
  (e.g. EK60 38 + 120 kHz): echopype 0.11.1's default depth binning then fails
  with "conflicting sizes for dimension 'depth_bins'".

COMMON OPTIONS
  --force				 recompute even if an identical product already
						  exists
  --base NAME			 name outputs after NAME instead of the input's base
						  name
  --dest DIR|gs://PREFIX  write the default-named output there instead of
						  beside the input
  --help-all			  the complete reference, every option
```

## aa_location

```bash
aa-location — Add latitude/longitude to an Sv dataset
  (echopype.consolidate.add_location).
  [scientific transform (hashed) · product: sv]

WHAT IT DOES
  Interpolates the platform position from the EchoData Platform group (NMEA
  fixes by default) to each ping_time of the Sv dataset and adds 'latitude'
  and 'longitude' (ping_time). Every input variable is kept unchanged; the
  product kind is the input's (sv, mvbs, ...).

INPUT (argument or stdin)
  One Sv .nc path or gs:// URI (aa-sv output, or aa-depth etc. downstream).

OUTPUT (stdout)
  The output file's absolute path (or gs:// URI).

METADATA
  Reads the input's provenance, appends this step with its canonical
  scientific options, and embeds it all in the output (NetCDF attributes
  aa_provenance, aa_recipe, aa_product_hash, aa_base, aa_tool, history). Two
  hashes: the recipe (this step and every step before it, without the data:
  the <hash8> in the name, the same for any data processed this way) and the
  product hash (this recipe applied to this input: decides reuse). The base
  name is carried through unchanged. Inspect with: aa-metadata FILE

OPTIONS
  --echodata ED.nc		  the EchoData (aa-nc output) the Sv came from.
							Needed in practice: without it the input itself is
							opened as EchoData, which an Sv file is not. Its
							content enters the product hash.
  --nmea-sentence GGA	   use only this NMEA sentence type
  --datagram-type MRU1|IDX  EK only: take position from MRU1 or IDX datagrams
							instead of NMEA (any case; cannot be combined with
							--nmea-sentence)
  -o, --output_path PATH	Explicit output, used exactly as given. Local path
							or gs:// URI.

SCIENTIFIC OPTIONS (change the product hash)
  --datagram-type  Position source: MRU1 or IDX datagrams (EK only); default
				   NMEA.
  --nmea-sentence  NMEA sentence type to use (e.g. GGA); default all.
  --echodata	   EchoData file: its content identity (not its path) enters
				   the hash.
  Flag order, alias spellings and explicit defaults do not change the hash.

FILES & URIs
  Reads Sv .nc and EchoData .nc/.zarr, local or gs://. Writes <base>_<hash>.nc
  beside the input (current directory for gs:// input), or -o, or --dest. An
  identical earlier result is reused. AA_NAMING=legacy: <stem>_loc.nc. Refuses
  to overwrite the input or the --echodata file.

IN A PIPELINE
  After aa-sv: ED=$(aa-nc x.raw --sonar_model EK60); aa-sv $ED | aa-location
  --echodata $ED

EXAMPLES
  ED=$(aa-nc D20160703-T060000.raw --sonar_model EK60)
  aa-sv "$ED" | aa-location --echodata "$ED"
  aa-location Sv.nc --echodata ED.nc --nmea-sentence GGA -o Sv_loc.nc

COMMON OPTIONS
  --force				 recompute even if an identical product already
						  exists
  --base NAME			 name outputs after NAME instead of the input's base
						  name
  --dest DIR|gs://PREFIX  write the default-named output there instead of
						  beside the input
  --help-all			  the complete reference, every option
```

## aa_metadata

```bash
aa-metadata — Show what made a product: inputs, pipeline, options, hash.
  [inspector (read-only)]

WHAT IT DOES
  Reads the provenance every aa-* tool embeds in its outputs and prints it:
  base name, recipe (the processing, which is the <hash8> in the file name and
  is the same for any data processed the same way), product hash (this
  processing of this data), the inputs and raw sources (with their origin,
  e.g. the NCEI object a .raw came from), every scientific step in order with
  its canonical options, and the software versions. --verify recomputes both
  hashes from the recorded step and checks that the hash in the file name is
  the recorded recipe.

INPUT (argument or stdin)
  Paths or gs:// URIs, one per line (or as arguments). aa/1 JSON handles work
  too.

OUTPUT (stdout)
  A summary per file; with --json the provenance document; with --hash the
  hash; with --tee the input path unchanged (summary goes to stderr).

METADATA
  Read-only: reads files and metadata, writes nothing.

OPTIONS
  --json	print the provenance document (compact, one line per file)
  --hash	print only the product hash (--full for all 64 hex digits)
  --verify  recompute the hash from the recorded step; exit 4 on mismatch
  --tee	 pass the path through on stdout; summary to stderr

FILES & URIs
  Local files, directories (.zarr) and gs:// URIs. For gs:// the file is read
  through a gcsfuse mount when one covers it, otherwise downloaded once to the
  cache (AA_CACHE_DIR).

IN A PIPELINE
  An inspector: at the end of a pipe, or in the middle with --tee.

EXAMPLES
  aa-metadata HB1603_EK60_20160703T060000-20160703T120000_71957ca9.nc
  aa-nc x.raw --sonar_model EK60 | aa-sv | aa-metadata --tee | aa-clean
  aa-metadata gs://bucket/derived/x_71957ca9.nc --verify

MORE
  --help-all  the complete reference, every option
```

## aa_min

```bash
aa-min — Impulse-noise mask, stored under the variable name 'Sv'.
  [scientific transform (hashed) · product: mask]

WHAT IT DOES
  The same computation as aa-impulse: Sv is averaged in --depth_bin bins and a
  sample is impulse noise when it is more than --impulse_noise_threshold above
  BOTH the ping --num_side_pings before it and the ping --num_side_pings after
  it (Ryan et al. 2015).

  The output holds one variable NAMED 'Sv' that is the MASK, not Sv: float64,
  1 = impulse noise, 0 = keep, dims (channel, range_sample, ping_time), with
  Sv's long_name/units attributes copied by echopype. The name is kept for
  compatibility; aa-impulse writes the same mask as 'impulse_mask'.

INPUT (argument or stdin)
  One Sv NetCDF (.nc/.netcdf4) path or gs:// URI that has the --range_var
  variable: depth (add it with aa-depth) or echo_range.

OUTPUT (stdout)
  The mask's absolute path (or gs:// URI), one line.

METADATA
  Reads the input's provenance, appends this step with its canonical
  scientific options, and embeds it all in the output (NetCDF attributes
  aa_provenance, aa_recipe, aa_product_hash, aa_base, aa_tool, history). Two
  hashes: the recipe (this step and every step before it, without the data:
  the <hash8> in the name, the same for any data processed this way) and the
  product hash (this recipe applied to this input: decides reuse). The base
  name is carried through unchanged. Inspect with: aa-metadata FILE

OPTIONS
  -o, --output_path PATH  Explicit output; '_mask-impulse-noise' is appended
						  to its stem and .nc forced, as always. Local path or
						  gs:// URI.

SCIENTIFIC OPTIONS (change the product hash)
  --depth_bin				Vertical averaging bin before the comparison,
							 e.g. 5m. (default: 5m)
  --num_side_pings		   Compare each ping with the ping this many pings
							 before and after it. (default: 2)
  --impulse_noise_threshold  dB above both neighbours that counts as impulse
							 noise, e.g. 10dB. (default: 10.0dB)
  --range_var				Vertical variable: depth or echo_range. (default:
							 depth)
  --use_index_binning		Bin by range_sample index (assumes uniform sample
							 spacing per channel). Faster.
  Flag order, alias spellings and explicit defaults do not change the hash.

FILES & URIs
  Reads NetCDF, local or gs://. Writes <base>_<hash>.nc beside the input
  (current directory for gs:// input), or -o, or --dest. AA_NAMING=legacy:
  <stem>_mask-impulse-noise.nc beside the input. An identical earlier result
  is reused.

IN A PIPELINE
  After aa-sv and aa-depth: ... | aa-sv | aa-depth | aa-min | aa-graph.
  Downstream tools that look for a variable called Sv will find this mask.

EXAMPLES
  aa-nc x.raw --sonar_model EK60 | aa-sv | aa-depth | aa-min --use_index_binning
  aa-min sv_depth.nc --impulse_noise_threshold 12dB --use_index_binning -o masks/run1.nc   # -> masks/run1_mask-impulse-noise.nc

NOTE
  --use_index_binning is needed when the channels cover different depth ranges
  (e.g. EK60 38 + 120 kHz): echopype 0.11.1's default depth binning then fails
  with "conflicting sizes for dimension 'depth_bins'".

COMMON OPTIONS
  --force				 recompute even if an identical product already
						  exists
  --base NAME			 name outputs after NAME instead of the input's base
						  name
  --dest DIR|gs://PREFIX  write the default-named output there instead of
						  beside the input
  --help-all			  the complete reference, every option
```

## aa_mvbs

```bash
aa-mvbs — Average Sv onto a regular range x time grid (MVBS).
  [scientific transform (hashed) · product: mvbs]

WHAT IT DOES
  Runs echopype.commongrid.compute_MVBS: averages Sv in the linear domain over
  bins of --range_bin metres of echo_range (or depth) and --ping_time_bin of
  ping_time. Output variable: Sv (the bin means, in dB) on channel x ping_time
  x echo_range (or depth), each coordinate being the bin's start.

INPUT (argument or stdin)
  One flat Sv .nc/.netcdf4 path or gs:// URI, from aa-sv or aa-clean (not the
  EchoData file from aa-nc). --range_var depth needs a depth variable: run
  aa-depth first.

OUTPUT (stdout)
  The MVBS file's absolute path (or gs:// URI).

METADATA
  Reads the input's provenance, appends this step with its canonical
  scientific options, and embeds it all in the output (NetCDF attributes
  aa_provenance, aa_recipe, aa_product_hash, aa_base, aa_tool, history). Two
  hashes: the recipe (this step and every step before it, without the data:
  the <hash8> in the name, the same for any data processed this way) and the
  product hash (this recipe applied to this input: decides reuse). The base
  name is carried through unchanged. Inspect with: aa-metadata FILE

OPTIONS
  -o, --output_path PATH		Explicit output; '_mvbs' is ALWAYS appended to
								its stem and .nc forced (-o out.nc writes
								out_mvbs.nc). Local path or gs:// URI.
  --range_var echo_range|depth  range coordinate to bin (default: echo_range)
  --range_bin 20m			   range bin size, metres (default: 20m)
  --ping_time_bin 20s		   time bin size, a pandas frequency such as 20s
								or 1min (default: 20s)
  --skipna / --no_skipna		ignore NaN samples in the means (default:
								skip)
  --fill_value X				value for empty bins, in LINEAR sv: echopype
								converts it to dB (1e-12 -> -120 dB, 0 ->
								-inf; default: NaN)
  --closed left|right		   closed side of each bin (default: left)
  --range_var_max 150m		  bin only up to this range (default: data
								maximum)
  --flox_kwargs K=V ...		 extra flox options, e.g. min_count=5
  --method map-reduce|coarsen|block
								flox strategy; performance only, not hashed
								(default: map-reduce)
  --reindex					 flox reindexing; performance only, not hashed;
								map-reduce only

SCIENTIFIC OPTIONS (change the product hash)
  --range_var				   Range coordinate binned: echo_range or depth.
								(default: echo_range)
  --range_bin				   Range bin size; '20m' and '20.0 m' are the
								same value. (default: 20m)
  --ping_time_bin			   Time bin size; '20s' and '20.0s' are the same
								value. (default: 20s)
  --no_skipna, --no-skipna, --skipna
								Skip NaN samples in the bin means (--skipna /
								--no_skipna). (default: True)
  --fill_value				  Value of empty bins, in linear sv (converted
								to dB with the means); default NaN, recorded
								as "NaN".
  --closed					  Closed side of each bin interval. (default:
								left)
  --range_var_max			   Upper end of the range bins.
  --flox_kwargs, --flox-kwargs  Extra flox options, except engine, method and
								reindex, which only change speed.
  Flag order, alias spellings and explicit defaults do not change the hash.

FILES & URIs
  Reads a flat Sv NetCDF, local or gs://. Writes <base>_<hash8>.nc beside the
  input (current directory for gs:// input), or in --dest DIR|gs://PREFIX, or
  at -o (+'_mvbs'). AA_NAMING=legacy restores the old default <input
  stem>_mvbs.nc. An identical earlier result is reused.

IN A PIPELINE
  aa-nc | aa-sv [| aa-clean] | aa-mvbs | aa-graph. Averages the variable named
  Sv: after aa-clean that is still the uncorrected Sv (aa-clean puts the
  cleaned values in Sv_corrected).

EXAMPLES
  aa-nc D20160703-T060000.raw --sonar_model EK60 | aa-sv | aa-mvbs
  aa-mvbs sv.nc --range_bin 5m --ping_time_bin 1min
  aa-sv ed.nc | aa-depth | aa-mvbs --range_var depth --range_bin 10m

COMMON OPTIONS
  --force				 recompute even if an identical product already
						  exists
  --base NAME			 name outputs after NAME instead of the input's base
						  name
  --dest DIR|gs://PREFIX  write the default-named output there instead of
						  beside the input
  --help-all			  the complete reference, every option
```

## aa_mvbs_index

```bash
aa-mvbs-index — MVBS binned by ping and sample counts (not seconds, metres).
  [scientific transform (hashed) · product: mvbs]

WHAT IT DOES
  Runs echopype.commongrid.compute_MVBS_index_binning: averages Sv in the
  linear domain over blocks of --ping-num pings x --range-sample-num range
  samples (the last block of each axis may be shorter). Output: Sv (block
  means, dB) and echo_range (each block's smallest range) on channel x
  ping_time x range_sample. Unlike aa-mvbs, bins are counts, not metres and
  seconds.

INPUT (argument or stdin)
  One .nc/.netcdf4 path or gs:// URI: a flat Sv file from aa-sv or aa-clean,
  or an EchoData file from aa-nc (then Sv is first computed with
  echopype.calibrate.compute_Sv defaults, EK60/AZFP only; this is recorded in
  the provenance).

OUTPUT (stdout)
  The MVBS file's absolute path (or gs:// URI).

METADATA
  Reads the input's provenance, appends this step with its canonical
  scientific options, and embeds it all in the output (NetCDF attributes
  aa_provenance, aa_recipe, aa_product_hash, aa_base, aa_tool, history). Two
  hashes: the recipe (this step and every step before it, without the data:
  the <hash8> in the name, the same for any data processed this way) and the
  product hash (this recipe applied to this input: decides reuse). The base
  name is carried through unchanged. Inspect with: aa-metadata FILE

OPTIONS
  -o, --output_path PATH  Explicit output, used as given with the extension
						  forced to .nc. Local path or gs:// URI.
  --range-sample-num N	range samples per bin (default: 100)
  --ping-num N			pings per bin (default: 100)

SCIENTIFIC OPTIONS (change the product hash)
  --range-sample-num  Range samples per bin. (default: 100)
  --ping-num		  Pings per bin. (default: 100)
  Flag order, alias spellings and explicit defaults do not change the hash.

FILES & URIs
  Reads NetCDF, local or gs://. Writes <base>_<hash8>.nc beside the input
  (current directory for gs:// input), or in --dest DIR|gs://PREFIX, or at -o.
  AA_NAMING=legacy restores the old default <input stem>_mvbs_index.nc. An
  identical earlier result is reused.

IN A PIPELINE
  aa-nc | aa-sv [| aa-clean] | aa-mvbs-index | aa-graph. Averages the variable
  named Sv: after aa-clean that is still the uncorrected Sv.

EXAMPLES
  aa-nc D20160703-T060000.raw --sonar_model EK60 | aa-sv | aa-mvbs-index
  aa-mvbs-index sv.nc --range-sample-num 30 --ping-num 5

COMMON OPTIONS
  --force				 recompute even if an identical product already
						  exists
  --base NAME			 name outputs after NAME instead of the input's base
						  name
  --dest DIR|gs://PREFIX  write the default-named output there instead of
						  beside the input
  --help-all			  the complete reference, every option
```

## aa_nasc

```bash
aa-nasc — Integrate Sv into NASC (m2 nmi-2) on depth x distance cells.
  [scientific transform (hashed) · product: nasc]

WHAT IT DOES
  Runs echopype.commongrid.compute_NASC: bins Sv by --range_bin metres of
  depth and --dist_bin of along-track distance (from latitude/longitude), then
  NASC = mean sv x mean cell height x 4 pi 1852^2 per cell. Output: NASC on
  channel x distance x depth (bin starts; distance in nmi), plus mean
  ping_time, latitude and longitude per distance bin.

INPUT (argument or stdin)
  One flat Sv .nc/.netcdf4 path or gs:// URI that has depth, latitude and
  longitude. aa-sv's output has none of these: run aa-depth and aa-location
  first (see IN A PIPELINE).

OUTPUT (stdout)
  The NASC file's absolute path (or gs:// URI).

METADATA
  Reads the input's provenance, appends this step with its canonical
  scientific options, and embeds it all in the output (NetCDF attributes
  aa_provenance, aa_recipe, aa_product_hash, aa_base, aa_tool, history). Two
  hashes: the recipe (this step and every step before it, without the data:
  the <hash8> in the name, the same for any data processed this way) and the
  product hash (this recipe applied to this input: decides reuse). The base
  name is carried through unchanged. Inspect with: aa-metadata FILE

OPTIONS
  -o, --output_path PATH  Explicit output; '_nasc' is ALWAYS appended to its
						  stem and .nc forced (-o out.nc writes out_nasc.nc).
						  Local path or gs:// URI.
  --range_bin 10m		 depth bin size, metres (default: 10m)
  --dist_bin 0.5nmi	   distance bin size, nautical miles (default: 0.5nmi)
  --skipna / --no_skipna  ignore NaN samples in the means (default: skip)
  --closed left|right	 closed side of each bin (default: left)
  --flox_kwargs K=V ...   extra flox options, e.g. min_count=5
  --method NAME		   flox strategy; performance only, not hashed
						  (default: map-reduce)

SCIENTIFIC OPTIONS (change the product hash)
  --range_bin, --range-bin	  Depth bin size; '10m' and '10.0 m' are the
								same value. (default: 10m)
  --dist_bin, --dist-bin		Distance bin size; '0.5nmi' and '.5 nmi' are
								the same value. (default: 0.5nmi)
  --no_skipna, --no-skipna, --skipna
								Skip NaN samples in the bin means (--skipna /
								--no_skipna). (default: True)
  --closed					  Closed side of each bin interval. (default:
								left)
  --flox_kwargs, --flox-kwargs  Extra flox options, except engine, method and
								reindex, which only change speed.
  Flag order, alias spellings and explicit defaults do not change the hash.

FILES & URIs
  Reads a flat Sv NetCDF, local or gs://. Writes <base>_<hash8>.nc beside the
  input (current directory for gs:// input), or in --dest DIR|gs://PREFIX, or
  at -o (+'_nasc'). AA_NAMING=legacy restores the old default <input
  stem>_nasc.nc. An identical earlier result is reused.

IN A PIPELINE
  aa-sv ED.nc | aa-depth | aa-location --echodata ED.nc | aa-nasc, where ED.nc
  is the EchoData file from aa-nc (aa-location reads the GPS from it).
  Integrates the variable named Sv: after aa-clean that is still the
  uncorrected Sv.

EXAMPLES
  aa-nc D20160703-T060000.raw --sonar_model EK60	 # writes D20160703-T060000.nc
  aa-sv D20160703-T060000.nc | aa-depth \
	| aa-location --echodata D20160703-T060000.nc | aa-nasc
  aa-nasc sv_depth_loc.nc --range_bin 20m --dist_bin 1nmi

COMMON OPTIONS
  --force				 recompute even if an identical product already
						  exists
  --base NAME			 name outputs after NAME instead of the input's base
						  name
  --dest DIR|gs://PREFIX  write the default-named output there instead of
						  beside the input
  --help-all			  the complete reference, every option
```

## aa_nc

```bash
aa-nc — Convert a raw echosounder file (.raw) to an EchoData NetCDF.
  [EchoData builder (starts the chain) · product: echodata]

WHAT IT DOES
  Parses the .raw with echopype.open_raw and writes the multi-group EchoData
  NetCDF that aa-sv calibrates. No Sv, no noise removal: this is only the
  conversion stage. The .raw is never modified.

INPUT (argument or stdin)
  One .raw path or gs:// URI (argument, or one line on stdin).

OUTPUT (stdout)
  The absolute path of the .nc (or its gs:// URI with -o/--dest gs://...).

METADATA
  Starts the provenance chain. Records the source file's identity (and its
  NCEI/GCS origin when known) and the conversion options inside the output
  (NetCDF attributes aa_provenance, aa_product_hash, aa_base). The base name
  comes from the source file's stem unless you set it.

OPTIONS
  --sonar_model MODEL	 REQUIRED. EK60, EK80, AZFP, EA640, ...
  -o, --output_path PATH  Where to write; the extension is forced to .nc.
						  Local path or gs:// URI.

SCIENTIFIC OPTIONS (change the product hash)
  --sonar_model  Which echopype parser reads the file.
  Flag order, alias spellings and explicit defaults do not change the hash.

FILES & URIs
  Reads a local .raw, or gs://.../x.raw (through your gcsfuse mount when it
  covers the path, otherwise downloaded once to the cache). Writes <base>.nc
  beside the input, where <base> is the raw file's stem (or --base NAME); with
  a gs:// input the default is the current directory. If <base>.nc already
  exists from the same raw file and options, it is reused instead of converted
  again.

IN A PIPELINE
  First stage after the data source. Feed it from aa-raw or aa-download, pipe
  its output into aa-sv.

EXAMPLES
  aa-nc D20160703-T060000.raw --sonar_model EK60
  aa-raw ... | aa-nc --sonar_model EK60 | aa-sv | aa-clean
  aa-nc gs://bucket/raw/D20160703-T060000.raw --sonar_model EK60 --dest gs://bucket/nc/

COMMON OPTIONS
  --force				 recompute even if an identical product already
						  exists
  --base NAME			 name outputs after NAME instead of the input's base
						  name
  --dest DIR|gs://PREFIX  write the default-named output there instead of
						  beside the input
  --help-all			  the complete reference, every option
```

## aa_noise_est

```bash
aa-noise-est — Estimate background noise (Sv_noise) from Sv.
  [scientific transform (hashed) · product: noise]

WHAT IT DOES
  Runs echopype.clean.estimate_background_noise: the noise level of each block
  of --ping-num pings is the lowest mean calibrated power over blocks of
  --range-sample-num samples, optionally capped at --background-noise-max,
  then expressed as Sv (spreading and absorption loss added back). Writes a
  NetCDF with one variable, Sv_noise, on channel x ping_time x range_sample.
  This is the noise estimate that aa-clean subtracts (De Robertis &
  Higginbottom 2007); the input is not changed.

INPUT (argument or stdin)
  One .nc path or gs:// URI: a flat Sv file from aa-sv (needs Sv, echo_range,
  sound_absorption), or an EchoData file from aa-nc (then Sv is first computed
  with echopype.calibrate.compute_Sv defaults, EK60/AZFP only; this is
  recorded in the provenance).

OUTPUT (stdout)
  The noise file's absolute path (or gs:// URI).

METADATA
  Reads the input's provenance, appends this step with its canonical
  scientific options, and embeds it all in the output (NetCDF attributes
  aa_provenance, aa_recipe, aa_product_hash, aa_base, aa_tool, history). Two
  hashes: the recipe (this step and every step before it, without the data:
  the <hash8> in the name, the same for any data processed this way) and the
  product hash (this recipe applied to this input: decides reuse). The base
  name is carried through unchanged. Inspect with: aa-metadata FILE

OPTIONS
  -o, --output_path PATH		Explicit output, used exactly as given (no
								suffix, no extension change). Local path or
								gs:// URI.
  --ping-num N				  pings per noise-estimation block (default: 20)
  --range-sample-num N		  range samples per block (default: 20)
  --background-noise-max=VALdB  cap on the noise estimate, e.g.
								--background-noise-max=-125dB (write '='
								before a negative value); default: no cap

SCIENTIFIC OPTIONS (change the product hash)
  --ping-num			  Pings per noise-estimation block. (default: 20)
  --range-sample-num	  Range samples per noise-estimation block. (default:
						  20)
  --background-noise-max  Upper limit on the noise estimate; '-125dB' and
						  '-125.0dB' are the same value.
  Flag order, alias spellings and explicit defaults do not change the hash.

FILES & URIs
  Reads NetCDF, local or gs://. Writes <base>_<hash8>.nc beside the input
  (current directory for gs:// input), or in --dest DIR|gs://PREFIX, or at -o.
  AA_NAMING=legacy restores the old default <input stem>_noise.nc. An
  identical earlier result is reused.

IN A PIPELINE
  A side branch for inspecting noise: aa-nc | aa-sv | aa-noise-est | aa-graph.
  Its output holds only Sv_noise, so it does not feed aa-clean or aa-mvbs
  (aa-clean estimates the same noise itself).

EXAMPLES
  aa-nc D20160703-T060000.raw --sonar_model EK60 | aa-sv | aa-noise-est
  aa-noise-est sv.nc --ping-num 50 --range-sample-num 200 \
	--background-noise-max=-120.0dB

COMMON OPTIONS
  --force				 recompute even if an identical product already
						  exists
  --base NAME			 name outputs after NAME instead of the input's base
						  name
  --dest DIR|gs://PREFIX  write the default-named output there instead of
						  beside the input
  --help-all			  the complete reference, every option
```

## aa_plot

```bash
aa-plot — Interactive echogram HTML page with drawing tools and EVL/EVR
  export.
  [representation (renders a product) · product: html]

WHAT IT DOES
  Renders one variable (default Sv) of a NetCDF product as a standalone HTML
  page: one tab per channel, hover values, click-to-pin, a colormap picker,
  freehand/polyline/region drawing tools that export Echoview EVL (lines) and
  EVR (regions) files, and a data summary that shows the product's provenance
  chain (inputs and every pipeline step with its options). Each tab's depth
  axis comes from that channel's own echo_range/depth.

INPUT (argument or stdin)
  One NetCDF path or gs:// URI (Sv, cleaned Sv, MVBS, a mask, a cluster map,
  ...).

OUTPUT (stdout)
  The HTML page's absolute path (or gs:// URI), one line.

METADATA
  Copies the provenance of the product it shows into the output (PNG text
  chunks, or a JSON block in the HTML head) and adds a non-scientific
  rendering step. The output is named after the product it shows, with its own
  extension.

OPTIONS
  --var NAME					variable to plot (default: Sv, Sv_clean, MVBS,
								TS, NASC, cluster_map, else the first data
								variable)
  --single | --channel NAME | --frequency HZ
								one channel instead of one tab per channel
  --vmin DB --vmax DB		   colour limits (default: the data range)
  --y NAME					  y axis (default: depth, echo_range, ... then
								range_sample)
  --decimate N				  plot every Nth ping (large files)
  --ymin M --ymax M			 depth window
  --no-draw					 no drawing tools and no EVL/EVR export
  -o, --output_path PATH		explicit output, local or gs://; '.html' is
								added when the name doesn't end in it (as
								Panel always did)
  --no-overwrite				exit 1 if a different file is already at the
								output path (an identical rendering is reused)
  --quiet					   warnings only on stderr

RENDERING OPTIONS (identify this rendering; the science shown is the input's)
  --var			 variable to plot
  --all			 one tab per channel (the default for multi-channel data)
  --single		  one channel only (channel 0 unless --channel/--frequency)
  --frequency	   the channel nearest this nominal frequency (Hz)
  --channel		 the channel with this exact name
  --x			   x axis name
  --y			   y axis name
  --no-flip		 don't draw depth/range increasing downwards
  --vmin			lower colour limit
  --vmax			upper colour limit
  --cmap			initial colormap (default: inferno)
  --width		   minimum plot width in px (default: 250)
  --height		  plot height in px (default: 450)
  --toolbar		 toolbar position (default: above)
  --no-hover		no hover tooltip
  --no-crosshair	no crosshair
  --no-cmap-picker  no colormap picker
  --no-log		  no data summary panel
  --no-draw		 no drawing tools
  --decimate		every Nth ping (default: 1)
  --ymin			top of the depth window
  --ymax			bottom of the depth window

FILES & URIs
  Writes <name of the input product>.html beside the input (current directory
  for gs:// input): the page for D20160703-T060000_35e8864f.nc is
  D20160703-T060000_35e8864f.html. Re-rendering with the same options reuses
  the page; different options replace it (use -o to keep several).
  AA_NAMING=legacy restores <stem>_plot.html. EVL/EVR files saved from the
  page are named <input product>_lines.evl and <input product>_regions.evr.

IN A PIPELINE
  Last stage: aa-nc x.raw --sonar_model EK60 | aa-sv | aa-plot

EXAMPLES
  aa-nc x.raw --sonar_model EK60 | aa-sv | aa-plot --vmin -80 --vmax -30
  aa-plot gs://bucket/derived/x_35e8864f.nc --frequency 38000 --dest gs://bucket/pages/

NOTE
  --group-by is accepted for backwards compatibility but has no effect (tabs
  are always per channel), so it is not a rendering option.

NOTE
  EVL/EVR export needs a time x axis (ping_time) and a metre y axis (depth or
  echo_range). A y axis in range_sample indices is warned about on stderr and
  in the page.

COMMON OPTIONS
  --force				 recompute even if an identical product already
						  exists
  --base NAME			 name outputs after NAME instead of the input's base
						  name
  --dest DIR|gs://PREFIX  write the default-named output there instead of
						  beside the input
  --help-all			  the complete reference, every option
```

## aa_raw

```bash
aa-raw — Download one raw echosounder file (.raw, with its .idx/.bot) from
  NCEI.
  [source (fetches data) · product: raw]

WHAT IT DOES
  Finds the file in NCEI's Water Column Sonar Data archive (public S3 bucket
  noaa-wcsd-pds, key data/raw/<ship>/<survey>/<sonar>/<file>) and downloads it
  into --file_download_directory, together with its .idx and .bot companions
  when NCEI has them. The ship name is matched to NCEI's folder spelling
  (close matches are accepted). A local file of the same name is replaced.
  --upload_to_gcp also copies the files to the aalibrary GCS bucket.

INPUT (argument or stdin)
  Nothing. aa-raw is a source: the file is named by the flags, stdin is not
  read.

OUTPUT (stdout)
  The absolute path of the .raw, one line, and nothing else (aalibrary's own
  messages go to stderr, also with --upload_to_gcp). Empty on failure (exit
  1).

METADATA
  Writes <file>.aa.json beside the .raw (and beside the .idx/.bot): the NCEI
  object it was downloaded from (s3://noaa-wcsd-pds/data/raw/...), its MD5 and
  size, and ship/survey/sonar as NCEI spells them. aa-nc records that origin
  as the source of the .nc it writes (aa-metadata FILE.nc shows it; products
  made from the .nc point back to the .nc). The sidecar never changes a hash:
  downstream tools identify the .raw by its content either way. The .raw
  itself is not modified.

OPTIONS
  --file_name NAME			  REQUIRED. File name with extension, e.g.
								D20190804-T113723.raw
  --ship_name NAME			  REQUIRED. e.g. Henry_B._Bigelow (spelling is
								matched to NCEI's)
  --survey_name NAME			REQUIRED. e.g. HB1907
  --sonar_model NAME			REQUIRED. NCEI's sonar folder, e.g. EK60, EK80
  --file_download_directory DIR
								where to download (default: current directory;
								created)
  --upload_to_gcp			   also upload the files to the aalibrary GCS
								bucket
  --quiet / --debug			 fewer / more log messages on stderr

SCIENTIFIC OPTIONS
  None. A fetched file is identified by its content (MD5), not by the
  options that selected it, so the same file always has the same identity.

FILES & URIs
  Reads NCEI's public bucket anonymously. aalibrary also looks up the file's
  copy in its GCS bucket (ggn-nmfs-aa-prod-1-data: aalibrary selects the
  production project when imported), so Google Cloud credentials are needed
  even without --upload_to_gcp. Writes DIR/<file>.raw, DIR/<stem>.idx,
  DIR/<stem>.bot and a .aa.json for each.

IN A PIPELINE
  First stage of a chain: aa-raw ... | aa-nc --sonar_model EK60 | aa-sv | ...
  It takes no stdin, so nothing can be piped into it; to fetch many files use
  aa-request ... | aa-fetch -.

EXAMPLES
  aa-raw --file_name D20190804-T113723.raw --ship_name Henry_B._Bigelow \
		 --survey_name HB1907 --sonar_model EK60 --file_download_directory ./downloads
  aa-raw ... | aa-nc --sonar_model EK60 | aa-sv | aa-graph
  aa-metadata ./downloads/D20190804-T113723.raw	 # origin and MD5 from the sidecar

MORE
  --help-all  the complete reference, every option
```

## aa_refresh

```bash
aa-refresh — Reinstall aalibrary and AA-SI-KMEANS from GitHub main.
  [utility]

WHAT IT DOES
  For each library: pip uninstall, then pip install --force-reinstall from the
  main branch on GitHub, into the Python environment aa-refresh itself runs
  in, with a live progress display. This is also how new aa-* tools reach your
  PATH: pip creates a tool's command (aa-metadata, aa-download, ...) only when
  it installs the package, so a tool added since your last install says
  'command not found' until you refresh.

INPUT (argument or stdin)
  Nothing.

OUTPUT (stdout)
  A progress display, then a summary table (library, ok/failed, time). For a
  failed library, the last lines of pip's output.

METADATA
  Produces no scientific product and records no provenance.

OPTIONS
  --only PIP_NAME  refresh one library: aalibrary or AA-SI-KMEANS (default:
				   both)

FILES & URIs
  Installs into the active environment (the interpreter that runs aa-refresh).
  Needs network access to GitHub and PyPI.

IN A PIPELINE
  Not a pipeline stage. Run it on its own, inside the environment you want to
  update (e.g. after 'source ~/venv313/bin/activate').

EXAMPLES
  aa-refresh
  aa-refresh --only aalibrary

NOTE
  It replaces a developer install from a git clone (pip install -e .) with the
  GitHub main version. In a clone, run 'pip install -e .' instead; that also
  installs new entry points.

NOTE
  Exits 1 if any library failed to install, 0 otherwise. Recommended every
  week or two.

MORE
  --help-all  the complete reference, every option
```

## aa_request

```bash
aa-request — Build, merge or check the request YAML that aa-fetch reads.
  [utility]

WHAT IT DOES
  Writes the vessel / survey / instrument / time-window document from flags
  (the same document aa-get builds by asking questions), adds windows to an
  existing document, or validates one with --check. Dates and times are always
  written quoted: unquoted, YAML 1.1 reads 12:30:00 as the integer 45000 and
  2012-08-13 as a date. A document written through aa-request has such values
  repaired.

INPUT (argument or stdin)
  An existing document to merge into or check (as EXISTING.yaml or -i), read
  from stdin only when no file is named and no --vessel/--survey/--instrument
  is given.

OUTPUT (stdout)
  Without -o: the YAML document itself, ready for aa-fetch -. With -o: the
  written file's absolute path. With --json: a JSON summary (for the
  Workbench; aa-fetch cannot read it). --check prints nothing on stdout unless
  --json is given; its report goes to stderr.

METADATA
  Produces no scientific product and records no provenance.

OPTIONS
  --vessel, --survey, --instrument
								the request's keys; building a request needs
								all three (--sonar_model is an alias of
								--instrument)
  --from WHEN --to WHEN		 one window. A date (2012-08-13, meaning
								00:00:00) or a datetime (2012-08-13T06:00:00);
								--from 2012-08-13 --to 2012-08-14 is one whole
								day
  --window FROM/TO			  another window (repeatable); also needs
								--vessel, --survey and --instrument
  --split-days N				break each window into N-day windows
  --pad-minutes N			   start the first window N minutes earlier, so
								the file that spans its start is fetched
								(default 0)
  EXISTING.yaml, -i PATH		merge into this document; new windows join the
								request with the same vessel, survey and
								instrument
  --merge-windows			   combine overlapping or touching windows
  --check					   validate only, write nothing; exit 4 on any
								problem OR warning
  -o, --output_path PATH		write here and print the path (an existing
								file needs --force)
  --json						JSON summary instead of YAML
  -q, --quiet / --debug		 fewer / more log messages on stderr

FILES & URIs
  Reads a local request YAML (or stdin). Writes a file only with -o.

IN A PIPELINE
  Feeds aa-fetch, either through the pipe (aa-request ... | aa-fetch -) or
  through a file (-o request.yaml; aa-fetch request.yaml). Exit codes: 0 ok; 1
  unreadable input or write error; 2 usage (missing keys, bad dates, -o exists
  without --force); 4 --check found a problem or a warning (warnings include
  unquoted dates/times, overlapping windows, unknown keys and an empty
  document), or, without --check, the document has a problem and was not
  written.

EXAMPLES
  aa-request --vessel Alaska_Knight --survey CHS12AK --instrument ES60 \
			 --from 2012-08-13 --to 2012-08-14 -o request.yaml
  aa-request --check request.yaml
  aa-request request.yaml --merge-windows -o merged.yaml
  aa-request --vessel Alaska_Knight --survey CHS12AK --instrument ES60 \
			 --from 2012-08-13 --to 2012-08-20 --split-days 1 | aa-fetch -

MORE
  --help-all  the complete reference, every option
```

## aa_setup

```bash
aa-setup — Reinstall the AA-SI workstation environment on a Google Cloud VM.
  [utility]

WHAT IT DOES
  Downloads the current init.sh from the AA-SI_GPCSetup repository into your
  home directory (replacing any old copy), runs it, activates ~/venv313, runs
  'gcloud auth application-default login' (opens a browser sign-in; this is
  the credential every aa-* tool uses for gs:// and BigQuery), and sets the
  gcloud project to ggn-nmfs-aa-dev-1. With --account it also selects that
  gcloud account. Each step runs only if the one before it succeeded.

INPUT (argument or stdin)
  Nothing.

OUTPUT (stdout)
  The setup script's own output, and the gcloud prompts.

METADATA
  Produces no scientific product and records no provenance.

OPTIONS
  --account EMAIL  also run 'gcloud config set account EMAIL' (default: leave
				   the active account as it is)

FILES & URIs
  Writes ~/init.sh, downloaded (with sudo) from

	https://raw.githubusercontent.com/nmfs-ost/AA-SI_GPCSetup/main/init.sh

  What init.sh installs is defined in that repository. Needs network access.

IN A PIPELINE
  Not a pipeline stage. Run it on its own, in a terminal.

EXAMPLES
  aa-setup
  aa-setup --account first.last@noaa.gov

NOTE
  Exits with the status of the first step that failed (0 when all succeed).
  After it finishes, open a new shell or 'source ~/venv313/bin/activate' to
  use the environment it set up.

MORE
  --help-all  the complete reference, every option
```

## aa_show

```bash
aa-show — Print a NetCDF file's contents summary (the xarray repr).
  [inspector (read-only)]

WHAT IT DOES
  Opens the file with xarray and prints its root group: dimensions,
  coordinates, data variables and global attributes. Nothing is computed and
  nothing is written.

INPUT (argument or stdin)
  One .nc/.netcdf4 path or gs:// URI (argument, or one line on stdin).

OUTPUT (stdout)
  The xarray repr of the root group, exactly print(xr.open_dataset(path)). For
  a multi-group EchoData file (e.g. from aa-nc) the root holds only
  attributes, so the group names are listed on stderr. The full channel names
  (the repr cuts them off) go to stderr under 'channels:', repr-quoted with
  frequency_nominal: use them as "channel=<name>" in aa-detect-shoal and
  aa-detect-seafloor. When the file was written by an aa-* tool, one line 'aa:
  <kind> <hash8> base=<base>' also goes to stderr. A reader that stops early
  (| head) is not an error.

METADATA
  Read-only: reads files and metadata, writes nothing.

FILES & URIs
  Reads .nc / .netcdf4, local or gs:// (through a gcsfuse mount when one
  covers it, otherwise downloaded once to the cache, AA_CACHE_DIR).

IN A PIPELINE
  End of a pipe, for a human: aa-nc x.raw --sonar_model EK60 | aa-sv | aa-show

EXAMPLES
  aa-show D20160703-T060000_35e8864f.nc
  aa-nc x.raw --sonar_model EK60 | aa-sv | aa-show
  aa-show gs://bucket/derived/x_35e8864f.nc

MORE
  --help-all  the complete reference, every option
```

## aa_sonar

```bash
aa-sonar — Detect a raw file's sonar model and print it (EK60, EK80, AZFP,
  AD2CP).
  [inspector (read-only)]

WHAT IT DOES
  Reads only what it needs: .ad2cp and .azfp are recognized by extension, an
  AZFP .xml by its InstrumentType element, and a Simrad .raw by its first
  (configuration) datagram. The file is never modified.

INPUT (argument or stdin)
  One path or gs:// URI (argument, or one line on stdin): .raw, .azfp, .ad2cp
  or .xml.

OUTPUT (stdout)
  One token: EK60, EK80, AZFP, AD2CP, or UNKNOWN (exit 0; exit 1 with --strict
  and nothing printed). Ready for aa-nc --sonar_model.

METADATA
  Read-only: reads files and metadata, writes nothing.

OPTIONS
  --strict	exit 1 instead of printing UNKNOWN when the model can't be
			  determined
  --raw-name  print the detector's own name (AZFP6 for .azfp) instead of the
			  echopype identifier

FILES & URIs
  Reads a local file or gs://.../x.raw. A .raw or .xml on gs:// is read
  through a gcsfuse mount when one covers it, otherwise downloaded once to the
  cache (AA_CACHE_DIR), where a following aa-nc of the same URI finds it;
  .ad2cp and .azfp on gs:// are only checked for existence.

IN A PIPELINE
  Used in command substitution to feed aa-nc: aa-nc --sonar_model "$(aa-sonar
  x.raw)" x.raw

EXAMPLES
  aa-sonar D20160703-T060000.raw
  aa-nc --sonar_model "$(aa-sonar gs://bucket/raw/x.raw)" gs://bucket/raw/x.raw

MORE
  --help-all  the complete reference, every option
```

## aa_sound_speed

```bash
aa-sound-speed — Seawater sound speed (m/s) from temperature, salinity and
  pressure.
  [scientific transform (hashed) · product: sound_speed]

WHAT IT DOES
  Evaluates echopype.utils.uwa.calc_sound_speed: Mackenzie (1981) by default,
  or the AZFP formula. Reads no file. Without -o it only prints the number;
  with -o it writes a NetCDF product.

INPUT (argument or stdin)
  Nothing. All inputs are options.

OUTPUT (stdout)
  Without -o: the sound speed as a bare number, e.g. 1539.0866009307247 (the
  same text as always). With -o: the NetCDF's absolute path (or gs:// URI).

METADATA
  Without -o nothing is written and there is no provenance. With -o the NetCDF
  holds scalar 'sound_speed' (units m s-1) and the global attributes
  temperature_degC, salinity_psu, pressure_dbar, formula_source, tool, plus aa
  provenance (this step with its canonical options, no inputs; see
  aa-metadata). Its base name is --base or the -o file's stem.

OPTIONS
  --temperature DEGC	  temperature in deg C (default 27)
  --salinity PSU		  salinity in PSU / ppt (default 35)
  --pressure DBAR		 pressure in dbar (default 10)
  --formula-source NAME   Mackenzie (default) or AZFP
  -o, --output_path PATH  write a NetCDF instead of printing the number; .nc
						  is forced. Local path or gs:// URI.
  --quiet				 warnings and errors only on stderr
  --force				 with -o: recompute even if an identical file is
						  already there
  --base NAME			 with -o: base name recorded in the provenance

SCIENTIFIC OPTIONS (change the product hash)
  --temperature	 deg C. (default: 27.0)
  --salinity		PSU / ppt. (default: 35.0)
  --pressure		dbar. (default: 10.0)
  --formula-source  Mackenzie or AZFP. (default: Mackenzie)
  Flag order, alias spellings and explicit defaults do not change the hash.

FILES & URIs
  Writes only with -o: exactly that path with its extension forced to .nc.

IN A PIPELINE
  A starting point, not a filter: use the number in shell substitution, e.g.
  c=$(aa-sound-speed --temperature 4 --salinity 34 --quiet).

EXAMPLES
  aa-sound-speed --temperature 10 --salinity 33 --pressure 5
  aa-sound-speed --temperature 2 --salinity 35 --pressure 1000 -o ssp.nc

MORE
  --help-all  the complete reference, every option
```

## aa_splitbeam_angle

```bash
aa-splitbeam-angle — Add split-beam angles to an Sv dataset
  (echopype.consolidate.add_splitbeam_angle).
  [scientific transform (hashed) · product: sv]

WHAT IT DOES
  Computes the alongship and athwartship split-beam angles (degrees) from the
  EchoData Beam group and adds 'angle_alongship' and 'angle_athwartship'
  (channel x ping_time x range_sample) to the Sv dataset. Every input variable
  is kept unchanged.

INPUT (argument or stdin)
  One Sv .nc path or gs:// URI (aa-sv output). Not a .raw: convert with aa-nc
  first.

OUTPUT (stdout)
  The output file's absolute path (or gs:// URI).

METADATA
  Reads the input's provenance, appends this step with its canonical
  scientific options, and embeds it all in the output (NetCDF attributes
  aa_provenance, aa_recipe, aa_product_hash, aa_base, aa_tool, history). Two
  hashes: the recipe (this step and every step before it, without the data:
  the <hash8> in the name, the same for any data processed this way) and the
  product hash (this recipe applied to this input: decides reuse). The base
  name is carried through unchanged. Inspect with: aa-metadata FILE

OPTIONS
  --echodata ED.nc			 the EchoData (aa-nc output) the Sv came from.
							   Needed in practice: without it the input itself
							   is opened as EchoData, which an Sv file is not.
							   Its content enters the product hash.
  --waveform-mode CW|BB		REQUIRED. CW narrowband (EK60 is always CW) or
							   BB broadband
  --encode-mode power|complex  REQUIRED. EK60: power. 'power' needs CW.
  --pulse-compression		  BB + complex only
  --no-overwrite			   exit 1 instead of replacing a different
							   existing output
  -o, --output_path PATH	   Explicit output, used exactly as given. Local
							   path or gs:// URI.

SCIENTIFIC OPTIONS (change the product hash)
  --waveform-mode	  Transmit waveform: CW (narrowband) or BB (broadband).
  --encode-mode		Recorded echo encoding: power or complex.
  --pulse-compression  Apply pulse compression (BB + complex only).
  --echodata		   EchoData file: its content identity (not its path)
					   enters the hash.
  Flag order, alias spellings and explicit defaults do not change the hash.

FILES & URIs
  Reads Sv .nc and EchoData .nc/.zarr, local or gs://. Writes <base>_<hash>.nc
  beside the input (current directory for gs:// input), or -o, or --dest. An
  identical earlier result is reused (even with --no-overwrite).
  AA_NAMING=legacy: <stem>_splitbeam_angle.nc. Refuses to overwrite the input
  or the --echodata file.

IN A PIPELINE
  After aa-sv, e.g. before aa-detect-seafloor --method blackwell, which needs
  the angles: aa-sv $ED | aa-depth | aa-splitbeam-angle --echodata $ED ...

EXAMPLES
  ED=$(aa-nc D20160703-T060000.raw --sonar_model EK60)
  aa-sv "$ED" | aa-splitbeam-angle --echodata "$ED" --waveform-mode CW --encode-mode power
  aa-splitbeam-angle Sv.nc --echodata ED.nc --waveform-mode BB --encode-mode complex --pulse-compression   # EK80

COMMON OPTIONS
  --force				 recompute even if an identical product already
						  exists
  --base NAME			 name outputs after NAME instead of the input's base
						  name
  --dest DIR|gs://PREFIX  write the default-named output there instead of
						  beside the input
  --help-all			  the complete reference, every option
```

## aa_store

```bash
aa-store — Describe or verify a Zarr store: dims, chunks written, bytes,
  codec, lineage.
  [inspector (read-only)]

WHAT IT DOES
  info describes a store from its metadata and one object listing, so it
  answers for half-written stores too. verify judges it: complete (exit 0),
  coherent but unfinished (3, resumable), finished and wrong (4). The aa_write
  marker aa-combine stamps is what tells sparsity from an interrupted write.

INPUT (argument or stdin)
  Store paths or URIs (local, file://, gs://, s3://), one per line, when none
  are given as arguments: bare paths or aa/1 handle lines (aa-combine --json).

OUTPUT (stdout)
  The store's URI (file://... for a local store), so a pipe keeps flowing; the
  human summary goes to stderr. With --json one aa/1 document per store
  (NDJSON), which the Workbench Metadata panel reads.

METADATA
  Read-only: never opens a write handle. Reports the lineage the store
  records: aa-combine's provenance {tool, version, parents, at}, and the aa
  provenance when present (--json keys product, base, pipeline; a 'product'
  line in the summary).

OPTIONS
  info | verify	the subcommand (first argument)
  --json		   one JSON document per store on stdout
  --arrays		 include the per-array breakdown in --json
  --group PATH	 restrict to one group, e.g. Sonar
  --no-census	  skip the object count (huge remote stores)
  --max-objects N  stop the census after N objects (default 2000000)
  --strict		 verify: no marker + missing chunks = unfinished (exit 3)

FILES & URIs
  Reads .zarr stores, Zarr v2 or v3, local or remote through fsspec (gs://
  needs gcsfs, s3:// needs s3fs; local stores need nothing).

IN A PIPELINE
  An inspector, usually last: aa-combine -o out.zarr --json | aa-store verify
  --json. Exit codes: 0 ok, 1 unreadable, 2 usage, 3 partial, 4 verify failed
  (the worst store wins).

EXAMPLES
  aa-store info combined.zarr
  aa-store verify --json gs://bucket/HB1603_L1.zarr

MORE
  --help-all  the complete reference, every option
```

## aa_sv

```bash
aa-sv — Calibrate EchoData to volume backscattering strength (Sv).
  [scientific transform (hashed) · product: sv]

WHAT IT DOES
  Runs echopype.calibrate.compute_Sv on a converted EchoData file and writes a
  flat Sv dataset (Sv, echo_range, sound_absorption, ... on channel x
  ping_time x range_sample).

INPUT (argument or stdin)
  One EchoData .nc/.zarr path or gs:// URI, from aa-nc, aa-ed or aa-combine.

OUTPUT (stdout)
  The Sv file's absolute path (or gs:// URI).

METADATA
  Reads the input's provenance, appends this step with its canonical
  scientific options, and embeds it all in the output (NetCDF attributes
  aa_provenance, aa_recipe, aa_product_hash, aa_base, aa_tool, history). Two
  hashes: the recipe (this step and every step before it, without the data:
  the <hash8> in the name, the same for any data processed this way) and the
  product hash (this recipe applied to this input: decides reuse). The base
  name is carried through unchanged. Inspect with: aa-metadata FILE

OPTIONS
  -o, --output_path PATH	   Explicit output; '_Sv' is appended to its stem,
							   as always. Local path or gs:// URI.
  --waveform_mode CW|BB|FM	 EK80 only. Omit for EK60.
  --encode_mode complex|power  EK80 only. Omit for EK60.

SCIENTIFIC OPTIONS (change the product hash)
  --waveform_mode  EK80 waveform. FM and BB are the same computation.
  --encode_mode	EK80 encoding.
  Flag order, alias spellings and explicit defaults do not change the hash.

FILES & URIs
  Reads EchoData .nc or .zarr, local or gs://. Writes <base>_<hash>.nc beside
  the input (current directory for gs:// input), or -o, or --dest. An
  identical earlier result is reused.

IN A PIPELINE
  Second stage: aa-nc | aa-sv | aa-graph, or aa-nc | aa-sv | aa-depth | ...
  (see aa-guide). Note: aa-clean writes the cleaned values to Sv_corrected;
  aa-mvbs, aa-nasc and aa-graph read Sv.

EXAMPLES
  aa-nc D20160703-T060000.raw --sonar_model EK60 | aa-sv
  aa-sv file.nc --waveform_mode BB --encode_mode complex   # EK80

NOTE
  EK80 needs both --waveform_mode and --encode_mode; echopype refuses EK80
  data without them.

COMMON OPTIONS
  --force				 recompute even if an identical product already
						  exists
  --base NAME			 name outputs after NAME instead of the input's base
						  name
  --dest DIR|gs://PREFIX  write the default-named output there instead of
						  beside the input
  --help-all			  the complete reference, every option
```

## aa_swap_freq

```bash
aa-swap-freq — Index a dataset by frequency_nominal instead of channel.
  [scientific transform (hashed) · product: sv]

WHAT IT DOES
  Runs echopype.consolidate.swap_dims_channel_frequency: every variable on the
  'channel' dimension is re-indexed by 'frequency_nominal' (Hz, e.g. 38000.,
  120000.), so you can select with .sel(frequency_nominal=38000). Values are
  not changed. Needs unique nominal frequencies. The product kind is the
  input's (sv, mvbs, mask, ...).

INPUT (argument or stdin)
  One NetCDF path or gs:// URI with a 'channel' dimension and
  'frequency_nominal' (Sv, MVBS, ...).

OUTPUT (stdout)
  The output file's absolute path (or gs:// URI).

METADATA
  Reads the input's provenance, appends this step with its canonical
  scientific options, and embeds it all in the output (NetCDF attributes
  aa_provenance, aa_recipe, aa_product_hash, aa_base, aa_tool, history). Two
  hashes: the recipe (this step and every step before it, without the data:
  the <hash8> in the name, the same for any data processed this way) and the
  product hash (this recipe applied to this input: decides reuse). The base
  name is carried through unchanged. Inspect with: aa-metadata FILE

OPTIONS
  --check-unique		  fail early (exit 1) if frequency_nominal is missing
						  or has duplicates
  --no-overwrite		  exit 1 instead of replacing a different existing
						  output
  -o, --output_path PATH  Explicit output, used exactly as given. Local path
						  or gs:// URI.

SCIENTIFIC OPTIONS (change the product hash)
  None. Every option is I/O or display and leaves the hash alone.

FILES & URIs
  Reads NetCDF, local or gs://. Writes <base>_<hash>.nc beside the input
  (current directory for gs:// input), or -o, or --dest. An identical earlier
  result is reused. AA_NAMING=legacy: <stem>_freqswap.nc.

IN A PIPELINE
  Usually last before analysis in Python: ... | aa-sv | aa-swap-freq. Tools
  that select by 'channel' will not work on its output.

EXAMPLES
  aa-nc D20160703-T060000.raw --sonar_model EK60 | aa-sv | aa-swap-freq --check-unique
  aa-swap-freq MVBS.nc -o MVBS_by_freq.nc

COMMON OPTIONS
  --force				 recompute even if an identical product already
						  exists
  --base NAME			 name outputs after NAME instead of the input's base
						  name
  --dest DIR|gs://PREFIX  write the default-named output there instead of
						  beside the input
  --help-all			  the complete reference, every option
```

## aa_test

```bash
aa-test — Offline self-test of the aa-* chain on synthetic EK60 data.
  [utility]

WHAT IT DOES
  Writes a small synthetic EK60 .raw (2 channels, 60 pings, no network needed)
  in a scratch directory and runs the core chain through real pipes, the way
  you would in a shell. It checks each stage's exit status, output file and
  file name, then the provenance of all four products (aa-metadata --json and
  --verify), then runs the chain again and checks that every stage reused its
  output, and finally that a stage fed an empty pipe fails with exit 1 instead
  of printing help.

INPUT (argument or stdin)
  Nothing.

OUTPUT (stdout)
  One line per check: PASS, FAIL, or SKIP (not run because an earlier stage
  failed), with the product name and time; under a failure, the last lines of
  that stage's stderr. Then 'aa-test: PASS' or 'aa-test: FAIL'.

METADATA
  Produces no scientific product and records no provenance.

OPTIONS
  --keep	 keep the temporary directory (it is also kept when a check fails)
  --dir DIR  run in DIR instead (created if needed, never removed); products
			 already there are reused

FILES & URIs
  Writes aatest-D20160703-T060000.raw and its products (.nc, .png) in a new
  temporary directory, with the download cache (AA_CACHE_DIR) inside it. Runs
  with AA_NAMING, AA_REUSE and AA_GCS_* unset so your settings cannot change
  the result. Tests the aa-* commands installed next to the Python that runs
  aa-test.

IN A PIPELINE
  Not a pipeline stage. Run it on its own; exit status 0 means all passed.

EXAMPLES
  aa-test
  aa-test --keep
  aa-test --dir ./aa-selftest && aa-metadata ./aa-selftest/*.png

NOTE
  Takes 20-60 s, most of it the four tools starting up; the second, reusing
  run is quick.

MORE
  --help-all  the complete reference, every option
```

## aa_transient

```bash
aa-transient — Transient-noise mask for Sv; --apply also writes cleaned Sv.
  [scientific transform (hashed) · product: mask]

WHAT IT DOES
  Runs echopype.clean.mask_transient_noise (Ryan et al. 2015). Each sample is
  compared with the pooled Sv (--func, in linear units) of its neighbourhood:
  +/- --depth-bin vertically and +/- --num-side-pings pings. A sample more
  than --transient-threshold above that pool is transient noise. Samples
  shallower than --exclude-above (plus --depth-bin without index binning) are
  never flagged.

  The mask file holds one variable, transient_mask: boolean, True = transient
  noise, False = keep, dims (channel, ping_time, range_sample). echopype
  copies Sv's long_name/units attributes onto the mask; they do not describe
  it.

INPUT (argument or stdin)
  One Sv NetCDF (.nc/.netcdf4) path or gs:// URI that has the --range-var
  variable: depth (add it with aa-depth) or echo_range. An EchoData file
  without Sv is calibrated first with compute_Sv defaults (recorded in the
  provenance as an implicit step).

OUTPUT (stdout)
  The MASK's absolute path (or gs:// URI), one line. With --apply the cleaned
  Sv's path goes to stderr instead, as 'aa-transient: cleaned Sv: PATH' (also
  when it is reused).

METADATA
  Reads the input's provenance, appends this step with its canonical
  scientific options, computes the product hash, and embeds it all in the mask
  (NetCDF attributes aa_provenance, aa_product_hash, aa_base, aa_tool,
  history). The --apply file is its own product: same step, variant 'apply',
  kind sv, its own hash. Inspect with: aa-metadata FILE

OPTIONS
  -o, --output_path PATH  The mask file, used as given with the extension
						  forced to .nc. Local path or gs:// URI. Does not
						  move the --apply file.
  --apply				 Also write the input's Sv with transient samples set
						  to NaN (all other variables copied).
  --chunk KEY=VAL ...	 Dask chunk sizes for the index-binning pooling, e.g.
						  ping_time=256 range_sample=512 (dims of Sv). Only
						  used with --use-index-binning; performance only, not
						  hashed. Put INPUT_PATH before --chunk (or end the
						  list with --).

SCIENTIFIC OPTIONS (change the product hash)
  --func				 Pooling function: nanmean or nanmedian (the only two
						 echopype 0.11.1 accepts; nanmedian is much slower).
						 (default: nanmean)
  --depth-bin			Vertical half-height of the pooling window, e.g. 10m.
						 (default: 10m)
  --num-side-pings	   Pings on each side in the pooling window. (default:
						 25)
  --exclude-above		Never flag samples shallower than this, e.g. 250m.
						 (default: 250.0m)
  --transient-threshold  dB above the pooled Sv that counts as transient
						 noise, e.g. 12dB. (default: 12.0dB)
  --range-var			Vertical variable: depth or echo_range. (default:
						 depth)
  --use-index-binning	Pool by range_sample index (assumes uniform sample
						 spacing per channel). Much faster.
  Flag order, alias spellings and explicit defaults do not change the hash.

FILES & URIs
  Reads NetCDF, local or gs://. Writes the mask to <base>_<hash>.nc beside the
  input (current directory for gs:// input), or -o, or --dest. With --apply a
  second <base>_<hash>.nc (a different hash) goes beside the input or into
  --dest, never to -o. AA_NAMING=legacy: <stem>_transient_mask.nc and
  <stem>_transient_cleaned.nc beside the input. Identical earlier results are
  reused.

IN A PIPELINE
  After aa-sv and aa-depth: ... | aa-sv | aa-depth | aa-transient | aa-graph
  (draws the mask). Only the mask travels down the pipe.

EXAMPLES
  aa-nc x.raw --sonar_model EK60 | aa-sv | aa-depth | aa-transient --use-index-binning --exclude-above 20m
  aa-transient sv_depth.nc --apply --exclude-above 20m --use-index-binning

NOTE
  Without --use-index-binning echopype 0.11.1 pools sample by sample in
  Python: minutes even for a small file.

NOTE
  Keep --exclude-above (default 250m) inside the data's depth range, e.g.
  --exclude-above 20m. With --use-index-binning, echopype 0.11.1 takes the cut
  from the first ping of the first channel: if no depth at all is deeper it
  excludes nothing (shallow samples can be flagged); if that ping is too short
  but other data go deeper, it fails with "overlapping depth ... larger than
  your array". The tool warns before either happens.

COMMON OPTIONS
  --force				 recompute even if an identical product already
						  exists
  --base NAME			 name outputs after NAME instead of the input's base
						  name
  --dest DIR|gs://PREFIX  write the default-named output there instead of
						  beside the input
  --help-all			  the complete reference, every option
```

## aa_ts

```bash
aa-ts — Calibrate EchoData to target strength (TS).
  [scientific transform (hashed) · product: ts]

WHAT IT DOES
  Runs echopype.calibrate.compute_TS on a converted EchoData file and writes a
  flat TS dataset (TS, echo_range, ... on channel x ping_time x range_sample).
  Environmental and calibration values stored in the file are used unless you
  override them with --env-param / --cal-param.

INPUT (argument or stdin)
  One EchoData .nc/.netcdf4 path or gs:// URI, from aa-nc, aa-ed or
  aa-combine.

OUTPUT (stdout)
  The TS file's absolute path (or gs:// URI).

METADATA
  Reads the input's provenance, appends this step with its canonical
  scientific options, and embeds it all in the output (NetCDF attributes
  aa_provenance, aa_recipe, aa_product_hash, aa_base, aa_tool, history). Two
  hashes: the recipe (this step and every step before it, without the data:
  the <hash8> in the name, the same for any data processed this way) and the
  product hash (this recipe applied to this input: decides reuse). The base
  name is carried through unchanged. Inspect with: aa-metadata FILE

OPTIONS
  -o, --output_path PATH	   Explicit output; '_ts' is ALWAYS appended to
							   its stem and .nc forced (-o out.nc writes
							   out_ts.nc). Local path or gs:// URI.
  --env-param KEY=VALUE		override an environmental value; repeatable,
							   e.g. --env-param sound_speed=1500
  --cal-param KEY=VALUE		override a calibration value; repeatable, e.g.
							   --cal-param gain_correction=25.9
  --waveform_mode CW|BB|FM	 EK80 waveform (default: CW). EK60/AZFP: always
							   CW.
  --encode_mode complex|power  EK80 encoding (default: complex). EK60/AZFP:
							   always power.

SCIENTIFIC OPTIONS (change the product hash)
  --env-param	  Environmental overrides (--env-param), as numbers; order
				   and 1500 vs 1500.0 do not matter.
  --cal-param	  Calibration overrides (--cal-param), as numbers.
  --waveform_mode  EK80 waveform. FM and BB are the same computation.
				   (default: CW)
  --encode_mode	EK80 encoding. (default: complex)
  Flag order, alias spellings and explicit defaults do not change the hash.

FILES & URIs
  Reads EchoData .nc/.netcdf4, local or gs://. Writes <base>_<hash8>.nc beside
  the input (current directory for gs:// input), or in --dest DIR|gs://PREFIX,
  or at -o (+'_ts'). AA_NAMING=legacy restores the old default <input
  stem>_ts.nc. An identical earlier result is reused.

IN A PIPELINE
  Parallel to aa-sv, on the same EchoData: aa-nc x.raw --sonar_model EK60 |
  aa-ts. Its output holds TS, not Sv, so it does not feed aa-clean, aa-mvbs or
  aa-nasc.

EXAMPLES
  aa-nc D20160703-T060000.raw --sonar_model EK60 | aa-ts
  aa-ts file.nc --env-param sound_speed=1500 --env-param temperature=10.5

NOTE
  For EK60 and AZFP data echopype ignores --waveform_mode and --encode_mode
  (it always uses CW power samples), but they are still recorded.

COMMON OPTIONS
  --force				 recompute even if an identical product already
						  exists
  --base NAME			 name outputs after NAME instead of the input's base
						  name
  --dest DIR|gs://PREFIX  write the default-named output there instead of
						  beside the input
  --help-all			  the complete reference, every option
```

## aa_upload

```bash
aa-upload — Upload files and products to a GCS bucket.
  [sink (stores products)]

WHAT IT DOES
  With a gs:// destination (aa-upload [FILE ...] gs://bucket/prefix/): uploads
  each input there, with its .aa.json sidecar, stamps the product hash into
  the object's metadata, and skips objects that already hold the same product
  or the same bytes. Folders (.zarr stores) are uploaded recursively.

  Without one, the original modes: echosounder mode keeps aalibrary's
  data/raw/<ship>/<survey>/<sonar>/ layout (needs --ship_name, --survey_name,
  --sonar_model); --as-is uploads under --destination_prefix in the configured
  bucket.

INPUT (argument or stdin)
  Paths (or gs:// URIs to copy between buckets), one per line, when no input
  argument is given. aa/1 JSON handles work too.

OUTPUT (stdout)
  gs:// destination: the gs:// URI of each uploaded (or already present)
  object; with --tee the local path instead. Original modes: the input path,
  unchanged.

METADATA
  Never changes the file. Uploads it with its .aa.json sidecar and stamps
  aa-product-hash / aa-base / aa-tool into the object's custom metadata, so
  the bucket can answer 'is this product already here?'.

OPTIONS
  gs://BUCKET/PREFIX/		   destination; ending in / (or several inputs,
								or a folder) means 'put it under this prefix';
								a name with an extension is the exact object
  --tee						 gs:// mode: print the local path, so the pipe
								continues locally
  --force					   gs:// mode: upload even if the object is
								identical
  --dry-run					 show what would be uploaded; upload nothing
  --ship_name/--survey_name/--sonar_model
								echosounder mode (all three)
  --as-is --destination_prefix PFX
								as-is mode
  --gcp_env prod|dev, --project_id, --gcp_bucket_name
								which project/bucket the original modes use

FILES & URIs
  Reads local files and folders (or gs:// objects). Writes gs:// objects with
  your Application Default Credentials (billing project: --project_id, else
  the project aalibrary is configured for). A .zarr store uploaded over an
  older one replaces it completely (objects it no longer has are removed);
  other folders only add and update files.

IN A PIPELINE
  The last stage (gs:// mode prints the URIs, which aa-metadata, aa-graph or
  aa-download accept), or a tee between stages with --tee or in the original
  modes.

EXAMPLES
  aa-nc x.raw --sonar_model EK60 | aa-sv | aa-clean | aa-upload gs://bucket/derived/me/
  aa-upload ./HB1603/EK60 --ship_name Henry_B._Bigelow --survey_name HB1603 --sonar_model EK60

MORE
  --help-all  the complete reference, every option
```

