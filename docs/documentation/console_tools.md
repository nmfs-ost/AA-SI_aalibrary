# Console Tools Documentation

## aa_absorption

```bash
Usage: aa-absorption [OPTIONS]

	Options:
	  --frequency FLOAT_OR_LIST  Frequency in Hz (e.g., 38000) or comma-separated list (e.g., 38000,120000). Required.
	  --temperature FLOAT		 Temperature in °C. Default: 27
	  --salinity FLOAT			Salinity in PSU. Default: 35
	  --pressure FLOAT			Pressure in dbar. Default: 10
	  --pH FLOAT				  pH of seawater. Default: 8.1
	  --formula-source STR		Formula source: 'AM', 'FG', or 'AZFP'. Default: AM
	  -o, --output_path PATH	  Optional NetCDF output path (default: none).
	  --quiet					 Print only numeric values (or array).
	  -h, --help				  Show this help message and exit.

	Description:
	  Computes seawater absorption in dB/m for given frequency(ies) and parameters.
```

## aa_abundance

```bash
Usage: aa-abundance [OPTIONS] [INPUT_PATH]

	Arguments:
	  INPUT_PATH				   Path to a NetCDF file (.nc) containing a calibrated
								   Dataset with 'echo_range'. Optional; defaults to
								   reading one token from stdin.

	Options:
	  -o, --output_path PATH	   Output NetCDF path (default: <stem>_abundance.nc).
	  --range-label STR			Name of the DataArray holding range (default: echo_range).
	  --try-calibrate			  If 'echo_range' is missing, try to open as converted
								   EchoData and compute Sv to obtain it.
	  --no-overwrite			   Do not overwrite an existing output file.
	  --quiet					  Print only the output path (or suppress extras).
	  -h, --help				   Show this help message and exit.

	Description:
	  Computes the Echopype abundance metric along the range axis and writes it to NetCDF.
```

## aa_aggregation

```bash
Usage: aa-aggregation [OPTIONS] [INPUT_PATH]

	Arguments:
	  INPUT_PATH				   Path to a NetCDF file (.nc) containing a calibrated
								   Dataset with 'echo_range'. Optional; defaults to
								   reading one token from stdin.

	Options:
	  -o, --output_path PATH	   Output NetCDF path (default: <stem>_aggregation.nc).
	  --range-label STR			Name of the DataArray holding range (default: echo_range).
	  --no-overwrite			   Do not overwrite an existing output file.
	  --quiet					  Print only the output path (or suppress extras).
	  -h, --help				   Show this help message and exit.

	Description:
	  Computes the Echopype aggregation metric of backscatter along the range axis.
```

## aa_attenuated

```bash
Usage: aa-attenuated [OPTIONS] [INPUT_PATH]

	Arguments:
	  INPUT_PATH				   Path to the calibrated .nc (NetCDF) file
								   containing Sv (preferred), or a converted
								   Echopype file that can be calibrated to Sv.
								   Optional. Defaults to stdin if not provided.

	Options:
	  -o, --output_path PATH	   Where to write the attenuated-signal mask (NetCDF).
								   Default: <stem>_attenuated_mask.nc
	  --apply					  Also apply the mask to Sv and write a cleaned
								   Sv file (suffix: _attenuated_cleaned.nc).

	  # mask_attenuated_signal parameters
	  --upper-limit-sl STR		 Upper limit of deep scattering layer line, e.g. '400.0m'.
								   Default: 400.0m
	  --lower-limit-sl STR		 Lower limit of deep scattering layer line, e.g. '500.0m'.
								   Default: 500.0m
	  --num-side-pings INT		 Pings on each side defining the comparison block.
								   Default: 15
	  --attenuation-threshold STR  Threshold above local context, e.g. '8.0dB'.
								   Default: 8.0dB
	  --range-var STR			  Name of the range/depth coordinate (e.g., 'depth').
								   Default: depth

	  -h, --help				   Show this help message and exit.

	Description:
	  Creates a boolean mask marking likely attenuated-signal pings based on
	  comparisons across neighboring ping blocks between two depth limits.
	  Optionally applies the mask to Sv to produce a cleaned Sv dataset.

	Examples:
	  aa-attenuated data.nc --upper-limit-sl 350m --lower-limit-sl 480m --num-side-pings 17
	  aa-attenuated data.nc --apply -o out_mask.nc
```

## aa_center_of_mass

```bash
Usage: aa-center-of-mass [OPTIONS] [INPUT_PATH]

	Arguments:
	  INPUT_PATH				   Path to a NetCDF file (.nc) containing a calibrated
								   Dataset with 'echo_range'. Optional; defaults to
								   reading one token from stdin.

	Options:
	  -o, --output_path PATH	   Output NetCDF path (default: <stem>_com.nc).
	  --range-label STR			Name of the DataArray holding range (default: echo_range).
	  --try-calibrate			  If 'echo_range' is missing, try to open as converted
								   EchoData and compute Sv to obtain it.
	  --no-overwrite			   Do not overwrite an existing output file.
	  --quiet					  Print only the output path (or suppress extras).
	  -h, --help				   Show this help message and exit.

	Description:
	  Computes the center of mass (depth-weighted mean) of backscatter along range.
	  Units: meters (same units as the provided range axis).
```

## aa_clean

```bash
Usage: aa-clean [OPTIONS] [INPUT_PATH]

	Arguments:
	INPUT_PATH				  Path to a Sv .nc / .netcdf4 file
								(typically the output of aa-sv).
								Optional. Defaults to stdin if not provided.

	Options:
	-o, --output_path		   Path to save processed output.
								Default: same directory as input, with
								'_clean' appended to the stem and a
								.nc suffix. Note: '_clean' is ALWAYS
								appended, even when -o is given, so the
								input file is never silently overwritten.

	--ping_num				  Number of pings to use for background
								noise estimation.
								Default: 20

	--range_sample_num		  Number of range samples to use for background
								noise estimation.
								Default: 20

	--background_noise_max	  Optional upper bound on the estimated
								background noise, e.g. "-125dB". Pass with
								the dB unit suffix.
								Default: None (no cap).

	--snr_threshold			 SNR threshold as a number in dB. The 'dB'
								unit suffix is appended automatically before
								handing off to echopype.
								Default: 3.0

	Description:
	Removes background noise from a Sv NetCDF using
	echopype.clean.remove_background_noise. The expected input is the
	output of aa-sv (a flat NetCDF Sv dataset, NOT a multi-group EchoData
	file from aa-nc).

	Pipeline example:
		aa-nc --sonar_model EK60 input.raw | aa-sv | aa-clean

	Direct example:
		aa-clean /path/to/input_Sv.nc \
				 --ping_num 50 --range_sample_num 200 \
				 --snr_threshold 5.0 -o /path/to/output.nc
```

## aa_coerce_time

```bash
Usage: aa-coerce-time [OPTIONS] [INPUT_PATH]

	Arguments:
	  INPUT_PATH				   Path to a NetCDF file (.nc) whose time coordinate
								   may contain local reversals. Optional; defaults to
								   reading a single token from stdin.

	Options:
	  -o, --output_path PATH	   Output NetCDF path (default: <stem>_timefix.nc).
	  --time-name STR			  Name of the time coordinate to coerce (default: ping_time).
	  --win-len INT				Local window length used to infer the next ping time
								   when a reversal is detected (default: 100).
	  --report					 Print a short report on time reversals before/after.
	  --no-overwrite			   Do not overwrite an existing output file.
	  -h, --help				   Show this help message and exit.

	Description:
	  Detects and fixes local backward jumps in a datetime coordinate by enforcing
	  a monotonically increasing series (forward-only time).

	Example:
	  aa-coerce-time pingdata.nc --time-name ping_time --win-len 120 --report -o pingdata_timefix.nc
```

## aa_combine

```bash
Usage: aa-combine [OPTIONS] [INPUTS...]

	Arguments:
	  INPUTS					Converted EchoData files (.nc / .zarr), or a
								directory containing them. Optional. With no
								inputs, aa-combine reads stdin; with neither,
								it globs --workdir.

	Input:
	  --workdir DIR			 Where to look when no inputs are given.
								Default: the current directory.
	  --recursive			   Search --workdir recursively.
	  --sort {time,given,name}  Order the inputs before combining.
								time  — by first ping_time (default). This is
										what echopype requires; unsorted input
										is its most common hard failure.
								given — the order they arrived in.
								name  — lexical, which for D…-T… names is
										chronological.
	  --channels LIST		   Comma-separated channel names to keep, passed
								to echopype as channel_selection. Leave unset
								to keep every channel. Required when the
								inputs do not all carry the same channels;
								echopype refuses that combine outright.
	  --sonar_model MODEL	   Assert the expected model (EK60, EK80, ...).
								Fails before loading anything if an input
								disagrees.

	Output:
	  -o, --output_path PATH	Output store or file. A .zarr suffix writes a
								store, .nc writes a single NetCDF export. May
								be a gs:// or s3:// URI for .zarr, which
								writes there directly rather than writing
								locally and copying a directory of thousands
								of objects afterwards. Default: combined.zarr
								in --workdir.
	  --overwrite			   Replace an existing output.
	  --chunk-pings N		   Chunk length along ping_time. Unset lets
								echopype target ~100 MB chunks, which is a
								good default and the wrong one once you know
								your query shape. Aim for 1-20 MB compressed;
								5-10 MB is the sweet spot on object storage.
	  --compression WHICH	   default | none | zlib | blosc-lz4 | blosc-zstd
								Default lets echopype pick per dtype (zstd for
								floats, lz4 for ints). zlib applies to NetCDF
								output only.
	  --consolidated			Write consolidated metadata (default on).
								Costs one small object; saves one request per
								array on every open, forever.
	  --no-consolidated		 Skip it.

	QC:
	  --check				   Run the QC pass and stop. Writes no store.
								Exit 4 if anything would have blocked or
								warned. This is the safe thing to run first.
	  --plan					Estimate the combine — files, pings, channels,
								bytes — and stop. Emits aa/plan/1 JSON.
	  --strict				  Treat seams, overlaps and duplicate ping times
								as blocking rather than advisory. Use this in
								a recipe, where nobody reads the warnings.
	  --gap_seconds N		   Minimum dead time before a gap counts as a
								seam. Default: 900 (15 minutes).
	  --gap_factor N			...and how many times the median file cadence
								it must also exceed. Default: 6.
	  --report [PATH]		   Write the QC report. Bare --report, or the
								flag omitted entirely, writes it beside the
								output. --report PATH chooses the path.
								--no-report skips it. The report URI is named
								in the handle, which is the only way the UI
								can surface it.
	  --no-report			   Skip the QC report.

	Machine interfaces:
	  --json					Emit an aa/1 handle line on stdout instead of
								the bare path.
	  --progress				Emit NDJSON progress events on stderr for a
								job runner to parse.
	  --describe				Emit this tool's own parameter schema as JSON
								and exit, so the catalogue can be generated
								rather than hand-maintained.

	  -q, --quiet			   Warnings and errors only.
	  --debug				   Verbose logging.
	  -h, --help				This message.

	Exit codes:
	  0 ok		1 runtime error	2 usage
	  3 partial (interrupted; store marked resumable)
	  4 QC failed (--check, or --strict with findings)

	Uploading:
	  There is no --upload. `aa-combine ... | aa-upload --as-is` composes, and
	  -o gs://... writes to the bucket directly, which is better than either:
	  no second pass over a store that may be hundreds of gigabytes.

	Examples:
	  aa-combine ./converted/ --check
	  aa-combine *.nc -o HB1603_L1.zarr --chunk-pings 500 --compression blosc-zstd
	  aa-combine --workdir ./converted -o gs://bucket/HB1603_L1.zarr
	  aa-ed ./raw/ | aa-combine -o out.zarr --json | aa-store verify --json
```

## aa_depth

```bash
Usage: aa-depth [OPTIONS] [INPUT_PATH]

	Arguments:
	INPUT_PATH				 Path to the .nc / .netcdf4 file containing Sv.
							   Optional. Defaults to stdin if not provided.

	Options:
	-o, --output_path		  Path to save processed output. If provided, it is
							   used as-is (a .nc suffix is added only when missing).
							   If omitted, defaults to the input path with '_depth'
							   appended to the stem and a .nc suffix.

	--depth-offset			 Offset (meters) along depth to account for transducer
							   position in water. Default: None (transducer at the
							   surface). If set, Platform vertical offsets are ignored.

	--tilt					 Transducer tilt angle in degrees (0 = vertical).
							   Default: None. If set, Platform/Beam angles are ignored.

	--downward / --no-downward Whether transducers point downward.
							   Default: --downward (True).

	--echodata				 Path to the converted EchoData file (.nc/.netcdf4/.zarr)
							   that the Sv dataset originated from. Required when using
							   any of the --use-* options below.

	--use-platform-vertical-offsets
							   Use the EchoData Platform group vertical offsets to
							   compute transducer depth (EK60/EK80 only). Ignored if
							   --depth-offset is given.

	--use-platform-angles	  Use the EchoData Platform group angles to scale
							   echo_range (EK60/EK80 only). Ignored if --tilt is given.
							   Cannot be combined with --use-beam-angles.

	--use-beam-angles		  Use the EchoData Beam group angles to scale echo_range
							   (EK60/EK80 only). Ignored if --tilt is given.
							   Cannot be combined with --use-platform-angles.

	Description:
	Loads a NetCDF Sv dataset, adds a depth coordinate via
	echopype.consolidate.add_depth, and writes the result to a new
	.nc file. The output path is printed to stdout for piping.

	Example:
	aa-depth /path/to/input_Sv.nc --depth-offset 1.5 --tilt 5
	aa-depth /path/to/input_Sv.nc --echodata /path/to/converted.zarr \
			 --use-platform-vertical-offsets --use-beam-angles
```

## aa_detect_seafloor

```bash
Usage: aa-detect-seafloor [OPTIONS] [INPUT_PATH]

	Arguments:
	INPUT_PATH				  Path to the calibrated Sv .nc / .netcdf4
								file, or a converted Echopype file that
								can be calibrated to Sv.
								Optional. Defaults to stdin if not provided.

	Options:
	-o, --output_path		   Path to save the bottom-line dataset.
								Default: same directory as input, with
								'_seafloor' appended to the stem and a
								.nc suffix.

	--method					Seafloor detection method (dispatcher key),
								e.g. 'basic', 'blackwell'. (REQUIRED)
	--param KEY=VAL [...]	   Parameters for the chosen method as
								key=value pairs. Values are safely parsed
								(int / float / bool / None) when possible;
								strings like '10m' remain strings.

	--emit-mask				 Also compute and save a 2D boolean mask of
								samples below the bottom line, suffix
								'_seafloor_mask' (True = below bottom).
	--range-label			   Range/depth variable name used to build the
								mask. Default: echo_range
	--apply					 Apply the bottom mask to Sv and write a
								cleaned Sv file, suffix '_seafloor_cleaned'.
								Implies mask construction.

	--no-overwrite			  Do not overwrite existing output files.

	Description:
	Dispatches to detect_seafloor(ds, method, params) and returns a 1-D
	bottom line (per ping). With --emit-mask, builds a 2D mask by
	comparing range to the bottom line (True below bottom). With
	--apply, applies the mask to Sv via echopype.mask.apply_mask. The
	bottom-line path is printed to stdout for piping into the next stage
	of the pipeline.

	Examples:
		aa-detect-seafloor input_Sv.nc --method blackwell --emit-mask
		aa-sv input.nc | aa-detect-seafloor --method basic \
			  --param threshold=-40.0 --apply
```

## aa_detect_shoal

```bash
Usage: aa-detect-shoal [OPTIONS] [INPUT_PATH]

	Arguments:
	  INPUT_PATH				 Path to a calibrated Sv NetCDF (.nc), or a converted
								 Echopype file that can be calibrated. Optional; defaults
								 to stdin if not provided.

	Options:
	  -o, --output_path PATH	 Where to write the shoal mask (NetCDF).
								 Default: <stem>_detect_shoal_mask.nc
	  --apply					Also apply the mask to Sv and write cleaned Sv to
								 <stem>_detect_shoal_cleaned.nc

	  # detect_shoal parameters
	  --method STR			   Shoal detection method (dispatcher key), e.g. 'echoview' or 'weill'. (required)
	  --param KEY=VAL [...]	  Parameters for the chosen method as key=value pairs.
								 Values are safely parsed (int/float/bool/None) when possible;
								 strings like '10m' or '12.0dB' remain strings.

	  --no-overwrite			 Do not overwrite an existing output file.
	  --quiet					Suppress logs; print only the final output path.
	  -h, --help				 Show this help message and exit.

	Description:
	  Dispatches shoal detection to the chosen method via Echopype’s
	  `detect_shoal(ds, method, params)` and returns a 2D boolean mask
	  (True = inside shoal). Optionally applies the mask to Sv and writes a
	  cleaned Sv file.
```

## aa_detect_transient

```bash
Usage: aa-detect-transient [OPTIONS] [INPUT_PATH]

	Arguments:
	  INPUT_PATH				 Path to a calibrated Sv NetCDF (.nc), or a
								 converted Echopype file that can be calibrated.
								 Optional. Defaults to stdin if not provided.

	Options:
	  -o, --output_path PATH	 Where to write the transient-noise mask (NetCDF).
								 Default: <stem>_detect_transient_mask.nc
	  --apply					Also apply the mask to Sv and write a cleaned
								 Sv file (suffix: _detect_transient_cleaned.nc).

	  # detect_transient parameters
	  --method STR			   Transient detection method name (dispatcher key).
								 (e.g., 'pooling', 'percentile', etc.—see docs)
	  --param KEY=VAL [...]	  Parameters for the chosen method as key=value pairs.
								 Values are safely parsed (int/float/bool/str).

	  --range-var STR			Name of the range/depth coordinate (if your method
								 expects it in params, you can also pass via --param).
								 Default: depth

	  -h, --help				 Show this help message and exit.

	Description:
	  Dispatches transient-noise detection to a selected method via Echopype’s
	  `detect_transient(ds, method, params)` and returns a boolean mask. Optionally
	  applies the mask to Sv to produce a cleaned Sv dataset.

	Examples:
	  aa-detect-transient data.nc --method pooling --param depth_bin=10m num_side_pings=25 transient_noise_threshold=12.0dB
	  aa-detect-transient data.nc --apply -o out_mask.nc --method percentile --param percentile=99.5 window=21
```

## aa_dispersion

```bash
Usage: aa-dispersion [OPTIONS] [INPUT_PATH]

	Arguments:
	  INPUT_PATH				  Path to a NetCDF file (.nc) containing a calibrated
								  Dataset with an `echo_range` (or similar) coordinate.
								  Optional; defaults to stdin if not provided.

	Options:
	  -o, --output_path		  Path to write the resulting dispersion (NetCDF).
								  Default: <stem>_dispersion.nc
	  --range-label STR		  Name of the range variable/coordinate (default: echo_range).
	  --no-overwrite			 Do not overwrite an existing output file.
	  --quiet					Print only the output path (suppress logs).

	Description:
	  Computes the inertia of the backscatter distribution (i.e., dispersion/spread)
	  using Echopype’s metrics.dispersion. The returned quantity has units m⁻².
```

## aa_ed

```bash
Usage: aa-ed [OPTIONS] [FILE_NAME]

	Arguments:
	  FILE_NAME				   The raw file (or directory) to process.
								  THREE shapes accepted, auto-detected:

								  - Bare filename (e.g.
									HB1603_L1-D20160703-T183957.raw)
									-> aa-ed queries the NCEI BigQuery
									cache for metadata, downloads the
									file, and writes the .nc into
									--file_download_directory.

								  - Path to an existing .raw file (e.g.
									/home/me/data/HB1603...raw or
									./data/HB1603...raw) -> aa-ed uses
									it as-is, detects the sonar model
									from the file header (no BigQuery,
									no NCEI download, no GCP creds
									needed), and writes the .nc
									ALONGSIDE the .raw.

								  - Path to an existing directory (e.g.
									/home/me/data/ or ./data/) ->
									DIRECTORY BATCH MODE. aa-ed globs
									*.raw inside (or **/*.raw with -r),
									runs the same offline conversion
									on each file, and prints the
									DIRECTORY path on stdout (not
									individual .nc paths). Standalone
									.nc files pass through silently.
									Per-file failures are logged but
									don't abort the batch; exit code
									is non-zero if any failed.

								  Optional; falls back to stdin if not
								  provided.

	Optional:
	  -o, --output_path PATH	  Path to save the converted NetCDF output.
								  Default: same directory as the downloaded
								  .raw, with a .nc suffix.

	  --file_download_directory PATH
								  Where to download the .raw to.
								  Default: current directory. Created if it
								  doesn't exist.

	  --ship_name NAME			Override the ship_name lookup
								  (e.g. Henry_B._Bigelow).
	  --survey_name NAME		  Override the survey_name lookup
								  (e.g. HB1603).
	  --sonar_model NAME		  Override the echosounder lookup
								  (e.g. EK60, EK80).

								  If all three overrides are provided, aa-ed
								  skips the NCEI cache lookup entirely. Use
								  this when BigQuery is unreachable or to
								  disambiguate a file name that collides
								  across multiple surveys.

	  --cleanup-raw			   Delete the downloaded .raw after the .nc
								  is produced. Off by default — the .raw is
								  source data and is kept so re-running
								  aa-ed (or aa-nc directly) is free.

	  --force, -f				 Re-download and re-convert even when the
								  .raw / .nc are already on disk. Default
								  behavior is to treat both as cached: an
								  existing .nc short-circuits everything
								  (including the BigQuery lookup), and an
								  existing .raw skips the NCEI download.
								  Use this if you suspect a cached file is
								  stale or corrupt.

	  --upload_to_gcp			 Also upload the downloaded .raw to GCP
								  (passed through to aalibrary.ingestion).

	  --data_source SRC		   Currently only 'NCEI' is wired through;
								  other values log a warning and proceed
								  as NCEI.

	Cloud output & URI caching (opt-in; all off by default):
	  --gcs-uri URI			   Use gs://bucket/path/file.nc as a cache for
								  the derived .nc. If the object already
								  exists it is reused instead of recomputed
								  (downloaded, or passed through with
								  --print-uri); on a miss the new .nc is
								  uploaded here after conversion, using the
								  same GCP primitive as aa-upload.

	  --gcs-prefix PREFIX		 Like --gcs-uri, but aa-ed names the object
								  <prefix>/<stem>.nc; the bucket comes from
								  --gcp_bucket_name / --gcp_env / env.
								  Mutually exclusive with --gcs-uri.

	  --print-uri				 With a GCS destination set, print the gs://
								  URI on stdout instead of the local path. On
								  a cache hit no download happens — the URI
								  is passed straight through.

	  --cloud-only				Delete the local .nc after a successful
								  upload (keeps local storage minimal).
								  Implies --print-uri.

	  --gcp_env {prod,dev}		Select the aalibrary GCP env for cloud
								  output (mirrors aa-upload).
	  --project_id ID			 Explicit GCP project id (overrides --gcp_env).
	  --gcp_bucket_name NAME	  Explicit GCP bucket (overrides --gcp_env;
								  ignored if a bucket is given in --gcs-uri).

	  --debug					 Verbose logging (DEBUG level on stderr).
	  --quiet					 Suppress INFO logs; final path still
								  prints on stdout.

	  -h, --help				  Show this help and exit.

	Description:
	  Resolves a raw file's ship/survey/echosounder by querying the NCEI
	  BigQuery cache, downloads the .raw from NCEI, and converts it to a
	  multi-group NetCDF EchoData file with echopype.open_raw /
	  EchoData.to_netcdf. The .nc absolute path is printed on stdout,
	  ready for piping into aa-sv and onward.

	  Equivalent (in output) to:

		  aa-raw --file_name FILE --ship_name S --survey_name SU \
				 --sonar_model M --file_download_directory DIR \
			| aa-nc --sonar_model M

	  ...but the user only has to supply FILE_NAME.

	Pipeline example:
	  echo HB1603_L1-D20160703-T183957.raw | aa-ed | aa-sv | aa-graph

	Direct example:
	  aa-ed HB1603_L1-D20160703-T183957.raw \
			--file_download_directory ./downloads -o ./out/HB1603.nc

	Cloud example (reuse-if-exists, else convert-and-upload):
	  aa-ed HB1603_L1-D20160703-T183957.raw \
			--gcs-prefix derived/nc/ --gcp_bucket_name my-bucket --print-uri
	  # hit  -> prints gs://my-bucket/derived/nc/HB1603...nc (no work done)
	  # miss -> converts locally, uploads, prints the same gs:// URI
```

## aa_evenness

```bash
Usage: aa-evenness [OPTIONS] [INPUT_PATH]

	Arguments:
	  INPUT_PATH				   Path to a NetCDF file (.nc) containing a calibrated
								   Dataset with 'echo_range'. Optional; defaults to
								   reading one token from stdin.

	Options:
	  -o, --output_path PATH	   Output NetCDF path (default: <stem>_evenness.nc).
	  --range-label STR			Name of the DataArray holding range (default: echo_range).
	  --try-calibrate			  If 'echo_range' is missing, attempt to open as converted
								   EchoData and compute Sv to obtain it.
	  --no-overwrite			   Do not overwrite an existing output file.
	  --quiet					  Print only the output path (or suppress extras).
	  -h, --help				   Show this help message and exit.

	Description:
	  Computes the Equivalent Area (EA) metric from Echopype (units: meters).
```

## aa_freqdiff

```bash
Usage: aa-freqdiff [OPTIONS] [INPUT_PATH]

	Arguments:
	  INPUT_PATH				Path to a NetCDF/Zarr file (or dataset) containing
							   Sv with a `channel` dimension and `frequency_nominal`, or
							   a conversion output. Optional — defaults to stdin if not provided.

	Options:
	  -o, --output_path PATH	Where to write the mask NetCDF (default: <stem>_freqdiff.nc).
	  --freqABEq STR			Frequency differencing expression, e.g. '"38.0kHz" - "120.0kHz">=10.0dB'.
	  --chanABEq STR			Channel-based differencing expression, e.g. '"chan1" - "chan2"<-5dB'.
	  --quiet				   Suppress logger info, only print output path.
	  -h, --help				Show this help message and exit.

	Description:
	  Computes a boolean mask of Sv data where one frequency minus another
	  meets a user-specified threshold/difference. Useful for identifying
	  scatterers with different frequency responses (for example krill).
	
	Examples:
	  aa-freqdiff data.nc --freqABEq '"38.0kHz" - "120.0kHz">=12.0dB' -o out_mask.nc
	  aa-freqdiff data.nc --chanABEq '"chan1" - "chan2"<-5dB'
```

## aa_graph

```bash
Usage: aa-graph [OPTIONS] [INPUT_PATH]

Arguments:
  INPUT_PATH				Path to a NetCDF file (.nc). Optional; if
							omitted, reads a single path token from stdin.

Variable & channel selection:
  --var VAR				 Variable to plot (default: Sv if present, else
							the first data_var).
  --channel N			   Plot only channel index N.
  --frequency F			 Plot only the channel nearest to F Hz.
  --single				  Shortcut for --channel 0.

Appearance:
  --vmin FLOAT			  Lower color limit. Default: per-variable
							(-80 dB for Sv/Sv_clean/MVBS, -90 dB for TS,
							autoscaled for NASC). For categorical data
							(cluster maps, masks) vmin/vmax are ignored
							and a discrete legend is used instead.
  --vmax FLOAT			  Upper color limit. Default: per-variable
							(-30 dB for Sv/Sv_clean/MVBS, -20 dB for TS,
							autoscaled for NASC). Ignored for categorical.
  --cmap NAME			   Matplotlib colormap (default: viridis). For
							cluster maps with many clusters, try 'hsv',
							'tab20', 'gist_rainbow' for more contrast.
  --figwidth FLOAT		  Figure width in inches (default: 10).
  --rowheight FLOAT		 Per-channel row height in inches (default: 3).
  --no-flip				 Don't auto-invert the y-axis for depth/range.
  --no-pie				  Suppress the pie-chart distribution row drawn
							below the echogram. By default a pie is shown
							per channel, sharing colors with the map: for
							cluster data each non-noise cluster is a wedge,
							for continuous data (Sv etc.) wedges represent
							value bins between vmin and vmax.
  --pie-height FLOAT		Height of the pie row in inches (default: 2.6).

Subsetting / performance:
  --decimate N			  Take every Nth sample along x-axis (default: 1).
  --ymin FLOAT			  Crop lower y-limit (in metres if axis is depth).
  --ymax FLOAT			  Crop upper y-limit (in metres if axis is depth).

Output:
  -o, --output_path PATH	Output PNG path (default: <stem>_graph.png).
  --dpi INT				 Output DPI (default: 100).
  --quiet				   Suppress INFO logs; final path still prints.
  -h, --help				Show this help and exit.

By default, multi-channel datasets are plotted with one subplot per channel,
vertically stacked, sharing the x-axis. Subplot titles are short and
descriptive: "38 kHz", "200 kHz" (frequency_nominal), or "ch 0", "ch 1"
when no frequency coord is available.
```

## aa_impulse

```bash
Usage: aa-impulse [OPTIONS] [INPUT_PATH]

	Arguments:
	INPUT_PATH				  Path to the calibrated .nc / .netcdf4 file
								containing Sv (preferred), or a converted
								Echopype file that can be calibrated to Sv.
								Optional. Defaults to stdin if not provided.

	Options:
	-o, --output_path		   Path to save the impulse-noise mask (NetCDF).
								Default: same directory as input, with
								'_impulse_mask' appended to the stem and a
								.nc suffix.

	--apply					 Also apply the mask to Sv and write a cleaned
								Sv file alongside the mask, suffix
								'_impulse_cleaned'.

	--depth-bin				 Vertical bin size for comparison, e.g. '5m'.
								Default: 5m
	--num-side-pings			Pings on each side for two-sided comparison.
								Default: 2
	--impulse-threshold		 Threshold in dB above local context, e.g.
								'10.0dB'. Default: 10.0dB
	--range-var				 Name of the range/depth coordinate.
								Default: depth
	--use-index-binning		 Use index-based binning instead of physical
								units.

	Description:
	Creates a boolean mask marking likely impulse-noise "flecks" using a
	ping-wise two-sided comparison in depth-binned windows. Optionally
	applies the mask to Sv to produce a cleaned Sv dataset. The mask path
	is printed to stdout for piping into the next stage of the pipeline.

	Example:
		aa-sv input.nc | aa-impulse --apply --depth-bin 5m \
			  --impulse-threshold 12dB
```

## aa_location

```bash
Usage: aa-location [OPTIONS] [INPUT_PATH]

	Arguments:
	  INPUT_PATH				   Path to an Sv NetCDF (.nc), or another Dataset
								   that has ping_time and can accept location.
								   Optional. Defaults to stdin if not provided.

	Options:
	  -o, --output_path PATH	   Where to write the output NetCDF with lat/lon.
								   Default: <stem>_loc.nc
	  --echodata PATH			  Path to an EchoData source (raw/converted file or
								   Zarr/NetCDF) that contains Platform/NMEA groups
								   for interpolation. (Required if INPUT lacks these.)
	  --datagram-type STR		  (Optional) Instrument/datagram type hint used by
								   add_location to select nav source.
	  --nmea-sentence STR		  (Optional) Specific NMEA sentence to use (e.g. 'GGA').

	  -h, --help				   Show this help message and exit.

	Description:
	  Interpolates geographic location (latitude, longitude) from the platform
	  navigation stream in the original file to the acoustic ping_time of the
	  Sv dataset, and writes the result to NetCDF.

	Examples:
	  aa-location sv.nc --echodata rawfile.raw
	  aa-location sv.nc --echodata cruise.zarr --nmea-sentence GGA -o sv_loc.nc
```

## aa_min

```bash
Usage: aa-min [OPTIONS] [INPUT_PATH]

	Arguments:
	INPUT_PATH				  Path to the .netcdf4 file.
								Optional. Defaults to stdin if not provided.

	Options:
	-o, --output_path		   Path to save processed output (NetCDF).
								Default: input file with "_mask" appended to stem.

	--depth_bin				 Downsampling vertical bin size (default: 5m)
	--num_side_pings			Number of side pings for two-sided comparison (default: 2)
	--impulse_noise_threshold   Threshold (dB) for impulse detection (default: "10.0dB")
	--range_var				 Range coordinate: "depth" or "echo_range" (default: depth)
	--use_index_binning		 Use index-based binning for speed (default: False)

	Example:
	aa-min /path/to/input.nc --depth_bin 5m --num_side_pings 3		 --impulse_noise_threshold "12.0dB" -o /path/to/output_mask.nc
```

## aa_mvbs

```bash
Usage: aa-mvbs [OPTIONS] [INPUT_PATH]

	Arguments:
	INPUT_PATH				  Path to a Sv .nc / .netcdf4 file
								(typically the output of aa-sv or aa-clean).
								Optional. Defaults to stdin if not provided.

	Options:
	-o, --output_path		   Path to save processed output.
								Default: same directory as input, with
								'_mvbs' appended to the stem and a .nc
								suffix. '_mvbs' is ALWAYS appended, so the
								input file is never silently overwritten.

	--range_var				 Range coordinate to bin over.
								Choices: echo_range, depth
								Default: echo_range

	--range_bin				 Bin size along the range dimension.
								Default: 20m

	--ping_time_bin			 Bin size along the ping_time dimension.
								Default: 20s

	--method					Computation method for binning.
								Choices: map-reduce, coarsen, block
								Default: map-reduce

	--reindex				   Reindex the result to match uniform bin edges.
								Default: False (omit the flag).

	--skipna					Skip NaN values when averaging (default).
	--no_skipna				 Include NaN values in mean calculations.

	--fill_value				Fill value for empty bins.
								Default: NaN

	--closed					Which side of the bin interval is closed.
								Choices: left, right
								Default: left

	--range_var_max			 Optional maximum value for range_var.
								Default: None

	--flox_kwargs			   Extra flox kwargs as KEY=VALUE pairs.
								Values are parsed safely via ast.literal_eval.
								Example: --flox_kwargs min_count=5

	Description:
	Computes MVBS (Mean Volume Backscattering Strength) from a Sv NetCDF
	using echopype.commongrid.compute_MVBS. Data are binned along range
	and ping_time dimensions with a configurable reduction method.

	The expected input is a flat Sv NetCDF (the output of aa-sv, optionally
	after aa-clean). It is NOT the multi-group EchoData NetCDF produced by
	aa-nc.

	Pipeline example:
		aa-nc --sonar_model EK60 input.raw | aa-sv | aa-mvbs

	Direct example:
		aa-mvbs /path/to/input_Sv.nc --range_var depth --range_bin 50m \
				--ping_time_bin 60s --method coarsen -o /path/to/output.nc
```

## aa_mvbs_index

```bash
Usage: aa-mvbs-index [OPTIONS] [INPUT_PATH]

	Arguments:
	INPUT_PATH				  Path to the calibrated Sv .nc / .netcdf4
								file, or a converted Echopype file that
								can be calibrated to Sv.
								Optional. Defaults to stdin if not provided.

	Options:
	-o, --output_path		   Path to save the MVBS dataset.
								Default: same directory as input, with
								'_mvbs_index' appended to the stem and a
								.nc suffix.

	--range-sample-num INT	  Number of samples per bin along
								'range_sample'. Default: 100
	--ping-num INT			  Number of pings per bin along the ping
								axis. Default: 100

	Description:
	Computes MVBS by binning along the index-based axes (range_sample
	and ping number). This is distinct from physical-unit binning
	(meters/seconds), which is what compute_MVBS uses. The output path
	is printed to stdout for piping into the next stage of the pipeline.

	Example:
		aa-mvbs-index /path/to/input_Sv.nc --range-sample-num 30 \
					  --ping-num 5 -o /path/to/mvbs.nc
```

## aa_nasc

```bash
Usage: aa-nasc [OPTIONS] [INPUT_PATH]

	Arguments:
	INPUT_PATH				  Path to a Sv .nc / .netcdf4 file
								(typically the output of aa-sv or aa-clean).
								Optional. Defaults to stdin if not provided.

	Options:
	-o, --output_path		   Path to save processed output.
								Default: same directory as input, with
								'_nasc' appended to the stem and a .nc
								suffix. '_nasc' is ALWAYS appended, so the
								input file is never silently overwritten.

	--range_bin				 Depth bin size, e.g. "10m".
								Default: 10m

	--dist_bin				  Horizontal distance bin size, e.g. "0.5nmi".
								Default: 0.5nmi

	--method					Flox reduction strategy.
								Default: map-reduce

	--skipna					Skip NaN values when averaging. (Default.)
	--no_skipna				 Include NaN values in mean calculations.

	--closed					Which side of the bin interval is closed.
								Choices: left, right
								Default: left

	--flox_kwargs			   Extra flox kwargs as KEY=VALUE pairs.
								Values are parsed safely via ast.literal_eval,
								so '5' becomes int, 'true' is treated as
								a string (use 'True' for the bool), and
								anything that doesn't parse as a literal
								is kept as a plain string.
								Example: --flox_kwargs min_count=5 engine=numpy

	Description:
	Computes NASC (Nautical Area Scattering Coefficient) from a Sv NetCDF
	using echopype.commongrid.compute_NASC. NASC integrates Sv across
	range and distance bins, producing a standardized measure for biomass
	estimation.

	The expected input is a flat Sv NetCDF (the output of aa-sv, optionally
	after aa-clean). It is NOT the multi-group EchoData NetCDF produced by
	aa-nc.

	Pipeline example:
		aa-nc --sonar_model EK60 input.raw | aa-sv | aa-nasc

	Direct example:
		aa-nasc /path/to/input_Sv.nc --range_bin 20m --dist_bin 1nmi \
				--method map-reduce -o /path/to/output.nc
```

## aa_nc

```bash
Usage: aa-nc [OPTIONS] INPUT_PATH

	Arguments:
	INPUT_PATH				  Path to the input .raw file. (Required,
								may also be supplied via stdin.)

	Options:
	-o, --output_path		   Path to save the converted NetCDF output.
								Default: same directory as input, with the
								.raw stem and a .nc suffix.

	--sonar_model			   Sonar model identifier (REQUIRED).
								Examples: EK60, EK80, AZFP, EA640.

	Description:
	Converts a raw echosounder file (.raw) into a multi-group NetCDF
	EchoData file using echopype.open_raw. The output is the input to
	the next pipeline stage (aa-sv), which is what actually computes Sv.

	The input .raw file is never modified.

	Example:
	aa-nc /path/to/input.raw --sonar_model EK60 -o /path/to/output.nc
```

## aa_noise_est

```bash
Usage: aa-noise-est [OPTIONS] [INPUT_PATH]

	Arguments:
	  INPUT_PATH				   Path to the calibrated .nc (NetCDF) file
								   containing Sv (preferred), or a converted
								   Echopype file that can be calibrated to Sv.
								   Optional. Defaults to stdin if not provided.

	Options:
	  -o, --output_path PATH	   Where to write the background-noise estimate (NetCDF).
								   Default: <stem>_noise.nc

	  --ping-num INT			   Number of pings used to obtain noise estimates.
								   Default: 20
	  --range-sample-num INT	   Number of samples along the range axis for each estimate.
								   Default: 20
	  --background-noise-max STR   Upper limit for background noise (dB), e.g. '−125.0dB'.
								   Default: None

	  -h, --help				   Show this help message and exit.

	Description:
	  Estimates background noise by computing mean calibrated power from
	  windows of pings and range samples. Writes a NetCDF containing a single
	  variable "Sv_noise".

	Examples:
	  aa-noise-est data.nc --ping-num 50 --range-sample-num 200 --background-noise-max -120.0dB
	  aa-noise-est data.nc -o cruise01_legA_noise.nc
```

## aa_plot

```bash
Usage: aa-plot [OPTIONS] [INPUT_PATH]

Arguments:
  INPUT_PATH				Path to a NetCDF file (.nc). Optional; if omitted,
							reads a single path token from stdin.

Core selection:
  --var VAR				 Variable to plot (default: Sv if present, else first data_var).
  --all					 Plot all channels/frequencies as tabs (default
							behavior when the dataset has a 'channel' dim
							with > 1 entry; flag kept for backwards compat).
  --single				  Plot only one channel (default channel 0) instead
							of all-channels tabs. Use --channel/--frequency
							to choose which.
  --frequency FLOAT		 Select single nominal frequency (Hz) (nearest match).
  --channel NAME			Select single channel by name (exact match preferred).
  --group-by {auto,channel,freq}
							When tabs are shown and both channel+freq dims are available:
							  auto   -> frequency outer tabs, channel inner tabs
							  channel-> channel outer tabs, frequency inner tabs
							  freq   -> frequency outer tabs, channel inner tabs

Axes:
  --x NAME				  Override x-axis dim/coord (default: auto-detect).
  --y NAME				  Override y-axis dim/coord (default: auto-detect).
  --no-flip				 Disable automatic y-axis inversion for range/depth axes.

Appearance:
  --vmin FLOAT			  Lower color limit.
  --vmax FLOAT			  Upper color limit.
  --cmap NAME			   Initial colormap name (default: inferno).
  --width INT			   Minimum plot width in px; stretches beyond this (default: 800).
  --height INT			  Plot height (default: 450).
  --toolbar STR			 Toolbar: above/below/left/right/disable (default: above).
  --no-hover				Disable hover tooltip overlay.
  --no-crosshair			Disable crosshair cursor.
  --no-cmap-picker		  Disable the interactive colormap picker in the HTML.
  --no-log				  Disable the copyable data-summary log panel.

Drawing & annotation:
  --no-draw				 Disable the freehand/polyline/region drawing tools.

Subsetting / performance:
  --decimate INT			Take every Nth sample along x-axis (default: 1).
  --ymin FLOAT			  Crop lower y-limit.
  --ymax FLOAT			  Crop upper y-limit.

Output:
  -o, --output_path PATH	Output HTML path (default: <stem>_plot.html).
  --no-overwrite			Fail if output already exists.
  --quiet				   Suppress info logs; still prints final path.
  -h, --help				Show this help and exit.
```

## aa_raw

```bash
Usage: aa-raw [OPTIONS]

	Required:
	  --file_name NAME			Name of the file to download
								  (e.g. D20190804-T113723.raw).
	  --ship_name NAME			Name of the ship (e.g. Henry_B._Bigelow).
	  --survey_name NAME		  Name of the survey (e.g. HB1907).
	  --sonar_model NAME		  Type of echosounder (e.g. EK60, EK80).

	Optional:
	  --file_type TYPE			File type (default: raw).
	  --data_source SRC		   Data source identifier (default: NCEI).
								  Currently only 'NCEI' is wired through; other
								  values log a warning and proceed as NCEI.
	  --file_download_directory PATH
								  Where to download. Default: current directory.
								  Created if it doesn't exist.
	  --upload_to_gcp			 Also upload the downloaded file to GCP.
	  --debug					 Verbose logging (DEBUG level on stderr).
	  --quiet					 Suppress INFO logs; final path still prints.
	  -h, --help				  Show this help and exit.

	Description:
	  Downloads a raw echosounder file from NCEI given (ship, survey,
	  sonar_model, file_name). The absolute path of the downloaded file
	  is printed on stdout, ready for piping into aa-nc and onward.

	Pipeline example:
	  aa-raw --file_name D20190804-T113723.raw \
			 --ship_name Henry_B._Bigelow --survey_name HB1907 \
			 --sonar_model EK60 --file_download_directory ./downloads \
		| aa-nc --sonar_model EK60 | aa-sv | aa-clean

	Direct example:
	  aa-raw --file_name D20190804-T113723.raw \
			 --ship_name Henry_B._Bigelow --survey_name HB1907 \
			 --sonar_model EK60 \
			 --file_download_directory Henry_B._Bigelow_HB1907_EK60_NCEI
```

## aa_request

```bash
Usage: aa-request [OPTIONS] [EXISTING.yaml]

	Arguments:
	  EXISTING.yaml			 A request document to merge into or check.
								Optional; also accepted via -i or stdin.

	Building a request:
	  --vessel NAME			 Vessel as NCEI spells it (Alaska_Knight).
								Spaces are converted to underscores.
	  --survey NAME			 Survey identifier (CHS12AK).
	  --instrument NAME		 Echosounder (ES60, EK60, EK80).
	  --from WHEN			   Window start. A date (2012-08-13) or a
								datetime (2012-08-13T06:00:00). A bare date
								means 00:00:00.
	  --to WHEN				 Window end. A bare date means 00:00:00, so
								--from 2012-08-13 --to 2012-08-14 is one
								whole day.
	  --window FROM/TO		  Another window for the same request. Repeat
								for as many as you need.

	  --split-days N			Break each window into N-day pieces. One
								request, many windows — which is what makes
								a long survey resumable a day at a time
								instead of all or nothing.

	  --pad-minutes N		   Move each window start N minutes earlier.
								Raw files span 30-60 minutes, so the file
								covering 00:00 usually *starts* before it. A
								window that begins exactly at 00:00 misses
								that file, and the data you asked for begins
								mid-file. The document has no way to say
								"and the file that spans this edge", so
								widening the window is how you say it.
								Default 0: nothing is widened silently.

	Working with an existing document:
	  -i, --input PATH		  Merge into this document rather than starting
								empty. New windows join an existing request
								when vessel, survey and instrument all match;
								otherwise a new request is appended.
	  --check				   Validate and report. Writes nothing. Exit 4
								if the document is malformed.
	  --merge-windows		   Combine overlapping or touching windows in the
								result. Two windows that abut describe one
								range, and aa-fetch would list the seam twice.

	Output:
	  -o, --output_path PATH	Write here and print the path to stdout.
								Without it, the YAML goes to stdout.
	  --json					Emit the document as JSON instead of YAML.
								For the Workbench, not for aa-fetch.
	  --force				   Overwrite an existing output file.

	  -q, --quiet			   Warnings and errors only.
	  --debug				   Verbose logging.
	  -h, --help				This message.

	Exit codes:
	  0 ok		1 runtime error	2 usage	4 validation failed

	Examples:
	  aa-request --vessel Alaska_Knight --survey CHS12AK --instrument ES60 \
				 --from 2012-08-13 --to 2012-08-14 -o request.yaml
	  aa-request --check request.yaml
	  aa-request -i request.yaml --window 2012-08-20/2012-08-21 --merge-windows
	  aa-request --vessel Alaska_Knight --survey CHS12AK --instrument ES60 \
				 --from 2012-08-13 --to 2012-08-20 --split-days 1 | aa-fetch -
```

## aa_setup

```bash
Usage: aa-setup

	Description:
	Reinstalls the startup script for the AA-SI GPCSetup environment on a Google Cloud VM. 
```

## aa_show

```bash
Options:
	INPUT_PATH				  Path to the .raw or .netcdf4 file. (Required)
	-o, --output_path		   Path to save processed output.
								Default: overwrites .nc files or creates a new .nc for RAW.

	Description:


	Example:
```

## aa_sonar

```bash
Usage: aa-sonar [OPTIONS] [INPUT_PATH]

	Arguments:
	INPUT_PATH				  Path to a raw echosounder file.
								Supported extensions: .raw, .azfp, .ad2cp,
								.xml (AZFP sidecar). Optional; falls back
								to stdin if not provided.

	Options:
	--strict					Exit with non-zero status if the sonar
								model cannot be determined. Default
								behavior is to print 'UNKNOWN' and exit 0.

	--raw-name				  Emit the literal detection result instead
								of the normalized echopype identifier.
								Distinguishes ER60 from EK60 and AZFP6
								from AZFP. Off by default.

	Description:
	Detects the sonar model of a raw echosounder file. For .ad2cp and
	.azfp files the check is extension-based; for AZFP XML sidecars the
	InstrumentType element is inspected; for Simrad .raw files the
	config datagram header is read via aalibrary's sonar_checker.

	By default the output is normalized to a value accepted by echopype's
	`sonar_model` parameter, so it can be piped directly into aa-nc:

		aa-nc --sonar_model "$(aa-sonar input.raw)" input.raw

	Normalization:
		ER60		-> EK60
		AZFP6	   -> AZFP
		(others pass through unchanged)

	Pass --raw-name to disable normalization.

	The input file is never modified. All logs go to stderr; only the
	model identifier goes to stdout.

	Examples:
	aa-sonar /path/to/input.raw
	echo /path/to/input.raw | aa-sonar
	aa-sonar --raw-name /path/to/input.raw
```

## aa_sound_speed

```bash
Usage: aa-sound-speed [OPTIONS]

	Options:
	  --temperature FLOAT	 Temperature in deg C (default: 27)
	  --salinity FLOAT		Salinity in PSU / ppt (default: 35)
	  --pressure FLOAT		Pressure in dbar (default: 10)
	  --formula-source STR	'Mackenzie' (default) or 'AZFP'
	  -o, --output_path PATH  Optional NetCDF output (default: none)
	  --quiet				 Print only the numeric value
	  -h, --help			  Show this help message and exit

	Description:
	  Computes seawater sound speed in m/s using Echopype’s utilities.
	  If an output path is provided, writes a small NetCDF with a scalar
	  variable 'sound_speed' and the input parameters as attributes.

	Examples:
	  aa-sound-speed --temperature 10 --salinity 33 --pressure 5
	  aa-sound-speed --temperature 2 --salinity 35 --pressure 1000 --formula-source Mackenzie -o ssp.nc
```

## aa_splitbeam_angle

```bash
Usage: aa-splitbeam-angle [OPTIONS] [INPUT_PATH]

	Arguments:
	  INPUT_PATH				   Path to an Sv NetCDF (.nc). Optional; if omitted,
								   a path token may be read from stdin.

	Options:
	  -o, --output_path PATH	   Output NetCDF path (default: <stem>_splitbeam_angle.nc).
	  --echodata PATH			  Path to EchoData source (raw/converted) that holds
								   Sonar/Beam_group* data required for angle computation.
								   If not provided, defaults to INPUT_PATH.
	  --waveform-mode {CW,BB}	  Transmit waveform mode: CW (narrowband) or BB (broadband).
								   Required.
	  --encode-mode {complex,power}  Return echo encoding type: 'complex' or 'power'.
								   Required. ('power' only valid with CW.)
	  --pulse-compression		  Use pulse compression (valid only for BB + complex).
	  --no-overwrite			   Do not overwrite an existing output file.

	  -h, --help				   Show this help message and exit.

	Description:
	  Computes alongship and athwartship split-beam angles and adds them to the Sv dataset.
	  Requires the associated raw or converted file containing beam group and transducer data.
```

## aa_store

```bash
Usage: aa-store [OPTIONS] SUBCOMMAND [STORE]

	Subcommands:
	  info					  Describe the store: dims, chunk shape,
								chunks written vs expected, stored vs
								logical bytes, codec, lineage.
	  verify					The same read, judged. Exits 0 when the
								store is complete, 3 when it is coherent
								but unfinished (resumable), 4 when it is
								finished and wrong.

	Arguments:
	  STORE					 Path to a .zarr store. Optional; falls
								back to stdin, which may be a bare path
								(what every other aa-* tool prints) or an
								aa/1 handle line.

	Options:
	  --json					Emit one JSON document on stdout instead
								of the human summary. This is what the
								Workbench reads.
	  --arrays				  Include the per-array breakdown in --json
								output. Off by default: an EchoData store
								has dozens of arrays and the UI wants the
								summary.
	  --group PATH			  Restrict to one group, e.g. --group Sonar.
								Default: the whole store.
	  --no-census			   Skip the object count. dims, chunks and
								codec still come out; chunkCount and
								bytes.stored do not. Use on a remote store
								with millions of objects, where the
								listing is the entire cost.
	  --max-objects N		   Give up the census after N objects and
								report what was counted with
								census.partial = true. Default: 2000000.
	  --strict				  verify only: treat a store with no write
								marker and missing chunks as unfinished
								(exit 3) rather than assuming it is sparse
								by design. Off by default — see below.

	  -q, --quiet			   Warnings and errors only.
	  --debug				   Verbose logging.
	  -h, --help				This message.

	Sparse or unfinished?
	  A missing chunk means "every value here is the fill value". For a
	  mask that is the point; for an interrupted write it is data loss.
	  Nothing in the Zarr format distinguishes them, so aa-* tools record
	  completion in the root group's attributes under `aa_write` when they
	  finish. verify uses it:

		marker present, complete   missing chunks are sparsity   -> 0
		marker present, partial	missing chunks are unwritten  -> 3
		marker absent			  unknowable; reported, not	 -> 0
								   judged, unless --strict		  (3)

	Exit codes:
	  0 ok		1 runtime error	2 usage
	  3 partial (coherent, resumable)   4 verify failed

	Examples:
	  aa-store info combined.zarr
	  aa-store info --json --arrays combined.zarr | jq '.arrays[0]'
	  aa-store verify --strict combined.zarr || echo "not finished"
	  echo gs://bucket/tr07-sv.zarr | aa-store info --json
```

## aa_sv

```bash
Usage: aa-sv [OPTIONS] [INPUT_PATH]

	Arguments:
	INPUT_PATH				  Path to the .nc / .netcdf4 EchoData file.
								Optional. Defaults to stdin if not provided.

	Options:
	-o, --output_path		   Path to save processed output.
								Default: same directory as input, with '_Sv'
								appended to the stem and a .nc suffix.

	--waveform_mode			 For EK80 echosounders ONLY: waveform mode.
								Choices: CW, BB, FM
								Default: not passed (echopype picks per-sonar).

	--encode_mode			   For EK80 echosounders ONLY: encoding mode.
								Choices: complex, power
								Default: not passed (echopype picks per-sonar).

	Description:
	This tool computes Sv (volume backscattering strength) from a previously-
	converted NetCDF EchoData file using echopype.calibrate.compute_Sv, and
	saves the result to a new .nc file. The output path is printed to stdout
	for piping into the next stage of the pipeline.

	For visualization, pipe the output into aa-plot:
		aa-nc --sonar_model EK60 input.raw | aa-sv | aa-plot

	Example:
		aa-sv /path/to/input.nc --waveform_mode FM --encode_mode power \
			  -o /path/to/output.nc
```

## aa_swap_freq

```bash
Usage: aa-swap-freq [OPTIONS] [INPUT_PATH]

	Arguments:
	  INPUT_PATH				   Path to a NetCDF file (.nc) with a 'channel' dimension
								   and a 'frequency_nominal' variable/coordinate.
								   Optional. Defaults to stdin if not provided.

	Options:
	  -o, --output_path PATH	   Where to write the swapped dataset (NetCDF).
								   Default: <stem>_freqswap.nc
	  --check-unique			   Fail early if duplicate frequency_nominal values exist.
	  --no-overwrite			   Do not overwrite an existing output file.

	  -h, --help				   Show this help message and exit.

	Description:
	  Replaces the 'channel' dimension with the 'frequency_nominal' coordinate so that
	  data are indexed by nominal transducer frequency (e.g., 18000., 38000., 120000.).
	  Operation requires unique frequencies.
```

## aa_transient

```bash
Usage: aa-transient [OPTIONS] [INPUT_PATH]

	Arguments:
	INPUT_PATH				  Path to the calibrated .nc / .netcdf4 file
								containing Sv (preferred), or a converted
								Echopype file that can be calibrated to Sv.
								Optional. Defaults to stdin if not provided.

	Options:
	-o, --output_path		   Path to save the transient-noise mask (NetCDF).
								Default: same directory as input, with
								'_transient_mask' appended to the stem
								and a .nc suffix.

	--apply					 Also apply the mask to Sv and write a cleaned
								Sv file alongside the mask, suffix
								'_transient_cleaned'.

	--func					  Pooling function ('nanmean', 'nanmedian', etc.).
								Default: nanmean
	--depth-bin				 Vertical bin size, e.g. '10m'. Default: 10m
	--num-side-pings			Pings on each side for the pooling window.
								Default: 25
	--exclude-above			 Exclude depths shallower than this, e.g.
								'250.0m'. Default: 250.0m
	--transient-threshold	   Threshold in dB above local context, e.g.
								'12.0dB'. Default: 12.0dB
	--range-var				 Name of the range/depth coordinate.
								Default: depth
	--use-index-binning		 Use index-based binning instead of physical
								units.
	--chunk KEY=VAL [...]	   Optional chunk sizes as key=value pairs
								(e.g., ping_time=256 depth=512).

	Description:
	Creates a boolean mask marking likely transient-noise events using a
	pooling comparison in depth-binned windows. Optionally applies the mask
	to Sv to produce a cleaned Sv dataset. The mask path is printed to
	stdout for piping into the next stage of the pipeline.

	Example:
		aa-sv input.nc | aa-transient --apply --depth-bin 10m \
			  --transient-threshold 14.0dB
```

## aa_ts

```bash
Usage: aa-ts [OPTIONS] [INPUT_PATH]

	Arguments:
	INPUT_PATH				  Path to the .nc / .netcdf4 EchoData file.
								Optional. Defaults to stdin if not provided.

	Options:
	-o, --output_path		   Path to save processed output.
								Default: same directory as input, with '_ts'
								appended to the stem and a .nc suffix.

	--env-param KEY=VALUE	   Environmental parameter override (repeatable).
								Example: --env-param sound_speed=1500
										 --env-param temperature=10.5

	--cal-param KEY=VALUE	   Calibration parameter override (repeatable).
								Example: --cal-param gain_correction=1.0

	--waveform_mode			 For EK80 echosounders: waveform mode.
								Choices: CW, BB, FM   (default: CW)

	--encode_mode			   For EK80 echosounders: encoding mode.
								Choices: complex, power   (default: complex)

	Description:
	This tool computes TS (target strength) from a previously-converted
	NetCDF EchoData file using echopype.calibrate.compute_TS, and saves
	the result to a new .nc file. The output path is printed to stdout
	for piping into the next stage of the pipeline.

	Example:
	aa-ts /path/to/input.nc --env-param sound_speed=1500 \
		--cal-param gain_correction=1.0 -o /path/to/input_ts.nc
```

## aa_upload

```bash
Usage: aa-upload [OPTIONS] [PATH]

	Arguments:
	  PATH						File or directory to upload. May be a
								  bare name (resolved against CWD), a
								  relative path, or absolute path.
								  Optional; falls back to stdin if not
								  given. Symlinks are followed.

	Echosounder-mode options (used unless --as-is is set):
	  --ship_name NAME			Ship name as stored in NCEI / GCP
								  (normalized form, e.g. Henry_B._Bigelow).
								  REQUIRED in echosounder mode.
	  --survey_name NAME		  Survey name (e.g. HB1603).
								  REQUIRED in echosounder mode.
	  --sonar_model NAME		  Echosounder model (e.g. EK60, EK80).
								  REQUIRED in echosounder mode.
	  --data_source SRC		   Data source tag stored alongside the
								  file in GCP. Defaults to 'HDD' (the
								  convention for local-disk uploads).
								  Other values: NCEI, OMAO, etc.

	As-is mode options:
	  --as-is, --as_is			Upload the input verbatim to GCP under
								  --destination_prefix. Accepts EITHER
								  a single file or a directory:
									- File	  -> blob path is
												   <destination_prefix>/<filename>
												   (via cloud_utils'
												   upload_file_to_gcp_bucket).
									- Directory -> uploaded via
												   egress.upload_folder_as_is_to_gcp.
								  No ship/survey/echosounder metadata
								  required — the prefix you supply IS
								  the path layout.
	  --destination_prefix PFX	Bucket-relative prefix to drop the
								  file or folder under (e.g. other/scratch/).
								  REQUIRED in as-is mode. Trailing slash
								  is normalized.

	GCP environment:
	  --gcp_env {prod,dev}		Switch the active aalibrary GCP env
								  before uploading via
								  aalibrary.config.use_gcp_prod() or
								  use_gcp_dev(). If neither this nor
								  the explicit overrides below are set,
								  whatever env vars are already exported
								  in the shell are used.
	  --project_id ID			 Explicit GCP project id (overrides
								  --gcp_env).
	  --gcp_bucket_name NAME	  Explicit GCP bucket name (overrides
								  --gcp_env).

	Other:
	  --dry-run, --dry_run		Resolve mode, validate everything,
								  set up the GCP bucket object, but do
								  NOT call the upload functions. Useful
								  for checking flags before a long run.
	  --debug					 Verbose logging (DEBUG level).
	  --quiet					 Suppress INFO logs; pass-through path
								  still prints on stdout.
	  -h, --help				  Show this help and exit.

	Description:
	  Uploads a single file or a directory to GCP via aalibrary.egress.

	  Single-file inputs are handled by symlinking the file into a
	  temporary directory and pointing the echosounder-mode uploader at
	  that temp directory. This way aa-upload never has to hardcode the
	  data/raw/<ship>/<survey>/<echosounder>/<file> path convention —
	  whichever convention the directory uploader uses is the one we
	  use. Only single files with extensions in {.raw, .idx, .bot, .nc}
	  are accepted in echosounder mode.

	  The input PATH is printed back to stdout unchanged so aa-upload
	  can sit in the middle of a pipeline as a side-effect tee. If
	  you're using aa-upload as the last stage, ignore stdout.

	Examples:
	  # As a side-effect tee between aa-ed and aa-sv:
	  echo HB1603_L1-D20160703-T183957.raw | aa-ed \
		| aa-upload --ship_name Henry_B._Bigelow \
					--survey_name HB1603 --sonar_model EK60 \
		| aa-sv | aa-graph

	  # Upload a whole survey directory under the canonical layout:
	  aa-upload ./Henry_B._Bigelow/HB1603/EK60 \
		--ship_name Henry_B._Bigelow --survey_name HB1603 \
		--sonar_model EK60 --data_source HDD

	  # Dump a folder anywhere in the bucket, ignoring conventions:
	  aa-upload ./scratch_data --as-is --destination_prefix other/junk/

	  # Upload a single arbitrary file (e.g. a region file):
	  aa-upload region.evr --as-is \
		--destination_prefix HDD/Henry_B_Bigelow/HB1603/Echosounder/Data/Evr/

	  # Dry-run before a long upload:
	  aa-upload ./big_dir --ship_name X --survey_name Y \
		--sonar_model EK80 --dry-run
```

