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
	INPUT_PATH				  Path to the .netcdf4 file.
								Optional. Defaults to stdin if not provided.

	Options:
	-o, --output_path		   Path to save processed output.
								Default: overwrites .nc files or creates a new .nc for RAW.
	--ping_num				  Number of pings to use for background noise removal.
								Default: 20
	--range_sample_num		  Number of range samples to use for background noise removal.
								Default: 20
	--background_noise_max	  Optional maximum background noise value.
								Default: None
	--snr_threshold			 SNR threshold in dB.
								Default: 3.0

	Description:
	This tool processes .netcdf4 files with Echopype and removes
	background noise using ping-based and range-based thresholds.

	Example:
	aa-clean /path/to/input.nc --ping_num 50 --range_sample_num 200 \
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

## aa_depth

```bash
Options:
	INPUT_PATH				  Path to the .raw or .netcdf4 file. (Required)
	-o, --output_path		   Path to save processed output.
								Default: overwrites .nc files or creates a new .nc for RAW.
	--variable				  Variable to add to the output dataset (e.g., depth, location, splitbeam_angle).

	Description:


	Example:
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

## aa_evl

```bash
aa-evl — apply Echoview line(s) (.evl) to an echogram NetCDF (.nc/.netcdf4)

USAGE
  echo input.nc | aa-evl --evl seafloor.evl | aa-plot --all
  aa-evl input.nc --evl top.evl bottom.evl --keep between
  echo input.nc | aa-evl --evl seafloor.evl --depth-offset -5.0 --overwrite

REQUIRED
  --evl EVL [EVL ...]	 One or more .evl paths (accepts wildcards via shell).

INPUT
  INPUT_PATH [INPUT_PATH ...]
	Optional positional .nc paths. If omitted, reads newline-delimited .nc paths
	from stdin.

OUTPUT
  -o, --output-path PATH  Only valid when processing exactly 1 input.
  --out-dir DIR		   Output directory for pipelines / multiple inputs.
  --suffix TEXT		   Suffix appended to output stem (default: _evl).
  --overwrite			 Overwrite output files if they exist.

MASKING
  --keep {above,below,between}
						  Which side of the line(s) to KEEP (default: above).
							above   : keep data shallower than the line; mask
									  deeper data.  Union = per-ping MIN depth.
									  Typical use: exclude seafloor / bottom.
							below   : keep data deeper than the line; mask
									  shallower data.  Union = per-ping MAX depth.
									  Typical use: exclude near-surface noise.
							between : keep data BETWEEN two lines.  Requires
									  exactly 2 EVL files (upper then lower).
  --depth-offset METRES   Shift the composite line by this many metres before
						  masking.  Negative = shift up (shallower); positive =
						  shift down (deeper).  Default: 0.0.
						  Example: --depth-offset -5  removes 5 m above seafloor.
  --var NAME			  Variable to mask (default: Sv).
  --time-dim NAME		 Time dimension name (default: infer ping_time or time).
  --depth-dim NAME		Depth dimension name (default: infer depth, range_sample,
						  or range_bin).
  --channel-index INT	 Channel used to resolve metre-depth coordinates when the
						  variable has a 'channel' dim (default: 0).
  --write-line			Write the interpolated composite line as a variable
						  'evl_line_depth' in the output NetCDF.
  --fail-empty			Exit non-zero if the composite line has no valid points.
  --debug				 Verbose diagnostics to stderr.

EXAMPLES
  # Mask everything below the seafloor line, with a 5 m safety buffer above it:
  aa-evl input.nc --evl seafloor.evl --keep above --depth-offset -5.0

  # Mask near-surface noise (everything above the surface exclusion line):
  aa-evl input.nc --evl surface.evl --keep below

  # Keep only data between two operator-drawn lines:
  aa-evl input.nc --evl upper.evl lower.evl --keep between

  # Full pipeline:
  echo D20090916-T132105.raw | aa-nc --sonar_model EK60 | aa-sv | aa-depth \
	| aa-evl --evl seafloor.evl --depth-offset -5.0 --overwrite | aa-plot --all

NOTE
  EVL files are Echoview line export files containing (datetime, depth_metres)
  pairs that define a boundary line across the echogram.  They differ from EVR
  (region) files, which contain closed polygons.
```

## aa_evr

```bash
aa-evr — apply Echoview region(s) (.evr) to an echogram NetCDF (.nc/.netcdf4)

USAGE
  # Apply existing EVR file(s):
  aa-sv input.raw | aa-depth | aa-evr --evr regions/*.evr | aa-plot --all
  aa-sv input.raw | aa-depth | aa-evl --evl bottom.evl | aa-evr --evr school.evr | aa-plot --all
  aa-evr input.nc --evr a.evr b.evr

  # Draw new regions interactively (--evr omitted):
  cat input.nc | aa-evr --name my_school.evr
  cat input.nc | aa-evr --name my_school.evr | aa-plot --all

MODES
  EVR mode (--evr provided)   Apply existing .evr file(s) to echogram.
  Draw mode (--evr omitted)   Open an interactive Bokeh browser app to draw
							  freehand ROI regions directly on the echogram.
							  Saves both a new .evr and a masked .nc.

REQUIRED (EVR mode only)
  --evr EVR [EVR ...]	 One or more .evr paths.

INPUT
  INPUT_PATH [INPUT_PATH ...]
	Optional positional .nc paths. If omitted, reads newline-delimited .nc paths from stdin.

OUTPUT
  -o, --output-path PATH  Only valid when processing exactly 1 input.
  --out-dir DIR		   Output directory for pipelines / multiple inputs.
  --suffix TEXT		   Output suffix appended to input stem (default: _evr).
  --overwrite			 Overwrite output files.

DRAWING MODE OPTIONS
  --name TEXT			 EVR output filename (default: <input_stem>_regions.evr).
						  May include or omit the .evr extension.
  --port INT			  Port for the Bokeh drawing server (default: 5006;
						  auto-increments if occupied).

MASKING (both modes)
  --var NAME			  Variable to mask (default: Sv).
  --time-dim NAME		 Time dimension name (default: infer ping_time else time).
  --depth-dim NAME		Depth dimension name (default: infer depth, range_sample,
						  or range_bin).
  --channel-index INT	 Channel used to build mask when var has 'channel' dim
						  (default: 0).
  --write-mask			Write union mask as int8 variable 'region_mask' in output.
  --fail-empty			Exit non-zero if union mask is empty (0 cells inside).
  --debug				 Verbose diagnostics to stderr.

EXAMPLES
  # Mask to EVR regions only:
  aa-evr input.nc --evr school.evr --overwrite

  # Pipeline: EVL bottom line first, then EVR school regions:
  echo D20090916-T132105.raw | aa-nc --sonar_model EK60 | aa-sv | aa-depth \
	| aa-evl --evl seafloor.evl --depth-offset -5.0 \
	| aa-evr --evr d20090916_t124739-t132105.evr --overwrite \
	| aa-plot --all

  # Draw new regions interactively, then plot:
  cat D20191001-T003423_Sv_depth.nc | aa-evr --name school_regions.evr | aa-plot --all

NOTE
  If the EVR polygon time range does not fully overlap the echogram ping_time range
  (e.g. because an upstream aa-evl has already masked the data), echoregions may
  return zero matching regions for some files. This is treated as an all-False mask
  for that file (nothing is kept from it) rather than a crash, and a warning is
  emitted. Run with --debug to compare time ranges.
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

## aa_impulse

```bash
Usage: aa-impulse [OPTIONS] [INPUT_PATH]

	Arguments:
	  INPUT_PATH				 Path to the calibrated .nc (NetCDF) file
								 containing Sv (preferred), or a converted
								 Echopype file that can be calibrated to Sv.
								 Optional. Defaults to stdin if not provided.

	Options:
	  -o, --output_path PATH	 Where to write the impulse-noise mask (NetCDF).
								 Default: <stem>_impulse_mask.nc
	  --apply					Also apply the mask to Sv and write a cleaned
								 Sv file (suffix: _impulse_cleaned.nc).
	  --depth-bin STR			Vertical bin size for comparison, e.g. '5m'.
								 Default: 5m
	  --num-side-pings INT	   Pings on each side for two-sided comparison.
								 Default: 2
	  --impulse-threshold STR	Threshold in dB above local context, e.g. '10.0dB'.
								 Default: 10.0dB
	  --range-var STR			Name of the range/depth coordinate (e.g., 'depth').
								 Default: depth
	  --use-index-binning		Use index-based binning instead of physical units.
	  -h, --help				 Show this help message and exit.

	Description:
	  Creates a boolean mask marking likely impulse-noise “flecks” using a
	  ping-wise two-sided comparison in depth-binned windows.
	  Optionally applies the mask to Sv to produce a cleaned Sv dataset.

	Examples:
	  aa-impulse data.nc --depth-bin 5m --num-side-pings 3 --impulse-threshold 12dB
	  aa-impulse data.nc --apply -o out_mask.nc
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
	INPUT_PATH				  Path to the .netcdf4 file.
								Optional. Defaults to stdin if not provided.

	Options:
	-o, --output_path		   Path to save processed MVBS output.
								Default: overwrites input .nc with MVBS group
								or creates a new .nc.

	--range_var				 Range coordinate to bin over.
								Choices: echo_range, depth
								Default: echo_range
	--range_bin				 Bin size along range dimension.
								Default: 20m
	--ping_time_bin			 Bin size along ping_time dimension.
								Default: 20s
	--method					Computation method for binning.
								Choices: map-reduce, coarsen, block
								Default: map-reduce
	--reindex				   Reindex result to match uniform bin edges.
								Default: False
	--skipna					Skip NaN values when averaging.
								Default: True
	--fill_value				Fill value for empty bins.
								Default: NaN
	--closed					Which side of bins are closed.
								Choices: left, right
								Default: left
	--range_var_max			 Optional maximum value for range_var.
								Default: None
	--flox_kwargs			   Optional advanced arguments for flox.
								Format: key=value

	Description:
	This tool computes MVBS (Mean Volume Backscattering Strength) from
	.raw or .netcdf4 files using Echopype. Data are binned along
	range and ping_time dimensions with configurable methods.

	Example:
	aa-mvbs /path/to/input.nc --range_var depth --range_bin 50m \
			--ping_time_bin 60s --method coarsen -o /path/to/output.nc
```

## aa_mvbs_index

```bash
Usage: aa-mvbs-index [OPTIONS] [INPUT_PATH]

	Arguments:
	  INPUT_PATH				   Path to the calibrated Sv NetCDF (.nc),
								   or a converted Echopype file that can be calibrated.
								   Optional. Defaults to stdin if not provided.

	Options:
	  -o, --output_path PATH	   Where to write the MVBS dataset (NetCDF).
								   Default: <stem>_mvbs_index.nc

	  --range-sample-num INT	   Number of samples along 'range_sample' per bin.
								   Default: 100
	  --ping-num INT			   Number of pings per bin along ping axis.
								   Default: 100

	  -h, --help				   Show this help message and exit.

	Description:
	  Computes Mean Volume Backscattering Strength (MVBS) by binning along
	  the index-based axes (range_sample and ping number). This differs from
	  physical-unit binning (meters/seconds) done by compute_MVBS.

	Examples:
	  aa-mvbs-index data.nc --range-sample-num 30 --ping-num 5
	  aa-mvbs-index data.nc -o mvbs_idx.nc
```

## aa_nasc

```bash
Usage: aa-nasc [OPTIONS] [INPUT_PATH]

	Arguments:
	INPUT_PATH				  Path to the .netcdf4 file.
								Optional. Defaults to stdin if not provided.

	Options:
	-o, --output_path		   Path to save processed NASC output.
								Default: overwrites input .nc with NASC group
								or creates a new .nc.

	--range-bin				 Depth bin size in meters.
								Default: 10m
	--dist-bin				  Horizontal distance bin size in nautical miles.
								Default: 0.5nmi
	--method					Flox reduction strategy for binning.
								Default: map-reduce
	--skipna					Skip NaN values when averaging.
								Default: enabled
	--no-skipna				 Include NaN values in mean calculations.
	--closed					Which side of the bin interval is closed.
								Choices: left, right
								Default: left
	--flox-kwargs			   Additional flox arguments as key=value pairs.
								Example: --flox-kwargs min_count=5

	Description:
	This tool computes NASC (Nautical Area Scattering Coefficient) from
	.raw or .netcdf4 files with Echopype. NASC integrates Sv (volume
	backscattering strength) across range and distance bins, producing
	standardized measures for biomass estimation and comparison.

	Example:
	aa-nasc /path/to/input.nc --range-bin 20m --dist-bin 1nmi \
			--method map-reduce -o /path/to/output.nc
```

## aa_nc

```bash
Usage: aa-nc [OPTIONS] INPUT_PATH

	Arguments:
	INPUT_PATH				 Path to the input .raw file. (Required)

	Options:
	-o, --output_path		   Path to save processed Sv output (.nc file).
								Default: creates a new .nc from the input .raw.

	--sonar_model			   Sonar model number (required).
								Example: EK60, EK80, etc.

	Description:
	This tool calculates Sv (volume backscattering strength) from a
	.raw file using Echopype. The output is always a NetCDF (.nc) file
	containing the computed Sv values. A new .nc file is created for the
	output; the input .raw file is never overwritten.

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
  --all					 Plot all channels/frequencies as tabs.
  --frequency FLOAT		 Select single nominal frequency (Hz) (nearest match).
  --channel NAME			Select single channel by name (exact match preferred).
  --group-by {auto,channel,freq}
							When --all and both channel+freq dimensions are available:
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

	Options:
	--file_name				 Name of the file to download. (Required)
	--file_type				 Type of the file. Default: raw
	--ship_name				 Name of the ship. (Required)
	--survey_name			   Name of the survey. (Required)
	--sonar_model			   Type of echosounder. (Required)
	--data_source			   Source of the data. Default: NCEI
	--file_download_directory   Directory to download the file. Default: current directory (.)
	--upload_to_gcp			 Flag to upload the downloaded file to GCP.
	--debug					 Enable debug mode for verbose output.

	Description:
	This tool downloads a raw file from Azure based on the specified
	ship, survey, and sonar model. Optionally, the file can be uploaded
	to GCP after download. Useful for automating access to remote
	acoustic data.

	Example:
	aa-raw --file_name D20190804-T113723.raw --ship_name Henry_B._Bigelow \
		   --survey_name HB1907 --sonar_model EK60 \
		   --file_download_directory Henry_B._Bigelow_HB1907_EK60_NCEI
```

## aa_refresh

```bash
Usage:
  aa-refresh [--help] [--only <pip_name>]

Description:
  Uninstalls and reinstalls development libraries from their GitHub
  repositories (main branch).  Uses --no-cache-dir and --force-reinstall
  so setuptools re-discovers any new sub-packages on install.

  Libraries refreshed:
	aalibrary		(AA-SI_aalibrary)
	AA-SI-KMEANS	 (AA-SI_KMeans)

Options:
  --only <pip_name>   Refresh a single library instead of all.
					  e.g.  aa-refresh --only AA-SI-KMEANS
  --help, -h		  Show this help message.

Notes:
  - Intended for use inside your active virtual environment.
  - Uses: python -m pip ... (so it targets the current interpreter).
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

## aa_sv

```bash
Usage: aa-sv [OPTIONS] [INPUT_PATH]

	Arguments:
	INPUT_PATH				 Path to the .raw or .netcdf4 file.
								Optional. Defaults to stdin if not provided.

	Options:
	-o, --output_path		   Path to save processed output.
								Default: overwrites .nc files or creates a new .nc for RAW.

	--plot [VALUE]			  Generate plots of the processed data.
								Optional argument with optional value.
								If provided without a value, defaults to 'Sv'.
								Example: --plot or --plot TS
								Default: None

	--waveform_mode			 For EK80 echosounders: specify waveform mode.
								Choices: CW, BB, FM
								Default: CW

	--encode_mode			   For EK80 echosounders: specify encoding mode.
								Choices: complex, power
								Default: complex

	Description:
	This tool computes Sv (volume backscattering strength) from .raw or
	.netcdf4 files with Echopype. It includes optional plotting and
	EK80-specific waveform/encoding configuration.

	Example:
	aa-sv /path/to/input.nc --waveform_mode FM --encode_mode power \
		  --plot Sv -o /path/to/output.nc
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
	  INPUT_PATH				 Path to the calibrated .nc (NetCDF) file
								 containing Sv (preferred), or a converted
								 Echopype file that can be calibrated to Sv.
								 Optional. Defaults to stdin if not provided.

	Options:
	  -o, --output_path PATH	 Where to write the transient-noise mask (NetCDF).
								 Default: <stem>_transient_mask.nc
	  --apply					Also apply the mask to Sv and write a cleaned
								 Sv file (suffix: _transient_cleaned.nc).

	  # mask_transient_noise parameters
	  --func STR				 Pooling function ('nanmean', 'nanmedian', etc.).
								 Default: nanmean
	  --depth-bin STR			Vertical bin size, e.g. '10m'. Default: 10m
	  --num-side-pings INT	   Pings on each side for pooling window.
								 Default: 25
	  --exclude-above STR		Exclude depths shallower than this (e.g. '250.0m').
								 Default: 250.0m
	  --transient-threshold STR  Threshold in dB above local context, e.g. '12.0dB'.
								 Default: 12.0dB
	  --range-var STR			Name of the range/depth coordinate (e.g., 'depth').
								 Default: depth
	  --use-index-binning		Use index-based binning instead of physical units.
	  --chunk KEY=VAL [...]	  Optional chunk sizes as key=value pairs (e.g., ping_time=256 depth=512).

	  -h, --help				 Show this help message and exit.

	Description:
	  Creates a boolean mask marking likely transient-noise events using a pooling
	  comparison in depth-binned windows. Optionally applies the mask to Sv to
	  produce a cleaned Sv dataset.

	Examples:
	  aa-transient data.nc --depth-bin 10m --num-side-pings 21 --transient-threshold 14.0dB
	  aa-transient data.nc --apply -o out_mask.nc
```

## aa_ts

```bash
Usage: aa-ts [OPTIONS] [INPUT_PATH]

	Arguments:
	INPUT_PATH				 Path to the .raw or .netcdf4 file. (Optional, defaults to stdin)

	Options:
	-o, --output_path		   Path to save processed output.
								Default: overwrites .nc files or creates a new .nc for RAW.

	Description:
	This tool processes .raw or .netcdf4 files with Echopype and removes
	background noise using ping-based and range-based thresholds.

	Example:
	aa-clean /path/to/input.raw --ping_num 50 --range_sample_num 200 \
			--snr_threshold 5.0 -o /path/to/output.nc
```

