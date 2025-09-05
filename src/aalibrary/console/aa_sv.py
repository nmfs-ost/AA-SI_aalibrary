#!/usr/bin/env python3
"""
Console tool for converting RAW files to NetCDF using Echopype,
removing background noise, applying transformations, and saving back.
"""

import argparse
from html import parser
import sys
from pathlib import Path



from loguru import logger
import echopype as ep  # make sure echopype is installed


import panel as pn
import hvplot.xarray
import hvplot
import holoviews as hv
import panel as pn
import nbformat as nbf
hv.extension('bokeh')
pn.extension('bokeh')
#hvplot.extension('matplotlib')



def write_panel_notebook(output_path: Path):
    nb = nbf.v4.new_notebook()

    # Intro markdown cell
    imports = """from loguru import logger
import echopype as ep  # make sure echopype is installed


import panel as pn
import hvplot.xarray
import hvplot
import holoviews as hv
import panel as pn
import nbformat as nbf
hv.extension('bokeh')
pn.extension('bokeh')
"""
    nb.cells.append(nbf.v4.new_markdown_cell(imports))

    # Panel + hvplot setup
    functions = """def process_file(
    input_path: Path,
    output_path: Path,
    plot: str = None
):
    


    logger.info(f"Loading NetCDF file {input_path} into EchoData...")
    ed = ep.open_converted(input_path)

    logger.info(f"Computing Sv from EchoData...")
    ds_Sv = ep.calibrate.compute_Sv(ed)

    # Step 4: Save back to NetCDF
    logger.info(f"Saving processed EchoData to {output_path} ...")

    #ds_Sv = clean_attrs(ds_Sv)

    output_path = output_path.with_suffix(".nc")
    ds_Sv.to_netcdf(output_path)
    
    
    logger.info("Sv computation complete.")

        
        # Slider for channel selection
    channel_slider = pn.widgets.IntSlider(
        name='Channel',
        start=0,
        end=ds_Sv.sizes['channel']-1,
        step=1,
        value=0
    )

    # Bind slider to plot
    interactive_plot = pn.bind(plot_channel, ds=ds_Sv, plot=plot, channel=channel_slider)

    # Layout: slider above plot
    dashboard = pn.Column(channel_slider, interactive_plot)
"""
    nb.cells.append(nbf.v4.new_code_cell(functions))

    # Example widget/plot cell
    executions = """import pandas as pd
import numpy as np

df = pd.DataFrame({
    'x': np.linspace(0, 10, 200),
    'y': np.sin(np.linspace(0, 10, 200))
})

plot = df.hvplot.line(x='x', y='y')
pn.panel(plot).servable()
"""
    nb.cells.append(nbf.v4.new_code_cell(executions))

    # Save the notebook
    with output_path.open("w") as f:
        nbf.write(nb, f)

# Usage
write_panel_notebook(Path("panel_demo.ipynb"))

def print_help():
    help_text = """
    Usage: aa-sv [OPTIONS] [INPUT_PATH]

    Arguments:
    INPUT_PATH                 Path to the .raw or .netcdf4 file. (Optional, defaults to stdin)

    Options:
    -o, --output_path           Path to save processed output.
                                Default: overwrites .nc files or creates a new .nc for RAW.

    Description:
    This tool processes .raw or .netcdf4 files with Echopype and removes
    background noise using ping-based and range-based thresholds.

    Example:
    aa-clean /path/to/input.raw --ping_num 50 --range_sample_num 200 \\
            --snr_threshold 5.0 -o /path/to/output.nc
    """
    print(help_text)


def main():

    if len(sys.argv) == 1:
        if not sys.stdin.isatty():
            stdin_data = sys.stdin.readline().strip()
            if stdin_data:
                sys.argv.append(stdin_data)
        else:
            print_help()
            sys.exit(0)

    parser = argparse.ArgumentParser(
        description="Process .raw or .netcdf4 files with Echopype and remove background noise."
    )

    # ---------------------------
    # Required file arguments
    # ---------------------------
    parser.add_argument(
        "input_path",
        type=Path,
        help="Path to the .raw or .netcdf4 file.",
        nargs="?",  # makes it optional
    )

    parser.add_argument(
        "-o",
        "--output_path",
        type=Path,
        help="Path to save processed output. Default behavior overwrites .nc files or creates a new .nc for RAW.",
    )
    
    
    parser.add_argument(
        "--plot",
        nargs="?",        # means "0 or 1 values allowed"
        const="Sv",  # value to use if provided without a value
        default=None,     # value if the option is not provided at all
        type=str,
        help="Optional argument with an optional value"
    )

    parser.add_argument(

        "--waveform_mode",
        type=str,
        help="Optional argument to specify the waveform mode",
        default="CW",     # value if the option is not provided at all
        choices=["CW", "BB", "FM"]
    )
    
    
    parser.add_argument(
        "--encode_mode",
        default="complex",     # value if the option is not provided at all
        type=str,
        choices=["complex", "power"],
        help="Optional argument with an optional value"
    )

    # ---------------------------
    # remove_background_noise arguments
    # ---------------------------


    args = parser.parse_args()

    # ---------------------------
    # Validate input
    # ---------------------------

    if args.input_path is None:
        # Read from stdin

        args.input_path = Path(sys.stdin.readline().strip())
        logger.info(f"Read input path from stdin: {args.input_path}")

    if not args.input_path.exists():
        logger.error(f"File '{args.input_path}' does not exist.")
        sys.exit(1)

    allowed_extensions = {".netcdf4": "netcdf", ".nc": "netcdf"}

    ext = args.input_path.suffix.lower()
    if ext not in allowed_extensions:
        logger.error(
            f"'{args.input_path.name}' is not a supported file type. "
            f"Allowed: {', '.join(allowed_extensions.keys())}"
        )
        sys.exit(1)

    file_type = allowed_extensions[ext]

    # ---------------------------
    # Set default output path
    # ---------------------------
    if args.output_path is None:
        if file_type == "netcdf":
            # Overwrite the existing NetCDF
            args.output_path = args.input_path

        else:
            # RAW file → produce NetCDF with same stem
            args.output_path = args.input_path.with_suffix(".nc")

    # ---------------------------
    # Process file
    # ---------------------------
    try:
        
        args.output_path = args.output_path.with_stem(args.output_path.stem + "_Sv")
        args.output_path = args.output_path.with_suffix(".nc")
        
        process_file(
            input_path=args.input_path,
            output_path=args.output_path,
            plot=args.plot,
            waveform_mode=args.waveform_mode,
            encode_mode=args.encode_mode
        )

        # Print output path to stdout for piping
        print(args.output_path.resolve())

    except Exception as e:
        logger.exception(f"Error during processing: {e}")
        sys.exit(1)


def clean_attrs(Sv):
    # Dataset-level attrs
    #keywords = top_check.attrs.get("keywords", "")
    #is_kongsberg = bool(re.search(combined_pattern, keywords))
    
    for k, v in Sv.attrs.items():
        if v is None:
            Sv.attrs[k] = "NA"  # or float('nan') if numeric

    # Variable-level attrs
    for var in Sv.data_vars:
        for k, v in Sv[var].attrs.items():
            if v is None:
                Sv[var].attrs[k] = "NA"  # or float('nan') if numeric
    return Sv

    # Function to update plot based on channel
    
def plot_channel(ds, plot, channel=0):
    data = ds[plot].isel(channel=channel)
    return data.hvplot(
    x="ping_time",
    y="range_sample",
    cmap="viridis",
    responsive=True,   # let it auto-resize, no aspect math
    width=2400,
    height=1200,
    invert_yaxis=True,
    title=f"Sv Plot - Channel {channel}"
    )
    


def process_file(
    input_path: Path,
    output_path: Path,
    plot: str = None,
    waveform_mode: str = None,
    encode_mode: str = None
):
    """
    Load EchoData from RAW or NetCDF, compute Sv and save to NetCDF.
    """


    logger.info(f"Loading NetCDF file {input_path} into EchoData...")
    ed = ep.open_converted(input_path)

    logger.info(f"Computing Sv from EchoData...")
    ds_Sv = ep.calibrate.compute_Sv(ed, waveform_mode=waveform_mode, encode_mode=encode_mode)

    # Step 4: Save back to NetCDF
    logger.info(f"Saving processed EchoData to {output_path} ...")

    ds_Sv = clean_attrs(ds_Sv)
    ed.Sv = ds_Sv.Sv  # Update EchoData with cleaned Sv
    output_path = output_path.with_suffix(".nc")
    ed.to_netcdf(output_path)
    
    
    logger.info("Sv computation complete.")


    if plot:
        
        # Slider for channel selection
        channel_slider = pn.widgets.IntSlider(
            name='Channel',
            start=0,
            end=ds_Sv.sizes['channel']-1,
            step=1,
            value=0
        )

        # Bind slider to plot
        interactive_plot = pn.bind(plot_channel, ds=ds_Sv, plot=plot, channel=channel_slider)

        # Layout: slider above plot
        dashboard = pn.Column(channel_slider, interactive_plot)

            
        # Serve interactive plot
        output_path = output_path.with_suffix(".html")
        #hvplot.save(plt, output_path)
        
        # Save standalone interactive HTML
        dashboard.save(output_path, embed=True)
        
        # Serve interactive plot
        output_path = output_path.with_suffix(".png")
        #hvplot.save(plt, output_path)
        
        # Save standalone interactive HTML
        #dashboard.save(output_path, embed=True)
        
        write_panel_notebook()
    
    

if __name__ == "__main__":
    main()
