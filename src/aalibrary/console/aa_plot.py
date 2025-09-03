import argparse
from email import parser
import sys
import os
import matplotlib.pyplot as plt
import echopype as ep
from pathlib import Path

import hvplot.xarray  # ensure hvplot is enabled
import holoviews as hv


from loguru import logger

def print_help():
    help_text = "HHHHHHHHHHHHHHHHEEEEEEEEEEEEEEEEELLLLLLLLLLLLLLLLLLPPPPPPPPPPPPPPPPPPP"
    print(help_text)



def save_echogram(channel):
    hv.extension("bokeh")
    hvplot.extension('matplotlib')
    frequency = "import frequency class"
    input_path = "import input path"
    echogram_color_map = "import echogram color map"
        # Get the channel index from the frequency string.
    #cmap = plt.get_cmap(self.color_map, self.n_clusters)
    logger.info("Saving echogram for frequency: " + frequency + ", channel: " + str(channel))      
    # Transpose and plot using hvplot
    # Create the plot
    #channel_int = int(channel)
    plot = channel.transpose("range_sample", "ping_time").hvplot(
        x="ping_time",
        y="range_sample",
        cmap="viridis",
        title=f"frequency = {frequency},    file = {input_path},    colormap = {echogram_color_map}",
        invert_yaxis=True,
        aspect='auto',
        width=2400,   # adjust as needed
        height=1600
    )

    # Save the plot as HTML
    hv.save(plot, "test.png")

def plot_ds(ds, x=None, y=None, cmap="viridis", title=None, linestyle="solid", linewidth=2, output_path=None, **kwargs):
    """
    Plot an xarray Dataset or DataArray using hvplot with consistent defaults
    and graceful dimension fallback.

    Parameters
    ----------
    ds : xarray.Dataset or xarray.DataArray
        The dataset or dataarray to plot.
    var : str, optional
        Variable name in the dataset to plot (ignored if ds is DataArray).
    x : str, optional
        Coordinate/dimension for the x-axis.
    y : str, optional
        Coordinate/dimension for the y-axis.
    cmap : str, default "viridis"
        Colormap for the plot.
    title : str, optional
        Plot title. If None, uses variable name.
    **kwargs :
        Extra keyword arguments passed to hvplot.

    Returns
    -------
    hvplot object
    """


    # Get list of dimensions
    dims = list(ds.dims)

    # Auto-fallback for x
    if x not in dims:
        x = dims[0] if len(dims) >= 1 else None

    # Auto-fallback for y
    if y not in dims:
        y = dims[1] if len(dims) >= 2 else (dims[0] if len(dims) == 1 else None)


    # Defaults
    default_kwargs = dict(
        x=x,
        y=y,
        cmap=cmap,
        title=title,
        invert_yaxis=True,
        aspect="auto",
        width=2400,
        height=1600,
    )
    default_kwargs.update(kwargs)

    hv.save(ds.hvplot(**default_kwargs), output_path)


def full_save(ds_Sv):
    for channel in ds_Sv["Sv"]:
        save_echogram(channel)




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
        description="Plot from .netcdf4 file path using echopype and hvplot."
    )


    parser.add_argument(
        "input_path",
        type=Path,
        help="Path to the .netcdf4 file.",
        nargs="?"                # makes it optional
    )
    
    parser.add_argument(
        "-o", "--output_path",
        type=Path,
        help="Path to save the output plot."
    )
    
    parser.add_argument("--x", type=str, default=None, help="Dimension/coordinate for x-axis")
    parser.add_argument("--y", type=str, default=None, help="Dimension/coordinate for y-axis")

    # Styling options
    parser.add_argument("--cmap", type=str, default="viridis", help="Colormap for the plot")
    parser.add_argument("--title", type=str, default=None, help="Plot title")
    parser.add_argument("--invert_yaxis", action="store_true", help="Invert the y-axis")
    parser.add_argument("--aspect", type=str, default="auto", help="Aspect ratio")
    parser.add_argument("--width", type=int, default=2400, help="Plot width in pixels")
    parser.add_argument("--height", type=int, default=1600, help="Plot height in pixels")
    parser.add_argument("--linestyle", type=str, default="solid", help="Line style")
    parser.add_argument("--linewidth", type=float, default=1.5, help="Line width")

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

    allowed_extensions = {
        ".netcdf4": "netcdf",
        ".nc": "netcdf"
    }

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
            logger.info(f"No output path provided. Overwriting input NetCDF: {args.output_path}")
            
        else:
            # RAW file → produce NetCDF with same stem
            args.output_path = args.input_path.with_suffix(".nc")
            logger.info(f"No output path provided. Saving to: {args.output_path}")

    # ---------------------------
    # Process file
    # ---------------------------
    try:
        
        args.output_path = args.output_path.with_stem(args.output_path.stem + "_plot")
        args.output_path = args.output_path.with_suffix(".png")
        logger.trace(f"Output path set to: {args.output_path}")
        
        process_file(
            input_path=args.input_path,
            output_path=args.output_path,       # map argparse --output-file
            
            x=args.x,
            y=args.y,
            cmap=args.cmap,
            title=args.title,
            linestyle=args.linestyle,
            linewidth=args.linewidth
        )

        print(args.output_path.resolve())

    except Exception as e:
        logger.exception(f"Error during processing: {e}")
        sys.exit(1)
    
def process_file(
    input_path: str = None,
    output_path: str = None,
    ds: str = None,
    x: str = None,
    y: str = None,
    title: str = None,
    cmap: str = "viridis",
    linestyle: str = "solid",
    linewidth: float = 1.5,
):

    
    
    
    
    """
    Load EchoData from RAW or NetCDF, remove background noise, apply transformations, and save to NetCDF.
    """

    logger.info(f"Loading converted file {input_path} into plotting application...")
    ed = ep.open_converted(input_path)
    
    if ds is None:
        logger.info("No dataset provided, computing Sv from EchoData...")
    if ds is Sv:
        logger.info("Using provided Sv dataset for plotting...")
    if ds 
    
    
    ds_Sv = ep.calibrate.compute_Sv(ed)

    plot_ds(ds_Sv, x, y, cmap, title=title, linestyle=linestyle, linewidth=linewidth, output_path=output_path)

    logger.info("Processing complete.")



if __name__ == "__main__":
    main()
