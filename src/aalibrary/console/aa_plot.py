import argparse
import sys
import os
import matplotlib.pyplot as plt
import echopype as ep
import shlex

import holoviews as hv
hv.extension("bokeh")

from loguru import logger

def plot_data(sv_da, args):
    print("📊 Plotting data...")

    # Create the figure and axis
    fig, ax = plt.subplots(figsize=(10, 5))

    # Plotting the Sv (dB) data
    im = ax.pcolormesh(
        sv_da.ping_time,
        sv_da.range_bin,
        sv_da.transpose(),  # Range on Y-axis, Time on X-axis
        shading="auto",
        cmap=args.color or "viridis",
    )

    # Title and labels
    ax.set_title(args.title or "Echo Data Plot")
    ax.set_xlabel(args.xlabel or "Ping Time")
    ax.set_ylabel(args.ylabel or "Range Bin")

    # Style the plot spines
    for spine in ax.spines.values():
        spine.set_linewidth(args.linewidth)
        spine.set_linestyle(args.linestyle)

    # Add colorbar
    fig.colorbar(im, ax=ax, label="Sv (dB)")

    plt.gca().invert_yaxis()
    # Layout adjustment
    plt.tight_layout()

    # Save the figure if output path is provided
    if getattr(args, "output_file", None):
        print(f"💾 Saving plot to {args.output_file}")
        plt.savefig(args.output_file, dpi=300)
    else:
        print("👀 Displaying plot interactively")
        plt.show()

def save_echogram(self, data_array, channel):
    
    frequency = self.get_frequency(channel)
        # Get the channel index from the frequency string.
    #cmap = plt.get_cmap(self.color_map, self.n_clusters)
    logger.info("Saving echogram for frequency: " + frequency + ", channel: " + str(channel))      
    # Transpose and plot using hvplot
    # Create the plot
    channel_int = int(channel)
    plot = data_array[channel_int].transpose("depth", "ping_time").hvplot(
        x="ping_time",
        y="range_sample",
        cmap=self.echogram_color_map,
        title=f"frequency = {frequency},    file = {self.input_path},    colormap = {self.echogram_color_map}",
        invert_yaxis=True,
        aspect='auto',
        width=2400,   # adjust as needed
        height=1600
    )

    # Save the plot as HTML
    hv.save(plot, f"{self.asset_path}/eg_{self.name}_{frequency}.html")

def full_save(self, Sv):
    for channel in self.Sv["Sv"]:
        self.save_echogram(self.Sv["Sv"], channel)

def main():

    # Read from stdin and split into argument list
    if not sys.stdin.isatty():
        input_line = sys.stdin.read().strip()
        args_list = shlex.split(input_line)
    else:
        args_list = sys.argv[1:]

    parser = argparse.ArgumentParser(
        description="Plot from .raw file path using echopype."
    )
    parser.add_argument("raw_path", help="Path to .raw file")
    parser.add_argument("--sonar_model", help="Sonar model (examples: EK60, EK80)")
    parser.add_argument("--frequency", help="Sonar frequency (examples: 18kHz, 38kHz, 70kHz)")

    # Plot options
    
    parser.add_argument("--title", type=str, default="Echo Data Plot")
    parser.add_argument("--xlabel", type=str, default="ping_time")
    parser.add_argument("--ylabel", type=str, default="range_bin")
    parser.add_argument("--color", type=str, default="viridis")
    parser.add_argument("--linestyle", type=str, default="solid")
    parser.add_argument("--linewidth", type=float, default=1.5)
    parser.add_argument("--output-file", type=str, help="Path to save the plot image")
    
    args = parser.parse_args(args_list)

    raw_path = args.raw_path
    sonar_model = args.sonar_model

    print(args_list)

    if not os.path.exists(raw_path):
        sys.exit(f"❌ File not found: {raw_path}")

    ed = ep.open_raw(raw_path, sonar_model=sonar_model)
    ds_Sv = ep.calibrate.compute_Sv(ed, waveform_mode="CW", encode_mode="complex")
    




if __name__ == "__main__":
    main()
