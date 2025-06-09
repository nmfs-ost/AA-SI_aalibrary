import argparse
import sys
import os
import matplotlib.pyplot as plt
import echopype as ep
import shlex

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
        cmap=args.color or 'viridis'
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
    fig.colorbar(im, ax=ax, label='Sv (dB)')

    # Layout adjustment
    plt.tight_layout()

    # Save the figure if output path is provided
    if getattr(args, "output_file", None):
        print(f"💾 Saving plot to {args.output_file}")
        plt.savefig(args.output_file, dpi=300)
    else:
        print("👀 Displaying plot interactively")
        plt.show()


def main():
    
    # Read from stdin and split into argument list
    if not sys.stdin.isatty():
        input_line = sys.stdin.read().strip()
        args_list = shlex.split(input_line)
    else:
        args_list = sys.argv[1:]    

    parser = argparse.ArgumentParser(description="Plot from .raw file path using echopype.")
    parser.add_argument('raw_path', help='Path to .raw file')
    parser.add_argument('echosounder', help='Sonar model (examples: EK60, EK80)')

    # Plot options
    parser.add_argument('--title', type=str, default='Echo Data Plot')
    parser.add_argument('--xlabel', type=str, default='ping_time')
    parser.add_argument('--ylabel', type=str, default='range_bin')
    parser.add_argument('--color', type=str, default='viridis')
    parser.add_argument('--linestyle', type=str, default='solid')
    parser.add_argument('--linewidth', type=float, default=1.5)
    parser.add_argument('--output-file', type=str, help='Path to save the plot image')

    args = parser.parse_args(args_list)

    raw_path = args.raw_path
    echosounder = args.echosounder

    print(args_list)

    if not os.path.exists(raw_path):
        sys.exit(f"❌ File not found: {raw_path}")

    ed = ep.open_raw(raw_path, sonar_model=echosounder)
    ds_Sv = ep.calibrate.compute_Sv(ed, waveform_mode="CW", encode_mode="complex")
    print(ds_Sv["Sv"][0])
    ds_Sv["Sv"][0].plot(
        linestyle=args.linestyle,
        linewidth=args.linewidth
    )
    plt.savefig("sv_plot.png", dpi=300, bbox_inches='tight')

if __name__ == '__main__':
    main()
