from aalibrary import quick_test
import argparse
import sys

def main():
    parser = argparse.ArgumentParser(description="Plot data via matplotlib from a .raw or .nc file.")
    parser.add_argument(
        "file_path",
        type=str,
        help="Path to the .raw or .nc file to be processed. Only .raw or .nc files are supported.",
    )

    args = parser.parse_args()

    # Validate the file extension
    if not args.file_path.lower().endswith((".raw", ".nc")):
        print("Error: Unsupported file type. Only .raw or .nc files can be used for plotting.")
        sys.exit(1)

    # Call the quick_test function with the provided file
    quick_test.start(args.file_path)
    sys.exit(0)

if __name__ == "__main__":
    main()