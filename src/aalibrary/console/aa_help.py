from aalibrary import quick_test
import sys


def print_console_tools_reference():
    reference = """
    Console Tools Reference:

    aa-test : Test aaa_SI library
    aa-plot : Plot an .nc or .raw file
    aa-raw : Download or upload raw file
    aa-nc : Download or upload .nc file
    aa-kmap : Run a kmeans cluster map operation
    aa-help : Print this example reference or help

    Example use cases:

    Plot an .nc file:
    aa-nc --file_name "2107RL_CW-D20210813-T220732.raw" --file_type "raw" \\
           --ship_name "Reuben_Lasker" --survey_name "RL2107" --echosounder "EK80" \\
           --data_source "NCEI" --file_download_directory "." \\
           --upload_to_gcp | aa-plot --title "My Custom Plot" --xlabel "Time" \\
           --ylabel "Distance" --color "green" --linestyle "dashed" --linewidth 3.0

    Plot a .raw file:
    aa-raw --file_name "2107RL_CW-D20210813-T220732.raw" --file_type "raw" \\
           --ship_name "Reuben_Lasker" --survey_name "RL2107" --echosounder "EK80" \\
           --data_source "NCEI" --file_download_directory "." \\
           --upload_to_gcp | aa-plot --title "My Custom Plot" --xlabel "Time" \\
           --ylabel "Power" --color "blue" --linestyle "dashed" --linewidth 1.0

    Run a default K Means Cluster Map operation on the .raw or .nc data:
    aa-nc --file_name "2107RL_CW-D20210813-T220732.raw" --file_type "raw" \\
           --ship_name "Reuben_Lasker" --survey_name "RL2107" --echosounder "EK80" \\
           --data_source "NCEI" --file_download_directory "." \\
           --upload_to_gcp | aa-kmap --config --override1 --override2
    """
    print(reference)


def main():
    
    # Call the function to display the reference
    print_console_tools_reference()
    sys.exit(0)

if __name__ == "__main__":
    main()