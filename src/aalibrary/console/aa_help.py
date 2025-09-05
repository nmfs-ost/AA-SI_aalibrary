from aalibrary import quick_test
import sys


def print_console_tools_reference():
    reference = """

    
    Active Acoustics Console Tooling Reference
    ------------------------------------------

    (For specific details of an individual tool, use the --help flag)

    Most tools are designed to work with NetCDF (.nc) files and support
    piping workflows, where the output of one tool is passed as input
    to the next.

    Basic Tools
    -----------
    aa-raw     : Download or upload raw acoustic files.
    aa-nc      : Convert raw input files into NetCDF format.
    aa-sv      : Compute and save volume backscattering strength (Sv).
    aa-ts      : Generate target strength (TS) datasets.
    aa-clean   : Denoise and clean input datasets.

    Processing Tools
    ----------------
    aa-mvbs    : Compute mean volume backscattering strength (MVBS).
    aa-nasc    : Compute Nautical Areal Scattering Coefficient (NASC)
                 from Sv datasets.
    aa-assign  : Assign new coordinates or metadata to datasets.
    aa-crop    : Extract a subsection of a dataset.

    Quality Control Tools
    ---------------------
    (designed to validate and subset datasets)
    aa-assign  : Assign new coordinates or metadata to datasets.
    aa-crop    : Extract a subsection of a dataset.

    Advanced Tools
    --------------
    aa-find    : Query the ICES database to find ship names.
    
    
    Example Usecases
    ----------------

    Description: A very useful tool for locating data.

    aa-find


    Example 1:
    In this example, a single raw file is passed explicitly as a positional argument rather than piped. 
    The original raw data is converted, processed, and summarized in one seamless command. 
    The raw data is passed to aa-nc to produce a NetCDF file with the specified EK60 sonar model. 
    Sv values are computed immediately with aa-sv, cleaned of noise with aa-clean, and then summarized 
    into Multi-Volume Backscatter using aa-mvbs. This one-liner demonstrates how modular console tools 
    can be chained together to perform a complete processing workflow efficiently, without creating intermediate 
    files. Defaults are supplied with argparse library.

    Command:
    aa-nc /home/mryan/Desktop/HB1603_L1-D20160707-T190150.raw --sonar_model EK60 | aa-sv | aa-clean | aa-mvbs


    Example 2:
    A raw acoustic file is first prepared with aa-raw, which automatically incorporates metadata such as 
    ship, survey, and echosounder information. Its output is then converted to a NetCDF file with aa-nc, 
    Sv values are computed on the fly with aa-sv, cleaned by aa-clean, and summarized for Multi-Volume 
    Backscatter using aa-mvbs. Each tool focuses on a specific task, and chaining them together allows the 
    entire processing workflow to be executed in a single, streamlined command. Defaults are supplied with 
    argparse library.

    Command:
    aa-raw --file_name "2107RL_CW-D20210813-T220732.raw" --file_type "raw" --ship_name "Reuben_Lasker" --survey_name "RL2107" --echosounder "EK80" --data_source "NCEI" --file_download_directory "."
    aa-nc <path-to-raw> --sonar_model <sonar_model> | aa-sv --plot Sv --x ping_time --y range_sample | aa-clean --plot Sv --x ping_time --y range_sample | aa-mvbs


    Example 3:
    Another raw file workflow using aa-raw to prepare the data, aa-nc to convert to NetCDF, and then 
    aa-sv, aa-clean, and aa-mvbs for processing and summarization. This example shows a fully automated 
    workflow for the EK60 echosounder. Defaults are supplied with argparse library.

    Command:
    aa-raw --file_name D20190804-T113723.raw --ship_name Henry_B._Bigelow --survey_name HB1907 --echosounder EK60 --file_download_directory Henry_B._Bigelow_HB1907_EK60_NCEI | aa-nc --sonar_model EK60 | aa-sv | aa-clean | aa-mvbs

    """
    print(reference)


def main():

    # Call the function to display the reference
    print_console_tools_reference()
    sys.exit(0)


if __name__ == "__main__":
    main()
