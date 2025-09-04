from aalibrary import quick_test
import sys


def print_console_tools_reference():
    reference = """
    Console Tools Reference
    -----------------------

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
    
    aa-find 
    
    aa-raw --file_name "2107RL_CW-D20210813-T220732.raw" --file_type "raw" --ship_name "Reuben_Lasker" --survey_name "RL2107" --echosounder "EK80" --data_source "NCEI" --file_download_directory "."
    
    aa-nc <path-to-raw> --sonar_model <sonar_model> | aa-sv --plot Sv --x ping_time --y range_sample | aa-clean --plot Sv --x ping_time --y range_sample | aa-mvbs 
    
    
    
    """
    print(reference)


def main():

    # Call the function to display the reference
    print_console_tools_reference()
    sys.exit(0)


if __name__ == "__main__":
    main()
