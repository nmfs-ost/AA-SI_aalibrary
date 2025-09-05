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

    Description : A very useful tool for locating data.

    aa-find
    
     
    Description : In this example, a single raw file is not piped but explicitlly written as a positional argument. The original raw data is converted, processed, and summarized in one seamless command. The raw data is passed to aa-nc to produce a NetCDF file with the specified EK60 sonar model. Sv values are computed immediately with aa-sv, cleaned of noise with aa-clean, and then summarized into Multi-Volume Backscatter using aa-mvbs. This one-liner showcases how modular console tools can be chained together to perform a full processing workflow efficiently, without creating intermediate files.
    
    aa-nc /home/mryan/Desktop/HB1603_L1-D20160707-T190150.raw --sonar_model EK60 | aa-sv | aa-clean | aa-mvbs
    
    
    aa-raw --file_name "2107RL_CW-D20210813-T220732.raw" --file_type "raw" --ship_name "Reuben_Lasker" --survey_name "RL2107" --echosounder "EK80" --data_source "NCEI" --file_download_directory "."
    
    
    aa-nc <path-to-raw> --sonar_model <sonar_model> | aa-sv --plot Sv --x ping_time --y range_sample | aa-clean --plot Sv --x ping_time --y range_sample | aa-mvbs


    Description : A raw acoustic file is first prepared with aa-raw, automatically incorporating metadata like ship, survey, and echosounder information. Its output is immediately converted to a NetCDF file with aa-nc, and Sv values are computed on the fly with aa-sv. The resulting data is then cleaned by aa-clean and summarized for Multi-Volume Backscatter using aa-mvbs. Each tool handles a focused task, and by chaining them together, a complete processing workflow is executed in a single, streamlined command.
    
    aa-raw --file_name D20190804-T113723.raw --ship_name Henry_B._Bigelow --survey_name HB1907 --echosounder EK60 --file_download_directory Henry_B._Bigelow_HB1907_EK60_NCEI | aa-nc --sonar_model EK60 | aa-sv | aa-clean | aa-mvbs

    """
    print(reference)


def main():

    # Call the function to display the reference
    print_console_tools_reference()
    sys.exit(0)


if __name__ == "__main__":
    main()
