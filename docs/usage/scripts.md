# One-Time Scripts

The repository for AALibrary also includes many useful, pre-made scripts that users can run. These scripts serve the purpose of being run once, and usually accomplish small but important tasks. You can take a look below:

!!! note "NOTE"
    These scripts are self-contained. AALibrary does not have to be installed to run most of these scripts.

## Scripts Location

Most scripts are located within the repo in the `other` folder.

## NCEI Metadata Backfilling Script

This script is located [here](https://github.com/nmfs-ost/AA-SI_aalibrary/blob/main/other/nncei_metadata_backfilling.ipynb). It is used for backfilling metadata from NCEI to the Metadata DB located in our GCP environment. You will need to extract all metadata from the NCEI database into an Excel file. The script will then automatically parse through the data in the Excel file and populate the Metadata DB in GCP.

## Comparing Local Files To NCEI

This script is located [here](https://github.com/nmfs-ost/AA-SI_aalibrary/blob/main/other/compare_local_to_NCEI.ipynb). It is used for comparing the files of a local survey's echosounder to the files that exist in NCEI. It compares file names, file sizes, and file checksums. An executive summary is generated (printed to console), and an Excel report can be saved if the param is set. There is also an option to upload the files to GCP for intermittent storage until they are ready to be archived at NCEI. To begin, download a copy of the notebook, fill out the first cell with your variables, and follow the directions in the script. Note: You might run into authorization errors which you can use [these](../getting-started/installation.md) instructions to fix.
