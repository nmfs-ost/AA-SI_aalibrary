# One-Time Scripts

The repository for AALibrary also includes many useful, pre-made scripts that users can run. These scripts serve the purpose of being run once, and usually accomplish small but important tasks. You can take a look below:

!!! note "NOTE"
    These scripts are self-contained. AALibrary does not have to be installed to run most of these scripts.

## Scripts Location

Most scripts are located within the repo in the `other` folder.

## NCEI Metadata Backfilling Script

This script is located [here](). It is used for backfilling metadata from NCEI to the Metadata DB located in our GCP environment. You will need to extract all metadata from the NCEI database into an Excel file. The script will then automatically parse through the data in the Excel file and populate the Metadata DB in GCP.
