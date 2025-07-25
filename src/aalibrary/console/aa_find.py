from aalibrary.ices_ship_names import get_all_ices_ship_names
from aalibrary.utils.ncei_utils import (
    get_all_survey_names_from_a_ship,
    get_all_echosounders_in_a_survey,
    get_all_ship_names_in_ncei,
)
from InquirerPy import inquirer
import subprocess, os
from loguru import logger



def main():

    mode = inquirer.select(
        message="Select option",
        choices=["Search By NCEI Vessels"],
        default="Search By NCEI Vessels",
    ).execute()

    if mode == "Search By NCEI Vessels":
        ship_name = inquirer.fuzzy(
            message="Select a vessel:",
            choices=get_all_ship_names_in_ncei(),
        ).execute()

        survey = inquirer.fuzzy(
            message="Select Survey from " + ship_name,
            choices=get_all_survey_names_from_a_ship(ship_name),
        ).execute()

        echosounder = inquirer.select(
            message="Select Echosounder from " + survey,
            choices=get_all_echosounders_in_a_survey(ship_name, survey),
        ).execute()

        operation = inquirer.select(
            message="Select Operation for " + echosounder,
            choices=["Download .raw", "Download .nc ", "Plot Echogram", "Run KMeans"],
        ).execute()

        if operation == "Download .raw":
            # Define the folder name
            folder_name = ship_name + "_" + survey + "_" + echosounder

            # Create the full path using '.'
            path = os.path.join('.', folder_name)

            # Make the directory (does nothing if it already exists)
            os.makedirs(path, exist_ok=True)
            logger.info(f"Downloading {echosounder} data for {ship_name} in {survey} to {folder_name}")
            logger.debug(f"Running command: aa-raw --file_name {folder_name} --ship_name {ship_name} --survey_name {survey} --echosounder {echosounder} --file_download_directory {folder_name}")
            subprocess.run([
                "aa-raw",
                "--file_name", folder_name,
                "--ship_name", ship_name,
                "--survey_name", survey,
                "--echosounder", echosounder,
                "--file_download_directory", folder_name,
            ])
            
    

if __name__ == "__main__":
    main()
