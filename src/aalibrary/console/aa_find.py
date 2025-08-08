from aalibrary.ices_ship_names import get_all_ices_ship_names
from aalibrary.utils.ncei_utils import (
    get_all_survey_names_from_a_ship,
    get_all_echosounders_in_a_survey,
    get_all_ship_names_in_ncei,
    get_all_file_names_from_survey,
    get_all_raw_file_names_from_survey,
)
from InquirerPy import inquirer
import subprocess, os
from loguru import logger
import warnings


warnings.filterwarnings("ignore")

def main():
    os.system('cls' if os.name == 'nt' else 'clear')
    mode = inquirer.select(
        message="Select option",
        choices=[
            "Search By NCEI Vessels",
        ],
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

        # Get all file names for the selected survey

        file_name = inquirer.select(
            message="Select Files from " + survey,
            choices=get_all_raw_file_names_from_survey(ship_name, survey, echosounder),
        ).execute()

        operation = inquirer.select(
            message="Select Operation for " + echosounder,
            choices=["Download .raw", "Download .nc ", "Plot Echogram", "Run KMeans"],
        ).execute()

        if operation == "Download .raw":
            # Define the folder name
            folder_name = ship_name + "_" + survey + "_" + echosounder + "_" + "NCEI"

            # Create the full path using '.'
            path = os.path.join(".", folder_name)

            # Make the directory (does nothing if it already exists)
            os.makedirs(path, exist_ok=True)
            logger.info(
                f"Downloading {echosounder} data for {ship_name} in {survey} to {folder_name}"
            )
            logger.debug(
                f"Running command: aa-raw --file_name {file_name} --ship_name {ship_name} --survey_name {survey} --echosounder {echosounder} --file_download_directory {folder_name} from directory: {os.getcwd()} from the environment: {subprocess.run(['which', 'python'], capture_output=True, text=True).stdout.strip()}"
            )
            subprocess.run(
                [
                    "aa-raw",
                    "--file_name",
                    file_name,
                    "--file_type",
                    "raw",
                    "--ship_name",
                    ship_name,
                    "--survey_name",
                    survey,
                    "--echosounder",
                    echosounder,
                    "--data_source",
                    "NCEI",
                    "--file_download_directory",
                    folder_name,
                ]
            )
            
        if operation == "Plot Echogram":
            # Define the folder name
            folder_name = ship_name + "_" + survey + "_" + echosounder + "_" + "NCEI"

            # Create the full path using '.'
            path = os.path.join(".", folder_name)

            # Make the directory (does nothing if it already exists)
            os.makedirs(path, exist_ok=True)
            logger.info(
                f"Plotting {echosounder} data for {ship_name} in {survey} to {folder_name}"
            )
            logger.debug(
                f"Running command: aa-plot {file_name} --sonar_model {echosounder} --output-file {folder_name}/echogram.png from directory: {os.getcwd()} from the environment: {subprocess.run(['which', 'python'], capture_output=True, text=True).stdout.strip()}"
            )
            subprocess.run(
                [
                    "aa-plot",
                    file_name,
                    "--sonar_model",
                    echosounder,
                    "--output-file",
                    f"{folder_name}/echogram.png",
                ]
            )


if __name__ == "__main__":
    main()
