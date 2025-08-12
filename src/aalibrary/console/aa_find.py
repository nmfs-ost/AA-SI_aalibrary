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

    while True:
        os.system("cls" if os.name == "nt" else "clear")
        mode = inquirer.select(
            message="Select option",
            choices=[
                "Search NCEI Data by Vessel",
                "Search OMAO Data by Vessel",
                "Authenticate with Google",
                "Resources and Documentation",
                "Exit",
            ],
            default="Search NCEI Data by Vessel",
        ).execute()

        if mode == "Search NCEI Data by Vessel":
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
                choices=get_all_raw_file_names_from_survey(
                    ship_name, survey, echosounder
                ),
            ).execute()

            operation = inquirer.select(
                message="Select Operation for " + echosounder,
                choices=[
                    "Download .raw",
                    "Download .nc ",
                    "Plot Echograms",
                    "Run KMeans",
                    "Run DBScan",
                ],
            ).execute()

            if operation == "Download .raw":
                # Define the folder name
                folder_name = (
                    ship_name + "_" + survey + "_" + echosounder + "_" + "NCEI"
                )

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

            if operation == "Plot Echograms":
                # Define the folder name
                folder_name = (
                    ship_name + "_" + survey + "_" + echosounder + "_" + "NCEI"
                )

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

            if operation == "Run KMeans":
                logger.info(
                    f"Running KMeans on {echosounder} data for {ship_name} in {survey}"
                )
                logger.info(f"This functionality is not yet available")

            if operation == "Run DBScan":
                logger.info(
                    f"Running DBScan on {echosounder} data for {ship_name} in {survey}"
                )
                logger.info(f"This functionality is not yet available")

        if mode == "Search OMAO Data by Vessel":
            logger.info("Searching OMAO Data by Vessel...")
            logger.info(f"This functionality is not yet available. ")

        if mode == "Authenticate with Google":
            logger.info("Authenticating with Google...")

            logger.info(f"This functionality is not yet available. ")

        if mode == "Resources and Documentation":
            logger.info("Accessing Resources and Documentation...")

            resource = inquirer.select(
                message="Select Resource",
                choices=[
                    "AA-SI Homepage",
                    "AA-SI GitHub",
                    "NCEI Website",
                    "OMAO Website",
                ],
            ).execute()

            if resource == "AA-SI Homepage":
                link = "https://www.ametecosystems.org/aa-si/"
                logger.info("For more information, visit: " + link)
                logger.info(f"This functionality is not yet available.")

            if resource == "AA-SI GitHub":
                link = "https://github.com/orgs/nmfs-ost/repositories?q=AA"
                logger.info("For more information, visit: " + link)
                repo = inquirer.select(
                    message="Select Repository",
                    choices=[
                        "AA-SI_aalibrary",
                        "AA-SI_GCPSetup",
                        "AA-SI_DataRoadMap",
                        "AA-SI_KMeans",
                        "AA-SI_DBScan",
                    ],
                ).execute()

                if repo == "AA-SI_aalibrary":
                    link = "https://github.com/nmfs-ost/AA-SI_aalibrary"
                    logger.info("For more information, visit: " + link)
                if repo == "AA-SI_GCPSetup":
                    link = "https://github.com/nmfs-ost/AA-SI_GCPSetup"
                    logger.info("For more information, visit: " + link)
                if repo == "AA-SI_DataRoadMap":
                    link = "https://github.com/nmfs-ost/AA-SI_DataRoadMap"
                    logger.info("For more information, visit: " + link)
                if repo == "AA-SI_KMeans":
                    link = "https://github.com/nmfs-ost/AA-SI_KMeans"
                    logger.info("For more information, visit: " + link)
                if repo == "AA-SI_DBScan":
                    link = "https://github.com/nmfs-ost/AA-SI_DBScan"
                    logger.info("For more information, visit: " + link)

            if resource == "OMAO Website":
                link = "https://www.omao.noaa.gov/"
                logger.info("For more information, visit: " + link)

            if resource == "NCEI Website":
                link = "https://www.ncei.noaa.gov/"
                logger.info("For more information, visit: " + link)

        if mode == "Exit":
            logger.info("Exiting aa-find")
            break


if __name__ == "__main__":
    main()
