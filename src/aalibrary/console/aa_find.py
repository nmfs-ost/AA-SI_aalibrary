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
import webbrowser


warnings.filterwarnings("ignore")
# ANSI colorized string
prompt_str = "aa-tools>"


def main():
    os.system("cls" if os.name == "nt" else "clear")
    while True:
        #os.system("cls" if os.name == "nt" else "clear")
        mode = inquirer.select(
            message=prompt_str,
            choices=[
                "search NCEI vessel data",
                "search OMAO vessel data",
                "authenticate with Google",
                "resources and documentation",
                "exit",
            ],
            default="search NCEI vessel data",
        ).execute()
        os.system("cls" if os.name == "nt" else "clear")
        if mode == "search NCEI vessel data":
            
            ship_name = inquirer.fuzzy(
                message="select NCEI vessel:",
                choices=["Back"]+get_all_ship_names_in_ncei(),
            ).execute()

            if ship_name == "Back":
                continue

            survey = inquirer.fuzzy(
                message="select survey from vessel : " + ship_name,
                choices=get_all_survey_names_from_a_ship(ship_name),
            ).execute()

            echosounder = inquirer.select(
                message="select echosounder from survey : " + survey,
                choices=get_all_echosounders_in_a_survey(ship_name, survey),
            ).execute()

            # Get all file names for the selected survey

            file_name = inquirer.select(
                message="select .raw files from survey : " + survey,
                choices=get_all_raw_file_names_from_survey(
                    ship_name, survey, echosounder
                ),
            ).execute()

            operation = inquirer.select(
                message="select operation for " + file_name,
                choices=[
                    "download .raw",
                    "download .nc ",
                    "plot echograms",
                    "run kmeans",
                    "run dbscan",
                ],
            ).execute()

            if operation == "download .raw":
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

            if operation == "plot echograms":
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

            if operation == "run kmeans":
                logger.info(
                    f"Running KMeans on {echosounder} data for {ship_name} in {survey}"
                )
                logger.info(f"This functionality is not yet available")

            if operation == "run dbscan":
                logger.info(
                    f"Running DBScan on {echosounder} data for {ship_name} in {survey}"
                )
                logger.info(f"This functionality is not yet available")

        if mode == "search OMAO vessel data":
            logger.info(f"This functionality is not yet available. ")

        if mode == "authenticate with Google":
            logger.info("Authenticating via Google...")

            logger.info(f"This functionality is not yet available. ")

        if mode == "resources and documentation":
            logger.info("Accessing Resources and Documentation...")



            os.system("cls" if os.name == "nt" else "clear")
            logger.info(                    
                    "\n".join([
                        "   AA-SI Homepage",
                        "   NCEI Website : https://www.ncei.noaa.gov/",
                        "   OMAO Website : https://www.omao.noaa.gov/",
                        "   OST Website : https://www.fisheries.noaa.gov/about/office-science-and-technology",
                        "   AA-SI GitHub : https://github.com/orgs/nmfs-ost/repositories?q=AA",
                        "   AA-SI_aalibrary : https://github.com/nmfs-ost/AA-SI_aalibrary",
                        "   AA-SI_GCPSetup : https://github.com/nmfs-ost/AA-SI_GCPSetup",
                        "   AA-SI_DataRoadMap : https://github.com/nmfs-ost/AA-SI_DataRoadMap",
                        "   AA-SI_KMeans : https://github.com/nmfs-ost/AA-SI_KMeans",
                        "   AA-SI_DBScan : https://github.com/nmfs-ost/AA-SI_DBScan"
                    ])
                    )
            

        
        if mode == "exit":
            os.system("cls" if os.name == "nt" else "clear")
            break


if __name__ == "__main__":
    main()
    os.system("cls" if os.name == "nt" else "clear")
