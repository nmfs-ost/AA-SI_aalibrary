from aalibrary.ices_ship_names import get_all_ices_ship_names
from aalibrary.utils.cloud_utils import create_s3_objs
from aalibrary.utils.ncei_utils import (
    get_all_survey_names_from_a_ship,
    get_all_echosounders_in_a_survey,
    get_all_ship_names_in_ncei,
    get_all_file_names_from_survey,
    get_all_raw_file_names_from_survey,
    get_folder_size_from_s3,
    get_file_size_from_s3,
)
from aalibrary.utils.discrepancies import get_file_size_from_s3
from InquirerPy import inquirer
import subprocess, os
from loguru import logger
import warnings
import webbrowser


warnings.filterwarnings("ignore")
# ANSI colorized string
prompt_str = "Select Action:"


def main():
    os.system("cls" if os.name == "nt" else "clear")
    while True:
        # os.system("cls" if os.name == "nt" else "clear")
        mode = inquirer.select(
            message=prompt_str,
            choices=[
                "Search NCEI Vessel Data",
                "Search OMAO Vessel Data",
                "Authenticate with Google",
                "View Resources & Documentation",
                "Exit Application",
            ],
            default="Search NCEI Vessel Data",
        ).execute()
        os.system("cls" if os.name == "nt" else "clear")
        if mode == "Search NCEI Vessel Data":

            while True:

                ship_name = inquirer.fuzzy(
                    message="select NCEI vessel:",
                    choices=["Back"] + get_all_ship_names_in_ncei(),
                ).execute()

                if ship_name == "Back":
                    ship_name = None
                    break

                while True:

                    survey = inquirer.fuzzy(
                        message="Select survey from vessel : " + ship_name,
                        choices=["Back"] + get_all_survey_names_from_a_ship(ship_name),
                    ).execute()

                    if survey == "Back":
                        survey = None
                        break

                    sonar_model = inquirer.select(
                        message="Select sonar_model from survey : " + survey,
                        choices=["Back"]
                        + get_all_echosounders_in_a_survey(ship_name, survey),
                    ).execute()

                    if sonar_model == "Back":
                        continue

                    # Get all file names for the selected survey

                    file_name = inquirer.fuzzy(
                        message="Select .raw files from survey : " + survey,
                        choices=["Back"]
                        + ["Survey Disk Usage"]
                        + get_all_raw_file_names_from_survey(
                            ship_name, survey, sonar_model
                        ),
                    ).execute()

                    if file_name == "Back":
                        continue

                    if file_name == "Survey Disk Usage":
                        s3_client, s3_resource, _ = create_s3_objs()
                        x = get_folder_size_from_s3(
                            folder_prefix=f"data/raw/{ship_name}/{survey}/{sonar_model}/",
                            s3_resource=s3_resource,
                        )
                        print(f"Folder size: {x} bytes")
                        print(f"Folder size: {x / (1024 ** 2):.2f} MB")
                        print(f"Folder size: {x / (1024 ** 3):.2f} GB")
                        continue

                    operation = inquirer.select(
                        message="Select operation for " + file_name,
                        choices=["Back"]
                        + [
                            "Download .raw",
                            "Download .nc ",
                            "Plot Echogram(s)",
                            "Run KMeans",
                            "Run DBScan",
                            "Check Disk Usage",
                        ],
                    ).execute()

                    if operation == "Back":
                        continue

                    if operation == "Download .raw":
                        # Define the folder name
                        folder_name = (
                            ship_name + "_" + survey + "_" + sonar_model + "_" + "NCEI"
                        )

                        # Create the full path using '.'
                        path = os.path.join(".", folder_name)

                        # Make the directory (does nothing if it already exists)
                        os.makedirs(path, exist_ok=True)
                        logger.info(
                            f"Downloading {sonar_model} data for {ship_name} in {survey} to {folder_name}"
                        )
                        logger.debug(
                            f"Running command: aa-raw --file_name {file_name} --ship_name {ship_name} --survey_name {survey} --sonar_model {sonar_model} --file_download_directory {folder_name} from directory: {os.getcwd()} from the environment: {subprocess.run(['which', 'python'], capture_output=True, text=True).stdout.strip()}"
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
                                "--sonar_model",
                                sonar_model,
                                "--data_source",
                                "NCEI",
                                "--file_download_directory",
                                folder_name,
                            ]
                        )

                    if operation == "Plot Echograms":
                        # Define the folder name
                        folder_name = (
                            ship_name + "_" + survey + "_" + sonar_model + "_" + "NCEI"
                        )

                        # Create the full path using '.'
                        path = os.path.join(".", folder_name)

                        # Make the directory (does nothing if it already exists)
                        os.makedirs(path, exist_ok=True)
                        logger.info(
                            f"Plotting {sonar_model} data for {ship_name} in {survey} to {folder_name}"
                        )
                        logger.debug(
                            f"Running command: aa-plot {file_name} --sonar_model {sonar_model} --output-file {folder_name}/echogram.png from directory: {os.getcwd()} from the environment: {subprocess.run(['which', 'python'], capture_output=True, text=True).stdout.strip()}"
                        )
                        subprocess.run(
                            [
                                "aa-plot",
                                file_name,
                                "--sonar_model",
                                sonar_model,
                                "--output-file",
                                f"{folder_name}/echogram.png",
                            ]
                        )

                    if operation == "Run KMeans":
                        logger.info(
                            f"Running KMeans on {sonar_model} data for {ship_name} in {survey}"
                        )
                        logger.info(f"This functionality is not yet available")

                    if operation == "Run DBScan":
                        logger.info(
                            f"Running DBScan on {sonar_model} data for {ship_name} in {survey}"
                        )
                        logger.info(f"This functionality is not yet available")

        if mode == "Search OMAO Vessel Data":
            logger.info(f"This functionality is not yet available. ")

        if mode == "Authenticate with Google":
            logger.info("Authenticating via Google...")

            commands = [
                "gcloud auth login",
                "gcloud auth application-default login",
                "gcloud config set account {ACCOUNT}",
                "gcloud config set project ggn-nmfs-aa-dev-1",
            ]

            for cmd in commands:
                subprocess.run(cmd, shell=True, check=True)

            # logger.info(f"This functionality is not yet available. ")

        if mode == "View Resources & Documentation":
            logger.info("Accessing Resources and Documentation...")

            os.system("cls" if os.name == "nt" else "clear")
            logger.info(
                "\n".join(
                    [
                        "   AA-SI Homepage",
                        "   NCEI Website : https://www.ncei.noaa.gov/",
                        "   OMAO Website : https://www.omao.noaa.gov/",
                        "   OST Website : https://www.fisheries.noaa.gov/about/office-science-and-technology",
                        "   AA-SI GitHub : https://github.com/orgs/nmfs-ost/repositories?q=AA",
                        "   AA-SI_aalibrary : https://github.com/nmfs-ost/AA-SI_aalibrary",
                        "   AA-SI_GCPSetup : https://github.com/nmfs-ost/AA-SI_GCPSetup",
                        "   AA-SI_DataRoadMap : https://github.com/nmfs-ost/AA-SI_DataRoadMap",
                        "   AA-SI_KMeans : https://github.com/nmfs-ost/AA-SI_KMeans",
                        "   AA-SI_DBScan : https://github.com/nmfs-ost/AA-SI_DBScan",
                    ]
                )
            )

        if mode == "Exit Application":
            os.system("cls" if os.name == "nt" else "clear")
            break


if __name__ == "__main__":
    main()
    os.system("cls" if os.name == "nt" else "clear")
