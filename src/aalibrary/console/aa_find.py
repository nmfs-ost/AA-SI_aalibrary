"""AA-Find: A console application to search and download acoustics data from
NCEI and OMAO."""

import sys
import os
import subprocess
import warnings

from loguru import logger
from InquirerPy import inquirer

from aalibrary.utils.cloud_utils import create_s3_objs
from aalibrary.utils.ncei_utils import (
    get_all_survey_names_from_a_ship,
    get_all_echosounders_in_a_survey,
    get_all_ship_names_in_ncei,
    get_all_raw_file_names_from_survey,
    get_folder_size_from_s3,
    get_file_size_from_s3,
)


def delete_last_line():
    "Use this function to delete the last line in the STDOUT"

    # cursor up one line
    sys.stdout.write("\x1b[1A")
    # delete last line
    sys.stdout.write("\x1b[2K")


warnings.filterwarnings("ignore")

def main():
    """Main interactive console application loop."""

    # ANSI colorized string
    prompt_str = "Select Action:"

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
            s3_client, s3_resource, _ = create_s3_objs()
            print("Fetching NCEI vessel list...")
            all_ship_names_in_ncei = sorted(
                get_all_ship_names_in_ncei(s3_client=s3_client)
            )
            delete_last_line()

            # Ship selection loop
            while True:
                ship_name = inquirer.fuzzy(
                    message="Select NCEI vessel:",
                    choices=["Back"] + all_ship_names_in_ncei,
                ).execute()

                if ship_name == "Back":
                    ship_name = None
                    os.system("cls" if os.name == "nt" else "clear")
                    break
                os.system("cls" if os.name == "nt" else "clear")
                print(f"{ship_name}")

                # Fetch surveys for the selected ship
                print(f"Fetching {ship_name} surveys...")
                all_survey_names_in_ncei = sorted(
                    get_all_survey_names_from_a_ship(
                        ship_name=ship_name, s3_client=s3_client
                    )
                )
                delete_last_line()

                # Survey selection loop
                while True:
                    survey = inquirer.fuzzy(
                        message="Select survey from vessel : " + ship_name,
                        choices=["Back"] + all_survey_names_in_ncei,
                    ).execute()

                    if survey == "Back":
                        survey = None
                        os.system("cls" if os.name == "nt" else "clear")
                        break
                    os.system("cls" if os.name == "nt" else "clear")
                    print(f"{ship_name}|{survey}")

                    # Fetch sonar models for the selected ship
                    print(f"Fetching {survey} sonar models...")
                    all_sonar_model_names_in_ncei = sorted(
                        get_all_echosounders_in_a_survey(
                            ship_name=ship_name,
                            survey_name=survey,
                            s3_client=s3_client,
                        )
                    )
                    delete_last_line()

                    # Sonar model selection loop
                    while True:
                        sonar_model = inquirer.select(
                            message="Select sonar_model from survey : "
                            + survey,
                            choices=["Back"] + all_sonar_model_names_in_ncei,
                        ).execute()

                        if sonar_model == "Back":
                            sonar_model = None
                            os.system("cls" if os.name == "nt" else "clear")
                            print(f"{ship_name}")
                            break
                        os.system("cls" if os.name == "nt" else "clear")
                        print(f"{ship_name}|{survey}|{sonar_model}")

                        # Fetch raw files for the selected sonar model
                        print(f"Fetching {sonar_model} raw files...")
                        all_raw_files_in_ncei = sorted(
                            get_all_raw_file_names_from_survey(
                                ship_name=ship_name,
                                survey_name=survey,
                                echosounder=sonar_model,
                                s3_resource=s3_resource,
                            )
                        )
                        delete_last_line()

                        # Raw file selection loop
                        while True:
                            file_name = inquirer.fuzzy(
                                message="Select .raw files from survey : "
                                + survey,
                                choices=["Back"]
                                + ["Survey Disk Usage"]
                                + all_raw_files_in_ncei,
                            ).execute()

                            if file_name == "Back":
                                file_name = None
                                os.system(
                                    "cls" if os.name == "nt" else "clear"
                                )
                                print(f"{ship_name}|{survey}")
                                break
                            os.system("cls" if os.name == "nt" else "clear")
                            print(
                                f"{ship_name}|{survey}|{sonar_model}|{file_name}"
                            )

                            if file_name == "Survey Disk Usage":
                                x = get_folder_size_from_s3(
                                    folder_prefix=f"data/raw/{ship_name}/{survey}/{sonar_model}/",
                                    s3_resource=s3_resource,
                                )
                                print(
                                    f"File size: {x} b|{x/(1024**2):.2f} MB|{x/(1024**3):.2f} GB"
                                )
                                continue

                            # Raw file operation loop
                            while True:
                                operation = inquirer.select(
                                    message="Select operation for "
                                    + file_name,
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
                                    operation = None
                                    os.system(
                                        "cls" if os.name == "nt" else "clear"
                                    )
                                    print(
                                        f"{ship_name}|{survey}|{sonar_model}"
                                    )
                                    break

                                if operation == "Download .raw":
                                    # Define the folder name
                                    folder_name = (
                                        ship_name
                                        + "_"
                                        + survey
                                        + "_"
                                        + sonar_model
                                        + "_"
                                        + "NCEI"
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
                                        ship_name
                                        + "_"
                                        + survey
                                        + "_"
                                        + sonar_model
                                        + "_"
                                        + "NCEI"
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
                                    logger.info(
                                        "This functionality is not yet available"
                                    )

                                if operation == "Run DBScan":
                                    logger.info(
                                        f"Running DBScan on {sonar_model} data for {ship_name} in {survey}"
                                    )
                                    logger.info(
                                        "This functionality is not yet available"
                                    )

                                if operation == "Check Disk Usage":
                                    _, s3_resource, _ = create_s3_objs()
                                    x = get_file_size_from_s3(
                                        object_key=f"data/raw/{ship_name}/{survey}/{sonar_model}/{file_name}",
                                        s3_resource=s3_resource,
                                    )
                                    print(
                                        f"File size: {x} b|{x/(1024**2):.2f} MB|{x/(1024**3):.2f} GB"
                                    )

        if mode == "Search OMAO Vessel Data":
            logger.info("This functionality is not yet available. ")

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
