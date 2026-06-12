"""This script is used to upload local Cruisepack database files to Google
Cloud Storage (GCS)."""

import sys
import os
from pathlib import Path
import platform
from typing import Tuple

from google.cloud import storage
import traceback

from InquirerPy import inquirer
from InquirerPy.base import Choice

GCP_PROJECT_IDS = ["ggn-nmfs-aa-dev-1", "ggn-nmfs-aa-prod-1"]
GCS_BUCKET_NAMES = ["ggn-nmfs-aa-dev-1-data", "ggn-nmfs-aa-prod-1-data"]
SCIENCE_CENTERS = ["AFSC", "NEFSC", "NWFSC", "OST", "PIFSC", "SEFSC", "SWFSC"]


def print_header():
    """Prints a clean text banner for the CLI."""
    print("=" * 50)
    print("               CRUISEPACK MIGRATOR               ")
    print("=" * 50)
    print("  This script will automatically find all ")
    print("  `cruiseData.sqlite` files on your computer,")
    print("  upload them to GCS, and then load them into BQ.")
    print("-" * 50 + "\n")


def search_computer_files(search_pattern, debug=False):
    """
    Searches for files/folders matching the pattern starting from a specific
    directory.
    """

    curr_os = platform.system()
    if curr_os == "Windows":
        start_dir = "C:\\"
    elif curr_os in ["Linux", "Darwin"]:
        start_dir = "/"
    else:
        start_dir = "/"

    print("Finding Cruisepack on your computer...")
    if debug:
        print(f"Starting directory: '{start_dir}'")
        print(f"Search pattern: '{search_pattern}'")
    path = Path(start_dir)
    matching_paths = []

    # rglob("*") searches everything recursively
    # Use .rglob(search_pattern) to filter immediately, e.g., "*.txt"
    try:
        for item in path.rglob(search_pattern):
            try:
                # Print the absolute path of the found item
                # print(item.resolve())
                matching_paths.append(item.resolve())
            except (PermissionError, FileNotFoundError):
                continue
    except PermissionError:
        print(f"Permission denied for the base directory: '{start_dir}'")

    if not matching_paths:
        raise FileNotFoundError(
            f"No items found matching '{search_pattern}' in '{start_dir}'."
        )
    else:
        # Keep only the directories.
        matching_paths = [x for x in matching_paths if x.is_dir()]

    return matching_paths


def get_database_folder_paths(debug=False):
    """
    Gets the absolute paths of all folders named "database" that are located
    within any folder named "cruise_pack_*" on the computer."""

    cruise_pack_paths = search_computer_files(
        search_pattern="cruise_pack_*", debug=debug
    )
    cruise_pack_paths.extend(search_computer_files(
        search_pattern="CruisePack_*", debug=debug
    ))
    cruise_pack_paths.extend(search_computer_files(
        search_pattern="packager_*", debug=debug
    ))
    database_folder_paths = []
    for cruise_pack_path in cruise_pack_paths:
        cruise_pack_path = Path(cruise_pack_path)
        for item in cruise_pack_path.rglob("*"):
            try:
                # Print the absolute path of the found item
                # print(item.resolve())
                if item.is_dir() and "database" in item.name.lower():
                    database_folder_paths.append(item.resolve())
            except (PermissionError, FileNotFoundError):
                continue
    if debug:
        print(f"Found {len(database_folder_paths)} database folders:")
    return database_folder_paths


def find_local_cruisepack_sqlite_files(debug=False):
    """This function takes care of looking through the local hard drive and
    finding all the cruiseData.sqlite files that are located within any
    cruise_pack_* folders. It returns a list of the absolute paths to these
    files."""
    # 1. Get database folder paths.
    database_folder_paths = get_database_folder_paths(debug=debug)
    if debug:
        print(f"Found {len(database_folder_paths)} database folders:")

    # 2. Get the cruiseData.sqlite file path from each database folder.
    cruise_data_sqlite_paths = []
    for database_folder_path in database_folder_paths:
        cruise_data_sqlite_path = database_folder_path / "cruiseData.sqlite"
        package_data_sqlite_path = database_folder_path / "packageData.sqlite"
        if cruise_data_sqlite_path.exists():
            cruise_data_sqlite_paths.append(cruise_data_sqlite_path)
        if package_data_sqlite_path.exists():
            cruise_data_sqlite_paths.append(package_data_sqlite_path)
    print(f"Found {len(cruise_data_sqlite_paths)} `cruiseData.sqlite` files.")
    return cruise_data_sqlite_paths


def setup_gcp_storage_objs(
    project_id: str = None, gcp_bucket_name: str = None, verbose: bool = False
) -> Tuple[storage.Client, str, storage.Client.bucket]:
    """Sets up Google Cloud Platform storage objects for use in accessing and
    modifying storage buckets.

    Args:
        project_id (str, optional): The project id that the gcp instance
            belongs to. Defaults to os.environ["AALIBRARY_GCP_PROJECT_ID"]
            which is "ggn-nmfs-aa-dev-1" by default but can be changed via
            `aalibrary.config.use_gcp_prod()`.
        gcp_bucket_name (str, optional): The name of the exact bucket you want
            to access. Defaults to os.environ["AALIBRARY_GCP_BUCKET_NAME"]
            which is "ggn-nmfs-aa-dev-1-data" by default but can be changed via
            `aalibrary.config.use_gcp_prod()`.
        verbose (bool, optional): Whether or not to notify the user of the
            bucket being created via print statement.

    Returns:
        Tuple[storage.Client, str, storage.Client.bucket]: The storage client,
            followed by the GCP bucket name (str) and then the actual bucket
            object itself (which will be executing the commands used in this
            api).
    """
    gcp_stor_client = storage.Client(project=project_id)

    gcp_bucket = gcp_stor_client.bucket(gcp_bucket_name)

    if verbose:
        print(
            f"Using GCP Project `{project_id}` and "
            f"GCP Storage Bucket `{gcp_bucket_name}`"
        )

    return (gcp_stor_client, gcp_bucket_name, gcp_bucket)


def upload_file_to_gcp_bucket(
    bucket: storage.Client.bucket = None,
    blob_file_path: str = "",
    local_file_path: str = "",
    debug: bool = False,
):
    """Uploads a file to the blob storage bucket.

    Args:
        bucket (storage.Client.bucket): The bucket object used for uploading.
        blob_file_path (str): The blob's file path.
            Ex. "data/itds/logs/execute_code_files/temp.csv"
            NOTE: This must include the file name as well as the extension.
        local_file_path (str): The local file path you wish to upload to the
            blob.
        debug (bool): Whether or not to print debug statements.
    """

    blob = bucket.blob(blob_file_path, chunk_size=1024 * 1024 * 1)
    # Upload a new blob
    try:
        blob.upload_from_filename(local_file_path)
        if debug:
            print(f"New data uploaded to {blob.name}")
    except Exception:
        print(traceback.format_exc())
        raise


def main():
    debug = False
    print_header()
    # Get the persons name for personalized file name in GCS.
    name = inquirer.text(
        message="Enter your name (so you can find your files in GCS):",
    ).execute()
    name = (
        name.lower().strip().replace(" ", "_")
    )  # Replace spaces with underscores.

    # Set correct GCP project and bucket names based on user input.
    gcp_project_choices = [Choice(id, name=id) for id in GCP_PROJECT_IDS]
    gcp_project_id = inquirer.select(
        message="Select the GCP project to upload to:",
        choices=gcp_project_choices,
        default="ggn-nmfs-aa-dev-1",
    ).execute()

    # Set correct bucket name based on project name.
    gcs_bucket_name = GCS_BUCKET_NAMES[GCP_PROJECT_IDS.index(gcp_project_id)]

    # Set correct science center.
    science_center_choices = [
        Choice(name, name=name) for name in SCIENCE_CENTERS
    ]
    science_center = inquirer.select(
        message="Select the science center you belong to:",
        choices=science_center_choices,
        default="AFSC",
    ).execute()

    # print(f"\nSelected GCP project: {gcp_project_id}")
    # print(f"Selected GCS bucket: {gcs_bucket_name}")
    # print(f"Selected science center: {science_center}")

    # Find local cruiseData.sqlite files.
    cruise_data_sqlite_paths = find_local_cruisepack_sqlite_files(debug=debug)

    # Create bucket object to use for uploading.
    gcp_stor_client, gcs_bucket_name, gcp_bucket = setup_gcp_storage_objs(
        project_id=gcp_project_id, gcp_bucket_name=gcs_bucket_name
    )

    # Upload cruiseData.sqlite files to GCS.
    for idx, path in enumerate(cruise_data_sqlite_paths):
        print(f"Uploading '{path}' to GCS bucket '{gcs_bucket_name}'...")
        file_name = f"{science_center}_{name}_{idx+1}_{path.name}"
        print(f"\tFile name: '{file_name}'")
        gcp_location = f"cruisepack/{science_center}/{file_name}"
        print(f"\tGCS location: '{gcp_location}'")
        upload_file_to_gcp_bucket(
            bucket=gcp_bucket,
            blob_file_path=gcp_location,
            local_file_path=str(path),
        )
        print(f"Finished uploading '{path}' to '{gcp_location}'.")


if __name__ == "__main__":
    main()