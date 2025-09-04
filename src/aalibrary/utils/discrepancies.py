"""This file is used to identify discrepancies between what data exists on
local versus what exists on the cloud. It considers the following things when
comparing:
* Number of files per cruise
* File Name/Types
* File Sizes
* Checksum
"""

import hashlib
import os
import glob

from aalibrary.utils.cloud_utils import (
    check_if_file_exists_in_s3,
    create_s3_objs,
)
from aalibrary.utils.ncei_utils import (
    get_file_size_from_s3,
    get_all_file_names_in_a_surveys_echosounder_folder,
)


def compare_local_cruise_files_to_cloud(
    local_cruise_file_path: str = "",
    ship_name: str = "",
    survey_name: str = "",
    echosounder: str = "",
):
    """Compares the locally stored cruise files (per echosounder) to what
    exists on the cloud by number of files, file sizes, and
    checksums. Reports any discrepancies in the console.

    Args:
        local_cruise_file_path (str, optional): The folder path for the locally
            stored cruise data. Defaults to "".
        ship_name (str, optional): The ship name that the cruise falls under.
            Defaults to "".
        survey_name (str, optional): The survey/cruise name. Defaults to "".
        echosounder (str, optional): The specific echosounder you want to
            check. Defaults to "".
    """

    # Create vars for use later
    _, s3_resource, _ = create_s3_objs()

    # Get all local files paths in cruise directory
    all_raw_file_paths = glob.glob(local_cruise_file_path + "/*.raw")
    all_idx_file_paths = glob.glob(local_cruise_file_path + "/*.idx")
    all_bot_file_paths = glob.glob(local_cruise_file_path + "/*.bot")
    # Check file numbers & types
    num_local_raw_files = len(all_raw_file_paths)
    num_local_idx_files = len(all_idx_file_paths)
    num_local_bot_files = len(all_bot_file_paths)
    num_local_files = (
        num_local_raw_files + num_local_idx_files + num_local_bot_files
    )
    # Get file names along with file paths
    # [(local_file_path, file_name_with_extension), (...)]
    all_raw_file_paths = [
        (file_path, file_path.split("/")[-1])
        for file_path in all_raw_file_paths
    ]
    all_idx_file_paths = [
        (file_path, file_path.split("/")[-1])
        for file_path in all_idx_file_paths
    ]
    all_bot_file_paths = [
        (file_path, file_path.split("/")[-1])
        for file_path in all_bot_file_paths
    ]

    # Compare number of files in cruise, local vs cloud
    num_files_in_s3 = get_all_file_names_in_a_surveys_echosounder_folder(
        ship_name=ship_name,
        survey_name=survey_name,
        echosounder=echosounder,
        s3_resource=s3_resource,
        return_full_paths=False,
    )
    if num_files_in_s3 == (num_local_files):
        print(
            "NUMBER OF FILES MATCH FOR"
            f" {ship_name}/{survey_name}/{echosounder}"
        )
    elif num_files_in_s3 != (num_local_files):
        print(
            "NUMBER OF FILES DO NOT MATCH FOR"
            f" {ship_name}/{survey_name}/{echosounder}"
        )
        print(
            f"NUMBER OF FILES IN S3: {num_files_in_s3} | NUMBER OF LOCAL "
            f"FILES: {num_local_files}"
        )

    # Go through each local file, and compare file existence, size, checksum
    for local_file_path, file_name in all_raw_file_paths:
        # Create s3 object key
        s3_object_key = (
            f"data/raw/{ship_name}/{survey_name}/{echosounder}/{file_name}"
        )
        # Get existence of file in s3
        file_exists_in_s3 = check_if_file_exists_in_s3(
            object_key=s3_object_key,
            s3_resource=s3_resource,
            s3_bucket_name="noaa-wcsd-pds",
        )
        # If file exists in s3, get size and checksum
        if file_exists_in_s3:
            # Get file size for s3 object key
            s3_file_size = get_file_size_from_s3(
                object_key=s3_object_key, s3_resource=s3_resource
            )
            # Get checksum for object key
            s3_checksum = get_checksum_sha256_from_s3(
                object_key=s3_object_key, s3_resource=s3_resource
            )

        # Get local file size
        local_file_size = get_local_file_size(local_file_path)
        # Get local file checksum
        local_file_checksum = get_local_sha256_checksum(local_file_path)

        # Compare existence
        if not file_exists_in_s3:
            print(
                f"LOCAL FILE {local_file_path} DOES NOT EXIST IN S3:"
                f" {s3_object_key}"
            )
        elif file_exists_in_s3:
            # Compare file sizes
            if local_file_size != s3_file_size:
                print(
                    f"FILE SIZE MISMATCH FOR {local_file_path} | LOCAL: "
                    f"{local_file_size} | S3: {s3_file_size}"
                )
            # Compare checksums
            if local_file_checksum != s3_checksum:
                print(
                    f"CHECKSUM MISMATCH FOR {local_file_path} | LOCAL: "
                    f"{local_file_checksum} | S3: {s3_checksum}"
                )


def get_local_file_size(local_file_path: str) -> int:
    """Gets the size of a local file in bytes.

    Args:
        local_file_path (str): The local file path.

    Returns:
        int: The size of the file in bytes.
    """
    return os.path.getsize(local_file_path)


def get_checksum_sha256_from_s3(object_key, s3_resource):
    """Gets the SHA-256 checksum of the s3 object."""
    obj = s3_resource.Object("noaa-wcsd-pds", object_key)
    checksum = obj.checksum_sha256
    return checksum


def get_local_sha256_checksum(local_file_path, chunk_size=65536) -> str:
    """
    Calculates the SHA256 checksum of a file.

    Args:
        local_file_path (str): The path to the file.
        chunk_size (int): The size of chunks to read the file in (in bytes).
                          Larger chunks can be more efficient for large files.

    Returns:
        str: The SHA256 checksum of the file as a hexadecimal string.
    """

    sha256_hash = hashlib.sha256()
    try:
        with open(local_file_path, "rb") as f:
            # Read the file in chunks to handle large files efficiently
            for chunk in iter(lambda: f.read(chunk_size), b""):
                sha256_hash.update(chunk)
        return sha256_hash.hexdigest()
    except FileNotFoundError:
        return "File not found."
    except Exception as e:
        return f"An error occurred: {e}"


if __name__ == "__main__":
    s3_client, s3_resource, s3_bucket = create_s3_objs()
    print(
        get_local_sha256_checksum(
            local_file_path=(
                r"C:\Users\hannan.khan\Desktop\repos\AA-SI_aa"
                r"library\test_data_dir\2107RL_CW-D20211001-T132449.raw"
            )
        )
    )
