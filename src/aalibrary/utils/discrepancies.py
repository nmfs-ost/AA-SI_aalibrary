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

import cloud_utils
from aalibrary.utils.ncei_utils import get_file_size_from_s3


def compare_local_cruise_files_to_cloud(
    local_cruise_file_path: str = "",
    ship_name: str = "",
    survey_name: str = "",
    echosounder: str = "",
):
    # Create vars for use later
    s3_client, s3_resource, s3_bucket = cloud_utils.create_s3_objs()

    all_raw_file_paths = glob.glob(local_cruise_file_path + "/*.raw")
    all_idx_file_paths = glob.glob(local_cruise_file_path + "/*.idx")
    all_bot_file_paths = glob.glob(local_cruise_file_path + "/*.bot")
    # Check file numbers & types
    num_raw_files = len(all_raw_file_paths)
    num_idx_files = len(all_idx_file_paths)
    num_bot_files = len(all_bot_file_paths)
    # Get file names along with file paths
    # [(local_file_path, file_name_with_extension), (...]
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

    for local_file_path, file_name in all_raw_file_paths:
        s3_object_key = (
            f"data/raw/{ship_name}/{survey_name}/{echosounder}/{file_name}"
        )
        # Get file size for object key
        s3_file_size = get_file_size_from_s3(
            object_key=s3_object_key, s3_resource=s3_resource
        )
        # Get checksum for object key
        s3_checksum = get_checksum_sha256_from_s3(
            object_key=s3_object_key, s3_resource=s3_resource
        )

    # Get all file(s) in s3 cruise object
    s3_object_key = (
        f"data/raw/{ship_name}/{survey_name}/{echosounder}/{file_name}"
    )


def get_local_file_size(local_file_path):
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
    s3_client, s3_resource, s3_bucket = cloud_utils.create_s3_objs()
    print(
        get_local_sha256_checksum(
            local_file_path=rf"C:\Users\hannan.khan\Desktop\repos\AA-SI_aalibrary\test_data_dir\2107RL_CW-D20211001-T132449.raw"
        )
    )
