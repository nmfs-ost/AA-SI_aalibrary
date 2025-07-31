"""This file contains code pertaining to auxiliary functions related to parsing
through our google storage bucket."""

from typing import List, Union
from difflib import get_close_matches
import boto3
from random import randint

from google.cloud import storage
from tqdm import tqdm

# For pytests-sake
if __package__ is None or __package__ == "":
    # uses current directory visibility
    from cloud_utils import (
        setup_gcp_storage_objs,
        list_all_folders_in_gcp_bucket_location,
        get_subdirectories_in_s3_bucket_location,
        create_s3_objs,
        list_all_objects_in_s3_bucket_location,
        check_if_file_exists_in_s3,
    )
    from helpers import normalize_ship_name
else:
    from aalibrary.utils.cloud_utils import (
        setup_gcp_storage_objs,
        list_all_folders_in_gcp_bucket_location,
        get_subdirectories_in_s3_bucket_location,
        create_s3_objs,
        list_all_objects_in_s3_bucket_location,
        check_if_file_exists_in_s3,
    )
    from aalibrary.utils.helpers import normalize_ship_name


def get_all_ship_names_in_gcp_bucket(
    project_id: str = "ggn-nmfs-aa-dev-1",
    gcp_bucket_name: str = "",
    gcp_bucket: storage.Client.bucket = None,
    return_full_paths: bool = False,
) -> List[str]:
    """Gets all of the ship names within a GCP storage bucket.

    Args:
        project_id (str, optional): The GCP project ID that the storage bucket resides in.
            Defaults to "ggn-nmfs-aa-dev-1".
        gcp_bucket_name (str, optional): The GCP storage bucket name. Defaults to "".
        gcp_bucket (storage.Client.bucket, optional): The GCP storage bucket client object.
            If none, one will be created for you based on the `project_id` and
            `gcp_bucket_name`. Defaults to None.
        return_full_paths (bool, optional): Whether or not you want a full
            path from bucket root to the subdirectory returned. Set to false
            if you only want the subdirectory names listed. Defaults to False.
            NOTE: You can set this parameter to `True` if you would like to see which folders
            contain which ships.
            For example: Reuben Lasker can have data coming from both OMAO and local upload HDD.
            It will look like: {'OMAO/Reuben_Lasker/', 'HDD/Reuben_Lasker/'}

    Returns:
        List[str]: A list of strings containing the ship names.
    """

    if gcp_bucket is None:
        gcp_stor_client, gcp_bucket_name, gcp_bucket = setup_gcp_storage_objs()
    # Get the initial subdirs
    prefixes = ["HDD/", "NCEI/", "OMAO/", "TEST/"]
    all_ship_names = set()
    for prefix in prefixes:
        ship_names = list_all_folders_in_gcp_bucket_location(
            location=prefix, gcp_bucket=gcp_bucket, return_full_paths=return_full_paths
        )
        all_ship_names.update(ship_names)

    return list(all_ship_names)


def get_all_surveys_in_storage_bucket(): ...


def get_all_survey_names_from_a_ship_in_storage_bucket(): ...


def get_all_echosounders_in_a_survey_in_storage_bucket(): ...


def get_all_echosounders_that_exist_in_storage_bucket(): ...


def get_all_file_names_from_survey_in_storage_bucket(): ...


def get_all_raw_file_names_from_survey_in_storage_bucket(): ...


def get_random_raw_file_from_storage_bucket(): ...


def get_echosounder_from_raw_file_in_storage_bucket(): ...


def check_if_tugboat_metadata_json_exists_in_survey(): ...


def get_closest_gcp_formatted_ship_name(): ...


def get_all_metadata_files_in_survey_in_storage_bucket(): ...


def check_if_cruise_exists_fully_in_storage_bucket(): ...


def get_netcdf_files_from_survey(): ...


if __name__ == "__main__":
    all_ship_names = get_all_ship_names_in_gcp_bucket()
    print(all_ship_names)
