"""This file contains code pertaining to auxiliary functions related to parsing
through our google storage bucket."""

from typing import List, Union
from difflib import get_close_matches
import boto3
from random import randint

from tqdm import tqdm

# For pytests-sake
if __package__ is None or __package__ == "":
    # uses current directory visibility
    from cloud_utils import (
        get_subdirectories_in_s3_bucket_location,
        create_s3_objs,
        list_all_objects_in_s3_bucket_location,
        check_if_file_exists_in_s3,
    )
    from helpers import normalize_ship_name
else:
    from aalibrary.utils.cloud_utils import (
        get_subdirectories_in_s3_bucket_location,
        create_s3_objs,
        list_all_objects_in_s3_bucket_location,
        check_if_file_exists_in_s3,
    )
    from aalibrary.utils.helpers import normalize_ship_name


def get_all_cruises_in_storage_bucket(): ...


def check_if_cruise_exists_fully_in_storage_bucket(): ...


def get_netcdf_files_from_survey(): ...


if __name__ == "__main__":
    ...
