"""This file contains code pertaining to auxiliary functions related to parsing
through NCEI's s3 bucket."""

import os
from typing import List, Union
from difflib import get_close_matches
from random import randint
import logging

import boto3
from tqdm import tqdm

# For pytests-sake
if __package__ is None or __package__ == "":
    # uses current directory visibility
    from cloud_utils import (
        get_subdirectories_in_s3_bucket_location,
        create_s3_objs,
        list_all_objects_in_s3_bucket_location,
        check_if_file_exists_in_s3,
        get_object_key_for_s3,
    )
    from helpers import normalize_ship_name, get_file_name_from_url
else:
    from aalibrary.utils.cloud_utils import (
        get_subdirectories_in_s3_bucket_location,
        create_s3_objs,
        list_all_objects_in_s3_bucket_location,
        check_if_file_exists_in_s3,
        get_object_key_for_s3,
    )
    from aalibrary.utils.helpers import (
        normalize_ship_name,
        get_file_name_from_url,
    )


def get_all_ship_names_in_ncei(
    normalize: bool = False,
    s3_client: boto3.client = None,
    return_full_paths: bool = False,
):
    """Gets all of the ship names from NCEI. This is based on all of the
    folders listed under the `data/raw/` prefix.

    Args:
        normalize (bool, optional): Whether or not to normalize the ship_name
            attribute to how GCP stores it. Defaults to False.
        s3_client (boto3.client, optional): The client used to perform this
            operation. Defaults to None, but creates a client for you instead.
        return_full_paths (bool, optional): Whether or not you want a full
            path from bucket root to the subdirectory returned. Set to false
            if you only want the subdirectory names listed. Defaults to False.
    """

    # Create client objects if they dont exist.
    if s3_client is None:
        s3_client, _, _ = create_s3_objs()

    # Get the initial subdirs
    prefix = "data/raw/"
    subdirs = get_subdirectories_in_s3_bucket_location(
        prefix=prefix, s3_client=s3_client, return_full_paths=return_full_paths
    )
    if normalize:
        subdirs = [normalize_ship_name(ship_name=subdir) for subdir in subdirs]
    return subdirs


def get_all_surveys_in_ncei(
    s3_client: boto3.client = None, return_full_paths: bool = False
) -> List[str]:
    """Gets a list of all of the possible survey names from NCEI.

    Args:
        s3_client (boto3.client, optional): The client used to perform this
            operation. Defaults to None, but creates a client for you instead.
        return_full_paths (bool, optional): Whether or not you want a full
            path from bucket root to the subdirectory returned. Set to false
            if you only want the subdirectory names listed. Defaults to False.
    Returns:
        List[str]: A list of strings, each being the survey name. Whether
            these are full paths or just folder names are specified by the
            `return_full_paths` parameter.
    """

    # Create client objects if they dont exist.
    if s3_client is None:
        s3_client, _, _ = create_s3_objs()

    # First we get all of the prefixes for each ship.
    all_ship_prefixes = get_all_ship_names_in_ncei(
        normalize=False, s3_client=s3_client, return_full_paths=True
    )
    all_surveys = set()
    for ship_prefix in tqdm(all_ship_prefixes, desc="Getting Surveys"):
        # Get a list of all of this ship's survey names
        all_ship_survey_names = get_subdirectories_in_s3_bucket_location(
            prefix=ship_prefix,
            s3_client=s3_client,
            return_full_paths=return_full_paths,
            bucket_name="noaa-wcsd-pds",
        )
        all_surveys.update(all_ship_survey_names)
    return list(all_surveys)


def get_all_survey_names_from_a_ship(
    ship_name: str = "",
    s3_client: boto3.client = None,
    return_full_paths: bool = False,
) -> List[str]:
    """Gets a list of all of the survey names that exist under a ship name.

    Args:
        ship_name (str, optional): The ship's name you want to get all surveys
            from. Defaults to None.
            NOTE: The ship's name MUST be spelled exactly as it is in NCEI. Use
            the `get_all_ship_names_in_ncei` function to see all possible NCEI
            ship names.
        s3_client (boto3.client, optional): The client used to perform this
            operation. Defaults to None, but creates a client for you instead.
        return_full_paths (bool, optional): Whether or not you want a full
            path from bucket root to the subdirectory returned. Set to false
            if you only want the subdirectory names listed. Defaults to False.
    Returns:
        List[str]: A list of strings, each being the survey name. Whether
            these are full paths or just folder names are specified by the
            `return_full_paths` parameter.
    """
    # Create client objects if they dont exist.
    if s3_client is None:
        s3_client, _, _ = create_s3_objs()

    # Make sure the ship name is valid
    all_ship_names = get_all_ship_names_in_ncei(
        normalize=False, s3_client=s3_client, return_full_paths=False
    )
    if ship_name not in all_ship_names:
        close_matches = get_close_matches(
            ship_name, all_ship_names, n=3, cutoff=0.6
        )
    assert ship_name in all_ship_names, (
        f"The ship name provided `{ship_name}` "
        "needs to be spelled exactly like in NCEI.\n"
        "Use the `get_all_ship_names_in_ncei` function to see all possible "
        "NCEI ship names.\n"
        f"Did you mean one of these possible ship names?\n{close_matches}"
    )

    ship_prefix = f"data/raw/{ship_name}/"
    all_surveys = set()
    # Get a list of all of this ship's survey names
    all_ship_survey_names = get_subdirectories_in_s3_bucket_location(
        prefix=ship_prefix,
        s3_client=s3_client,
        return_full_paths=return_full_paths,
        bucket_name="noaa-wcsd-pds",
    )
    all_surveys.update(all_ship_survey_names)
    return list(all_surveys)


def get_all_echosounders_in_a_survey(
    ship_name: str = "",
    survey_name: str = "",
    s3_client: boto3.client = None,
    return_full_paths: bool = False,
) -> List[str]:
    """Gets all of the echosounders in a particular survey from NCEI.

    Args:
        ship_name (str, optional): The ship's name you want to get all surveys
            from. Defaults to None.
            NOTE: The ship's name MUST be spelled exactly as it is in NCEI. Use
            the `get_all_ship_names_in_ncei` function to see all possible NCEI
            ship names.
        survey_name (str, optional): The survey name exactly as it is in NCEI.
            Defaults to "".
        s3_client (boto3.client, optional): The client used to perform this
            operation. Defaults to None, but creates a client for you instead.
        return_full_paths (bool, optional): Whether or not you want a full
            path from bucket root to the subdirectory returned. Set to false
            if you only want the subdirectory names listed. Defaults to False.

    Returns:
        List[str]: A list of strings, each being the echosounder name. Whether
            these are full paths or just folder names are specified by the
            `return_full_paths` parameter.
    """

    survey_prefix = f"data/raw/{ship_name}/{survey_name}/"
    all_survey_folder_names = get_subdirectories_in_s3_bucket_location(
        prefix=survey_prefix,
        s3_client=s3_client,
        return_full_paths=return_full_paths,
        bucket_name="noaa-wcsd-pds",
    )
    # Get echosounder folders by ignoring the other metadata folders
    all_echosounders = []
    for folder_name in all_survey_folder_names:
        if (
            ("calibration" not in folder_name.lower())
            and ("metadata" not in folder_name.lower())
            and ("json" not in folder_name.lower())
            and ("doc" not in folder_name.lower())
        ):
            all_echosounders.append(folder_name)

    return all_echosounders


def get_all_echosounders_that_exist_in_ncei(
    s3_client: boto3.client = None,
) -> List[str]:
    """Gets a list of all possible echosounders from NCEI.

    Args:
        s3_client (boto3.client, optional): The client used to perform this
            operation. Defaults to None, but creates a client for you instead.

    Returns:
        List[str]: A list of strings, each being the echosounder name. Whether
            these are full paths or just folder names are specified by the
            `return_full_paths` parameter.
    """

    # Create client objects if they dont exist.
    if s3_client is None:
        s3_client, _, _ = create_s3_objs()

    # First we get all of the prefixes for each survey to exist in NCEI.
    all_survey_prefixes = get_all_surveys_in_ncei(
        s3_client=s3_client, return_full_paths=True
    )
    all_echosounders = set()
    for survey_prefix in tqdm(
        all_survey_prefixes, desc="Getting Echosounders"
    ):
        # Remove trailing `/`
        survey_prefix = survey_prefix.strip("/")
        survey_name = survey_prefix.split("/")[-1]
        ship_name = survey_prefix.split("/")[-2]
        survey_echosounders = get_all_echosounders_in_a_survey(
            ship_name=ship_name,
            survey_name=survey_name,
            s3_client=s3_client,
            return_full_paths=False,
        )
        all_echosounders.update(survey_echosounders)

    return list(all_echosounders)


def get_all_file_names_from_survey(
    ship_name: str = "",
    survey_name: str = "",
    s3_resource: boto3.resource = None,
    return_full_paths: bool = False,
) -> List[str]:
    """Gets all of the file names from a particular NCEI survey.

    Args:
        ship_name (str, optional): The ship's name you want to get all surveys
            from. Defaults to None.
            NOTE: The ship's name MUST be spelled exactly as it is in NCEI. Use
            the `get_all_ship_names_in_ncei` function to see all possible NCEI
            ship names.
        survey_name (str, optional): The survey name exactly as it is in NCEI.
            Defaults to "".
        s3_resource (boto3.resource, optional): The resource used to perform
            this operation. Defaults to None, but creates a client for you
            instead.
        return_full_paths (bool, optional): Whether or not you want a full
            path from bucket root to the subdirectory returned. Set to false
            if you only want the subdirectory names listed. Defaults to False.

    Returns:
        List[str]: A list of strings, each being the echosounder name. Whether
            these are full paths or just folder names are specified by the
            `return_full_paths` parameter.
    """

    survey_prefix = f"data/raw/{ship_name}/{survey_name}/"
    all_files = list_all_objects_in_s3_bucket_location(
        prefix=survey_prefix,
        s3_resource=s3_resource,
        return_full_paths=return_full_paths,
    )
    return all_files


def get_all_file_names_in_a_surveys_echosounder_folder(
    ship_name: str = "",
    survey_name: str = "",
    echosounder: str = "",
    s3_resource: boto3.resource = None,
    return_full_paths: bool = False,
) -> List[str]:
    """Gets all of the file names from a particular NCEI survey's echosounder
    folder.

    Args:
        ship_name (str, optional): The ship's name you want to get all surveys
            from. Defaults to None.
            NOTE: The ship's name MUST be spelled exactly as it is in NCEI. Use
            the `get_all_ship_names_in_ncei` function to see all possible NCEI
            ship names.
        survey_name (str, optional): The survey name exactly as it is in NCEI.
            Defaults to "".
        echosounder (str, optional): The echosounder used. Defaults to "".
        s3_resource (boto3.resource, optional): The resource used to perform
            this operation. Defaults to None, but creates a client for you
            instead.
        return_full_paths (bool, optional): Whether or not you want a full
            path from bucket root to the subdirectory returned. Set to false
            if you only want the subdirectory names listed. Defaults to False.

    Returns:
        List[str]: A list of strings, each being the file name. Whether
            these are full paths or just file names are specified by the
            `return_full_paths` parameter.
    """

    survey_prefix = f"data/raw/{ship_name}/{survey_name}/{echosounder}/"
    all_files = list_all_objects_in_s3_bucket_location(
        prefix=survey_prefix,
        s3_resource=s3_resource,
        return_full_paths=return_full_paths,
    )
    return all_files


def get_all_raw_file_names_from_survey(
    ship_name: str = "",
    survey_name: str = "",
    echosounder: str = "",
    s3_resource: boto3.resource = None,
    return_full_paths: bool = False,
) -> List[str]:
    """Gets all of the file names from a particular NCEI survey.

    Args:
        ship_name (str, optional): The ship's name you want to get all surveys
            from. Defaults to None.
            NOTE: The ship's name MUST be spelled exactly as it is in NCEI. Use
            the `get_all_ship_names_in_ncei` function to see all possible NCEI
            ship names.
        survey_name (str, optional): The survey name exactly as it is in NCEI.
            Defaults to "".
        echosounder (str, optional): The echosounder used. Defaults to "".
        s3_resource (boto3.resource, optional): The resource used to perform
            this operation. Defaults to None, but creates a client for you
            instead.
        return_full_paths (bool, optional): Whether or not you want a full
            path from bucket root to the subdirectory returned. Set to false
            if you only want the subdirectory names listed. Defaults to False.

    Returns:
        List[str]: A list of strings, each being the raw file name. Whether
            these are full paths or just folder names are specified by the
            `return_full_paths` parameter.
    """

    survey_prefix = f"data/raw/{ship_name}/{survey_name}/{echosounder}/"
    all_files = list_all_objects_in_s3_bucket_location(
        prefix=survey_prefix,
        s3_resource=s3_resource,
        return_full_paths=return_full_paths,
    )
    all_files = [file for file in all_files if file.endswith(".raw")]
    return all_files


def get_random_raw_file_from_ncei() -> List[str]:
    """Creates a test raw file for NCEI. This is used for testing purposes
    only. Retries automatically if an error occurs.

    Returns:
        List[str]: A list object with strings denoting each parameter required
            for creating a raw file object.
            Ex. [
                random_ship_name,
                random_survey_name,
                random_echosounder,
                random_raw_file,
            ]
    """

    try:
        # Get all of the ship names
        all_ship_names = get_all_ship_names_in_ncei(
            normalize=False, return_full_paths=False
        )
        random_ship_name = all_ship_names[randint(0, len(all_ship_names) - 1)]
        # Get all of the surveys for this ship
        all_surveys_for_this_ship = get_all_survey_names_from_a_ship(
            ship_name=random_ship_name, return_full_paths=False
        )
        random_survey_name = all_surveys_for_this_ship[
            randint(0, len(all_surveys_for_this_ship) - 1)
        ]
        # Get all of the echosounders in this survey
        all_echosounders_for_this_survey = get_all_echosounders_in_a_survey(
            ship_name=random_ship_name,
            survey_name=random_survey_name,
            return_full_paths=False,
        )
        random_echosounder = all_echosounders_for_this_survey[
            randint(0, len(all_echosounders_for_this_survey) - 1)
        ]
        # Get all of the raw files in this echosounder
        all_raw_files_in_echosounder = get_all_raw_file_names_from_survey(
            ship_name=random_ship_name,
            survey_name=random_survey_name,
            echosounder=random_echosounder,
            return_full_paths=False,
        )
        random_raw_file = all_raw_files_in_echosounder[
            randint(0, len(all_raw_files_in_echosounder) - 1)
        ]

        return [
            random_ship_name,
            random_survey_name,
            random_echosounder,
            random_raw_file,
        ]
    except Exception:
        return get_random_raw_file_from_ncei()


# def get_random_raw_file_from_ncei_with_search_parameter(
#     ship_name: str = "",
#     survey_name: str = "",
#     echosounder: str = "",
# ) -> List[str]:
#     """Gets a test raw file's parameters for NCEI. This is used for testing
#     purposes only. You can specify whichever parameters you would like. Retries
#     automatically if an error occurs or no files are found.
#     NOTE: Will keep retrying indefinitely until a valid raw file is found. If
#     you notice that it is taking too long, try specifying more parameters.

#     Args:
#         ship_name (str, optional): The ship's name you want to get all surveys
#             from. Defaults to None.
#             NOTE: The ship's name MUST be spelled exactly as it is in NCEI. Use
#             the `get_all_ship_names_in_ncei` function to see all possible NCEI
#             ship names.
#         survey_name (str, optional): The survey name exactly as it is in NCEI.
#             Defaults to "".
#         echosounder (str, optional): The echosounder used. Defaults to "".

#     Returns:
#         List[str]: A list object with strings denoting each parameter required
#             for creating a raw file object.
#             Ex. [
#                 random_ship_name,
#                 random_survey_name,
#                 random_echosounder,
#                 random_raw_file,
#             ]
#     """

#     try:
#         # Get all of the ship names
#         all_ship_names = get_all_ship_names_in_ncei(
#             normalize=False, return_full_paths=False
#         )
#         # If ship_name is not provided, get a random ship_name
#         if ship_name == "":
#             random_ship_name = all_ship_names[
#                 randint(0, len(all_ship_names) - 1)
#             ]
#         else:
#             random_ship_name = get_closest_ncei_formatted_ship_name(
#                 ship_name=ship_name
#             )
#         # Get all of the surveys for this ship
#         all_surveys_for_this_ship = get_all_survey_names_from_a_ship(
#             ship_name=random_ship_name, return_full_paths=False
#         )
#         # If survey_name is not provided, get a random survey_name
#         if survey_name == "":
#             random_survey_name = all_surveys_for_this_ship[
#                 randint(0, len(all_surveys_for_this_ship) - 1)
#             ]
#         else:
#             random_survey_name = survey_name
#         # Get all of the echosounders in this survey
#         all_echosounders_for_this_survey = get_all_echosounders_in_a_survey(
#             ship_name=random_ship_name,
#             survey_name=random_survey_name,
#             return_full_paths=False,
#         )
#         # If echosounder is not provided, get a random echosounder
#         if echosounder == "":
#             random_echosounder = all_echosounders_for_this_survey[
#                 randint(0, len(all_echosounders_for_this_survey) - 1)
#             ]
#         else:
#             random_echosounder = echosounder
#         # Get all of the raw files in this echosounder
#         all_raw_files_in_echosounder = get_all_raw_file_names_from_survey(
#             ship_name=random_ship_name,
#             survey_name=random_survey_name,
#             echosounder=random_echosounder,
#             return_full_paths=False,
#         )
#         random_raw_file = all_raw_files_in_echosounder[
#             randint(0, len(all_raw_files_in_echosounder) - 1)
#         ]

#         return [
#             random_ship_name,
#             random_survey_name,
#             random_echosounder,
#             random_raw_file,
#         ]
#     except Exception:
#         return get_random_raw_file_from_ncei_with_search_parameter(
#             ship_name=ship_name,
#             survey_name=survey_name,
#             echosounder=echosounder,
#         )


def search_ncei_objects_for_string(search_param: str = "") -> List[str]:
    """Searches NCEI for object keys that contain a particular string. This
    string can be anything, such as an echosounder name, ship name,
    survey name, or even a partial file name.
    NOTE: This function takes a long time to run, as it has to search through
    ALL of NCEI's objects.
    NOTE: Use a folder name as the search_param to get all object keys that
    contain that folder name. (e.g. '/EK80/')

    Args:
        search_param (str, optional): The string to search for. Defaults to "".

    Returns:
        List[str]: A list of strings, each being an object key that contains
            the search parameter.
    """

    s3_client, _, _ = create_s3_objs()
    paginator = s3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket="noaa-wcsd-pds")
    matching_object_keys = []
    # Vpcs[?contains(`["vpc-blabla1", "vpc-blabla2"]`, VpcId)].OtherKey
    # objects = page_iterator.search(f"
    # Contents[?contains(Key, `{search_param}`) && ends_with(Key, `.raw`)][]")
    objects = page_iterator.search(
        f"Contents[?contains(Key, `{search_param}`)][]"
    )
    # objects = page_iterator.search("Contents[?ends_with(Key, `.csv`)][]")
    for item in objects:
        print(item["Key"])
        matching_object_keys.append(item["Key"])
    return matching_object_keys


def search_ncei_file_objects_for_string(
    search_param: str = "", file_extension: str = ".raw"
) -> List[str]:
    """Searches NCEI for a file type's object keys that contain a particular
    string. This string can be anything, such as an echosounder name,
    ship name, survey name, or even a partial file name. The file type can be
    specified by the file_extension parameter.
    NOTE: This function takes a long time to run, as it has to search through
    ALL of NCEI's objects.

    Args:
        search_param (str, optional): The string to search for. Defaults to "".
        file_extension (str, optional): The file extension to filter results
            by. Defaults to ".raw".

    Returns:
        List[str]: A list of strings, each being an object key that contains
            the search parameter.
    """

    s3_client, _, _ = create_s3_objs()
    paginator = s3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket="noaa-wcsd-pds")
    matching_object_keys = []
    objects = page_iterator.search(
        f"Contents[?contains(Key, `{search_param}`)"
        f" && ends_with(Key, `{file_extension}`)][]"
    )
    for item in objects:
        print(item["Key"])
        matching_object_keys.append(item["Key"])
    return matching_object_keys


def get_echosounder_from_raw_file(
    file_name: str = "",
    ship_name: str = "",
    survey_name: str = "",
    echosounders: List[str] = None,
    s3_client: boto3.client = None,
    s3_resource: boto3.resource = None,
    s3_bucket: boto3.resource = None,
):
    """Gets the echosounder used for a particular raw file."""

    if (s3_client is None) or (s3_resource is None) or (s3_bucket is None):
        s3_client, s3_resource, s3_bucket = create_s3_objs()

    if echosounders is None:
        echosounders = get_all_echosounders_in_a_survey(
            ship_name=ship_name,
            survey_name=survey_name,
            s3_client=s3_client,
            return_full_paths=False,
        )

    for echosounder in echosounders:
        raw_file_location = (
            f"data/raw/{ship_name}/{survey_name}/{echosounder}/{file_name}"
        )
        raw_file_exists = check_if_file_exists_in_s3(
            object_key=raw_file_location,
            s3_resource=s3_resource,
            s3_bucket_name=s3_bucket.name,
        )
        if raw_file_exists:
            return echosounder

    return ValueError("An echosounder could not be found for this raw file.")


def check_if_tugboat_metadata_json_exists_in_survey(
    ship_name: str = "",
    survey_name: str = "",
    s3_bucket: boto3.resource = None,
) -> Union[str, None]:
    """Checks whether a Tugboat metadata JSON file exists within a survey.
    Returns the file's object key or None if it does not exist.

    Args:
        ship_name (str, optional): The ship's name you want to get all surveys
            from. Defaults to None.
            NOTE: The ship's name MUST be spelled exactly as it is in NCEI. Use
            the `get_all_ship_names_in_ncei` function to see all possible NCEI
            ship names.
        survey_name (str, optional): The survey name exactly as it is in NCEI.
            Defaults to "".
        s3_bucket (boto3.resource, optional): The bucket resource object.
            Defaults to None.
    Returns:
        Union[str, None]: Returns the file's object key string or None if it
            does not exist.
    """

    # Find all metadata files within the metadata/ folder in NCEI
    all_metadata_obj_keys = list_all_objects_in_s3_bucket_location(
        prefix=f"data/raw/{ship_name}/{survey_name}/metadata",
        s3_resource=s3_bucket,
    )

    for obj_key, file_name in all_metadata_obj_keys:
        # Handle for main metadata file for upload to BigQuery.
        if file_name.endswith("metadata.json"):
            return obj_key

    return None


def get_closest_ncei_formatted_ship_name(
    ship_name: str = "",
    s3_client: boto3.client = None,
) -> Union[str, None]:
    """Gets the closest NCEI formatted ship name to the given ship name.
    NOTE: Only use if the `data_source`=="NCEI".

    Args:
        ship_name (str, optional): The ship name to search the closest match
            for.
            Defaults to "".
        s3_client (boto3.client, optional): The client used to perform this
            operation. Defaults to None, but creates a client for you instead.

    Returns:
        Union[str, None]: The NCEI formatted ship name or None, if none
            matched.
    """

    # Create client objects if they dont exist.
    if s3_client is None:
        s3_client, _, _ = create_s3_objs()

    all_ship_names = get_all_ship_names_in_ncei(
        normalize=False, s3_client=s3_client, return_full_paths=False
    )
    close_matches = get_close_matches(
        ship_name, all_ship_names, n=3, cutoff=0.85
    )
    if len(close_matches) >= 1:
        return close_matches[0]
    else:
        return None


def get_all_metadata_files_in_survey(
    ship_name: str = "",
    survey_name: str = "",
    s3_resource: boto3.resource = None,
    return_full_paths: bool = False,
) -> List[str]:
    """Gets all of the metadata file names from a particular NCEI survey.

    Args:
        ship_name (str, optional): The ship's name you want to get all surveys
            from. Defaults to None.
            NOTE: The ship's name MUST be spelled exactly as it is in NCEI. Use
            the `get_all_ship_names_in_ncei` function to see all possible NCEI
            ship names.
        survey_name (str, optional): The survey name exactly as it is in NCEI.
            Defaults to "".
        s3_resource (boto3.resource, optional): The resource used to perform
            this operation. Defaults to None, but creates a client for you
            instead.
        return_full_paths (bool, optional): Whether or not you want a full
            path from bucket root to the subdirectory returned. Set to false
            if you only want the subdirectory names listed. Defaults to False.

    Returns:
        List[str]: A list of strings, each being the metadata file name.
            Whether these are full paths or just folder names are specified by
            the `return_full_paths` parameter. Returns empty list '[]' if no
            metadata files are present.
    """

    survey_prefix = f"data/raw/{ship_name}/{survey_name}/metadata/"
    all_metadata_files = list_all_objects_in_s3_bucket_location(
        prefix=survey_prefix,
        s3_resource=s3_resource,
        return_full_paths=return_full_paths,
    )
    return all_metadata_files


def get_file_size_from_s3(object_key, s3_resource):
    """Gets the file size of an object in s3."""
    obj = s3_resource.Object("noaa-wcsd-pds", object_key)
    file_size = obj.content_length
    return file_size


def get_folder_size_from_s3(
    folder_prefix: str, s3_resource: boto3.resource
) -> int:
    """Gets the folder size in bytes from S3.

    Args:
        folder_prefix (str): The object key prefix of the folder in S3.
        s3_resource (boto3.resource, optional): The resource used to perform
            this operation. Defaults to None, but creates a client for you
            instead.

    Returns:
        int: The total size of the folder in bytes.
    """
    if s3_resource is None:
        _, s3_resource, _ = create_s3_objs()

    # Initialize total size
    total_size = 0

    # Get all objects' keys in the folder
    all_files_object_keys = list_all_objects_in_s3_bucket_location(
        prefix=folder_prefix,
        s3_resource=s3_resource,
        return_full_paths=True,
    )

    for file_object_key in tqdm(
        all_files_object_keys, desc="Calculating Folder Size"
    ):
        total_size += get_file_size_from_s3(
            object_key=file_object_key, s3_resource=s3_resource
        )

    return total_size


def get_checksum_sha256_from_s3(object_key, s3_resource):
    """Gets the SHA-256 checksum of the s3 object."""
    obj = s3_resource.Object("noaa-wcsd-pds", object_key)
    checksum = obj.checksum_sha256
    return checksum


def download_specific_folder_from_ncei(
    folder_prefix: str = "", download_directory: str = "", debug: bool = False
):
    """Downloads a specific folder and all of its contents from NCEI to a local
    directory.

    Args:
        folder_prefix (str, optional): The folder's path in the s3 bucket.
            Ex. 'data/raw/Reuben_Lasker/'
            Defaults to "".
        download_directory (str, optional): The directory you want to download
            the folder and all of its contents to. Defaults to "".
        debug (bool, optional): Whether or not to print debug information.
            Defaults to False.
    """

    if not folder_prefix.endswith("/"):
        folder_prefix += "/"

    assert (download_directory is not None) and (
        download_directory != ""
    ), "You must provide a download_directory to download the folder to."

    if debug:
        logging.debug("FORMATTED DOWNLOAD DIRECTORY: %s", download_directory)

    # Get all s3 objects for the survey
    print(f"GETTING ALL S3 OBJECTS FOR FOLDER `{folder_prefix}`...")
    _, s3_resource, _ = create_s3_objs()
    s3_objects = list_all_objects_in_s3_bucket_location(
        prefix=folder_prefix,
        s3_resource=s3_resource,
        return_full_paths=True,
    )
    print(f"FOUND {len(s3_objects)} FILES.")

    subdirs = set()
    # Get the subfolders from object keys
    for s3_object in s3_objects:
        # Skip folders
        if s3_object.endswith("/"):
            continue
        # Get the subfolder structure from the object key
        subfolder_key = os.sep.join(
            s3_object.replace("data/raw/", "").split("/")[:-1]
        )
        subdirs.add(subfolder_key)
    for subdir in subdirs:
        os.makedirs(os.sep.join([download_directory, subdir]), exist_ok=True)

    # Create the directory if it doesn't exist.
    if not os.path.isdir(download_directory):
        print(f"CREATING download_directory `{download_directory}`")
        os.makedirs(download_directory, exist_ok=True)
    # normalize the path
    download_directory = os.path.normpath(download_directory)
    print("CREATED DOWNLOAD SUBDIRECTORIES.")

    for idx, object_key in enumerate(tqdm(s3_objects, desc="Downloading")):
        file_name = object_key.split("/")[-1]
        local_object_path = object_key.replace("data/raw/", "")
        download_location = os.path.normpath(
            os.sep.join([download_directory, local_object_path])
        )
        download_single_file_from_aws(
            file_url=object_key, download_location=download_location
        )
    print(f"DOWNLOAD COMPLETE {os.path.abspath(download_directory)}.")


def download_single_file_from_aws(
    file_url: str = "",
    download_location: str = "",
):
    """Safely downloads a file from AWS storage bucket, aka the NCEI
    repository.

    Args:
        file_url (str, optional): The file url. Defaults to "".
        download_location (str, optional): The local download location for the
            file. Defaults to "".
    """

    try:
        _, s3_resource, s3_bucket = create_s3_objs()
    except Exception as e:
        logging.error("CANNOT ESTABLISH CONNECTION TO S3 BUCKET..\n{%s}", e)
        raise

    # We replace the beginning of common file paths
    file_url = get_object_key_for_s3(file_url=file_url)
    file_name = get_file_name_from_url(file_url)

    # Check if the file exists in s3
    file_exists = check_if_file_exists_in_s3(
        object_key=file_url,
        s3_resource=s3_resource,
        s3_bucket_name=s3_bucket.name,
    )

    if file_exists:
        # Finally download the file.
        try:
            logging.info("DOWNLOADING `%s`...", file_name)
            s3_bucket.download_file(file_url, download_location)
            logging.info(
                "DOWNLOADED `%s` TO `%s`", file_name, download_location
            )
        except Exception as e:
            logging.error(
                "ERROR DOWNLOADING FILE `%s` DUE TO\n%s", file_name, e
            )
            raise
    else:
        logging.error(
            "FILE %s DOES NOT EXIST IN NCEI S3 BUCKET. SKIPPING...", file_name
        )


if __name__ == "__main__":
    s3_client, s3_resource, _ = create_s3_objs()
    download_specific_folder_from_ncei(
        folder_prefix="data/raw/Reuben_Lasker/RL2107/metadata/",
        download_directory="./RL2107_metadata_test/",
        debug=True,
    )
    # x = get_folder_size_from_s3(
    #     folder_prefix="data/raw/Reuben_Lasker/RL2107/metadata/",
    #     s3_resource=s3_resource,
    # )
    # print(f"Folder size: {x} bytes")
    # subdirs = get_all_survey_names_from_a_ship(
    #   ship_name="Reuben Lasker", s3_client=s3_client, return_full_paths=False
    # )
    # print(subdirs)
    # echos = get_all_echosounders_in_a_survey(
    #     ship_name="Reuben_Lasker",
    #     survey_name="RL2107",
    #     s3_client=s3_client,
    #     return_full_paths=False,
    # )
    # print(echos)

    # all_echos = get_all_echosounders_that_exist_in_NCEI(s3_client=s3_client)
    # print(all_echos)

    # md_files = get_all_metadata_files_in_survey(
    #     ship_name="Alaska_Knight",
    #     survey_name="EBS11AK",
    #     s3_resource=s3_resource,
    #     return_full_paths=True,
    # )
    # print(md_files)

    # all_files = get_all_file_names_from_survey(
    #     ship_name="Reuben_Lasker",
    #     survey_name="RL2107",
    #     s3_resource=s3_resource,
    #     return_full_paths=True,
    # )
    # print(all_files)

    # print(get_random_raw_file_from_ncei())
    # print(search_ncei_objects_for_string(search_param="EK500"))
