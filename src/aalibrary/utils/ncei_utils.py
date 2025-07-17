"""This file contains code pertaining to auxiliary functions related to parsing
through NCEI's s3 bucket."""

from typing import List
from difflib import get_close_matches
import boto3
from random import randint

# For pytests-sake
if __package__ is None or __package__ == "":
    # uses current directory visibility
    from cloud_utils import (
        get_subdirectories_in_s3_bucket_location,
        create_s3_objs,
        list_all_objects_in_s3_bucket_location,
    )
    from helpers import normalize_ship_name
else:
    from aalibrary.utils.cloud_utils import (
        get_subdirectories_in_s3_bucket_location,
        create_s3_objs,
        list_all_objects_in_s3_bucket_location,
    )
    from aalibrary.utils.helpers import normalize_ship_name


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
    for ship_prefix in all_ship_prefixes:
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


def get_all_echosounders_that_exist_in_NCEI(
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
    for survey_prefix in all_survey_prefixes:
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


def check_if_metadata_json_exists_in_survey(): ...


if __name__ == "__main__":
    s3_client, s3_resource, _ = create_s3_objs()
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

    # all_files = get_all_file_names_from_survey(
    #     ship_name="Reuben_Lasker",
    #     survey_name="RL2107",
    #     s3_resource=s3_resource,
    #     return_full_paths=True,
    # )
    # print(all_files)

    print(get_random_raw_file_from_ncei())
