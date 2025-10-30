"""For helper functions."""

import os
from typing import List
import json
import logging
import string
import requests


import boto3

from aalibrary import config

# def parse_and_check_file_download_location(file_download_location: str = ""):
#     """Will clean (return a file download location and the file download
#     location directory) and check if the directory even exists in the first
#     place, if not makes it.

#     Args:
#         file_download_location (str, optional): _description_.
#         Defaults to "".
#     """

#     # Get the last directory of the file download location.
#     file_download_location = os.path.normpath(file_download_location)
#     file_download_location.split(os.sep)
#     file_download_location_directory = os.sep.join([
#                        os.path.normpath(file_download_location), file_name])


def get_file_name_from_url(url: str = "") -> str:
    """Extracts the file name from a given storage bucket url. Includes the
    file extension.

    Args:
        url (str, optional): The full url of the storage object.
            Defaults to "".
            Example: "https://noaa-wcsd-pds.s3.amazonaws.com/data/raw/Reuben_La
                      sker/RL2107/EK80/2107RL_CW-D20210813-T220732.raw"

    Returns:
        str: The file name. Example: 2107RL_CW-D20210813-T220732.raw
    """

    return url.split("/")[-1]


def get_file_paths_via_json_link(link: str = ""):
    """This function helps in getting the links from a json request, parsing
    the contents of that url into a json object. The output is a json of the
    filename, and the cloud path link (s3 bucket link).
    Code from: https://www.ngdc.noaa.gov/mgg/wcd/S3_download.html

    Args:
        link (str, optional): The link to the json url. Defaults to "".
    """

    url = requests.get(link, timeout=10)
    text = url.text
    contents = json.loads(text)
    for k in contents.keys():
        print(k)
    for i in contents["features"]:
        file_name = i["attributes"]["FILE_NAME"]
        cloud_path = i["attributes"]["CLOUD_PATH"]
        if cloud_path:
            print(f"{file_name}, {cloud_path}")


def parse_variables_from_ncei_file_url(url: str = ""):
    """Gets the file variables associated with a file url in NCEI.
    File urls in NCEI follow this template:
    data/raw/{ship_name}/{survey_name}/{echosounder}/{file_name}

    NOTE: file_name will include the extension."""

    file_name = get_file_name_from_url(url=url)
    file_type = file_name.split(".")[-1]
    echosounder = url.split("/")[-2]
    survey_name = url.split("/")[-3]
    ship_name = url.split("/")[-4]

    return file_name, file_type, echosounder, survey_name, ship_name


def get_all_ship_objects_from_ncei(
    ship_name: str = "", bucket: boto3.resource = None
) -> List[str]:
    """Gets all of the object keys from a ship from the NCEI database.

    Args:
        ship_name (str, optional): The name of the ship. Must be title-case
            and have spaces substituted for underscores. Defaults to "".
        bucket (boto3.resource, optional): The boto3 bucket resource for the
            bucket that the ship data resides in. Defaults to None.

    Returns:
        List[str]: A list of strings. Each one being an object key (path to
            the object inside of the bucket).
    """

    assert ship_name != "", (
        "Please provide a valid Titlecase",
        " ship_name using underscores as spaces.",
    )
    assert " " not in ship_name, (
        "Please provide a valid Titlecase",
        " ship_name using underscores as spaces.",
    )
    assert bucket is not None, "Please pass in a boto3 bucket object."

    ship_objects = []

    for obj in bucket.objects.filter(Prefix=f"data/raw/{ship_name}"):
        ship_objects.append(obj.key)

    return ship_objects


def get_all_objects_in_survey_from_ncei(
    ship_name: str = "",
    survey_name: str = "",
    s3_bucket: boto3.resource = None,
) -> List[str]:
    """Gets all of the object keys from a ship survey from the NCEI database.

    Args:
        ship_name (str, optional): The name of the ship. Must be title-case
            and have spaces substituted for underscores. Defaults to "".
        survey_name (str, optional): The name of the survey. Must match what
            we have in the NCEI database. Defaults to "".
        s3_bucket (boto3.resource, optional): The boto3 bucket resource for
            the bucket that the ship data resides in. Defaults to None.

    Returns:
        List[str]: A list of strings. Each one being an object key (path to
            the object inside of the bucket).
    """

    assert ship_name != "", (
        "Please provide a valid Titlecase",
        " ship_name using underscores as spaces.",
    )
    assert " " not in ship_name, (
        "Please provide a valid Titlecase",
        " ship_name using underscores as spaces.",
    )
    assert survey_name != "", "Please provide a valid survey name."
    assert s3_bucket is not None, "Please pass in a boto3 bucket object."

    survey_objects = []

    for obj in s3_bucket.objects.filter(
        Prefix=f"data/raw/{ship_name}/{survey_name}"
    ):
        survey_objects.append(obj.key)

    return survey_objects


def create_omao_file_path_from_variables(
    file_name: str = "",
    ship_name: str = "",
    survey_name: str = "",
    echosounder: str = "",
    # year: str = "",
    # month: str = "",
    # date: str = "",
    # hours: str = "",
    # minutes: str = "",
    # seconds: str = "",
):
    if file_name != "":
        azure_url = f"{ship_name}/{survey_name}/{echosounder}/{file_name}"
        return azure_url
    else:
        logging.error("COULD NOT FIND FILE GIVEN THE PARAMETERS.")
        # Here we have to search for the file in s3. Just to see if something
        # exists.
        # partial_file_name = (
        #     f"-D{year}{month}{date}-T{hours}{minutes}{seconds}.{file_type}"
        # )
        # TODO: make sure to check that a raw and idx files both exist.
        raise FileNotFoundError


def create_ncei_url_from_variables(
    file_name: str = "",
    ship_name: str = "",
    survey_name: str = "",
    echosounder: str = "",
    # year: str = "",
    # month: str = "",
    # date: str = "",
    # hours: str = "",
    # minutes: str = "",
    # seconds: str = "",
):
    if file_name != "":
        ncei_url = (
            "https://noaa-wcsd-pds.s3.amazonaws.com/data/raw/"
            f"{ship_name}/{survey_name}/{echosounder}/{file_name}"
        )
        return ncei_url
    else:
        logging.error("COULD NOT FIND FILE GIVEN THE PARAMETERS.")
        # Here we have to search for the file in s3. Just to see if something
        # exists.
        # partial_file_name = (
        #     f"-D{year}{month}{date}-T{hours}{minutes}{seconds}.raw"
        # )
        # TODO: make sure to check that a raw and idx files both exist.
        raise FileNotFoundError


def parse_correct_gcp_storage_bucket_location(
    file_name: str = "",
    file_type: str = "",
    ship_name: str = "",
    survey_name: str = "",
    echosounder: str = "",
    data_source: str = "",
    is_metadata: bool = False,
    is_survey_metadata: bool = False,
    debug: bool = False,
) -> str:
    """Calculates the correct gcp storage location based on data source, file
    type, and if the file is metadata or not.

    Args:
        file_name (str, optional): The file name (includes extension).
            Defaults to "".
        file_type (str, optional): The file type (not include the dot ".").
            Defaults to "".
        ship_name (str, optional): The ship name associated with this survey.
            Defaults to "".
        survey_name (str, optional): The survey name/identifier. Defaults
            to "".
        echosounder (str, optional): The echosounder used to gather the data.
            Defaults to "".
        data_source (str, optional): The source of the data. Can be one of
            ["NCEI", "OMAO"]. Defaults to "".
        is_metadata (bool, optional): Whether or not the file is a metadata
            file. Necessary since files that are considered metadata (metadata
            json, or readmes) are stored in a separate directory. Defaults to
            False.
        is_survey_metadata (bool, optional): Whether or not the file is a
            metadata file associated with a survey. The files are stored at
            the survey level, in the `metadata/` folder. Defaults to False.
        debug (bool, optional): Whether or not to print debug statements.
            Defaults to False.

    Returns:
        str: The correctly parsed GCP storage bucket location.
    """

    assert (
        (is_metadata and is_survey_metadata is False)
        or (is_metadata is False and is_survey_metadata)
        or (is_metadata is False and is_survey_metadata is False)
    ), (
        "Please make sure that only one of `is_metadata` and"
        " `is_survey_metadata` is True. Or you can set both to False."
    )

    # Creating the correct upload location
    if is_survey_metadata:
        gcp_storage_bucket_location = (
            f"{data_source}/{ship_name}/{survey_name}/metadata/{file_name}"
        )
    elif is_metadata:
        gcp_storage_bucket_location = (
            f"{data_source}/{ship_name}/{survey_name}/{echosounder}/metadata/"
        )
        # Figure out if its a raw or idx file (belongs in raw folder)
        if file_type.lower() in config.RAW_DATA_FILE_TYPES:
            gcp_storage_bucket_location = (
                gcp_storage_bucket_location + f"raw/{file_name}.json"
            )
        elif file_type.lower() in config.CONVERTED_DATA_FILE_TYPES:
            gcp_storage_bucket_location = (
                gcp_storage_bucket_location + f"netcdf/{file_name}.json"
            )
    else:
        # Figure out if its a raw or idx file (belongs in raw folder)
        if file_type.lower() in config.RAW_DATA_FILE_TYPES:
            gcp_storage_bucket_location = (
                f"{data_source}/{ship_name}/"
                f"{survey_name}/{echosounder}/data/raw/{file_name}"
            )
        elif file_type.lower() in config.CONVERTED_DATA_FILE_TYPES:
            gcp_storage_bucket_location = (
                f"{data_source}/{ship_name}/"
                f"{survey_name}/{echosounder}/data/netcdf/{file_name}"
            )

    if debug:
        logging.debug(
            "PARSED GCP_STORAGE_BUCKET_LOCATION: %s",
            gcp_storage_bucket_location,
        )

    return gcp_storage_bucket_location


def get_netcdf_gcp_location_from_raw_gcp_location(
    gcp_storage_bucket_location: str = "",
):
    """Gets the netcdf location of a raw file within GCP."""

    gcp_storage_bucket_location = gcp_storage_bucket_location.replace(
        "/raw/", "/netcdf/"
    )
    # get rid of file extension and replace with netcdf
    netcdf_gcp_storage_bucket_location = (
        ".".join(gcp_storage_bucket_location.split(".")[:-1]) + ".nc"
    )

    return netcdf_gcp_storage_bucket_location


def normalize_ship_name(ship_name: str = "") -> str:
    """Normalizes a ship's name. This is necessary for creating a deterministic
    file structure within our GCP storage bucket.
    The ship name is returned as a Title_Cased_And_Snake_Cased ship name, with
    no punctuation.
    Ex. `HENRY B. BIGELOW` will return `Henry_B_Bigelow`

    Args:
        ship_name (str, optional): The ship name string. Defaults to "".

    Returns:
        str: The formatted and normalized version of the ship name.
    """

    # Lower case the string
    ship_name = ship_name.lower()
    # Un-normalize (replace `_` with ` ` to help further processing)
    # In the edge-case that users include an underscore.
    ship_name = ship_name.replace("_", " ")
    # Remove all punctuation.
    ship_name = "".join(
        [char for char in ship_name if char not in string.punctuation]
    )
    # Title-case it
    ship_name = ship_name.title()
    # Snake-case it
    ship_name = ship_name.replace(" ", "_")

    return ship_name


def check_for_assertion_errors(**kwargs):
    """Checks for errors in the kwargs provided."""

    if "file_name" in kwargs:
        assert kwargs["file_name"] != "", (
            "Please provide a valid file name with the file extension"
            " (ex. `2107RL_CW-D20210813-T220732.raw`)"
        )
    if "file_type" in kwargs:
        assert kwargs["file_type"] != "", "Please provide a valid file type."
        assert kwargs["file_type"] in config.VALID_FILETYPES, (
            "Please provide a valid file type (extension) "
            f"from the following: {config.VALID_FILETYPES}"
        )
    if "ship_name" in kwargs:
        assert kwargs["ship_name"] != "", (
            "Please provide a valid ship name "
            "(Title_Case_With_Underscores_As_Spaces)."
        )
    if "survey_name" in kwargs:
        assert (
            kwargs["survey_name"] != ""
        ), "Please provide a valid survey name."
    if "echosounder" in kwargs:
        assert (
            kwargs["echosounder"] != ""
        ), "Please provide a valid echosounder."
        assert kwargs["echosounder"] in config.VALID_ECHOSOUNDERS, (
            "Please provide a valid echosounder from the "
            f"following: {config.VALID_ECHOSOUNDERS}"
        )
    if "data_source" in kwargs:
        assert kwargs["data_source"] != "", (
            "Please provide a valid data source from the "
            f"following: {config.VALID_DATA_SOURCES}"
        )
        assert kwargs["data_source"] in config.VALID_DATA_SOURCES, (
            "Please provide a valid data source from the "
            f"following: {config.VALID_DATA_SOURCES}"
        )
    if "file_download_directory" in kwargs:
        assert (
            kwargs["file_download_directory"] != ""
        ), "Please provide a valid file download directory."
        assert os.path.isdir(kwargs["file_download_directory"]), (
            f"File download location `{kwargs['file_download_directory']}` is"
            " not found to be a valid dir, please reformat it."
        )
    if "gcp_bucket" in kwargs:
        assert kwargs["gcp_bucket"] is not None, (
            "Please provide a gcp_bucket object with"
            " `utils.cloud_utils.setup_gcp_storage()`"
        )
    if "directory" in kwargs:
        assert kwargs["directory"] != "", "Please provide a valid directory."
        assert os.path.isdir(kwargs["directory"]), (
            f"Directory location `{kwargs['directory']}` is not found to be a"
            " valid dir, please reformat it."
        )
    if "data_lake_directory_client" in kwargs:
        assert kwargs["data_lake_directory_client"] is not None, (
            f"The data lake directory client cannot be a"
            f" {type(kwargs['data_lake_directory_client'])} object. It needs "
            "to be of the type `DataLakeDirectoryClient`."
        )


def create_azure_config_file(download_directory: str = ""):
    """Creates an empty config file for azure storage keys.

    Args:
        download_directory (str, optional): The directory to store the
            azure config file. Defaults to "".
    """

    assert (
        download_directory != ""
    ), "Please provide a valid download directory."
    download_directory = os.path.normpath(download_directory)
    assert os.path.isdir(download_directory), (
        f"Directory location `{download_directory}` is not found to be a"
        " valid dir, please reformat it."
    )

    azure_config_file_path = os.path.join(
        download_directory, "azure_config.ini"
    )

    empty_config_str = """[DEFAULT]
azure_storage_account_name = 
azure_storage_account_key = 
azure_account_url = 
azure_connection_string = """

    with open(
        azure_config_file_path, "w", encoding="utf-8"
    ) as azure_config_file:
        azure_config_file.write(empty_config_str)

    print(
        f"Please fill out the azure config file at: {azure_config_file_path}"
    )
    return azure_config_file_path


if __name__ == "__main__":
    # print(string.punctuation)
    # print(normalize_ship_name("Reuben Lasker"))
    create_azure_config_file(download_directory="./test_data_dir/")
