"""For helper functions."""

from typing import List
import requests
import json

import boto3

from aalibrary.ingestion import get_file_name_from_url


# def parse_and_check_file_download_location(file_download_location: str = ""):
#     """Will clean (return a file download location and the file download location directory)
#     and check if the directory even exists in the first place, if not makes it.

#     Args:
#         file_download_location (str, optional): _description_. Defaults to "".
#     """

#     # Get the last directory of the file download location.
#     file_download_location = os.path.normpath(file_download_location) # normalize the path
#     file_download_location.split(os.sep)
#     file_download_location_directory = os.sep.join([os.path.normpath(file_download_location), file_name])


def get_file_paths_via_json_link(link: str = ""):
    """This function helps in getting the links from a json request, parsing
    the contents of that url into a json object. The output is a json of the
    filename, and the cloud path link (s3 bucket link).
    Code from: https://www.ngdc.noaa.gov/mgg/wcd/S3_download.html

    Args:
        link (str, optional): The link to the json url. Defaults to "".
    """

    url = requests.get(link)
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
        ship_name (str, optional): The name of the ship. Must be title-case and have
            spaces substituted for underscores. Defaults to "".
        bucket (boto3.resource, optional): The boto3 bucket resource for the bucket
            that the ship data resides in. Defaults to None.

    Returns:
        List[str]: A list of strings. Each one being an object key (path to the object
            inside of the bucket).
    """

    assert (
        ship_name != ""
    ), "Please provide a valid Titlecase ship_name using underscores as spaces."
    assert (
        " " not in ship_name
    ), "Please provide a valid Titlecase ship_name using underscores as spaces."
    assert bucket is not None, "Please pass in a boto3 bucket object."

    ship_objects = []

    for object in bucket.objects.filter(Prefix=f"data/raw/{ship_name}"):
        ship_objects.append(object.key)

    return ship_objects


def get_all_objects_in_survey_from_ncei(
    ship_name: str = "", survey_name: str = "", s3_bucket: boto3.resource = None
) -> List[str]:
    """Gets all of the object keys from a ship survey from the NCEI database.

    Args:
        ship_name (str, optional): The name of the ship. Must be title-case and have
            spaces substituted for underscores. Defaults to "".
        survey_name (str, optional): The name of the survey. Must match what we have
            in the NCEI database. Defaults to "".
        s3_bucket (boto3.resource, optional): The boto3 bucket resource for the bucket
            that the ship data resides in. Defaults to None.

    Returns:
        List[str]: A list of strings. Each one being an object key (path to the object
            inside of the bucket).
    """

    assert (
        ship_name != ""
    ), "Please provide a valid Titlecase ship_name using underscores as spaces."
    assert (
        " " not in ship_name
    ), "Please provide a valid Titlecase ship_name using underscores as spaces."
    assert survey_name != "", "Please provide a valid survey name."
    assert s3_bucket is not None, "Please pass in a boto3 bucket object."

    survey_objects = []

    for object in s3_bucket.objects.filter(
        Prefix=f"data/raw/{ship_name}/{survey_name}"
    ):
        survey_objects.append(object.key)

    return survey_objects
