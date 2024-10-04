"""This script contains functions used to ingest Active Acoustics data into GCP
from various sources such as AWS buckets and Azure Data Lake."""

import sys
import os
import json
import subprocess
import requests
import time
from typing import List
import logging

import boto3
from botocore import UNSIGNED
from botocore.client import Config
from google.cloud import bigquery, storage

import utils

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
    for i in contents['features']:
        file_name = i['attributes']['FILE_NAME']
        cloud_path = i['attributes']['CLOUD_PATH']
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


def get_file_name_from_url(url: str = ""):
    """Extracts the file name from a given storage bucket url. Includes the file
    extension.

    Args:
        url (str, optional): The full url of the storage object. Defaults to "".
            Example: https://noaa-wcsd-pds.s3.amazonaws.com/data/raw/Reuben_Lasker/RL2107/EK80/2107RL_CW-D20210813-T220732.raw

    Returns:
        str: The file name. Example: 2107RL_CW-D20210813-T220732.raw
    """

    return url.split("/")[-1]


def download_single_file_from_aws(bucket: str = "noaa-wcsd-pds",
                                  file_url: str = "https://noaa-wcsd-pds.s3.amazonaws.com/data/raw/Reuben_Lasker/RL2107/EK80/2107RL_CW-D20210706-T172335.idx",
                                  download_location: str = ""):
    """Downloads a file from AWS storage bucket, aka the NCEI repository."""
    
    try:
        s3_resource, bucket = utils.create_s3_objs()
    except Exception as e:
        print(f"Cannot establish connection to s3 bucket..\n{e}")
    
    # We replace the beginning of common file paths
    file_url = file_url.replace("https://noaa-wcsd-pds.s3.amazonaws.com/", '')
    file_url = file_url.replace("s3://noaa-wcsd-pds/", '')
    file_name = get_file_name_from_url(file_url)

    # Finally download the file.
    try:
        bucket.download_file(file_url, file_name)
        print(f"Downloaded: {file_name} to {download_location}")
    except Exception as e:
        print(f"Error downloading file {file_name}.\n{e}")


def download_single_survey_from_ncei(ship_name: str = "",
                                     survey_name: str = "",
                                     echosounder: str = "",
                                     survey_folder_url: str = "",
                                     download_location: str = "",
                                     bucket: boto3.resource = None):
    """Downloads a single survey from NCEI to the `download_location`.

    Args:
        ship_name (str, optional): The name of the ship. Must be title-case and have
            spaces substituted for underscores. Defaults to "".
        bucket (boto3.resource, optional): The boto3 bucket resource for the bucket
            that the ship data resides in. Defaults to None.
        survey_name (str, optional): _description_. Defaults to "".
        echosounder (str, optional): _description_. Defaults to "".
        survey_folder_url (str, optional): _description_. Defaults to "".
        download_location (str, optional): _description_. Defaults to "".
    """    
    # Get a list of urls of objects from that folder.
    # TODO: Check if its already cached.
    # TODO
    ...


def get_all_ship_objects_from_ncei(ship_name: str = "",
                                   bucket: boto3.resource = None) -> List[str]:
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

    assert ship_name != "", "Please provide a valid Titlecase ship_name using underscores as spaces."
    assert " " not in ship_name, "Please provide a valid Titlecase ship_name using underscores as spaces."
    assert bucket is not None, "Please pass in a boto3 bucket object."

    ship_objects = []

    for object in bucket.objects.filter(Prefix=f"data/raw/{ship_name}"):
        ship_objects.append(object.key)
    
    return ship_objects


def get_all_objects_in_survey_from_ncei(ship_name: str = "",
                                        survey_name: str = "",
                                        bucket: boto3.resource = None) -> List[str]:
    """Gets all of the object keys from a ship survey from the NCEI database.

    Args:
        ship_name (str, optional): The name of the ship. Must be title-case and have
            spaces substituted for underscores. Defaults to "".
        survey_name (str, optional): The name of the survey. Must match what we have
            in the NCEI database. Defaults to "".
        bucket (boto3.resource, optional): The boto3 bucket resource for the bucket
            that the ship data resides in. Defaults to None.

    Returns:
        List[str]: A list of strings. Each one being an object key (path to the object
            inside of the bucket).
    """

    assert ship_name != "", "Please provide a valid Titlecase ship_name using underscores as spaces."
    assert " " not in ship_name, "Please provide a valid Titlecase ship_name using underscores as spaces."
    assert survey_name != "", "Please provide a valid survey name."
    assert bucket is not None, "Please pass in a boto3 bucket object."

    survey_objects = []

    for object in bucket.objects.filter(Prefix=f"data/raw/{ship_name}/{survey_name}"):
        survey_objects.append(object.key)
    
    return survey_objects


def parse_correct_gcp_storage_bucket_location(file_name: str = "",
                                              file_type: str = "",
                                              ship_name: str = "",
                                              survey_name: str = "",
                                              echosounder: str = "",
                                              data_source: str = "NCEI",
                                              is_metadata: bool = False,
                                              debug: bool = False):
    """Calculates the correct gcp storage location based on data source, file
    type, and if the file is metadata or not.

    Args:
        file_name (str, optional): The file name (includes extension). Defaults to "".
        file_type (str, optional): The file type (not include the dot "."). Defaults to "".
        ship_name (str, optional): The ship name associated with this survey. Defaults to "".
        survey_name (str, optional): The survey name/identifier. Defaults to "".
        echosounder (str, optional): The echosounder used to gather the data. Defaults to "".
        data_source (str, optional): The source of the data. Can be one of ["NCEI", "OMAO"]. Defaults to "NCEI".
        is_metadata (bool, optional): Whether or not the file is a metadata file. Necessary since
            files that are considered metadata (metadata json, or readmes) are stored
            in a separate directory. Defaults to False.
        debug (bool, optional): Whether or not to print debug statements. Defaults to False.

    Returns:
        _type_: _description_
    """    
    # Creating the correct upload location
    if not is_metadata:
        # Figure out if its a raw or idx file (belongs in raw folder)
        if file_type.lower() in ["raw", "idx"]:
            gcp_storage_bucket_location = f"{data_source}/{ship_name}/{survey_name}/{echosounder}/data/raw/{file_name}"
        elif file_type.lower() in ["netCDF"]:
            gcp_storage_bucket_location = f"{data_source}/{ship_name}/{survey_name}/{echosounder}/data/netcdf/{file_name}"
    else:
        gcp_storage_bucket_location = f"{data_source}/{ship_name}/{survey_name}/{echosounder}/metadata/"
    
    if debug:
        print(f"PARSED GCP_STORAGE_BUCKET_LOCATION: {gcp_storage_bucket_location}")

    return gcp_storage_bucket_location


def upload_file_to_gcp_storage_bucket(file_name: str = "",
                                      file_type: str = "",
                                      ship_name: str = "",
                                      survey_name: str = "",
                                      echosounder: str = "",
                                      file_location: str = "",
                                      gcp_bucket: storage.Client.bucket = None,
                                      data_source: str = "NCEI",
                                      is_metadata: bool = False,
                                      debug: bool = False):
    """Uploads a local file to the storage bucket.

    Args:
        file_name (str, optional): The file name (includes extension). Defaults to "".
        file_type (str, optional): The file type (not include the dot "."). Defaults to "".
        ship_name (str, optional): The ship name associated with this survey. Defaults to "".
        survey_name (str, optional): The survey name/identifier. Defaults to "".
        echosounder (str, optional): The echosounder used to gather the data. Defaults to "".
        file_location (str, optional): The local location of the file. Defaults to "".
        gcp_bucket (storage.Client.bucket, optional): The GCP bucket object used to upload
            the file. Defaults to None.
        data_source (str, optional): The source of the data. Can be one of ["NCEI", "OMAO"]. Defaults to "NCEI".
        is_metadata (bool, optional): Whether or not the file is a metadata file. Necessary since
            files that are considered metadata (metadata json, or readmes) are stored
            in a separate directory. Defaults to False.
        debug (bool, optional): Whether or not to print debug statements. Defaults to False.
    """
    
    gcp_storage_bucket_location = parse_correct_gcp_storage_bucket_location()

    # Upload to storage bucket.
    utils.upload_file_to_gcp_bucket(bucket=gcp_bucket, blob_file_path=gcp_storage_bucket_location,
                                    local_file_path=file_location, debug=debug)

    return


if __name__ == '__main__':
    s3_resource, bucket = utils.create_s3_objs()
    # survey_stuff = get_all_objects_from_survey_ncei(ship_name="Reuben_Lasker",
    #                                  survey_name="RL2107",
    #                                  bucket=bucket)
    # print(survey_stuff)
    # resp = s3_resource.(bucket="noaa-wcsd-pds",
    #                           prefix="data/raw/Reuben_Lasker/RL2107")
    # print(resp)

    # print(utils.count_objects_in_bucket_location(prefix="data/raw/Reuben_Lasker/RL2107/",
    #                                              bucket=bucket))
    
    print(utils.get_subdirectories_in_bucket_location(prefix="data/raw/Reuben_Lasker/RL2107/",
                                                 bucket=bucket,
                                                 return_full_paths=True))
