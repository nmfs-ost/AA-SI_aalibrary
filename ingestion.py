"""This script contains functions used to ingest Active Acoustics data into GCP
from various sources such as AWS buckets and Azure Data Lake."""

import sys
import os
import json
import subprocess
import requests
import time
from typing import List

import boto3
from botocore import UNSIGNED
from botocore.client import Config

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


def create_s3_objs(bucket: str = "noaa-wcsd-pds"):
    """Creates the boto3 object used for downloading file objects."""

    # Setup access to S3 bucket as an anonymous user
    s3_resource = boto3.resource(
        's3',
        aws_access_key_id='',
        aws_secret_access_key='',
        config=Config(signature_version=UNSIGNED),
        )
    
    bucket = s3_resource.Bucket(bucket)
    
    return s3_resource, bucket


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
        s3_resource, bucket = create_s3_objs()
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


def download_single_survey_from_aws(bucket_name: str = "noaa-wvsd-pds",
                                    survey_folder_url: str = "",
                                    download_location: str = ""):
    """_summary_

    Args:
        bucket_name (str, optional): _description_. Defaults to "noaa-wvsd-pds".
        survey_folder_url (str, optional): _description_. Defaults to "".
        download_location (str, optional): _description_. Defaults to "".
    """

    # Get a list of urls of objects from that folder.


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

def get_all_objects_from_survey_ncei(ship_name: str = "",
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


def main():
    ...


if __name__ == '__main__':
    main()
    s3_resource, bucket = create_s3_objs()
    survey_stuff = get_all_objects_from_survey_ncei(ship_name="Reuben_Lasker",
                                     survey_name="RL2107",
                                     bucket=bucket)
    print(survey_stuff)
    # resp = s3_resource.(bucket="noaa-wcsd-pds",
    #                           prefix="data/raw/Reuben_Lasker/RL2107")
    # print(resp)
