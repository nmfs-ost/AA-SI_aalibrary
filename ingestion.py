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

def create_ncei_url_from_variables(file_name: str = "",
                                file_type: str = "",
                                ship_name: str = "",
                                survey_name: str = "",
                                echosounder: str = "",
                                year: str = "",
                                month: str = "",
                                date: str = ""):
    if file_name != "":
        ncei_url = f"https://noaa-wcsd-pds.s3.amazonaws.com/data/raw/{ship_name}/{survey_name}/{echosounder}/{file_name}"
        return ncei_url
    else:
        # Here we have to search for the file in s3. Just to see if something exists.
        partial_file_name = f"-D{year}{month}{date}-"
        # TODO: make sure to check that a raw and idx files both exist.


def download_single_file_from_aws(bucket_name: str = "noaa-wcsd-pds",
                                  file_url: str = "https://noaa-wcsd-pds.s3.amazonaws.com/data/raw/Reuben_Lasker/RL2107/EK80/2107RL_CW-D20210706-T172335.idx",
                                  download_location: str = ""):
    """Downloads a file from AWS storage bucket, aka the NCEI repository."""
    
    try:
        s3_resource, bucket_name = utils.create_s3_objs()
    except Exception as e:
        print(f"Cannot establish connection to s3 bucket..\n{e}")
    
    # We replace the beginning of common file paths
    file_url = file_url.replace("https://noaa-wcsd-pds.s3.amazonaws.com/", '')
    file_url = file_url.replace("s3://noaa-wcsd-pds/", '')
    file_name = get_file_name_from_url(file_url)

    # Finally download the file.
    try:
        bucket_name.download_file(file_url, file_name)
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
    # TODO: Check if ALL OF IT is already cached.
    # TODO
    ...

def download_transect_from_NCEI(file_name: str = "",
                                file_type: str = "",
                                ship_name: str = "",
                                survey_name: str = "",
                                echosounder: str = "",
                                file_download_location: str = "",
                                is_metadata: bool = False,
                                force_download_from_ncei: bool = False,
                                debug: bool = False):
    """Downloads a transect file from NCEI for use on your workstation.
    ENTRYPOINT FOR END-USERS

    Args:
        file_name (str, optional): The file name (includes extension). Defaults to "".
        file_type (str, optional): The file type (not include the dot "."). Defaults to "".
        ship_name (str, optional): The ship name associated with this survey. Defaults to "".
        survey_name (str, optional): The survey name/identifier. Defaults to "".
        echosounder (str, optional): The echosounder used to gather the data. Defaults to "".
        file_download_location (str, optional): The local file path you want to store your
            file in. Defaults to "".
        is_metadata (bool, optional): Whether or not the file is a metadata file. Necessary since
            files that are considered metadata (metadata json, or readmes) are stored
            in a separate directory. Defaults to False.
        force_download_from_ncei (bool, optional): Whether or not to override caching and force
            a download from NCEI. Defaults to False.
        debug (bool, optional): Whether or not to print debug statements. Defaults to False.
    """

    # Get the correct location of the file in GCP storage bucket, whether it exists
    # or not, we need the variable.
    file_ncei_url = create_ncei_url_from_variables(file_name=file_name, ship_name=ship_name,
                                                   survey_name=survey_name, echosounder=echosounder)
    file_name_idx = ".".join(file_name.split(".")[:-1]) + ".idx"
    file_ncei_idx_url = ".".join(file_ncei_url.split(".")[:-1]) + ".idx"
    file_download_location_idx = ".".join(file_download_location.split(".")[:-1]) + ".idx"
    gcp_storage_bucket_location = parse_correct_gcp_storage_bucket_location(file_name=file_name,
                                                                            file_type=file_type,
                                                                            ship_name=ship_name,
                                                                            survey_name=survey_name,
                                                                            echosounder=echosounder,
                                                                            data_source="NCEI",
                                                                            is_metadata=is_metadata,
                                                                            debug=debug)
    
    # Create vars for use later.
    gcp_stor_client, gcp_bucket_name, gcp_bucket = utils.setup_gbq_storage_objs()

    # Check if the file exists in cache (GCP).
    file_exists_in_gcp = utils.check_if_file_exists_in_gcp(bucket=gcp_bucket,
                                                           file_path=gcp_storage_bucket_location)
    if file_exists_in_gcp:
        # Inform user if file exists
        print(f"FILE `{file_name}` ALREADY EXISTS IN GOOGLE STORAGE BUCKET.")
        # Force download from NCEI if enabled.
        if force_download_from_ncei:
            try:
                print("FORCE DOWNLOAD FROM NCEI WAS ENABLED")
                print(f"DOWNLOADING FILE {file_name} FROM NCEI")
                download_single_file_from_aws(bucket_name="noaa-wcsd-pds",
                                            file_url=file_ncei_url,
                                            download_location=file_download_location)
                print(f"DOWNLOADED FILE {file_name} FROM NCEI")
            except Exception as e:
                print(f"COULD NOT DOWNLOAD FILE FROM NCEI DUE TO THE FOLLOWING ERROR:\n{e}")
                return
        else:
            print(f"SINCE FILE EXISTS IN GCP, CHECKING FOR NETCDF VERSION...")
            netcdf_exists_in_gcp = check_if_netcdf_file_exists_in_gcp(gcp_storage_bucket_location=gcp_storage_bucket_location,
                                                                      gcp_bucket=gcp_bucket,
                                                                      debug=debug)
            if netcdf_exists_in_gcp:
                # Inform the user if a netcdf version exists in cache.
                print(f"FILE {file_name} EXISTS AS A NETCDF ALREADY. DOWNLOADING NETCDF...")
                netcdf_gcp_storage_bucket_location = parse_netcdf_gcp_location(gcp_storage_bucket_location=gcp_storage_bucket_location)
                # Download from gcp to file_download_location.
                utils.download_file_from_gcp(gcp_bucket=gcp_bucket,
                                             blob_file_path=netcdf_gcp_storage_bucket_location,
                                             local_file_path=file_download_location,
                                             debug=debug)
                print(f"DOWNLOADED.")
    else:
        # Download the raw file.
        try:
            print(f"DOWNLOADING FILE {file_name} FROM NCEI")
            download_single_file_from_aws(bucket_name="noaa-wcsd-pds",
                                        file_url=file_ncei_url,
                                        download_location=file_download_location)
            print(f"DOWNLOADED FILE {file_name} FROM NCEI\nUPLOADING TO GCP...")
        except Exception as e:
            print(f"COULD NOT DOWNLOAD FILE FROM NCEI DUE TO THE FOLLOWING ERROR:\n{e}")
            return
        # Upload to GCP at the correct storage bucket location.
        try:
            print("CONTINUING UPLOAD TO GCP...")
            _, _, gcp_bucket = utils.setup_gbq_storage_objs()
            upload_file_to_gcp_storage_bucket(file_name=file_name, file_type=file_type,
                                              ship_name=ship_name, survey_name=survey_name,
                                              echosounder=echosounder, file_location=file_download_location,
                                              gcp_bucket=gcp_bucket, data_source="NCEI",
                                              is_metadata=is_metadata, debug=debug)
            print(f"UPLOADED FILE {file_name} TO GCP.")
            # TODO: Maybe submit a dataproc job here to convert the file (background)??????
        except Exception as e:
            print(f"COULD NOT UPLOAD FILE {file_name} TO GCP STORAGE BUCKET DUE TO THE FOLLOWING ERROR:\n{e}")
        

        # Download the idx file.
        try:
            print(f"DOWNLOADING IDX FILE {file_name_idx} FROM NCEI")
            download_single_file_from_aws(bucket_name="noaa-wcsd-pds",
                                        file_url=file_ncei_idx_url,
                                        download_location=file_download_location_idx)
            print(f"DOWNLOADED FILE {file_name_idx} FROM NCEI\nUPLOADING TO GCP...")
        except Exception as e:
            print(f"COULD NOT DOWNLOAD FILE FROM NCEI DUE TO THE FOLLOWING ERROR:\n{e}")
            return
        # Upload to GCP at the correct storage bucket location.
        try:
            print("CONTINUING UPLOAD TO GCP...")
            _, _, gcp_bucket = utils.setup_gbq_storage_objs()
            upload_file_to_gcp_storage_bucket(file_name=file_name_idx, file_type=file_type,
                                              ship_name=ship_name, survey_name=survey_name,
                                              echosounder=echosounder, file_location=file_download_location_idx,
                                              gcp_bucket=gcp_bucket, data_source="NCEI",
                                              is_metadata=is_metadata, debug=debug)
            print(f"UPLOADED FILE {file_name_idx} TO GCP.")
        except Exception as e:
            print(f"COULD NOT UPLOAD FILE {file_name_idx} TO GCP STORAGE BUCKET DUE TO THE FOLLOWING ERROR:\n{e}")


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
        elif file_type.lower() in ["netcdf"]:
            gcp_storage_bucket_location = f"{data_source}/{ship_name}/{survey_name}/{echosounder}/data/netcdf/{file_name}"
    else:
        gcp_storage_bucket_location = f"{data_source}/{ship_name}/{survey_name}/{echosounder}/metadata/"
    
    if debug:
        print(f"PARSED GCP_STORAGE_BUCKET_LOCATION: {gcp_storage_bucket_location}")

    return gcp_storage_bucket_location


def parse_netcdf_gcp_location(gcp_storage_bucket_location: str = ""):
    """Gets the netcdf location of a raw file within GCP."""

    gcp_storage_bucket_location = gcp_storage_bucket_location.replace("/raw/", "/netcdf/")
    # get rid of file extension and replace with netcdf
    netcdf_gcp_storage_bucket_location = ".".join(gcp_storage_bucket_location.split(".")[:-1]) + ".netcdf"
    
    return netcdf_gcp_storage_bucket_location

def check_if_netcdf_file_exists_in_gcp(file_name: str = "",
                                file_type: str = "",
                                ship_name: str = "",
                                survey_name: str = "",
                                echosounder: str = "",
                                data_source: str = "",
                                gcp_storage_bucket_location: str = "",
                                gcp_bucket: storage.Bucket = None,
                                debug: bool = False):
    
    assert gcp_bucket is not None, "Please provide a gcp_bucket object with `utils.setup_gcp_storage()`"
    
    if gcp_storage_bucket_location != "":
        gcp_storage_bucket_location = parse_correct_gcp_storage_bucket_location(file_name=file_name,
                                                                        file_type=file_type,
                                                                        survey_name=survey_name,
                                                                        ship_name=ship_name,
                                                                        echosounder=echosounder,
                                                                        data_source=data_source,
                                                                        is_metadata=False,
                                                                        debug=debug)
    netcdf_gcp_storage_bucket_location = parse_netcdf_gcp_location(gcp_storage_bucket_location=gcp_storage_bucket_location)
    # check if the file exists in gcp
    return utils.check_if_file_exists_in_gcp(bucket=gcp_bucket,
                                        file_path=netcdf_gcp_storage_bucket_location)


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

    gcp_storage_bucket_location = parse_correct_gcp_storage_bucket_location(file_name=file_name,
                                                                            file_type=file_type,
                                                                            ship_name=ship_name,
                                                                            survey_name=survey_name,
                                                                            echosounder=echosounder,
                                                                            data_source=data_source,
                                                                            is_metadata=is_metadata,
                                                                            debug=debug)

    # Upload to storage bucket.
    utils.upload_file_to_gcp_bucket(bucket=gcp_bucket, blob_file_path=gcp_storage_bucket_location,
                                    local_file_path=file_location, debug=debug)

    return


if __name__ == '__main__':
    s3_resource, s3_bucket = utils.create_s3_objs()
    # survey_stuff = get_all_objects_from_survey_ncei(ship_name="Reuben_Lasker",
    #                                  survey_name="RL2107",
    #                                  bucket=bucket)
    # print(survey_stuff)
    # resp = s3_resource.(bucket="noaa-wcsd-pds",
    #                           prefix="data/raw/Reuben_Lasker/RL2107")
    # print(resp)

    # print(utils.count_objects_in_bucket_location(prefix="data/raw/Reuben_Lasker/RL2107/",
    #                                              bucket=bucket))
    
    file_name, file_type, echosounder, survey_name, ship_name = parse_variables_from_ncei_file_url(url="https://noaa-wcsd-pds.s3.amazonaws.com/data/raw/Reuben_Lasker/RL2107/EK80/2107RL_CW-D20210813-T220732.raw")
    print(parse_correct_gcp_storage_bucket_location(file_name=file_name,
                                                    file_type=file_type,
                                                    ship_name=ship_name,
                                                    survey_name=survey_name,
                                                    echosounder=echosounder,
                                                    data_source="NCEI",
                                                    is_metadata=False,
                                                    debug=True))
    # https://noaa-wcsd-pds.s3.amazonaws.com/data/raw/Reuben_Lasker/RL2107/EK80/2107RL_CW-D20210706-T172335.idx
    download_transect_from_NCEI(file_name="2107RL_CW-D20210813-T220732.raw",
                                file_type="raw",
                                ship_name="Reuben_Lasker",
                                survey_name="RL2107",
                                echosounder="EK80",
                                file_download_location=f"./2107RL_CW-D20210813-T220732.raw",
                                is_metadata=False,
                                force_download_from_ncei=False,
                                debug=True)
    # gcp_stor_client, gcp_bucket_name, gcp_bucket = utils.setup_gbq_storage_objs()
    # print(utils.check_if_file_exists_in_gcp(gcp_bucket, file_path="NCEI/Reuben_Lasker/RL2107/EK80/data/raw/2107RL_CW-D20210813-T220732a.raw"))
