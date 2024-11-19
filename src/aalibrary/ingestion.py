"""This script contains functions used to ingest Active Acoustics data into GCP
from various sources such as AWS buckets and Azure Data Lake."""

import glob
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
from echopype import open_raw

# For pytests-sake
if __name__ == "__main__":
    import utils
    import config
    from utils import cloud_utils
else:
    from src.aalibrary import utils
    from src.aalibrary import config
    from src.aalibrary.utils import cloud_utils


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


def create_ncei_url_from_variables(
    file_name: str = "",
    file_type: str = "",
    ship_name: str = "",
    survey_name: str = "",
    echosounder: str = "",
    year: str = "",
    month: str = "",
    date: str = "",
    hours: str = "",
    minutes: str = "",
    seconds: str = "",
):
    if file_name != "":
        ncei_url = f"https://noaa-wcsd-pds.s3.amazonaws.com/data/raw/{ship_name}/{survey_name}/{echosounder}/{file_name}"
        return ncei_url
    else:
        print(f"COULD NOT FIND FILE GIVEN THE PARAMETERS.")
        # Here we have to search for the file in s3. Just to see if something exists.
        partial_file_name = f"-D{year}{month}{date}-T{hours}{minutes}{seconds}.raw"
        # TODO: make sure to check that a raw and idx files both exist.
        raise FileNotFoundError


def download_single_file_from_aws(
    s3_bucket: str = "noaa-wcsd-pds", file_url: str = "", download_location: str = ""
):
    """Safely downloads a file from AWS storage bucket, aka the NCEI repository.

    Args:
        s3_bucket (str, optional): _description_. Defaults to "noaa-wcsd-pds".
        file_url (str, optional): _description_. Defaults to "".
        download_location (str, optional): _description_. Defaults to "".
    """
    """Downloads a file from AWS storage bucket, aka the NCEI repository."""

    try:
        s3_client, s3_resource, s3_bucket = utils.cloud_utils.create_s3_objs()
    except Exception as e:
        print(f"CANNOT ESTABLISH CONNECTION TO S3 BUCKET..\n{e}")
        raise

    # We replace the beginning of common file paths
    file_url = file_url.replace("https://noaa-wcsd-pds.s3.amazonaws.com/", "")
    file_url = file_url.replace("s3://noaa-wcsd-pds/", "")
    file_name = get_file_name_from_url(file_url)

    # Check if the file exists in s3
    print(f"s3_bucket.name: {s3_bucket.name}")
    file_exists = utils.cloud_utils.check_if_file_exists_in_s3(
        object_key=file_url, s3_resource=s3_resource, s3_bucket_name=s3_bucket.name
    )

    # Finally download the file.
    try:
        print(f"DOWNLOADING `{file_name}`")
        s3_bucket.download_file(file_url, download_location)
        print(f"DOWNLOADED: `{file_name}` TO `{download_location}`")
    except Exception as e:
        print(f"ERROR DOWNLOADING FILE `{file_name}` DUE TO\n{e}")
        raise


def download_raw_file_from_ncei(
    file_name: str = "",
    file_type: str = "raw",
    ship_name: str = "",
    survey_name: str = "",
    echosounder: str = "",
    data_source: str = "",
    file_download_location: str = ".",
    is_metadata: bool = False,
    upload_to_gcp: bool = False,
    debug: bool = False,
):
    """ENTRYPOINT FOR END-USERS
    Downloads a raw and idx file from NCEI. If `upload_to_gcp` is enabled, the
    downloaded files will also upload to the GCP storage bucket.

    Args:
        file_name (str, optional): The file name (includes extension). Defaults to "".
        file_type (str, optional): The file type (do not include the dot "."). Defaults to "".
        ship_name (str, optional): The ship name associated with this survey. Defaults to "".
        survey_name (str, optional): The survey name/identifier. Defaults to "".
        echosounder (str, optional): The echosounder used to gather the data. Defaults to "".
        data_source (str, optional): The source of the file. Necessary due to the
            way the storage bucket is organized. Can be one of ["NCEI", "OMAO", "HDD"].
            Defaults to "".
        file_download_location (str, optional): The local file directory you want to store your
            file in. Defaults to current directory. Defaults to ".".
        is_metadata (bool, optional): Whether or not the file is a metadata file. Necessary since
            files that are considered metadata (metadata json, or readmes) are stored
            in a separate directory. Defaults to False.
        upload_to_gcp (bool, optional): Whether or not you want to upload to GCP. Defaults to False.
        debug (bool, optional): Whether or not to print debug statements. Defaults to False.
    """

    # User-error-checking
    assert (
        file_name != ""
    ), "Please provide a valid file name with the file extension (ex. `2107RL_CW-D20210813-T220732.raw`)"
    assert file_type != "", "Please provide a valid file type."
    assert (
        file_type in config.VALID_FILETYPES
    ), f"Please provide a valid file type (extension) from the following: {config.VALID_FILETYPES}"
    assert (
        ship_name != ""
    ), "Please provide a valid ship name (Title_Case_With_Underscores_As_Spaces)."
    assert survey_name != "", "Please provide a valid survey name."
    assert echosounder != "", "Please provide a valid echosounder."
    assert (
        echosounder in config.VALID_ECHOSOUNDERS
    ), f"Please provide a valid echosounder from the following: {config.VALID_ECHOSOUNDERS}"
    assert (
        data_source != ""
    ), f"Please provide a valid data source from the following: {config.VALID_DATA_SOURCES}"
    assert (
        data_source in config.VALID_DATA_SOURCES
    ), f"Please provide a valid data source from the following: {config.VALID_DATA_SOURCES}"
    assert (
        file_download_location != ""
    ), "Please provide a valid file download locaiton (a directory)."
    assert (
        os.path.isdir(file_download_location) == True
    ), f"File download location `{file_download_location}` is not found to be a valid dir, please reformat it."

    # Create vars for use later.
    file_download_location = os.sep.join(
        [os.path.normpath(file_download_location), file_name]
    )
    file_ncei_url = create_ncei_url_from_variables(
        file_name=file_name,
        ship_name=ship_name,
        survey_name=survey_name,
        echosounder=echosounder,
    )
    file_name_idx = ".".join(file_name.split(".")[:-1]) + ".idx"
    file_ncei_idx_url = ".".join(file_ncei_url.split(".")[:-1]) + ".idx"
    file_download_location_idx = (
        ".".join(file_download_location.split(".")[:-1]) + ".idx"
    )
    gcp_storage_bucket_location = parse_correct_gcp_storage_bucket_location(
        file_name=file_name,
        file_type=file_type,
        ship_name=ship_name,
        survey_name=survey_name,
        echosounder=echosounder,
        data_source=data_source,
        is_metadata=is_metadata,
        debug=debug,
    )
    gcp_storage_bucket_location_idx = parse_correct_gcp_storage_bucket_location(
        file_name=file_name_idx,
        file_type="idx",
        ship_name=ship_name,
        survey_name=survey_name,
        echosounder=echosounder,
        data_source=data_source,
        is_metadata=is_metadata,
        debug=debug,
    )
    gcp_stor_client, gcp_bucket_name, gcp_bucket = (
        utils.cloud_utils.setup_gcp_storage_objs()
    )

    # Check if the file(s) exists in cache (GCP).
    file_exists_in_gcp = utils.cloud_utils.check_if_file_exists_in_gcp(
        bucket=gcp_bucket, file_path=gcp_storage_bucket_location
    )
    idx_file_exists_in_gcp = cloud_utils.check_if_file_exists_in_gcp(
        bucket=gcp_bucket, file_path=gcp_storage_bucket_location_idx
    )

    print(f"DOWNLOADING FILE {file_name} FROM NCEI")
    download_single_file_from_aws(
        s3_bucket="noaa-wcsd-pds",
        file_url=file_ncei_url,
        download_location=file_download_location,
    )
    # Force download the idx file.
    print(f"DOWNLOADING IDX FILE {file_name_idx} FROM NCEI")
    download_single_file_from_aws(
        s3_bucket="noaa-wcsd-pds",
        file_url=file_ncei_idx_url,
        download_location=file_download_location_idx,
    )

    if upload_to_gcp:
        if file_exists_in_gcp:
            print(f"RAW FILE ALREADY EXISTS IN GCP AT `{gcp_storage_bucket_location}`")
        else:
            # Upload raw to GCP at the correct storage bucket location.
            upload_file_to_gcp_storage_bucket(
                file_name=file_name,
                file_type=file_type,
                ship_name=ship_name,
                survey_name=survey_name,
                echosounder=echosounder,
                file_location=file_download_location,
                gcp_bucket=gcp_bucket,
                data_source="NCEI",
                is_metadata=is_metadata,
                debug=debug,
            )

        if idx_file_exists_in_gcp:
            print(
                f"IDX FILE ALREADY EXISTS IN GCP AT `{gcp_storage_bucket_location_idx}`"
            )
        else:
            # Upload idx to GCP at the correct storage bucket location.
            upload_file_to_gcp_storage_bucket(
                file_name=file_name_idx,
                file_type=file_type,
                ship_name=ship_name,
                survey_name=survey_name,
                echosounder=echosounder,
                file_location=file_download_location_idx,
                gcp_bucket=gcp_bucket,
                data_source="NCEI",
                is_metadata=is_metadata,
                debug=debug,
            )

        return


def download_single_survey_from_ncei(
    ship_name: str = "",
    survey_name: str = "",
    echosounder: str = "",
    survey_folder_url: str = "",
    download_location: str = "",
    bucket: boto3.resource = None,
):
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


def download_raw_file(
    file_name: str = "",
    file_type: str = "raw",
    ship_name: str = "",
    survey_name: str = "",
    echosounder: str = "",
    data_source: str = "",
    file_download_location: str = ".",
    is_metadata: bool = False,
    debug: bool = False,
):
    """ENTRYPOINT FOR END-USERS
    Downloads a raw and idx file from NCEI for use on your workstation.
    Works as follows:
        1. Checks if raw file exists in GCP.
            a. If it exists,
                checks if a netcdf version also exists or not
                lets the user know.
                i. If `force_download_from_ncei` is True
                    downloads the raw and idx file from NCEI instead.
            b. If it doesn't exist,
                downloads .raw from NCEI and uploads to GCP for caching
                downloads .idx from NCEI and uploads to GCP for caching

    Args:
        file_name (str, optional): The file name (includes extension). Defaults to "".
        file_type (str, optional): The file type (do not include the dot "."). Defaults to "".
        ship_name (str, optional): The ship name associated with this survey. Defaults to "".
        survey_name (str, optional): The survey name/identifier. Defaults to "".
        echosounder (str, optional): The echosounder used to gather the data. Defaults to "".
        data_source (str, optional): The source of the file. Necessary due to the
            way the storage bucket is organized. Can be one of ["NCEI", "OMAO", "HDD"].
            Defaults to "".
        file_download_location (str, optional): The local file directory you want to store your
            file in. Defaults to current directory. Defaults to ".".
        is_metadata (bool, optional): Whether or not the file is a metadata file. Necessary since
            files that are considered metadata (metadata json, or readmes) are stored
            in a separate directory. Defaults to False.
        debug (bool, optional): Whether or not to print debug statements. Defaults to False.
    """

    # User-error-checking
    assert (
        file_name != ""
    ), "Please provide a valid file name with the file extension (ex. `2107RL_CW-D20210813-T220732.raw`)"
    assert file_type != "", "Please provide a valid file type."
    assert (
        file_type in config.VALID_FILETYPES
    ), f"Please provide a valid file type (extension) from the following: {config.VALID_FILETYPES}"
    assert (
        ship_name != ""
    ), "Please provide a valid ship name (Title_Case_With_Underscores_As_Spaces)."
    assert survey_name != "", "Please provide a valid survey name."
    assert echosounder != "", "Please provide a valid echosounder."
    assert (
        echosounder in config.VALID_ECHOSOUNDERS
    ), f"Please provide a valid echosounder from the following: {config.VALID_ECHOSOUNDERS}"
    assert (
        data_source != ""
    ), f"Please provide a valid data source from the following: {config.VALID_DATA_SOURCES}"
    assert (
        data_source in config.VALID_DATA_SOURCES
    ), f"Please provide a valid data source from the following: {config.VALID_DATA_SOURCES}"
    assert (
        file_download_location != ""
    ), "Please provide a valid file download locaiton (a directory)."
    assert (
        os.path.isdir(file_download_location) == True
    ), f"File download location `{file_download_location}` is not found to be a valid dir, please reformat it."

    # Create vars for use later.
    file_download_location = os.sep.join(
        [os.path.normpath(file_download_location), file_name]
    )
    file_ncei_url = create_ncei_url_from_variables(
        file_name=file_name,
        ship_name=ship_name,
        survey_name=survey_name,
        echosounder=echosounder,
    )
    file_name_idx = ".".join(file_name.split(".")[:-1]) + ".idx"
    file_ncei_idx_url = ".".join(file_ncei_url.split(".")[:-1]) + ".idx"
    file_download_location_idx = (
        ".".join(file_download_location.split(".")[:-1]) + ".idx"
    )
    file_name_netcdf = ".".join(file_name.split(".")[:-1]) + ".nc"
    gcp_storage_bucket_location = parse_correct_gcp_storage_bucket_location(
        file_name=file_name,
        file_type=file_type,
        ship_name=ship_name,
        survey_name=survey_name,
        echosounder=echosounder,
        data_source=data_source,
        is_metadata=is_metadata,
        debug=debug,
    )
    gcp_storage_bucket_location_idx = parse_correct_gcp_storage_bucket_location(
        file_name=file_name_idx,
        file_type="idx",
        ship_name=ship_name,
        survey_name=survey_name,
        echosounder=echosounder,
        data_source=data_source,
        is_metadata=is_metadata,
        debug=debug,
    )
    gcp_storage_bucket_location_netcdf = parse_correct_gcp_storage_bucket_location(
        file_name=file_name_netcdf,
        file_type="netcdf",
        ship_name=ship_name,
        survey_name=survey_name,
        echosounder=echosounder,
        data_source=data_source,
        is_metadata=is_metadata,
        debug=debug,
    )
    gcp_stor_client, gcp_bucket_name, gcp_bucket = (
        utils.cloud_utils.setup_gcp_storage_objs()
    )

    # Check if the file(s) exists in cache (GCP).
    file_exists_in_gcp = utils.cloud_utils.check_if_file_exists_in_gcp(
        bucket=gcp_bucket, file_path=gcp_storage_bucket_location
    )
    idx_file_exists_in_gcp = cloud_utils.check_if_file_exists_in_gcp(
        bucket=gcp_bucket, file_path=gcp_storage_bucket_location_idx
    )

    if file_exists_in_gcp:
        # Inform user if file exists in GCP.
        print(f"FILE `{file_name}` ALREADY EXISTS IN GOOGLE STORAGE BUCKET.")
        # Here we download the raw file from GCP. We also check for a netcdf
        # version and let the user know.
        print(f"CHECKING FOR NETCDF VERSION...")
        netcdf_exists_in_gcp = check_if_netcdf_file_exists_in_gcp(
            file_name=file_name_netcdf,
            file_type="netcdf",
            ship_name=ship_name,
            survey_name=survey_name,
            echosounder=echosounder,
            data_source=data_source,
            gcp_storage_bucket_location=gcp_storage_bucket_location_netcdf,
            gcp_bucket=gcp_bucket,
            debug=debug,
        )
        if netcdf_exists_in_gcp:
            # Inform the user if a netcdf version exists in cache.
            print(
                f"FILE `{file_name}` EXISTS AS A NETCDF ALREADY. PLEASE DOWNLOAD THE NETCDF VERSION IF NEEDED."
            )
        else:
            print(
                f"FILE `{file_name}` DOES NOT EXIST AS NETCDF. CONSIDER RUNNING A CONVERSION FUNCTION"
            )

        # Here we download the raw from GCP.
        print(f"DOWNLOADING FILE `{file_name}` FROM GCP TO `{file_download_location}`")
        utils.cloud_utils.download_file_from_gcp(
            gcp_bucket=gcp_bucket,
            blob_file_path=gcp_storage_bucket_location,
            local_file_path=file_download_location,
            debug=debug,
        )
        print(f"DOWNLOADED.")

        # Checking to make sure the idx exists in GCP...
        if idx_file_exists_in_gcp:
            print("CORRESPONDING IDX FILE FOUND IN GCP. DOWNLOADING...")
            # Here we download the idx from GCP.
            print(
                f"DOWNLOADING FILE `{file_name_idx}` FROM GCP TO `{file_download_location_idx}`"
            )
            utils.cloud_utils.download_file_from_gcp(
                gcp_bucket=gcp_bucket,
                blob_file_path=gcp_storage_bucket_location_idx,
                local_file_path=file_download_location_idx,
                debug=debug,
            )
            print(f"DOWNLOADED.")
        else:
            print(
                "CORRESPONDING IDX FILE NOT FOUND IN GCP. DOWNLOADING FROM NCEI AND UPLOADING TO GCP..."
            )
            # Safely download and upload the idx file.
            download_single_file_from_aws(
                s3_bucket="noaa-wcsd-pds",
                file_url=file_ncei_idx_url,
                download_location=file_download_location_idx,
            )
            # Upload to GCP at the correct storage bucket location.
            upload_file_to_gcp_storage_bucket(
                file_name=file_name_idx,
                file_type=file_type,
                ship_name=ship_name,
                survey_name=survey_name,
                echosounder=echosounder,
                file_location=file_download_location_idx,
                gcp_bucket=gcp_bucket,
                data_source="NCEI",
                is_metadata=is_metadata,
                debug=debug,
            )
    else:
        # Download and upload the raw file.
        download_single_file_from_aws(
            s3_bucket="noaa-wcsd-pds",
            file_url=file_ncei_url,
            download_location=file_download_location,
        )
        # TODO: try out a background process if possible -- file might have a lock. only async options, otherwise subprocess gsutil to upload it.
        # Upload raw to GCP at the correct storage bucket location.
        upload_file_to_gcp_storage_bucket(
            file_name=file_name,
            file_type=file_type,
            ship_name=ship_name,
            survey_name=survey_name,
            echosounder=echosounder,
            file_location=file_download_location,
            gcp_bucket=gcp_bucket,
            data_source="NCEI",
            is_metadata=is_metadata,
            debug=debug,
        )
        # Download and upload the idx file.
        download_single_file_from_aws(
            s3_bucket="noaa-wcsd-pds",
            file_url=file_ncei_idx_url,
            download_location=file_download_location_idx,
        )
        # Upload to GCP at the correct storage bucket location.
        upload_file_to_gcp_storage_bucket(
            file_name=file_name_idx,
            file_type=file_type,
            ship_name=ship_name,
            survey_name=survey_name,
            echosounder=echosounder,
            file_location=file_download_location_idx,
            gcp_bucket=gcp_bucket,
            data_source="NCEI",
            is_metadata=is_metadata,
            debug=debug,
        )
    return


def download_netcdf_file(
    file_name: str = "",
    file_type: str = "netcdf",
    ship_name: str = "",
    survey_name: str = "",
    echosounder: str = "",
    data_source: str = "",
    file_download_location: str = "",
    gcp_bucket: storage.Client.bucket = None,
    is_metadata: bool = False,
    debug: bool = False,
):
    """ENTRYPOINT FOR END-USERS
    Downloads a netcdf file from GCP storage bucket for use on your workstation.
    Works as follows:
        1. Checks if the exact netcdf exists in gcp.
            a. If it doesn't exists, prompts user to download it first.
            b. If it exists, downloads to the `file_download_location`.

    Args:
        file_name (str, optional): The file name (includes extension). Defaults to "".
        file_type (str, optional): The file type (do not include the dot "."). Defaults to "".
        ship_name (str, optional): The ship name associated with this survey. Defaults to "".
        survey_name (str, optional): The survey name/identifier. Defaults to "".
        echosounder (str, optional): The echosounder used to gather the data. Defaults to "".
        data_source (str, optional): The source of the file. Necessary due to the
            way the storage bucket is organized. Can be one of ["NCEI", "OMAO", "HDD"].
            Defaults to "".
        file_download_location (str, optional): The local file path you want to store your
            file in. Defaults to "".
        gcp_bucket (storage.Client.bucket, optional): The GCP bucket object used to download
            the file. Defaults to None.
        is_metadata (bool, optional): Whether or not the file is a metadata file. Necessary since
            files that are considered metadata (metadata json, or readmes) are stored
            in a separate directory. Defaults to False.
        debug (bool, optional): Whether or not to print debug statements. Defaults to False.
    """

    # User-error-checking
    assert (
        file_name != ""
    ), "Please provide a valid file name with the file extension (ex. `2107RL_CW-D20210813-T220732.raw`)"
    assert file_type != "", "Please provide a valid file type."
    assert (
        file_type in config.VALID_FILETYPES
    ), f"Please provide a valid file type (extension) from the following: {config.VALID_FILETYPES}"
    assert (
        ship_name != ""
    ), "Please provide a valid ship name (Title_Case_With_Underscores_As_Spaces)."
    assert survey_name != "", "Please provide a valid survey name."
    assert echosounder != "", "Please provide a valid echosounder."
    assert (
        echosounder in config.VALID_ECHOSOUNDERS
    ), f"Please provide a valid echosounder from the following: {config.VALID_ECHOSOUNDERS}"
    assert (
        data_source != ""
    ), f"Please provide a valid data source from the following: {config.VALID_DATA_SOURCES}"
    assert (
        data_source in config.VALID_DATA_SOURCES
    ), f"Please provide a valid data source from the following: {config.VALID_DATA_SOURCES}"
    assert (
        file_download_location != ""
    ), "Please provide a valid file download locaiton (a directory)."
    assert (
        os.path.isdir(file_download_location) == True
    ), f"File download locaiton `{file_download_location}` is not found to be a valid path, please reformat it."

    # Create vars for use later.
    file_download_location = os.sep.join(
        [os.path.normpath(file_download_location), file_name]
    )
    gcp_storage_bucket_location = parse_correct_gcp_storage_bucket_location(
        file_name=file_name,
        file_type=file_type,
        ship_name=ship_name,
        survey_name=survey_name,
        echosounder=echosounder,
        data_source=data_source,
        is_metadata=is_metadata,
        debug=debug,
    )

    # Check if the netcdf exists in GCP:
    netcdf_exists_in_gcp = check_if_netcdf_file_exists_in_gcp(
        file_name=file_name,
        file_type=file_type,
        ship_name=ship_name,
        survey_name=survey_name,
        echosounder=echosounder,
        data_source=data_source,
        gcp_storage_bucket_location=gcp_storage_bucket_location,
        gcp_bucket=gcp_bucket,
        debug=debug,
    )
    if netcdf_exists_in_gcp:
        print(f"FILE LOCATED IN GCP: `{gcp_storage_bucket_location}`\nDOWNLOADING...")
        utils.cloud_utils.download_file_from_gcp(
            gcp_bucket=gcp_bucket,
            blob_file_path=gcp_storage_bucket_location,
            local_file_path=file_download_location,
            debug=debug,
        )
        print(f"FILE `{file_name}` DOWNLOADED TO `{file_download_location}`")
        return
    else:
        print(
            f"NETCDF FILE `{file_name}` DOES NOT EXIST IN GCP AT THE LOCATION: `{gcp_storage_bucket_location}`."
        )
        print(f"PLEASE CONVERT AND UPLOAD THE RAW FILE FIRST VIA `download_raw_file`.")
        return


def convert_local_raw_to_netcdf(
    raw_file_location: str = "",
    netcdf_file_download_location: str = "",
    echosounder: str = "",
):
    """ENTRYPOINT FOR END-USERS
    Converts a local (on your computer) file from raw into netcdf using echopype.

    Args:
        raw_file_location (str, optional): The location of the raw file. Defaults to "".
        netcdf_file_download_location (str, optional): The location you want to
            download your netcdf file to. Defaults to "".
        echosounder (str, optional): The echosounder used. Can be one of ["EK80", "EK70"].
            Defaults to "".
    """

    netcdf_file_download_directory = os.sep.join(
        netcdf_file_download_location.split(os.sep)[:-1]
    )

    try:
        print("CONVERTING RAW TO NETCDF...")
        raw_file_echopype = open_raw(
            raw_file=raw_file_location, sonar_model=echosounder
        )
        raw_file_echopype.to_netcdf(save_path=netcdf_file_download_directory)
        print("CONVERTED.")
        return
    except Exception as e:
        print(f"COULD NOT CONVERT DUE TO ERROR {e}")
        return


def convert_raw_to_netcdf(
    file_name: str = "",
    file_type: str = "raw",
    ship_name: str = "",
    survey_name: str = "",
    echosounder: str = "",
    data_source: str = "",
    file_download_location: str = "",
    gcp_bucket: storage.Client.bucket = None,
    is_metadata: bool = False,
    debug: bool = False,
):
    """ENTRYPOINT FOR END-USERS
    This function allows one to convert a file from raw to netcdf. Then uploads
    the file to GCP storage for caching.

    Args:
        file_name (str, optional): The file name (includes extension). Defaults to "".
        file_type (str, optional): The file type (do not include the dot "."). Defaults to "".
        ship_name (str, optional): The ship name associated with this survey. Defaults to "".
        survey_name (str, optional): The survey name/identifier. Defaults to "".
        echosounder (str, optional): The echosounder used to gather the data. Defaults to "".
        data_source (str, optional): The source of the file. Necessary due to the
            way the storage bucket is organized. Can be one of ["NCEI", "OMAO", "HDD"].
            Defaults to "".
        file_download_location (str, optional): The local file path you want to store your
            file in. Defaults to "".
        gcp_bucket (storage.Client.bucket, optional): The GCP bucket object used to download
            the file. Defaults to None.
        is_metadata (bool, optional): Whether or not the file is a metadata file. Necessary since
            files that are considered metadata (metadata json, or readmes) are stored
            in a separate directory. Defaults to False.
        debug (bool, optional): Whether or not to print debug statements. Defaults to False.
    """

    # User-error-checking
    assert (
        file_name != ""
    ), "Please provide a valid file name with the file extension (ex. `2107RL_CW-D20210813-T220732.raw`)"
    assert file_type != "", "Please provide a valid file type."
    assert (
        file_type in config.VALID_FILETYPES
    ), f"Please provide a valid file type (extension) from the following: {config.VALID_FILETYPES}"
    assert (
        ship_name != ""
    ), "Please provide a valid ship name (Title_Case_With_Underscores_As_Spaces)."
    assert survey_name != "", "Please provide a valid survey name."
    assert echosounder != "", "Please provide a valid echosounder."
    assert (
        echosounder in config.VALID_ECHOSOUNDERS
    ), f"Please provide a valid echosounder from the following: {config.VALID_ECHOSOUNDERS}"
    assert (
        data_source != ""
    ), f"Please provide a valid data source from the following: {config.VALID_DATA_SOURCES}"
    assert (
        data_source in config.VALID_DATA_SOURCES
    ), f"Please provide a valid data source from the following: {config.VALID_DATA_SOURCES}"
    assert (
        file_download_location != ""
    ), "Please provide a valid file download location (a directory)."
    assert (
        os.path.isdir(file_download_location) == True
    ), f"File download location `{file_download_location}` is not found to be a valid path, please reformat it."

    # Create vars for use later.
    # file_download_location = os.sep.join([os.path.normpath(file_download_location), file_name])
    file_name_netcdf = ".".join(file_name.split(".")[:-1]) + ".nc"
    # needs to be a directory...
    file_download_location_netcdf = file_download_location
    file_path_netcdf = os.sep.join(
        [os.path.normpath(file_download_location_netcdf), file_name_netcdf]
    )
    gcp_storage_bucket_location_raw = parse_correct_gcp_storage_bucket_location(
        file_name=file_name,
        file_type=file_type,
        ship_name=ship_name,
        survey_name=survey_name,
        echosounder=echosounder,
        data_source="NCEI",
        is_metadata=is_metadata,
        debug=debug,
    )
    gcp_storage_bucket_location_netcdf = parse_correct_gcp_storage_bucket_location(
        file_name=file_name_netcdf,
        file_type="netcdf",
        ship_name=ship_name,
        survey_name=survey_name,
        echosounder=echosounder,
        data_source="NCEI",
        is_metadata=is_metadata,
        debug=debug,
    )
    # We check if the netcdf exists in GCP
    raw_file_exists_in_gcp_storage = cloud_utils.check_if_file_exists_in_gcp(
        bucket=gcp_bucket, file_path=gcp_storage_bucket_location_raw
    )

    # Here we check for a netcdf version of the raw file on GCP
    print(f"CHECKING FOR NETCDF VERSION ON GCP...")
    netcdf_exists_in_gcp = check_if_netcdf_file_exists_in_gcp(
        file_name=file_name_netcdf,
        file_type="netcdf",
        ship_name=ship_name,
        survey_name=survey_name,
        echosounder=echosounder,
        data_source="NCEI",
        gcp_storage_bucket_location=gcp_storage_bucket_location_netcdf,
        gcp_bucket=gcp_bucket,
        debug=debug,
    )
    if netcdf_exists_in_gcp:
        # Inform the user if a netcdf version exists in cache.
        download_netcdf_file(
            file_name=file_name_netcdf,
            file_type="netcdf",
            ship_name=ship_name,
            survey_name=survey_name,
            echosounder=echosounder,
            data_source=data_source,
            file_download_location=file_download_location,
            gcp_bucket=gcp_bucket,
            is_metadata=False,
            debug=debug,
        )
    else:
        print(
            f"FILE `{file_name}` DOES NOT EXIST AS NETCDF. DOWNLOADING/CONVERTING/UPLOADING RAW..."
        )

        # Download the raw file.
        download_raw_file(
            file_name=file_name,
            file_type=file_type,
            ship_name=ship_name,
            survey_name=survey_name,
            echosounder=echosounder,
            file_download_location=file_download_location,
            is_metadata=is_metadata,
            debug=debug,
        )

        # Convert the raw file to netcdf.
        convert_local_raw_to_netcdf(
            raw_file_location=os.sep.join(
                [os.path.normpath(file_download_location), file_name]
            ),
            netcdf_file_download_location=file_download_location_netcdf,
            echosounder=echosounder,
        )

        # Upload the netcdf to the correct location for parsing.
        print(f"file_path_netcdf {file_path_netcdf}")
        upload_file_to_gcp_storage_bucket(
            file_name=file_name_netcdf,
            file_type="netcdf",
            ship_name=ship_name,
            survey_name=survey_name,
            echosounder=echosounder,
            file_location=file_path_netcdf,
            gcp_bucket=gcp_bucket,
            data_source=data_source,
            is_metadata=False,
            debug=debug,
        )


def parse_correct_gcp_storage_bucket_location(
    file_name: str = "",
    file_type: str = "",
    ship_name: str = "",
    survey_name: str = "",
    echosounder: str = "",
    data_source: str = "",
    is_metadata: bool = False,
    debug: bool = False,
) -> str:
    """Calculates the correct gcp storage location based on data source, file
    type, and if the file is metadata or not.

    Args:
        file_name (str, optional): The file name (includes extension). Defaults to "".
        file_type (str, optional): The file type (not include the dot "."). Defaults to "".
        ship_name (str, optional): The ship name associated with this survey. Defaults to "".
        survey_name (str, optional): The survey name/identifier. Defaults to "".
        echosounder (str, optional): The echosounder used to gather the data. Defaults to "".
        data_source (str, optional): The source of the data. Can be one of ["NCEI", "OMAO"]. Defaults to "".
        is_metadata (bool, optional): Whether or not the file is a metadata file. Necessary since
            files that are considered metadata (metadata json, or readmes) are stored
            in a separate directory. Defaults to False.
        debug (bool, optional): Whether or not to print debug statements. Defaults to False.

    Returns:
        str: The correctly parsed GCP storage bucket location.
    """

    # Creating the correct upload location
    if is_metadata:
        gcp_storage_bucket_location = (
            f"{data_source}/{ship_name}/{survey_name}/{echosounder}/metadata/"
        )
    else:
        # Figure out if its a raw or idx file (belongs in raw folder)
        if file_type.lower() in config.RAW_DATA_FILE_TYPES:
            gcp_storage_bucket_location = f"{data_source}/{ship_name}/{survey_name}/{echosounder}/data/raw/{file_name}"
        elif file_type.lower() in config.CONVERTED_DATA_FILE_TYPES:
            gcp_storage_bucket_location = f"{data_source}/{ship_name}/{survey_name}/{echosounder}/data/netcdf/{file_name}"

    if debug:
        print(f"PARSED GCP_STORAGE_BUCKET_LOCATION: {gcp_storage_bucket_location}")

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


def check_if_netcdf_file_exists_in_gcp(
    file_name: str = "",
    file_type: str = "",
    ship_name: str = "",
    survey_name: str = "",
    echosounder: str = "",
    data_source: str = "",
    gcp_storage_bucket_location: str = "",
    gcp_bucket: storage.Bucket = None,
    debug: bool = False,
):

    assert (
        gcp_bucket is not None
    ), "Please provide a gcp_bucket object with `utils.cloud_utils.setup_gcp_storage()`"

    if gcp_storage_bucket_location != "":
        gcp_storage_bucket_location = parse_correct_gcp_storage_bucket_location(
            file_name=file_name,
            file_type="netcdf",
            survey_name=survey_name,
            ship_name=ship_name,
            echosounder=echosounder,
            data_source=data_source,
            is_metadata=False,
            debug=debug,
        )
    netcdf_gcp_storage_bucket_location = get_netcdf_gcp_location_from_raw_gcp_location(
        gcp_storage_bucket_location=gcp_storage_bucket_location
    )
    # check if the file exists in gcp
    return cloud_utils.check_if_file_exists_in_gcp(
        bucket=gcp_bucket, file_path=netcdf_gcp_storage_bucket_location
    )


def upload_file_to_gcp_storage_bucket(
    file_name: str = "",
    file_type: str = "",
    ship_name: str = "",
    survey_name: str = "",
    echosounder: str = "",
    file_location: str = "",
    gcp_bucket: storage.Client.bucket = None,
    data_source: str = "",
    is_metadata: bool = False,
    debug: bool = False,
):
    """Safely uploads a local file to the storage bucket. Will also check to see if the
    file already exists.

    Args:
        file_name (str, optional): The file name (includes extension). Defaults to "".
        file_type (str, optional): The file type (do not include the dot "."). Defaults to "".
        ship_name (str, optional): The ship name associated with this survey. Defaults to "".
        survey_name (str, optional): The survey name/identifier. Defaults to "".
        echosounder (str, optional): The echosounder used to gather the data. Defaults to "".
        file_location (str, optional): The local location of the file. Defaults to "".
        gcp_bucket (storage.Client.bucket, optional): The GCP bucket object used to upload
            the file. Defaults to None.
        data_source (str, optional): The source of the data. Can be one of ["NCEI", "OMAO", "HDD", "TEST"]. Defaults to "".
        is_metadata (bool, optional): Whether or not the file is a metadata file. Necessary since
            files that are considered metadata (metadata json, or readmes) are stored
            in a separate directory. Defaults to False.
        debug (bool, optional): Whether or not to print debug statements. Defaults to False.
    """

    gcp_storage_bucket_location = parse_correct_gcp_storage_bucket_location(
        file_name=file_name,
        file_type=file_type,
        ship_name=ship_name,
        survey_name=survey_name,
        echosounder=echosounder,
        data_source=data_source,
        is_metadata=is_metadata,
        debug=debug,
    )

    # Check if the file exists in GCP
    file_exists_in_gcp = cloud_utils.check_if_file_exists_in_gcp(
        gcp_bucket, file_path=gcp_storage_bucket_location
    )
    if file_exists_in_gcp:
        print(
            f"FILE `{file_name}` ALREADY EXISTS IN GCP AT `{gcp_storage_bucket_location}`."
        )
    else:
        try:
            print(
                f"UPLOADING FILE `{file_name}` TO GCP AT `{gcp_storage_bucket_location}`..."
            )
            # Upload to storage bucket.
            utils.cloud_utils.upload_file_to_gcp_bucket(
                bucket=gcp_bucket,
                blob_file_path=gcp_storage_bucket_location,
                local_file_path=file_location,
                debug=debug,
            )
            print(f"UPLOADED.")
        except Exception as e:
            print(
                f"COULD NOT UPLOAD FILE {file_name} TO GCP ({gcp_storage_bucket_location}) STORAGE BUCKET DUE TO THE FOLLOWING ERROR:\n{e}"
            )

    return


def upload_local_raw_and_idx_files_from_directory_to_gcp_storage_bucket(
    directory: str = "",
    ship_name: str = "",
    survey_name: str = "",
    echosounder: str = "",
    debug: bool = False,
):
    """ENTRYPOINT FOR END-USERS
    Uploads all of the .raw (and their corresponding .idx) files from a directory
    into the appropriate location in the GCP storage bucket.
    NOTE: Assumes that all files share the same metadata.

    Args:
        directory (str, optional): The directory which contains all of the files
            you want to upload. Defaults to "".
        ship_name (str, optional): The ship name associated with this survey. Defaults to "".
        survey_name (str, optional): The survey name/identifier. Defaults to "".
        echosounder (str, optional): The echosounder used to gather the data. Defaults to "".
        debug (bool, optional): Whether or not to print debug statements. Defaults to False.
    """

    # Warn user that this function assumes the same metadata for all files within directory.
    print(
        f"WARNING: THIS FUNCTION ASSUMES THAT ALL FILES WITHIN THIS DIRECTORY ARE FROM THE SAME SHIP, SURVEY, AND ECHOSOUNDER."
    )
    directory = os.path.normpath(directory)
    # Assert that the directory exists
    assert os.path.isdir(
        directory
    ), f"The location provided `{directory}` does not match a directory."
    # Check (glob) for raw and idx files.
    raw_files = [x for x in glob.glob(os.sep.join([directory, "*.raw"]))]
    idx_files = [x for x in glob.glob(os.sep.join([directory, "*.idx"]))]
    # TODO: Look for netcdf and upload it as well
    # Let the user know how many of each file has been found to upload.
    print(f"FOUND {len(raw_files)} RAW FILES | {len(idx_files)} IDX FILES")

    # Upload each raw file to gcp
    for raw_file in raw_files:
        file_name = raw_file.split(os.sep)[-1]
        gcp_storage_bucket_location = parse_correct_gcp_storage_bucket_location(
            file_name=file_name,
            file_type="raw",
            ship_name=ship_name,
            survey_name=survey_name,
            echosounder=echosounder,
            data_source="HDD",
            is_metadata=False,
            debug=debug,
        )
        raw_file_exists = cloud_utils.check_if_file_exists_in_gcp(
            bucket=gcp_bucket, file_path=gcp_storage_bucket_location
        )
        if raw_file_exists:
            print(
                f"FILE `{file_name}` ALREADY EXISTS IN THE GCP STORAGE BUCKET AT `{gcp_storage_bucket_location}`"
            )
        else:
            # Upload raw to GCP at the correct storage bucket location.
            upload_file_to_gcp_storage_bucket(
                file_name=file_name,
                file_type="raw",
                ship_name=ship_name,
                survey_name=survey_name,
                echosounder=echosounder,
                file_location=raw_file,
                gcp_bucket=gcp_bucket,
                data_source="HDD",
                is_metadata=False,
                debug=debug,
            )

    # Upload each idx file to gcp
    for idx_file in idx_files:
        file_name = idx_file.split(os.sep)[-1]
        gcp_storage_bucket_location = parse_correct_gcp_storage_bucket_location(
            file_name=file_name,
            file_type="idx",
            ship_name=ship_name,
            survey_name=survey_name,
            echosounder=echosounder,
            data_source="HDD",
            is_metadata=False,
            debug=debug,
        )
        idx_file_exists = cloud_utils.check_if_file_exists_in_gcp(
            bucket=gcp_bucket, file_path=gcp_storage_bucket_location
        )
        if idx_file_exists:
            print(
                f"FILE `{file_name}` ALREADY EXISTS IN THE GCP STORAGE BUCKET AT `{gcp_storage_bucket_location}`"
            )
        else:
            # Upload idx to GCP at the correct storage bucket location.
            upload_file_to_gcp_storage_bucket(
                file_name=file_name,
                file_type="idx",
                ship_name=ship_name,
                survey_name=survey_name,
                echosounder=echosounder,
                file_location=raw_file,
                gcp_bucket=gcp_bucket,
                data_source="HDD",
                is_metadata=False,
                debug=debug,
            )


if __name__ == "__main__":
    # set up storage objects
    s3_client, s3_resource, s3_bucket = utils.cloud_utils.create_s3_objs()
    gcp_stor_client, gcp_bucket_name, gcp_bucket = (
        utils.cloud_utils.setup_gcp_storage_objs()
    )

    upload_local_raw_and_idx_files_from_directory_to_gcp_storage_bucket(
        directory="./test_data_dir",
        ship_name="Bristol_Explorer",
        survey_name="BE201301",
        echosounder="ES60",
        debug=False,
    )
    # survey_stuff = get_all_objects_from_survey_ncei(ship_name="Reuben_Lasker",
    #                                  survey_name="RL2107",
    #                                  bucket=bucket)
    # print(survey_stuff)
    # resp = s3_resource.(bucket="noaa-wcsd-pds",
    #                           prefix="data/raw/Reuben_Lasker/RL2107")
    # print(resp)

    # print(utils.cloud_utils.count_objects_in_bucket_location(prefix="data/raw/Reuben_Lasker/RL2107/",
    #                                              bucket=bucket))

    # file_name, file_type, echosounder, survey_name, ship_name = parse_variables_from_ncei_file_url(url="https://noaa-wcsd-pds.s3.amazonaws.com/data/raw/Reuben_Lasker/RL2107/EK80/2107RL_CW-D20210813-T220732.raw")
    # print(parse_correct_gcp_storage_bucket_location(file_name=file_name,
    #                                                 file_type=file_type,
    #                                                 ship_name=ship_name,
    #                                                 survey_name=survey_name,
    #                                                 echosounder=echosounder,
    #                                                 data_source="NCEI",
    #                                                 is_metadata=False,
    #                                                 debug=True))
    # https://noaa-wcsd-pds.s3.amazonaws.com/data/raw/Reuben_Lasker/RL2107/EK80/2107RL_CW-D20210706-T172335.idx
    # print(utils.cloud_utils.check_if_file_exists_in_s3(object_key="data/raw/Reuben_Lasker/RL2107/EK80/2107RL_CW-D20210706-T172335.idx",
    #                                  s3_resource=s3_resource,
    #                                  s3_bucket_name="noaa-wcsd-pds"))
    # download_raw_file(file_name="2107RL_CW-D20210813-T220732.raw",
    #                             file_type="raw",
    #                             ship_name="Reuben_Lasker",
    #                             survey_name="RL2107",
    #                             echosounder="EK80",
    #                             file_download_location=f"./",
    #                             is_metadata=False,
    #                             force_download_from_ncei=False,
    #                             debug=True)
    # print(utils.cloud_utils.check_if_file_exists_in_gcp(gcp_bucket, file_path="NCEI/Reuben_Lasker/RL2107/EK80/data/raw/2107RL_CW-D20210813-T220732a.raw"))
    # convert_local_raw_to_netcdf(raw_file_location="2107RL_CW-D20210813-T220732.raw",
    #                             netcdf_file_download_location="./2107RL_CW-D20210813-T220732.nc",
    #                             echosounder="EK80")

    # convert_raw_to_netcdf(file_name="2107RL_CW-D20210813-T220732.raw",
    #                       file_type="raw", ship_name="Reuben_Lasker",
    #                       survey_name="RL2107", echosounder="EK80",
    #                       data_source="NCEI", file_download_location="./",
    #                       gcp_bucket=gcp_bucket, is_metadata=False,
    #                       debug=False)
    # download_netcdf(file_name="2107RL_CW-D20210813-T220732.raw",
    #                 file_type="nc", ship_name="Reuben_Lasker",
    #                 survey_name="RL2107", echosounder="EK80",
    #                 file_download_location=".", gcp_bucket=gcp_bucket,
    #                 is_metadata=False,debug=False)

"""NTH: Not pass a filename, but file type, ship name, echosounder, date field, to match
with a file name(s).

We need to be able to support MULTIPLE file names.

multiple file names option.

add endpoint for multiple raw files

Keep api responses consistent

API should be predictable.

conversion to netcdf should have its own endpoint"""
