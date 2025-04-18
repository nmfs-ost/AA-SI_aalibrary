"""This file contains functions that have to do with metadata."""

import os
import sys
from datetime import datetime, timezone
import subprocess
import json
import logging
import platform

from google.cloud import storage
import numpy as np

import echopype

# For pytests-sake
if __package__ is None or __package__ == "":
    # uses current directory visibility
    import ingestion
    import utils
    from utils import nc_reader
else:
    # uses current package visibility
    from aalibrary import ingestion, utils
    from aalibrary.utils import nc_reader


def create_metadata_json(
    debug: bool = False,
):
    """Creates a JSON object containing metadata for the current user.

    Args:
        debug (bool, optional): Whether or not to print out the metadata json.
            Defaults to False.

    Returns:
        dict: The metadata json dict.
    """

    get_curr_user_email_cmd = ["gcloud", "config", "get-value", "account"]
    if platform.system() == "Windows":
        email = subprocess.run(
            get_curr_user_email_cmd, shell=True, capture_output=True, text=True
        ).stdout
    else:
        email = subprocess.run(
            get_curr_user_email_cmd, capture_output=True, text=True
        ).stdout
    email = email.replace("\n", "")
    # TODO: replace json with a pandas DataFrame.
    metadata_json = {
        "DATE_CREATED": datetime.now(timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        ),
        "UPLOADED_BY": email,
        "ECHOPYPE_VERSION": echopype.__version__,
        "PYTHON_VERSION": sys.version.split(' ')[0],
        "NUMPY_VERSION": np.version.version,
        # maybe just add in echopype's reqs.
        # pip lock file - for current environment as
    }
    if debug:
        logging.debug(metadata_json)

    return metadata_json


def create_and_upload_metadata_file(
    file_name: str = "",
    file_type: str = "",
    ship_name: str = "",
    survey_name: str = "",
    echosounder: str = "",
    data_source: str = "",
    gcp_bucket: storage.Bucket = None,
    netcdf_local_file_location: str = "",
    debug: bool = False,
):
    """Creates a metadata file with appropriate information. Then uploads it
    to the correct location in GCP.

    Args:
        file_name (str, optional): The file name (includes extension).
            Defaults to "".
        file_type (str, optional): The file type (do not include the dot ".").
            Defaults to "".
        ship_name (str, optional): The ship name associated with this survey.
            Defaults to "".
        survey_name (str, optional): The survey name/identifier.
            Defaults to "".
        echosounder (str, optional): The echosounder used to gather the data.
            Defaults to "".
        data_source (str, optional): The source of the file. Necessary due to
            the way the storage bucket is organized. Can be one of
            ["NCEI", "OMAO", "HDD"]. Defaults to "".
        gcp_bucket (storage.Client.bucket, optional): The GCP bucket object
            used to download the file. Defaults to None.
        netcdf_local_file_location (str, optional): The local file path for the
            netcdf that is to be uploaded. Necessary for extracting headers
            from the netcdf file. Defaults to "".
        debug (bool, optional): Whether or not to print debug statements.
            Defaults to False.
    """

    # Create the metadata file to be uploaded.
    metadata_json = create_metadata_json(debug=debug)
    # If the file is a netcdf, we extract even more data from its headers.
    if netcdf_local_file_location:
        # Extract the metadata
        netcdf_metadata = nc_reader.get_netcdf_header(
            file_path=netcdf_local_file_location
        )
        # Merge the netcdf metadata with the metadata we have created.
        metadata_json.update(netcdf_metadata)
    # Extract the metadata string
    metadata_json_str = json.dumps(metadata_json)
    with open(f"./{file_name}.json", "w") as jf:
        jf.write(metadata_json_str)

    # Upload to GCP
    ingestion.upload_file_to_gcp_storage_bucket(
        file_name=f"{file_name}.json",
        file_type=file_type,
        ship_name=ship_name,
        survey_name=survey_name,
        echosounder=echosounder,
        file_location=f"./{file_name}.json",
        gcp_bucket=gcp_bucket,
        data_source=data_source,
        is_metadata=True,
        debug=debug,
    )

    # Remove temp metadata file.
    os.remove(f"./{file_name}.json")

    return


def upload_metadata_df_to_bigquery():
    """Takes a metadata dataframe of a file, and uploads it to the
    `aalibrary_file_metadata` database table."""
    ...


def upload_ncei_metadata_df_to_bigquery():
    """Takes the metadata obtained from a survey on NCEI, and uploads it to the
    `ncei_cruise_metadata` database table in bigquery. Also handles for extra
    database entries that are needed, such as uploading to the 
    `ncei_instrument_metadata` when necessary."""
    ...


def get_metadata_in_df_format():
    """Retrieves the metadata associated with all objects in GCP in DataFrame
    format."""
    # TODO:
    ...


if __name__ == "__main__":
    gcp_stor_client, gcp_bucket_name, gcp_bucket = (
        utils.cloud_utils.setup_gcp_storage_objs()
    )

    create_and_upload_metadata_file(
        file_name="2107RL_CW-D20210813-T220732.raw",
        file_type="raw",
        ship_name="Reuben_Lasker",
        survey_name="RL2107",
        echosounder="EK80",
        data_source="NCEI",
        gcp_bucket=gcp_bucket,
        debug=True,
    )
