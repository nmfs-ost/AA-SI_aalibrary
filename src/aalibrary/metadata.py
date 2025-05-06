"""This file contains functions that have to do with metadata."""

import sys
from datetime import datetime, timezone
import subprocess
import logging
import platform
import boto3
import json

import numpy as np
import pandas as pd

import echopype

# For pytests-sake
if __package__ is None or __package__ == "":
    # uses current directory visibility
    import utils

    # from utils import nc_reader
    from utils.cloud_utils import list_all_objects_in_s3_bucket_location
    from raw_file import RawFile
else:
    # uses current package visibility
    from aalibrary import utils
    from aalibrary.raw_file import RawFile

    # from aalibrary.utils import nc_reader
    from aalibrary.utils.cloud_utils import (
        list_all_objects_in_s3_bucket_location,
    )


def create_metadata_json(
    rf: RawFile = None,
    debug: bool = False,
) -> pd.DataFrame:
    """Creates a JSON object containing metadata for the current user.

    Args:
        rf (RawFile, optional): The RawFile object associated with this file.
            Defaults to None.
        debug (bool, optional): Whether or not to print out the metadata json.
            Defaults to False.

    Returns:
        pd.DataFrame: The metadata dataframe for the `aalibrary_file_metadata`
            database table.
    """

    # Gets the current gcloud user's email
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

    file_datetime = datetime.strptime(
        rf.get_file_datetime_str(), "%Y-%m-%d %H:%M:%S"
    )

    metadata_json = {
        "FILE_NAME": rf.raw_file_name,
        "DATE_CREATED": datetime.now(timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
        "UPLOADED_BY": email,
        "ECHOPYPE_VERSION": echopype.__version__,
        "PYTHON_VERSION": sys.version.split(" ")[0],
        "NUMPY_VERSION": np.version.version,
        # maybe just add in echopype's reqs.
        # pip lock file - for current environment
        "NCEI_CRUISE_ID": rf.survey_name,
        "NCEI_URI": rf.raw_file_s3_object_key,
        "GCP_URI": rf.raw_gcp_storage_bucket_location,
        "FILE_DATETIME": file_datetime,
    }

    aalibrary_metadata_df = pd.json_normalize(metadata_json)
    # make sure data types are conserved before upload to BigQuery.
    aalibrary_metadata_df["DATE_CREATED"] = pd.to_datetime(
        aalibrary_metadata_df["DATE_CREATED"], format="%Y-%m-%d %H:%M:%S"
    )
    aalibrary_metadata_df["FILE_DATETIME"] = pd.to_datetime(
        aalibrary_metadata_df["FILE_DATETIME"], format="%Y-%m-%d %H:%M:%S"
    )

    if debug:
        print(aalibrary_metadata_df)
        logging.debug(aalibrary_metadata_df)

    return aalibrary_metadata_df


def create_and_upload_metadata_df(
    rf: RawFile = None,
    debug: bool = False,
):
    """Creates a metadata file with appropriate information. Then uploads it
    to the correct table in GCP.

    Args:
        rf (RawFile, optional): The RawFile object associated with this file.
            Defaults to None.
        debug (bool, optional): Whether or not to print debug statements.
            Defaults to False.
    """

    # Create the metadata file to be uploaded.
    metadata_df = create_metadata_json(
        rf=rf,
        debug=debug,
    )
    # TODO: take care of netcdf files, possibly upload to another table with\
    # their metadata.
    # If the file is a netcdf, we extract even more data from its headers.
    # if netcdf_local_file_location:
    #     # Extract the metadata
    #     netcdf_metadata = nc_reader.get_netcdf_header(
    #         file_path=netcdf_local_file_location
    #     )
    #     # Merge the netcdf metadata with the metadata we have created.
    #     metadata_df.update(netcdf_metadata)
    # # Extract the metadata string
    # metadata_json_str = json.dumps(metadata_df)
    # with open(f"./{file_name}.json", "w") as jf:
    #     jf.write(metadata_json_str)

    # Upload to GCP BigQuery
    metadata_df.to_gbq(
        destination_table="metadata.aalibrary_file_metadata",
        project_id="ggn-nmfs-aa-dev-1",
        if_exists="append",
    )

    return


def create_and_upload_metadata_df_for_netcdf(**kwargs):
    # TODO: implement
    ...


# TODO: implement this func at an appropriate place.
def upload_ncei_metadata_df_to_bigquery(
    ship_name: str = "",
    survey_name: str = "",
    download_location: str = "",
    s3_bucket: boto3.resource = None,
):
    """Finds the metadata obtained from a survey on NCEI, and uploads it to the
    `ncei_cruise_metadata` database table in bigquery. Also handles for extra
    database entries that are needed, such as uploading to the
    `ncei_instrument_metadata` when necessary.

    Args:
        ship_name (str, optional): The ship name associated with this survey.
            Defaults to "".
        survey_name (str, optional): The survey name/identifier.
            Defaults to "".
        bucket (boto3.resource, optional): The bucket resource object.
            Defaults to None.
        download_location (str, optional): The local download location for the
            file. Defaults to "".
    """

    # Find all metadata files within the metadata/ folder in NCEI
    all_metadata_obj_keys = list_all_objects_in_s3_bucket_location(
        prefix=f"data/raw/{ship_name}/{survey_name}/metadata", bucket=s3_bucket
    )

    # TODO: Download all metadata files to local for download? Even
    # calibration files?

    for obj_key, file_name in all_metadata_obj_keys:
        # Handle for main metadata file for upload to BigQuery.
        if file_name.endswith("metadata.json"):
            s3_bucket.download_file(obj_key, download_location)
            # Subroutine to parse this file and upload to gcp.
            _parse_and_upload_ncei_survey_level_metadata(
                survey_name=survey_name, file_location=download_location
            )


def _parse_and_upload_ncei_survey_level_metadata(
    ship_name: str = "",
    survey_name: str = "",
    file_location: str = "",
):
    """Handles upload of NCEI survey-level metadata to the
    `ncei_cruise_metadata` table in bigquery.
    """

    # Load the file as a json object
    with open(file_location, "r") as file:
        file_json = json.load(file)

    # Get all 'metadata_author'
    metadata_author_str = file_json["metadata_author"]["uuid"]
    # Get all 'sponsors'
    sponsors_str = []
    for sponsor_dict in file_json["sponsors"]:
        sponsors_str.append(sponsor_dict["uuid"])
    sponsors_str = ",".join(sponsors_str)
    # Get all 'funders'
    funders_str = []
    for funder_dict in file_json["funders"]:
        funders_str.append(funder_dict["uuid"])
    funders_str = ",".join(funders_str)
    # Get all 'scientists'
    scientists_str = []
    for scientist_dict in file_json["scientists"]:
        scientists_str.append(scientist_dict["uuid"])
    scientists_str = ",".join(scientists_str)
    # Get all 'projects'
    projects_str = ",".join(file_json["projects"])
    # Get all 'instruments'
    instruments_str = []
    for instrument_dict in file_json["instruments"]:
        instruments_str.append(instrument_dict["uuid"])
    instruments_str = ",".join(instruments_str)
    # Get all 'package_instruments'
    package_instruments_str = []
    for package_instrument_name in file_json["package_instruments"]:
        package_instruments_str.append(
            file_json["package_instruments"][package_instrument_name]["uuid"]
        )
    package_instruments_str = ",".join(package_instruments_str)

    ncei_survey_level_metadata_json = {
        "CRUISE_ID": survey_name,
        "SEGMENT_ID": file_json["segment_id"],
        "PACKAGE_ID": file_json["package_id"],
        "MASTER_RELEASE_DATE": file_json["master_release_date"],
        "SHIP": file_json["ship"],
        "SHIP_UUID": file_json["ship_uuid"],
        "DEPARTURE_PORT": file_json["departure_port"],
        "DEPARTURE_DATE": file_json["departure_date"],
        "ARRIVAL_PORT": file_json["arrival_port"],
        "ARRIVAL_DATE": file_json["arrival_date"],
        "SEA_AREA": file_json["sea_area"],
        "CRUISE_TITLE": file_json["cruise_title"],
        "CRUISE_PURPOSE": file_json["cruise_purpose"],
        "CRUISE_DESCRIPTION": file_json["cruise_description"],
        "METADATA_AUTHOR": metadata_author_str,
        "SPONSORS": sponsors_str,
        "FUNDERS": funders_str,
        "SCIENTISTS": scientists_str,
        "PROJECTS": projects_str,
        "INSTRUMENTS": instruments_str,
        "PACKAGE_INSTRUMENTS": package_instruments_str,
    }

    ncei_survey_level_metadata_df = pd.json_normalize(
        ncei_survey_level_metadata_json
    )
    # Upload to GCP BigQuery
    ncei_survey_level_metadata_df.to_gbq(
        destination_table="metadata.ncei_cruise_metadata",
        project_id="ggn-nmfs-aa-dev-1",
        if_exists="append",
    )

    # Upload the lookup table values for ncei persons
    _parse_and_upload_ncei_persons_metadata(file_json=file_json)


def _parse_and_upload_ncei_persons_metadata(file_json: dict):
    """Gets the persons in the file_json object, and uploads them to the
    correct table in BigQuery.

    Args:
        file_json (dict): The dictionary obtained from reading the survey level
            metadata file.
    """

    ncei_survey_persons = {
        "UUID": file_json["metadata_author"]["uuid"],
        "NAME": file_json["metadata_author"]["name"],
    }
    # Create DataFrame
    ncei_survey_persons_df = pd.json_normalize(ncei_survey_persons)

    subsections_to_cover = ["sponsors", "funders", "scientists"]
    for subsection in subsections_to_cover:
        for sub_dict in file_json[subsection]:
            temp_json = {"UUID": sub_dict["uuid"], "NAME": sub_dict["name"]}
            temp_df = pd.json_normalize(temp_json)
            ncei_survey_persons_df = pd.concat(
                [ncei_survey_persons_df, temp_df], axis=0
            )

    # Get rid of unnecessary duplicates
    ncei_survey_persons_df.drop_duplicates(subset=["UUID"], inplace=True)
    # TODO: Implement uploading only unique ids.
    # Get rid of UUIDs that already exist in the table.
    # qry = "SELECT DISTINCT(UUID) FROM `ggn-nmfs-aa-dev-1.metadata.
    # ncei_persons`"
    # uuids_df = utils.cloud_utils.bq_query_to_pandas(client=)

    # Upload to GCP BigQuery
    ncei_survey_persons_df.to_gbq(
        destination_table="metadata.ncei_persons",
        project_id="ggn-nmfs-aa-dev-1",
        if_exists="append",
    )


def get_metadata_in_df_format():
    """Retrieves the metadata associated with all objects in GCP in DataFrame
    format."""
    # TODO:
    ...


if __name__ == "__main__":
    gcp_stor_client, gcp_bucket_name, gcp_bucket = (
        utils.cloud_utils.setup_gcp_storage_objs()
    )
    s3_client, s3_resource, s3_bucket = utils.cloud_utils.create_s3_objs()
    rf = RawFile(
        file_name="2107RL_CW-D20210916-T165047.raw",
        file_type="raw",
        ship_name="Reuben_Lasker",
        survey_name="RL2107",
        echosounder="EK80",
        data_source="NCEI",
        file_download_directory="./test_data_dir",
        is_metadata=False,
        debug=True,
        s3_bucket=s3_bucket,
        s3_resource=s3_resource,
        # s3_bucket_name=s3_bucket_name,
        gcp_bucket=gcp_bucket,
        gcp_bucket_name=gcp_bucket_name,
        gcp_stor_client=gcp_stor_client,
    )
    # create_metadata_json(
    #     rf=rf,
    #     debug=True,
    # )
    # create_and_upload_metadata_df(
    #     rf=rf,
    #     debug=True,
    # )
    upload_ncei_metadata_df_to_bigquery(
        ship_name="Reuben_Lasker",
        survey_name="RL2107",
        download_location="RL2107_EK80_WCSD_EK80-metadata.json",
        s3_bucket=s3_bucket,
    )
