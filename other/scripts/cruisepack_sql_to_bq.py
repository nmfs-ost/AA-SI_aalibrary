"""This script is used to take previously uploaded Cruisepack database files
from Google Cloud Storage (GCS), format them, and load them into BigQuery (BQ).

NOTES ====================================================================
cruiseData.sqlite - Contains the cruise-level metadata.
localData.sqlite - Contains data such as organization, people, projects.
sourceData.sqlite - Contains data such as ships, sea areas, ports, and
    instruments.
"""

import os
import re
import ast
import sqlite3
from pprint import pprint
import traceback
from pathlib import Path
import json
import string
from typing import List

import pandas as pd
import pandas_gbq
from google.cloud import storage

all_possible_columns = [
    "ABSTRACT",
    "ABSTRACT_TEXT",
    "ARRIVAL_PORT",
    "ARRIVAL_TIME",
    "AUDIO_END_TIME",
    "AUDIO_START_TIME",
    "BIO_PATH",
    "CALIBRATION_PATH",
    "CAL_DATE",
    "CAL_LAT",
    "CAL_LOCATION",
    "CAL_LON",
    "CAL_PATH",
    "CAL_STATE",
    "CITATION_TITLE",
    "CREATION_TIME",
    "CRUISE_ID",
    "CRUISE_TITLE",
    "CTD_PATH",
    "DATASETS",
    "DATA_COMMENT",
    "DEPARTURE_PORT",
    "DEPARTURE_TIME",
    "DEPLOYMENT_ALIAS",
    "DEPLOYMENT_ID",
    "DEPLOY_BOTTOM_DEPTH",
    "DEPLOY_INSTRUMENT_DEPTH",
    "DEPLOY_LAT",
    "DEPLOY_LON",
    "DEPLOY_SCIENTIST_1",
    "DEPLOY_SCIENTIST_1_UUID",
    "DEPLOY_SCIENTIST_2",
    "DEPLOY_SCIENTIST_2_UUID",
    "DEPLOY_SCIENTIST_3",
    "DEPLOY_SCIENTIST_3_UUID",
    "DEPLOY_SCIENTIST_COUNT",
    "DEPLOY_SHIP",
    "DEPLOY_TIME",
    "DESTINATION_PATH",
    "DOCS_PATH",
    "FILE_ABSTRACT",
    "FUNDERS",
    "FUNDER_1",
    "FUNDER_1_UUID",
    "FUNDER_2",
    "FUNDER_2_UUID",
    "FUNDER_3",
    "FUNDER_3_UUID",
    "FUNDER_COUNT",
    "ID",
    "INSTRUMENT_1",
    "INSTRUMENT_1_UUID",
    "INSTRUMENT_2",
    "INSTRUMENT_2_UUID",
    "INSTRUMENT_3",
    "INSTRUMENT_3_UUID",
    "INSTRUMENT_4",
    "INSTRUMENT_4_UUID",
    "INSTRUMENT_5",
    "INSTRUMENT_5_UUID",
    "INSTRUMENT_6",
    "INSTRUMENT_6_UUID",
    "INSTRUMENT_COUNT",
    "INSTRUMENT_ID",
    "INSTRUMENT_TYPE",
    "MASTER_RELEASE_DATE",
    "METADATA_AUTHOR",
    "META_AUTHOR_UUID",
    "OMICS",
    "OTHER_PATH",
    "PACKAGE_ID",
    "POINT_COUNT",
    "POS_PATH",
    "PROJECT",
    "PROJECTS",
    "PROJECT_1",
    "PROJECT_2",
    "PROJECT_3",
    "PROJECT_COUNT",
    "PUBLICATION_DATE",
    "PURPOSE",
    "PURPOSE_TEXT",
    "RECOVER_BOTTOM_DEPTH",
    "RECOVER_INSTRUMENT_DEPTH",
    "RECOVER_LAT",
    "RECOVER_LON",
    "RECOVER_SCIENTIST_1",
    "RECOVER_SCIENTIST_1_UUID",
    "RECOVER_SCIENTIST_2",
    "RECOVER_SCIENTIST_2_UUID",
    "RECOVER_SCIENTIST_3",
    "RECOVER_SCIENTIST_3_UUID",
    "RECOVER_SCIENTIST_COUNT",
    "RECOVER_SHIP",
    "RECOVER_TIME",
    "SCIENTISTS",
    "SCIENTIST_1",
    "SCIENTIST_1_UUID",
    "SCIENTIST_2",
    "SCIENTIST_2_UUID",
    "SCIENTIST_3",
    "SCIENTIST_3_UUID",
    "SCIENTIST_4",
    "SCIENTIST_4_UUID",
    "SCIENTIST_5",
    "SCIENTIST_5_UUID",
    "SCIENTIST_6",
    "SCIENTIST_6_UUID",
    "SCIENTIST_COUNT",
    "SCS_PATH",
    "SEA_AREA",
    "SEGMENT_ID",
    "SHIP",
    "SHIP_NAME_NORMALIZED",
    "SHIP_UUID",
    "SITE_ALIAS",
    "SITE_CRUISE",
    "SITE_OR_CRUISE",
    "SOURCE_PATH",
    "SPONSORS",
    "SPONSOR_1",
    "SPONSOR_1_UUID",
    "SPONSOR_2",
    "SPONSOR_2_UUID",
    "SPONSOR_3",
    "SPONSOR_3_UUID",
    "SPONSOR_4",
    "SPONSOR_4_UUID",
    "SPONSOR_5",
    "SPONSOR_5_UUID",
    "SPONSOR_6",
    "SPONSOR_6_UUID",
    "SPONSOR_COUNT",
    "TEMP_PATH",
    "TITLE",
    "UPDATE_TIME",
    "USE",
    "XBT_PATH",
]


def list_all_objects_in_gcp_bucket_location(
    location: str = "", bucket_name: str = None
) -> List[str]:
    """Gets all of the files within a GCP storage bucket location.

    Args:
        location (str, optional): The location to search for files. Defaults
            to "".
            Ex. "NCEI/Reuben_Lasker/RL2107"
        bucket_name (str, optional): The name of the GCP storage bucket to use.
            Defaults to None.

    Returns:
        List[str]: A list of strings containing all URIs for each file in the
            bucket.
    """
    storage_client = storage.Client()
    gcp_bucket = storage_client.bucket(bucket_name)

    all_blobs_in_this_location = []
    for blob in gcp_bucket.list_blobs(prefix=location):
        all_blobs_in_this_location.append(blob.name)
    return all_blobs_in_this_location


def download_file_from_gcp(
    project_id: str = "",
    gcp_bucket_name: str = "",
    blob_file_path: str = "",
    local_file_path: str = "",
    debug: bool = False,
):
    """Downloads a file from the blob storage bucket.

    Args:
        gcp_bucket (storage.Client.bucket): The bucket object used for
            downloading from.
        blob_file_path (str): The blob's file path.
            Ex. "data/itds/logs/execute_rasp_ii/temp.csv"
            NOTE: This must include the file name as well as the extension.
        local_file_path (str): The local file path you wish to download the
            blob to.
        debug (bool): Whether or not to print debug statements.
    """
    gcp_stor_client = storage.Client(project=project_id)
    gcp_bucket = gcp_stor_client.bucket(gcp_bucket_name)

    # Make dirs if they dont exist.
    local_file_path = Path(local_file_path)
    local_file_path.parent.mkdir(parents=True, exist_ok=True)

    blob = gcp_bucket.blob(blob_file_path, chunk_size=1024 * 1024 * 1)
    # Download from blob
    try:
        blob.download_to_filename(str(local_file_path))
        if debug:
            print(f"New data downloaded to {local_file_path}")
    except Exception:
        print(traceback.format_exc())
        raise


def download_file_from_gcp_as_bytes(
    project_id: str = "", gcp_bucket_name: str = "", blob_file_path: str = ""
) -> bytes:
    """Downloads a file from the blob storage bucket as a bytes object.

    Args:
        project_id (str): The id of the project. Defaults to "".
        gcp_bucket_name (str): The name of the bucket the file resides in.
            Defaults to "".
        gcp_bucket (storage.Client.bucket): The bucket object used for
            downloading from.
        blob_file_path (str): The blob's file path.
            Ex. "data/itds/logs/execute_rasp_ii/temp.csv"
            NOTE: This must include the file name as well as the extension.

    Returns:
        bytes-like-object: The bytes representation of the file.
    """
    gcp_stor_client = storage.Client(project=project_id)

    gcp_bucket = gcp_stor_client.bucket(gcp_bucket_name)

    blob = gcp_bucket.blob(blob_file_path, chunk_size=1024 * 1024 * 1)
    # Download from blob
    try:
        return blob.download_as_bytes()
    except Exception:
        print(traceback.format_exc())
        raise


def convert_time_to_bq_format(time_str):
    """Converts a time string from the format used in the Cruisepack database
    to a format that can be loaded into BigQuery."""
    # Example input: "2023-08-01T12:00:00Z"
    # Desired output: "2023-08-01 12:00:00"
    if time_str is None:
        return None
    if (time_str == pd.NaT) or (time_str == ""):
        return None
    else:
        try:
            dt = pd.to_datetime(time_str)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception as e:
            print(f"Error converting time string '{time_str}': {e}")
            return None


def clean_ship_name(ship_name):
    """Cleans the ship name by removing any unwanted characters or formatting."""

    if ship_name is None:
        return None
    # Remove parentheses and their contents,
    # e.g., "Ship Name (123)" -> "Ship Name"
    cleaned_name = re.sub(r"\s*\(.*?\)\s*", " ", ship_name)
    # Remove any extra whitespace and convert to title case.
    cleaned_name = cleaned_name.strip().title()
    return cleaned_name


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


def _preprocess_projects_str(projects_str) -> str:
    if projects_str is None:
        return ""

    projects_list = ast.literal_eval(projects_str)

    return ",".join(projects_list)


def _preprocess_scientists_str(scientists_str) -> str:
    """Converts a string into a list of dicts. Then extracts scientists names
    and concatenates them using `,` to return a string."""
    if scientists_str is None:
        return ""

    scientists_list = json.loads(scientists_str)

    scientists = [scientist_dict["name"] for scientist_dict in scientists_list]

    return ",".join(scientists)


def _preprocess_multiple_scientists(df: pd.DataFrame) -> pd.DataFrame:
    """Sometimes there are multiple columns with scientists, such as
    "SCIENTIST_1" and "SCIENTIST_2", and so on. This function takes care of
    that by creating a new column called "SCIENTISTS" for each row."""

    # Get scientist_N columns where n is a number.
    scientist_number_cols = []
    for col in df.columns:
        if col.lower().startswith("scientist_") and col[-1].isdigit():
            scientist_number_cols.append(col)

    # Go through each row and concat multiple scientist into one string and col
    df["SCIENTISTS"] = df[scientist_number_cols].apply(
        lambda row: ",".join(row.values.astype(str)), axis=1
    )


def _preprocess_sponsors_str(sponsors_str) -> str:
    """Converts a string into a list of dicts. Then extracts sponsors names
    and concatenates them using `,` to return a string."""
    if sponsors_str is None:
        return ""

    sponsors_list = json.loads(sponsors_str)

    sponsors = [sponsor_dict["name"] for sponsor_dict in sponsors_list]

    return ",".join(sponsors)


def preprocess_cruisepack_df(df: pd.DataFrame) -> pd.DataFrame:
    """Preprocesses the Cruisepack dataframe to make it ready for loading into
    BigQuery."""
    # Remove the first row if it is the example row.
    if (
        (len(df) >= 1)
        and (df.iloc[0]["PACKAGE_ID"] == "Select Existing Record")
        and (df.iloc[0]["SHIP"] == "Select Ship Name")
    ):
        df = df.drop(df.index[0])
    # Convert time columns to BigQuery format.
    for col in df.columns:
        if "time" in col.lower():
            df[col] = pd.to_datetime(
                df[col], format="%Y-%m-%d %H:%M:%S", errors="coerce"
            )
        # Clean ship names.
        if col.lower() == "ship":
            df[col] = df[col].apply(clean_ship_name)
            # Normalize name
            df["SHIP_NAME_NORMALIZED"] = df[col].apply(normalize_ship_name)
        if col.lower() == "platform":
            df[col] = df[col].apply(clean_ship_name)
            # Rename to `ship`
            df = df.rename(columns={col: "SHIP"})
        if col.lower() == "scientists":
            df[col] = df[col].apply(_preprocess_scientists_str)
        if col.lower() == "projects":
            df[col] = df[col].apply(_preprocess_projects_str)
        if col.lower() == "sponsors":
            df[col] = df[col].apply(_preprocess_sponsors_str)

    return df


def _get_all_columns(
    project_id: str = "ggn-nmfs-aa-dev-1",
    bucket_name: str = "",
    blob_file_path: str = "",
) -> List:
    """Helper function to return a set of all columns that exist across
    multiple sqlite files. This includes CruiseData and PackageData files."""

    # Process all files in directory, one-by-one
    if blob_file_path.endswith("/"):
        list_of_file_blobs = list_all_objects_in_gcp_bucket_location(
            location=blob_file_path, bucket_name=bucket_name
        )
        print("Blob folder detected. Files detected:")
        pprint(list_of_file_blobs)
    else:
        list_of_file_blobs = [blob_file_path]

    all_columns = set()
    files_with_multiple_scientist_columns = []
    for blob_file_path in list_of_file_blobs:
        print(f"Parsing `{blob_file_path}`...")
        local_file_path = os.path.normpath(
            os.sep.join([".", "temp", blob_file_path])
        )
        download_file_from_gcp(
            project_id=project_id,
            gcp_bucket_name=bucket_name,
            blob_file_path=blob_file_path,
            local_file_path=local_file_path,
        )

        df = read_sqlite_db_to_dict(local_file_path)
        for col in df.columns:
            if col == "SCIENTIST_1":
                files_with_multiple_scientist_columns.append(blob_file_path)
            all_columns.add(col)

    return list(sorted(list(all_columns)))


def _get_all_table_names(
    project_id: str = "ggn-nmfs-aa-dev-1",
    bucket_name: str = "",
    blob_file_path: str = "",
) -> List:
    """Helper function to get all the table names that exist in every database
    file."""
    # Process all files in directory, one-by-one
    if blob_file_path.endswith("/"):
        list_of_file_blobs = list_all_objects_in_gcp_bucket_location(
            location=blob_file_path, bucket_name=bucket_name
        )
        print("Blob folder detected. Files detected:")
        pprint(list_of_file_blobs)
    else:
        list_of_file_blobs = [blob_file_path]

    all_tables = set()
    for blob_file_path in list_of_file_blobs:
        print(f"Parsing `{blob_file_path}`...")
        local_file_path = os.path.normpath(
            os.sep.join([".", "temp", blob_file_path])
        )
        download_file_from_gcp(
            project_id=project_id,
            gcp_bucket_name=bucket_name,
            blob_file_path=blob_file_path,
            local_file_path=local_file_path,
        )

        # Get all table names in the database
        conn = sqlite3.connect(local_file_path)
        qry = "SELECT name FROM sqlite_master WHERE type='table';"
        df = pd.read_sql_query(qry, conn)
        print(df)
        for table_name in df["name"]:
            all_tables.add(table_name)

    return list(sorted(list(all_tables)))


def read_sqlite_db_to_dict(path_to_db):
    """Reads a SQLite database and returns its contents as a dictionary."""
    conn = sqlite3.connect(path_to_db)

    # Get all table names in the database
    tables_df = pd.read_sql_query(
        "SELECT name FROM sqlite_master WHERE type='table';", conn
    )
    tables = tables_df["name"].tolist()

    if "CRUISE_DATA" in tables:
        qry = "SELECT * FROM CRUISE_DATA"
    elif "DEPLOYMENT_DATA" in tables:
        qry = "SELECT * FROM DEPLOYMENT_DATA"
    df = pd.read_sql_query(qry, conn)
    print(df)
    conn.close()

    # Clean and preprocess the dataframe.
    df = preprocess_cruisepack_df(df)

    return df


def cruisepack_sql_to_bq(
    dataset: str = "metadata",
    table_id: str = "cruisepack_data",
    project_id: str = "ggn-nmfs-aa-dev-1",
    bucket_name: str = "",
    blob_file_path: str = "",
):
    """This function takes the blob (gcs file path) of a previously used
    cruisepack file and converts its data to be parseable by BigQuery. The
    contents of the file are then backed up to GCP."""

    # Process all files in directory, one-by-one
    if blob_file_path.endswith("/"):
        list_of_file_blobs = list_all_objects_in_gcp_bucket_location(
            location=blob_file_path, bucket_name=bucket_name
        )
        print("Blob folder detected. Files detected:")
        pprint(list_of_file_blobs)
    else:
        list_of_file_blobs = [blob_file_path]

    for blob_file_path in list_of_file_blobs:
        print(f"Parsing `{blob_file_path}`...")
        local_file_path = os.path.normpath(
            os.sep.join([".", "temp", blob_file_path])
        )
        download_file_from_gcp(
            project_id=project_id,
            gcp_bucket_name=bucket_name,
            blob_file_path=blob_file_path,
            local_file_path=local_file_path,
        )

        df = read_sqlite_db_to_dict(local_file_path)

        print(df.dtypes)
        pandas_gbq.to_gbq(
            df,
            destination_table=f"{dataset}.{table_id}",
            project_id=project_id,
            if_exists="append",
        )
        print(f"Finished {blob_file_path}")


if __name__ == "__main__":
    # tables = _get_all_table_names(
    #     project_id="ggn-nmfs-aa-prod-1",
    #     bucket_name="ggn-nmfs-aa-prod-1-data",
    #     blob_file_path="cruisepack/NEFSC/NEFSC_michael_jech_8_packageData.sqlite",
    # )
    # pprint(tables)
    # cols = _get_all_columns(
    #     project_id="ggn-nmfs-aa-prod-1",
    #     bucket_name="ggn-nmfs-aa-prod-1-data",
    #     blob_file_path="cruisepack/",
    # )
    # pprint(cols)
    cruisepack_sql_to_bq(
        dataset="metadata",
        table_id="cruisepack_data",
        project_id="ggn-nmfs-aa-prod-1",
        bucket_name="ggn-nmfs-aa-prod-1-data",
        # blob_file_path="cruisepack/",
        blob_file_path="cruisepack/NEFSC/NEFSC_michael_jech_7_cruiseData.sqlite",
    )

    # # Read and clean local db and turn it into a dataframe.
    # df = read_sqlite_db_to_dict(path_to_db)
    # print(df.dtypes)
    # print(df.iloc[4])
    # # pprint(db_contents)
    # pandas_gbq.to_gbq(
    #     df,
    #     destination_table=f"{dataset}.{table_id}",
    #     project_id=project_id,
    #     if_exists="append",
    # )
