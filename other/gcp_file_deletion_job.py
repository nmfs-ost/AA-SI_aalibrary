"""This file contains the code necessary for automating file deletion within
the GCP storage buckets. File deletion is calculated at metadata upload time.
File deletion can be delayed using the function in this script, based on the
file name.
"""

import sys
import os
from typing import List
from datetime import datetime, timedelta
from google.cloud import bigquery, storage
import pandas as pd


def get_files_ready_to_be_deleted():
    """This function executes a query to get the GCP URI's of any files that
    have a DELETION_DATETIME before the current datetime. It returns the file
    URI's as a [str].
    """
    # Create vars for use later.
    query = """SELECT GCP_URI FROM
    `ggn-nmfs-aa-dev-1.metadata.aalibrary_file_metadata`
    WHERE DELETION_DATETIME <= CURRENT_DATETIME()"""
    gcp_bq_client = bigquery.Client(location="US")
    job = gcp_bq_client.query(query)
    df = job.result().to_dataframe()
    return df["GCP_URI"].tolist()


def delete_files_from_storage(uris: List[str] = None):
    """This function takes a list of GCP storage bucket URi's and deletes them.

    Args:
        uris ([str], optional): A list of URI strings. Defaults to None.
    """

    # Create vars for use later.
    bucket_name = "ggn-nmfs-aa-dev-1-data"
    bucket = storage.Client().get_bucket(bucket_or_name=bucket_name)
    files_deleted = 0
    file_deletion_errors = 0
    files_with_errors = []
    # TODO: delete associated idx and bot files.
    for uri in uris:
        try:
            blob = bucket.blob(uri)
            blob.delete()
            files_deleted += 1
        except Exception as e:
            print(f"File not found at {uri}.\n{e}")
            file_deletion_errors += 1
            files_with_errors.append(uri)
    print(f"Files with errors:\n{files_with_errors}")
    print(f"{files_deleted} Files Deleted. {file_deletion_errors} Errors.")


def get_deletion_datetime_of_file(file_name: str = ""):
    """Gets the DELETION_DATETIME of a file. Returns a datetime object.

    Args:
        file_name (str, optional): The file name. Defaults to "".
    """

    query = f"""SELECT DELETION_DATETIME
    FROM `ggn-nmfs-aa-dev-1.metadata.aalibrary_file_metadata`
    WHERE FILE_NAME = '{file_name}'"""
    gcp_bq_client = bigquery.Client(location="US")
    job = gcp_bq_client.query(query)
    file_deletion_datetime = (
        job.result().to_dataframe()["DELETION_DATETIME"].tolist()[0]
    )
    file_deletion_datetime = str(file_deletion_datetime)
    file_deletion_datetime = datetime.strptime(
        file_deletion_datetime, "%Y-%m-%d %H:%M:%S"
    )
    return file_deletion_datetime


def delay_file_deletion(file_name: str = "", days: int = 0):
    """Delays a file's DELETION_DATETIME by the number of days specified.

    Args:
        file_name (str, optional): The unique file name. Defaults to "".
        days (int, optional): The number of days by which to delay the file'
            execution. Defaults to 0.
    """
    # Get the file deletion datetime.
    file_deletion_datetime = get_deletion_datetime_of_file(file_name=file_name)
    # Extend it by the number of days specified.
    file_deletion_datetime = file_deletion_datetime + timedelta(days=days)

    query = f"""UPDATE `ggn-nmfs-aa-dev-1.metadata.aalibrary_file_metadata`
    SET DELETION_DATETIME = CAST("{str(file_deletion_datetime)}" AS DATETIME)
    WHERE FILE_NAME = '{file_name}' """
    gcp_bq_client = bigquery.Client(location="US")
    try:
        job = gcp_bq_client.query(query)
        job.result()
        print(
            f"File DELETION_DATETIME delayed by {days} day(s) to"
            f" {file_deletion_datetime}"
        )
    except Exception as e:
        print(f"Could not update DELETION_DATETIME due to:\n{e}")
        return


if __name__ == "__main__":
    # print(
    #     get_deletion_datetime_of_file(
    #         file_name="2107RL_CW-D20210916-T165047.raw"
    #     )
    # )
    delay_file_deletion(file_name="2107RL_CW-D20210916-T165047.raw", days=1)
    # uris = get_files_ready_to_be_deleted()
    # delete_files_from_storage(uris=uris)
