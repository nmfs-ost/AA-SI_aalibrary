"""This file contains the code necessary for automating file deletion within
the GCP storage buckets. File deletion is calculated at metadata upload time.
File deletion can be delayed using the function in this script, based on the
file name.
"""

import sys
import os
from typing import List
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


if __name__ == "__main__":
    uris = get_files_ready_to_be_deleted()
    delete_files_from_storage(uris=uris)
