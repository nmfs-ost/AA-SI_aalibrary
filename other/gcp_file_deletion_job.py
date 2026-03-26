"""This file contains the code necessary for automating file deletion within
the GCP storage buckets. File deletion is calculated at metadata upload time.
File deletion can be delayed using the function in this script, based on the
file name.

The file should get executed weekly at 12:00 AM on Mondays using the crontab
command below:
0 0 * * 1 python gcp_file_deletion_job.py
"""

# pylint: disable=W1201,W1203


import sys
import os
from typing import List
from datetime import datetime, timedelta
import logging
from google.cloud import bigquery, storage


def get_files_ready_to_be_deleted(
    gcp_project_id: str = "ggn-nmfs-aa-dev-1",
) -> List[str]:
    """This function executes a query to get the GCP URI's of any files that
    have a DELETION_DATETIME before the current datetime. It returns the file
    URI's as a [str].

    Args:
        gcp_project_id (str, optional): The GCP project ID.
            Defaults to "ggn-nmfs-aa-dev-1".

    Returns:
        List[str]: A list of GCP URI's for files ready to be deleted.
    """
    # Create vars for use later.
    query = f"""SELECT GCP_URI FROM
    `{gcp_project_id}.metadata.aalibrary_file_metadata`
    WHERE DELETION_DATETIME <= CURRENT_DATETIME()"""
    gcp_bq_client = bigquery.Client(location="US")

    job = gcp_bq_client.query(query)
    df = job.result().to_dataframe()
    return df["GCP_URI"].tolist()


def delete_files_from_storage(
    uris: List[str] = None,
    bucket_name: str = "ggn-nmfs-aa-dev-1-data",
):
    """This function takes a list of GCP storage bucket URi's and deletes them.

    Args:
        uris ([str], optional): A list of URI strings.
            Defaults to None.
        bucket_name (str, optional): The name of the GCP storage bucket.
            Defaults to "ggn-nmfs-aa-dev-1-data".
    """

    # Create vars for use later.
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
            logging.exception(f"File not found at {uri}.\n{e}", exc_info=True)
            file_deletion_errors += 1
            files_with_errors.append(uri)
    logging.info(f"Files with errors:\n{files_with_errors}")
    logging.info(f"Files deleted: {files_deleted}")
    logging.info(
        f"{len(files_deleted)} Files Deleted With {file_deletion_errors} Errors."
    )


def get_deletion_datetime_of_file(
    file_name: str = "", gcp_project_id: str = "ggn-nmfs-aa-dev-1"
) -> datetime:
    """Gets the DELETION_DATETIME of a file. Returns a datetime object.

    Args:
        file_name (str, optional): The file name. Defaults to "".
        gcp_project_id (str, optional): The GCP project ID.
            Defaults to "ggn-nmfs-aa-dev-1".
    Returns:
        datetime: The DELETION_DATETIME of the file as a datetime object.
    """

    query = f"""SELECT DELETION_DATETIME
    FROM `{gcp_project_id}.metadata.aalibrary_file_metadata`
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
    # Set logging.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s|%(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(
                filename="./logs/gcp_file_deletion_job.log", mode="a"
            ),
            logging.StreamHandler(sys.stdout),
        ],
    )
    # print(
    #     get_deletion_datetime_of_file(
    #         file_name="2107RL_CW-D20210916-T165047.raw"
    #     )
    # )
    # delay_file_deletion(file_name="2107RL_CW-D20210916-T165047.raw", days=1)

    ### Standard execution of the file deletion job. ###
    logging.info("Starting GCP file deletion job.")
    # Get environment variable for gcp project.
    aalibrary_env = os.getenv("AALIBRARY_ENV")
    if aalibrary_env == "DEV":
        logging.info(
            "Executing GCP file deletion job in DEV environment"
            " `ggn-nmfs-aa-dev-1`."
        )
        GCP_PROJECT_ID = "ggn-nmfs-aa-dev-1"
        BUCKET_NAME = "ggn-nmfs-aa-dev-1-data"
    elif aalibrary_env == "PROD":
        logging.info(
            "Executing GCP file deletion job in PROD environment"
            " `ggn-nmfs-aa-prod-1`."
        )
        GCP_PROJECT_ID = "ggn-nmfs-aa-prod-1"
        BUCKET_NAME = "ggn-nmfs-aa-prod-1-data"
    else:
        logging.error(
            "`AALIBRARY_ENV` environment variable not set to 'dev' or 'prod'."
            " Exiting file deletion job."
        )
        sys.exit(1)
    uris = get_files_ready_to_be_deleted(gcp_project_id=GCP_PROJECT_ID)
    logging.info(f"Files ready to be deleted:\n{uris}")
    # delete_files_from_storage(uris=uris, bucket_name=BUCKET_NAME)
