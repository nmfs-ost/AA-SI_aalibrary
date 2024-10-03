"""This file contains all utility functions for Active Acoustics."""


import traceback
from typing import List, Tuple
import gcsfs
from google.cloud import bigquery, storage
from pprint import pprint
from google.cloud.exceptions import NotFound
from botocore import UNSIGNED
from botocore.client import Config
import boto3
import botocore


def setup_gbq_client_objs(location: str = "US",
              project_id: str = "ggn-nmfs-gsds-prod-1"):
    # Setup GBQ
    bq_client = bigquery.Client(location=location)

    gcs_file_system = gcsfs.GCSFileSystem(project=project_id)

    return bq_client, gcs_file_system


def setup_gbq_storage_objs(project_id: str = "ggn-nmfs-aa-dev-1",
                  bucket_name: str = "ggn-nmfs-aa-dev-1-data") -> Tuple[storage.Client, str, storage.Client.bucket]:
    # Setup storage

    stor_client = storage.Client(project = project_id)

    bucket = stor_client.bucket(bucket_name)

    return (stor_client, bucket_name, bucket)


def upload_file_to_gcp_bucket(bucket: storage.Client.bucket,
                              blob_file_path: str,
                              local_file_path: str,
                              debug: bool = False):
    """Uploads a file to the blob storage bucket.

    Args:
        bucket (storage.Client.bucket): The bucket object used for uploading.
        blob_file_path (str): The blob's file path.
            Ex. "data/itds/logs/execute_rasp_ii/temp.csv"
            NOTE: This must include the file name as well as the extension.
        local_file_path (str): The local file path you wish to upload to the blob.
        debug (bool): Whether or not to print debug statements.
    """

    if not bucket:
        _, _, bucket = setup_gbq_storage_objs()

    blob = bucket.blob(blob_file_path,
                       chunk_size=1024*1024*1)
    # Upload a new blob
    try:
        blob.upload_from_filename(local_file_path)
        if debug:
            print("New csv data uploaded to {}".format(blob.name))
    except Exception as e:
        print(traceback.format_exc())
        raise


def create_s3_objs(bucket: str = "noaa-wcsd-pds"):
    """Creates the boto3 object used for downloading file objects."""

    # Setup access to S3 bucket as an anonymous user
    s3 = boto3.resource(
        's3',
        aws_access_key_id='',
        aws_secret_access_key='',
        config=Config(signature_version=UNSIGNED),
        )
    
    bucket = s3.Bucket(bucket)
    
    return s3, bucket
