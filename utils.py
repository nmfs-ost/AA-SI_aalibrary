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


def setup_gbq_client_objs(location: str = "US",
              project_id: str = "ggn-nmfs-gsds-prod-1"):
    # Setup GBQ
    gcp_bq_client = bigquery.Client(location=location)

    gcp_gcs_file_system = gcsfs.GCSFileSystem(project=project_id)

    return gcp_bq_client, gcp_gcs_file_system


def setup_gbq_storage_objs(project_id: str = "ggn-nmfs-aa-dev-1",
                           gcp_bucket_name: str = "ggn-nmfs-aa-dev-1-data") -> Tuple[storage.Client, str, storage.Client.bucket]:
    # Setup storage

    gcp_stor_client = storage.Client(project = project_id)

    gcp_bucket = gcp_stor_client.bucket(gcp_bucket_name)

    return (gcp_stor_client, gcp_bucket_name, gcp_bucket)


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
            print("New data uploaded to {}".format(blob.name))
    except Exception as e:
        print(traceback.format_exc())
        raise


def create_s3_objs(bucket_name: str = "noaa-wcsd-pds"):
    """Creates the boto3 object used for downloading file objects."""

    # Setup access to S3 bucket as an anonymous user
    s3_client = boto3.client('s3',
                             aws_access_key_id='',
                             aws_secret_access_key='',
                             config=Config(signature_version=UNSIGNED))
    s3_resource = boto3.resource('s3',
                                 aws_access_key_id='',
                                 aws_secret_access_key='',
                                 config=Config(signature_version=UNSIGNED))
    
    s3_bucket = s3_resource.Bucket(bucket_name)
    
    return s3_client, s3_resource, s3_bucket


def count_objects_in_s3_bucket_location(prefix: str = "",
                                     bucket: boto3.resource = None) -> int:
    """Counts the number of objects within a bucket location.
    NOTE: This DOES NOT include folders, as those do not count as objects.

    Args:
        prefix (str, optional): The bucket locaiton. Defaults to "".
        bucket (boto3.resource, optional): The bucket resource object. Defaults to None.

    Returns:
        int: The count of objects within the location.
    """    
    
    count = sum(1 for _ in bucket.objects.filter(Prefix=prefix).all())
    return count


def count_subdirectories_in_s3_bucket_location(prefix: str = "",
                                            bucket: boto3.resource = None) -> int:
    """Counts the number of subdirectories within a bucket location.

    Args:
        prefix (str, optional): The bucket locaiton. Defaults to "".
        bucket (boto3.resource, optional): The bucket resource object. Defaults to None.

    Returns:
        int: The count of subdirectories within the location.
    """    
    
    subdirs = set()
    for obj in bucket.objects.filter(Prefix=prefix):
        prefix = "/".join(obj.key.split("/")[:-1])
        if len(prefix) and prefix not in subdirs:
            subdirs.add(prefix)
            # print(prefix + "/")
    return len(subdirs)


def get_subdirectories_in_s3_bucket_location(prefix: str = "",
                                          bucket: boto3.resource = None,
                                          return_full_paths: bool = False) -> List[str]:
    """Gets a list of all the subdirectories in a specific bucket location (called prefix).
    The return can be with full paths (root to folder inclusive), or just the folder
    names.

    Args:
        prefix (str, optional): The bucket location. Defaults to "".
        bucket (boto3.resource, optional): The bucket resource object. Defaults to None.
        return_full_paths (bool, optional): Whether or not you want a full path from
            bucket root to the subdirectory returned. Set to false if you only want
            the subdirectory names listed. Defaults to False.

    Returns:
        List[str]: A list of strings, each being the subdirectory. Whether these
            are full paths or not are specified by the `return_full_paths` parameter.
    """    

    subdirs = set()
    for obj in bucket.objects.filter(Prefix=prefix):
        prefix = "/".join(obj.key.split("/")[:-1])
        if len(prefix) and prefix not in subdirs:
            subdirs.add(prefix)
            # print(prefix + "/")
    if return_full_paths:
        return list(subdirs)
    else:
        subdirs = [x.split("/")[-1] for x in subdirs]
        return subdirs


def check_if_file_exists_in_gcp(bucket: storage.Bucket = None,
                                file_path: str = ""):
    return bucket.blob(file_path).exists()


def download_file_from_gcp(gcp_bucket: storage.Client.bucket,
                           blob_file_path: str,
                           local_file_path: str,
                           debug: bool = False):
    """Downloads a file from the blob storage bucket.

    Args:
        bucket (storage.Client.bucket): The bucket object used for downloading from.
        blob_file_path (str): The blob's file path.
            Ex. "data/itds/logs/execute_rasp_ii/temp.csv"
            NOTE: This must include the file name as well as the extension.
        local_file_path (str): The local file path you wish to download the blob to.
        debug (bool): Whether or not to print debug statements.
    """    

    blob = gcp_bucket.blob(blob_file_path,
                       chunk_size=1024*1024*1)
    # Download from blob
    try:
        blob.download_to_filename(local_file_path)
        if debug:
            print("New data downloaded to {}".format(local_file_path))
    except Exception as e:
        print(traceback.format_exc())
        raise


def check_if_file_exists_in_s3(object_key: str = "",
                               s3_resource: boto3.resource = None,
                               s3_bucket_name: str = ""):
    try:
        s3_resource.Object(s3_bucket_name, object_key).load()
        return True
    except Exception as e:
        # object key does not exist.
        print(e)
        return False