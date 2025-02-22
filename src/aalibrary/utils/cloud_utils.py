"""This file contains all utility functions for Active Acoustics."""

import traceback
from typing import List, Tuple
import gcsfs
from google.cloud import bigquery, storage
from botocore import UNSIGNED
from botocore.client import Config
import boto3


def setup_gbq_client_objs(
    location: str = "US", project_id: str = "ggn-nmfs-gsds-prod-1"
) -> Tuple[bigquery.Client, gcsfs.GCSFileSystem]:
    """Sets up Google Big Query client objects used to execute queries and
    such.

    Args:
        location (str, optional): The location of the big-query
            tables/database. This is usually set when creating the database in
            big query. Defaults to "US".
        project_id (str, optional): The project id that the big query instance
            belongs to. Defaults to "ggn-nmfs-gsds-prod-1".

    Returns:
        Tuple: The big query client object, along with an object for the Google
            Cloud Storage file system.
    """

    gcp_bq_client = bigquery.Client(location=location)

    gcp_gcs_file_system = gcsfs.GCSFileSystem(project=project_id)

    return gcp_bq_client, gcp_gcs_file_system


def setup_gcp_storage_objs(
    project_id: str = "ggn-nmfs-aa-dev-1",
    gcp_bucket_name: str = "ggn-nmfs-aa-dev-1-data",
) -> Tuple[storage.Client, str, storage.Client.bucket]:
    """Sets up Google Cloud Platform storage objects for use in accessing and
    modifying storage buckets.

    Args:
        project_id (str, optional): The project id of the project you want to
            access. Defaults to "ggn-nmfs-aa-dev-1".
        gcp_bucket_name (str, optional): The name of the exact bucket you want
            to access. Defaults to "ggn-nmfs-aa-dev-1-data".

    Returns:
        Tuple[storage.Client, str, storage.Client.bucket]: The storage client,
            followed by the GCP bucket name (str) and then the actual bucket
            object itself (which will be executing the commands used in this
            api).
    """

    gcp_stor_client = storage.Client(project=project_id)

    gcp_bucket = gcp_stor_client.bucket(gcp_bucket_name)

    return (gcp_stor_client, gcp_bucket_name, gcp_bucket)


def upload_file_to_gcp_bucket(
    bucket: storage.Client.bucket,
    blob_file_path: str,
    local_file_path: str,
    debug: bool = False,
):
    """Uploads a file to the blob storage bucket.

    Args:
        bucket (storage.Client.bucket): The bucket object used for uploading.
        blob_file_path (str): The blob's file path.
            Ex. "data/itds/logs/execute_code_files/temp.csv"
            NOTE: This must include the file name as well as the extension.
        local_file_path (str): The local file path you wish to upload to the
            blob.
        debug (bool): Whether or not to print debug statements.
    """

    if not bucket:
        _, _, bucket = setup_gcp_storage_objs()

    blob = bucket.blob(blob_file_path, chunk_size=1024 * 1024 * 1)
    # Upload a new blob
    try:
        blob.upload_from_filename(local_file_path)
        if debug:
            print("New data uploaded to {}".format(blob.name))
    except Exception:
        print(traceback.format_exc())
        raise


def create_s3_objs(bucket_name: str = "noaa-wcsd-pds") -> Tuple:
    """Creates the s3 objects needed for using boto3 for a particular bucket.

    Args:
        bucket_name (str, optional): The bucket you want to refer to. The
            default points to the NCEI bucket. Defaults to "noaa-wcsd-pds".

    Returns:
        Tuple: The s3 client (used for certain portions of the boto3 api), the
            s3 resource (newer, more used object for accessing s3 buckets), and
            the actual s3 bucket itself.
    """

    # Setup access to S3 bucket as an anonymous user
    s3_client = boto3.client(
        "s3",
        aws_access_key_id="",
        aws_secret_access_key="",
        config=Config(signature_version=UNSIGNED),
    )
    s3_resource = boto3.resource(
        "s3",
        aws_access_key_id="",
        aws_secret_access_key="",
        config=Config(signature_version=UNSIGNED),
    )

    s3_bucket = s3_resource.Bucket(bucket_name)

    return s3_client, s3_resource, s3_bucket


def count_objects_in_s3_bucket_location(
    prefix: str = "", bucket: boto3.resource = None
) -> int:
    """Counts the number of objects within a bucket location.
    NOTE: This DOES NOT include folders, as those do not count as objects.

    Args:
        prefix (str, optional): The bucket location. Defaults to "".
        bucket (boto3.resource, optional): The bucket resource object.
        Defaults to None.

    Returns:
        int: The count of objects within the location.
    """

    count = sum(1 for _ in bucket.objects.filter(Prefix=prefix).all())
    return count


def count_subdirectories_in_s3_bucket_location(
    prefix: str = "", bucket: boto3.resource = None
) -> int:
    """Counts the number of subdirectories within a bucket location.

    Args:
        prefix (str, optional): The bucket location. Defaults to "".
        bucket (boto3.resource, optional): The bucket resource object.
        Defaults to None.

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


def get_subdirectories_in_s3_bucket_location(
    prefix: str = "", bucket: boto3.resource = None, return_full_paths: bool = False
) -> List[str]:
    """Gets a list of all the subdirectories in a specific bucket location
    (called a prefix). The return can be with full paths (root to folder
    inclusive), or just the folder names.

    Args:
        prefix (str, optional): The bucket location. Defaults to "".
        bucket (boto3.resource, optional): The bucket resource object.
            Defaults to None.
        return_full_paths (bool, optional): Whether or not you want a full
            path from bucket root to the subdirectory returned. Set to false
            if you only want the subdirectory names listed. Defaults to False.

    Returns:
        List[str]: A list of strings, each being the subdirectory. Whether
            these are full paths or not are specified by the
            `return_full_paths` parameter.
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


def list_all_objects_in_s3_bucket_location(
    prefix: str = "", bucket: boto3.resource = None
):
    """Lists all of the objects in a s3 bucket location denoted by `prefix`.
    Returns a list containing tuples. Each tuple refers to one object, with
    the first item in the tuple being the full path of the object, and the
    second item being the object name (file name).

    Args:
        prefix (str, optional): The bucket location. Defaults to "".
        bucket (boto3.resource, optional): The bucket resource object.
        Defaults to None.

    Returns:
        List[Tuple(str, str)]: Each tuple refers to one object, with the first
            item in the tuple being the full path of the object, and the
            second item being the object name (file name).
    """

    object_keys = []
    for obj in bucket.objects.filter(Prefix=prefix):
        object_keys.append((obj.key, obj.key.split("/")[-1]))

    return object_keys


def check_if_file_exists_in_gcp(
    bucket: storage.Bucket = None, file_path: str = ""
) -> bool:
    """Checks whether a particular file exists in GCP using the file path
    (blob).

    Args:
        bucket (storage.Bucket, optional): The bucket object used to check for
            the file. Defaults to None.
        file_path (str, optional): The blob file path within the bucket.
            Defaults to "".

    Returns:
        Bool: True if the file already exists, False otherwise.
    """

    return bucket.blob(file_path).exists()


def download_file_from_gcp(
    gcp_bucket: storage.Client.bucket,
    blob_file_path: str,
    local_file_path: str,
    debug: bool = False,
):
    """Downloads a file from the blob storage bucket.

    Args:
        bucket (storage.Client.bucket): The bucket object used for downloading
            from.
        blob_file_path (str): The blob's file path.
            Ex. "data/itds/logs/execute_rasp_ii/temp.csv"
            NOTE: This must include the file name as well as the extension.
        local_file_path (str): The local file path you wish to download the
            blob to.
        debug (bool): Whether or not to print debug statements.
    """

    blob = gcp_bucket.blob(blob_file_path, chunk_size=1024 * 1024 * 1)
    # Download from blob
    try:
        blob.download_to_filename(local_file_path)
        if debug:
            print("New data downloaded to {}".format(local_file_path))
    except Exception:
        print(traceback.format_exc())
        raise


def delete_file_from_gcp(
    gcp_bucket: storage.Client.bucket, blob_file_path: str, debug: bool = False
):
    file_exists_in_gcp = check_if_file_exists_in_gcp(gcp_bucket, blob_file_path)
    assert file_exists_in_gcp, f"File does not exist in GCP at `{blob_file_path}`."

    blob = gcp_bucket.blob(blob_file_path)
    try:
        blob.delete()
        return
    except Exception:
        print(traceback.format_exc())
        raise


def check_if_file_exists_in_s3(
    object_key: str = "", s3_resource: boto3.resource = None, s3_bucket_name: str = ""
) -> bool:
    """Checks to see if a file exists in an s3 bucket. Intended for use with
    NCEI, but will work with other s3 buckets as well.

    Args:
        object_key (str, optional): The object key (location of the object).
            Defaults to "".
        s3_resource (boto3.resource, optional): The boto3 resource for this
            particular bucket. Defaults to None.
        s3_bucket_name (str, optional): The bucket name. Defaults to "".

    Returns:
        bool: True if the file exists within the bucket. False otherwise.
    """

    try:
        s3_resource.Object(s3_bucket_name, object_key).load()
        return True
    except Exception as e:
        # object key does not exist.
        print(e)
        return False
