"""This file contains all utility functions for Active Acoustics."""

import traceback
from typing import List, Tuple
import gcsfs
from google.cloud import bigquery, storage
from botocore import UNSIGNED
from botocore.client import Config
import boto3

from aalibrary.raw_file import RawFile
from aalibrary.utils import cloud_utils, helpers
from aalibrary.utils.helpers import (
    get_netcdf_gcp_location_from_raw_gcp_location,
)


def setup_gbq_client_objs(
    location: str = "US", project_id: str = "ggn-nmfs-aa-dev-1"
) -> Tuple[bigquery.Client, gcsfs.GCSFileSystem]:
    """Sets up Google Big Query client objects used to execute queries and
    such.

    Args:
        location (str, optional): The location of the big-query
            tables/database. This is usually set when creating the database in
            big query. Defaults to "US".
        project_id (str, optional): The project id that the big query instance
            belongs to. Defaults to "ggn-nmfs-aa-dev-1".

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
    prefix: str = "",
    s3_client: boto3.resource = None,
    return_full_paths: bool = False,
    bucket_name: str = "noaa-wcsd-pds",
) -> List[str]:
    """Gets a list of all the subdirectories in a specific bucket location
    (called a prefix). The return can be with full paths (root to folder
    inclusive), or just the folder names.

    Args:
        prefix (str, optional): The bucket folder location. Defaults to "".
        s3_resource (boto3.resource, optional): The bucket resource object.
            Defaults to None.
        return_full_paths (bool, optional): Whether or not you want a full
            path from bucket root to the subdirectory returned. Set to false
            if you only want the subdirectory names listed. Defaults to False.
        bucket_name (str, optional): The bucket name. Defaults to
            "noaa-wcsd-pds".

    Returns:
        List[str]: A list of strings, each being the subdirectory. Whether
            these are full paths or not are specified by the
            `return_full_paths` parameter.
    """

    subdirs = set()
    result = s3_client.list_objects(
        Bucket=bucket_name, Prefix=prefix, Delimiter="/"
    )
    for o in result.get("CommonPrefixes"):
        subdir_full_path_from_prefix = o.get("Prefix")
        if return_full_paths:
            subdir = subdir_full_path_from_prefix
        else:
            subdir = subdir_full_path_from_prefix.replace(prefix, "")
            subdir = subdir.replace('/','')
        subdirs.add(subdir)
    return list(subdirs)


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
    file_exists_in_gcp = check_if_file_exists_in_gcp(
        gcp_bucket, blob_file_path
    )
    assert (
        file_exists_in_gcp
    ), f"File does not exist in GCP at `{blob_file_path}`."

    blob = gcp_bucket.blob(blob_file_path)
    try:
        blob.delete()
        return
    except Exception:
        print(traceback.format_exc())
        raise


def check_if_file_exists_in_s3(
    object_key: str = "",
    s3_resource: boto3.resource = None,
    s3_bucket_name: str = "",
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
    except Exception:
        # object key does not exist.
        # print(e)
        return False


def get_object_key_for_s3(
    file_url: str = "",
    file_name: str = "",
    file_type: str = "raw",
    ship_name: str = "",
    survey_name: str = "",
    echosounder: str = "",
):
    """Creates an object key for a file within s3 given the parameters above.

    Args:
        file_url (str, optional): The entire url to the file resource in s3.
            Starts with "https://" or "s3://". Defaults to "".
            NOTE: If this is specified, there is no need to provide the other
            parameters.
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
    """

    if file_url:
        # We replace the beginning of common file paths
        file_url = file_url.replace(
            "https://noaa-wcsd-pds.s3.amazonaws.com/", ""
        )
        file_url = file_url.replace("s3://noaa-wcsd-pds/", "")
        return file_url
    else:
        # We default to using the parameters to create an object key according
        # to NCEI standards.
        object_key = (
            f"data/raw/{ship_name}/{survey_name}/{echosounder}/{file_name}"
        )
        return object_key


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

    if gcp_storage_bucket_location != "":
        gcp_storage_bucket_location = (
            helpers.parse_correct_gcp_storage_bucket_location(
                file_name=file_name,
                file_type="netcdf",
                survey_name=survey_name,
                ship_name=ship_name,
                echosounder=echosounder,
                data_source=data_source,
                is_metadata=False,
                debug=debug,
            )
        )
    netcdf_gcp_storage_bucket_location = (
        get_netcdf_gcp_location_from_raw_gcp_location(
            gcp_storage_bucket_location=gcp_storage_bucket_location
        )
    )
    # check if the file exists in gcp
    return cloud_utils.check_if_file_exists_in_gcp(
        bucket=gcp_bucket, file_path=netcdf_gcp_storage_bucket_location
    )


def check_existence_of_supplemental_files(
    file_name: str = "",
    file_type: str = "raw",
    ship_name: str = "",
    survey_name: str = "",
    echosounder: str = "",
    debug: bool = False,
) -> RawFile:
    """Checks the existence of supplemental files (idx, bot, etc.) for a raw
    files. Will check for existence in all data sources.

    Args:
        file_name (str, optional): The file name (includes extension).
            Defaults to "".
        file_type (str, optional): The file type (do not include the dot ".").
            Defaults to "".
        ship_name (str, optional): The ship name associated with this survey.
            Defaults to "".
        survey_name (str, optional): The survey name/identifier. Defaults
            to "".
        echosounder (str, optional): The echosounder used to gather the data.
            Defaults to "".
        debug (bool, optional): Whether or not to print debug statements.
            Defaults to False.

    Returns:
        RawFile: Returns a RawFile object, existence can be accessed as a
            boolean via the variable within.
            Ex. rf.idx_file_exists_in_ncei
    """

    # Create connection vars
    gcp_stor_client, gcp_bucket_name, gcp_bucket = setup_gcp_storage_objs()
    s3_client, s3_resource, s3_bucket = create_s3_objs()

    # Create the RawFile object.
    rf = RawFile(
        file_name=file_name,
        file_type=file_type,
        ship_name=ship_name,
        survey_name=survey_name,
        echosounder=echosounder,
        debug=debug,
        gcp_bucket=gcp_bucket,
        gcp_bucket_name=gcp_bucket_name,
        gcp_stor_client=gcp_stor_client,
        s3_resource=s3_resource,
    )

    return rf


def bq_query_to_pandas(client: bigquery.Client = None, query: str = ""):
    """Takes a SQL query and returns the end result as a DataFrame."""

    job = client.query(query)
    return job.result().to_dataframe()


if __name__ == "__main__":
    s3_client, s3_resource, s3_bucket = create_s3_objs()
    all_objs = list_all_objects_in_s3_bucket_location(
        prefix="data/raw/Reuben_Lasker/RL2107/metadata", bucket=s3_bucket
    )

    print(all_objs)
