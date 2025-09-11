"""This script contains functions used to ingest Active Acoustics data into GCP
from various sources such as AWS buckets and Azure Data Lake."""

import glob
from pprint import pprint
import sys
import os
import logging
import configparser

from google.cloud import storage
from azure.storage.filedatalake import (
    DataLakeDirectoryClient,
    DataLakeFileClient,
)
import boto3
from tqdm import tqdm

# For pytests-sake
if __package__ is None or __package__ == "":
    # uses current directory visibility
    import utils
    from utils import cloud_utils, helpers
    from utils.cloud_utils import get_data_lake_directory_client
    from utils.ncei_utils import download_single_file_from_aws
    import config
    import metadata
    from raw_file import RawFile
else:
    # uses current package visibility
    from aalibrary import utils
    from aalibrary import config
    from aalibrary.utils import cloud_utils, helpers
    from aalibrary import metadata
    from aalibrary.raw_file import RawFile
    from aalibrary.utils.cloud_utils import get_data_lake_directory_client
    from aalibrary.utils.ncei_utils import download_single_file_from_aws


def download_file_from_azure_directory(
    directory_client: DataLakeDirectoryClient,
    file_system: str = "testcontainer",
    download_directory: str = "./",
    file_path: str = "",
):
    """Downloads a single file from an azure directory using the
    DataLakeDirectoryClient. Useful for numerous operations, as authentication
    is only required once for the creation of each DataLakeDirectoryClient.

    Args:
        directory_client (DataLakeDirectoryClient): The
            DataLakeDirectoryClient that will be used to connect to a
            download from an azure file system in the data lake.
        file_system (str): The file system (container) you wish to download
            your file from. Defaults to "testcontainer" for testing purposes.
        download_directory (str): The local directory you want to download to.
            Defaults to "./".
        file_path (str): The file path you want to download.
    """

    # User-error-checking
    check_for_assertion_errors(
        data_lake_directory_client=directory_client,
        file_download_directory=download_directory,
    )

    file_client = directory_client.get_file_client(
        file_path=file_path, file_system=file_system
    )

    download_directory = os.path.normpath(download_directory)
    file_name = os.path.normpath(file_path).split(os.path.sep)[-1]

    with open(
        file=os.sep.join([download_directory, file_name]), mode="wb"
    ) as local_file:
        download = file_client.download_file()
        local_file.write(download.readall())
        local_file.close()


def download_specific_file_from_azure(
    config_file_path: str = "",
    container_name: str = "testcontainer",
    file_path_in_container: str = "",
):
    """Creates a DataLakeFileClient and downloads a specific file from
    `container_name`.

    Args:
        config_file_path (str, optional): The location of the config file.
            Needs a `[DEFAULT]` section with a `azure_connection_string`
            variable defined. Defaults to "".
        container_name (str, optional): The container within Azure Data Lake
            you are trying to access. Defaults to "testcontainer".
        file_path_in_container (str, optional): The file path of the file you
            would like downloaded. Defaults to "".
    """

    conf = configparser.ConfigParser()
    conf.read(config_file_path)

    file = DataLakeFileClient.from_connection_string(
        conf["DEFAULT"]["azure_connection_string"],
        file_system_name=container_name,
        file_path=file_path_in_container,
    )

    file_name = file_path_in_container.split("/")[-1]

    with open(f"./{file_name}", "wb") as my_file:
        download = file.download_file()
        download.readinto(my_file)


def download_raw_file_from_azure(
    file_name: str = "",
    file_type: str = "raw",
    ship_name: str = "",
    survey_name: str = "",
    echosounder: str = "",
    data_source: str = "OMAO",
    file_download_directory: str = ".",
    config_file_path: str = "",
    upload_to_gcp: bool = False,
    debug: bool = False,
):
    """ENTRYPOINT FOR END-USERS

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
        file_download_directory (str, optional): The local directory you want
            to store your file in. Defaults to current directory. Defaults
            to ".".
        config_file_path (str, optional): The location of the config file.
            Needs a `[DEFAULT]` section with a `azure_connection_string`
            variable defined. Defaults to "".
        upload_to_gcp (bool, optional): Whether or not you want to upload to
            GCP. Defaults to False.
        debug (bool, optional): Whether or not to print debug statements.
            Defaults to False.
    """
    # Create gcp bucket objects
    _, _, gcp_bucket = utils.cloud_utils.setup_gcp_storage_objs()
    try:
        _, s3_resource, _ = utils.cloud_utils.create_s3_objs()
    except Exception as e:
        logging.error("CANNOT ESTABLISH CONNECTION TO S3 BUCKET..\n %s", e)
        raise

    rf = RawFile(
        file_name=file_name,
        file_type=file_type,
        ship_name=ship_name,
        survey_name=survey_name,
        echosounder=echosounder,
        data_source=data_source,
        file_download_directory=file_download_directory,
        is_metadata=False,
        upload_to_gcp=upload_to_gcp,
        debug=debug,
        gcp_bucket=gcp_bucket,
        s3_resource=s3_resource,
    )

    # Location of temporary file in sandbox environment.
    # https://contracttest4.blob.core.windows.net/testcontainer/Reuben_Lasker/RL_1601/EK_60/1601RL-D20160107-T074016.bot

    # Create Azure Directory Client
    azure_datalake_directory_client = get_data_lake_directory_client(
        config_file_path=config_file_path
    )

    # TODO: check to see if you want to download from gcp instead.

    # TODO: add if statement to check if the file exists in azure or not.
    print(f"DOWNLOADING FILE {rf.raw_file_name} FROM OMAO")
    download_file_from_azure_directory(
        directory_client=azure_datalake_directory_client,
        download_directory=rf.file_download_directory,
        file_path=rf.raw_omao_file_path,
    )

    # Force download the idx file.
    print(f"DOWNLOADING IDX FILE {rf.idx_file_name} FROM OMAO")
    download_file_from_azure_directory(
        directory_client=azure_datalake_directory_client,
        download_directory=rf.file_download_directory,
        file_path=rf.idx_omao_file_path,
    )

    # Force download the bot file.
    print(f"DOWNLOADING BOT FILE {rf.bot_file_name} FROM OMAO")
    download_file_from_azure_directory(
        directory_client=azure_datalake_directory_client,
        download_directory=rf.file_download_directory,
        file_path=rf.bot_omao_file_path,
    )

    if upload_to_gcp:
        if rf.raw_file_exists_in_gcp:
            print(
                (
                    "RAW FILE ALREADY EXISTS IN GCP AT "
                    f"`{rf.raw_gcp_storage_bucket_location}`"
                )
            )
        else:
            # TODO: try out a background process if possible -- file might
            # have a lock. only async options, otherwise subprocess gsutil to
            # upload it.
            # Upload raw to GCP at the correct storage bucket location.
            upload_file_to_gcp_storage_bucket(
                file_name=file_name,
                file_type=file_type,
                ship_name=ship_name,
                survey_name=survey_name,
                echosounder=echosounder,
                file_location=rf.raw_file_download_path,
                gcp_bucket=gcp_bucket,
                data_source=data_source,
                debug=debug,
            )
            # Upload the metadata file as well.
            metadata.create_and_upload_metadata_df(
                rf=rf,
                debug=debug,
            )

        if rf.idx_file_exists_in_gcp:
            print(
                (
                    "IDX FILE ALREADY EXISTS IN GCP AT "
                    f"`{rf.idx_gcp_storage_bucket_location}`"
                )
            )
        else:
            # Upload idx to GCP at the correct storage bucket location.
            upload_file_to_gcp_storage_bucket(
                file_name=rf.idx_file_name,
                file_type=file_type,
                ship_name=ship_name,
                survey_name=survey_name,
                echosounder=echosounder,
                file_location=rf.idx_file_download_path,
                gcp_bucket=gcp_bucket,
                data_source=data_source,
                debug=debug,
            )

        if rf.bot_file_exists_in_gcp:
            print(
                (
                    "BOT FILE ALREADY EXISTS IN GCP AT"
                    f" `{rf.bot_gcp_storage_bucket_location}`"
                )
            )
        else:
            # Upload bot to GCP at the correct storage bucket location.
            upload_file_to_gcp_storage_bucket(
                file_name=rf.bot_file_name,
                file_type=file_type,
                ship_name=ship_name,
                survey_name=survey_name,
                echosounder=echosounder,
                file_location=rf.bot_file_download_path,
                gcp_bucket=gcp_bucket,
                data_source=data_source,
                debug=debug,
            )

        return


def download_raw_file_from_ncei(
    file_name: str = "",
    file_type: str = "raw",
    ship_name: str = "",
    survey_name: str = "",
    echosounder: str = "",
    data_source: str = "NCEI",
    file_download_directory: str = ".",
    upload_to_gcp: bool = False,
    debug: bool = False,
):
    """ENTRYPOINT FOR END-USERS
    Downloads a raw, idx, and bot file from NCEI. If `upload_to_gcp` is
    enabled, the downloaded files will also upload to the GCP storage bucket
    if they do not exist.

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
        file_download_directory (str, optional): The local file directory you
            want to store your file in. Defaults to current directory.
            Defaults to ".".
        upload_to_gcp (bool, optional): Whether or not you want to upload to
            GCP. Defaults to False.
        debug (bool, optional): Whether or not to print debug statements.
            Defaults to False.
    """
    _, _, gcp_bucket = utils.cloud_utils.setup_gcp_storage_objs()
    try:
        _, s3_resource, _ = utils.cloud_utils.create_s3_objs()
    except Exception as e:
        logging.error("CANNOT ESTABLISH CONNECTION TO S3 BUCKET..\n%s", e)
        raise

    rf = RawFile(
        file_name=file_name,
        file_type=file_type,
        ship_name=ship_name,
        survey_name=survey_name,
        echosounder=echosounder,
        data_source=data_source,
        file_download_directory=file_download_directory,
        upload_to_gcp=upload_to_gcp,
        debug=debug,
        gcp_bucket=gcp_bucket,
        s3_resource=s3_resource,
    )

    if rf.raw_file_exists_in_ncei:
        download_single_file_from_aws(
            file_url=rf.raw_file_ncei_url,
            download_location=rf.raw_file_download_path,
        )
    if rf.idx_file_exists_in_ncei:
        # Force download the idx file.
        download_single_file_from_aws(
            file_url=rf.idx_file_ncei_url,
            download_location=rf.idx_file_download_path,
        )
    if rf.bot_file_exists_in_ncei:
        # Force download the bot file.
        download_single_file_from_aws(
            file_url=rf.bot_file_ncei_url,
            download_location=rf.bot_file_download_path,
        )

    if upload_to_gcp:
        if rf.raw_file_exists_in_gcp:
            print(
                (
                    "RAW FILE ALREADY EXISTS IN GCP AT "
                    f"`{rf.raw_gcp_storage_bucket_location}`"
                )
            )
        else:
            # TODO: try out a background process if possible -- file might
            # have a lock. only async options, otherwise subprocess gsutil to
            # upload it.

            # Upload raw to GCP at the correct storage bucket location.
            upload_file_to_gcp_storage_bucket(
                file_name=rf.file_name,
                file_type="raw",
                ship_name=rf.ship_name,
                survey_name=rf.survey_name,
                echosounder=rf.echosounder,
                file_location=rf.raw_file_download_path,
                gcp_bucket=rf.gcp_bucket,
                data_source=rf.data_source,
                debug=rf.debug,
            )
            # Upload the metadata file as well.
            metadata.create_and_upload_metadata_df(
                rf=rf,
                debug=rf.debug,
            )

        if rf.idx_file_exists_in_gcp:
            print(
                (
                    "IDX FILE ALREADY EXISTS IN GCP AT "
                    f"`{rf.idx_gcp_storage_bucket_location}`"
                )
            )
        elif rf.idx_file_exists_in_ncei and (not rf.idx_file_exists_in_gcp):
            # Upload idx to GCP at the correct storage bucket location.
            upload_file_to_gcp_storage_bucket(
                file_name=rf.idx_file_name,
                file_type="idx",
                ship_name=rf.ship_name,
                survey_name=rf.survey_name,
                echosounder=echosounder,
                file_location=rf.idx_file_download_path,
                gcp_bucket=rf.gcp_bucket,
                data_source=rf.data_source,
                is_metadata=False,
                debug=rf.debug,
            )

        if rf.bot_file_exists_in_gcp:
            print(
                (
                    "BOT FILE ALREADY EXISTS IN GCP AT "
                    f"`{rf.bot_gcp_storage_bucket_location}`"
                )
            )
        elif rf.bot_file_exists_in_ncei and (not rf.bot_file_exists_in_gcp):
            # Upload bot to GCP at the correct storage bucket location.
            upload_file_to_gcp_storage_bucket(
                file_name=rf.bot_file_name,
                file_type="bot",
                ship_name=rf.ship_name,
                survey_name=rf.survey_name,
                echosounder=rf.echosounder,
                file_location=rf.bot_file_download_path,
                gcp_bucket=rf.gcp_bucket,
                data_source=rf.data_source,
                is_metadata=False,
                debug=rf.debug,
            )

        return


def _download_survey_from_ncei(
    ship_name: str = "",
    survey_name: str = "",
    echosounder: str = "",
    data_source: str = "NCEI",
    file_download_directory: str = ".",
    s3_client: boto3.client = None,
    upload_to_gcp: bool = False,
    debug: bool = False,
):
    """ENTRYPOINT FOR END-USERS
    Downloads the raw, idx, and bot files from a survey from NCEI. If
    `upload_to_gcp` is enabled, the downloaded files will also upload to the
    GCP storage bucket.

    Args:
        ship_name (str, optional): The ship name associated with this survey.
            Defaults to "".
        survey_name (str, optional): The survey name/identifier. Defaults
            to "".
        echosounder (str, optional): The echosounder used to gather the data.
            Defaults to "".
        data_source (str, optional): The source of the file. Necessary due to
            the way the storage bucket is organized. Can be one of
            ["NCEI", "OMAO", "HDD"]. Defaults to "".
        file_download_directory (str, optional): The local file directory you
            want to store your file in. Defaults to current directory.
            Defaults to ".".
        s3_client (boto3.client, optional): The bucket client object.
            Defaults to None.
        upload_to_gcp (bool, optional): Whether or not you want to upload to
            GCP. Defaults to False.
        debug (bool, optional): Whether or not to print debug statements.
            Defaults to False.
    """

    # TODO: convert to using RawFile object.

    # User-error-checking
    check_for_assertion_errors(
        ship_name=ship_name,
        survey_name=survey_name,
        echosounder=echosounder,
        data_source=data_source,
        file_download_directory=file_download_directory,
    )

    # Create the download directory (path) if it doesn't exist
    if not os.path.exists(file_download_directory):
        os.makedirs(file_download_directory)

    # Get all raw file names associated with this survey from NCEI.
    prefix = f"data/raw/{ship_name}/{survey_name}/{echosounder}/"
    survey_file_names = cloud_utils.get_subdirectories_in_s3_bucket_location(
        prefix=prefix, s3_client=s3_client, return_full_paths=False
    )
    # Filter out only the raw files (the download function takes care of
    # downloading the idx and bot files).
    survey_file_names = [x for x in survey_file_names if x.endswith(".raw")]

    # Download/upload each file, one by one.
    for survey_file_name in survey_file_names:
        download_raw_file_from_ncei(
            file_name=survey_file_name,
            file_type="raw",
            ship_name=ship_name,
            survey_name=survey_name,
            echosounder=echosounder,
            data_source="NCEI",
            file_download_directory=file_download_directory,
            upload_to_gcp=upload_to_gcp,
            debug=debug,
        )


def check_for_assertion_errors(**kwargs):
    """Checks for errors in the kwargs provided."""

    if "file_name" in kwargs:
        assert kwargs["file_name"] != "", (
            "Please provide a valid file name with the file extension"
            " (ex. `2107RL_CW-D20210813-T220732.raw`)"
        )
    if "file_type" in kwargs:
        assert kwargs["file_type"] != "", "Please provide a valid file type."
        assert kwargs["file_type"] in config.VALID_FILETYPES, (
            "Please provide a valid file type (extension) "
            f"from the following: {config.VALID_FILETYPES}"
        )
    if "ship_name" in kwargs:
        assert kwargs["ship_name"] != "", (
            "Please provide a valid ship name "
            "(Title_Case_With_Underscores_As_Spaces)."
        )
    if "survey_name" in kwargs:
        assert (
            kwargs["survey_name"] != ""
        ), "Please provide a valid survey name."
    if "echosounder" in kwargs:
        assert (
            kwargs["echosounder"] != ""
        ), "Please provide a valid echosounder."
        assert kwargs["echosounder"] in config.VALID_ECHOSOUNDERS, (
            "Please provide a valid echosounder from the "
            f"following: {config.VALID_ECHOSOUNDERS}"
        )
    if "data_source" in kwargs:
        assert kwargs["data_source"] != "", (
            "Please provide a valid data source from the "
            f"following: {config.VALID_DATA_SOURCES}"
        )
        assert kwargs["data_source"] in config.VALID_DATA_SOURCES, (
            "Please provide a valid data source from the "
            f"following: {config.VALID_DATA_SOURCES}"
        )
    if "file_download_directory" in kwargs:
        assert (
            kwargs["file_download_directory"] != ""
        ), "Please provide a valid file download directory."
        assert os.path.isdir(kwargs["file_download_directory"]), (
            f"File download location `{kwargs['file_download_directory']}` is"
            " not found to be a valid dir, please reformat it."
        )
    if "gcp_bucket" in kwargs:
        assert kwargs["gcp_bucket"] is not None, (
            "Please provide a gcp_bucket object with"
            " `utils.cloud_utils.setup_gcp_storage()`"
        )
    if "directory" in kwargs:
        assert kwargs["directory"] != "", "Please provide a valid directory."
        assert os.path.isdir(kwargs["directory"]), (
            f"Directory location `{kwargs['directory']}` is not found to be a"
            " valid dir, please reformat it."
        )
    if "data_lake_directory_client" in kwargs:
        assert kwargs["data_lake_directory_client"] is not None, (
            f"The data lake directory client cannot be a"
            f" {type(kwargs['data_lake_directory_client'])} object. It needs "
            "to be of the type `DataLakeDirectoryClient`."
        )


def download_raw_file(
    file_name: str = "",
    file_type: str = "raw",
    ship_name: str = "",
    survey_name: str = "",
    echosounder: str = "",
    data_source: str = "",
    file_download_directory: str = ".",
    gcp_bucket: storage.Bucket = None,
    debug: bool = False,
):
    """ENTRYPOINT FOR END-USERS
    Downloads a raw and idx file from NCEI for use on your workstation.
    Works as follows:
        1. Checks if raw file exists in GCP.
            a. If it exists,
                checks if a netcdf version also exists or not
                lets the user know.
                i. If `force_download_from_ncei` is True
                    downloads the raw and idx file from NCEI instead.
            b. If it doesn't exist,
                downloads .raw from NCEI and uploads to GCP for caching
                downloads .idx from NCEI and uploads to GCP for caching

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
        data_source (str, optional): The source of the file. Necessary due to
            the way the storage bucket is organized. Can be one of
            ["NCEI", "OMAO", "HDD"]. Defaults to "".
        file_download_directory (str, optional): The local file directory you
            want to store your file in. Defaults to current directory.
            Defaults to ".".
        gcp_bucket (storage.Client.bucket, optional): The GCP bucket object
            used to download the file. Defaults to None.
        debug (bool, optional): Whether or not to print debug statements.
            Defaults to False.
    """

    if gcp_bucket is None:
        _, _, gcp_bucket = utils.cloud_utils.setup_gcp_storage_objs()
    _, s3_resource, _ = utils.cloud_utils.create_s3_objs()

    rf = RawFile(
        file_name=file_name,
        file_type=file_type,
        ship_name=ship_name,
        survey_name=survey_name,
        echosounder=echosounder,
        data_source=data_source,
        file_download_directory=file_download_directory,
        debug=debug,
        gcp_bucket=gcp_bucket,
        s3_resource=s3_resource,
    )

    if rf.raw_file_exists_in_gcp:
        # Inform user if file exists in GCP.
        print(
            f"FILE `{rf.raw_file_name}` ALREADY EXISTS IN"
            " GOOGLE STORAGE BUCKET."
        )
        # Here we download the raw file from GCP. We also check for a netcdf
        # version and let the user know.
        print("CHECKING FOR NETCDF VERSION...")
        if rf.netcdf_file_exists_in_gcp:
            # Inform the user if a netcdf version exists in cache.
            print(
                (
                    f"FILE `{rf.raw_file_name}` EXISTS AS A NETCDF ALREADY."
                    " PLEASE DOWNLOAD THE NETCDF VERSION IF NEEDED."
                )
            )
        else:
            print(
                (
                    f"FILE `{rf.raw_file_name}` DOES NOT EXIST AS NETCDF."
                    " CONSIDER RUNNING A CONVERSION FUNCTION"
                )
            )

        # Here we download the raw from GCP.
        print(
            (
                f"DOWNLOADING FILE `{rf.raw_file_name}` FROM GCP TO"
                f" `{rf.raw_file_download_path}`"
            )
        )
        utils.cloud_utils.download_file_from_gcp(
            gcp_bucket=rf.gcp_bucket,
            blob_file_path=rf.raw_gcp_storage_bucket_location,
            local_file_path=rf.raw_file_download_path,
            debug=rf.debug,
        )
        print("DOWNLOADED.")

    elif rf.raw_file_exists_in_ncei and (
        not rf.raw_file_exists_in_gcp
    ):  # File does not exist in gcp and needs to be downloaded from NCEI
        download_raw_file_from_ncei(
            file_name=rf.raw_file_name,
            file_type="raw",
            ship_name=rf.ship_name,
            survey_name=rf.survey_name,
            echosounder=rf.echosounder,
            data_source=rf.data_source,
            file_download_directory=rf.file_download_directory,
            upload_to_gcp=True,
            debug=rf.debug,
        )

    # Checking to make sure the idx exists in GCP...
    if rf.idx_file_exists_in_gcp:
        print("CORRESPONDING IDX FILE FOUND IN GCP. DOWNLOADING...")
        # Here we download the idx from GCP.
        print(
            (
                f"DOWNLOADING FILE `{rf.idx_file_name}` FROM GCP TO "
                f"`{rf.idx_file_download_path}`"
            )
        )
        utils.cloud_utils.download_file_from_gcp(
            gcp_bucket=rf.gcp_bucket,
            blob_file_path=rf.idx_gcp_storage_bucket_location,
            local_file_path=rf.idx_file_download_path,
            debug=rf.debug,
        )
        print("DOWNLOADED.")
    elif rf.idx_file_exists_in_ncei and (not rf.idx_file_exists_in_gcp):
        print(
            (
                "CORRESPONDING IDX FILE NOT FOUND IN GCP."
                " DOWNLOADING FROM NCEI AND UPLOADING TO GCP..."
            )
        )
        # Safely download and upload the idx file.
        download_single_file_from_aws(
            file_url=rf.idx_file_ncei_url,
            download_location=rf.idx_file_download_path,
        )
        # Upload to GCP at the correct storage bucket location.
        upload_file_to_gcp_storage_bucket(
            file_name=rf.idx_file_name,
            file_type="idx",
            ship_name=rf.ship_name,
            survey_name=rf.survey_name,
            echosounder=rf.echosounder,
            file_location=rf.idx_file_download_path,
            gcp_bucket=rf.gcp_bucket,
            data_source=rf.data_source,
            debug=rf.debug,
        )

    # Checking to make sure the bot exists in GCP...
    if rf.bot_file_exists_in_gcp:
        print("CORRESPONDING BOT FILE FOUND IN GCP. DOWNLOADING...")
        # Here we download the bot from GCP.
        print(
            (
                f"DOWNLOADING FILE `{rf.bot_file_name}` FROM GCP"
                f" TO `{rf.bot_file_download_path}`"
            )
        )
        utils.cloud_utils.download_file_from_gcp(
            gcp_bucket=rf.gcp_bucket,
            blob_file_path=rf.bot_gcp_storage_bucket_location,
            local_file_path=rf.bot_file_download_path,
            debug=rf.debug,
        )
        print("DOWNLOADED.")
    elif rf.bot_file_exists_in_ncei and (not rf.bot_file_exists_in_gcp):
        print(
            (
                "CORRESPONDING BOT FILE NOT FOUND IN GCP. TRYING TO "
                "DOWNLOAD FROM NCEI AND UPLOADING TO GCP..."
            )
        )
        # Safely download and upload the bot file.
        download_single_file_from_aws(
            file_url=rf.bot_file_ncei_url,
            download_location=rf.bot_file_download_path,
        )
        # Upload to GCP at the correct storage bucket location.
        upload_file_to_gcp_storage_bucket(
            file_name=rf.bot_file_name,
            file_type="bot",
            ship_name=rf.ship_name,
            survey_name=rf.survey_name,
            echosounder=rf.echosounder,
            file_location=rf.bot_file_download_path,
            gcp_bucket=rf.gcp_bucket,
            data_source=rf.data_source,
            debug=rf.debug,
        )

    return


def download_netcdf_file(
    raw_file_name: str = "",
    file_type: str = "netcdf",
    ship_name: str = "",
    survey_name: str = "",
    echosounder: str = "",
    data_source: str = "",
    file_download_directory: str = "",
    gcp_bucket: storage.Client.bucket = None,
    debug: bool = False,
):
    """ENTRYPOINT FOR END-USERS
    Downloads a netcdf file from GCP storage bucket for use on your
    workstation.
    Works as follows:
        1. Checks if the exact netcdf exists in gcp.
            a. If it doesn't exists, prompts user to download it first.
            b. If it exists, downloads to the `file_download_directory`.

    Args:
        raw_file_name (str, optional): The raw file name (includes extension).
            Defaults to "".
        file_type (str, optional): The file type (do not include the dot ".").
            Defaults to "netcdf".
        ship_name (str, optional): The ship name associated with this survey.
            Defaults to "".
        survey_name (str, optional): The survey name/identifier.
            Defaults to "".
        echosounder (str, optional): The echosounder used to gather the data.
            Defaults to "".
        data_source (str, optional): The source of the file. Necessary due to
            the way the storage bucket is organized. Can be one of
            ["NCEI", "OMAO", "HDD"]. Defaults to "".
        file_download_directory (str, optional): The local directory you want
            to store your file in. Defaults to "".
        gcp_bucket (storage.Client.bucket, optional): The GCP bucket object
            used to download the file. Defaults to None.
        is_metadata (bool, optional): Whether or not the file is a metadata
            file. Necessary since files that are considered metadata (metadata
            json, or readmes) are stored in a separate directory. Defaults to
            False.
        debug (bool, optional): Whether or not to print debug statements.
            Defaults to False.
    """

    _, s3_resource, _ = utils.cloud_utils.create_s3_objs()

    rf = RawFile(
        file_name=raw_file_name,
        file_type=file_type,
        ship_name=ship_name,
        survey_name=survey_name,
        echosounder=echosounder,
        data_source=data_source,
        file_download_directory=file_download_directory,
        gcp_bucket=gcp_bucket,
        debug=debug,
        s3_resource=s3_resource,
    )

    if rf.netcdf_file_exists_in_gcp:
        print(
            (
                f"NETCDF FILE LOCATED IN GCP"
                f": `{rf.netcdf_gcp_storage_bucket_location}`\nDOWNLOADING..."
            )
        )
        utils.cloud_utils.download_file_from_gcp(
            gcp_bucket=gcp_bucket,
            blob_file_path=rf.netcdf_gcp_storage_bucket_location,
            local_file_path=rf.netcdf_file_download_path,
            debug=debug,
        )
        print(
            f"FILE `{raw_file_name}` DOWNLOADED "
            f"TO `{rf.netcdf_file_download_path}`"
        )
        return
    else:
        logging.error(
            "NETCDF FILE `%s` DOES NOT EXIST IN GCP AT THE LOCATION: `%s`.",
            raw_file_name,
            rf.netcdf_gcp_storage_bucket_location,
        )
        logging.error(
            "PLEASE CONVERT AND UPLOAD THE RAW FILE FIRST VIA"
            " `download_raw_file`."
        )
        raise FileNotFoundError


# def upload_entire_cruise_directory_to_gcp_storage_bucket(
#     directory_path: str = "",
#     gcp_bucket: storage.Client.bucket = None,
#     debug: bool = False,
# ):
#     """Uploads an entire folder to the GCP storage bucket AS-IS. The folderis
#     assumed to be the cruise/survey folder.
#     NOTE: Does not check for metadata, file or folder naming conventions,etc.

#     Args:
#         directory_path (str, optional): The directory you would like to copy to
#             the storage bucket. Defaults to "".
#         gcp_bucket (storage.Client.bucket, optional): The storage bucket object
#             used for uploading. Defaults to None.
#         debug (bool, optional): Whether or not to print debug statements.
#             Defaults to False.
#     """
#     # Get all files in the directory_path
#     all_files_paths = []
#     for root, _, files in os.walk(directory_path):
#         for file in files:
#             all_files_paths.append(os.path.join(root, file))


def upload_file_to_gcp_storage_bucket(
    file_name: str = "",
    file_type: str = "",
    ship_name: str = "",
    survey_name: str = "",
    echosounder: str = "",
    file_location: str = "",
    gcp_bucket: storage.Client.bucket = None,
    data_source: str = "",
    is_metadata: bool = False,
    is_survey_metadata: bool = False,
    debug: bool = False,
):
    """Safely uploads a local file to the storage bucket. Will also check to
    see if the file already exists.

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
        file_location (str, optional): The local location of the file.
            Defaults to "".
        gcp_bucket (storage.Client.bucket, optional): The GCP bucket object
            used to upload the file. Defaults to None.
        data_source (str, optional): The source of the data. Can be one of
            ["NCEI", "OMAO", "HDD", "TEST"]. Defaults to "".
        is_metadata (bool, optional): Whether or not the file is a metadata
            file. Necessary since files that are considered metadata (metadata
            json, or readmes) are stored in a separate directory. Defaults to
            False.
        is_survey_metadata (bool, optional): Whether or not the file is a
            metadata file associated with a survey. The files are stored at
            the survey level, in the `metadata/` folder. Defaults to False.
        debug (bool, optional): Whether or not to print debug statements.
            Defaults to False.
    """

    gcp_storage_bucket_location = (
        helpers.parse_correct_gcp_storage_bucket_location(
            file_name=file_name,
            file_type=file_type,
            ship_name=ship_name,
            survey_name=survey_name,
            echosounder=echosounder,
            data_source=data_source,
            is_metadata=is_metadata,
            is_survey_metadata=is_survey_metadata,
            debug=debug,
        )
    )

    # Check if the file exists in GCP
    file_exists_in_gcp = cloud_utils.check_if_file_exists_in_gcp(
        gcp_bucket, file_path=gcp_storage_bucket_location
    )
    if file_exists_in_gcp:
        print(
            (
                f"FILE `{file_name}` ALREADY EXISTS IN GCP AT "
                f"`{gcp_storage_bucket_location}`."
            )
        )
    else:
        try:
            print(
                (
                    f"UPLOADING FILE `{file_name}` TO GCP AT"
                    f" `{gcp_storage_bucket_location}`..."
                )
            )
            # Upload to storage bucket.
            utils.cloud_utils.upload_file_to_gcp_bucket(
                bucket=gcp_bucket,
                blob_file_path=gcp_storage_bucket_location,
                local_file_path=file_location,
                debug=debug,
            )
            print("UPLOADED.")
        except Exception as e:
            logging.error(
                "COULD NOT UPLOAD FILE %s TO GCP (%s) STORAGE BUCKET DUE TO "
                "THE FOLLOWING ERROR:\n%s",
                file_name,
                gcp_storage_bucket_location,
                e,
            )

    return


def upload_local_echosounder_files_from_directory_to_gcp_storage_bucket(
    local_echosounder_directory_to_upload: str = "",
    ship_name: str = "",
    survey_name: str = "",
    echosounder: str = "",
    data_source: str = "",
    gcp_bucket: storage.Client.bucket = None,
    debug: bool = False,
):
    """ENTRYPOINT FOR END-USERS
    Uploads all of the .raw (and their corresponding .idx) files from a
    directory into the appropriate location in the GCP storage bucket.
    NOTE: Assumes that all files share the same metadata.

    Args:
        local_echosounder_directory_to_upload (str, optional): The directory
            which contains all of the files you want to upload. Defaults to "".
        ship_name (str, optional): The ship name associated with this survey.
            Defaults to "".
        survey_name (str, optional): The survey name/identifier. Defaults
            to "".
        echosounder (str, optional): The echosounder used to gather the data.
            Defaults to "".
        data_source (str, optional): The source of the file. Necessary due to
            the way the storage bucket is organized. Can be one of
            ["NCEI", "OMAO", "HDD"]. Defaults to "".
        gcp_bucket (storage.Client.bucket, optional): The GCP bucket object
            used to download the file. Defaults to None.
        debug (bool, optional): Whether or not to print debug statements.
            Defaults to False.
    """

    # Warn user that this function assumes the same metadata for all files
    # within directory.
    logging.warning(
        (
            "WARNING: THIS FUNCTION ASSUMES THAT ALL FILES WITHIN THIS "
            "DIRECTORY ARE FROM THE SAME SHIP, SURVEY, AND ECHOSOUNDER."
        )
    )
    local_echosounder_directory_to_upload = os.path.normpath(
        local_echosounder_directory_to_upload
    )
    # Check that the directory exists
    check_for_assertion_errors(
        directory=local_echosounder_directory_to_upload,
        ship_name=ship_name,
        survey_name=survey_name,
        echosounder=echosounder,
    )

    # normalize ship name
    ship_name_normalized = helpers.normalize_ship_name(ship_name)
    if debug:
        print(f"NORMALIZED SHIP NAME: {ship_name_normalized}")
        print(
            "LOCAL DIRECTORY TO UPLOAD:"
            f" {local_echosounder_directory_to_upload}"
        )
    # Make sure GCP bucket is setup
    assert gcp_bucket is not None, "Please provide a gcp_bucket object."

    # Check (glob) for raw and idx files.
    print("CHECKING DIRECTORY FOR RAW, IDX, BOT, AND NETCDF FILES...")
    raw_files = [
        x
        for x in glob.glob(
            os.sep.join([local_echosounder_directory_to_upload, "*.raw"])
        )
    ]
    idx_files = [
        x
        for x in glob.glob(
            os.sep.join([local_echosounder_directory_to_upload, "*.idx"])
        )
    ]
    bot_files = [
        x
        for x in glob.glob(
            os.sep.join([local_echosounder_directory_to_upload, "*.bot"])
        )
    ]
    netcdf_files = [
        x
        for x in glob.glob(
            os.sep.join([local_echosounder_directory_to_upload, "*.nc"])
        )
    ]
    # Create vars for use later.
    raw_upload_count = 0
    idx_upload_count = 0
    bot_upload_count = 0
    netcdf_upload_count = 0

    # Let the user know how many of each file has been found to upload.
    print(
        (
            f"FOUND {len(raw_files)} RAW FILES | {len(idx_files)} IDX FILES |"
            f" {len(bot_files)} BOT FILES | {len(netcdf_files)} NETCDF FILES"
        )
    )

    # Upload each idx file to gcp
    if len(idx_files) > 0:
        for idx_file in tqdm(idx_files, desc="Uploading idx files"):
            file_name = idx_file.split(os.sep)[-1]
            # Upload idx to GCP at the correct storage bucket location.
            # The function already checks if the file exists.
            upload_file_to_gcp_storage_bucket(
                file_name=file_name,
                file_type="idx",
                ship_name=ship_name_normalized,
                survey_name=survey_name,
                echosounder=echosounder,
                file_location=idx_file,
                gcp_bucket=gcp_bucket,
                data_source=data_source,
                is_metadata=False,
                debug=debug,
            )
            idx_upload_count += 1
        print(f"{idx_upload_count} IDX FILES UPLOADED.")

    # Upload each bot file to gcp
    if len(bot_files) > 0:
        for bot_file in tqdm(bot_files, desc="Uploading bot files"):
            file_name = bot_file.split(os.sep)[-1]
            # Upload idx to GCP at the correct storage bucket location.
            # The function already checks if the file exists.
            upload_file_to_gcp_storage_bucket(
                file_name=file_name,
                file_type="bot",
                ship_name=ship_name_normalized,
                survey_name=survey_name,
                echosounder=echosounder,
                file_location=bot_file,
                gcp_bucket=gcp_bucket,
                data_source=data_source,
                is_metadata=False,
                debug=debug,
            )
            bot_upload_count += 1
        print(f"{bot_upload_count} BOT FILES UPLOADED.")

    # Upload each raw file to gcp
    if len(raw_files) > 0:
        for raw_file in tqdm(raw_files, desc="Uploading raw files"):
            file_name = raw_file.split(os.sep)[-1]
            # Upload raw to GCP at the correct storage bucket location.
            # The function already checks if the file exists.
            upload_file_to_gcp_storage_bucket(
                file_name=file_name,
                file_type="raw",
                ship_name=ship_name_normalized,
                survey_name=survey_name,
                echosounder=echosounder,
                file_location=raw_file,
                gcp_bucket=gcp_bucket,
                data_source=data_source,
                is_metadata=False,
                debug=debug,
            )
            # metadata.create_and_upload_metadata_df(
            #     file_name=file_name,
            #     file_type="raw",
            #     ship_name=ship_name,
            #     survey_name=survey_name,
            #     echosounder=echosounder,
            #     data_source=data_source,
            #     gcp_bucket=gcp_bucket,
            #     debug=debug,
            # )
            raw_upload_count += 1
        print(f"{raw_upload_count} RAW FILES UPLOADED.")

    # Upload each netcdf file to gcp
    if len(netcdf_files) > 0:
        for netcdf_file in tqdm(netcdf_files, desc="Uploading netcdf files"):
            file_name = netcdf_file.split(os.sep)[-1]
            # Upload idx to GCP at the correct storage bucket location.
            # The function already checks if the file exists.
            upload_file_to_gcp_storage_bucket(
                file_name=file_name,
                file_type="netcdf",
                ship_name=ship_name_normalized,
                survey_name=survey_name,
                echosounder=echosounder,
                file_location=netcdf_file,
                gcp_bucket=gcp_bucket,
                data_source=data_source,
                is_metadata=False,
                debug=debug,
            )
            # metadata.create_and_upload_metadata_df_for_netcdf(
            #     file_name=file_name,
            #     file_type="netcdf",
            #     ship_name=ship_name_normalized,
            #     survey_name=survey_name,
            #     echosounder=echosounder,
            #     data_source=data_source,
            #     gcp_bucket=gcp_bucket,
            #     debug=debug,
            # )
            netcdf_upload_count += 1
        print(f"{netcdf_upload_count} NETCDF FILES UPLOADED.")

    print(
        (
            f"UPLOADS COMPLETE\nRAW ({raw_upload_count}) | IDX "
            f"({idx_upload_count}) | BOT {bot_upload_count} | "
            f"NETCDF ({netcdf_upload_count})"
        )
    )


def find_and_upload_survey_metadata_from_s3(
    ship_name: str = "",
    survey_name: str = "",
    gcp_bucket: storage.Client.bucket = None,
    debug: bool = False,
):
    """Finds the metadata that is associated with a particular survey in s3,
    then uploads all of those files into the correct gcp location.

    Args:
        ship_name (str, optional): The ship name associated with this survey.
            Defaults to "".
        survey_name (str, optional): The survey name/identifier. Defaults
            to "".
        gcp_bucket (storage.Client.bucket, optional): The GCP bucket object
            used to download the file. Defaults to None.
        debug (bool, optional): Whether or not to print debug statements.
            Defaults to False.
    """

    metadata_location_in_s3 = f"data/raw/{ship_name}/{survey_name}/metadata/"

    try:
        _, _, s3_bucket = utils.cloud_utils.create_s3_objs()
    except Exception as e:
        logging.error("CANNOT ESTABLISH CONNECTION TO S3 BUCKET..\n%s", e)
        raise

    num_metadata_objects = cloud_utils.count_objects_in_s3_bucket_location(
        prefix=metadata_location_in_s3, bucket=s3_bucket
    )

    if debug:
        logging.debug(
            "%d num_metadata_objects FOUND IN S3 FOR %s - %s",
            num_metadata_objects,
            ship_name,
            survey_name,
        )

    if num_metadata_objects >= 1:
        # Get object keys
        s3_objects = cloud_utils.list_all_objects_in_s3_bucket_location(
            prefix=metadata_location_in_s3, s3_resource=s3_bucket
        )
        # Download and upload each object
        for full_path, file_name in s3_objects:
            # Get the correct full file download location
            file_download_directory = os.sep.join(
                [os.path.normpath("./"), file_name]
            )
            # Download from aws
            download_single_file_from_aws(
                file_url=full_path, download_location=file_download_directory
            )
            # Upload to gcp
            upload_file_to_gcp_storage_bucket(
                file_name=file_name,
                ship_name=ship_name,
                survey_name=survey_name,
                file_location=file_download_directory,
                gcp_bucket=gcp_bucket,
                data_source="NCEI",
                is_metadata=False,
                is_survey_metadata=True,
                debug=debug,
            )
            # Remove local file (it's temporary)
            os.remove(file_download_directory)


def find_data_source_for_file():
    """Finds the data source of a given filename by checking all possible data
    sources."""
    # TODO:
    # Check HDD storage bucket on GCP
    # Check NCEI S3 bucket.
    # TODO: Check OMAO Data Lake


def download_survey_from_ncei(
    ship_name: str = "",
    survey_name: str = "",
    download_directory: str = "",
    max_limit: int = None,
    debug: bool = False,
):
    """Downloads an entire survey from NCEI to a local directory while
    maintaining folder structure.

    Args:
        ship_name (str, optional): The ship name. Defaults to "".
        survey_name (str, optional): The name of the survey you would like to
            download. Defaults to "".
        download_directory (str, optional): The directory to which the files
            will be downloaded. Creates a directory in the cwd if not
            specified. Defaults to "".
            NOTE: The directory specified will have the `ship_name/survey_name`
            folders created within it.
        max_limit (int, optional): The maximum number of random files to
            download.
            Defaults to include all files.
        debug (bool, optional): Whether or not you want to print debug
            statements. Defaults to False.
    """

    if download_directory == "":
        # Create a directory in the cwd
        download_directory = os.sep.join(
            [os.path.normpath("./"), f"{ship_name}", f"{survey_name}"]
        )

    if debug:
        logging.debug("FORMATTED DOWNLOAD DIRECTORY: %s", download_directory)

    # Get all s3 objects for the survey
    print(f"GETTING ALL S3 OBJECTS FOR SURVEY {survey_name}...")
    _, s3_resource, _ = utils.cloud_utils.create_s3_objs()
    s3_objects = cloud_utils.list_all_objects_in_s3_bucket_location(
        prefix=f"data/raw/{ship_name}/{survey_name}/",
        s3_resource=s3_resource,
        return_full_paths=True,
    )
    print(f"FOUND {len(s3_objects)} FILES.")

    if max_limit is None:
        max_limit = len(s3_objects)

    subdirs = set()
    # Get the subfolders from object keys
    for s3_object in s3_objects:
        # Skip folders
        if s3_object.endswith("/"):
            continue
        # Get the subfolder structure from the object key
        subfolder_key = os.sep.join(
            s3_object.replace("data/raw/", "").split("/")[:-1]
        )
        subdirs.add(subfolder_key)
    for subdir in subdirs:
        os.makedirs(os.sep.join([download_directory, subdir]), exist_ok=True)

    # Create the directory if it doesn't exist.
    if not os.path.isdir(download_directory):
        print(f"CREATING download_directory `{download_directory}`")
        os.makedirs(download_directory, exist_ok=True)
    # normalize the path
    download_directory = os.path.normpath(download_directory)
    print("CREATED DOWNLOAD DIRECTORIES.")

    for _, object_key in enumerate(
        tqdm(s3_objects[:max_limit], desc="Downloading")
    ):
        # file_name = object_key.split("/")[-1]
        local_object_path = object_key.replace("data/raw/", "")
        download_location = os.path.normpath(
            os.sep.join([download_directory, local_object_path])
        )
        download_single_file_from_aws(
            file_url=object_key, download_location=download_location
        )
    print(f"DOWNLOAD COMPLETE {os.path.abspath(download_directory)}.")


if __name__ == "__main__":
    # set logging config
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    download_survey_from_ncei(
        ship_name="Reuben_Lasker",
        survey_name="RL2107",
        download_directory="./test_data_dir",
        max_limit=5,
        debug=True,
    )
    gcp_stor_client, gcp_bucket_name, gcp_bucket = (
        cloud_utils.setup_gcp_storage_objs(
            project_id="ggn-nmfs-aa-dev-1",
            gcp_bucket_name="ggn-nmfs-aa-dev-1-data",
        )
    )

    upload_local_echosounder_files_from_directory_to_gcp_storage_bucket(
        local_echosounder_directory_to_upload="./test_data_dir/Reuben_Lasker/RL2107/EK80/",
        ship_name="Reuben_Lasker",
        survey_name="RL2107",
        echosounder="EK80",
        data_source="HDD",
        gcp_bucket=gcp_bucket,
        debug=True,
    )

    # azure_datalake_directory_client = get_data_lake_directory_client(
    #     config_file_path="./azure_config.ini"
    # )
    # download_file_from_azure_directory(
    #     directory_client=azure_datalake_directory_client,
    #     file_system="testcontainer",
    #     download_directory="./",
    #     file_name="RL2107_EK80_WCSD_EK80-metadata.json",
    # )
    # download_raw_file_from_azure(
    #     file_name="1601RL-D20160107-T074016.raw",
    #     file_type="raw",
    #     ship_name="Reuben_Lasker",
    #     survey_name="RL_1601",
    #     echosounder="EK_60",
    #     data_source="OMAO",
    #     file_download_directory=".",
    #     config_file_path="./azure_config.ini",
    #     is_metadata=False,
    #     upload_to_gcp=True,
    #     debug=True,
    # )
    # download_specific_file_from_azure(
    #     config_file_path="./azure_config.ini",
    #     container_name="testcontainer",
    #     file_path_in_container="RL2107_EK80_WCSD_EK80-metadata.json",
    # )

    # set up storage objects
    # s3_client, s3_resource, s3_bucket = utils.cloud_utils.create_s3_objs()
    # gcp_stor_client, gcp_bucket_name, gcp_bucket = (
    #     utils.cloud_utils.setup_gcp_storage_objs()
    # )

    # find_and_upload_survey_metadata_from_s3(
    #     ship_name="Reuben_Lasker", survey_name="RL2107"
    # )

    # upload_local_raw_and_idx_files_from_directory_to_gcp_storage_bucket(
    #     directory="./test_data_dir",
    #     ship_name="Reuben_Lasker",
    #     survey_name="RL2107",
    #     echosounder="EK80",
    #     data_source="NCEI",
    #     gcp_bucket=gcp_bucket,
    #     debug=False,
    # )

    # survey_stuff = get_all_objects_from_survey_ncei(
    #                                  ship_name="Reuben_Lasker",
    #                                  survey_name="RL2107",
    #                                  bucket=bucket)
    # print(survey_stuff)
    # resp = s3_resource.(bucket="noaa-wcsd-pds",
    #                           prefix="data/raw/Reuben_Lasker/RL2107")
    # print(resp)

    # print(utils.cloud_utils.count_objects_in_bucket_location(
    #                               prefix="data/raw/Reuben_Lasker/RL2107/",
    #                               bucket=bucket))

    # file_name, file_type, echosounder, survey_name, ship_name = \
    # parse_variables_from_ncei_file_url(url="https://noaa-wcsd-pds.s3.amazonaws.\
    # com/data/raw/Reuben_Lasker/RL2107/EK80/2107RL_CW-D20210813-T220732.raw")
    # print(helpers.parse_correct_gcp_storage_bucket_location(file_name=file_name,
    #                                                 file_type=file_type,
    #                                                 ship_name=ship_name,
    #                                                 survey_name=survey_name,
    #                                                 echosounder=echosounder,
    #                                                 data_source="NCEI",
    #                                                 is_metadata=False,
    #                                                 debug=True))
    # https://noaa-wcsd-pds.s3.amazonaws.com/data/raw/Reuben_Lasker/RL2107/EK80/2107RL_CW-D20210706-T172335.idx
    # print(utils.cloud_utils.check_if_file_exists_in_s3(object_key="data/raw/"
    # "Reuben_Lasker/RL2107/EK80/2107RL_CW-D20210706-T172335.idx",
    #                                  s3_resource=s3_resource,
    #                                  s3_bucket_name="noaa-wcsd-pds"))
    # download_raw_file(file_name="2107RL_FM-D20210808-T033245.raw",
    #                   file_type="raw",
    #                   ship_name="Reuben_Lasker",
    #                   survey_name="RL2107",
    #                   echosounder="EK80",
    #                   data_source="NCEI",
    #                   file_download_directory=f"./test_data_dir/",
    #                   is_metadata=False,
    #                   debug=True)
    # print(utils.cloud_utils.check_if_file_exists_in_gcp(gcp_bucket,
    # file_path="NCEI/Reuben_Lasker/RL2107/EK80/data/raw/2107RL_CW-D20210813-T220732a.raw"))
    # convert_local_raw_to_netcdf(
    #     raw_file_location="./test_data_dir/2107RL_FM-D20210804-T214458.raw",
    #     netcdf_file_download_directory="./test_data_dir",
    #     echosounder="EK80",
    # )
    # convert_local_raw_to_netcdf(
    #     raw_file_location="./test_data_dir/2107RL_FM-D20210808-T033245.raw",
    #     netcdf_file_download_directory="./test_data_dir",
    #     echosounder="EK80",
    # )
    # convert_local_raw_to_netcdf(
    #     raw_file_location="./test_data_dir/2107RL_FM-D20211012-T022341.raw",
    #     netcdf_file_download_directory="./test_data_dir",
    #     echosounder="EK80",
    # )
    # download_netcdf(file_name="2107RL_CW-D20210813-T220732.raw",
    #                 file_type="nc", ship_name="Reuben_Lasker",
    #                 survey_name="RL2107", echosounder="EK80",
    #                 file_download_directory=".", gcp_bucket=gcp_bucket,
    #                 is_metadata=False,debug=False)
