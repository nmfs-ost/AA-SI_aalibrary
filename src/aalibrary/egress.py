"""This file contains functions related to data egress, such as uploading
files to cloud storage services.
"""

import glob
from pprint import pprint
import os
import logging

from google.cloud import storage
from tqdm import tqdm

# For pytests-sake
if __package__ is None or __package__ == "":
    # uses current directory visibility
    from utils import cloud_utils, helpers
    from utils.helpers import check_for_assertion_errors
else:
    # uses current package visibility
    from aalibrary.utils import cloud_utils, helpers
    from aalibrary.utils.helpers import check_for_assertion_errors


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
    Uploads all of the .raw (and their corresponding .idx/.bot/.nc) files from
    a echosounder directory into the appropriate location in the GCP storage
    bucket.
    NOTE: Assumes that all files share the same metadata.

    Args:
        local_echosounder_directory_to_upload (str, optional): The echosounder
            directory which contains all of the files you want to upload.
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


def upload_folder_as_is_to_gcp(
    local_folder_path: str = "",
    gcp_bucket: storage.Client.bucket = None,
    destination_prefix: str = "",
    debug: bool = False,
):
    """Uploads a local folder and its contents to a GCP storage bucket. Copies
    the folder AS-IS, maintaining the folder structure.
    NOTE: USE WITH CAUTION. THIS WILL UPLOAD EVERYTHING IN THE FOLDER IN THE
    SAME MANNER AS THE FOLDER ITSELF. THIS MEANS THAT RETRIEVAL OF THE FILES
    MIGHT NOT BE POSSIBLE IF THE FOLDER STRUCTURE DOES NOT ADHERE TO AALIBRARY
    NAMING CONVENTIONS.

    Args:
        local_folder_path (str): The path to the local folder to upload.
        gcp_bucket (storage.Client.bucket, optional): The GCP bucket object
            used to download the file. Defaults to None.
        destination_prefix (str, optional): Where to place the folder in the
            storage bucket. Defaults to "".
        debug (bool, optional): Whether or not to print debug statements.
            Defaults to False.
    """

    # normalize the path
    local_folder_path = os.path.normpath(local_folder_path)
    # Make sure GCP bucket is setup
    assert gcp_bucket is not None, "Please provide a gcp_bucket object."

    file_info = {}
    for root, _, files in os.walk(local_folder_path):
        for file_name in files:
            local_file_path = os.path.join(root, file_name)
            if os.path.isfile(local_file_path):
                file_size = os.path.getsize(local_file_path)
                file_info[local_file_path] = (file_size, file_name)
    file_info = sorted(file_info.items(), key=lambda item: item[1])
    if debug:
        pprint(file_info)

    for local_file_path, (file_size, file_name) in file_info:
        # Calculate the relative path from the local_folder_path
        relative_path = os.path.relpath(local_file_path, local_folder_path)

        # Construct the GCS blob name
        if destination_prefix:
            gcs_blob_name = os.path.join(
                destination_prefix, relative_path
            ).replace("\\", "/")
        else:
            gcs_blob_name = relative_path.replace("\\", "/")

        # Check if file already exists in GCP
        file_exists_in_gcp = cloud_utils.check_if_file_exists_in_gcp(
            bucket=gcp_bucket, file_path=gcs_blob_name
        )
        if file_exists_in_gcp:
            print(
                (
                    f"FILE `{file_name}` ALREADY EXISTS IN GCP AT"
                    f" `{gcs_blob_name}`. SKIPPING UPLOAD."
                )
            )
        else:
            blob = gcp_bucket.blob(gcs_blob_name)
            blob.upload_from_filename(local_file_path)
            print(f"Uploaded {local_file_path} to {gcs_blob_name}")


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
            cloud_utils.upload_file_to_gcp_bucket(
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


if __name__ == "__main__":
    gcp_stor_client, gcp_bucket_name, gcp_bucket = (
        cloud_utils.setup_gcp_storage_objs(
            project_id="ggn-nmfs-aa-dev-1",
            gcp_bucket_name="ggn-nmfs-aa-dev-1-data",
        )
    )

    # upload_local_echosounder_files_from_directory_to_gcp_storage_bucket(
    #     local_echosounder_directory_to_upload="./test_data_dir/Reuben_Lasker/RL2107/EK80/",
    #     ship_name="Reuben_Lasker",
    #     survey_name="RL2107",
    #     echosounder="EK80",
    #     data_source="HDD",
    #     gcp_bucket=gcp_bucket,
    #     debug=True,
    # )

    upload_folder_as_is_to_gcp(
        local_folder_path="./test_data_dir/Reuben_Lasker/",
        gcp_bucket=gcp_bucket,
        destination_prefix="other/deletable/",
    )
