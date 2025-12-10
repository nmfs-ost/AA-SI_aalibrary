"""This file contains code pertaining to auxiliary functions related to parsing
through our google storage bucket."""

from typing import List

from google.cloud import storage

# For pytests-sake
if __package__ is None or __package__ == "":
    # uses current directory visibility
    from cloud_utils import (
        setup_gcp_storage_objs,
        list_all_folders_in_gcp_bucket_location,
    )
    from helpers import normalize_ship_name
else:
    from aalibrary.utils.cloud_utils import (
        setup_gcp_storage_objs,
        list_all_folders_in_gcp_bucket_location,
    )
    from aalibrary.utils.helpers import normalize_ship_name


def get_all_ship_names_in_gcp_bucket(
    project_id: str = "ggn-nmfs-aa-dev-1",
    gcp_bucket_name: str = "ggn-nmfs-aa-dev-1-data",
    gcp_bucket: storage.Client.bucket = None,
    return_full_paths: bool = False,
) -> List[str]:
    """Gets all of the ship names within a GCP storage bucket.

    Args:
        project_id (str, optional): The GCP project ID that the storage bucket
            resides in.
            Defaults to "ggn-nmfs-aa-dev-1".
        gcp_bucket_name (str, optional): The GCP storage bucket name.
            Defaults to "ggn-nmfs-aa-dev-1-data".
        gcp_bucket (storage.Client.bucket, optional): The GCP storage bucket
            client object.
            If none, one will be created for you based on the `project_id` and
            `gcp_bucket_name`. Defaults to None.
        return_full_paths (bool, optional): Whether or not you want a full
            path from bucket root to the subdirectory returned. Set to false
            if you only want the subdirectory names listed. Defaults to False.
            NOTE: You can set this parameter to `True` if you would like to see
            which folders contain which ships.
            For example: Reuben Lasker can have data coming from both OMAO and
            local upload HDD. It will look like:
            {'OMAO/Reuben_Lasker/', 'HDD/Reuben_Lasker/'}

    Returns:
        List[str]: A list of strings containing the ship names.
    """

    if gcp_bucket is None:
        _, _, gcp_bucket = setup_gcp_storage_objs(
            project_id=project_id, gcp_bucket_name=gcp_bucket_name
        )
    # Get the initial subdirs
    prefixes = ["HDD/", "NCEI/", "OMAO/", "TEST/"]
    all_ship_names = set()
    for prefix in prefixes:
        ship_names = list_all_folders_in_gcp_bucket_location(
            location=prefix,
            gcp_bucket=gcp_bucket,
            return_full_paths=return_full_paths,
        )
        all_ship_names.update(ship_names)

    return list(all_ship_names)


def get_all_surveys_in_storage_bucket(
    project_id: str = "ggn-nmfs-aa-dev-1",
    gcp_bucket_name: str = "ggn-nmfs-aa-dev-1-data",
    gcp_bucket: storage.Client.bucket = None,
    return_full_paths: bool = False,
) -> List[str]:
    """Gets all of the surveys in a GCP storage bucket.

    Args:
        project_id (str, optional): The GCP project ID that the storage bucket
            resides in.
            Defaults to "ggn-nmfs-aa-dev-1".
        gcp_bucket_name (str, optional): The GCP storage bucket name.
            Defaults to "ggn-nmfs-aa-dev-1-data".
        gcp_bucket (storage.Client.bucket, optional): The GCP storage bucket
            client object.
            If none, one will be created for you based on the `project_id` and
            `gcp_bucket_name`. Defaults to None.
        return_full_paths (bool, optional): Whether or not you want a full
            path from bucket root to the subdirectory returned. Set to false
            if you only want the survey names listed. Defaults to False.

    Returns:
        List[str]: A list of strings containing the survey names.
    """

    if gcp_bucket is None:
        _, gcp_bucket_name, gcp_bucket = setup_gcp_storage_objs(
            project_id=project_id, gcp_bucket_name=gcp_bucket_name
        )

    all_ship_prefixes = get_all_ship_names_in_gcp_bucket(
        project_id=project_id,
        gcp_bucket_name=gcp_bucket_name,
        gcp_bucket=gcp_bucket,
        return_full_paths=True,
    )
    all_surveys = set()
    for ship_prefix in all_ship_prefixes:
        # Get surveys from each ship prefix
        ship_surveys = list_all_folders_in_gcp_bucket_location(
            location=ship_prefix,
            gcp_bucket=gcp_bucket,
            return_full_paths=return_full_paths,
        )
        all_surveys.update(ship_surveys)

    return list(all_surveys)


def get_all_survey_names_from_a_ship_in_storage_bucket(
    ship_name: str = "",
    project_id: str = "ggn-nmfs-aa-dev-1",
    gcp_bucket_name: str = "ggn-nmfs-aa-dev-1-data",
    gcp_bucket: storage.Client.bucket = None,
    return_full_paths: bool = False,
) -> List[str]:
    """Gets all of the survey names from a particular ship in a GCP storage
    bucket.

    Args:
        ship_name (str, optional): The ship's name you want to get all surveys
            from. Will get normalized to GCP standards. Defaults to None.
        project_id (str, optional): The GCP project ID that the storage bucket
            resides in.
            Defaults to "ggn-nmfs-aa-dev-1".
        gcp_bucket_name (str, optional): The GCP storage bucket name.
            Defaults to "ggn-nmfs-aa-dev-1-data".
        gcp_bucket (storage.Client.bucket, optional): The GCP storage bucket
            client object.
            If none, one will be created for you based on the `project_id` and
            `gcp_bucket_name`. Defaults to None.
        return_full_paths (bool, optional): Whether or not you want a full
            path from bucket root to the subdirectory returned. Set to false
            if you only want the survey names listed. Defaults to False.

    Returns:
        List[str]: A list of strings containing the survey names.
    """

    if gcp_bucket is None:
        _, _, gcp_bucket = setup_gcp_storage_objs(
            project_id=project_id, gcp_bucket_name=gcp_bucket_name
        )

    # Normalize the ship name.
    ship_name = normalize_ship_name(ship_name=ship_name)
    # Search all possible directories for ship surveys
    prefixes = [
        f"HDD/{ship_name}/",
        f"NCEI/{ship_name}/",
        f"OMAO/{ship_name}/",
        f"TEST/{ship_name}/",
    ]
    all_survey_names = set()
    for prefix in prefixes:
        survey_names = list_all_folders_in_gcp_bucket_location(
            location=prefix,
            gcp_bucket=gcp_bucket,
            return_full_paths=return_full_paths,
        )
        all_survey_names.update(survey_names)

    return list(all_survey_names)


def get_all_echosounders_in_a_survey_in_storage_bucket(
    ship_name: str = "",
    survey_name: str = "",
    project_id: str = "ggn-nmfs-aa-dev-1",
    gcp_bucket_name: str = "ggn-nmfs-aa-dev-1-data",
    gcp_bucket: storage.Client.bucket = None,
    return_full_paths: bool = False,
) -> List[str]:
    """Gets all of the echosounders in a survey in a GCP storage bucket.

    Args:
        ship_name (str, optional): The ship's name you want to get all surveys
            from. Will get normalized to GCP standards. Defaults to None.
        survey_name (str, optional): The survey name/identifier.
            Defaults to "".
        project_id (str, optional): The GCP project ID that the storage bucket
            resides in.
            Defaults to "ggn-nmfs-aa-dev-1".
        gcp_bucket_name (str, optional): The GCP storage bucket name.
            Defaults to "ggn-nmfs-aa-dev-1-data".
        gcp_bucket (storage.Client.bucket, optional): The GCP storage bucket
            client object.
            If none, one will be created for you based on the `project_id` and
            `gcp_bucket_name`. Defaults to None.
        return_full_paths (bool, optional): Whether or not you want a full
            path from bucket root to the subdirectory returned. Set to false
            if you only want the survey names listed. Defaults to False.

    Returns:
        List[str]: A list of strings containing the echosounder names that
            exist in a survey.
    """

    if gcp_bucket is None:
        _, _, gcp_bucket = setup_gcp_storage_objs(
            project_id=project_id, gcp_bucket_name=gcp_bucket_name
        )

    # Normalize the ship name.
    ship_name = normalize_ship_name(ship_name=ship_name)
    # Search all possible directories for ship surveys
    prefixes = [
        f"HDD/{ship_name}/{survey_name}/",
        f"NCEI/{ship_name}/{survey_name}/",
        f"OMAO/{ship_name}/{survey_name}/",
        f"TEST/{ship_name}/{survey_name}/",
    ]
    all_subfolder_names = set()
    all_echosounders = set()
    # Get all subfolders from this survey, whichever directory it resides in.
    for prefix in prefixes:
        subfolder_names = list_all_folders_in_gcp_bucket_location(
            location=prefix,
            gcp_bucket=gcp_bucket,
            return_full_paths=return_full_paths,
        )
        all_subfolder_names.update(subfolder_names)
    # Filter out any folder that is not an echosounder.
    for folder_name in list(all_subfolder_names):
        if (
            ("calibration" not in folder_name.lower())
            and ("metadata" not in folder_name.lower())
            and ("json" not in folder_name.lower())
            and ("doc" not in folder_name.lower())
        ):
            # Use 'add' since each 'folder_name' is a string.
            all_echosounders.add(folder_name)

    return list(all_echosounders)


def get_all_echosounders_that_exist_in_storage_bucket(): ...


def get_all_file_names_from_survey_in_storage_bucket(): ...


def get_all_raw_file_names_from_survey_in_storage_bucket(): ...


def get_random_raw_file_from_storage_bucket(): ...


def get_echosounder_from_raw_file_in_storage_bucket(): ...


def check_if_tugboat_metadata_json_exists_in_survey(): ...


def get_closest_gcp_formatted_ship_name(): ...


def get_all_metadata_files_in_survey_in_storage_bucket(): ...


def check_if_cruise_exists_fully_in_storage_bucket(): ...


def get_netcdf_files_from_survey(): ...


def rename_gcs_folder(
    gcp_bucket_name: str = "",
    old_folder_prefix: str = "",
    new_folder_prefix: str = "",
) -> None:
    """Renames a 'folder' in a GCS bucket by renaming its contained objects.

    Args:
        gcp_bucket_name (str, optional): The GCP bucket where the folder
            resides. Defaults to "".
        old_folder_prefix (str, optional): The old folder prefix.
            Ex. ""other/HBigelow/"
            Defaults to "".
        new_folder_prefix (str, optional): The new folder prefix.
            Ex. "other/Henry_B_Bigelow/"
            NOTE: Make sure to include the other folders too.
            Defaults to "".
    """

    storage_client = storage.Client()
    bucket = storage_client.bucket(gcp_bucket_name)

    # Ensure prefixes end with a slash for proper "folder" handling
    if not old_folder_prefix.endswith("/"):
        old_folder_prefix += "/"
    if not new_folder_prefix.endswith("/"):
        new_folder_prefix += "/"

    blobs = bucket.list_blobs(prefix=old_folder_prefix)

    for blob in blobs:
        # Construct the new blob name
        new_blob_name = new_folder_prefix + blob.name[len(old_folder_prefix):]

        # Rename the blob
        new_blob = bucket.rename_blob(blob, new_blob_name)
        print(f"Renamed {blob.name} to {new_blob.name}")


def move_folder_in_gcs(
    gcp_bucket_name: str = "",
    source_prefix: str = "",
    destination_prefix: str = "",
) -> None:
    """Moves all objects under a given source_prefix to a new
    destination_prefix within the same bucket.

    Args:
        gcp_bucket_name (str, optional): The GCP bucket where the folder
            resides. Defaults to "".
        source_prefix (str, optional): The folder or object prefix to move.
            NOTE: If moving a folder, make sure to include the trailing slash.
            Defaults to "".
        destination_prefix (str, optional): The destination prefix to move the
            folder or object to.
            NOTE: If moving a folder, make sure to include the trailing slash.
            Defaults to "".
    """

    storage_client = storage.Client()
    bucket = storage_client.bucket(gcp_bucket_name)

    # List all blobs (objects) with the source prefix
    blobs = bucket.list_blobs(prefix=source_prefix)

    for blob in blobs:
        # Construct the new blob name
        # This removes the source_prefix and adds the destination_prefix
        new_blob_name = destination_prefix + blob.name[len(source_prefix):]

        # Copy the blob to the new name
        new_blob = bucket.copy_blob(blob, bucket, new_name=new_blob_name)

        # Delete the original blob
        blob.delete()

        print(f"Moved '{blob.name}' to '{new_blob.name}'")


def copy_folder_within_gcs(
    gcp_bucket_name: str = "",
    source_prefix: str = "",
    destination_prefix: str = "",
) -> None:
    """Copies all objects under a given source_prefix to a new
    destination_prefix within the same bucket.

    Args:
        gcp_bucket_name (str, optional): The GCP bucket where the folder
            resides. Defaults to "".
        source_prefix (str, optional): The folder or object prefix to copy.
            NOTE: If copying a folder, make sure to include the trailing slash.
            Defaults to "".
        destination_prefix (str, optional): The destination prefix to copy the
            folder or object to.
            NOTE: If copying a folder, make sure to include the trailing slash.
            Defaults to "".
    """

    storage_client = storage.Client()
    bucket = storage_client.bucket(gcp_bucket_name)

    # List all blobs (objects) with the source prefix
    blobs = bucket.list_blobs(prefix=source_prefix)

    for blob in blobs:
        # Construct the new blob name
        # This removes the source_prefix and adds the destination_prefix
        new_blob_name = destination_prefix + blob.name[len(source_prefix):]

        # Copy the blob to the new name
        new_blob = bucket.copy_blob(blob, bucket, new_name=new_blob_name)

        print(f"Copied '{blob.name}' to '{new_blob.name}'")


if __name__ == "__main__":
    # all_ship_names = get_all_ship_names_in_gcp_bucket()
    # print(all_ship_names)

    # all_surveys = get_all_surveys_in_storage_bucket()
    # print(all_surveys)

    # all_rl_surveys = get_all_survey_names_from_a_ship_in_storage_bucket(
    #     ship_name="reuben lasker", return_full_paths=True
    # )
    # print(all_rl_surveys)

    # all_rl2107_echos = get_all_echosounders_in_a_survey_in_storage_bucket(
    #     ship_name="reuben lasker", survey_name="RL2107"
    # )
    # print(all_rl2107_echos)

    rename_gcs_folder(
        gcp_bucket_name="ggn-nmfs-aa-dev-1-data",
        old_folder_prefix="other/HB_Data/",
        new_folder_prefix="other/Henry_B_Bigelow/",
    )
