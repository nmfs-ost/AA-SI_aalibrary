"""This file contains functions related to data egress, such as uploading
files to cloud storage services.
"""

import glob
from pprint import pprint
import os
import logging
import re

from difflib import get_close_matches

from google.cloud import storage
from tqdm import tqdm

# For pytests-sake
if __package__ is None or __package__ == "":
    # uses current directory visibility
    import config
    import metadata
    from raw_file import RawFile
    from utils import cloud_utils, helpers
    from utils.helpers import check_for_assertion_errors
else:
    # uses current package visibility
    from aalibrary import config
    from aalibrary import metadata
    from aalibrary.raw_file import RawFile
    from aalibrary.utils import cloud_utils, helpers
    from aalibrary.utils.helpers import check_for_assertion_errors

_log = logging.getLogger(__name__)


# The echosounder files the directory uploaders look for, in upload order:
# .idx and .bot before .raw, so a .raw file's metadata finds them in GCP.
_ECHOSOUNDER_FILE_TYPES = (
    ("idx", "*.idx"),
    ("bot", "*.bot"),
    ("raw", "*.raw"),
    ("netcdf", "*.nc"),
)


def _echosounder_files(directory: str) -> dict:
    """The echosounder files in `directory` (not its subfolders), by type."""
    return {
        file_type: sorted(glob.glob(os.path.join(directory, pattern)))
        for file_type, pattern in _ECHOSOUNDER_FILE_TYPES
    }


def _check_ship_name(
    ship_name_normalized: str, provisional_ship_name: bool, refuse: bool
) -> None:
    """Checks the ship name against the ICES ship list before anything is
    uploaded. An unlisted name is refused when `refuse` (the metadata step
    would refuse it after the uploads), unless it's marked provisional;
    otherwise it's a warning."""
    try:
        names = RawFile.valid_ICES_ship_names
    except Exception as e:  # ICES didn't answer
        if refuse:
            raise
        _log.warning(
            "Couldn't check `%s` against the ICES ship list: %s",
            ship_name_normalized,
            e,
        )
        return
    if ship_name_normalized in names:
        return
    close = get_close_matches(ship_name_normalized, names, n=3, cutoff=0.6)
    hint = f" Did you mean one of {close}?" if close else ""
    if provisional_ship_name:
        print(
            f"NOTE: `{ship_name_normalized}` isn't in the ICES ship list; "
            "uploading under it as a provisional name (no ICES code)."
            f"{hint}"
        )
    elif refuse:
        raise ValueError(
            f"The ship name `{ship_name_normalized}` isn't in the ICES ship "
            f"list.{hint} Nothing has been uploaded. If this ship or platform "
            "doesn't have an ICES code yet, pass provisional_ship_name=True "
            "(aa-upload: --provisional-ship-name)."
        )
    else:
        _log.warning(
            "The ship name `%s` isn't in the ICES ship list.%s",
            ship_name_normalized,
            hint,
        )


def _metadata_tables(files: dict) -> list:
    tables = []
    if files.get("raw"):
        tables.append(metadata.RAW_FILE_METADATA_TABLE)
    if files.get("netcdf"):
        tables.append(metadata.NETCDF_FILE_METADATA_TABLE)
    return tables


def _upload_summary(file_type: str, results: list) -> str:
    uploaded = sum(1 for status, _ in results if status == "uploaded")
    present = sum(1 for status, _ in results if status == "exists")
    failed = sum(1 for status, _ in results if status == "failed")
    line = (
        f"{len(results)} {file_type.upper()} FILES: {uploaded} UPLOADED, "
        f"{present} ALREADY IN GCP"
    )
    return line + (f", {failed} FAILED (see the errors above)." if failed else ".")


def upload_local_echosounder_files_from_directory_to_gcp_storage_bucket(
    local_echosounder_directory_to_upload: str,
    ship_name: str,
    survey_name: str,
    echosounder: str,
    data_source: str,
    gcp_bucket: storage.Client.bucket = None,
    debug: bool = False,
    upload_metadata: bool = True,
    provisional_ship_name: bool = False,
):
    """ENTRYPOINT FOR END-USERS
    Uploads all of the .raw (and their corresponding .idx/.bot/.nc) files from
    a echosounder directory into the appropriate location in the GCP storage
    bucket, then records the .raw and .nc files' metadata in BigQuery.
    NOTE: Assumes that all files share the same metadata.

    Everything that can be checked is checked before anything is uploaded:
    the directory, the names, the ship name (against the ICES ship list), and
    whether your account can add the metadata to BigQuery. Files already in
    the bucket aren't uploaded again, and files whose metadata is already in
    BigQuery don't get a second row, so running this again is safe (it's how
    you finish an interrupted upload).

    Args:
        local_echosounder_directory_to_upload (str): Required. The folder on
            your computer that holds the files (its subfolders aren't
            searched).
        ship_name (str): Required. The ship name, e.g. "Henry B. Bigelow"
            (normalized to "Henry_B_Bigelow" for the bucket).
        survey_name (str): Required. The survey name/identifier, e.g.
            "HB2407".
        echosounder (str): Required. The echosounder used to gather the data,
            e.g. "EK80".
        data_source (str): Required. Where the files come from: one of
            ["NCEI", "OMAO", "HDD", "TEST"]. "HDD" for files on a local drive.
        gcp_bucket (storage.Client.bucket, optional): The GCP bucket object to
            upload to. Defaults to None: the bucket aalibrary is configured
            for.
        debug (bool, optional): Whether or not to print debug statements.
            Defaults to False.
        upload_metadata (bool, optional): Whether to record the files'
            metadata in BigQuery after uploading them. Defaults to True.
            False uploads the files only (no BigQuery access needed); their
            metadata can be recorded later with
            `upload_local_echosounder_metadata_from_directory_to_bigquery`.
        provisional_ship_name (bool, optional): Set to True for a ship or
            platform that isn't in the ICES ship list yet. Defaults to False,
            which refuses an unlisted name before uploading anything (it's
            usually a misspelling).
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
        os.fspath(local_echosounder_directory_to_upload)
    )
    # Check that the directory exists, and the names
    check_for_assertion_errors(
        directory=local_echosounder_directory_to_upload,
        ship_name=ship_name,
        survey_name=survey_name,
        echosounder=echosounder,
        data_source=data_source,
    )

    # normalize ship name
    ship_name_normalized = helpers.normalize_ship_name(ship_name)
    if debug:
        print(f"NORMALIZED SHIP NAME: {ship_name_normalized}")
        print(
            "LOCAL DIRECTORY TO UPLOAD:"
            f" {local_echosounder_directory_to_upload}"
        )
    # Make sure GCP bucket is setup with default values if None specified.
    if gcp_bucket is None:
        _, _, gcp_bucket = cloud_utils.setup_gcp_storage_objs(verbose=True)

    # Check (glob) for raw, idx, bot and netcdf files.
    print("CHECKING DIRECTORY FOR RAW, IDX, BOT, AND NETCDF FILES...")
    files = _echosounder_files(local_echosounder_directory_to_upload)

    # Let the user know how many of each file has been found to upload.
    print(
        (
            f"FOUND {len(files['raw'])} RAW FILES | {len(files['idx'])} IDX "
            f"FILES | {len(files['bot'])} BOT FILES | "
            f"{len(files['netcdf'])} NETCDF FILES"
        )
    )

    # Before uploading anything: the checks the metadata step would otherwise
    # fail on after the uploads.
    tables = _metadata_tables(files) if upload_metadata else []
    _check_ship_name(
        ship_name_normalized, provisional_ship_name, refuse=bool(tables)
    )
    if tables:
        problem = metadata.check_metadata_write_access(tables=tables)
        if problem:
            raise PermissionError(
                f"{problem} Nothing has been uploaded. Either ask a project "
                "admin for the BigQuery Data Editor role on that dataset, or "
                "upload the files now without their metadata "
                "(upload_metadata=False; aa-upload: --no-metadata) and record "
                "it later with "
                "upload_local_echosounder_metadata_from_directory_to_bigquery()."
            )

    # Upload each type of file to its place in the bucket. The upload function
    # skips files that are already there.
    counts = {}
    for file_type, _ in _ECHOSOUNDER_FILE_TYPES:
        if not files[file_type]:
            counts[file_type] = 0
            continue
        results = []
        for file_path in tqdm(
            files[file_type], desc=f"Uploading {file_type} files"
        ):
            results.append(
                _upload_file(
                    file_name=os.path.basename(file_path),
                    file_type=file_type,
                    ship_name=ship_name_normalized,
                    survey_name=survey_name,
                    echosounder=echosounder,
                    file_location=file_path,
                    gcp_bucket=gcp_bucket,
                    data_source=data_source,
                    verbose=False,
                    debug=debug,
                )
            )
        counts[file_type] = sum(
            1 for status, _ in results if status != "failed"
        )
        print(_upload_summary(file_type, results))

    print(
        (
            f"UPLOADS COMPLETE\nRAW ({counts['raw']}) | IDX "
            f"({counts['idx']}) | BOT ({counts['bot']}) | "
            f"NETCDF ({counts['netcdf']}) IN GCP"
        )
    )

    if upload_metadata:
        _record_echosounder_metadata(
            directory=local_echosounder_directory_to_upload,
            files=files,
            ship_name=ship_name,
            survey_name=survey_name,
            echosounder=echosounder,
            data_source=data_source,
            gcp_bucket=gcp_bucket,
            provisional_ship_name=provisional_ship_name,
            debug=debug,
        )
    elif files["raw"] or files["netcdf"]:
        print(
            "METADATA NOT RECORDED (upload_metadata=False). To record it later,"
            " call upload_local_echosounder_metadata_from_directory_to_bigquery"
            "() with the same arguments."
        )


def upload_local_echosounder_metadata_from_directory_to_bigquery(
    local_echosounder_directory: str,
    ship_name: str,
    survey_name: str,
    echosounder: str,
    data_source: str,
    gcp_bucket: storage.Client.bucket = None,
    provisional_ship_name: bool = False,
    debug: bool = False,
):
    """ENTRYPOINT FOR END-USERS
    Records the BigQuery metadata of the .raw and .nc files in a local
    echosounder directory whose files are already in the GCP storage bucket
    (uploaded with `upload_metadata=False`, or by an upload that couldn't
    record their metadata). Uploads nothing to the bucket. Files whose
    metadata is already in BigQuery are skipped, so running it twice is safe.

    Args:
        local_echosounder_directory (str): Required. The folder on your
            computer that holds the files (each .raw file's first datagram is
            read from it).
        ship_name (str): Required. The ship name, as given for the upload.
        survey_name (str): Required. The survey name/identifier.
        echosounder (str): Required. The echosounder, e.g. "EK80".
        data_source (str): Required. One of ["NCEI", "OMAO", "HDD", "TEST"],
            as given for the upload.
        gcp_bucket (storage.Client.bucket, optional): The GCP bucket object
            the files were uploaded to. Defaults to None: the bucket aalibrary
            is configured for.
        provisional_ship_name (bool, optional): Set to True for a ship or
            platform that isn't in the ICES ship list yet. Defaults to False.
        debug (bool, optional): Whether or not to print debug statements.
            Defaults to False.
    """
    local_echosounder_directory = os.path.normpath(
        os.fspath(local_echosounder_directory)
    )
    check_for_assertion_errors(
        directory=local_echosounder_directory,
        ship_name=ship_name,
        survey_name=survey_name,
        echosounder=echosounder,
        data_source=data_source,
    )
    if gcp_bucket is None:
        _, _, gcp_bucket = cloud_utils.setup_gcp_storage_objs(verbose=True)
    files = _echosounder_files(local_echosounder_directory)
    tables = _metadata_tables(files)
    if not tables:
        print("NO RAW OR NETCDF FILES FOUND; NO METADATA TO RECORD.")
        return
    _check_ship_name(
        helpers.normalize_ship_name(ship_name), provisional_ship_name, True
    )
    problem = metadata.check_metadata_write_access(tables=tables)
    if problem:
        raise PermissionError(
            f"{problem} Ask a project admin for the BigQuery Data Editor role "
            "on that dataset, then run this again."
        )
    _record_echosounder_metadata(
        directory=local_echosounder_directory,
        files=files,
        ship_name=ship_name,
        survey_name=survey_name,
        echosounder=echosounder,
        data_source=data_source,
        gcp_bucket=gcp_bucket,
        provisional_ship_name=provisional_ship_name,
        debug=debug,
    )


def _record_echosounder_metadata(
    directory: str,
    files: dict,
    ship_name: str,
    survey_name: str,
    echosounder: str,
    data_source: str,
    gcp_bucket,
    provisional_ship_name: bool,
    debug: bool,
) -> dict:
    """Adds a BigQuery metadata row for each .raw and .nc file in `files`
    that's in the bucket and doesn't have one yet. A permission error stops
    it (no other row would be added either); any other error is reported for
    that file and the rest carry on. Returns the counts."""
    ship_name_normalized = helpers.normalize_ship_name(ship_name)
    kinds = (
        ("raw", metadata.RAW_FILE_METADATA_TABLE, "raw"),
        ("netcdf", metadata.NETCDF_FILE_METADATA_TABLE, "netcdf"),
    )
    counts = {"recorded": 0, "already": 0, "not_in_gcp": 0, "failed": 0}
    for file_type, table, folder_type in kinds:
        if not files.get(file_type):
            continue
        locations = {
            file_path: helpers.parse_correct_gcp_storage_bucket_location(
                file_name=os.path.basename(file_path),
                file_type=folder_type,
                ship_name=ship_name_normalized,
                survey_name=survey_name,
                echosounder=echosounder,
                data_source=data_source,
            )
            for file_path in files[file_type]
        }
        recorded = metadata.metadata_already_recorded(
            locations.values(), table=table
        )
        if recorded is None:
            print(
                f"NOTE: couldn't check which of these files already have "
                f"metadata in {table}; recording it for all of them."
            )
            recorded = set()
        for file_path in tqdm(
            files[file_type], desc=f"Recording {file_type} metadata"
        ):
            file_name = os.path.basename(file_path)
            if locations[file_path] in recorded:
                counts["already"] += 1
                continue
            try:
                rf = RawFile(
                    file_name=file_name,
                    file_type=file_type,
                    ship_name=ship_name,
                    survey_name=survey_name,
                    echosounder=echosounder,
                    file_download_directory=directory,
                    data_source=data_source,
                    # All three, or RawFile replaces the bucket with the
                    # configured one (and would look for the files there).
                    gcp_bucket=gcp_bucket,
                    gcp_bucket_name=getattr(gcp_bucket, "name", None),
                    gcp_stor_client=getattr(gcp_bucket, "client", None),
                    provisional_ship_name=provisional_ship_name,
                    debug=debug,
                )
                in_gcp = (
                    rf.raw_file_exists_in_gcp
                    if file_type == "raw"
                    else rf.netcdf_file_exists_in_gcp
                )
                if not in_gcp:
                    counts["not_in_gcp"] += 1
                    print(
                        f"SKIPPED `{file_name}`: it isn't in GCP, so its "
                        "metadata wasn't recorded."
                    )
                    continue
                if file_type == "raw":
                    metadata.create_and_upload_metadata_df_for_raw(
                        rf=rf, debug=debug
                    )
                else:
                    metadata.create_and_upload_metadata_df_for_netcdf(
                        rf=rf, debug=debug
                    )
                counts["recorded"] += 1
            except Exception as e:
                if metadata.is_permission_error(e):
                    raise PermissionError(
                        f"BigQuery refused the metadata for `{file_name}` "
                        f"({e}). The files are in GCP; {counts['recorded']} "
                        "metadata rows were recorded before this. Once your "
                        "account has the BigQuery Data Editor role on the "
                        "metadata dataset, run "
                        "upload_local_echosounder_metadata_from_directory_to_"
                        "bigquery() with the same arguments to record the "
                        "rest."
                    ) from e
                counts["failed"] += 1
                _log.error(
                    "COULD NOT RECORD THE METADATA OF %s:\n%s", file_name, e
                )
    print(
        f"METADATA: {counts['recorded']} RECORDED, {counts['already']} ALREADY"
        f" IN BIGQUERY, {counts['not_in_gcp']} NOT IN GCP, "
        f"{counts['failed']} FAILED."
    )
    return counts


def upload_local_calibration_files_from_directory_to_gcp_storage_bucket(
    local_calibration_directory_to_upload: str,
    ship_name: str,
    survey_name: str,
    echosounder: str,
    data_source: str,
    gcp_bucket: storage.Client.bucket = None,
    debug: bool = False,
):
    """ENTRYPOINT FOR END-USERS
    Uploads all of the files from a local calibration directory into the
    appropriate location in the GCP storage bucket.
    NOTE: Assumes that all files share the same metadata.

    Args:
        local_calibration_directory_to_upload (str): Required. The calibration
            directory which contains all of the files you want to upload.
        ship_name (str): Required. The ship name associated with this survey
            (normalized for the bucket, e.g. "Henry_B_Bigelow").
        survey_name (str): Required. The survey name/identifier.
        echosounder (str): Required. The echosounder used to gather the data,
            e.g. "EK80".
        data_source (str): Required. The source of the file. Necessary due to
            the way the storage bucket is organized. Can be one of
            ["NCEI", "OMAO", "HDD", "TEST"].
        gcp_bucket (storage.Client.bucket, optional): The GCP bucket object
            used to upload the files. Defaults to None: the bucket aalibrary
            is configured for.
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
    local_calibration_directory_to_upload = os.path.normpath(
        local_calibration_directory_to_upload
    )
    # Check that the directory exists
    check_for_assertion_errors(
        directory=local_calibration_directory_to_upload,
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
            f" {local_calibration_directory_to_upload}"
        )
    # Make sure GCP bucket is setup with default values if None specified.
    if gcp_bucket is None:
        _, _, gcp_bucket = cloud_utils.setup_gcp_storage_objs(verbose=True)

    # Check (glob) for raw and idx files.
    print("CHECKING DIRECTORY FOR FILES...")
    all_files = [
        x
        for x in glob.glob(
            os.sep.join([local_calibration_directory_to_upload, "*"])
        )
    ]
    file_upload_count = 0
    file_error_count = 0

    # Upload each file to GCP at the correct location.
    if len(all_files) > 0:
        pbar = tqdm(all_files, desc="Uploading calibration files")
        for file in pbar:
            try:
                file_name = file.split(os.sep)[-1]
                pbar.set_postfix_str(f"{file_name}")
                # Upload file to GCP at the correct storage bucket location.
                # The function already checks if the file exists.
                result = upload_file_to_gcp_storage_bucket(
                    file_name=file_name,
                    ship_name=ship_name_normalized,
                    survey_name=survey_name,
                    echosounder=echosounder,
                    file_location=file,
                    gcp_bucket=gcp_bucket,
                    data_source=data_source,
                    is_survey_metadata=False,
                    is_calibration_file=True,
                    is_calibration_mapping_file=False,
                    is_auxiliary_file=False,
                    verbose=False,
                    debug=debug,
                )
                if result is None:
                    file_error_count += 1
                else:
                    file_upload_count += 1
            except Exception as e:
                file_error_count += 1
                print(e)
    print(
        f"{file_upload_count} FILES IN GCP (UPLOADED OR ALREADY THERE) WITH"
        f" {file_error_count} ERRORS."
    )


def upload_local_auxiliary_files_from_directory_to_gcp_storage_bucket(
    local_auxiliary_directory_to_upload: str,
    ship_name: str,
    survey_name: str,
    echosounder: str,
    data_source: str,
    gcp_bucket: storage.Client.bucket = None,
    debug: bool = False,
):
    """ENTRYPOINT FOR END-USERS
    Uploads all of the files from a local auxiliary directory into the
    appropriate location in the GCP storage bucket.
    NOTE: Assumes that all files share the same metadata.

    Args:
        local_auxiliary_directory_to_upload (str): Required. The auxiliary
            directory which contains all of the files you want to upload.
        ship_name (str): Required. The ship name associated with this survey
            (normalized for the bucket, e.g. "Henry_B_Bigelow").
        survey_name (str): Required. The survey name/identifier.
        echosounder (str): Required. The echosounder used to gather the data,
            e.g. "EK80".
        data_source (str): Required. The source of the file. Necessary due to
            the way the storage bucket is organized. Can be one of
            ["NCEI", "OMAO", "HDD", "TEST"].
        gcp_bucket (storage.Client.bucket, optional): The GCP bucket object
            used to upload the files. Defaults to None: the bucket aalibrary
            is configured for.
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
    local_auxiliary_directory_to_upload = os.path.normpath(
        local_auxiliary_directory_to_upload
    )
    # Check that the directory exists
    check_for_assertion_errors(
        directory=local_auxiliary_directory_to_upload,
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
            f" {local_auxiliary_directory_to_upload}"
        )
    # Make sure GCP bucket is setup with default values if None specified.
    if gcp_bucket is None:
        _, _, gcp_bucket = cloud_utils.setup_gcp_storage_objs(verbose=True)

    # Check (glob) for raw and idx files.
    print("CHECKING DIRECTORY FOR FILES...")
    all_files = [
        x
        for x in glob.glob(
            os.sep.join([local_auxiliary_directory_to_upload, "*"])
        )
    ]
    file_upload_count = 0
    file_error_count = 0

    # Upload each file to GCP at the correct location.
    if len(all_files) > 0:
        pbar = tqdm(all_files, desc="Uploading auxiliary files")
        for file in pbar:
            try:
                file_name = file.split(os.sep)[-1]
                pbar.set_postfix_str(f"{file_name}")
                # Upload file to GCP at the correct storage bucket location.
                # The function already checks if the file exists.
                result = upload_file_to_gcp_storage_bucket(
                    file_name=file_name,
                    ship_name=ship_name_normalized,
                    survey_name=survey_name,
                    echosounder=echosounder,
                    file_location=file,
                    gcp_bucket=gcp_bucket,
                    data_source=data_source,
                    is_survey_metadata=False,
                    is_calibration_file=False,
                    is_calibration_mapping_file=False,
                    is_auxiliary_file=True,
                    verbose=False,
                    debug=debug,
                )
                if result is None:
                    file_error_count += 1
                else:
                    file_upload_count += 1
            except Exception as e:
                file_error_count += 1
                print(e)
    print(
        f"{file_upload_count} FILES IN GCP (UPLOADED OR ALREADY THERE) WITH"
        f" {file_error_count} ERRORS."
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
    # Make sure GCP bucket is setup with default values if None specified.
    if gcp_bucket is None:
        _, _, gcp_bucket = cloud_utils.setup_gcp_storage_objs(verbose=True)

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
                    f"INFO: FILE `{file_name}` ALREADY EXISTS IN GCP AT"
                    f" `{gcs_blob_name}`. SKIPPING UPLOAD."
                )
            )
        else:
            blob = gcp_bucket.blob(gcs_blob_name)
            blob.upload_from_filename(local_file_path)
            print(f"Uploaded {local_file_path} to {gcs_blob_name}")


def _local_file(file_name, file_location) -> tuple:
    """The file to upload: (its path on this computer, its file name).
    `file_location` may be the file's full path or the folder it's in."""
    file_name = os.fspath(file_name) if file_name else ""
    file_location = os.fspath(file_location) if file_location else ""
    if not file_location and file_name and os.path.isfile(file_name):
        # The file's full path was given as file_name.
        file_location = file_name
    assert file_location != "", (
        "A `file_location` is needed to upload: the file's path on your "
        "computer (or the folder it's in, with `file_name`)."
    )
    if os.path.isdir(file_location):
        assert file_name != "", (
            f"`file_location` is a folder ({file_location}); give `file_name`"
            " too, or give the file's full path as `file_location`."
        )
        local_path = os.path.join(file_location, os.path.basename(file_name))
    else:
        local_path = file_location
    name = os.path.basename(file_name) if file_name else ""
    return local_path, name or os.path.basename(local_path)


# A file extension: a dot and up to 10 letters/digits, e.g. ".raw", ".nc".
# (Folder names like "Henry_B._Bigelow" or "Henry B. Bigelow" have none.)
_EXTENSION = re.compile(r"\.[A-Za-z0-9]{1,10}$")


def _bucket_object_path(location, file_name: str, bucket) -> str:
    """The object path within the bucket for `gcp_storage_bucket_location`.
    Accepts what people naturally write: backslashes (a Windows path), a
    leading "gs://bucket/" or the bucket's own name, and a folder rather than
    the full object path (it ends in "/", or its last part has no file
    extension while the file has one), in which case the file name is added.
    """
    path = os.fspath(location).strip().replace("\\", "/")
    bucket_name = getattr(bucket, "name", None)
    if path.lower().startswith("gs://"):
        named, _, path = path[len("gs://"):].partition("/")
        if bucket_name and named != bucket_name:
            raise ValueError(
                f"`gcp_storage_bucket_location` {location} is in bucket "
                f"`{named}`, but this upload goes to bucket `{bucket_name}`."
            )
    path = re.sub("/+", "/", path).lstrip("/")
    if bucket_name and (path == bucket_name or path.startswith(bucket_name + "/")):
        path = path[len(bucket_name):].lstrip("/")
    last = path.rsplit("/", 1)[-1]
    is_folder = (
        path == ""
        or path.endswith("/")
        or (
            last != file_name
            and not _EXTENSION.search(last)
            and bool(_EXTENSION.search(file_name))
        )
    )
    if is_folder:
        path = f"{path.rstrip('/')}/{file_name}".lstrip("/")
    return path


def _upload_file(
    file_name: str = "",
    file_type: str = "",
    ship_name: str = "",
    survey_name: str = "",
    echosounder: str = "",
    file_location: str = "",
    gcp_storage_bucket_location: str = "",
    gcp_bucket: storage.Client.bucket = None,
    data_source: str = "",
    user_email: str = "",
    is_survey_metadata: bool = False,
    is_calibration_file: bool = False,
    is_calibration_mapping_file: bool = False,
    is_auxiliary_file: bool = False,
    is_derived_product: bool = False,
    force_upload: bool = False,
    verbose: bool = True,
    debug: bool = False,
) -> tuple:
    """upload_file_to_gcp_storage_bucket, returning (status, object path):
    status is "uploaded", "exists" (it was already there) or "failed"."""
    local_path, file_name = _local_file(file_name, file_location)
    if not file_type and "." in file_name:
        file_type = file_name.rsplit(".", 1)[-1]
    if gcp_bucket is None:
        _, _, gcp_bucket = cloud_utils.setup_gcp_storage_objs()

    if gcp_storage_bucket_location:
        gcp_storage_bucket_location = _bucket_object_path(
            gcp_storage_bucket_location, file_name, gcp_bucket
        )
    else:
        gcp_storage_bucket_location = (
            helpers.parse_correct_gcp_storage_bucket_location(
                file_name=file_name,
                file_type=file_type,
                # The same folder name the directory uploader and the
                # download functions use.
                ship_name=(
                    helpers.normalize_ship_name(ship_name) if ship_name else ""
                ),
                survey_name=survey_name,
                echosounder=echosounder,
                data_source=data_source,
                user_email=user_email,
                is_survey_metadata=is_survey_metadata,
                is_calibration_file=is_calibration_file,
                is_calibration_mapping_file=is_calibration_mapping_file,
                is_auxiliary_file=is_auxiliary_file,
                is_derived_product=is_derived_product,
                debug=debug,
            )
        )
    gcs_uri = f"gs://{getattr(gcp_bucket, 'name', '')}/{gcp_storage_bucket_location}"

    # Check if the file exists in GCP
    file_exists_in_gcp = cloud_utils.check_if_file_exists_in_gcp(
        gcp_bucket, file_path=gcp_storage_bucket_location
    )
    if file_exists_in_gcp and not force_upload:
        print(
            (
                f"INFO: FILE `{file_name}` ALREADY EXISTS IN GCP AT "
                f"`{gcp_storage_bucket_location}`."
            )
        )
        return "exists", gcp_storage_bucket_location
    if not os.path.isfile(local_path):
        _log.error(
            "COULD NOT UPLOAD FILE %s TO GCP (%s): `%s` isn't a file on this "
            "computer.",
            file_name,
            gcs_uri,
            local_path,
        )
        return "failed", gcp_storage_bucket_location
    try:
        if verbose:
            print(f"UPLOADING FILE `{local_path}` TO `{gcs_uri}`...")
        # Upload to storage bucket.
        cloud_utils.upload_file_to_gcp_bucket(
            bucket=gcp_bucket,
            blob_file_path=gcp_storage_bucket_location,
            local_file_path=local_path,
            debug=debug,
        )
        if verbose:
            print("UPLOADED.")
        return "uploaded", gcp_storage_bucket_location
    except Exception as e:
        _log.error(
            "COULD NOT UPLOAD FILE %s TO GCP (%s) STORAGE BUCKET DUE TO "
            "THE FOLLOWING ERROR:\n%s",
            file_name,
            gcs_uri,
            e,
        )
        return "failed", gcp_storage_bucket_location


def upload_file_to_gcp_storage_bucket(
    file_name: str = "",
    file_type: str = "",
    ship_name: str = "",
    survey_name: str = "",
    echosounder: str = "",
    file_location: str = "",
    gcp_storage_bucket_location: str = "",
    gcp_bucket: storage.Client.bucket = None,
    data_source: str = "",
    user_email: str = "",
    is_survey_metadata: bool = False,
    is_calibration_file: bool = False,
    is_calibration_mapping_file: bool = False,
    is_auxiliary_file: bool = False,
    is_derived_product: bool = False,
    verbose: bool = True,
    debug: bool = False,
):
    """Safely uploads one local file to the storage bucket. Files already in
    the bucket aren't uploaded again. Upload errors are reported, not raised.

    Two ways to say where the file goes:

    * Give `gcp_storage_bucket_location`: the object path within the bucket,
      e.g. "HDD/Henry_B_Bigelow/DX2301/EK80/test/D20231017-T211129.idx", or a
      folder, e.g. "HDD/Henry_B_Bigelow/DX2301/EK80/test/" (the file name is
      added). Back slashes, a leading "gs://bucket/" or the bucket's name are
      all accepted. The names, file type and flags below are then not used.
    * Or leave it out, and the standard location is built from the names and
      flags (the one the directory uploaders and the download functions use).
      For echosounder files that needs `ship_name`, `survey_name`,
      `echosounder` and `data_source`.

    Args:
        file_name (str, optional): The file's name, e.g. "D20231017-T211129.idx"
            (a full path works too; only its name is used). Optional when
            `file_location` is the file's full path.
        file_type (str, optional): The file name suffix, e.g. "raw", "idx",
            "nc", without the dot ".". Optional: defaults to the suffix of the
            file name. Used only to build the standard location.
        ship_name (str): Required to build the standard location. The ship
            name, e.g. "Henry B. Bigelow" (normalized to "Henry_B_Bigelow").
        survey_name (str): Required to build the standard location. The
            survey name/identifier, e.g. "DX2301".
        echosounder (str): Required to build the standard location of
            echosounder and calibration files. The echosounder, e.g. "EK80".
        file_location (str): Required. Where the file is on your computer:
            its full path (e.g. r"C:/data/D20231017-T211129.idx"), or the
            folder it's in together with `file_name`.
        gcp_storage_bucket_location (str, optional): Where the file goes in
            the bucket (see above). Optional: defaults to the standard
            location.
        gcp_bucket (storage.Client.bucket, optional): The GCP bucket object
            used to upload the file. Defaults to None: the bucket aalibrary is
            configured for.
        data_source (str): Required to build the standard location. The
            source of the data: one of ["NCEI", "OMAO", "HDD", "TEST"].
        user_email (str, optional): The user email as a string, for derived
            products. Defaults to "": the signed-in gcloud account.
        is_survey_metadata (bool, optional): Whether or not the file is a
            metadata file associated with a survey. The files are stored at
            the survey level, in the `metadata/` folder. Defaults to False.
        is_calibration_file (bool, optional): Whether of not the file is a
            calibration file. These files are stored within their own
            `calibration/` sub-directory within the survey. Defaults to False.
        is_calibration_mapping_file (bool, optional): Whether or not a file is
            a calibration mapping file. This is stored in a separate directory
            called `mappings/` within the `calibration/` directory. Defaults
            to False.
        is_auxiliary_file (bool, optional): Whether or not the file is an
            auxiliary file associated with the survey. These files can be of
            any extension. And do not necessarily have to be data files.
            Defaults to False.
        is_derived_product (bool, optional): Whether or not the file is a
            derived product. These can be analysis work products or anything
            else that is not a file that analysis can be derived from (netcdf).
            Defaults to False.
        verbose (bool, optional): Whether or not you want to print intermediate
            statements about uploads. Helpful for if you are using progress
            bars.
            Defaults to True.
        debug (bool, optional): Whether or not to print debug statements.
            Defaults to False.

    Returns:
        str: The object path within the bucket when the file is there (just
            uploaded, or already there), None when it couldn't be uploaded.
    """
    status, gcp_storage_bucket_location = _upload_file(
        file_name=file_name,
        file_type=file_type,
        ship_name=ship_name,
        survey_name=survey_name,
        echosounder=echosounder,
        file_location=file_location,
        gcp_storage_bucket_location=gcp_storage_bucket_location,
        gcp_bucket=gcp_bucket,
        data_source=data_source,
        user_email=user_email,
        is_survey_metadata=is_survey_metadata,
        is_calibration_file=is_calibration_file,
        is_calibration_mapping_file=is_calibration_mapping_file,
        is_auxiliary_file=is_auxiliary_file,
        is_derived_product=is_derived_product,
        verbose=verbose,
        debug=debug,
    )
    return None if status == "failed" else gcp_storage_bucket_location


if __name__ == "__main__":
    config.use_gcp_dev()
    gcp_stor_client, gcp_bucket_name, gcp_bucket = (
        cloud_utils.setup_gcp_storage_objs(
            project_id="ggn-nmfs-aa-dev-1",
            gcp_bucket_name="ggn-nmfs-aa-dev-1-data",
        )
    )

    print(os.getcwd())

    upload_local_echosounder_files_from_directory_to_gcp_storage_bucket(
        local_echosounder_directory_to_upload=r"test_data_dir/Reuben_Lasker/RL2107/EK80",
        ship_name="Reuben_Lasker",
        survey_name="RL2107",
        echosounder="EK80",
        data_source="HDD",
        gcp_bucket=gcp_bucket,
        debug=True,
    )

    # upload_folder_as_is_to_gcp(
    #     local_folder_path="./test_data_dir/Reuben_Lasker/",
    #     gcp_bucket=gcp_bucket,
    #     destination_prefix="other/deletable/",
    # )
