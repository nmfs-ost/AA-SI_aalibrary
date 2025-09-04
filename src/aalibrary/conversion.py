"""This file is used to store conversion functions for the AALibrary."""

import os
import logging

from google.cloud import storage

from echopype import open_raw

# For pytests-sake
if __package__ is None or __package__ == "":
    # uses current directory visibility
    from ingestion import (
        download_netcdf_file,
        download_raw_file,
        upload_file_to_gcp_storage_bucket,
    )
    import utils
    import metadata
    from raw_file import RawFile
    from utils.sonar_checker import sonar_checker
else:
    # uses current package visibility
    from aalibrary.ingestion import (
        download_netcdf_file,
        download_raw_file,
        upload_file_to_gcp_storage_bucket,
    )
    from aalibrary import utils
    from aalibrary import metadata
    from aalibrary.raw_file import RawFile
    from aalibrary.utils.sonar_checker import sonar_checker


def convert_local_raw_to_netcdf(
    raw_file_location: str = "",
    netcdf_file_download_directory: str = "",
    echosounder: str = "",
    overwrite: bool = False,
    delete_raw_after: bool = False,
):
    """ENTRYPOINT FOR END-USERS
    Converts a local (on your computer) file from raw into netcdf using
    echopype.

    Args:
        raw_file_location (str, optional): The location of the raw file.
            Defaults to "".
        netcdf_file_download_directory (str, optional): The location you want
            to download your netcdf file to. Defaults to "".
        echosounder (str, optional): The echosounder used. Can be one of
            ["EK80", "EK70"]. Defaults to "".
        overwrite (bool, optional): Whether or not to overwrite the netcdf
            file. Defaults to False.
        delete_raw_after (bool, optional): Whether or not to delete the raw
            file after conversion is complete. Defaults to False.
    """

    netcdf_file_download_directory = os.sep.join(
        [os.path.normpath(netcdf_file_download_directory)]
    )
    print(f"netcdf_file_download_directory {netcdf_file_download_directory}")

    # Create the download directory (path) if it doesn't exist
    if not os.path.exists(netcdf_file_download_directory):
        os.makedirs(netcdf_file_download_directory)

    # Make sure the echosounder specified matches the raw file data.
    if echosounder.lower() == "ek80":
        assert sonar_checker.is_EK80(
            raw_file=raw_file_location, storage_options={}
        ), (
            f"THE ECHOSOUNDER SPECIFIED `{echosounder}` DOES NOT MATCH THE "
            "ECHOSOUNDER FOUND WITHIN THE RAW FILE."
        )
    elif echosounder.lower() == "ek60":
        assert sonar_checker.is_EK60(
            raw_file=raw_file_location, storage_options={}
        ), (
            f"THE ECHOSOUNDER SPECIFIED `{echosounder}` DOES NOT MATCH THE "
            "ECHOSOUNDER FOUND WITHIN THE RAW FILE."
        )
    elif echosounder.lower() == "azfp6":
        assert sonar_checker.is_AZFP6(
            raw_file=raw_file_location, storage_options={}
        ), (
            f"THE ECHOSOUNDER SPECIFIED `{echosounder}` DOES NOT MATCH THE "
            "ECHOSOUNDER FOUND WITHIN THE RAW FILE."
        )
    elif echosounder.lower() == "azfp":
        assert sonar_checker.is_AZFP(
            raw_file=raw_file_location, storage_options={}
        ), (
            f"THE ECHOSOUNDER SPECIFIED `{echosounder}` DOES NOT MATCH THE "
            "ECHOSOUNDER FOUND WITHIN THE RAW FILE."
        )
    elif echosounder.lower() == "ad2cp":
        assert sonar_checker.is_AD2CP(
            raw_file=raw_file_location, storage_options={}
        ), (
            f"THE ECHOSOUNDER SPECIFIED `{echosounder}` DOES NOT MATCH THE "
            "ECHOSOUNDER FOUND WITHIN THE RAW FILE."
        )
    elif echosounder.lower() == "er60":
        assert sonar_checker.is_ER60(
            raw_file=raw_file_location, storage_options={}
        ), (
            f"THE ECHOSOUNDER SPECIFIED `{echosounder}` DOES NOT MATCH THE "
            "ECHOSOUNDER FOUND WITHIN THE RAW FILE."
        )

    try:
        print("CONVERTING RAW TO NETCDF...")
        raw_file_echopype = open_raw(
            raw_file=raw_file_location, sonar_model=echosounder
        )
        raw_file_echopype.to_netcdf(
            save_path=netcdf_file_download_directory, overwrite=overwrite
        )
        print("CONVERTED.")
        if delete_raw_after:
            try:
                print("DELETING RAW FILE...")
                os.remove(raw_file_location)
                print("DELETED.")
            except Exception as e:
                print(e)
                print(
                    "THE RAW FILE COULD NOT BE DELETED DUE TO THE ERROR ABOVE."
                )
    except Exception as e:
        logging.error(
            "COULD NOT CONVERT `%s` DUE TO ERROR %s", raw_file_location, e
        )
        raise e


def convert_raw_to_netcdf(
    file_name: str = "",
    file_type: str = "raw",
    ship_name: str = "",
    survey_name: str = "",
    echosounder: str = "",
    data_source: str = "",
    file_download_directory: str = "",
    overwrite: bool = False,
    delete_raw_after: bool = False,
    gcp_bucket: storage.Client.bucket = None,
    is_metadata: bool = False,
    debug: bool = False,
):
    """ENTRYPOINT FOR END-USERS
    This function allows one to convert a file from raw to netcdf. Then uploads
    the file to GCP storage for caching.

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
        file_download_directory (str, optional): The local directory you want
            to store your file in. Defaults to "".
        overwrite (bool, optional): Whether or not to overwrite the netcdf
            file. Defaults to False.
        delete_raw_after (bool, optional): Whether or not to delete the raw
            file after conversion is complete. Defaults to False.
        gcp_bucket (storage.Client.bucket, optional): The GCP bucket object
            used to download the file. Defaults to None.
        is_metadata (bool, optional): Whether or not the file is a metadata
            file. Necessary since files that are considered metadata (metadata
            json, or readmes) are stored in a separate directory. Defaults to
            False.
        debug (bool, optional): Whether or not to print debug statements.
            Defaults to False.
    """
    # TODO: Implement an 'upload' param default to True.

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
        overwrite=overwrite,
        gcp_bucket=gcp_bucket,
        is_metadata=is_metadata,
        debug=debug,
        s3_resource=s3_resource,
        s3_bucket_name="noaa-wcsd-pds",
    )

    # Here we check for a netcdf version of the raw file on GCP
    print("CHECKING FOR NETCDF VERSION ON GCP...")
    if rf.netcdf_file_exists_in_gcp:
        # Inform the user if a netcdf version exists in cache.
        download_netcdf_file(
            raw_file_name=rf.raw_file_name,
            file_type="netcdf",
            ship_name=rf.ship_name,
            survey_name=rf.survey_name,
            echosounder=rf.echosounder,
            data_source=rf.data_source,
            file_download_directory=rf.file_download_directory,
            gcp_bucket=gcp_bucket,
            debug=rf.debug,
        )
    else:
        logging.info(
            "FILE `%s` DOES NOT EXIST AS NETCDF. DOWNLOADING/CONVERTING/"
            "UPLOADING RAW...",
            rf.raw_file_name,
        )

        # Download the raw file.
        # This function should take care of checking whether the raw file
        # exists in any of the data sources, and fetching it.
        download_raw_file(
            file_name=rf.file_name,
            file_type=rf.file_type,
            ship_name=rf.ship_name,
            survey_name=rf.survey_name,
            echosounder=rf.echosounder,
            data_source=rf.data_source,
            file_download_directory=rf.file_download_directory,
            debug=rf.debug,
        )

        # Convert the raw file to netcdf.
        convert_local_raw_to_netcdf(
            raw_file_location=rf.raw_file_download_path,
            netcdf_file_download_directory=rf.file_download_directory,
            echosounder=rf.echosounder,
            overwrite=overwrite,
            delete_raw_after=delete_raw_after,
        )

        # Upload the netcdf to the correct location for parsing.
        upload_file_to_gcp_storage_bucket(
            file_name=rf.netcdf_file_name,
            file_type="netcdf",
            ship_name=rf.ship_name,
            survey_name=rf.survey_name,
            echosounder=rf.echosounder,
            file_location=rf.netcdf_file_download_path,
            gcp_bucket=gcp_bucket,
            data_source=rf.data_source,
            is_metadata=False,
            debug=rf.debug,
        )
        # Upload the metadata file associated with this
        # TODO: implement metadata generation here.
        # metadata.create_and_upload_metadata_df_for_netcdf(
        #     file_name=rf.netcdf_file_name,
        #     file_type="netcdf",
        #     ship_name=rf.ship_name,
        #     survey_name=rf.survey_name,
        #     echosounder=rf.echosounder,
        #     data_source=rf.data_source,
        #     gcp_bucket=gcp_bucket,
        #     netcdf_local_file_location=rf.netcdf_file_download_path,
        #     debug=debug,
        # )


if __name__ == "__main__":
    # set up storage objects
    s3_client, s3_resource, s3_bucket = utils.cloud_utils.create_s3_objs()
    gcp_stor_client, gcp_bucket_name, gcp_bucket = (
        utils.cloud_utils.setup_gcp_storage_objs()
    )
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
    convert_raw_to_netcdf(
        file_name="2107RL_CW-D20210916-T165047.raw",
        file_type="raw",
        ship_name="Reuben_Lasker",
        survey_name="RL2107",
        echosounder="EK80",
        data_source="NCEI",
        file_download_directory="./",
        gcp_bucket=gcp_bucket,
        is_metadata=False,
        debug=True,
    )
