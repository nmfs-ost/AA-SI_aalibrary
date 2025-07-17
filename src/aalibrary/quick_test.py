"""Contains quick tests for the API to verify that the connections are working
as intended. Also checks to see if a raw file can be downloaded."""

import sys
import os

from aalibrary.utils import cloud_utils
from aalibrary import ingestion, metadata


def start():
    # `gcloud` setup test
    try:
        print("`gcloud` SETUP TEST...", end="")
        metadata_json = metadata.create_metadata_json_for_raw_files()
        assert (
            metadata_json["UPLOADED_BY"] != ""
        ), "Please login to `gcloud` using `gcloud auth login --no-browser`"
        assert metadata_json["ECHOPYPE_VERSION"] != "", (
            "Please install requirements using `pip install -r src/aalibrary/"
            "requirements.txt`, or you can try reinstalling `aalibrary` to "
            "automatically take care of dependencies."
        )
        print(f"PASSED\n{metadata_json}")
    except Exception as e:
        print(
            f"`gcloud` SETUP TEST FAILED DUE TO THE FOLLOWING ERROR:\n{e}",
            file=sys.stderr,
        )

    # CONNECTION TEST: set up storage objects
    try:
        print("GCP CONNECTION TEST...", end="")
        _, _, gcp_bucket = cloud_utils.setup_gcp_storage_objs()
        print("PASSED")
    except Exception as e:
        print(
            f"CONNECTION TEST TO GCP FAILED DUE TO THE FOLLOWING ERROR:\n{e}",
            file=sys.stderr,
        )

    try:
        print("S3 CONNECTION TEST...", end="")
        s3_client, s3_resource, s3_bucket = cloud_utils.create_s3_objs()
        print("PASSED")
    except Exception as e:
        print(
            f"CONNECTION TEST TO s3 FAILED DUE TO THE FOLLOWING ERROR:\n{e}",
            file=sys.stderr,
        )

    # FUNCTIONAL TEST: download a raw file
    file_name = "2107RL_CW-D20210916-T165047.raw"
    file_type = "raw"
    ship_name = "Reuben_Lasker"
    survey_name = "RL2107"
    echosounder = "EK80"
    data_source = "TEST"
    file_download_directory = "."

    try:
        print("NCEI DOWNLOAD TEST...", end="")
        ingestion.download_raw_file(
            file_name=file_name,
            file_type=file_type,
            ship_name=ship_name,
            survey_name=survey_name,
            echosounder=echosounder,
            data_source=data_source,
            file_download_directory=file_download_directory,
            is_metadata=False,
            debug=False,
        )
        print("PASSED")
    except Exception as e:
        print(
            f"NCEI DOWNLOAD TEST FAILED DUE TO THE FOLLOWING ERROR:\n{e}",
            file=sys.stderr,
        )


def init_test_folder(folder_name: str = "test_data_dir"):
    """Creates a test folder in the current directory for quick tests and
    downloads test files.

    Args:
        folder_name (str, optional): The name of the folder you want to
            download test files into. Defaults to "test_data_dir".
    """

    test_folder_name = "test_data_dir"
    current_dir = os.getcwd()
    test_folder_directory = os.path.join(current_dir, test_folder_name)
    # Create test folder
    print(
        f"Creating test folder '{test_folder_name}' in"
        f" `{test_folder_directory}`..."
    )
    os.makedirs(f"{test_folder_directory}", exist_ok=True)
    print(
        f"Test folder '{test_folder_name}' created successfully in"
        f" `{test_folder_directory}`."
    )

    print("Downloading test files...")
    ingestion.download_raw_file_from_ncei(
        file_name="2107RL_FM-D20210804-T214458.raw",
        file_type="raw",
        ship_name="Reuben_Lasker",
        survey_name="RL2107",
        echosounder="EK80",
        data_source="TEST",
        file_download_directory=test_folder_directory,
        is_metadata=False,
        debug=False,
    )
    ingestion.download_raw_file_from_ncei(
        file_name="2107RL_FM-D20210808-T033245.raw",
        file_type="raw",
        ship_name="Reuben_Lasker",
        survey_name="RL2107",
        echosounder="EK80",
        data_source="TEST",
        file_download_directory=test_folder_directory,
        is_metadata=False,
        debug=False,
    )
    ingestion.download_raw_file_from_ncei(
        file_name="2107RL_FM-D20211012-T022341.raw",
        file_type="raw",
        ship_name="Reuben_Lasker",
        survey_name="RL2107",
        echosounder="EK80",
        data_source="TEST",
        file_download_directory=test_folder_directory,
        is_metadata=False,
        debug=False,
    )
    ingestion.download_raw_file_from_ncei(
        file_name="2107RL_CW-D20211001-T132449.raw",
        file_type="raw",
        ship_name="Reuben_Lasker",
        survey_name="RL2107",
        echosounder="EK80",
        data_source="TEST",
        file_download_directory=test_folder_directory,
        is_metadata=False,
        debug=False,
    )
    print("Test files downloaded successfully.")


if __name__ == "__main__":
    # start()
    init_test_folder()
