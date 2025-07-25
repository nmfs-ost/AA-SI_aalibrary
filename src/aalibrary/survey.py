"""This script contains the Survey class for managing survey data.
It contains useful functions related to survey data management, including a
Survey class that can be used to manage surveys."""

import sys
import logging
import os
import pprint

from google.cloud import storage
import boto3
from tqdm import tqdm

# For pytests-sake
if __package__ is None or __package__ == "":
    # uses current directory visibility
    import utils
    import ices_ship_names
    from utils import ncei_utils
    from raw_file import RawFile
else:
    # uses current package visibility
    from aalibrary import utils
    from aalibrary import ices_ship_names
    from aalibrary.utils import ncei_utils
    from aalibrary.raw_file import RawFile


class Survey:
    """A class used to represent a survey."""

    ship_name: str = ""
    survey_name: str = ""
    data_source: str = ""
    file_download_directory: str = ""
    upload_to_gcp: bool = False
    debug: bool = False
    gcp_bucket: storage.Client.bucket = None
    s3_resource: boto3.resource = None
    s3_client: boto3.client = None
    # Get all valid and normalized ICES ship names
    valid_ICES_ship_names = ices_ship_names.get_all_ices_ship_names(
        normalize_ship_names=True
    )

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        self._create_vars_for_use_later()
        self._handle_paths()
        self._create_download_directories_if_not_exists()

        # TODO: self._check_for_assertion_errors()

    def __repr__(self):
        """Return a string representation of the Survey object."""
        pprint.pprint(self.__dict__)

    def __str__(self):
        return pprint.pformat(self.__dict__, indent=4)

    def _handle_paths(self):
        """Handles all minute functions and adjustments related to paths."""

        # Normalize paths
        if "file_download_directory" in self.__dict__:
            self.file_download_directory = (
                os.path.normpath(self.file_download_directory) + os.sep
            )
            if self.debug:
                logging.debug("normalized", self.file_download_directory)

        # Convert locations into directories as needed.
        if (
            "file_download_directory" in self.__dict__
            and self.file_download_directory != "."
        ):
            # Edge-case: when dirname is passed ".", it responds with ""
            self.file_download_directory = (
                os.path.dirname(self.file_download_directory) + os.sep
            )
            if self.debug:
                logging.debug(
                    "converted to directory", self.file_download_directory
                )

    def _create_download_directories_if_not_exists(self):
        """Create the download directory (path) if it doesn't exist."""

        if "file_download_directory" in self.__dict__:
            if not os.path.exists(self.file_download_directory):
                os.makedirs(self.file_download_directory)

    def _create_vars_for_use_later(self):
        """Creates vars that will add value and can be utilized later."""

        # Creating RawFile objects for all raw files in this survey takes a lot
        # of time and memory, so we will implement a boolean to check if
        # we have created them or not.
        self._raw_file_objects_created = False

        # Normalize ship name
        if "ship_name" in self.__dict__:
            self.ship_name_unnormalized = self.ship_name
            self.ship_name = utils.helpers.normalize_ship_name(self.ship_name)
        # If the ship name exists in ICES, get the ICES code for it.
        if self.ship_name in self.valid_ICES_ship_names:
            self.ices_code = ices_ship_names.get_ices_code_from_ship_name(
                ship_name=self.ship_name, is_normalized=True
            )
        else:
            self.ices_code = ""

        # Take care of an empty file_download_directory and treat it like the
        # cwd.
        if (self.__dict__["file_download_directory"] == "") or (
            "file_download_directory" not in self.__dict__
        ):
            self.file_download_directory = "."

        # Create connection objects if they dont exist
        self.s3_bucket_name = "noaa-wcsd-pds"
        if "gcp_bucket" not in self.__dict__:
            self.gcp_stor_client, self.gcp_bucket_name, self.gcp_bucket = (
                utils.cloud_utils.setup_gcp_storage_objs()
            )
        if ("s3_resource" not in self.__dict__) or (
            "s3_client" not in self.__dict__
        ):
            self.s3_client, self.s3_resource, self.s3_bucket = (
                utils.cloud_utils.create_s3_objs()
            )

        # Get all echosounders in this survey.
        if self.data_source == "NCEI":
            self.echosounders = ncei_utils.get_all_echosounders_in_a_survey(
                ship_name=self.ship_name,
                survey_name=self.survey_name,
                s3_client=self.s3_client,
                return_full_paths=False,
            )

        # Get all files that exist in the survey.
        if self.data_source == "NCEI":
            self.all_files_paths = ncei_utils.get_all_file_names_from_survey(
                ship_name=self.ship_name,
                survey_name=self.survey_name,
                s3_resource=self.s3_resource,
                return_full_paths=True,
            )
            self.all_files = [
                file.split("/")[-1] for file in self.all_files_paths
            ]

        # Get all raw files in this survey
        if self.data_source == "NCEI":
            self.raw_files_paths = [
                file for file in self.all_files_paths if file.endswith(".raw")
            ]
            self.raw_files = [
                file.split("/")[-1] for file in self.raw_files_paths
            ]

        # TODO: Get all metadata files in this survey.

        # TODO:

    def _check_for_assertion_errors(self):
        """Check for assertion errors in the survey object."""
        ...

    def create_raw_file_objects(self):
        """Create RawFile objects for all raw files in this survey."""
        if self.data_source == "NCEI":
            if not self._raw_file_objects_created:
                self.raw_file_objects = []
                for raw_file in tqdm(self.raw_files):
                    # Get the echosounder for this raw file
                    echosounder = ncei_utils.get_echosounder_from_raw_file(
                        file_name=raw_file,
                        ship_name=self.ship_name,
                        survey_name=self.survey_name,
                        echosounders=self.echosounders,
                        s3_client=self.s3_client,
                        s3_resource=self.s3_resource,
                    )
                    raw_file_obj = RawFile(
                        file_name=raw_file,
                        file_type="raw",
                        ship_name=self.ship_name,
                        survey_name=self.survey_name,
                        echosounder=echosounder,
                        data_source=self.data_source,
                        file_download_directory=self.file_download_directory,
                        is_metadata=False,
                        debug=self.debug,
                        s3_bucket=self.s3_bucket,
                        s3_resource=self.s3_resource,
                        s3_bucket_name=self.s3_bucket_name,
                        gcp_bucket=self.gcp_bucket,
                        gcp_bucket_name=self.gcp_bucket_name,
                        gcp_stor_client=self.gcp_stor_client,
                    )
                    self.raw_file_objects.append(raw_file_obj)
                self._raw_file_objects_created = True


if __name__ == "__main__":
    # set logging config
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    # set up storage objects
    s3_client, s3_resource, s3_bucket = utils.cloud_utils.create_s3_objs()
    gcp_stor_client, gcp_bucket_name, gcp_bucket = (
        utils.cloud_utils.setup_gcp_storage_objs()
    )

    # create a survey object
    survey = Survey(
        ship_name="Reuben_Lasker",
        survey_name="RL2107",
        data_source="NCEI",
        file_download_directory="./data/",
        upload_to_gcp=False,
        debug=True,
        gcp_bucket=gcp_bucket,
        s3_resource=s3_resource,
    )

    survey.create_raw_file_objects()
    print(survey.raw_files)