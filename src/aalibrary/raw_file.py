"""This script contains the necessary classes RawFile, which is used to obtain
all of the attributes of a file."""

from collections import OrderedDict
import logging
import os
import pprint
from difflib import get_close_matches

from google.cloud import storage
import boto3

# For pytests-sake
if __package__ is None or __package__ == "":
    # uses current directory visibility
    import utils
    import config
    import ices_ship_names
else:
    # uses current package visibility
    from aalibrary import utils
    from aalibrary import config
    from aalibrary import ices_ship_names


class RawFile:
    """A class used to represent a raw file, from given parameters."""

    file_name: str = ""
    file_type: str = ""
    ship_name: str = ""
    survey_name: str = ""
    echosounder: str = ""
    data_source: str = ""
    file_download_directory: str = ""
    is_metadata: bool = False
    upload_to_gcp: bool = False
    debug: bool = False
    gcp_bucket: storage.Client.bucket = None
    s3_resource: boto3.resource = None
    # Get all valid and normalized ICES ship names
    valid_ICES_ship_names = ices_ship_names.get_all_ices_ship_names(
        normalize_ship_names=True
    )

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        self._create_vars_for_use_later()
        self._handle_paths()
        self._create_download_directories_if_not_exists()

        self._check_for_assertion_errors()

    def __repr__(self):
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

        # Get the parsed datetime of the file.
        # For NCEI - according to their format for naming files.
        if (self.data_source == "NCEI") or (self.data_source == "OMAO"):
            # ex. 2107RL_CW-D20211001-T132449.raw
            # TODO: `telegram` within raw file has a time stamp, maybe extract
            temp = self.file_name.lower().split("d")[-1].replace(".raw", "")
            self.year_str = temp[:4]
            self.month_str = temp[4:6]
            self.date_str = temp[6:8]
            self.year = int(self.year_str)
            self.month = int(self.month_str)
            self.date = int(self.date_str)
            temp = temp.split("t")[-1]
            self.hour_str = temp[:2]
            self.minute_str = temp[2:4]
            self.second_str = temp[4:]
            self.hour = int(self.hour_str)
            self.minute = int(self.minute_str)
            self.second = int(self.second_str)

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
        if "s3_resource" not in self.__dict__:
            self.s3_client, self.s3_resource, self.s3_bucket = (
                utils.cloud_utils.create_s3_objs()
            )

        # Create file names for all other files that can exist
        self.raw_file_name = self.file_name
        self.file_name_wo_extension = self.file_name.split(".")[0]
        self.idx_file_name = ".".join(self.file_name.split(".")[:-1]) + ".idx"
        self.bot_file_name = ".".join(self.file_name.split(".")[:-1]) + ".bot"
        self.netcdf_file_name = (
            ".".join(self.file_name.split(".")[:-1]) + ".nc"
        )

        # Create download paths for all four types of files
        self.raw_file_download_path = os.path.normpath(
            os.sep.join([self.file_download_directory, self.file_name])
        )
        self.idx_file_download_path = os.path.normpath(
            os.sep.join([self.file_download_directory, self.idx_file_name])
        )
        self.bot_file_download_path = os.path.normpath(
            os.sep.join([self.file_download_directory, self.bot_file_name])
        )
        self.netcdf_file_download_path = os.path.normpath(
            os.sep.join([self.file_download_directory, self.netcdf_file_name])
        )

        # Create all possible NCEI urls that can exist
        # We have to use the un-normalized version of the ship name since
        # NCEI does not normalize it.
        self.raw_file_ncei_url = utils.helpers.create_ncei_url_from_variables(
            file_name=self.raw_file_name,
            ship_name=self.ship_name_unnormalized,
            survey_name=self.survey_name,
            echosounder=self.echosounder,
        )
        self.idx_file_ncei_url = utils.helpers.create_ncei_url_from_variables(
            file_name=self.idx_file_name,
            ship_name=self.ship_name_unnormalized,
            survey_name=self.survey_name,
            echosounder=self.echosounder,
        )
        self.bot_file_ncei_url = utils.helpers.create_ncei_url_from_variables(
            file_name=self.bot_file_name,
            ship_name=self.ship_name_unnormalized,
            survey_name=self.survey_name,
            echosounder=self.echosounder,
        )
        # NCEI does not store netcdf files, so we will not be creating a url
        # for them.

        # Create all GCP Storage bucket locations for each possible file
        self.raw_gcp_storage_bucket_location = (
            utils.helpers.parse_correct_gcp_storage_bucket_location(
                file_name=self.raw_file_name,
                file_type="raw",
                ship_name=self.ship_name,
                survey_name=self.survey_name,
                echosounder=self.echosounder,
                data_source=self.data_source,
                is_metadata=self.is_metadata,
                debug=self.debug,
            )
        )
        self.idx_gcp_storage_bucket_location = (
            utils.helpers.parse_correct_gcp_storage_bucket_location(
                file_name=self.idx_file_name,
                file_type="idx",
                ship_name=self.ship_name,
                survey_name=self.survey_name,
                echosounder=self.echosounder,
                data_source=self.data_source,
                is_metadata=self.is_metadata,
                debug=self.debug,
            )
        )
        self.bot_gcp_storage_bucket_location = (
            utils.helpers.parse_correct_gcp_storage_bucket_location(
                file_name=self.bot_file_name,
                file_type="bot",
                ship_name=self.ship_name,
                survey_name=self.survey_name,
                echosounder=self.echosounder,
                data_source=self.data_source,
                is_metadata=self.is_metadata,
                debug=self.debug,
            )
        )
        self.netcdf_gcp_storage_bucket_location = (
            utils.helpers.parse_correct_gcp_storage_bucket_location(
                file_name=self.netcdf_file_name,
                file_type="netcdf",
                ship_name=self.ship_name,
                survey_name=self.survey_name,
                echosounder=self.echosounder,
                data_source=self.data_source,
                is_metadata=self.is_metadata,
                debug=self.debug,
            )
        )

        # Create all OMAO storage locations for each file.
        self.raw_omao_file_path = (
            utils.helpers.create_omao_file_path_from_variables(
                self.raw_file_name,
                file_type="raw",
                ship_name=self.ship_name,
                survey_name=self.survey_name,
                echosounder=self.echosounder,
            )
        )
        self.idx_omao_file_path = (
            utils.helpers.create_omao_file_path_from_variables(
                file_name=self.idx_file_name,
                file_type="idx",
                ship_name=self.ship_name,
                survey_name=self.survey_name,
                echosounder=self.echosounder,
            )
        )
        self.bot_omao_file_path = (
            utils.helpers.create_omao_file_path_from_variables(
                file_name=self.bot_file_name,
                file_type="bot",
                ship_name=self.ship_name,
                survey_name=self.survey_name,
                echosounder=self.echosounder,
            )
        )
        self.netcdf_omao_file_path = (
            utils.helpers.create_omao_file_path_from_variables(
                file_name=self.netcdf_file_name,
                file_type="netcdf",
                ship_name=self.ship_name,
                survey_name=self.survey_name,
                echosounder=self.echosounder,
            )
        )

        # Create object keys for NCEI
        self.raw_file_s3_object_key = utils.cloud_utils.get_object_key_for_s3(
            file_name=self.raw_file_name,
            file_type="raw",
            ship_name=self.ship_name_unnormalized,
            survey_name=self.survey_name,
            echosounder=self.echosounder,
        )
        self.idx_file_s3_object_key = utils.cloud_utils.get_object_key_for_s3(
            file_name=self.idx_file_name,
            file_type="idx",
            ship_name=self.ship_name_unnormalized,
            survey_name=self.survey_name,
            echosounder=self.echosounder,
        )
        self.bot_file_s3_object_key = utils.cloud_utils.get_object_key_for_s3(
            file_name=self.bot_file_name,
            file_type="bot",
            ship_name=self.ship_name_unnormalized,
            survey_name=self.survey_name,
            echosounder=self.echosounder,
        )
        # NCEI does not have netcdf files, so we will not create object keys.

        # Check if the file(s) exist in NCEI
        self.raw_file_exists_in_ncei = (
            utils.cloud_utils.check_if_file_exists_in_s3(
                object_key=self.raw_file_s3_object_key,
                s3_resource=self.s3_resource,
                s3_bucket_name=self.s3_bucket_name,
            )
        )
        self.idx_file_exists_in_ncei = (
            utils.cloud_utils.check_if_file_exists_in_s3(
                object_key=self.idx_file_s3_object_key,
                s3_resource=self.s3_resource,
                s3_bucket_name=self.s3_bucket_name,
            )
        )
        self.bot_file_exists_in_ncei = (
            utils.cloud_utils.check_if_file_exists_in_s3(
                object_key=self.bot_file_s3_object_key,
                s3_resource=self.s3_resource,
                s3_bucket_name=self.s3_bucket_name,
            )
        )
        # NCEI does not store netcdf files, so we will not be checking.

        # Check if the file(s) exist in GCP
        self.raw_file_exists_in_gcp = (
            utils.cloud_utils.check_if_file_exists_in_gcp(
                bucket=self.gcp_bucket,
                file_path=self.raw_gcp_storage_bucket_location,
            )
        )
        self.idx_file_exists_in_gcp = (
            utils.cloud_utils.check_if_file_exists_in_gcp(
                bucket=self.gcp_bucket,
                file_path=self.idx_gcp_storage_bucket_location,
            )
        )
        self.bot_file_exists_in_gcp = (
            utils.cloud_utils.check_if_file_exists_in_gcp(
                bucket=self.gcp_bucket,
                file_path=self.bot_gcp_storage_bucket_location,
            )
        )
        self.netcdf_file_exists_in_gcp = (
            utils.cloud_utils.check_if_file_exists_in_gcp(
                bucket=self.gcp_bucket,
                file_path=self.netcdf_gcp_storage_bucket_location,
            )
        )

        # TODO: create vars for omao data lake existence.

    def _check_for_assertion_errors(self):
        """Checks for errors in each variable in our self.__dict__."""

        if "file_name" in self.__dict__:
            assert self.file_name != "", (
                "Please provide a valid file name with the file extension"
                " (ex. `2107RL_CW-D20210813-T220732.raw`)"
            )
        if "file_type" in self.__dict__:
            assert self.file_type != "", "Please provide a valid file type."
            assert self.file_type in config.VALID_FILETYPES, (
                "Please provide a valid file type (extension) "
                f"from the following: {config.VALID_FILETYPES}"
            )
        if "ship_name" in self.__dict__:
            assert self.ship_name != "", (
                "Please provide a valid ship name "
                "(Title_Case_With_Underscores_As_Spaces)."
            )
            assert " " not in self.ship_name, (
                "Please provide a valid ship name "
                "(Title_Case_With_Underscores_As_Spaces)."
            )
            # Check for spell check using custom list
            spell_check_list = get_close_matches(
                self.ship_name, self.valid_ICES_ship_names, n=3, cutoff=0.6
            )
            if len(spell_check_list) > 0:
                assert self.ship_name in self.valid_ICES_ship_names, (
                    f"This `ship_name` {self.ship_name} does not"
                    " exist in the ICES database. Did you mean one of the"
                    f" following?\n{spell_check_list}"
                )
            else:
                assert self.ship_name in self.valid_ICES_ship_names, (
                    f"This `ship_name` {self.ship_name} does not"
                    " exist in the ICES database."
                )
        if "survey_name" in self.__dict__:
            assert (
                self.survey_name != ""
            ), "Please provide a valid survey name."
        if "echosounder" in self.__dict__:
            assert (
                self.echosounder != ""
            ), "Please provide a valid echosounder."
            assert self.echosounder in config.VALID_ECHOSOUNDERS, (
                "Please provide a valid echosounder from the "
                f"following: {config.VALID_ECHOSOUNDERS}"
            )
        if "data_source" in self.__dict__:
            assert self.data_source != "", (
                "Please provide a valid data source from the "
                f"following: {config.VALID_DATA_SOURCES}"
            )
            assert self.data_source in config.VALID_DATA_SOURCES, (
                "Please provide a valid data source from the "
                f"following: {config.VALID_DATA_SOURCES}"
            )
        if "file_download_directory" in self.__dict__:
            assert (
                self.file_download_directory != ""
            ), "Please provide a valid file download location (a directory)."
            assert os.path.isdir(self.file_download_directory), (
                f"File download location `{self.file_download_directory}` is"
                " not found to be a valid dir, please reformat it."
            )
        if "gcp_bucket" in self.__dict__:
            assert self.gcp_bucket is not None, (
                "Please provide a gcp_bucket object with"
                " `utils.cloud_utils.setup_gcp_storage()`"
            )
        if "directory" in self.__dict__:
            assert self.directory != "", "Please provide a valid directory."
            assert os.path.isdir(self.directory), (
                f"Directory location `{self.directory}` is not found to be a"
                " valid dir, please reformat it."
            )
        if "data_lake_directory_client" in self.__dict__:
            assert self.data_lake_directory_client is not None, (
                f"The data lake directory client cannot be a"
                f" {type(self.data_lake_directory_client)} object. It needs "
                "to be of the type `DataLakeDirectoryClient`."
            )

    # TODO:
    def _raw_file_exists_in_azure_data_lake(self): ...
    def _idx_file_exists_in_azure_data_lake(self): ...
    def _bot_file_exists_in_azure_data_lake(self): ...
    def _netcdf_file_exists_in_azure_data_lake(self): ...

    def get_str_times(self) -> dict:
        """Gets the parsed times of the current file in dict format

        Returns:
            dict: An OrderedDict containing all of the data collection times
                (based on name), for this file.
        """

        temp_dict = OrderedDict(
            [
                ("year", self.year_str),
                ("month", self.month_str),
                ("date", self.date_str),
                ("hour", self.hour_str),
                ("minute", self.minute_str),
                ("second", self.second_str),
            ]
        )
        return temp_dict

    def print_times(self) -> str:
        """Prints the parsed times of the current file in dict format.

        Returns:
            str: The pretty print version of a string of the current file's
                data collection datetime (based on file name).
        """

        temp_dict = OrderedDict(
            [
                ("year", self.year),
                ("month", self.month),
                ("date", self.date),
                ("hour", self.hour),
                ("minute", self.minute),
                ("second", self.second),
            ]
        )

        return pprint.pformat(temp_dict, indent=4)

    def get_file_datetime_str(self) -> str:
        """Gets the datetime as a datetime formatted string.
        Format: "%Y-%m-%d %H:%M:%S"

        Returns:
            str: The datetime formatted string.
        """

        datetime_str = (
            f"{self.year_str}-{self.month_str}-{self.date_str} "
            f"{self.hour_str}:{self.minute_str}:{self.second_str}"
        )
        return datetime_str


if __name__ == "__main__":
    s3_bucket_name = "noaa-wcsd-pds"
    try:
        s3_client, s3_resource, s3_bucket = utils.cloud_utils.create_s3_objs()
    except Exception as e:
        logging.error(f"CANNOT ESTABLISH CONNECTION TO S3 BUCKET..\n{e}")
        raise
    # Create gcp bucket objects
    gcp_stor_client, gcp_bucket_name, gcp_bucket = (
        utils.cloud_utils.setup_gcp_storage_objs()
    )
    rf = RawFile(
        file_name="2107RL_CW-D20210916-T165047.raw",
        file_type="raw",
        ship_name="Reuben_Lasker",
        survey_name="RL2107",
        echosounder="EK80",
        data_source="NCEI",
        file_download_directory="./test_data_dir",
        is_metadata=False,
        debug=True,
        s3_bucket=s3_bucket,
        s3_resource=s3_resource,
        s3_bucket_name=s3_bucket_name,
        gcp_bucket=gcp_bucket,
        gcp_bucket_name=gcp_bucket_name,
        gcp_stor_client=gcp_stor_client,
    )

    # print(rf)
    # print(rf.bot_file_download_path)
    # print(rf.print_times())
    # print(rf.get_str_times())
    # print(rf.get_file_datetime_str())
