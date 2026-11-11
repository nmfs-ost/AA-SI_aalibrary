"""This script contains the Survey class for managing survey data.
It contains useful functions related to survey data management, including a
Survey class that can be used to manage surveys."""

# pylint: disable=attribute-defined-outside-init

import logging
import os
import pprint
from glob import glob
from pathlib import Path
from datetime import datetime, timedelta
from typing import List

from google.cloud import storage
import boto3
from tqdm import tqdm
from tqdm.contrib.concurrent import thread_map, process_map

# For pytests-sake
if __package__ is None or __package__ == "":
    # uses current directory visibility
    import ices_ship_names
    from utils import ncei_cache_utils
    from utils.cloud_utils import (
        setup_gcp_storage_objs,
        create_s3_objs,
        check_if_file_exists_in_gcp,
        upload_file_to_gcp_bucket,
    )
    from utils.helpers import (
        normalize_ship_name,
        parse_correct_gcp_storage_bucket_location_based_on_file_type,
        _get_single_local_file_crc32c_checksum_multi_process,
    )
    from utils.gcp_utils import get_gcp_file_checksum
    from raw_file import RawFile
    from config import VALID_ECHOSOUNDERS

else:
    # uses current package visibility
    from aalibrary import ices_ship_names
    from aalibrary.utils import ncei_cache_utils
    from aalibrary.utils.cloud_utils import (
        setup_gcp_storage_objs,
        create_s3_objs,
        check_if_file_exists_in_gcp,
        upload_file_to_gcp_bucket,
    )
    from aalibrary.utils.helpers import (
        normalize_ship_name,
        parse_correct_gcp_storage_bucket_location_based_on_file_type,
        _get_single_local_file_crc32c_checksum_multi_process,
    )
    from aalibrary.utils.gcp_utils import get_gcp_file_checksum
    from aalibrary.raw_file import RawFile
    from aalibrary.config import VALID_ECHOSOUNDERS


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
    # All valid and normalized ICES ship names, fetched on first use.
    valid_ICES_ship_names = ices_ship_names.NormalizedIcesShipNames()

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        self._create_vars_for_use_later()
        self._handle_paths()
        self._create_download_directories_if_not_exists()

        # TODO: self._check_for_assertion_errors()

    def __repr__(self):
        """Return a string representation of the Survey object."""
        return pprint.pformat(self.__dict__, indent=4)

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
                logging.debug(
                    "normalized file download directory = %s",
                    self.file_download_directory,
                )

        # Take care of an empty file_download_directory and treat it like the
        # cwd.
        if (self.__dict__["file_download_directory"] == "") or (
            "file_download_directory" not in self.__dict__
        ):
            self.file_download_directory = "."
            if self.debug:
                logging.debug(
                    "converted file_download_directory to directory %s",
                    self.file_download_directory,
                )

    def _create_download_directories_if_not_exists(self):
        """Create the download directory (path) if it doesn't exist."""

        if "file_download_directory" in self.__dict__:
            if not os.path.exists(self.file_download_directory):
                os.makedirs(self.file_download_directory)

    def _create_vars_for_use_later(self):
        """Creates vars that will add value and can be utilized later."""

        # Normalize ship name
        if "ship_name" in self.__dict__:
            self.ship_name_unnormalized = self.ship_name
            self.ship_name = normalize_ship_name(self.ship_name)
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
        if (
            (("gcp_bucket" not in self.__dict__) or (self.gcp_bucket is None))
            or ("gcp_bucket_name" not in self.__dict__)
            or ("gcp_stor_client" not in self.__dict__)
        ):
            self.gcp_stor_client, self.gcp_bucket_name, self.gcp_bucket = (
                setup_gcp_storage_objs()
            )
        if (
            ("s3_resource" not in self.__dict__)
            or ("s3_client" not in self.__dict__)
            or ("s3_bucket" not in self.__dict__)
        ):
            self.s3_client, self.s3_resource, self.s3_bucket = (
                create_s3_objs()
            )

        # Creating RawFile objects for all raw files in this survey takes a lot
        # of time and memory, so we will implement a boolean to check if
        # we have created them or not.
        self.raw_file_objects = []
        self._raw_file_objects_created = False

        # Get all echosounders in this survey.
        if self.data_source == "NCEI":
            self.echosounders = (
                ncei_cache_utils.get_all_echosounders_in_a_survey(
                    ship_name=self.ship_name,
                    survey_name=self.survey_name,
                    s3_client=self.s3_client,
                    return_full_paths=False,
                )
            )
        else:
            self.echosounders = None

        # Get all files that exist in the survey.
        if self.data_source == "NCEI":
            self.all_files_paths = (
                ncei_cache_utils.get_all_file_names_from_survey(
                    ship_name=self.ship_name,
                    survey_name=self.survey_name,
                    s3_resource=self.s3_resource,
                    return_full_paths=True,
                )
            )
            self.all_files = [
                file.split("/")[-1] for file in self.all_files_paths
            ]
        else:
            self.all_files_paths = None
            self.all_files = None

        # Get all raw files in this survey
        if self.data_source == "NCEI":
            self.raw_files_paths = [
                file for file in self.all_files_paths if file.endswith(".raw")
            ]
            self.raw_files = [
                file.split("/")[-1] for file in self.raw_files_paths
            ]
        else:
            self.raw_files_paths = None
            self.raw_files = None

        # Get all idx files in this survey
        if self.data_source == "NCEI":
            self.idx_files_paths = [
                file for file in self.all_files_paths if file.endswith(".idx")
            ]
            self.idx_files = [
                file.split("/")[-1] for file in self.idx_files_paths
            ]
        else:
            self.idx_files_paths = None
            self.idx_files = None

        # Get all bot files in this survey
        if self.data_source == "NCEI":
            self.bot_files_paths = [
                file for file in self.all_files_paths if file.endswith(".bot")
            ]
            self.bot_files = [
                file.split("/")[-1] for file in self.bot_files_paths
            ]
        else:
            self.bot_files_paths = None
            self.bot_files = None

        # Get all netcdf files in this survey
        if self.data_source == "NCEI":
            self.netcdf_files_paths = [
                file for file in self.all_files_paths if file.endswith(".nc")
            ]
            self.netcdf_files = [
                file.split("/")[-1] for file in self.netcdf_files_paths
            ]
        else:
            self.netcdf_files_paths = None
            self.netcdf_files = None

        # Get all metadata files in this survey.
        if self.data_source == "NCEI":
            self.metadata_files_paths = [
                file
                for file in self.all_files_paths
                if file.contains("/metadata/")
            ]
            self.metadata_files = [
                file.split("/")[-1] for file in self.metadata_files_paths
            ]
        else:
            self.metadata_files = None

        # Get all calibration files in this survey.
        if self.data_source == "NCEI":
            self.calibration_files_paths = [
                file
                for file in self.all_files_paths
                if file.contains("calibration")
            ]
            self.calibration_files = [
                file.split("/")[-1] for file in self.calibration_files_paths
            ]
        else:
            self.calibration_files = None

        # Get all auxiliary files in this survey.
        if self.data_source == "NCEI":
            self.auxiliary_files_paths = [
                file
                for file in self.all_files_paths
                if file.contains("auxiliary")
            ]
            self.auxiliary_files = [
                file.split("/")[-1] for file in self.auxiliary_files_paths
            ]
        else:
            self.auxiliary_files = None

    def _check_for_assertion_errors(self):
        """Check for assertion errors in the survey object."""
        # TODO: Implement this function to check for assertion errors

    def create_raw_file_objects(self):
        """Create RawFile objects for all raw files in this survey."""
        if self.data_source == "NCEI":
            if not self._raw_file_objects_created:
                self.raw_file_objects = []
                for raw_file in tqdm(
                    self.raw_files, desc="Creating RawFile Objects"
                ):
                    # Get the echosounder for this raw file
                    echosounder = (
                        ncei_cache_utils.get_echosounder_from_raw_file(
                            file_name=raw_file,
                            ship_name=self.ship_name,
                            survey_name=self.survey_name,
                            gcp_bq_client=self.gcp_bq_client,
                        )
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


class LocalSurvey:
    """A survey's files on your computer. Making one analyses a folder (and
    its subfolders): every file's size, type, echosounder and its standard
    location in the GCP storage bucket. You can then upload the files there,
    check the uploads against the bucket by checksum, or move the files into
    the standard folder layout locally.

    It uploads files only: no BigQuery metadata is recorded, and the ship name
    isn't checked against the ICES ship list (so a ship or platform without an
    ICES code yet works).

    Example:
        ```python
        from aalibrary.survey import LocalSurvey

        survey = LocalSurvey(
            directory_path=r"C:/data/DX2301",
            ship_name="Henry B. Bigelow",
            survey_name="DX2301",
            echosounder="EK80",
            data_source="HDD",
        )
        print(survey)                 # what was found
        survey.upload_files()         # upload everything (skips files
                                      # already in the bucket)
        survey.verify_gcp_uploads()   # compare checksums with the bucket
        ```

    Args:
        directory_path (str): Required. The folder that holds the survey's
            files. Subfolders are included.
        ship_name (str): Required. The ship name, e.g. "Henry B. Bigelow"
            (normalized to "Henry_B_Bigelow").
        survey_name (str): Required. The survey name/identifier.
        echosounder (str, optional): The echosounder, e.g. "EK80". Defaults to
            "": each file's echosounder is taken from the name of a folder
            above it (e.g. ".../EK80/file.raw"). Give it when the folders
            aren't named after the echosounder; otherwise those files can't be
            uploaded.
        data_source (str, optional): One of ["NCEI", "OMAO", "HDD", "TEST"].
            Defaults to "HDD" (files on a local drive).
        relocation_path (str, optional): A local folder; when given, each
            file's place in the standard layout under it is worked out, and
            `relocate()` moves the files there. Defaults to "".
        gcp_bucket (storage.Client.bucket, optional): The bucket to upload to.
            Defaults to None: the bucket aalibrary is configured for.
        gcp_project_id (str, optional): The GCP project. Defaults to the one
            aalibrary is configured for.
        gcp_bucket_name (str, optional): The bucket's name. Defaults to the
            one aalibrary is configured for.
        debug (bool, optional): Whether or not to print debug statements.
            Defaults to False.

    Attributes:
        all_file_paths_in_directory (dict): For each file path: its "type"
            (suffix), "size" (bytes), "echosounder" and
            "gcp_storage_bucket_location" (and "relocation_path").
        num_all_files_in_directory (int), num_raw_files_in_directory (int),
        num_idx_files_in_directory (int), num_bot_files_in_directory (int):
            File counts.
        total_file_size_gb_in_directory (float): Total size.
    """

    ship_name: str = ""
    survey_name: str = ""
    echosounder: str = ""
    data_source: str = "HDD"
    directory_path: str = ""
    auxiliary_directory_path: str = ""
    relocation_path: str = ""
    debug: bool = False
    gcp_bucket: storage.Client.bucket = None
    gcp_project_id: str = None
    gcp_bucket_name: str = None
    upload_to_gcp: bool = None
    missing_or_incorrectly_uploaded_files: set = set()
    # Get all valid and normalized ICES ship names
    valid_ICES_ship_names = ices_ship_names.get_all_ices_ship_names(
        normalize_ship_names=True
    )

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        self._handle_paths()
        print(f"ANALYZING DIRECTORY: {self.directory_path}")
        self._create_vars_for_use_later()
        if self.upload_to_gcp:
            print("UPLOAD TO GCP ENABLED")
            self._upload_to_gcp()

    def __repr__(self):
        print_dict = {
            "ship_name": self.ship_name,
            "survey_name": self.survey_name,
            "num_all_files_in_directory": self.num_all_files_in_directory,
            "total_file_size_bytes_in_directory": (
                self.total_file_size_bytes_in_directory
            ),
            "total_file_size_mb_in_directory": round(
                self.total_file_size_mb_in_directory, 2
            ),
            "total_file_size_gb_in_directory": round(
                self.total_file_size_gb_in_directory, 2
            ),
            "all_file_types_in_directory": self.all_file_types_in_directory,
            "num_raw_files_in_directory": self.num_raw_files_in_directory,
            "num_idx_files_in_directory": self.num_idx_files_in_directory,
            "num_bot_files_in_directory": self.num_bot_files_in_directory,
        }
        return pprint.pformat(print_dict, indent=4)

    def _handle_paths(self):
        """Handles all minute functions and adjustments related to paths."""

        assert (
            "directory_path" in self.__dict__
        ), "`directory_path` is required for the LocalSurvey object."

        # Normalize paths
        if "directory_path" in self.__dict__:
            self.directory_path = (
                os.path.normpath(self.directory_path) + os.sep
            )
            assert os.path.exists(self.directory_path), (
                "The directory provided does not exist or could not be found:"
                f" `{self.directory_path}`"
            )
            if self.debug:
                logging.debug(
                    "normalized directory path = %s", self.directory_path
                )
        assert "auxiliary_directory_path" in self.__dict__, (
            "`auxiliary_directory_path` is required for the LocalSurvey object"
            ". If necessary, please create an empty `auxiliary` directory in "
            "the survey directory and pass it as a parameter."
        )

        if "auxiliary_directory_path" in self.__dict__:
            self.auxiliary_directory_path = (
                os.path.normpath(self.auxiliary_directory_path) + os.sep
            )
            if self.debug:
                logging.debug(
                    "normalized auxiliary directory path = %s",
                    self.auxiliary_directory_path,
                )
        if "relocation_path" in self.__dict__:
            self.relocation_path = (
                os.path.normpath(self.relocation_path) + os.sep
            )
            if self.debug:
                logging.debug(
                    "normalized relocation path = %s", self.relocation_path
                )
            # Create the relocation path if it does not exist.
            if not os.path.exists(self.relocation_path):
                os.makedirs(self.relocation_path)

    def _create_vars_for_use_later(self):
        """Creates vars for use later."""

        # Normalize ship name
        if "ship_name" in self.__dict__:
            self.ship_name_unnormalized = self.ship_name
            self.ship_name = normalize_ship_name(self.ship_name)
        # If the ship name exists in ICES, get the ICES code for it.
        if self.ship_name in self.valid_ICES_ship_names:
            self.ices_code = ices_ship_names.get_ices_code_from_ship_name(
                ship_name=self.ship_name, is_normalized=True
            )
        else:
            self.ices_code = ""

        # Handle undefined GCP project id and bucket name by using environment
        # variables.
        if self.gcp_project_id is None:
            self.gcp_project_id = os.getenv("AALIBRARY_GCP_PROJECT_ID")
        if self.gcp_bucket_name is None:
            self.gcp_bucket_name = os.getenv("AALIBRARY_GCP_BUCKET_NAME")
        if self.gcp_bucket is None:
            _, _, self.gcp_bucket = setup_gcp_storage_objs(
                project_id=self.gcp_project_id,
                gcp_bucket_name=self.gcp_bucket_name,
                verbose=False,
            )

        # Get all files in this directory.
        directory_path_glob = os.sep.join([self.directory_path, "**", "*"])
        # Get all files in auxiliary directory
        auxiliary_directory_path_glob = os.sep.join(
            [self.auxiliary_directory_path, "**", "*"]
        )
        # EX. self.all_file_paths_in_directory = {
        # 'C:\\Users\\Hannan\\AmSam2HawaiiSE2602L3\\D20260717-T000432.raw.evi':
        # {'echosounder': None,
        # 'gcp_storage_bucket_location':
        #   'HDD/Sette/SE2602/None/data/raw/D20260717-T000432.raw.evi',
        # 'size': 500698,
        # 'type': '.evi'}, ...
        # }
        self.all_file_paths_in_directory = glob(
            directory_path_glob, recursive=True
        )
        self.all_auxiliary_file_paths = glob(
            auxiliary_directory_path_glob, recursive=True
        )
        self.all_file_paths_in_directory = {
            file_path: {"is_auxiliary": False}
            for file_path in self.all_file_paths_in_directory
        }
        # Check for and update auxiliary file paths with a boolean in
        # all_file_paths_in_directory.
        for file_path in self.all_auxiliary_file_paths:
            self.all_file_paths_in_directory[file_path] = {
                "is_auxiliary": True
            }
        # Remove directories from the list of files and assign attributes to
        # each file.
        # List to get rid of deleting while iterating error.
        # Pbar to keep track of directory analysis.
        self.init_pbar_total_steps = 4
        if self.relocation_path != "":
            self.init_pbar_total_steps += 1
        self.init_pbar = tqdm(
            total=self.init_pbar_total_steps,
            desc="Analyzing Directory",
            position=0,
            leave=True,
        )
        for file_path in tqdm(
            list(self.all_file_paths_in_directory),
            desc="Getting File Metadata",
            position=1,
            leave=False,
        ):
            p: Path = Path(file_path)
            # Remove it from the count if it is a directory.
            if p.is_dir():
                del self.all_file_paths_in_directory[file_path]
                continue
            # Assign attributes such as file type.
            self.all_file_paths_in_directory[file_path]["type"] = p.suffix
            # Assign size (bytes).
            self.all_file_paths_in_directory[file_path][
                "size"
            ] = p.stat().st_size
            if self.echosounder == "":
                # Assign echosounder used based on folder name.
                # Get parent folders
                parent_dirs = [
                    parent.name
                    for parent in p.parents
                    if parent.name and parent.name in VALID_ECHOSOUNDERS
                ]
                echosounder = parent_dirs[0] if parent_dirs else None
            else:
                echosounder = self.echosounder
            self.all_file_paths_in_directory[file_path][
                "echosounder"
            ] = echosounder
        self.init_pbar.update(1)
        # Keep track of all files sorted by ascending size (list of keys for
        # self.all_file_paths_in_directory)
        self.all_files_sorted_by_size = [
            k
            for k, v in sorted(
                self.all_file_paths_in_directory.items(),
                key=lambda item: item[1]["size"],
            )
        ]

        # Get number of all files in this directory.
        self.num_all_files_in_directory = len(self.all_file_paths_in_directory)
        # Get all file types in this directory.
        self.all_file_types_in_directory = list(
            set(
                [
                    self.all_file_paths_in_directory[file_path_dict]["type"]
                    for file_path_dict in self.all_file_paths_in_directory
                ]
            )
        )
        # Get file sizes in bytes.
        for file_path in tqdm(
            self.all_file_paths_in_directory,
            desc="Getting File Sizes",
            position=1,
            leave=False,
        ):
            self.all_file_paths_in_directory[file_path]["size"] = (
                os.path.getsize(file_path)
            )
        self.init_pbar.update(1)
        # Get total file size of all files in bytes in this directory.
        self.total_file_size_bytes_in_directory = sum(
            file_info["size"]
            for file_info in self.all_file_paths_in_directory.values()
        )
        self.total_file_size_mb_in_directory = (
            self.total_file_size_bytes_in_directory / (1024**2)
        )
        self.total_file_size_gb_in_directory = (
            self.total_file_size_bytes_in_directory / (1024**3)
        )

        self.init_pbar.set_description("Parsing Raw Files Metadata")
        self._parse_raw_files_in_directory()
        self.init_pbar.update(1)
        self.init_pbar.set_description("Parsing GCP Storage Bucket Locations")
        self._parse_gcp_storage_bucket_locations_for_all_files_in_directory()
        self.init_pbar.update(1)
        # Create relocation paths if specified.
        if self.relocation_path != "":
            self.init_pbar.set_description("Parsing Relocation Paths")
            self._parse_relocation_paths_for_all_files_in_directory()
            self.init_pbar.update(1)
        self.init_pbar.set_description("Directory Analysis Complete")
        self.init_pbar.close()

    def _parse_raw_files_in_directory(self):
        """Parses through all of raw data files in the directory."""

        # Get all raw files in directory.
        self.raw_files = [
            file_path
            for file_path in self.all_file_paths_in_directory
            if self.all_file_paths_in_directory[file_path]["type"]
            .lower()
            .endswith(".raw")
        ]
        # Get num of all raw files in directory.
        self.num_raw_files_in_directory = len(self.raw_files)

        # Get all idx files in directory.
        self.idx_files = [
            file_path
            for file_path in self.all_file_paths_in_directory
            if self.all_file_paths_in_directory[file_path]["type"]
            .lower()
            .endswith(".idx")
        ]
        # Get num of all idx files in directory.
        self.num_idx_files_in_directory = len(self.idx_files)

        # Get all bot files in directory.
        self.bot_files = [
            file_path
            for file_path in self.all_file_paths_in_directory
            if self.all_file_paths_in_directory[file_path]["type"]
            .lower()
            .endswith(".bot")
        ]
        # Get num of all bot files in directory.
        self.num_bot_files_in_directory = len(self.bot_files)

        # Get all evi files in directory.
        self.evi_files = [
            file_path
            for file_path in self.all_file_paths_in_directory
            if self.all_file_paths_in_directory[file_path]["type"]
            .lower()
            .endswith(".evi")
        ]
        # Get num of all evi files in directory.
        self.num_evi_files_in_directory = len(self.evi_files)

    def _parse_gcp_storage_bucket_locations_for_all_files_in_directory(
        self,
    ):
        """Parses through all of the files in the directory and gets the
        correct GCP storage bucket location for each file."""

        for file_path in tqdm(
            self.all_file_paths_in_directory,
            desc="Getting File GCP Locations",
            position=1,
            leave=False,
        ):
            file_name = os.path.basename(file_path)
            file_type = self.all_file_paths_in_directory[file_path]["type"]
            is_auxiliary = self.all_file_paths_in_directory[file_path][
                "is_auxiliary"
            ]
            if not is_auxiliary:
                # Parse based on file-type
                gcp_storage_bucket_location = parse_correct_gcp_storage_bucket_location_based_on_file_type(
                    file_name=file_name,
                    file_type=file_type,
                    ship_name=self.ship_name,
                    survey_name=self.survey_name,
                    echosounder=self.all_file_paths_in_directory[file_path][
                        "echosounder"
                    ],
                    data_source=self.data_source,
                    debug=self.debug,
                )
            elif is_auxiliary:
                # Parse using standard method instead of base file type
                # so we can bypass file-type based allocation.
                # This keeps the auxiliary directory layout.
                file_path_past_aux_directory = os.path.normpath(
                    file_path.split("auxiliary" + os.sep)[-1]
                )
                # Convert any '\\' to '/'
                file_path_past_aux_directory = (
                    file_path_past_aux_directory.replace("\\", "/")
                )
                gcp_storage_bucket_location = f"{self.data_source}/{self.ship_name}/{self.survey_name}/auxiliary/{file_path_past_aux_directory}"

            self.all_file_paths_in_directory[file_path][
                "gcp_storage_bucket_location"
            ] = gcp_storage_bucket_location

    def _parse_relocation_paths_for_all_files_in_directory(
        self,
    ):
        """Parses through all of the files in the directory and gets the
        correct relocation path for each file."""
        for file_path in self.all_file_paths_in_directory:
            file_name = os.path.basename(file_path)
            file_type = self.all_file_paths_in_directory[file_path]["type"]
            gcp_storage_bucket_location = parse_correct_gcp_storage_bucket_location_based_on_file_type(
                file_name=file_name,
                file_type=file_type,
                ship_name=self.ship_name,
                survey_name=self.survey_name,
                echosounder=self.all_file_paths_in_directory[file_path][
                    "echosounder"
                ],
                # This is on purpose!
                data_source=self.relocation_path,
                debug=self.debug,
            )
            self.all_file_paths_in_directory[file_path][
                "relocation_path"
            ] = gcp_storage_bucket_location
        # Normalize the relocation paths
        for file_path in self.all_file_paths_in_directory:
            self.all_file_paths_in_directory[file_path]["relocation_path"] = (
                os.path.normpath(
                    self.all_file_paths_in_directory[file_path][
                        "relocation_path"
                    ]
                )
                + os.sep
            )

    def print_all_files_in_directory(self):
        """Prints all the files in the directory."""
        pprint.pprint(self.all_file_paths_in_directory)

    def _test_upload_to_gcp_speeds(
        self, num_files: int = 0, megabytes: int = 0
    ):
        """Tests a small batch upload to GCP storage buckets using a specified
        number of files or number of megabytes. Prints out the average upload
        speed given the current network.

        Args:
            num_files (int, optional): _description_. Defaults to 0.
            megabytes (int, optional): _description_. Defaults to 0.
        """
        print("TESTING GCP UPLOAD SPEEDS")

        # Use XOR operator to assert that only one of these values is True.
        # [num_files,megabytes]
        assert (num_files ^ megabytes) or not any(
            [
                num_files,
                megabytes,
            ]
        ), (
            "Make sure that only one of the following params is set to True:\n"
            "[num_files,megabytes]\n"
            "\nor that all are set to false."
            f"[{num_files} {megabytes}"
        )

        # Get the files needed for upload.
        if num_files != 0:
            # Get the smallest `num_files` number of files.
            files_to_upload = self.all_files_sorted_by_size[:num_files]
        elif megabytes != 0:
            # Get the minimum number of files needed to upload `megabytes`
            # number of MBs to GCP.
            mb_in_bytes = (1024**2) * megabytes
            print(
                f"Calculating number of file(s) needed to reach {megabytes}"
                f" mbs ({mb_in_bytes} bytes)..."
            )
            # Edge condition where the lowest file size is equal to or bigger
            # than the specified megabytes. Use only that one file in this case
            if (
                self.all_file_paths_in_directory[
                    self.all_files_sorted_by_size[0]
                ]["size"]
                >= mb_in_bytes
            ):
                print(
                    "The smallest file is larger than the specified number "
                    "of `megabytes`. Using this single file for upload"
                    " testing."
                )
                files_to_upload = [self.all_files_sorted_by_size[0]]
            else:
                # Calculate the number of files needed to reach at least
                # `megabytes` bytes.
                bytes_allocated = 0
                all_files_sorted_by_size_idx = 0
                files_to_upload = []
                while bytes_allocated <= mb_in_bytes:
                    # get the next smallest file by size
                    next_file = self.all_files_sorted_by_size[
                        all_files_sorted_by_size_idx
                    ]
                    # Add the file to the list of files_to_upload.
                    files_to_upload.append(next_file)
                    # Update the bytes allocated.
                    bytes_allocated += self.all_file_paths_in_directory[
                        next_file
                    ]["size"]
                    all_files_sorted_by_size_idx += 1
        # Print statement with files and size allocated.
        bytes_allocated = 0
        for file in files_to_upload:
            bytes_allocated += self.all_file_paths_in_directory[file]["size"]
        megabytes_to_upload = bytes_allocated / (1024**2)
        print(
            f"`{len(files_to_upload)}` file(s) selected to upload."
            f" {megabytes_to_upload:.2f} mb total."
        )

        self._upload_to_gcp(files_to_upload=files_to_upload)

    def upload_files(
        self,
        files_to_upload: list[str] | None = None,
        save_results_loc: str = "",
    ):
        """Uploads the survey's files to their standard locations in the GCP
        storage bucket (see `all_file_paths_in_directory`), smallest first.
        Files already in the bucket aren't uploaded again. Prints the average
        upload speed at the end.

        Args:
            files_to_upload (List[str], optional): The file paths to upload
                (keys of `all_file_paths_in_directory`). Defaults to None: all
                of the survey's files.
            save_results_loc (str, optional): A folder to save a text file of
                the upload timings in. Defaults to "": not saved.
        """
        self._upload_to_gcp(
            files_to_upload=files_to_upload, save_results_loc=save_results_loc
        )

    def _upload_to_gcp(
        self,
        files_to_upload: List[str] = None,
        save_results_loc: str = "",
        force_reupload: bool = False,
    ):
        """Uploads all files to GCP according to their GCP storage bucket
        locations."""
        if files_to_upload is None:
            # Upload all files.
            files_to_upload = self.all_files_sorted_by_size
        # A file whose echosounder couldn't be found would go to a ".../None/"
        # folder. Refuse before uploading anything.
        no_echosounder = [
            file
            for file in files_to_upload
            if "/None/"
            in self.all_file_paths_in_directory[file][
                "gcp_storage_bucket_location"
            ]
        ]
        if no_echosounder:
            raise ValueError(
                f"{len(no_echosounder)} file(s) have no echosounder (e.g. "
                f"`{no_echosounder[0]}`): none of the folders above them is "
                "named after one. Give it when making the LocalSurvey, e.g. "
                'echosounder="EK80". Nothing has been uploaded.'
            )
        if not files_to_upload:
            print("NO FILES TO UPLOAD.")
            return
        # Upload with timings to calculate upload speeds in megabytes.
        file_upload_timings = []  # in seconds
        file_sizes_in_bytes = []  # in bytes
        print("BEGINNING UPLOAD(S)...")
        for file in tqdm(files_to_upload):
            # Start timer.
            start_time = datetime.now()
            file_name = file.split(os.sep)[-1]
            self.upload_file_to_gcs(
                file_name=file_name,
                file_location=file,
                gcp_storage_bucket_location=self.all_file_paths_in_directory[
                    file
                ]["gcp_storage_bucket_location"],
                gcp_bucket=self.gcp_bucket,
                force_upload=force_reupload,
                verbose=False,
                debug=self.debug,
            )
            # End the timer.
            end_time = datetime.now()
            elapsed_time_seconds = (end_time - start_time).total_seconds()
            file_upload_timings.append(elapsed_time_seconds)
            file_sizes_in_bytes.append(
                self.all_file_paths_in_directory[file]["size"]
            )

        # Calculate total elapsed time for uploads:
        total_file_size_uploaded_in_bytes = sum(file_sizes_in_bytes)
        total_elapsed_time_in_seconds = sum(file_upload_timings)
        total_elapsed_time_formatted_str = str(
            timedelta(seconds=total_elapsed_time_in_seconds)
        )
        # Average out the upload speeds:
        upload_speed_bytes_per_sec = total_file_size_uploaded_in_bytes / max(
            total_elapsed_time_in_seconds, 1e-9
        )
        upload_speed_in_mbitsps = (upload_speed_bytes_per_sec) / 125000

        print("Uploads complete.")
        print(
            "Total Size Uploaded in Bytes:"
            f" {total_file_size_uploaded_in_bytes}"
        )
        print(f"Total Elapsed Time: {total_elapsed_time_formatted_str}")
        print(
            f"Average Upload Speed: {upload_speed_in_mbitsps:.2f}"
            " megabits/second"
        )
        if save_results_loc != "":
            # Add current datetime to upload results file name.
            now = datetime.now()
            date_string = now.strftime("D%Y%m%dT%H%M%S")
            save_file_name = f"{date_string}_aalibrary_upload_results.txt"
            save_results_loc = os.sep.join([save_results_loc, save_file_name])
            save_results_loc = os.path.normpath(save_results_loc) + os.sep
            # Create the file if it doesn't exist
            with open(save_results_loc, "w", encoding="utf-8") as f:
                f.write("Uploads complete.\n")
                f.write(
                    "Total Size Uploaded in Bytes:"
                    f" {total_file_size_uploaded_in_bytes}\n"
                )
                f.write(
                    f"Total Elapsed Time: {total_elapsed_time_formatted_str}\n"
                )
                f.write(
                    f"Average Upload Speed: {upload_speed_in_mbitsps:.2f}"
                    " megabits/second\n"
                )

    def upload_file_to_gcs(
        self,
        file_name: str = "",
        file_location: str = "",
        gcp_storage_bucket_location: str = "",
        gcp_bucket: storage.Client.bucket = None,
        force_upload: bool = False,
        verbose: bool = True,
        debug: bool = False,
    ):
        """Safely uploads a local file to the storage bucket. Will also check
        to see if the file already exists.

        Args:
            file_name (str, optional): The file name (includes extension).
                Defaults to "".
            file_location (str, optional): The local location of the file.
                Defaults to "".
            gcp_storage_bucket_location (str, optional): The location of the
                file within GCP Storage Buckets.
                Defaults to "".
                NOTE: If this is not provided, a gcp_storage_bucket_location
                will be parsed from the other parameters provided.
            gcp_bucket (storage.Client.bucket, optional): The GCP bucket object
                used to upload the file. Defaults to None.
            force_upload (bool, optional): Whether or not to force the upload
                of the file to GCP. If True, the file will be uploaded
                regardless of whether or not it already exists in GCP.
                Defaults to False.
            verbose (bool, optional): Whether or not you want to print
                intermediate statements about uploads. Helpful for if you are
                using progress bars.
                Defaults to True.
            debug (bool, optional): Whether or not to print debug statements.
                Defaults to False.
        """
        assert file_location != "", "A `file_location` is needed to upload."

        # Check if the file exists in GCP
        file_exists_in_gcp = check_if_file_exists_in_gcp(
            gcp_bucket, file_path=gcp_storage_bucket_location
        )
        if file_exists_in_gcp and not force_upload:
            print(
                (
                    f"INFO: FILE `{file_name}` ALREADY EXISTS IN GCP AT "
                    f"`{gcp_storage_bucket_location}`."
                )
            )
        else:
            try:
                if verbose:
                    print(
                        (
                            f"UPLOADING FILE `{file_name}` TO GCP AT"
                            f" `{gcp_storage_bucket_location}`..."
                        )
                    )
                # Upload to storage bucket.
                upload_file_to_gcp_bucket(
                    bucket=gcp_bucket,
                    blob_file_path=gcp_storage_bucket_location,
                    local_file_path=file_location,
                    debug=debug,
                )
                if verbose:
                    print("UPLOADED.")
            except Exception as e:
                logging.error(
                    "COULD NOT UPLOAD FILE %s TO GCP (%s) STORAGE BUCKET DUE "
                    "TO THE FOLLOWING ERROR:\n%s",
                    file_name,
                    gcp_storage_bucket_location,
                    e,
                )

        return

    def relocate(self):
        """Relocates all of the files in the directory to the relocation path."""
        for file_path in self.all_file_paths_in_directory:
            relocation_path = self.all_file_paths_in_directory[file_path][
                "relocation_path"
            ]
            # Create the directory if it doesn't exist.
            if not os.path.exists(os.path.dirname(relocation_path)):
                os.makedirs(os.path.dirname(relocation_path))
            # Move the file to the relocation path.
            os.rename(file_path, relocation_path)

    def _get_single_file_gcp_checksum(self, file_path):
        """Gets a single file's GCP CRC32C checksum. Used for multithreading
        this process. Sets the checksum in the all_file_paths_in_directory
        dict."""

        gcp_crc32c_checksum = get_gcp_file_checksum(
            blob_name=self.all_file_paths_in_directory[file_path][
                "gcp_storage_bucket_location"
            ],
            bucket_name=self.gcp_bucket_name,
            gcp_bucket=self.gcp_bucket,
        )
        self.all_file_paths_in_directory[file_path][
            "gcp_crc32c_checksum"
        ] = gcp_crc32c_checksum

    def verify_gcp_uploads(
        self, reupload_incorrectly_uploaded_files: bool = False
    ):
        """Verifies the files that have been uploaded to GCP using file
        checksums. Gives a printout of the number of files that have/haven't
        been uploaded.

        Args:
            reupload_incorrectly_uploaded_files (bool, optional): If True,
                reuploads files that were incorrectly uploaded.
                Defaults to False.
        """
        # Go through all files in the directory and verify their checksums with
        # the files in GCP.
        files_uploaded = 0
        files_uploaded_size_in_bytes = 0
        files_uploaded_incorrectly = 0
        files_uploaded_incorrectly_size_in_bytes = 0
        files_not_uploaded = 0
        files_not_uploaded_size_in_bytes = 0
        print("VERIFYING FILES...")
        # Get the GCP CRC32C checksums for all files in the directory using
        # a multi-threaded approach.
        thread_map(
            self._get_single_file_gcp_checksum,
            list(self.all_file_paths_in_directory.keys()),
            chunksize=1,
            desc="Getting GCP CRC32C Checksums",
            leave=False,
        )
        # pprint.pprint(
        #     [
        #         self.all_file_paths_in_directory[file_path].get(
        #             "gcp_crc32c_checksum", None
        #         )
        #         for file_path in self.all_file_paths_in_directory
        #     ]
        # )
        # Get the CRC32C checksums for all local files in the directory using
        # a multi-processing approach.
        # Only calculate CRC32C checksums for files that have already been
        # uploaded to GCP. This offers a slight improvement in time.
        uploaded_file_paths = [
            file_path
            for file_path in self.all_file_paths_in_directory.keys()
            if self.all_file_paths_in_directory[file_path][
                "gcp_crc32c_checksum"
            ]
            is not None
        ]
        # multiprocessing_results is [(file_path, checksum),(...]
        multi_processing_results = process_map(
            _get_single_local_file_crc32c_checksum_multi_process,
            uploaded_file_paths,
            chunksize=1,
            desc="Calculating Local CRC32C Checksums for Uploaded Files",
            leave=False,
        )
        # Update the main dict with the resulting checksums.
        for file_path, checksum in multi_processing_results:
            self.all_file_paths_in_directory[file_path][
                "local_crc32c_checksum"
            ] = checksum

        # Calculate local checksums and compare with GCP checksums.
        for file_path in tqdm(self.all_file_paths_in_directory):
            gcp_crc32c_checksum = self.all_file_paths_in_directory[file_path][
                "gcp_crc32c_checksum"
            ]
            # If the gcp crc32c checksum is None, then the object does not
            # exist in GCP. Add it to the files not uploaded.
            if gcp_crc32c_checksum is None:
                self.missing_or_incorrectly_uploaded_files.add(file_path)
                files_not_uploaded += 1
                files_not_uploaded_size_in_bytes += (
                    self.all_file_paths_in_directory[file_path]["size"]
                )
                # Continue to avoid wasting time calculating local checksum.
                continue

            # Get the local file checksum.
            local_file_checksum = self.all_file_paths_in_directory[file_path][
                "local_crc32c_checksum"
            ]
            # In the case the blob exists but the checksums do not match (has
            # been uploaded incorrectly)
            if gcp_crc32c_checksum != local_file_checksum:
                self.missing_or_incorrectly_uploaded_files.add(file_path)
                files_uploaded_incorrectly += 1
                files_uploaded_incorrectly_size_in_bytes += (
                    self.all_file_paths_in_directory[file_path]["size"]
                )
            # In the case the blob exists and the checksums match (has been
            # uploaded correctly)
            elif gcp_crc32c_checksum == local_file_checksum:
                files_uploaded += 1
                files_uploaded_size_in_bytes += (
                    self.all_file_paths_in_directory[file_path]["size"]
                )

        # Print the results of the verification.
        print("VERIFICATION COMPLETE.")
        print("RESULTS:")
        print(f"Files uploaded correctly: {files_uploaded}")
        print(f"Files uploaded incorrectly: {files_uploaded_incorrectly}")
        print(f"Files not uploaded: {files_not_uploaded}")
        print(
            f"Total size of uploaded files (correctly):\n"
            f"\t{files_uploaded_size_in_bytes / (1024**2):.2f} megabytes"
            f"\t{files_uploaded_size_in_bytes / (1024**3):.2f} gigabytes"
        )
        print(
            f"Total size of uploaded files (incorrectly):\n"
            f"\t{files_uploaded_incorrectly_size_in_bytes / (1024**2):.2f}"
            " megabytes"
            f"\t{files_uploaded_incorrectly_size_in_bytes / (1024**3):.2f}"
            " gigabytes"
        )
        print(
            f"Total size of not uploaded files:\n"
            f"\t{files_not_uploaded_size_in_bytes / (1024**2):.2f} megabytes"
            f"\t{files_not_uploaded_size_in_bytes / (1024**3):.2f} gigabytes"
        )
        print(
            "NOTE: Files uploaded incorrectly are counted based on "
            "mismatched checksums."
        )

        if (
            reupload_incorrectly_uploaded_files
            and self.missing_or_incorrectly_uploaded_files
        ):
            print(
                "Reuploading missing or mismatched checksum files to GCP "
                "storage bucket..."
            )
            self._upload_to_gcp(
                files_to_upload=list(
                    self.missing_or_incorrectly_uploaded_files
                ),
                save_results_loc="",
                force_reupload=True,
            )
            print("Reupload complete. Please verify again.")


if __name__ == "__main__":
    # set logging config
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    from aalibrary import config

    config.use_gcp_dev()

    # logging.basicConfig(
    #     level=logging.DEBUG,
    #     format="%(asctime)s [%(levelname)s] %(message)s",
    #     handlers=[logging.StreamHandler(sys.stdout)],
    # )
    # set up storage objects
    s3_client, s3_resource, s3_bucket = create_s3_objs()
    gcp_stor_client, gcp_bucket_name, gcp_bucket = (
        setup_gcp_storage_objs()
    )

    # create a survey object
    # survey = Survey(
    #     ship_name="Reuben_Lasker",
    #     survey_name="RL2107",
    #     data_source="NCEI",
    #     file_download_directory="./data/",
    #     upload_to_gcp=False,
    #     debug=True,
    #     gcp_stor_client=gcp_stor_client,
    #     gcp_bucket=gcp_bucket,
    #     gcp_bucket_name=gcp_bucket_name,
    #     s3_client=s3_client,
    #     s3_bucket=s3_bucket,
    #     s3_resource=s3_resource,
    # )

    # survey.create_raw_file_objects()
    # print(survey.raw_files)

    # local_survey = LocalSurvey(
    #     ship_name="Henry_B._Bigelow",
    #     survey_name="HB2407",
    #     data_source="HDD",
    #     directory_path="./HDD/",
    #     # relocation_path="./Copy_HDD/",
    #     upload_to_gcp=False,
    #     debug=True,
    #     gcp_bucket=gcp_bucket,
    #     gcp_bucket_name=gcp_bucket_name,
    # )
    # local_survey.verify_gcp_uploads()
    # local_survey.print_all_files_in_directory()
    # local_survey._test_upload_to_gcp_speeds(megabytes=100)
    # i = 0
    # for file_path in local_survey.all_file_paths_in_directory:
    #     if (
    #         local_survey.all_file_paths_in_directory[file_path]["type"].lower()
    #         == ".yaml"
    #     ):
    #         i += 1
    # print(i)

    # Test auxiliary file uploads
    local_survey = LocalSurvey(
        ship_name="Reuben_Lasker",
        survey_name="TEST1234",
        data_source="HDD",
        directory_path="./TEST1234/",
        auxiliary_directory_path="./TEST1234/auxiliary/",
        upload_to_gcp=False,
        debug=True,
        gcp_bucket=gcp_bucket,
        gcp_bucket_name=gcp_bucket_name,
    )
    pprint.pprint(local_survey.all_file_paths_in_directory)
    local_survey.verify_gcp_uploads(reupload_incorrectly_uploaded_files=True)
