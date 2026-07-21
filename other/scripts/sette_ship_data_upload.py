"""This script is used to test out data uploads directly from the Sette."""

import sys
import os
import subprocess
import pip
import logging
import os
import pprint
from glob import glob
from pathlib import Path
import pprint
import string


GCP_PROJECT_ID = ["ggn-nmfs-aa-prod-1"]
GCS_BUCKET_NAME = ["ggn-nmfs-aa-prod-1-data"]
SCIENCE_CENTERS = ["AFSC", "NEFSC", "NWFSC", "OST", "PIFSC", "SEFSC", "SWFSC"]
VALID_ECHOSOUNDERS = [
    "ar040-hat-55145",
    "ar040-jax-5146",
    "ar040-vac-55144",
    "ar049-hat-5145",
    "ar049-vac-5144",
    "DAFT1-C11-201701",
    "DAFT2-C1-201701",
    "DAFT4-C11-201801",
    "DAFT5-C1-201801",
    "DAFT6-C4-201801",
    "EK500",
    "EK60-EK5",
    "EK60",
    "EK80",
    "EM122",
    "EM124",
    "EM2040",
    "EM2040C",
    "EM2040P",
    "EM2045",
    "EM3002",
    "EM302",
    "EM304",
    "EM710",
    "EM712",
    "en615-hat-55145",
    "en615-jax-55146",
    "en615-vac-55144",
    "en626-hat-55145",
    "en626-jax-55146",
    "en626-vac-55144",
    "ES60",
    "ES80",
    "GU1402L1",
    "GU1402L2",
    "M3",
    "ME70",
    "MS70",
    "RESON7125",
    "sme100-201901",
    "sme120-201901",
    "sme140-201901",
    "sme80-201901",
]
RAW_DATA_FILE_TYPES = ["raw", "idx", "bot", "evi"]
CONVERTED_DATA_FILE_TYPES = ["netcdf", "nc"]
METADATA_FILE_TYPES = ["json"]
AUXILIARY_EV_FILE_TYPES = ["EV", "evb"]
AUXILIARY_REGION_DEFS_FILE_ENDINGS = ["rdefs.evr", "regiondefs.csv"]
AUXILIARY_SEABED_LINES_FILE_ENDINGS = [".evl"]
AUXILIARY_TEMPLATE_FILE_FILE_ENDINGS = ["template.ev"]
AUXILIARY_SA_FILE_FILE_ENDINGS = [
    "sa_Cells.csv",
    "sa_RegionsByCells.csv",
    "Cells_sa.csv",
    "_sa.csv",
]
CALIBRATION_MANUFACTURER_REPORT_FILE_BEGINNINGS = ["CalibrationDataFile"]
CALIBRATION_STANDARDIZED_REPORT_FILE_ENDINGS = ["config-1.yaml"]
VALID_FILETYPES = ["raw", "idx", "netcdf", "nc", "json", "bot", "evi"]
VALID_FILE_ENDINGS = (
    RAW_DATA_FILE_TYPES
    + CONVERTED_DATA_FILE_TYPES
    + METADATA_FILE_TYPES
    + AUXILIARY_EV_FILE_TYPES
    + AUXILIARY_REGION_DEFS_FILE_ENDINGS
    + AUXILIARY_SEABED_LINES_FILE_ENDINGS
    + AUXILIARY_TEMPLATE_FILE_FILE_ENDINGS
    + AUXILIARY_SA_FILE_FILE_ENDINGS
)
REQUIRED_PACKAGES = [
    "google-cloud",
    "google-api-python-client",
    "inquirerpy",
    "google-cloud-storage",
]


def check_and_install_missing_pkgs():
    """Checks and installs missing packages."""

    missing_packages = []
    # for package in REQUIRED_PACKAGES:
    #     # Look up the module specification without actually importing it
    #     if importlib.util.find_spec(package) is None:
    #         missing_packages.append(package)
    for package in REQUIRED_PACKAGES:
        try:
            __import__(package)
        except ImportError:
            print(f"Installing {package}...")
            subprocess.check_call(
                [sys.executable, "-m", "pip", "install", package]
            )

    if missing_packages:
        print(
            f"Error: Missing required packages: {', '.join(missing_packages)}"
        )
        for package in missing_packages:
            print(f"Installing `{package}`")
            pip.main(["install", package])

    print("All dependencies are satisfied. Running script...")


def normalize_ship_name(ship_name: str = "") -> str:
    """Normalizes a ship's name. This is necessary for creating a deterministic
    file structure within our GCP storage bucket.
    The ship name is returned as a Title_Cased_And_Snake_Cased ship name, with
    no punctuation.
    Ex. `HENRY B. BIGELOW` will return `Henry_B_Bigelow`

    Args:
        ship_name (str, optional): The ship name string. Defaults to "".

    Returns:
        str: The formatted and normalized version of the ship name.
    """

    # Lower case the string
    ship_name = ship_name.lower()
    # Un-normalize (replace `_` with ` ` to help further processing)
    # In the edge-case that users include an underscore.
    ship_name = ship_name.replace("_", " ")
    # Remove all punctuation.
    ship_name = "".join(
        [char for char in ship_name if char not in string.punctuation]
    )
    # Title-case it
    ship_name = ship_name.title()
    # Snake-case it
    ship_name = ship_name.replace(" ", "_")

    return ship_name


def parse_correct_gcp_storage_bucket_location_based_on_file_type(
    file_name: str = "",
    file_type: str = "",
    ship_name: str = "",
    survey_name: str = "",
    echosounder: str = "",
    data_source: str = "",
    debug: bool = False,
) -> str:
    """This function will parse the correct GCP storage bucket location based
    on the file type. This is necessary because different file types are stored
    in different locations within the GCP storage bucket.

    Args:
        file_name (str, optional): The file name (includes extension).
            Defaults to "".

    Returns:
        str: The correctly parsed GCP storage bucket location according to
            AALibrary standards.
    """

    gcp_storage_bucket_location = None
    file_name_lower = file_name.lower()
    file_type = file_name.split(".")[-1]
    if file_type.lower() in RAW_DATA_FILE_TYPES:
        gcp_storage_bucket_location = f"{data_source}/{ship_name}/{survey_name}/{echosounder}/data/raw/{file_name}"
    elif file_type.lower() in CONVERTED_DATA_FILE_TYPES:
        gcp_storage_bucket_location = f"{data_source}/{ship_name}/{survey_name}/{echosounder}/data/netcdf/{file_name}"
    elif file_type.lower() in AUXILIARY_EV_FILE_TYPES:
        gcp_storage_bucket_location = f"{data_source}/{ship_name}/{survey_name}/auxiliary/ev_files/{file_name}"
    else:
        # Check for file endings
        # Check for EV files
        for ending in AUXILIARY_EV_FILE_TYPES:
            ending = ending.lower()
            if file_name_lower.endswith(ending):
                gcp_storage_bucket_location = f"{data_source}/{ship_name}/{survey_name}/auxiliary/ev_files/{file_name}"
                break
        # Check for region defs files
        for ending in AUXILIARY_REGION_DEFS_FILE_ENDINGS:
            ending = ending.lower()
            if file_name_lower.endswith(ending):
                gcp_storage_bucket_location = f"{data_source}/{ship_name}/{survey_name}/auxiliary/ev_region_defs/{file_name}"
                break
        # Check for seabed lines files
        for ending in AUXILIARY_SEABED_LINES_FILE_ENDINGS:
            ending = ending.lower()
            if file_name_lower.endswith(ending):
                gcp_storage_bucket_location = f"{data_source}/{ship_name}/{survey_name}/auxiliary/ev_seabed_lines/{file_name}"
                break
        # Check for template files
        for ending in AUXILIARY_TEMPLATE_FILE_FILE_ENDINGS:
            ending = ending.lower()
            if file_name_lower.endswith(ending):
                gcp_storage_bucket_location = f"{data_source}/{ship_name}/{survey_name}/auxiliary/ev_templates/{file_name}"
                break
        # Check for sa files
        for ending in AUXILIARY_SA_FILE_FILE_ENDINGS:
            ending = ending.lower()
            if file_name_lower.endswith(ending):
                gcp_storage_bucket_location = f"{data_source}/{ship_name}/{survey_name}/auxiliary/sa_files/{file_name}"
                break
        # Check for calibration manufacturer reports
        for beginning in CALIBRATION_MANUFACTURER_REPORT_FILE_BEGINNINGS:
            beginning = beginning.lower()
            if file_name_lower.startswith(beginning):
                gcp_storage_bucket_location = f"{data_source}/{ship_name}/{survey_name}/{echosounder}/calibration/manufacturer_reports/{file_name}"
                break
        # Check for calibration standardized reports
        for ending in CALIBRATION_STANDARDIZED_REPORT_FILE_ENDINGS:
            ending = ending.lower()
            if file_name_lower.endswith(ending):
                gcp_storage_bucket_location = f"{data_source}/{ship_name}/{survey_name}/{echosounder}/calibration/standardized_reports/{file_name}"
                break
        # Check for channel mapping file
        if file_name_lower == "channel_mapping.yaml":
            gcp_storage_bucket_location = f"{data_source}/{ship_name}/{survey_name}/{echosounder}/calibration/{file_name}"
        # If the file path still cannot be deduced.
        if gcp_storage_bucket_location is None:
            # Place all unknown files in `other` folder
            gcp_storage_bucket_location = f"{data_source}/{ship_name}/{survey_name}/auxiliary/other/{file_name}"

    if debug:
        logging.debug(
            "PARSED GCP_STORAGE_BUCKET_LOCATION: %s",
            gcp_storage_bucket_location,
        )
    return gcp_storage_bucket_location


class LocalSurvey:
    """Used to represent surveys that exist on your local machine. You can use
    this object to automatically parse through a directory that contains
    survey data, add metadata to the survey, and ultimately upload the data
    to the appropriate location in GCP."""

    ship_name: str = ""
    survey_name: str = ""
    data_source: str = "HDD"
    directory_path: str = ""
    relocation_path: str = ""
    debug: bool = False
    gcp_bucket: storage.Client.bucket = None
    gcp_project_id: str = None
    gcp_bucket_name: str = None
    upload_to_gcp: bool = None
    # Get all valid and normalized ICES ship names
    # valid_ICES_ship_names = ices_ship_names.get_all_ices_ship_names(
    #     normalize_ship_names=True
    # )

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        self._handle_paths()
        self._create_vars_for_use_later()

    def __repr__(self):
        print_dict = {
            "ship_name": self.ship_name,
            "survey_name": self.survey_name,
            "num_all_files_in_directory": self.num_all_files_in_directory,
            "total_file_size_bytes_in_directory": self.total_file_size_bytes_in_directory,
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
            if self.debug:
                logging.debug(
                    "normalized directory path = %s", self.directory_path
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
        # if self.ship_name in self.valid_ICES_ship_names:
        #     self.ices_code = ices_ship_names.get_ices_code_from_ship_name(
        #         ship_name=self.ship_name, is_normalized=True
        #     )
        # else:
        #     self.ices_code = ""

        # Handle undefined GCP project id and bucket name by using environment
        # variables.
        if self.gcp_project_id is None:
            self.gcp_project_id = os.getenv("AALIBRARY_GCP_PROJECT_ID")
        if self.gcp_bucket_name is None:
            self.gcp_bucket_name = os.getenv("AALIBRARY_GCP_BUCKET_NAME")

        # Get all files in this directory.
        directory_path_glob = os.sep.join([self.directory_path, "**", "*"])
        self.all_file_paths_in_directory = glob(
            directory_path_glob, recursive=True
        )
        self.all_file_paths_in_directory = {
            file_path: {} for file_path in self.all_file_paths_in_directory
        }
        # List to get rid of deleting while iterating error.
        for file_path in list(self.all_file_paths_in_directory):
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
            # Assign echosounder used based on folder name.
            # Get parent folders
            parent_dirs = [
                parent.name
                for parent in p.parents
                if parent.name and parent.name in VALID_ECHOSOUNDERS
            ]
            echosounder = parent_dirs[0] if parent_dirs else None
            self.all_file_paths_in_directory[file_path][
                "echosounder"
            ] = echosounder

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
        print(self.all_file_types_in_directory)
        # Get file sizes in bytes.
        for file_path in self.all_file_paths_in_directory:
            self.all_file_paths_in_directory[file_path]["size"] = (
                os.path.getsize(file_path)
            )
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

        self._parse_raw_files_in_directory()
        self._parse_metadata_files_in_directory()
        self._parse_calibration_files_in_directory()
        self._parse_auxiliary_files_in_directory()
        self._parse_unknown_files_in_directory()
        self._parse_all_gcp_storage_bucket_locations_for_all_files_in_directory()
        # Create relocation paths if specified.
        if self.relocation_path != "":
            self._parse_relocation_paths_for_all_files_in_directory()

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

    def _parse_metadata_files_in_directory(self): ...

    def _parse_calibration_files_in_directory(self): ...

    def _parse_auxiliary_files_in_directory(self): ...

    def _parse_unknown_files_in_directory(self): ...

    def _parse_all_gcp_storage_bucket_locations_for_all_files_in_directory(
        self,
    ):
        """Parses through all of the files in the directory and gets the
        correct GCP storage bucket location for each file."""

        for file_path in self.all_file_paths_in_directory:
            file_name = os.path.basename(file_path)
            file_type = self.all_file_paths_in_directory[file_path]["type"]
            gcp_storage_bucket_location = (
                parse_correct_gcp_storage_bucket_location_based_on_file_type(
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
            )
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

    def _upload_to_gcp(self):
        """Uploads to GCP at the correct location."""
        self.all_files_sorted_by_size = [
            k
            for k, v in sorted(
                self.all_file_paths_in_directory.items(),
                key=lambda item: item[1]["size"],
            )
        ]
        # TODO: upload each file one by one.

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


if __name__ == "__main__":
    directory_path = r"C:\Users\Reka.Domokos-Boyer\Desktop\Work\DataAnalyses\AmSam2HawaiiSE2602L3\PreliminaryLooks\ExampleRawFiles"

    local_survey = LocalSurvey(
        ship_name="OSCAR ELTON SETTE",
        survey_name="SE2602",
        data_source="HDD",
        directory_path=directory_path,
        upload_to_gcp=False,
        debug=True,
    )
    local_survey.print_all_files_in_directory()
    print(local_survey)
    local_survey._test_upload_to_gcp_speeds(megabytes=100)
    local_survey._upload_to_gcp()
