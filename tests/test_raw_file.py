"""For testing the custom RawFile object."""

import os
import pytest

from aalibrary.raw_file import RawFile
from aalibrary.utils import cloud_utils


class TestRawFileCreationErrors:
    """A class which tests various scenarios related to the creation of a
    RawFile object."""

    def setup_class(self):
        """Used for setting up the class."""

        # Create basic vars
        self.file_name = "2107RL_CW-D20210919-T172430.raw"
        self.file_type = "raw"
        self.ship_name = "Reuben_Lasker"
        self.survey_name = "RL2107"
        self.echosounder = "EK80"
        self.data_source = "TEST"
        self.file_download_directory = "."
        self.is_metadata = False
        self.debug = False

        # Create connection vars
        self.gcp_stor_client, self.gcp_bucket_name, self.gcp_bucket = (
            cloud_utils.setup_gcp_storage_objs()
        )
        self.s3_client, self.s3_resource, self.s3_bucket = (
            cloud_utils.create_s3_objs()
        )

        # Create the basic RawFile object for internal checking.
        self.rf = RawFile(
            file_name=self.file_name,
            file_type=self.file_type,
            ship_name=self.ship_name,
            survey_name=self.survey_name,
            echosounder=self.echosounder,
            data_source=self.data_source,
            file_download_directory=self.file_download_directory,
            is_metadata=self.is_metadata,
            debug=self.debug,
            gcp_bucket=self.gcp_bucket,
            s3_resource=self.s3_resource,
        )

    def test_raw_file_object_creation_null_file_name(self):
        """Tests the error-handling for RawFile class when there is an empty
        `file_name` param."""
        with pytest.raises(Exception):
            RawFile(
                file_name="",
                file_type=self.file_type,
                ship_name=self.ship_name,
                survey_name=self.survey_name,
                echosounder=self.echosounder,
                data_source=self.data_source,
                file_download_directory=self.file_download_directory,
                is_metadata=self.is_metadata,
                debug=self.debug,
            )

    def test_raw_file_object_creation_null_file_type(self):
        """Tests the error-handling for RawFile class when there is an empty
        `file_type` param."""
        with pytest.raises(Exception):
            RawFile(
                file_name=self.file_name,
                file_type="",
                ship_name=self.ship_name,
                survey_name=self.survey_name,
                echosounder=self.echosounder,
                data_source=self.data_source,
                file_download_directory=self.file_download_directory,
                is_metadata=self.is_metadata,
                debug=self.debug,
            )

    def test_raw_file_object_creation_invalid_file_type(self):
        """Tests the error-handling for RawFile class when there is an invalid
        `file_type` param."""
        with pytest.raises(Exception):
            RawFile(
                file_name=self.file_name,
                file_type="abcd",
                ship_name=self.ship_name,
                survey_name=self.survey_name,
                echosounder=self.echosounder,
                data_source=self.data_source,
                file_download_directory=self.file_download_directory,
                is_metadata=self.is_metadata,
                debug=self.debug,
            )

    def test_raw_file_object_creation_null_ship_name(self):
        """Tests the error-handling for RawFile class when there is an invalid
        `ship_name` param."""
        with pytest.raises(Exception):
            RawFile(
                file_name=self.file_name,
                file_type=self.file_type,
                ship_name="",
                survey_name=self.survey_name,
                echosounder=self.echosounder,
                data_source=self.data_source,
                file_download_directory=self.file_download_directory,
                is_metadata=self.is_metadata,
                debug=self.debug,
            )

    def test_raw_file_object_creation_validity_of_normalized_ship_name(self):
        """Tests the error-handling for RawFile class when there is an
        `ship_name` param that does not exist in the ICES database."""
        with pytest.raises(Exception):
            RawFile(
                file_name=self.file_name,
                file_type=self.file_type,
                ship_name="Queen Anne's Revenge",
                survey_name=self.survey_name,
                echosounder=self.echosounder,
                data_source=self.data_source,
                file_download_directory=self.file_download_directory,
                is_metadata=self.is_metadata,
                debug=self.debug,
            )

    def test_raw_file_object_creation_null_survey_name(self):
        """Tests the error-handling for RawFile class when there is a null
        `survey_name` param."""
        with pytest.raises(Exception):
            RawFile(
                file_name=self.file_name,
                file_type=self.file_type,
                ship_name=self.ship_name,
                survey_name="",
                echosounder=self.echosounder,
                data_source=self.data_source,
                file_download_directory=self.file_download_directory,
                is_metadata=self.is_metadata,
                debug=self.debug,
            )

    def test_raw_file_object_creation_null_echosounder(self):
        """Tests the error-handling for RawFile class when there is a null
        `echosounder` param."""
        with pytest.raises(Exception):
            RawFile(
                file_name=self.file_name,
                file_type=self.file_type,
                ship_name=self.ship_name,
                survey_name=self.survey_name,
                echosounder="",
                data_source=self.data_source,
                file_download_directory=self.file_download_directory,
                is_metadata=self.is_metadata,
                debug=self.debug,
            )

    def test_raw_file_object_creation_invalid_echosounder(self):
        """Tests the error-handling for RawFile class when there is an invalid
        `echosounder` param."""
        with pytest.raises(Exception):
            RawFile(
                file_name=self.file_name,
                file_type=self.file_type,
                ship_name=self.ship_name,
                survey_name=self.survey_name,
                echosounder="abcd",
                data_source=self.data_source,
                file_download_directory=self.file_download_directory,
                is_metadata=self.is_metadata,
                debug=self.debug,
            )

    def test_raw_file_object_creation_invalid_file_download_directory(self):
        """Tests the error-handling for RawFile class when there is an invalid
        `file_download_directory` param."""
        with pytest.raises(Exception):
            # Create a test file to point the file download location to.
            with open("file.temp", "a"):
                os.utime("file.temp", None)

            RawFile(
                file_name=self.file_name,
                file_type=self.file_type,
                ship_name=self.ship_name,
                survey_name=self.survey_name,
                echosounder=self.echosounder,
                data_source=self.data_source,
                file_download_directory="file.temp",
                is_metadata=self.is_metadata,
                debug=self.debug,
            )

    def teardown_class(self):
        """Tears-down any temporary files, variables, or anything that was used
        for testing."""
        os.remove("file.temp")


class TestRawFileVarsHandling:
    """A class which tests various scenarios related to the creation of a
    RawFile object's vars."""

    def setup_class(self):
        """Used for setting up the class."""

        # Create basic vars
        self.file_name = "2107RL_CW-D20210919-T172430.raw"
        self.file_type = "raw"
        self.ship_name = "Reuben_Lasker"
        self.survey_name = "RL2107"
        self.echosounder = "EK80"
        self.data_source = "TEST"
        self.file_download_directory = "."
        self.is_metadata = False
        self.debug = False

        # Create connection vars
        self.gcp_stor_client, self.gcp_bucket_name, self.gcp_bucket = (
            cloud_utils.setup_gcp_storage_objs()
        )
        self.s3_client, self.s3_resource, self.s3_bucket = (
            cloud_utils.create_s3_objs()
        )

        # Create the basic RawFile object for internal checking.
        self.rf = RawFile(
            file_name=self.file_name,
            file_type=self.file_type,
            ship_name=self.ship_name,
            survey_name=self.survey_name,
            echosounder=self.echosounder,
            data_source=self.data_source,
            file_download_directory=self.file_download_directory,
            is_metadata=self.is_metadata,
            debug=self.debug,
        )

    def test_raw_file_object_creation_null_file_download_directory(self):
        """Tests the variable-handling for RawFile class when there is a null
        `file_download_directory` param."""

        assert (
            RawFile(
                file_name=self.file_name,
                file_type=self.file_type,
                ship_name=self.ship_name,
                survey_name=self.survey_name,
                echosounder=self.echosounder,
                data_source=self.data_source,
                file_download_directory=".",
                is_metadata=self.is_metadata,
                debug=self.debug,
            ).file_download_directory
            == (os.path.normpath(self.file_download_directory) + os.sep)
        ), (
            "RawFile is not handling an empty `file_download_directory` "
            "correctly."
        )

    def test_raw_file_object_creation_incorrect_os_sep_file_download_directory(
        self,
    ):
        """Tests the variable-handling for RawFile class when there is a
        separator used by another os in the `file_download_directory` param."""

        assert (
            RawFile(
                file_name=self.file_name,
                file_type=self.file_type,
                ship_name=self.ship_name,
                survey_name=self.survey_name,
                echosounder=self.echosounder,
                data_source=self.data_source,
                file_download_directory="./",
                is_metadata=self.is_metadata,
                debug=self.debug,
            ).file_download_directory
            == (os.path.normpath(self.file_download_directory) + os.sep)
        ), (
            "RawFile is not normalizing the `file_download_directory` "
            "param correctly."
        )

    def test_gcp_object_creation(self):
        assert (
            RawFile(
                file_name=self.file_name,
                file_type=self.file_type,
                ship_name=self.ship_name,
                survey_name=self.survey_name,
                echosounder=self.echosounder,
                data_source=self.data_source,
                file_download_directory=self.file_download_directory,
                is_metadata=self.is_metadata,
                debug=self.debug,
            ).gcp_bucket
            is not None
        ), "RawFile has failed to instantiate a `gcp_bucket` object."

    def test_s3_object_creation(self):
        assert (
            RawFile(
                file_name=self.file_name,
                file_type=self.file_type,
                ship_name=self.ship_name,
                survey_name=self.survey_name,
                echosounder=self.echosounder,
                data_source=self.data_source,
                file_download_directory=self.file_download_directory,
                is_metadata=self.is_metadata,
                debug=self.debug,
            ).s3_resource
            is not None
        ), "RawFile has failed to instantiate a `s3_resource` object."

    def test_file_name_wo_extension(self):
        assert (
            "." not in self.rf.file_name_wo_extension
        ), "RawFile object contains a file extension parameter (dot)."

    def test_adhoc_file_name_creations(self):
        assert (
            self.rf.raw_file_name == "2107RL_CW-D20210919-T172430.raw"
        ), "RawFile object has created an incorrect raw file name."
        assert (
            self.rf.idx_file_name == "2107RL_CW-D20210919-T172430.idx"
        ), "RawFile object has created an incorrect idx file name."
        assert (
            self.rf.bot_file_name == "2107RL_CW-D20210919-T172430.bot"
        ), "RawFile object has created an incorrect bot file name."
        assert (
            self.rf.netcdf_file_name == "2107RL_CW-D20210919-T172430.nc"
        ), "RawFile object has created an incorrect nc file name."

    def test_file_download_paths(self):
        assert (
            self.rf.raw_file_download_path == "2107RL_CW-D20210919-T172430.raw"
        ), "RawFile object has created an incorrect raw file download path."
        assert (
            self.rf.idx_file_download_path == "2107RL_CW-D20210919-T172430.idx"
        ), "RawFile object has created an incorrect idx file download path."
        assert (
            self.rf.bot_file_download_path == "2107RL_CW-D20210919-T172430.bot"
        ), "RawFile object has created an incorrect bot file download path."
        assert (
            self.rf.netcdf_file_download_path
            == "2107RL_CW-D20210919-T172430.nc"
        ), "RawFile object has created an incorrect netcdf file download path."
