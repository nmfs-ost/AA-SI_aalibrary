"""For testing ingestion."""

import os
import pytest

from app import ingestion
from app.utils import cloud_utils


class TestNCEIIngestion:
    """A class which tests various ingestion functionality of the API."""

    def setup_class(self):
        """Used for setting up the class."""
        self.file_name = "2107RL_CW-D20210813-T220732.raw"
        self.file_type = "nc"
        self.ship_name = "Reuben_Lasker"
        self.survey_name = "RL2107"
        self.echosounder = "EK80"
        self.data_source = "TEST"
        self.file_download_location = "."

        self.local_raw_file_path = os.sep.join(
            [self.file_download_location, self.file_name]
        )
        self.local_idx_file_path = (
            ".".join(self.local_raw_file_path.split(".")[:-1]) + ".idx"
        )
        # set up storage objects
        _, _, self.gcp_bucket = cloud_utils.setup_gcp_storage_objs()
        self.s3_client, self.s3_resource, self.s3_bucket = cloud_utils.create_s3_objs()

    def test_force_download_from_NCEI(self):
        """Tests downloading a raw file direct from NCEI."""
        # assert files exists
        # assert files sizes are correct
        ingestion.download_raw_file(
            file_name=self.file_name,
            file_type=self.file_type,
            ship_name=self.ship_name,
            survey_name=self.survey_name,
            echosounder=self.echosounder,
            file_download_location=self.file_download_location,
            is_metadata=False,
            force_download_from_ncei=True,
            debug=False,
        )
        # assert that both raw and idx files exist after they have been downloaded.
        assert os.path.exists(self.local_raw_file_path) and os.path.exists(
            self.local_idx_file_path
        ), "Raw or Idx file has not been downloaded locally."

    def test_download_raw_idx_from_GCP(self): ...

    def test_parse_correct_gcp_location(self): ...


class TestNCEIIngestionUserErrors:
    """A class that tests various end-user error-handling capabilities of the API."""

    def setup_class(self):
        """Used for setting up the class."""
        self.file_name = "2107RL_CW-D20210813-T220732.raw"
        self.file_type = "nc"
        self.ship_name = "Reuben_Lasker"
        self.survey_name = "RL2107"
        self.echosounder = "EK80"
        self.data_source = "TEST"
        self.file_download_location = "."
        # set up storage objects
        _, _, self.gcp_bucket = cloud_utils.setup_gcp_storage_objs()
        self.s3_client, self.s3_resource, self.s3_bucket = cloud_utils.create_s3_objs()

    def test_download_raw_file_null_file_name(self):
        """Tests the error-handling for the `download_raw_file` function when there
        is an empty `file_name` param."""
        with pytest.raises(Exception) as e:
            ingestion.download_raw_file(
                file_name="",
                file_type=self.file_type,
                ship_name=self.ship_name,
                survey_name=self.survey_name,
                echosounder=self.echosounder,
                file_download_location=self.file_download_location,
                gcp_bucket=self.gcp_bucket,
                is_metadata=False,
                debug=False,
            )

    def test_download_raw_file_null_file_type(self):
        """Tests the error-handling for the `download_raw_file` function when there
        is an empty `file_type` param."""
        with pytest.raises(Exception) as e:
            ingestion.download_raw_file(
                file_name=self.file_name,
                file_type="",
                ship_name=self.ship_name,
                survey_name=self.survey_name,
                echosounder=self.echosounder,
                file_download_location=self.file_download_location,
                gcp_bucket=self.gcp_bucket,
                is_metadata=False,
                debug=False,
            )

    def test_download_raw_file_invalid_file_type(self):
        """Tests the error-handling for the `download_raw_file` function when there
        is an invalid `file_type` param."""
        with pytest.raises(Exception) as e:
            ingestion.download_raw_file(
                file_name=self.file_name,
                file_type="abc",
                ship_name=self.ship_name,
                survey_name=self.survey_name,
                echosounder=self.echosounder,
                file_download_location=self.file_download_location,
                gcp_bucket=self.gcp_bucket,
                is_metadata=False,
                debug=False,
            )

    def test_download_raw_file_null_ship_name(self):
        """Tests the error-handling for the `download_raw_file` function when there
        is an empty `ship_name` param."""
        with pytest.raises(Exception) as e:
            ingestion.download_raw_file(
                file_name=self.file_name,
                file_type=self.file_type,
                ship_name="",
                survey_name=self.survey_name,
                echosounder=self.echosounder,
                file_download_location=self.file_download_location,
                gcp_bucket=self.gcp_bucket,
                is_metadata=False,
                debug=False,
            )

    def test_download_raw_file_null_survey_name(self):
        """Tests the error-handling for the `download_raw_file` function when there
        is an empty `survey_name` param."""
        with pytest.raises(Exception) as e:
            ingestion.download_raw_file(
                file_name=self.file_name,
                file_type=self.file_type,
                ship_name=self.ship_name,
                survey_name="",
                echosounder=self.echosounder,
                file_download_location=self.file_download_location,
                gcp_bucket=self.gcp_bucket,
                is_metadata=False,
                debug=False,
            )

    def test_download_raw_file_null_echosounder(self):
        """Tests the error-handling for the `download_raw_file` function when there
        is an empty `echosounder` param."""
        with pytest.raises(Exception) as e:
            ingestion.download_raw_file(
                file_name=self.file_name,
                file_type=self.file_type,
                ship_name=self.ship_name,
                survey_name=self.survey_name,
                echosounder="",
                file_download_location=self.file_download_location,
                gcp_bucket=self.gcp_bucket,
                is_metadata=False,
                debug=False,
            )

    def test_download_raw_file_invalid_echosounder(self):
        """Tests the error-handling for the `download_raw_file` function when there
        is an invalid `echosounder` param."""
        with pytest.raises(Exception) as e:
            ingestion.download_raw_file(
                file_name=self.file_name,
                file_type=self.file_type,
                ship_name=self.ship_name,
                survey_name=self.survey_name,
                echosounder="abc",
                file_download_location=self.file_download_location,
                gcp_bucket=self.gcp_bucket,
                is_metadata=False,
                debug=False,
            )

    def test_download_raw_file_null_file_download_location(self):
        """Tests the error-handling for the `download_raw_file` function when there
        is an empty `file_download_location` param."""
        with pytest.raises(Exception) as e:
            ingestion.download_raw_file(
                file_name=self.file_name,
                file_type=self.file_type,
                ship_name=self.ship_name,
                survey_name=self.survey_name,
                echosounder=self.echosounder,
                file_download_location="",
                gcp_bucket=self.gcp_bucket,
                is_metadata=False,
                debug=False,
            )

    def test_download_raw_file_invalid_file_download_location(self):
        """Tests the error-handling for the `download_raw_file` function when there
        is an invalid `file_download_location` param (not a dir)."""
        with pytest.raises(Exception) as e:
            # Create a test file to point the file download location to.
            with open("file.temp", "a"):
                os.utime("file.temp", None)

            ingestion.download_raw_file(
                file_name=self.file_name,
                file_type=self.file_type,
                ship_name=self.ship_name,
                survey_name=self.survey_name,
                echosounder=self.echosounder,
                file_download_location="file.temp",
                gcp_bucket=self.gcp_bucket,
                is_metadata=False,
                debug=False,
            )

    def test_download_netcdf_file_null_file_name(self):
        """Tests the error-handling for the `download_netcdf_file` function when there
        is an empty `file_name` param."""
        with pytest.raises(Exception) as e:
            ingestion.download_netcdf_file(
                file_name="",
                file_type=self.file_type,
                ship_name=self.ship_name,
                survey_name=self.survey_name,
                echosounder=self.echosounder,
                file_download_location=self.file_download_location,
                gcp_bucket=self.gcp_bucket,
                is_metadata=False,
                debug=False,
            )

    def test_download_netcdf_file_null_file_type(self):
        """Tests the error-handling for the `download_netcdf_file` function when there
        is an empty `file_type` param."""
        with pytest.raises(Exception) as e:
            ingestion.download_netcdf_file(
                file_name=self.file_name,
                file_type="",
                ship_name=self.ship_name,
                survey_name=self.survey_name,
                echosounder=self.echosounder,
                file_download_location=self.file_download_location,
                gcp_bucket=self.gcp_bucket,
                is_metadata=False,
                debug=False,
            )

    def test_download_netcdf_file_invalid_file_type(self):
        """Tests the error-handling for the `download_netcdf_file` function when there
        is an invalid `file_type` param."""
        with pytest.raises(Exception) as e:
            ingestion.download_netcdf_file(
                file_name=self.file_name,
                file_type="abc",
                ship_name=self.ship_name,
                survey_name=self.survey_name,
                echosounder=self.echosounder,
                file_download_location=self.file_download_location,
                gcp_bucket=self.gcp_bucket,
                is_metadata=False,
                debug=False,
            )

    def test_download_netcdf_file_null_ship_name(self):
        """Tests the error-handling for the `download_netcdf_file` function when there
        is an empty `ship_name` param."""
        with pytest.raises(Exception) as e:
            ingestion.download_netcdf_file(
                file_name=self.file_name,
                file_type=self.file_type,
                ship_name="",
                survey_name=self.survey_name,
                echosounder=self.echosounder,
                file_download_location=self.file_download_location,
                gcp_bucket=self.gcp_bucket,
                is_metadata=False,
                debug=False,
            )

    def test_download_netcdf_file_null_survey_name(self):
        """Tests the error-handling for the `download_netcdf_file` function when there
        is an empty `survey_name` param."""
        with pytest.raises(Exception) as e:
            ingestion.download_netcdf_file(
                file_name=self.file_name,
                file_type=self.file_type,
                ship_name=self.ship_name,
                survey_name="",
                echosounder=self.echosounder,
                file_download_location=self.file_download_location,
                gcp_bucket=self.gcp_bucket,
                is_metadata=False,
                debug=False,
            )

    def test_download_netcdf_file_null_echosounder(self):
        """Tests the error-handling for the `download_netcdf_file` function when there
        is an empty `echosounder` param."""
        with pytest.raises(Exception) as e:
            ingestion.download_netcdf_file(
                file_name=self.file_name,
                file_type=self.file_type,
                ship_name=self.ship_name,
                survey_name=self.survey_name,
                echosounder="",
                file_download_location=self.file_download_location,
                gcp_bucket=self.gcp_bucket,
                is_metadata=False,
                debug=False,
            )

    def test_download_netcdf_file_invalid_echosounder(self):
        """Tests the error-handling for the `download_netcdf_file` function when there
        is an invalid `echosounder` param."""
        with pytest.raises(Exception) as e:
            ingestion.download_netcdf_file(
                file_name=self.file_name,
                file_type=self.file_type,
                ship_name=self.ship_name,
                survey_name=self.survey_name,
                echosounder="abc",
                file_download_location=self.file_download_location,
                gcp_bucket=self.gcp_bucket,
                is_metadata=False,
                debug=False,
            )

    def test_download_netcdf_file_null_file_download_location(self):
        """Tests the error-handling for the `download_netcdf_file` function when there
        is an empty `file_download_location` param."""
        with pytest.raises(Exception) as e:
            ingestion.download_netcdf_file(
                file_name=self.file_name,
                file_type=self.file_type,
                ship_name=self.ship_name,
                survey_name=self.survey_name,
                echosounder=self.echosounder,
                file_download_location="",
                gcp_bucket=self.gcp_bucket,
                is_metadata=False,
                debug=False,
            )

    def test_download_netcdf_file_invalid_file_download_location(self):
        """Tests the error-handling for the `download_netcdf_file` function when there
        is an invalid `file_download_location` param (not a dir)."""
        with pytest.raises(Exception) as e:
            # Create a test file to point the file download location to.
            with open("file.temp", "a"):
                os.utime("file.temp", None)

            ingestion.download_netcdf_file(
                file_name=self.file_name,
                file_type=self.file_type,
                ship_name=self.ship_name,
                survey_name=self.survey_name,
                echosounder=self.echosounder,
                file_download_location="file.temp",
                gcp_bucket=self.gcp_bucket,
                is_metadata=False,
                debug=False,
            )

    def test_convert_raw_to_netcdf_null_file_name(self):
        """Tests the error-handling for the `convert_raw_to_netcdf` function when there
        is an empty `file_name` param."""
        with pytest.raises(Exception) as e:
            ingestion.convert_raw_to_netcdf(
                file_name="",
                file_type=self.file_type,
                ship_name=self.ship_name,
                survey_name=self.survey_name,
                echosounder=self.echosounder,
                data_source=self.data_source,
                file_download_location=self.file_download_location,
                gcp_bucket=self.gcp_bucket,
                is_metadata=False,
                debug=False,
            )

    def test_convert_raw_to_netcdf_null_file_type(self):
        """Tests the error-handling for the `convert_raw_to_netcdf` function when there
        is an empty `file_type` param."""
        with pytest.raises(Exception) as e:
            ingestion.convert_raw_to_netcdf(
                file_name=self.file_name,
                file_type="",
                ship_name=self.ship_name,
                survey_name=self.survey_name,
                echosounder=self.echosounder,
                data_source=self.data_source,
                file_download_location=self.file_download_location,
                gcp_bucket=self.gcp_bucket,
                is_metadata=False,
                debug=False,
            )

    def test_convert_raw_to_netcdf_invalid_file_type(self):
        """Tests the error-handling for the `convert_raw_to_netcdf` function when there
        is an invalid `file_type` param."""
        with pytest.raises(Exception) as e:
            ingestion.convert_raw_to_netcdf(
                file_name=self.file_name,
                file_type="abc",
                ship_name=self.ship_name,
                survey_name=self.survey_name,
                echosounder=self.echosounder,
                data_source=self.data_source,
                file_download_location=self.file_download_location,
                gcp_bucket=self.gcp_bucket,
                is_metadata=False,
                debug=False,
            )

    def test_convert_raw_to_netcdf_null_ship_name(self):
        """Tests the error-handling for the `convert_raw_to_netcdf` function when there
        is an empty `ship_name` param."""
        with pytest.raises(Exception) as e:
            ingestion.convert_raw_to_netcdf(
                file_name=self.file_name,
                file_type=self.file_type,
                ship_name="",
                survey_name=self.survey_name,
                echosounder=self.echosounder,
                data_source=self.data_source,
                file_download_location=self.file_download_location,
                gcp_bucket=self.gcp_bucket,
                is_metadata=False,
                debug=False,
            )

    def test_convert_raw_to_netcdf_null_survey_name(self):
        """Tests the error-handling for the `convert_raw_to_netcdf` function when there
        is an empty `survey_name` param."""
        with pytest.raises(Exception) as e:
            ingestion.convert_raw_to_netcdf(
                file_name=self.file_name,
                file_type=self.file_type,
                ship_name=self.ship_name,
                survey_name="",
                echosounder=self.echosounder,
                data_source=self.data_source,
                file_download_location=self.file_download_location,
                gcp_bucket=self.gcp_bucket,
                is_metadata=False,
                debug=False,
            )

    def test_convert_raw_to_netcdf_null_echosounder(self):
        """Tests the error-handling for the `convert_raw_to_netcdf` function when there
        is an empty `echosounder` param."""
        with pytest.raises(Exception) as e:
            ingestion.convert_raw_to_netcdf(
                file_name=self.file_name,
                file_type=self.file_type,
                ship_name=self.ship_name,
                survey_name=self.survey_name,
                echosounder="",
                data_source=self.data_source,
                file_download_location=self.file_download_location,
                gcp_bucket=self.gcp_bucket,
                is_metadata=False,
                debug=False,
            )

    def test_convert_raw_to_netcdf_invalid_echosounder(self):
        """Tests the error-handling for the `convert_raw_to_netcdf` function when there
        is an invalid `echosounder` param."""
        with pytest.raises(Exception) as e:
            ingestion.convert_raw_to_netcdf(
                file_name=self.file_name,
                file_type=self.file_type,
                ship_name=self.ship_name,
                survey_name=self.survey_name,
                echosounder="abc",
                data_source=self.data_source,
                file_download_location=self.file_download_location,
                gcp_bucket=self.gcp_bucket,
                is_metadata=False,
                debug=False,
            )

    def test_convert_raw_to_netcdf_null_data_source(self):
        """Tests the error-handling for the `convert_raw_to_netcdf` function when there
        is an empty `data_source` param."""
        with pytest.raises(Exception) as e:
            ingestion.convert_raw_to_netcdf(
                file_name=self.file_name,
                file_type=self.file_type,
                ship_name=self.ship_name,
                survey_name=self.survey_name,
                echosounder=self.echosounder,
                data_source="",
                file_download_location=self.file_download_location,
                gcp_bucket=self.gcp_bucket,
                is_metadata=False,
                debug=False,
            )

    def test_convert_raw_to_netcdf_null_file_download_location(self):
        """Tests the error-handling for the `convert_raw_to_netcdf` function when there
        is an empty `file_download_location` param."""
        with pytest.raises(Exception) as e:
            ingestion.convert_raw_to_netcdf(
                file_name=self.file_name,
                file_type=self.file_type,
                ship_name=self.ship_name,
                survey_name=self.survey_name,
                echosounder=self.echosounder,
                data_source=self.data_source,
                file_download_location="",
                gcp_bucket=self.gcp_bucket,
                is_metadata=False,
                debug=False,
            )

    def test_convert_raw_to_netcdf_invalid_file_download_location(self):
        """Tests the error-handling for the `convert_raw_to_netcdf` function when there
        is an invalid `file_download_location` param (not a dir)."""
        with pytest.raises(Exception) as e:
            # Create a test file to point the file download location to.
            with open("file.temp", "a"):
                os.utime("file.temp", None)

            ingestion.convert_raw_to_netcdf(
                file_name=self.file_name,
                file_type=self.file_type,
                ship_name=self.ship_name,
                survey_name=self.survey_name,
                echosounder=self.echosounder,
                data_source=self.data_source,
                file_download_location="file.temp",
                gcp_bucket=self.gcp_bucket,
                is_metadata=False,
                debug=False,
            )

    def teardown_class(self):
        """Tearsdown any temporary files, variables, or anything that was used for testing."""
        os.remove("file.temp")
