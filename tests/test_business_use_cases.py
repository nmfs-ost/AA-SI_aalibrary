# pylint: disable=attribute-defined-outside-init
"""For testing the business-use cases. These consist of series of functions
that emulate how users would use AALibrary usually."""

import os
import pytest

from aalibrary.raw_file import RawFile
from aalibrary.utils import cloud_utils
from aalibrary import ingestion
from aalibrary import conversion
from aalibrary import tugboat_api
from aalibrary import utils


class TestBusinessUseCases:
    """Class for testing the business-use-cases."""

    def setup_class(self):
        """Used for setting up the class."""
        self.s3_bucket_name = "noaa-wcsd-pds"
        try:
            self.s3_client, self.s3_resource, self.s3_bucket = (
                utils.cloud_utils.create_s3_objs()
            )
        except Exception as e:
            print("CANNOT ESTABLISH CONNECTION TO S3 BUCKET..\n%s", e)
            raise
        # Create gcp bucket objects
        self.gcp_stor_client, self.gcp_bucket_name, self.gcp_bucket = (
            utils.cloud_utils.setup_gcp_storage_objs()
        )
        # RawFile object for testing NCEI
        self.rf = RawFile(
            file_name="2107RL_CW-D20210916-T165047.raw",
            file_type="raw",
            ship_name="reuben_lasker",
            survey_name="RL2107",
            echosounder="EK80",
            data_source="NCEI",
            file_download_directory="./test_data_dir",
            is_metadata=False,
            debug=True,
            s3_bucket=self.s3_bucket,
            s3_resource=self.s3_resource,
            s3_bucket_name=self.s3_bucket_name,
            gcp_bucket=self.gcp_bucket,
            gcp_bucket_name=self.gcp_bucket_name,
            gcp_stor_client=self.gcp_stor_client,
        )
        # RawFile object for testing OMAO
        self.rf_omao = RawFile(
            file_name="1601RL-D20160107-T074016.raw",
            file_type="raw",
            ship_name="Reuben_Lasker",
            survey_name="RL1601",
            echosounder="EK60",
            data_source="OMAO",
            file_download_directory="./test_data_dir",
            is_metadata=False,
            debug=True,
            s3_bucket=self.s3_bucket,
            s3_resource=self.s3_resource,
            s3_bucket_name=self.s3_bucket_name,
            gcp_bucket=self.gcp_bucket,
            gcp_bucket_name=self.gcp_bucket_name,
            gcp_stor_client=self.gcp_stor_client,
        )

    def teardown_class(self):
        """Tears-down any temporary files, variables, or anything that was used
        for testing."""
        # Delete the temporary test data files
        temp_files = [
            self.rf.raw_file_download_path,
            self.rf.netcdf_file_download_path,
            self.rf_omao.raw_file_download_path,
            self.rf_omao.netcdf_file_download_path,
        ]
        for file in temp_files:
            if os.path.exists(file):
                os.remove(file)

    def test_download_from_ncei_and_convert(self):
        """Tests downloading a file from NCEI and converting it to a netCDF."""
        # Download the file
        ingestion.download_raw_file_from_ncei(
            file_name=self.rf.file_name,
            file_type=self.rf.file_type,
            ship_name=self.rf.ship_name,
            survey_name=self.rf.survey_name,
            echosounder=self.rf.echosounder,
            data_source=self.rf.data_source,
            file_download_directory=self.rf.file_download_directory,
            upload_to_gcp=False,
            debug=self.rf.debug,
        )

        # Convert the file to a netCDF and upload it to GCP & delete raw
        conversion.convert_local_raw_to_netcdf(
            raw_file_location=self.rf.raw_file_download_path,
            netcdf_file_download_directory=self.rf.file_download_directory,
            echosounder=self.rf.echosounder,
            overwrite=False,
            delete_raw_after=True,
        )

    def test_download_cached_netcdf_file(self):
        """Tests the downloading of a cached netCDF file from GCP."""

        # Download, convert, and upload the netcdf the file first
        conversion.convert_raw_to_netcdf(
            file_name=self.rf.file_name,
            file_type=self.rf.file_type,
            ship_name=self.rf.ship_name,
            survey_name=self.rf.survey_name,
            echosounder=self.rf.echosounder,
            data_source=self.rf.data_source,
            file_download_directory=self.rf.file_download_directory,
            overwrite=True,
            delete_raw_after=True,
            gcp_bucket=self.rf.gcp_bucket,
            is_metadata=False,
            debug=self.rf.debug,
        )

        # Download the file from GCP
        ingestion.download_netcdf_file(
            raw_file_name=self.rf.file_name,
            file_type="netcdf",
            ship_name=self.rf.ship_name,
            survey_name=self.rf.survey_name,
            echosounder=self.rf.echosounder,
            data_source=self.rf.data_source,
            file_download_directory=self.rf.file_download_directory,
            gcp_bucket=self.rf.gcp_bucket,
            debug=self.rf.debug,
        )

    def test_download_from_omao_and_convert(self):
        """Tests downloading a file from OMAO and converting it to a netCDF."""
        # Download the file
        ingestion.download_raw_file_from_azure(
            file_name=self.rf_omao.file_name,
            file_type=self.rf_omao.file_type,
            ship_name=self.rf_omao.ship_name,
            survey_name=self.rf_omao.survey_name,
            echosounder=self.rf_omao.echosounder,
            data_source=self.rf_omao.data_source,
            file_download_directory=self.rf_omao.file_download_directory,
            config_file_path="./azure_config.ini",
            upload_to_gcp=False,
            debug=self.rf_omao.debug,
        )
        # Convert the file to a netCDF and upload it to GCP & delete raw
        conversion.convert_local_raw_to_netcdf(
            raw_file_location=self.rf_omao.raw_file_download_path,
            netcdf_file_download_directory=self.rf_omao.file_download_directory,
            echosounder=self.rf_omao.echosounder,
            overwrite=False,
            delete_raw_after=True,
        )

    # def test_tugboat_metadata_creation_and_upload(self):
    #     """Tests the creation and upload of Tugboat metadata for a netCDF
    #     file."""
    #     # Create Tugboat metadata for a netCDF file

    #     # Clean up temporary files
