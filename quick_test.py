"""Contains quick tests for the API to verify that the connections are working as intended.
Also checks to see if a raw file can be downloaded."""

from src.aalibrary.utils import cloud_utils
from src.aalibrary import ingestion

# CONNECTION TEST: set up storage objects
_, _, gcp_bucket = cloud_utils.setup_gcp_storage_objs()
s3_client, s3_resource, s3_bucket = cloud_utils.create_s3_objs()

# FUNCTIONAL TEST: download a raw file
file_name = "2107RL_CW-D20210813-T220732.raw"
file_name_idx = "2107RL_CW-D20210813-T220732.idx"
file_type = "raw"
ship_name = "Reuben_Lasker"
survey_name = "RL2107"
echosounder = "EK80"
data_source = "TEST"
file_download_location = "."
is_metadata = False
ingestion.download_raw_file(
    file_name=file_name,
    file_type=file_type,
    ship_name=ship_name,
    survey_name=survey_name,
    echosounder=echosounder,
    data_source=data_source,
    file_download_location=file_download_location,
    is_metadata=False,
    debug=True,
)
