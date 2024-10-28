"""Contains quick tests for the API to verify that the connections are working as intended.
Also checks to see if a raw file can be downloaded."""

from app.utils import cloud_utils
from app import ingestion

_, _, _ = cloud_utils.setup_gcp_storage_objs()

# download a raw file
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
    force_download_from_ncei=False,
    debug=True,
)
