"""Contains quick tests for the API to verify that the connections are working as intended.
Also checks to see if a raw file can be downloaded."""

from app.utils import cloud_utils

_, _, _ = cloud_utils.setup_gcp_storage_objs()

