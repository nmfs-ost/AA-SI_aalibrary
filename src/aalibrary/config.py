"""Used for storing environment-specific settings such as database URIs and
such.
Also contains functions for setting environment variables to use different GCP
resources."""

import sys
import os

from loguru import logger

logger.remove()  # remove default handler so you're explicit
logger.add(sys.stderr, level="INFO")

GCP_DEV_PROJECT_ID = "ggn-nmfs-aa-dev-1"
GCP_PROD_PROJECT_ID = "ggn-nmfs-aa-prod-1"
GCP_DEV_BUCKET_NAME = "ggn-nmfs-aa-dev-1-data"
GCP_PROD_BUCKET_NAME = "ggn-nmfs-aa-prod-1-data"

RAW_DATA_FILE_TYPES = ["raw", "idx", "bot"]
CONVERTED_DATA_FILE_TYPES = ["netcdf", "nc"]
METADATA_FILE_TYPES = ["json"]
VALID_FILETYPES = ["raw", "idx", "netcdf", "nc", "json", "bot"]

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
VALID_DATA_SOURCES = ["NCEI", "OMAO", "HDD", "TEST"]


def use_gcp_dev():
    """Sets environment variables to use GCP development resources."""
    os.environ["AALIBRARY_GCP_PROJECT_ID"] = GCP_DEV_PROJECT_ID
    os.environ["AALIBRARY_GCP_BUCKET_NAME"] = GCP_DEV_BUCKET_NAME
    logger.debug("You are now using the GCP Dev environment.")


def use_gcp_prod():
    """Sets environment variables to use GCP production resources."""
    os.environ["AALIBRARY_GCP_PROJECT_ID"] = GCP_PROD_PROJECT_ID
    os.environ["AALIBRARY_GCP_BUCKET_NAME"] = GCP_PROD_BUCKET_NAME
    logger.debug("You are now using the GCP Prod environment.")
