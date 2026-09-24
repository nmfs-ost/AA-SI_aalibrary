"""This file contains code pertaining to auxiliary functions related to parsing
through the Ocean Data Lake (ODL) Azure storage bucket."""

from typing import List

# For pytests-sake
if __package__ is None or __package__ == "":
    from config import OCEAN_DATA_LAKE_STORAGE_ACCOUNT_URL
    from utils.cloud_utils import (
        get_current_odl_credentials,
        get_odl_blob_service_client,
    )
else:
    from aalibrary.config import OCEAN_DATA_LAKE_STORAGE_ACCOUNT_URL
    from aalibrary.utils.cloud_utils import (
        get_current_odl_credentials,
        get_odl_blob_service_client,
    )


def list_odl_containers() -> List[str]:
    """Lists all containers in the Ocean Data Lake (ODL) using the Interactive
    Browser Broker method. This will open a browser window for the user to sign
    in with their credentials. The credentials will be cached as an environment
    variable for future use.

    Returns:
        List[str]: A list of container names in the ODL.
    """

    client = get_odl_blob_service_client()

    containers = [container.name for container in client.list_containers()]

    return containers


if __name__ == "__main__":
    # For pytests-sake
    print(list_odl_containers())