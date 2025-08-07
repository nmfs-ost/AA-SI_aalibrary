"""This file contains functions to interface with the Tugboat API."""

import sys
import os
import json
import tempfile

# For pytests-sake
if __package__ is None or __package__ == "":
    # uses current directory visibility
    from utils import cloud_utils
else:
    # uses current package visibility
    from aalibrary.utils import cloud_utils


class TugboatAPI:
    """
    This class provides methods to interact with the Tugboat API.
    """

    project_id: str = "ggn-nmfs-aa-dev-1"
    gcp_bucket_name: str = "ggn-nmfs-aa-dev-1-data"

    def __init__(self, **kwargs):
        """
        Initializes the TugboatAPI class.
        """
        self.__dict__.update(kwargs)
        self._create_vars()
        self._get_tugboat_credentials()

    def _create_vars(self):
        """Creates vars for use later."""

        # Create google connection objects to download the Tugboat creds
        if (
            ("gcp_stor_client" not in self.__dict__)
            or ("gcp_bucket_name" not in self.__dict__)
            or ("gcp_bucket" not in self.__dict__)
        ):
            self.gcp_stor_client, self.gcp_bucket_name, self.gcp_bucket = (
                cloud_utils.setup_gcp_storage_objs(
                    project_id=self.project_id, gcp_bucket_name=self.gcp_bucket_name
                )
            )

    def _get_tugboat_credentials(self):
        """Gets the tugboat credentials for AALibrary from the google storage bucket."""
        local_creds_file_path = r"./tugboat_credentials.json"

        # Download as string so that we dont have to worry about storing it anywhere
        self._tugboat_cred = cloud_utils.download_file_from_gcp_as_string(
            gcp_bucket=self.gcp_bucket,
            blob_file_path="other/tugboat_creds",
            debug=False,
        )


if __name__ == "__main__":
    tb_api = TugboatAPI()
    print(tb_api._tugboat_cred)
