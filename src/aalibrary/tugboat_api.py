"""This file contains functions to interface with the Tugboat API."""

import sys
import os
import json
import tempfile
import urllib
import requests

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
    empty_submission_file_path: str = "other/tugboat_empty_submission.json"
    _tugboat_cred: str = None
    tugboat_api_url: str = "https://ccog.colorado.edu/tugboat/api/v1/"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    def __init__(self, **kwargs):
        """
        Initializes the TugboatAPI class.
        """
        self.__dict__.update(kwargs)
        self._create_vars()
        self._set_tugboat_credentials()

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
                    project_id=self.project_id,
                    gcp_bucket_name=self.gcp_bucket_name,
                )
            )

    def _set_tugboat_credentials(self):
        """Sets the tugboat credentials for AALibrary from the google storage
        bucket as an environment var."""

        # Download as string so that we dont have to worry about storing it
        # anywhere
        os.environ["TUGBOAT_CREDENTIALS"] = (
            cloud_utils.download_file_from_gcp_as_string(
                gcp_bucket=self.gcp_bucket,
                blob_file_path="other/tugboat_creds",
            )
        )
        self._tugboat_cred = os.environ.get("TUGBOAT_CREDENTIALS")
        self.headers["Authorization"] = f"Bearer {self._tugboat_cred}"

    def _get_request_as_json(self, url: str) -> dict:
        """Helper function to make a GET request and return the response as JSON."""
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            # Raise an HTTPError for bad responses(4xx or 5xx)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error during GET request: {e}")


    def create_empty_submission_file(
        self, file_download_directory: str = ".", file_name: str = ""
    ):
        """Creates an empty submission file at the file_download_location.

        Args:
            file_download_directory (str, optional): The directory where you
                want to download the file. Defaults to ".".
            file_name (str, optional): _description_. Defaults to "".
        """

        # Normalize paths
        file_download_directory = (
            os.path.normpath(file_download_directory) + os.sep
        )
        # Convert locations into directories as needed.
        if file_download_directory != ".":
            # Edge-case: when dirname is passed ".", it responds with ""
            file_download_directory = (
                os.path.dirname(file_download_directory) + os.sep
            )
        file_download_path = os.path.normpath(
            os.sep.join([file_download_directory, file_name])
        )

        # Read empty submission file
        with open(self.empty_submission_file_path, "r", encoding="utf-8") as f:
            empty_submission = json.load(f)

        # Write empty submission file to the specified path
        with open(file_download_path, "w", encoding="utf-8") as f:
            json.dump(empty_submission, f, indent=4)

        print(f"Empty submission file created at: {file_download_path}")
        print("Please update the file with your submission details.")
        return file_download_path

    def get_all_platforms(self):
        """Fetches all platforms from the Tugboat API."""
        url = urllib.parse.urljoin(self.tugboat_api_url, "platforms")
        return self._get_request_as_json(url)

    def get_all_instruments(self):
        """Fetches all instruments from the Tugboat API."""
        url = urllib.parse.urljoin(self.tugboat_api_url, "instruments")
        return self._get_request_as_json(url)

    def post_new_submission(self, submission_json_file_path: str = ""):
        """Posts a new submission to the Tugboat API."""

        # Create the URL for the submission endpoint
        url = urllib.parse.urljoin(self.tugboat_api_url, "submissions")
        # Read the submission JSON file
        with open(submission_json_file_path, "r", encoding="utf-8") as f:
            submission_payload = json.load(f)

        response = requests.post(
            url, headers=self.headers, json=submission_payload, timeout=10
        )
        # Checking the response status code
        if response.status_code == 201:  # 201 Created for successful POST
            print("POST request successful!")
            print("Response JSON:")
            print(response.json())
        else:
            print(
                f"POST request failed with status code: {response.status_code}"
            )
            print(response.text)

    def get_all_submissions(self):
        """Fetches all submissions from the Tugboat API."""
        url = urllib.parse.urljoin(self.tugboat_api_url, "submissions")
        all_submissions = self._get_request_as_json(url)
        return all_submissions['items']

    def get_submission_by_id(self, submission_id: str):
        """Fetches a submission by its ID from the Tugboat API."""
        url = urllib.parse.urljoin(
            self.tugboat_api_url, f"submissions/{submission_id}"
        )
        return self._get_request_as_json(url)


if __name__ == "__main__":
    tb_api = TugboatAPI()
    # tb_api.create_empty_submission_file(
    #     file_download_directory=".",
    #     file_name="tugboat_test_submission.json",
    # )
    # tb_api.get_all_platforms()
    # tb_api.get_all_instruments()
    # tb_api.post_new_submission(
    #     submission_json_file_path="./tugboat_test_submission.json"
    # )
    # tb_api.get_submission_by_id(submission_id="test0")
    tb_api.get_all_submissions()
