"""This file contains the functions utilized by the metadata UI for interacting
with tugboat submissions and more within GCS. These should all be helpers
functions.
"""

from typing import Union

import json

from google.cloud import storage

from aalibrary import config

# For pytests-sake
# if __package__ is None or __package__ == "":
#     # from config import get_current_gcp_bucket_name
#     from utils.helpers import (
#         parse_correct_gcp_storage_bucket_location,
#     )
#     from metadata import create_and_upload_metadata_df_for_derived_files
#     from ices_ship_names import get_ices_code_from_ship_name
# else:
from aalibrary.config import get_current_gcp_bucket_name
from aalibrary.utils.helpers import (
    parse_correct_gcp_storage_bucket_location,
    normalize_ship_name,
)
from aalibrary.metadata import (
    create_and_upload_metadata_df_for_derived_files,
)
from aalibrary.ices_ship_names import get_ices_code_from_ship_name
from aalibrary.derived import get_all_submission_save_states_for_current_user


def upload_submission_save_state_to_gcs(
    submission: Union[str, dict] = None,
    user_email: str = "",
    debug: bool = False,
):
    """Uploads the submission save state to the current GCS bucket.

    Args:
        submission (Union[str, dict], optional): The submission dict, if a file
            path has been provided, then the dict will be loaded from the file.
            Defaults to None.
        user_email (str, optional): The user email as a string. Defaults to "".
        debug (bool, optional): Whether or not to print debug statements.
            Defaults to False.

    Raises:
        ValueError: If no submission is provided, or if the required fields
            `ship_name` or `platform`, and `cruiseId` are not provided in the
            submission.
    """

    # Error-checking
    if submission is None:
        raise ValueError("No submission provided for upload.")
    if submission.get("ship_name", submission.get("platform", "")) == "":
        raise ValueError(
            "The `ship_name` or `platform` field is required but not provided."
        )
    if submission.get("cruiseId", "") == "":
        raise ValueError("The `cruiseId` field is required but not provided.")

    if isinstance(submission, str):
        # Load the submission dict from the file path
        with open(submission, "r", encoding="utf-8") as f:
            submission = json.load(f)

    # Get the current GCP bucket name
    bucket_name = get_current_gcp_bucket_name()

    # Create a file name for the save state.
    file_name = "tugboat_submission_save_state.json"

    # Get the normalized ship name for the submission
    ship_name = submission.get("ship_name", submission.get("platform", ""))
    ship_name_normalized = normalize_ship_name(ship_name)

    # Get the survey name
    survey_name = submission.get("cruiseId", "").upper()

    # Get the ICES code from the ship name in the submission
    ices_code = get_ices_code_from_ship_name(
        ship_name_normalized,
        is_normalized=True,
    )

    # Define the GCS path for the submission save state
    gcs_path = parse_correct_gcp_storage_bucket_location(
        file_name=file_name,
        file_type="json",
        ship_name=ship_name_normalized,
        survey_name=survey_name,
        is_tugboat_submission=True,
        debug=debug,
    )

    # Upload the submission save state to GCS
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)

    # Convert the submission dict to JSON string
    submission_json = json.dumps(submission)

    # Upload the JSON string to GCS
    blob.upload_from_string(submission_json, content_type="application/json")

    # Check if the file exists in GCS
    file_exists_in_gcp = blob.exists()

    # Create and upload metadata for the submission save state to BigQuery
    create_and_upload_metadata_df_for_derived_files(
        file_name=file_name,
        survey_name=survey_name,
        gcp_bucket_name=bucket_name,
        gcp_storage_bucket_location=gcs_path,
        ices_code=ices_code,
        ship_name=ship_name_normalized,
        user_email=user_email,
        file_exists_in_gcp=file_exists_in_gcp,
        debug=debug,
    )

    print(f"Submission save state uploaded to {gcs_path}.")


def get_all_save_states_for_user() -> dict:
    """Gets all of the submission save states for the current user from GCS.

    Returns:
        dict: A dictionary where the keys are the survey_names that have
            submission save states available, and the values are the GCS paths.
    """

    return get_all_submission_save_states_for_current_user()


def get_users_save_state_for_survey(survey_name: str = "") -> Union[dict, None]:
    """Gets the submission save state for the current user for a specific
    survey from GCS.

    Args:
        survey_name (str, optional): The survey name string. Defaults to "".

    Returns:
        dict: The submission save state dict if one has been found. Returns
            None if no save state has been found for the survey.
    """

    survey_name = survey_name.upper()
    all_save_states = get_all_save_states_for_user()
    save_state_gcs_path = all_save_states.get(survey_name, "")
    # Download the save state as a JSON
    if save_state_gcs_path:
        client = storage.Client()
        bucket_name = get_current_gcp_bucket_name()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(save_state_gcs_path)
        save_state_json = blob.download_as_text()
        save_state_dict = json.loads(save_state_json)
        return save_state_dict
    else:
        return {}


if __name__ == "__main__":
    # config.use_gcp_dev()
    # Example usage
    example_submission = {
        "platform": "Henry B. Bigelow",
        "cruiseId": "HB2407",
        "comments": "This is an example submission for testing purposes.",
    }
    upload_submission_save_state_to_gcs(
        submission=example_submission,
        debug=True,
    )
    print(get_users_save_state_for_survey("HB2407"))
