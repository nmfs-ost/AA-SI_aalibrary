"""This file contains functions for interacting with derived products in GCS."""

from typing import List
from aalibrary import config
from aalibrary.utils.helpers import (
    get_current_gcp_user_email,
    parse_correct_gcp_storage_bucket_location,
    normalize_ship_name,
)
from aalibrary.utils.cloud_utils import (
    list_all_objects_in_gcp_bucket_location,
    setup_gcp_storage_objs,
    check_if_file_exists_in_gcp,
)
from aalibrary.egress import upload_file_to_gcp_storage_bucket
from aalibrary.metadata import create_and_upload_metadata_df_for_derived_files
from aalibrary.ices_ship_names import get_ices_code_from_ship_name


def get_all_derived_products_for_current_user(
    user_email: str = "",
) -> List[str]:
    """Gets a list of URIs for all derived products for the current user (
    based on provided email, or the user that is logged into `gcloud`).

    Args:
        user_email (str, optional): The user email as a string. Defaults to "".

    Returns:
        List[str]: A list of URIs of all the derived products for the current
            user.
    """

    if user_email == "":
        user_name = get_current_gcp_user_email()
        user_name = user_name.split("@")[0]
    else:
        user_name = user_email.split("@")[0]

    blob_path = f"derived_products/{user_name}/"

    return list_all_objects_in_gcp_bucket_location(
        location=blob_path, bucket_name=config.get_current_gcp_bucket_name()
    )


def search_derived_products_in_gcp(): ...


def get_all_submission_save_states_for_current_user(
    user_email: str = "",
) -> List[str]:
    """Gets a list of URIs for all submission save states for the current user
    (the one that is logged into gcloud).

    Args:
        user_email (str, optional): The user email as a string. Defaults to "".

    Returns:
        dict: A dictionary where the keys are the survey_names that have
            submission save states available, and the values are the GCS paths.
    """

    if user_email == "":
        user_name = get_current_gcp_user_email()
        user_name = user_name.split("@")[0]
    else:
        user_name = user_email.split("@")[0]

    blob_path = f"derived_products/{user_name}/"

    all_objects_in_users_derived_products = (
        list_all_objects_in_gcp_bucket_location(
            location=blob_path,
            bucket_name=config.get_current_gcp_bucket_name(),
        )
    )
    all_submission_save_states = {}
    for obj in all_objects_in_users_derived_products:
        if obj.endswith("tugboat_submission_save_state.json"):
            # Get the ship folder.
            survey_name = obj.split("/")[-2]
            all_submission_save_states[survey_name] = obj

    return all_submission_save_states


def upload_derived_product_to_gcp(
    file_name: str = "",
    file_type: str = "",
    file_location: str = "",
    ship_name: str = "",
    survey_name: str = "",
    echosounder: str = "",
    user_email: str = "",
    data_source: str = "",
    debug: bool = False,
):
    """Uploads a derived product file to GCP as well as its metadata to BQ.

    Args:
        file_name (str, optional): The name of the file. Defaults to "".
        file_type (str, optional): The file type. Defaults to "".
        file_location (str, optional): The local file path. Defaults to "".
        ship_name (str, optional): The ship name associated with this file.
            Defaults to "".
        survey_name (str, optional): The survey name associated with this file.
            Defaults to "".
        echosounder (str, optional): The echosounder associated with this file.
            Defaults to "".
        user_email (str, optional): The user email as a string. Defaults to "".
        data_source (str, optional): The data source associated with this file.
            Defaults to "".
        debug (bool, optional): Whether or not to print debug statements.
            Defaults to False.
    """

    print("Uploading derived file...")

    # Get the gcp storage bucket location
    gcp_storage_bucket_location = parse_correct_gcp_storage_bucket_location(
        file_name=file_name,
        file_type=file_type,
        ship_name=ship_name,
        survey_name=survey_name,
        echosounder=echosounder,
        data_source=data_source,
        user_email=user_email,
        is_derived_product=True,
        debug=debug,
    )

    # Normalize the ship name
    ship_name_normalized = normalize_ship_name(ship_name=ship_name)

    # Get the ICES code
    ices_code = get_ices_code_from_ship_name(
        ship_name=ship_name_normalized, is_normalized=True
    )

    # Create storage objects
    _, gcp_bucket_name, gcp_bucket = setup_gcp_storage_objs()

    # Upload to the GCP location
    upload_file_to_gcp_storage_bucket(
        file_name=file_name,
        file_type="raw",
        ship_name=ship_name_normalized,
        survey_name=survey_name,
        echosounder=echosounder,
        file_location=file_location,
        gcp_bucket=gcp_bucket,
        data_source=data_source,
        is_derived_product=True,
        verbose=False,
        debug=debug,
    )

    # Check that the file exists
    file_exists_in_gcp = check_if_file_exists_in_gcp(
        bucket=gcp_bucket,
        file_path=gcp_storage_bucket_location,
    )

    # Upload associated metadata to BQ.
    create_and_upload_metadata_df_for_derived_files(
        file_name=file_name,
        survey_name=survey_name,
        gcp_bucket_name=gcp_bucket_name,
        gcp_storage_bucket_location=gcp_storage_bucket_location,
        ices_code=ices_code,
        ship_name=ship_name,
        echosounder=echosounder,
        user_email=user_email,
        file_exists_in_gcp=file_exists_in_gcp,
        debug=debug,
    )

    print("Uploaded.")


if __name__ == "__main__":
    file_path = r"C:\Users\Hannah Khan\Desktop\repos\AA-SI_aalibrary\HDD\Henry_B_Bigelow\HB2407\Derived\img.png"

    # upload_derived_product_to_gcp(
    #     file_name="img.png",
    #     file_type="png",
    #     file_location=file_path,
    #     ship_name="Henry B. Bigelow",
    #     survey_name="HB2407",
    #     echosounder="EK80",
    #     data_source="HDD",
    #     debug=False,
    # )

    print(get_all_derived_products_for_current_user())
