import os
from datetime import datetime
import requests
import pprint
import json
import base64
from google.cloud import pubsub_v1
from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from google.auth.transport.requests import Request
from google.oauth2 import id_token


PROJECT_ID = "ggn-nmfs-aa-dev-1"
REGION = "us-east4"
FUNCTION_URL = (
    "https://tugboat-archival-notifier-465755541677.us-east4.run.app"
)
ENDPOINT_ROUTE = "tugboat-submission-function"
# Dynamically find the directory where THIS DAG file lives
DAG_DIR = os.path.dirname(os.path.abspath(__file__))
TUGBOAT_ERROR_EMAIL_TEMPLATE_PATH = os.path.join(
    DAG_DIR, "tugboat_error_submission_template.html"
)
TUGBOAT_SUCCESSFUL_EMAIL_TEMPLATE_PATH = os.path.join(
    DAG_DIR, "tugboat_successful_submission_template.html"
)


# Define your default parameter values and validation types
PARAMS = {
    "data_submitter_name": Param(
        default="Enter Your Name Here.",
        type="string",
        description="The data submitter name. This name will be used in the"
        " submission for Tugboat as the point of contact for this dataset.",
    ),
    "data_submitter_email": Param(
        default="Enter Your Email Here.",
        type="string",
        description="The data submitter email. This email will be sent a"
        " notification with the results of the submission.",
    ),
    "other_people_to_notify_emails": Param(
        default="Enter Emails of Others to Notify Here.",
        type="string",
        description="The emails of the all other people to notify. Separate "
        "using commas. Ex. user1@example.com,user2@example.com. "
        "These emails will be sent a notification with the results of the "
        "submission. They will be attached as CC.",
    ),
    "project_title": Param(
        default="Enter The Project Title Here.",
        type="string",
        description="This should exactly match the `Working Project Title`"
        " within the project's metadata PDF sheet. This is the title of the"
        " project that the data submission will be associated with. If the"
        " project does not exist, you must create it first in Tugboat before"
        " submitting the data.",
    ),
    "deployment_code": Param(
        default="Enter The Deployment Code Here.",
        type="string",
        description="The deployment code for the dataset. Must match exactly"
        " with the deployment code in Makara/BQ.",
    ),
    "use_tugboat_dev": Param(
        default=True,  # TODO: set to false when ready.
        type="boolean",
        description="Whether to use the Tugboat development environment.",
    ),
}


def _print_raise_for_status(response):
    """Prints the details of:`HTTPError`, if one occurred."""

    http_error_msg = ""
    if isinstance(response.reason, bytes):
        # We attempt to decode utf-8 first because some servers
        # choose to localize their reason strings. If the string
        # isn't utf-8, we fall back to iso-8859-1 for all other
        # encodings. (See PR #3538)
        try:
            reason = response.reason.decode("utf-8")
        except UnicodeDecodeError:
            reason = response.reason.decode("iso-8859-1")
    else:
        reason = response.reason

    if 400 <= response.status_code < 500:
        http_error_msg = f"{response.status_code} Client Error: {reason} for url: {response.url}"

    elif 500 <= response.status_code < 600:
        http_error_msg = f"{response.status_code} Server Error: {reason} for url: {response.url}"

    if http_error_msg:
        print(http_error_msg)


@task(task_id="invoking_cloud_run_function")
def invoke_cloud_run_function(**context):
    """This task is used to trigger a CloudRun function that will create the
    deployment's submission JSON and submit it using the PAMTugboatAPI."""

    # # Get ADC
    # credentials, project = google.auth.default()
    # # Create authorized session using a ID token.
    # authed_session = AuthorizedSession(credentials)
    # Fetch the ID token using the Airflow worker's default service account
    # credentials
    # Ensure your worker's service account has the 'Cloud Run Invoker' role
    auth_req = Request()
    token = id_token.fetch_id_token(auth_req, audience=FUNCTION_URL)

    # Construct authenticated HTTP headers
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    payload = {
        "project_id": context.get("project_id", "ggn-nmfs-pacm-dev-1"),
        "dataset_type": context.get("dataset_type", "audio"),
        "deployment_code": context["params"]["deployment_code"],
        "project_title": context["params"]["project_title"],
        "data_submitter_name": context["params"]["data_submitter_name"],
        "data_submitter_email": context["params"]["data_submitter_email"],
        "other_people_to_notify_emails": context["params"][
            "other_people_to_notify_emails"
        ],
        "use_tugboat_dev": context["params"]["use_tugboat_dev"],
    }

    # Make the authenticated POST request
    cloud_run_response = requests.post(
        FUNCTION_URL, headers=headers, json=payload
    )
    print("response.json().keys()", cloud_run_response.json().keys())
    print("response that dag receives:", cloud_run_response.text)
    _print_raise_for_status(cloud_run_response)
    # Error handling if there is no json in the response body.
    cloud_run_response = cloud_run_response.json()
    return_package = {
        "submission_json": cloud_run_response.get("submission_json", {}),
        "response_status_code": cloud_run_response.get(
            "response_status_code", None
        ),
        "response_text": cloud_run_response.get("response_text", None),
        "request_params": cloud_run_response.get("request_params", {}),
        "response_json": cloud_run_response.get("response_json", {}),
    }
    # Returning the package back to the HTTP caller (Airflow)
    return return_package


@task(task_id="create_email_str_task")
def create_email_str(response):
    if not response:
        return {
            "email_str": "No response found or an error has occurred with `tugboat-submission-dag`."
        }
    else:
        if response.get("response_json", None) is not None:
            # An error has been returned from the Tugboat API
            # The errors are contained within response_json
            # load the error email HTML string
            with open(
                TUGBOAT_ERROR_EMAIL_TEMPLATE_PATH,
                "r",
                encoding="utf-8",
            ) as f:
                email_html = f.read()
            response_json_pp = pprint.pformat(
                response["response_json"], indent=1, width=200,
            )
            email_html = email_html.replace("{{errors_str}}", response_json_pp)
        else:
            # Everything worked out fine, no error has been received.
            with open(
                TUGBOAT_SUCCESSFUL_EMAIL_TEMPLATE_PATH,
                "r",
                encoding="utf-8",
            ) as f:
                email_html = f.read()

        # Parse through everything else, and return the html email string.
        submission_json_pp = pprint.pformat(
            response["submission_json"], indent=1, width=200,
        )
        use_tugboat_dev_env = response["request_params"]["use_tugboat_dev"]
        if use_tugboat_dev_env:
            use_tugboat_dev_env = "dev"
        else:
            use_tugboat_dev_env = "prod"
        email_html = email_html.replace(
            "{{submission_json}}", submission_json_pp
        )
        email_html = email_html.replace(
            "{{deployment_id}}",
            str(response["submission_json"]["deploymentTitle"]),
        )
        email_html = email_html.replace(
            "use_tugboat_dev_env", use_tugboat_dev_env
        )

        return {"email_str": email_html}


@task(task_id="send_out_email")
def send_pubsub_email_task(email_str, cloud_run_response):
    # Safely unpack the nested dictionary values in native Python
    print("cloud_run_response", cloud_run_response)
    request_params = cloud_run_response.get("request_params", {})
    target_email = request_params.get("data_submitter_email", "")
    deployment_code = request_params.get("deployment_code", "")
    other_people_to_notify_emails = request_params.get(
        "other_people_to_notify_emails", ""
    )
    print("target_email", target_email)

    # Initialize the official Google Cloud Pub/Sub client
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(
        "ggn-nmfs-pamarc-dev-1", "tugboat-submission-notifier"
    )

    # Build the identical message payload structures
    message_1 = {
        "email_str": base64.b64encode(
            email_str["email_str"].encode("utf-8")
        ).decode("utf-8"),
        "email": target_email,
        "deployment_code": deployment_code,
        "other_people_to_notify_emails": other_people_to_notify_emails,
    }

    # Publish both messages exactly as the original operator would
    publisher.publish(
        topic_path,
        data=json.dumps(message_1, indent=2, sort_keys=True).encode("utf-8"),
    )


# DAG for submitting data to Tugboat
with DAG(
    dag_id="tugboat_submission_dag",
    start_date=datetime(2026, 7, 1),
    schedule=None,  # Manual trigger only
    catchup=False,
    params=PARAMS,
    description=(
        "This DAG is used to submit a JSON data package to Tugboat"
        "using the PAMTugboatAPI. This process assumes that a project has "
        "already been created in Tugboat."
    ),
) as dag:

    invoke_cloud_run_function_task = invoke_cloud_run_function()

    create_email_str_task = create_email_str(
        response=invoke_cloud_run_function_task
    )

    send_out_email = send_pubsub_email_task(
        email_str=create_email_str_task,
        cloud_run_response=invoke_cloud_run_function_task,
    )

    invoke_cloud_run_function_task >> create_email_str_task >> send_out_email
