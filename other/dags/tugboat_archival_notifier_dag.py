from datetime import datetime
import requests
from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from google.auth.transport.requests import Request
from google.oauth2 import id_token
from airflow.providers.google.cloud.operators.pubsub import (
    PubSubPublishMessageOperator,
)

PROJECT_ID = "ggn-nmfs-aa-dev-1"
REGION = "us-central1"
FUNCTION_URL = (
    "https://tugboat-submission-function-465755541677.us-central1.run.app"
)
ENDPOINT_ROUTE = "tugboat-submission-function"

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


def invoke_cloud_run_function(**context):
    """This task is used to trigger a CloudRun function that will create the
    deployment's submission JSON and submit it using the PAMTugboatAPI."""

    # # Get ADC
    # credentials, project = google.auth.default()
    # # Create authorized session using a ID token.
    # authed_session = AuthorizedSession(credentials)
    # Fetch the ID token using the Airflow worker's default service account credentials
    # Ensure your worker's service account has the 'Cloud Run Invoker' role
    token = id_token.fetch_id_token(Request(), audience=FUNCTION_URL)

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
        "use_tugboat_dev": context["params"]["use_tugboat_dev"],
    }

    # Make the authenticated POST request
    response = requests.post(FUNCTION_URL, headers=headers, json=payload)
    response.raise_for_status()
    print("response that dag receives:", response.text)
    # Error handling if there is no json in the response body.
    return_package = {"submission_json": response.submission_json,
                      "response_status_code": response.status_code,
                      "response_text": response.text}
    if not response.json():
        # Returning the package back to the HTTP caller (Airflow)
        return return_package
    else:
        return_package["response_json"] = response.json()
        # Returning the package back to the HTTP caller (Airflow)
        return return_package


@task
def create_email_str(response):
    if not response:
        return "No response found.".encode("utf-8")

    else:
        email_s = (
            "Tugboat Submission submitted with response"
            f" code: {response.message.data.status_code}.\n"
        )
        email_s += f" Response:\n{response.message.data.text}"
        return email_s.encode("utf-8")


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

    invoke_cloud_run_function_task = PythonOperator(
        task_id="invoking_cloud_run_function",
        python_callable=invoke_cloud_run_function,
        provide_context=True,
    )

    email_str = create_email_str(invoke_cloud_run_function_task.output)

    send_out_email = PubSubPublishMessageOperator(
        task_id="send_out_email",
        project_id="ggn-nmfs-aa-dev-1",
        topic="tugboat-archival-notifier",
        messages=[
            {
                "data": email_str,
            },
            {"attributes": {"event_type": "dag_complete"}},
        ],
        gcp_conn_id="google_cloud_default",
    )

    invoke_cloud_run_function_task >> send_out_email
