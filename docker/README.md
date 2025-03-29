
# GCloud Docker Setup

This repository contains a Dockerfile that sets up a Python 3.12 environment, installs the Google Cloud SDK, creates a virtual environment, installs the `aalibrary` from GitHub, and sets up the necessary environment for Google Cloud authentication.

## Prerequisites

Ensure that you have Docker installed on your system. You will also need to have a Google Cloud account and appropriate permissions for authentication.

## Building the Docker Image

To build the Docker image, run the following command in the project directory where the `Dockerfile` is located:

```bash
docker build -t aalibrary .
```

This will build the Docker image and install all necessary dependencies.

## Running the Docker Container

To run the Docker container, use the following command:

```bash
docker run -e ACCOUNT="your-account@example.com" -e PROJECT="ggn-nmfs-aa-dev-1" aalibrary
```

This will start the container, run the `gcloud_login.sh` script to authenticate with Google Cloud, set the account and project, and then run the `test_entrypoint.py` script to check the functionality of the `aalibrary`.

## Interactive Testing (Optional)

If you want to enter an interactive Python session inside the container to test things manually, you can run:

```bash
docker run -it --rm aalibrary /my-venv/bin/python
```

Once inside the Python shell, you can run:

```python
from aalibrary import quick_test
quick_test.start()
```

This will run a test to verify connectivity in your environment.

## Notes

- Ensure that you replace `your-account@example.com` with your actual Google Cloud account and `ggn-nmfs-aa-dev-1` with your desired project.
- The container uses a virtual environment to isolate dependencies for Python packages.