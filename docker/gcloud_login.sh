
#!/bin/bash

# Authenticate using the user account
gcloud auth login

# Authenticate using application default credentials
gcloud auth application-default login

# Set the account from the environment variable ACCOUNT
gcloud config set account ${ACCOUNT}

# Set the project from the environment variable PROJECT
gcloud config set project ${PROJECT}

# Start the main process
exec "$@"
