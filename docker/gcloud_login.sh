#!/bin/bash

# Ensure required environment variables are set
if [ -z "$ACCOUNT" ] || [ -z "$PROJECT" ]; then
    echo "Error: ACCOUNT and PROJECT environment variables must be set."
    exit 1
fi

# Authenticate using the user account
echo "Authenticating user..."
gcloud auth login --no-browser --quiet
if [ $? -ne 0 ]; then
    echo "Failed to authenticate user."
    exit 1
fi

# Set the account from the environment variable ACCOUNT
echo "Setting active account to: $ACCOUNT"
gcloud config set account "$ACCOUNT"

# Set the project from the environment variable PROJECT
echo "Setting project to: $PROJECT"
gcloud config set project "$PROJECT"

# Authenticate using application default credentials
echo "Authenticating application default credentials..."
gcloud auth application-default login --no-browser --quiet
if [ $? -ne 0 ]; then
    echo "Failed to authenticate application default credentials."
    exit 1
fi

# Start the main process if arguments are provided
if [ $# -gt 0 ]; then
    echo "Executing: $@"
    exec "$@"
else
    echo "No command provided, exiting."
    exit 0
fi
