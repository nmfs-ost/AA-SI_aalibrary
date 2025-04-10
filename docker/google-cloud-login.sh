#!/bin/bash

# Ensure required environment variables are set
if [ -z "$ACCOUNT" ] || [ -z "$PROJECT" ]; then
    echo "Error: ACCOUNT and PROJECT environment variables must be set."
    exit 1
fi

# Set the account from the environment variable ACCOUNT
echo "Setting active account to: $ACCOUNT"
gcloud config set account "$ACCOUNT"

# Set the project from the environment variable PROJECT
echo "Setting project to: $PROJECT"
gcloud config set project "$PROJECT"

# Authenticate using the user account
echo "Authenticating user..."
gcloud auth login --no-browser --quiet
if [ $? -ne 0 ]; then
    echo "Failed to authenticate user."
    exit 1
fi