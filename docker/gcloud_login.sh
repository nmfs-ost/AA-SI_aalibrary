
#!/bin/bash


# Set the account from the environment variable ACCOUNT
gcloud config set account ${ACCOUNT}
echo "Account set to ${ACCOUNT}"
# Set the project from the environment variable PROJECT
gcloud config set project ${PROJECT}
echo "Project set to ${PROJECT}"
# Authenticate using the user account
gcloud auth login --no-browser

# Authenticate using application default credentials
gcloud auth application-default login --no-browser

# Start the main process
exec "$@"
