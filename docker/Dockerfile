
FROM python:3.12-slim

# Update package lists and upgrade system packages
RUN apt-get update && apt-get upgrade -y && apt-get clean

# Install dependencies for gcloud SDK and virtualenv
RUN apt-get install -y apt-transport-https ca-certificates gnupg sudo python3-virtualenv

# Add the Google Cloud SDK repository
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list

# Import the Google Cloud public key
RUN curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | tee /usr/share/keyrings/cloud.google.gpg

# Install the Google Cloud SDK
RUN apt-get update && apt-get install -y google-cloud-sdk

# Clean up
RUN rm -rf /var/lib/apt/lists/*

# Install Python client libraries for Google Cloud
RUN pip install --no-cache-dir google-cloud-storage google-cloud-bigquery google-cloud-logging

# Create a virtual environment
RUN python -m virtualenv /my-venv

# Set the environment to use the virtualenv
ENV PATH="/my-venv/bin:$PATH"

# Install aalibrary from GitHub repository into the virtualenv
RUN /my-venv/bin/pip install aalibrary@git+https://github.com/nmfs-ost/AA-SI_aalibrary.git

# Add script to handle login and account configuration
COPY gcloud_login.sh /usr/local/bin/gcloud_login.sh
RUN chmod +x /usr/local/bin/gcloud_login.sh

# Test entrypoint to run quick_test
COPY test_entrypoint.py /usr/local/bin/test_entrypoint.py

# Set the default entrypoint to run the test Python script
ENTRYPOINT ["python", "/usr/local/bin/test_entrypoint.py"]
