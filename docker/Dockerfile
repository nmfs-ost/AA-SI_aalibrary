FROM python:3.12-slim

# Update package lists and install dependencies in one go
RUN apt-get update && apt-get install -y \
    apt-transport-https \
    ca-certificates \
    gnupg \
    sudo \
    curl \
    git \
    wget \
    unzip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Add Google Cloud SDK repository and install gcloud CLI
RUN curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" > /etc/apt/sources.list.d/google-cloud-sdk.list \
    && apt-get update && apt-get install -y google-cloud-cli \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Python client libraries for Google Cloud and Virtualenv
RUN pip install --no-cache-dir \
    google-cloud-storage \
    google-cloud-bigquery \
    google-cloud-logging \
    virtualenv

# Create and activate a virtual environment
RUN python -m virtualenv /my-venv
ENV PATH="/my-venv/bin:$PATH"

# Install aalibrary from GitHub inside the virtualenv
RUN pip install git+https://github.com/nmfs-ost/AA-SI_aalibrary.git

# Set working directory for scripts
WORKDIR /app

# Copy all .sh and .py files from the host to the container
COPY *.sh *.py /app/  
RUN chmod +x /app/google-cloud-login.sh


# Set default command to enter bash shell
CMD [ "bash" ]