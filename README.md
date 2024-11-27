<!-- markdownlint-configure-file {
  "MD013": {
    "code_blocks": true,
    "tables": false
  },
  "MD033": false,
  "MD041": false,
  "MD013": false
} -->

<div align="center">

<a href="https://www.warp.dev/?utm_source=github&utm_medium=referral&utm_campaign=zoxide_20231001">
  <div>
    <img src="other/img.png" width="230" alt="Warp" />
  </div>
</a>
<hr />

Active Acoustics Strategic Initiative (AASI) aims to bring more data modernization to NOAA NMFS. This library provides end-users the tools needed to easily access data from disparate sources. It also implements caching for raw, idx, and netcdf4 files within the GCP ecosystem. `</br>`
It is an improvement over previous methods, which would require end-users to fetch each piece of data from its respective source separately. `</br>` `</br>`

[Getting Started](#getting-started) •
[Installation](#installation) •
[Dependencies](#dependencies) •
[Usage](#usage) •
[Recipes](#recipes)

</div>

This repo contains files, code, and other necessary scripts for execution of data pipelines for NOAA Active Acoustics.

# Getting Started

If this library has already been installed, you can run a quick test using `python quick_test.py`. This test will check if your network connections to the data sources are working, and download a raw file as an initial test.

## Installation

To securely install this package via pip, use the following:

### Step 1 - Log Into `gcloud`

Issue the following command, and follow the instructions to login to `gcloud`:

```bash
gcloud auth application-default login
```

### Step 2 - Install Necessary Dependencies Before The `pip install`

We need to install some dependencies, and check two authentication parameters before we install.

#### Step 2.1 - Run The Following Commands To Install Dependencies

```python
pip install keyring
pip install keyrings.google-artifactregistry-auth
```

#### Step 2.2 - Run The Following Command & Check The Output

Check if `ChainerBackend(priority:10)` and `GooglePythonAuth(priority: 9)` are both present in the output of the following command:

```python
keyring --list-backends
```

If they are, proceed forward. If they are not, please re-install `keyring` and `keyrings.google-artifactregistry-auth`.

### Step 3 - It's Finally `pip install` Time

To finally be able to pip-install the library, make sure you have access to the repository (contact hannan.khan@noaa.gov), then use the following command:

```python
python -m pip install --index-url https://us-central1-python.pkg.dev/ggn-nmfs-aa-dev-1/aalibrary/simple/ aalibrary
```

**Note:** You can also use the same command to upgrade the current version of the package to the newest version.

## Dependencies

Dependencies are listed within the `requirements.txt` file within the Cloud Source Repo.

## Usage

Here are some examples of functions that you can use in this library.

### Converting A Raw Into Netcdf

In order to convert a raw file into a netcdf, use the following example as a guide:

```python
from aalibrary import utils
from aalibrary.ingestion import convert_raw_to_netcdf

# Create a GCP bucket object
gcp_stor_client, gcp_bucket_name, gcp_bucket = utils.cloud_utils.setup_gbq_storage_objs()

# This function takes care of downloading, converting, and uploading (caching) the netcdf file in gcp.
convert_raw_to_netcdf(file_name="2107RL_CW-D20210813-T220732.raw",
                      file_type="raw",
                      ship_name="Reuben_Lasker",
                      survey_name="RL2107",
                      echosounder="EK80",
                      data_source="NCEI",
                      file_download_location="./",
                      gcp_bucket=gcp_bucket,
                      is_metadata=False,
                      debug=False)
```

### Downloading A Raw File From NCEI

In order to download a raw file from NCEI, use the following example:

```python
from aalibrary.ingestion import download_raw_file_from_ncei

# This function takes care of downloading, converting, and uploading (caching) the netcdf file in gcp.
download_raw_file_from_ncei(file_name="2107RL_CW-D20210813-T220732.raw",
                            file_type="raw",
                            ship_name="Reuben_Lasker",
                            survey_name="RL2107",
                            echosounder="EK80",
                            data_source="NCEI",
                            file_download_location=".",
                            is_metadata=False,
                            upload_to_gcp=True,   # Set to True if you want to upload the raw file to gcp
                            debug=False)
```

If you would like to just download a raw file, but do not care about it's source, you can use the following function:

```python
from aalibrary.ingestion import download_raw_file

download_raw_file(file_name="2107RL_CW-D20210813-T220732.raw",
                  file_type="raw",
                  ship_name="Reuben_Lasker",
                  survey_name="RL2107",
                  echosounder="EK80",
                  data_source="NCEI",
                  file_download_location=".",
                  is_metadata=False,
                  upload_to_gcp=True,   # Set to True if you want to upload the raw file to gcp
                  debug=False)
```

### Downloading A Netcdf

Netcdf files (converted over from raw) only exist in the GCP cache as of now. The following example takes care of downloading a particular raw file as netcdf4 (if it had already been converted and cached in GCP, otherwise an error is thrown):

```python
from aalibrary import utils
from aalibrary.ingestion import download_netcdf_file

# Create a GCP bucket object
gcp_stor_client, gcp_bucket_name, gcp_bucket = utils.cloud_utils.setup_gbq_storage_objs()

# This function takes care of downloading the netcdf.
download_netcdf_file(file_name="2107RL_CW-D20210813-T220732.raw",
                file_type="netcdf",
		ship_name="Reuben_Lasker",
                survey_name="RL2107",
		echosounder="EK80",
                file_download_location=".",
		gcp_bucket=gcp_bucket,
                is_metadata=False,
		debug=False)
```

## Recipes

The following contains common recipes that an end-user might encounter.
