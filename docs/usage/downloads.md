# Downloads

Here are some examples of download/ingestion functions that you can use in this library.

## Downloading A Raw File From NCEI

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
                            file_download_directory=".",
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
                  file_download_directory=".",
                  debug=False)
```

## Downloading An Entire Survey From NCEI

Sometimes, you will need to download an entire survey from NCEI for analysis. This is possible using the AALibrary. Follow the code snippet below.

!!! note "NOTE"
    This function will automatically create the appropriate subdirectories within the `download_directory` param that you have specified. For example: within the snippet below, the data will exist in `./test_data_dir/Reuben_Lasker/RL2107/...`

```python
from aalibrary.ingestion import download_survey_from_ncei

download_survey_from_ncei(ship_name="Reuben_Lasker",
                          survey_name="RL2107",
                          download_directory="./test_data_dir",
                          debug: bool = False)
```

## Downloading A Raw File From Azure Data Lake (OMAO)

Use the following code if you would like to download a file from the Azure Data Lake. The code requires a `config.ini` file.

**NOTE:** This file needs to have a `[DEFAULT]` section with a `azure_connection_string` variable set.

```python
from aalibrary.ingestion import download_raw_file_from_azure

download_raw_file_from_azure(
    file_name="1601RL-D20160107-T074016.raw",
    file_type="raw",
    ship_name="Reuben_Lasker",
    survey_name="RL1601",
    echosounder="EK60",
    data_source="OMAO",
    file_download_directory=".",
    config_file_path="./azure_config.ini",
    upload_to_gcp=True,
    debug=True,
)
```

If you would like a single file downloaded using a path, you can use the following much more simple code:

```python
from aalibrary.ingestion import download_specific_file_from_azure

download_specific_file_from_azure(
    config_file_path="./azure_config.ini",
    container_name="testcontainer",
    file_path_in_container="RL2107_EK80_WCSD_EK80-metadata.json",
)
```

**NOTE:** Please keep in mind that this method creates a connection every single time you call it.

## Downloading A Netcdf

Netcdf files (converted over from raw) only exist in the GCP cache as of now. The following example takes care of downloading a particular raw file as netcdf4 (if it had already been converted and cached in GCP, otherwise an error message is thrown):

```python
from aalibrary import utils
from aalibrary.ingestion import download_netcdf_file

# Create a GCP bucket object
gcp_stor_client, gcp_bucket_name, gcp_bucket = utils.cloud_utils.setup_gcp_storage_objs()

# This function takes care of downloading the netcdf.
download_netcdf_file(
                raw_file_name="2107RL_CW-D20210813-T220732.raw",
                file_type="netcdf",
                ship_name="Reuben_Lasker",
                survey_name="RL2107",
                echosounder="EK80",
                data_source="NCEI",
                file_download_location=".",
                gcp_bucket=gcp_bucket,
                debug=False)
```

## Downloading Multiple Files From A Survey

```python
from aalibrary.ingestion import download_raw_file_from_ncei

file_names = ["2107RL_CW-D20210813-T220732.raw",
              "2107RL_CW-D20210706-T172335.raw"]
for file_name in file_names:
  download_raw_file_from_ncei(
    file_name=file_name,
    file_type="raw",
    ship_name="Reuben_Lasker",
    survey_name="RL2107",
    echosounder="EK80",
    data_source="NCEI",
    file_download_directory=".",
    upload_to_gcp=True,   # Set to True if you want to upload the raw file to gcp
    debug=False)
```
