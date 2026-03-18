# Conversions

!!! note "NOTE: Default GCP Environment"
    By default, `aalibrary` uses the prod GCP project and bucket. If you would like to switch to the dev environment, simply call aalibrary.config.use_gcp_dev() before running your functions. If you would like to use a custom environment, follow the instructions outlined [here](../usage/configuration.md#gcp-environment-configuration).

## Converting A Raw Into Netcdf

In order to convert a raw file into a netcdf, use the following example as a guide:

```python
from aalibrary import utils
from aalibrary.conversion import convert_raw_to_netcdf

# Create a GCP bucket object
gcp_stor_client, gcp_bucket_name, gcp_bucket = utils.cloud_utils.setup_gcp_storage_objs()

# This function takes care of downloading, converting, and uploading (caching) the netcdf file in gcp.
convert_raw_to_netcdf(file_name="2107RL_CW-D20210813-T220732.raw",
                      file_type="raw",
                      ship_name="Reuben_Lasker",
                      survey_name="RL2107",
                      echosounder="EK80",
                      data_source="NCEI",
                      file_download_directory="./",
                      overwrite=False,
                      gcp_bucket=gcp_bucket,
                      debug=False)
```
