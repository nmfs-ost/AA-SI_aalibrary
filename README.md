# Active Acoustics

This repo contains files, code, and other necessary scripts for execution of data pipelines for NOAA Active Acoustics.

## Examples

### Converting A Raw Into Netcdf

In order to convert a raw file into a netcdf, use the following example as a guide:

```python
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

### Downloading A Netcdf

```python
# Create a GCP bucket object
gcp_stor_client, gcp_bucket_name, gcp_bucket = utils.cloud_utils.setup_gbq_storage_objs()

# This function takes care of downloading the netcdf.
download_netcdf(file_name="2107RL_CW-D20210813-T220732.raw",
                    file_type="netcdf", ship_name="Reuben_Lasker",
                    survey_name="RL2107", echosounder="EK80",
                    file_download_location=".", gcp_bucket=gcp_bucket,
                    is_metadata=False,debug=False)
```
