# Uploads

You can use AALibrary to upload your active acoustics data to our GCP environments. Make sure you have the necessary permissions for the correct GCP environment before you start.

There are numerous methods for uploads, each with its own benefits. If uploading over network, the size of the data should be limited to 5 TBs in size, and the upload must take place during a time period where network traffic is minimal. This is done to limit the amount of bandwidth used.

For a detailed description (pros/cons) of all upload tools/options, take a look at the [Data Migration Options Documentation](https://docs.google.com/document/d/1O42DUPIgnoTopwLOBWcLyWYTElzvNT845bYMnpy6O4A/edit?tab=t.0#heading=h.7twvrhkxwqt).

!!! info "INFO: More Info On GCP"
    For more information on GCP implementation, please take a look at the [GCP Overview Page](../documentation/gcp_overview.md).

## Uploading Echosounder Files To GCP

In order to upload selective Echosounder files (.raw, .idx, .bot, .nc) to GCP, use the following snippet. This function maintains the formatting and folder structure that AALibrary uses. This makes retrieval of the files using the AALibrary possible.

```python
from aalibrary.egress import (
    upload_local_echosounder_files_from_directory_to_gcp_storage_bucket
)
from aalibrary.utils.cloud_utils import setup_gcp_storage_objs

gcp_stor_client, gcp_bucket_name, gcp_bucket = (
    setup_gcp_storage_objs(
        project_id="ggn-nmfs-aa-dev-1",
        gcp_bucket_name="ggn-nmfs-aa-dev-1-data",
    )
)
upload_local_echosounder_files_from_directory_to_gcp_storage_bucket(
        local_echosounder_directory_to_upload="./Reuben_Lasker/RL2107/EK80/",
        ship_name="Reuben_Lasker",
        survey_name="RL2107",
        echosounder="EK80",
        data_source="HDD", # <== Refers to the fact that this is uploaded from local.
        gcp_bucket=gcp_bucket,
        debug=True,
)
```

## Uploading A Survey/Folder `As-Is` To GCP

If you would like to upload a folder to the GCP storage bucket as-is, you can use this function.

```python
from aalibrary.egress import upload_folder_as_is_to_gcp
from aalibrary.utils.cloud_utils import setup_gcp_storage_objs

# Here we specify the project and bucket we would like to upload to
gcp_stor_client, gcp_bucket_name, gcp_bucket = (
    setup_gcp_storage_objs(
        project_id="ggn-nmfs-aa-dev-1",
        gcp_bucket_name="ggn-nmfs-aa-dev-1-data",
    )
)

# You can also specify a 'destination prefix'; used for putting the folder
# in a certain place within the bucket.
upload_folder_as_is_to_gcp(
    local_folder_path="./test_data_dir/Reuben_Lasker/",
    gcp_bucket=gcp_bucket,
    destination_prefix="other/deletable/",
)
```
