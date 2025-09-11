# Uploads

You can use AALibrary to upload your active acoustics data to our GCP environments. Make sure you have the necessary permissions for the correct GCP environment before you start.

## Uploading Echosounder Files To GCP

In order to upload relevant Echosounder files (.raw, .idx, .bot, .nc) to GCP, use the following snippet.

```python
from aalibrary.egress import (
    upload_local_echosounder_files_from_directory_to_gcp_storage_bucket
)
from aalibrary.utils.cloud_utils import setup_gcp_storage_objects

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

## Uploading A Survey `As-Is` To GCP

