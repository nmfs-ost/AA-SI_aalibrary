# Configuration

AALibrary comes with many default configuration options. For example, the default GCP project that will be used when creating GCP storage objects is the prod project `ggn-nmfs-aa-prod-1`.

You can take a look at all of the default configs within code in the function signatures, or take a peek at the [config.py](https://github.com/nmfs-ost/AA-SI_aalibrary/blob/main/src/aalibrary/config.py) file for variables that are used as standards within code.

## GCP Environment Configuration

The default environment in AALibrary is the prod environment, `ggn-nmfs-aa-prod-1`. To switch to the dev environment, you can use the following code before making other function calls:

```python
from aalibrary import config

config.use_gcp_dev()
```

### Using Custom Environments/Buckets in GCP

In order to use a GCP environment or bucket that is not dev or prod, you can use the following code to create custom environment connection objects:

```python
from aalibrary.utils.cloud_utils import (
    setup_gbq_client_objs,
    setup_gcp_storage_objs
)

gcp_bq_client, gcp_gcs_file_system = setup_gbq_client_objs(
            project_id="custom-project-id")
gcp_stor_client, gcp_bucket_name, gcp_bucket = setup_gcp_storage_objs(
            project_id="custom-project-id",
            gcp_bucket_name="custom-bucket-name")
```

After these objects have been created. You can pass them into the functions that require them.

## Ocean Data Lake (ODL) Configuration

The easiest way to configure the Ocean Data Lake connection is to download and install the Azure CLI. Once you have downloaded this CLI, you can sign in using `az login`. The code in AALibrary will then use the default credentials from this CLI, similar to how we have `gcloud` set up. This install might require you to submit a ticket to NOAA IT since it might require admin privileges.
