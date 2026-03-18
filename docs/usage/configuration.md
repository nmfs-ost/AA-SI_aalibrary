# Configuration

AALibrary comes with many default configuration options. For example, the default GCP project that will be used when creating GCP storage objects is the dev project `ggn-nmfs-aa-dev-1`.

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

## Azure Configuration

Azure configuration requires an `azure_config.ini` file that is used for storing connection strings and keys. You can create an empty file using the `create_azure_config_file()` function found in `helpers.py`.

!!! note "NOTE"
    You will also need to have a space before and after the equals sign `=` when defining a value. For example, `azure_account_url = https...`.
