# Tugboat Integration

AALibrary comes equipped with Tugboat integration so you can focus on the analysis of data, and let Tugboat take care of the archival process.

## Tugboat Archival Process Using AALibrary

You can archive cruise files that have been uploaded to our Google Cloud Storage (via AALibrary or other means) to Tugboat using the following method.
The general process works by creating a Metadata JSON file, uploading it to GCS as backup, and submitting it to Tugboat for archival.

### Step 1. Creating A Metadata JSON File For Tugboat Archival Using Aalibrary

The first step is to associate a Tugboat Metadata JSON file with our submission. To do this, you will need to create a JSON file where all of the submission fields are empty. This is possible using the following code:

```python
from aalibrary.tugboat_api import TugboatAPI

tb_api = TugboatAPI()
tb_api.create_empty_submission_file(
    # Where you want the file to be located.
    file_download_directory=".",
    # Rename it to your cruise/survey name, if possible.
    file_name="tugboat_test_submission.json",
)
```

The empty submission file should look something like this:

```json
{
  "type": "string",
  "cruiseId": "string",
  "masterReleaseDate": "string",
  "ship": "string",
  ...
}
```

Now you can fill in the details of the submission file to match your particular cruise that you would like to archive.

### Step 2. Validating The Metadata JSON File

You can use the Tugboat Validator to validate your submission file, once you have completed filling it out.
The code for that can be viewed below:

```python
from aalibrary.tugboat_validations import TugboatValidator

# Object automatically validates the file upon creation.
validator = TugboatValidator(submission_json_file_path="./tugboat_test_submission.json")
```

If there are any errors within the file, you will see the output printed.
Otherwise, if the Metadata JSON is validated successfully, we can proceed to the next step.

### Step 3. Uploading The JSON File To GCS As Backup

We should upload this file to Google Cloud Storage Buckets as backup.
We can use the following code to accomplish that.

```python
from aalibrary.utils.cloud_utils import setup_gcp_storage_objs, upload_file_to_gcp_bucket
from aalibrary.utils.helpers import parse_correct_gcp_storage_bucket_location

# Set the GCS connection variables.
gcp_stor_client, gcp_bucket_name, gcp_bucket = setup_gcp_storage_objs(
    project_id = "ggn-nmfs-aa-dev-1",
    gcp_bucket_name = "ggn-nmfs-aa-dev-1-data"
)

# Parse the correct location for the file.
gcp_storage_bucket_location = parse_correct_gcp_storage_bucket_location(
    file_name = "tugboat_test_submission.json",
    file_type = "json",
    ship_name = "test_ship",
    survey_name = "test1203",
    echosounder = "EK80",
    data_source = "HDD",
    is_metadata = False,
    is_survey_metadata = True, # NOTE: It is important to set this to True.
    debug = True,
)

# Upload the file to the correct location.
upload_file_to_gcp_bucket(
    bucket = gcp_bucket,
    blob_file_path = gcp_storage_bucket_location,
    local_file_path = "./tugboat_test_submission.json,
    debug: bool = True,
)
```

After running this snippet of code, your Tugboat Metadata JSON will be backed-up to Google Cloud Storage.

### Step 4. Submitting The JSON File To Tugboat For Archival

Now you can finally submit your validated, backed-up Metadata JSON file to Tugboat for submission.
Use the code below, and read the output to double-check that the file has been submitted.

```python
from aalibrary.tugboat_api import TugboatAPI
tb_api = TugboatAPI()
tb_api.post_new_submission(
    submission_json_file_path="./tugboat_test_submission.json"
)
```

If you see a successful POST response message, such as `POST request successful!`, then you are good to go!

Tugboat will take care of the actual archival process, including pulling all of the data from Cloud Storage.
