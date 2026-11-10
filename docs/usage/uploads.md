# Uploads

You can use AALibrary to upload your active acoustics data to our GCP environments. Make sure you have the necessary permissions for the correct GCP environment before you start.

There are numerous methods for uploads, each with its own benefits. If uploading over network, the size of the data should be limited to 5 TBs in size, and the upload must take place during a time period where network traffic is minimal. This is done to limit the amount of bandwidth used.

For a detailed description (pros/cons) of all upload tools/options, take a look at the [Data Migration Options Documentation](https://docs.google.com/document/d/1O42DUPIgnoTopwLOBWcLyWYTElzvNT845bYMnpy6O4A/edit?tab=t.0#heading=h.7twvrhkxwqt).

!!! info "INFO: More Info On GCP"
    For more information on GCP implementation, please take a look at the [GCP Overview Page](../documentation/gcp_overview.md).

!!! note "NOTE: Default GCP Environment"
    By default, `aalibrary` uses the prod GCP project and bucket. If you would like to switch to the dev environment, simply call aalibrary.config.use_gcp_dev() before running your functions. If you would like to use a custom environment, follow the instructions outlined [here](../usage/configuration.md#gcp-environment-configuration).

## Uploading Echosounder Files To GCP

To upload a folder of echosounder files (.raw, .idx, .bot, .nc) to GCP, use the following snippet. This function maintains the formatting and folder structure that AALibrary uses. This makes retrieval of the files using the AALibrary possible.

```python
from aalibrary.egress import (
    upload_local_echosounder_files_from_directory_to_gcp_storage_bucket
)

upload_local_echosounder_files_from_directory_to_gcp_storage_bucket(
    local_echosounder_directory_to_upload=r"C:/data/DX2301/EK80",
    ship_name="Henry B. Bigelow",   # normalized to Henry_B_Bigelow
    survey_name="DX2301",
    echosounder="EK80",
    data_source="HDD",  # <== Refers to the fact that this is uploaded from local.
)
```

The files go to `HDD/Henry_B_Bigelow/DX2301/EK80/data/raw/` (and `.../data/netcdf/` for .nc files) in the bucket aalibrary is configured for; pass `gcp_bucket=` to choose another. Then the .raw and .nc files' metadata is recorded in BigQuery.

Before anything is uploaded, it checks:

* the folder and the names (the echosounder and data source must be ones aalibrary knows);
* the ship name, against the ICES ship list (see [ships without an ICES code](#ships-and-platforms-without-an-ices-code-yet));
* that your account can add rows to the BigQuery metadata tables. If it can't, nothing is uploaded and the message says what to do.

Files already in the bucket aren't uploaded again, and files whose metadata is already in BigQuery don't get a second row. So if an upload is interrupted, run the same call again.

### Uploading Without The Metadata, And Adding It Later

Recording metadata needs the **BigQuery Data Editor** role on the `metadata` dataset (ask a project admin). Without it, or when you just want the files up quickly, upload the files only:

```python
upload_local_echosounder_files_from_directory_to_gcp_storage_bucket(
    local_echosounder_directory_to_upload=r"C:/data/DX2301/EK80",
    ship_name="Henry B. Bigelow",
    survey_name="DX2301",
    echosounder="EK80",
    data_source="HDD",
    upload_metadata=False,
)
```

Later (from the same folder, with the same names), record their metadata. Nothing is uploaded to the bucket, and files that already have metadata are skipped:

```python
from aalibrary.egress import (
    upload_local_echosounder_metadata_from_directory_to_bigquery
)

upload_local_echosounder_metadata_from_directory_to_bigquery(
    local_echosounder_directory=r"C:/data/DX2301/EK80",
    ship_name="Henry B. Bigelow",
    survey_name="DX2301",
    echosounder="EK80",
    data_source="HDD",
)
```

From the command line, `aa-upload` does the same: `--no-metadata` uploads the files only.

### Ships And Platforms Without An ICES Code Yet

An unlisted ship name is refused, since it's usually a misspelling (the message suggests the closest ICES names). For a new ship or platform that doesn't have an ICES code yet, say so:

```python
upload_local_echosounder_files_from_directory_to_gcp_storage_bucket(
    ...,
    ship_name="DriX 1",
    provisional_ship_name=True,   # aa-upload: --provisional-ship-name
)
```

The files are filed under the name as given (normalized: `Drix_1`), and their metadata has no ICES code. If the name ICES approves is different, the files will need to be moved to the new name's folder.

## Uploading One File

`upload_file_to_gcp_storage_bucket` uploads a single file. Tell it where the file is with `file_location`: the file's full path, or the folder it's in together with `file_name`.

To put it in the standard place (where the directory uploader would), give the names and leave out `gcp_storage_bucket_location`:

```python
from pathlib import Path
from aalibrary.egress import upload_file_to_gcp_storage_bucket

f = Path(r"C:/data/DX2301/EK80/D20231017-T211129.idx")
upload_file_to_gcp_storage_bucket(
    file_location=f,              # or file_location=f.parent, file_name=f.name
    ship_name="Henry B. Bigelow",
    survey_name="DX2301",
    echosounder="EK80",
    data_source="HDD",
)
# -> HDD/Henry_B_Bigelow/DX2301/EK80/data/raw/D20231017-T211129.idx
```

To put it somewhere else, give `gcp_storage_bucket_location`: a folder in the bucket (the file keeps its name), or the full object path. A Windows-style path, a leading `gs://bucket/` or the bucket's name are all fine:

```python
upload_file_to_gcp_storage_bucket(
    file_location=f,
    gcp_storage_bucket_location="HDD/Henry_B_Bigelow/DX2301/EK80/test/",
)
# -> HDD/Henry_B_Bigelow/DX2301/EK80/test/D20231017-T211129.idx
```

It prints the `gs://` address the file went to, skips a file that's already there, and returns the object's path in the bucket (or `None` if it couldn't be uploaded). It doesn't record BigQuery metadata.

`file_type` is the file name suffix (e.g. "raw", "idx", "nc", without the dot); it's taken from the file name when you leave it out.

## Uploading A Whole Survey With `LocalSurvey`

`LocalSurvey` looks through a survey's folder, subfolders included, and works out where every file belongs in the bucket. You can then upload them all and check the uploads by checksum. It uploads files only: no BigQuery metadata, and no ICES check on the ship name.

```python
from aalibrary.survey import LocalSurvey

survey = LocalSurvey(
    directory_path=r"C:/data/DX2301",
    ship_name="Henry B. Bigelow",
    survey_name="DX2301",
    echosounder="EK80",   # or leave it out if the files sit in folders named after it (.../EK80/...)
    data_source="HDD",
)
print(survey)                       # counts and sizes of what was found
survey.print_all_files_in_directory()  # each file and where it will go
survey.upload_files()               # files already in the bucket are skipped
survey.verify_gcp_uploads()         # compares checksums with the bucket
```

See the [LocalSurvey reference](../documentation/survey.md) for everything it offers.

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

## Uploading To Ocean Data Lake

!!! note: Your ability to upload data to the Ocean Data Lake depends on your permission level. Most are granted read-only permissions, and you might have to reach out to get permission to upload.

The goal of the ODL is to upload from the ship directly, so chances are that end-users will not have to upload any files. If by any chance, you encounter the need to upload files to the ODL, please use the [Azure CLI](https://learn.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-cli#upload-a-blob) to accomplish this.
