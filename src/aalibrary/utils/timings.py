""""This script deals with the times associated with ingesting/preprocessing
    data from various sources. It works as follows:
    * A large file (usually 1 GB) is selected to repeatedly be downloaded and
        uploaded to a GCP bucket.
    * Download and upload times are recorded for each of these n iterations.
    * The average of these times are presented.
"""

import time

from aalibrary import ingestion
from aalibrary.utils import cloud_utils


def time_ingestion_and_upload_from_ncei(
    n: int = 10,
    ncei_file_url: str = (
        "https://noaa-wcsd-pds.s3.amazonaws.com/data/raw/"
        "Reuben_Lasker/RL2107/EK80/"
        "2107RL_CW-D20210813-T220732.raw"
    ),
    ncei_bucket: str = "noaa-wcsd-pds",
    download_location: str = "./",
):
    """Used for timing the ingestion from the NCEI AWS S3 bucket."""

    download_times = []
    upload_times = []
    file_name = ingestion.get_file_name_from_url(ncei_file_url)

    for i in range(n):
        start_time = time.time()
        ingestion.download_single_file_from_aws(
            s3_bucket=ncei_bucket,
            file_url=ncei_file_url,
            download_location=download_location,
        )
        time_elapsed = time.time() - start_time
        print(
            (
                f"Downloading took {time_elapsed} seconds."
                f"\nThat's {1000/time_elapsed} mb/sec."
            )
        )
        print("Uploading file to cloud storage")
        start_time = time.time()
        cloud_utils.upload_file_to_gcp_bucket(
            bucket=None,
            blob_file_path="timing_test_raw_upload.raw",
            local_file_path=file_name,
        )
        time_elapsed = time.time() - start_time
        print(
            (
                f"Uploading took {time_elapsed} seconds."
                f"\nThat's {1000/time_elapsed} mb/sec."
            )
        )

    print(
        (
            "Average download time for this file:"
            f" {sum(download_times)/len(download_times)}"
        )
    )
    print(
        (
            "Average upload time for this file:"
            f" {sum(upload_times)/len(upload_times)}"
        )
    )


if __name__ == "__main__":
    time_ingestion_and_upload_from_ncei()
