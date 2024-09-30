""""This script deals with the times associated with ingesting/preprocessing data
from various sources. It works as follows:
    * A large file (usually 1 GB) is selected to repeatedly be downloaded and
        uploaded to a GCP bucket.
    * Download and upload times are recorded for each of these n iterations.
    * The average of these times are presented.
"""

import time

import ingestion
import utils


def time_ingestion_and_upload_from_ncei(n: int = 10,
                             ncei_file_url: str = "https://noaa-wcsd-pds.s3.amazonaws.com/data/raw/Reuben_Lasker/RL2107/EK80/2107RL_CW-D20210813-T220732.raw",
                             ncei_bucket: str = "noaa-wcsd-pds",
                             download_location: str = "./",
                             ):
    """Used for timing the ingestion from the NCEI AWS S3 bucket."""

    download_times = []
    upload_times = []
    file_name = ingestion.get_file_name_from_url(ncei_file_url)

    for i in range(n):
        start_time = time.time()
        ingestion.download_single_file_from_aws(bucket=ncei_bucket,
                                        file_url=ncei_file_url,
                                        download_location=download_location)
        time_elapsed = time.time() - start_time
        print(f"Downloading took {time_elapsed} seconds.\nThat's {1000/time_elapsed} mb/sec.")
        print(f"Uploading file to cloud storage")
        start_time = time.time()
        utils.upload_file_to_gcp_bucket(bucket=None,
                                        blob_file_path="timing_test_raw_upload.raw",
                                        local_file_path=file_name)
        time_elapsed = time.time() - start_time
        print(f"Uploading took {time_elapsed} seconds.\nThat's {1000/time_elapsed} mb/sec.")
    
    print(f"Average download time for this file: {sum(download_times)/len(download_times)}")
    print(f"Average upload time for this file: {sum(upload_times)/len(upload_times)}")


if __name__ == '__main__':
    time_ingestion_and_upload_from_ncei()
