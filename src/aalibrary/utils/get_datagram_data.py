"""This file provides functions to get datagram data from raw data files,
including streaming datagram data from NCEI raw data files in S3. This allows
users to access relevant metadata from the datagram without having to download
the entire raw data file, which can be very large."""

from pprint import pprint
import fsspec

from google.cloud import storage

from aalibrary.utils.sonar_checker import ek_raw_io
from aalibrary.utils.ncei_cache_utils import (
    get_random_raw_file_from_ncei_cache_with_search_param,
)
from aalibrary.config import NCEI_BUCKET_NAME, get_current_gcp_bucket_name
from aalibrary.utils.cloud_utils import create_s3_objs


def get_datagram_dict_from_raw_file(raw_file_path, storage_options) -> dict:
    """Gets a datagram dictionary from a raw file.

    Args:
        raw_file_path (str): The path to the raw file to get the datagram from.
        storage_options (dict): A dictionary of storage options to use when
            accessing the raw file. See echopype for more details.

    Returns:
        dict: A dictionary containing the datagram data, or an empty dictionary
            if an error occurs.
    """
    try:
        with ek_raw_io.RawSimradFile(
            raw_file_path, "r", storage_options=storage_options
        ) as fid:
            config_datagram = fid.read(1)
            # config_datagram["timestamp"] = np.datetime64(
            #     config_datagram["timestamp"].replace(tzinfo=None), "[ns]"
            # )

            return config_datagram
    except Exception as e:
        print(
            f"Error during parsing of datagram for file {raw_file_path}:\n{e}"
        )
        return {}


def stream_datagram_dict_from_ncei(s3_object_key: str = None) -> dict:
    """Stream a datagram from an NCEI raw data file in S3 by reading in
    the first few megabytes and parsing the datagram to get relevant metadata.

    Args:
        s3_object_key (str): The S3 object key for the raw data file to stream
            the datagram from. Must be provided.

    Returns:
        dict: A dictionary containing the datagram data, or an empty dictionary
            if an error occurs.
    """
    if s3_object_key is None:
        raise ValueError("s3_object_key must be provided.")
    # Create S3 client
    s3_client, _, _ = create_s3_objs()
    # Get datagram/header size by reading first 4 bytes
    response = s3_client.get_object(
        Bucket=NCEI_BUCKET_NAME, Key=s3_object_key, Range="bytes=0-3"
    )
    # datagram_size = struct.unpack("=l", buf)[0]
    datagram_size = int.from_bytes(response["Body"].read(), byteorder="big")
    # Create streamingBody object
    response = s3_client.get_object(
        Bucket=NCEI_BUCKET_NAME,
        Key=s3_object_key,
        Range=f"bytes=0-{datagram_size-1}",
    )
    # Read in the contents of the streamingBody object
    response_body = response["Body"].read()
    try:
        # Create a filesystem object for in-memory operations
        fs = fsspec.filesystem("memory")
        # Write bytes to a virtual path
        fs.pipe("virtual/path.raw", response_body)
    except Exception as e:
        print(f"Error writing to in-memory filesystem: {e}")
        return {}

    # file_like = io.BytesIO(response_body)
    # print("response", type(response), response)
    # print("response['Body']", type(response["Body"]), response["Body"])
    # print("response_body", type(response_body), response_body)
    # print("file_like", type(file_like), file_like)
    # content = file_like.getbuffer().tobytes()
    # with open("./temp.raw", "wb") as f:
    #     f.write(content)
    # print("content", type(content), content)
    # # Unpack integer ('>I' for big-endian unsigned int)
    # datagram_length = struct.unpack('=l', file_like.read(4))[0]
    # print("datagram_length", type(datagram_length), datagram_length)
    # payload = file_like.read(datagram_length)
    # print("payload", type(payload), payload)
    # unpacked_data = struct.unpack('>I', payload[:4])
    # print("unpacked_data", type(unpacked_data), unpacked_data)
    try:
        with ek_raw_io.RawSimradFile(
            "memory://virtual/path.raw", "r", storage_options={}
        ) as fid:
            config_datagram = fid.read(5)
            # config_datagram["timestamp"] = np.datetime64(
            #     config_datagram["timestamp"].replace(tzinfo=None), "[ns]"
            # )
        return config_datagram
    except Exception as e:
        print(
            f"Error during parsing of datagram for object {s3_object_key}:\n{e}"
        )
        return {}


def stream_datagram_dict_from_gcs(
    gcs_blob_path: str = "",
    gcs_bucket_name: str = "",
    gcs_bucket: storage.Client.bucket = None,
) -> dict:
    """Stream a datagram from a GCS raw data file by reading in
    the first few megabytes and parsing the datagram to get relevant metadata.

    Args:
        gcs_blob_path (str): The GCS blob path for the raw data file to stream
            the datagram from. Must be provided.
        gcs_bucket_name (str): The GCS bucket name for the raw data file to
            stream the datagram from. If not provided, the current GCP bucket
            name will inferred from aalibrary.config.
        gcs_bucket (storage.Client.bucket): A GCS bucket object. If not
            provided, a new bucket object will be created using the provided
            gcs_bucket_name or the inferred gcs_bucket_name from
            aalibrary.config.
    Returns:
        dict: A dictionary containing the datagram data, or an empty dictionary
            if an error occurs.
    """

    # Check for required parameters, and build storage clients.
    if gcs_blob_path is None or gcs_blob_path == "":
        raise ValueError("gcs_blob_path must be provided.")
    if gcs_bucket_name is None:
        gcs_bucket_name = get_current_gcp_bucket_name()
    if gcs_bucket is None:
        gcs_client = storage.Client()
        gcs_bucket = gcs_client.bucket(gcs_bucket_name)

    # Get datagram/header size by reading first 4 bytes
    blob = gcs_bucket.blob(gcs_blob_path)
    datagram_size = int.from_bytes(blob.download_as_bytes(start=0, end=3))

    # Read in the contents of the blob for the datagram
    response_body = blob.download_as_bytes(start=0, end=(datagram_size - 1))
    try:
        # Create a filesystem object for in-memory operations
        fs = fsspec.filesystem("memory")
        # Write bytes to a virtual path
        fs.pipe("virtual/path.raw", response_body)
    except Exception as e:
        print(f"Error writing to in-memory filesystem: {e}")
        return {}

    try:
        with ek_raw_io.RawSimradFile(
            "memory://virtual/path.raw", "r", storage_options={}
        ) as fid:
            config_datagram = fid.read(5)
            # config_datagram["timestamp"] = np.datetime64(
            #     config_datagram["timestamp"].replace(tzinfo=None), "[ns]"
            # )
        return config_datagram
    except Exception as e:
        print(
            f"Error during parsing of datagram for object `{gcs_blob_path}`:\n{e}"
        )
        return {}


if __name__ == "__main__":
    from aalibrary import config
    config.use_gcp_dev()

    file_path = r"C:\Users\Hannah Khan\Desktop\repos\AA-SI_aalibrary\other\test_data_dir\L0010-D20060603-T011017-ES60.raw"
    storage_options = {}
    datagram_dict = {}
    i = 0
    # while datagram_dict == [] or datagram_dict == {}:
    #     print(f"Attempt {i+1}: Streaming datagram from NCEI...")
    #     (
    #         random_ship_name,
    #         random_survey_name,
    #         random_echosounder,
    #         random_raw_file,
    #     ) = get_random_raw_file_from_ncei_cache_with_search_param(
    #         search_param="EK80"
    #     )
    #     datagram_dict = stream_datagram_dict_from_ncei(
    #         f"data/raw/{random_ship_name}/{random_survey_name}/{random_echosounder}/{random_raw_file}"
    #     )
    # print(search_ncei_object_keys_for_string("L0010-D20060603-T011017-ES60.raw"))
    # ES60 datagram example:
    # datagram_dict = stream_datagram_dict_from_ncei(
    #     s3_object_key="data/raw/Arcturus/EBS06AR/ES60/L0010-D20060603-T011017-ES60.raw",
    # )
    # EK80 datagram example:
    # datagram_dict = stream_datagram_dict_from_ncei(
    #     s3_object_key="data/raw/Endeavor/EN727_Towed_Array/EK80/Endeavor2025Jan-D20250127-T154119.raw",
    # )
    # download_raw_file_from_ncei(
    #     file_name=random_raw_file,
    #     file_type="raw",
    #     ship_name=random_ship_name,
    #     survey_name=random_survey_name,
    #     echosounder=random_echosounder,
    #     file_download_directory=os.sep.join(file_path.split(os.sep)[:-1]),
    # )
    # datagram_dict = get_datagram_dict_from_raw_file(file_path, storage_options)
    gcs_raw_blob_path = "HDD/Henry_B_Bigelow/HB2407/EK80/data/raw/D20240904-T121523.raw"
    datagram_dict = stream_datagram_dict_from_gcs(
        gcs_blob_path=gcs_raw_blob_path,
        gcs_bucket_name=get_current_gcp_bucket_name(),
    )[0]
    pprint(datagram_dict, width=100)

    print(datagram_dict["timestamp"])
    print(type(datagram_dict["timestamp"]))
    print(datagram_dict["timestamp"].tzinfo)
    print(type(datagram_dict["timestamp"].tzinfo))
    print(str(datagram_dict["timestamp"].tzinfo))
