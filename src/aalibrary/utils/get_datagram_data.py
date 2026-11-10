"""This file provides functions to get datagram data from raw data files,
including streaming datagram data from NCEI raw data files in S3. This allows
users to access relevant metadata from the datagram without having to download
the entire raw data file, which can be very large."""

import contextlib
from pprint import pprint
import uuid

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


# A .raw file is a series of datagrams, each framed by its length (a
# little-endian int32 that counts the datagram itself) before and after it.
# The first datagram is the configuration (CON0 for EK60, XML0 for EK80),
# which is all the metadata needs. Some kB for EK60, tens of kB for EK80.
_LENGTH_FIELD = 4
_MAX_FIRST_DATAGRAM = 16 * 1024 * 1024


def _first_datagram_end(head: bytes) -> int:
    """The offset of the last byte of a file's first datagram (with both of
    its length fields), from the file's first four bytes. 0 if `head` doesn't
    start a .raw file."""
    if len(head) < _LENGTH_FIELD:
        return 0
    length = int.from_bytes(head[:_LENGTH_FIELD], byteorder="little", signed=True)
    if length <= 0 or length > _MAX_FIRST_DATAGRAM:
        return 0
    return _LENGTH_FIELD + length + _LENGTH_FIELD - 1


def _parse_datagrams(data: bytes, where: str):
    """Parses the datagrams in `data`, the start of a .raw file. Returns a
    list of them, or an empty dictionary if they can't be parsed."""
    fs = fsspec.filesystem("memory")
    path = f"aalibrary-datagrams/{uuid.uuid4().hex}.raw"
    try:
        fs.pipe(path, data)
    except Exception as e:
        print(f"Error writing to in-memory filesystem: {e}")
        return {}
    try:
        with ek_raw_io.RawSimradFile(
            f"memory://{path}", "r", storage_options={}
        ) as fid:
            datagrams = fid.read(5)
        return datagrams if datagrams else {}
    except Exception as e:
        print(f"Error during parsing of datagram for object `{where}`:\n{e}")
        return {}
    finally:
        with contextlib.suppress(Exception):
            fs.rm(path)


def stream_datagram_dict_from_ncei(s3_object_key: str = None) -> dict:
    """Stream the first datagram (the configuration) of an NCEI raw data file
    in S3, reading only that datagram's bytes rather than the whole file.

    Args:
        s3_object_key (str): The S3 object key for the raw data file to stream
            the datagram from. Required.

    Returns:
        list: The datagrams read (the first is the configuration datagram,
            a dict that holds e.g. its `timestamp`), or an empty dictionary if
            an error occurs.
    """
    if s3_object_key is None:
        raise ValueError("s3_object_key must be provided.")
    # Create S3 client
    s3_client, _, _ = create_s3_objs()
    # The first four bytes give the first datagram's length.
    response = s3_client.get_object(
        Bucket=NCEI_BUCKET_NAME, Key=s3_object_key, Range="bytes=0-3"
    )
    end = _first_datagram_end(response["Body"].read())
    if not end:
        print(f"`{s3_object_key}` doesn't start like a .raw file.")
        return {}
    response = s3_client.get_object(
        Bucket=NCEI_BUCKET_NAME,
        Key=s3_object_key,
        Range=f"bytes=0-{end}",
    )
    return _parse_datagrams(response["Body"].read(), s3_object_key)


def stream_datagram_dict_from_gcs(
    gcs_blob_path: str = "",
    gcs_bucket_name: str = "",
    gcs_bucket: storage.Client.bucket = None,
) -> dict:
    """Stream the first datagram (the configuration) of a raw data file in
    GCS, reading only that datagram's bytes rather than the whole file.

    Args:
        gcs_blob_path (str): The GCS blob path for the raw data file to stream
            the datagram from. Required.
        gcs_bucket_name (str): The GCS bucket name for the raw data file to
            stream the datagram from. Optional: if not provided, the current
            GCP bucket name will be inferred from aalibrary.config.
        gcs_bucket (storage.Client.bucket): A GCS bucket object. Optional: if
            not provided, a new bucket object will be created using the
            provided gcs_bucket_name or the inferred gcs_bucket_name from
            aalibrary.config.
    Returns:
        list: The datagrams read (the first is the configuration datagram,
            a dict that holds e.g. its `timestamp`), or an empty dictionary if
            an error occurs.
    """

    # Check for required parameters, and build storage clients.
    if gcs_blob_path is None or gcs_blob_path == "":
        raise ValueError("gcs_blob_path must be provided.")
    if not gcs_bucket_name:
        gcs_bucket_name = get_current_gcp_bucket_name()
    if gcs_bucket is None:
        gcs_client = storage.Client()
        gcs_bucket = gcs_client.bucket(gcs_bucket_name)

    # The first four bytes give the first datagram's length.
    blob = gcs_bucket.blob(gcs_blob_path)
    end = _first_datagram_end(blob.download_as_bytes(start=0, end=3))
    if not end:
        print(f"`{gcs_blob_path}` doesn't start like a .raw file.")
        return {}
    return _parse_datagrams(
        blob.download_as_bytes(start=0, end=end), gcs_blob_path
    )


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
