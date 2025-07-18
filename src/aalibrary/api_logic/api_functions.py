import logging
from aalibrary import utils
from aalibrary.raw_file import RawFile
    

def stream_raw_file_from_ncei(
    file_name: str = "",
    file_type: str = "raw",
    ship_name: str = "",
    survey_name: str = "",
    echosounder: str = "",
    data_source: str = "NCEI",
    is_metadata: bool = False,
    debug: bool = False,
):
    """Similar to download_raw_file_from_ncei but instead of downloading the file,
    it returns a file object that can be streamed to the client.
    
    This function is designed for API use to enable streaming files directly to clients.

    Args:
        file_name (str, optional): The file name (includes extension).
            Defaults to "".
        file_type (str, optional): The file type (do not include the dot ".").
            Defaults to "".
        ship_name (str, optional): The ship name associated with this survey.
            Defaults to "".
        survey_name (str, optional): The survey name/identifier.
            Defaults to "".
        echosounder (str, optional): The echosounder used to gather the data.
            Defaults to "".
        data_source (str, optional): The source of the file. Necessary due to
            the way the storage bucket is organized. Can be one of
            ["NCEI", "OMAO", "HDD"]. Defaults to "".
        is_metadata (bool, optional): Whether or not the file is a metadata
            file. Necessary since files that are considered metadata (metadata
            json, or readmes) are stored in a separate directory. Defaults to
            False.
        debug (bool, optional): Whether or not to print debug statements.
            Defaults to False.
            
    Returns:
        tuple: A tuple containing:
            - stream (file-like object): A file-like object that can be streamed
            - content_type (str): The content type of the file
            - filename (str): The name of the file for use in Content-Disposition headers
    """

    print(f"STREAMING {file_name} FROM NCEI...")
    try:
        s3_client, s3_resource, s3_bucket = utils.cloud_utils.create_s3_objs()
    except Exception as e:
        logging.error(f"CANNOT ESTABLISH CONNECTION TO S3 BUCKET..\n{e}")
        raise

    rf = RawFile(
        file_name=file_name,
        file_type=file_type,
        ship_name=ship_name,
        survey_name=survey_name,
        echosounder=echosounder,
        data_source=data_source,
        # We don't need a download directory as we're streaming
        file_download_directory="",
        is_metadata=is_metadata,
        upload_to_gcp=False,
        debug=debug,
        s3_resource=s3_resource,
    )

    content_type = "application/octet-stream"
    
    if rf.raw_file_exists_in_ncei:
        # Get the file object from S3
        s3_object = s3_resource.Object("noaa-wcsd-pds", rf.raw_file_s3_object_key)
        
        # Get the response object which can be streamed
        response = s3_object.get()
        file_stream = response['Body']
        
        # Determine content type based on file extension
        if file_name.lower().endswith('.raw'):
            content_type = "application/octet-stream"
        elif file_name.lower().endswith('.idx'):
            content_type = "application/octet-stream"
        elif file_name.lower().endswith('.bot'):
            content_type = "application/octet-stream"
        
        return file_stream, content_type, file_name
    else:
        raise FileNotFoundError(f"File {file_name} does not exist in NCEI storage")

