"""
This script contains classes that have SQL queries used for interaction
with the metadata database in BigQuery.
"""

from dataclasses import dataclass


@dataclass
class MetadataQueries():
    """This class contains queries related to the upload, alteration, and
    retrieval of metadata from our BigQuery instance.
    """
    get_all_aalibrary_metadata_records: str = """
    SELECT * FROM `ggn-nmfs-aa-dev-1.metadata.aalibrary_file_metadata`"""
