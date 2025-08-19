"""
This script contains classes that have SQL queries used for interaction
with the metadata database in BigQuery.
"""

from dataclasses import dataclass


@dataclass
class MetadataQueries:
    """This class contains queries related to the upload, alteration, and
    retrieval of metadata from our BigQuery instance.
    """

    get_all_aalibrary_metadata_records: str = """
    SELECT * FROM `ggn-nmfs-aa-dev-1.metadata.aalibrary_file_metadata`"""

    # TODO for mike ryan
    get_all_possible_ship_names_from_database: str = """
    SELECT ship_name from `ggn-nmfs-aa-dev-1.metadata.aalibrary_file_metadata`
    """

    def get_all_surveys_associated_with_a_ship_name(self, ship_name: str = ""):
        get_all_surveys_associated_with_a_ship_name_query: str = """"""
        return get_all_surveys_associated_with_a_ship_name_query

    def get_all_echosounders_used_in_a_survey(self, survey: str = ""): ...

    def get_all_netcdf_files_in_database(self): ...


if __name__ == "__main__":
    ...
