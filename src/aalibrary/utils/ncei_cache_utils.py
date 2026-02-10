"""This file contains code pertaining to auxiliary functions related to parsing
through NCEI's s3 bucket using the cache created by the
`daily_ncei_cache.ipynb` script."""

from typing import List

from google.cloud import bigquery

# For pytests-sake
if __package__ is None or __package__ == "":
    # uses current directory visibility
    from cloud_utils import (
        bq_query_to_pandas,
        setup_gbq_client_objs,
    )
    from helpers import normalize_ship_name
else:
    from aalibrary.utils.cloud_utils import (
        bq_query_to_pandas,
        setup_gbq_client_objs,
    )
    from aalibrary.utils.helpers import (
        normalize_ship_name,
    )


def get_all_ship_names_in_ncei_cache(
    normalize: bool = True,
    gcp_bq_client: bigquery.Client = None,
    return_full_paths: bool = False,
) -> List[str]:
    """Gets all of the ship names from the NCEI cache in BigQuery for faster
    retrieval. This is based on all of the folders listed under the
    `data/raw/` prefix.

    Args:
        normalize (bool, optional): Whether or not to normalize the ship_name
            attribute to how GCP stores it.
            NOTE: Can only normalize if `return_full_paths=False`.
            Defaults to False.
        gcp_bq_client (bigquery.Client, optional): A GCP BigQuery client
            object. If not provided, one will be created.
            NOTE: By default, the created object will be using a connection to
            the `ggn-nmfs-aa-dev-1` project.
        return_full_paths (bool, optional): Whether or not you want a full
            path from bucket root to the subdirectory returned. Set to false
            if you only want the subdirectory names listed. Defaults to False.
    Returns:
        List[str]: A list of strings, each being the ship name. Whether
            these are full paths or just ship names are specified by the
            `return_full_paths` parameter.
    """

    gcp_bq_client = setup_gbq_client_objs()[0]

    if return_full_paths:
        query = """SELECT s3_object_key
        FROM `ggn-nmfs-aa-dev-1.metadata.ncei_cache`"""
        df = bq_query_to_pandas(gcp_bq_client, query)
        df["full_ship_name_path"] = df["s3_object_key"].apply(
            lambda x: (
                "/".join(x.split("/")[:3])
                if x.startswith("data/raw/")
                else None
            )
        )
        return df["full_ship_name_path"].dropna().unique().tolist()

    query = """SELECT DISTINCT(ship_name)
    FROM `ggn-nmfs-aa-dev-1.metadata.ncei_cache`"""
    df = bq_query_to_pandas(gcp_bq_client, query)

    if normalize:
        df["ship_name"] = df["ship_name"].apply(normalize_ship_name)

    return df["ship_name"].tolist()


def get_all_surveys_in_ncei_cache(
    gcp_bq_client: bigquery.Client = None,
    return_full_paths: bool = False,
) -> List[str]:
    """Gets all of the survey names from the NCEI cache in BigQuery for faster
    retrieval. This is based on all of the folders listed under the
    `data/raw/{ship_name}/` prefix.

    Args:
        gcp_bq_client (bigquery.Client, optional): A GCP BigQuery client
            object. If not provided, one will be created.
            NOTE: By default, the created object will be using a connection to
            the `ggn-nmfs-aa-dev-1` project.
        return_full_paths (bool, optional): Whether or not you want a full
            path from bucket root to the subdirectory returned. Set to false
            if you only want the subdirectory names listed. Defaults to False.
    Returns:
        List[str]: A list of strings, each being the survey name. Whether
            these are full paths or just survey names are specified by the
            `return_full_paths` parameter.
    """

    gcp_bq_client = setup_gbq_client_objs()[0]

    if return_full_paths:
        query = """SELECT s3_object_key
        FROM `ggn-nmfs-aa-dev-1.metadata.ncei_cache`"""
        df = bq_query_to_pandas(gcp_bq_client, query)
        df["full_survey_name_path"] = df["s3_object_key"].apply(
            lambda x: (
                "/".join(x.split("/")[:4])
                if x.startswith("data/raw/")
                else None
            )
        )
        return df["full_survey_name_path"].dropna().unique().tolist()

    query = """SELECT DISTINCT(survey_name)
    FROM `ggn-nmfs-aa-dev-1.metadata.ncei_cache`"""
    df = bq_query_to_pandas(gcp_bq_client, query)

    return df["survey_name"].tolist()


def get_all_survey_names_from_a_ship_in_ncei_cache(
    ship_name: str = "",
    gcp_bq_client: bigquery.Client = None,
    return_full_paths: bool = False,
) -> List[str]:
    """Gets all of the survey names from a specific ship in the NCEI cache in
    BigQuery.

    Args:
        ship_name (str): The name of the ship to get survey names for.
            NOTE: The ship's name MUST be spelled exactly as it is in NCEI. Use
            the `get_all_ship_names_in_ncei` function to see all possible NCEI
            ship names.
        gcp_bq_client (bigquery.Client, optional): A GCP BigQuery client
            object. If not provided, one will be created.
            NOTE: By default, the created object will be using a connection to
            the `ggn-nmfs-aa-dev-1` project.
        return_full_paths (bool, optional): Whether or not you want a full
            path from bucket root to the subdirectory returned. Set to false
            if you only want the subdirectory names listed. Defaults to False.
    Returns:
        List[str]: A list of strings, each being the survey name for a
            specific ship.
    """
    gcp_bq_client = (
        setup_gbq_client_objs()[0] if gcp_bq_client is None else gcp_bq_client
    )
    ship_name_normalized = normalize_ship_name(ship_name=ship_name)

    if return_full_paths:
        query = f"""SELECT s3_object_key
        FROM `ggn-nmfs-aa-dev-1.metadata.ncei_cache`
        WHERE ship_name_normalized = '{ship_name_normalized}'"""
        df = bq_query_to_pandas(gcp_bq_client, query)
        df["full_survey_name_path"] = df["s3_object_key"].apply(
            lambda x: (
                "/".join(x.split("/")[:4])
                if x.startswith("data/raw/")
                else None
            )
        )
        return df["full_survey_name_path"].dropna().unique().tolist()

    query = f"""SELECT DISTINCT(survey_name)
    FROM `ggn-nmfs-aa-dev-1.metadata.ncei_cache`
    WHERE ship_name_normalized = '{ship_name_normalized}'"""
    df = bq_query_to_pandas(gcp_bq_client, query)

    return df["survey_name"].tolist()


def get_all_echosounders_in_a_survey_in_ncei_cache(
    ship_name: str = "",
    survey_name: str = "",
    gcp_bq_client: bigquery.Client = None,
    return_full_paths: bool = False,
) -> List[str]:
    """Gets all of the echosounder names from a specific survey of a specific
    ship in the NCEI cache in BigQuery.

    Args:
        ship_name (str): The name of the ship to get echosounder names for.
            NOTE: The ship's name MUST be spelled exactly as it is in NCEI. Use
            the `get_all_ship_names_in_ncei` function to see all possible NCEI
            ship names.
        survey_name (str): The name of the survey to get echosounder names for.
            NOTE: The survey's name MUST be spelled exactly as it is in NCEI.
            Use  the `get_all_surveys_in_ncei` function to see all possible
            NCEI survey names.
        gcp_bq_client (bigquery.Client, optional): A GCP BigQuery client
            object. If not provided, one will be created.
            NOTE: By default, the created object will be using a connection to
            the `ggn-nmfs-aa-dev-1` project.
        return_full_paths (bool, optional): Whether or not you want a full
            path from bucket root to the subdirectory returned. Set to false
            if you only want the subdirectory names listed. Defaults to False.

    Returns:
        List[str]: A list of strings, each being the echosounder name for a
            specific survey of a specific ship.
    """
    gcp_bq_client = (
        setup_gbq_client_objs()[0] if gcp_bq_client is None else gcp_bq_client
    )
    ship_name_normalized = normalize_ship_name(ship_name=ship_name)

    if return_full_paths:
        query = f"""SELECT s3_object_key
        FROM `ggn-nmfs-aa-dev-1.metadata.ncei_cache`
        WHERE ship_name_normalized = '{ship_name_normalized}'
        AND survey_name = '{survey_name}'"""
        df = bq_query_to_pandas(gcp_bq_client, query)
        df["full_echosounder_name_path"] = df["s3_object_key"].apply(
            lambda x: (
                "/".join(x.split("/")[:5])
                if x.startswith("data/raw/")
                else None
            )
        )
        return df["full_echosounder_name_path"].dropna().unique().tolist()

    query = f"""SELECT DISTINCT(echosounder_name)
    FROM `ggn-nmfs-aa-dev-1.metadata.ncei_cache`
    WHERE ship_name_normalized = '{ship_name_normalized}'
    AND survey_name = '{survey_name}'"""
    df = bq_query_to_pandas(gcp_bq_client, query)

    return df["echosounder_name"].tolist()


def get_all_echosounders_in_ncei_cache(
    gcp_bq_client: bigquery.Client = None,
    return_full_paths: bool = False,
) -> List[str]:
    """Gets all of the echosounder names in the NCEI cache in BigQuery.

    Args:
        gcp_bq_client (bigquery.Client, optional): A GCP BigQuery client
            object. If not provided, one will be created.
            NOTE: By default, the created object will be using a connection to
            the `ggn-nmfs-aa-dev-1` project.
        return_full_paths (bool, optional): Whether or not you want a full
            path from bucket root to the subdirectory returned. Set to false
            if you only want the subdirectory names listed. Defaults to False.

    Returns:
        List[str]: A list of strings, each being an echosounder name in NCEI.
    """
    gcp_bq_client = (
        setup_gbq_client_objs()[0] if gcp_bq_client is None else gcp_bq_client
    )

    if return_full_paths:
        query = """SELECT s3_object_key
        FROM `ggn-nmfs-aa-dev-1.metadata.ncei_cache`
        WHERE echosounder_name IS NOT NULL"""
        df = bq_query_to_pandas(gcp_bq_client, query)
        df["full_echosounder_name_path"] = df["s3_object_key"].apply(
            lambda x: (
                "/".join(x.split("/")[:5])
                if x.startswith("data/raw/")
                else None
            )
        )
        return df["full_echosounder_name_path"].dropna().unique().tolist()

    query = """SELECT DISTINCT(echosounder_name)
    FROM `ggn-nmfs-aa-dev-1.metadata.ncei_cache`
    WHERE echosounder_name IS NOT NULL"""
    df = bq_query_to_pandas(gcp_bq_client, query)

    return df["echosounder_name"].tolist()


def get_all_file_names_from_survey_in_ncei_cache(
    ship_name: str = "",
    survey_name: str = "",
    gcp_bq_client: bigquery.Client = None,
    return_full_paths: bool = False,
) -> List[str]:
    """Gets all of the file names from a specific survey of a specific
    ship in the NCEI cache in BigQuery.

    Args:
        ship_name (str): The name of the ship to get file names for.
            NOTE: The ship's name MUST be spelled exactly as it is in NCEI. Use
            the `get_all_ship_names_in_ncei` function to see all possible NCEI
            ship names.
        survey_name (str): The name of the survey to get file names for.
            NOTE: The survey's name MUST be spelled exactly as it is in NCEI.
            Use  the `get_all_surveys_in_ncei` function to see all possible
            NCEI survey names.
        gcp_bq_client (bigquery.Client, optional): A GCP BigQuery client
            object. If not provided, one will be created.
            NOTE: By default, the created object will be using a connection to
            the `ggn-nmfs-aa-dev-1` project.
        return_full_paths (bool, optional): Whether or not you want a full
            path from bucket root to the subdirectory returned. Set to false
            if you only want the subdirectory names listed. Defaults to False.

    Returns:
        List[str]: A list of strings, each being a file name for a specific
            survey of a specific ship.
    """
    gcp_bq_client = (
        setup_gbq_client_objs()[0] if gcp_bq_client is None else gcp_bq_client
    )
    ship_name_normalized = normalize_ship_name(ship_name=ship_name)

    if return_full_paths:
        query = f"""SELECT s3_object_key
        FROM `ggn-nmfs-aa-dev-1.metadata.ncei_cache`
        WHERE ship_name_normalized = '{ship_name_normalized}'
        AND survey_name = '{survey_name}'"""
        df = bq_query_to_pandas(gcp_bq_client, query)
        return df["s3_object_key"].dropna().unique().tolist()

    query = f"""SELECT file_name
    FROM `ggn-nmfs-aa-dev-1.metadata.ncei_cache`
    WHERE ship_name_normalized = '{ship_name_normalized}'
    AND survey_name = '{survey_name}'"""
    df = bq_query_to_pandas(gcp_bq_client, query)
    return df["file_name"].dropna().unique().tolist()


def get_all_file_names_for_a_surveys_echosounder(
    ship_name: str = "",
    survey_name: str = "",
    echosounder_name: str = "",
    gcp_bq_client: bigquery.Client = None,
    return_full_paths: bool = False,
):
    """Gets all of the file names from a specific survey of a specific
    ship in the NCEI cache in BigQuery.

    Args:
        ship_name (str): The name of the ship to get file names for.
            NOTE: The ship's name MUST be spelled exactly as it is in NCEI. Use
            the `get_all_ship_names_in_ncei` function to see all possible NCEI
            ship names.
        survey_name (str): The name of the survey to get file names for.
            NOTE: The survey's name MUST be spelled exactly as it is in NCEI.
            Use  the `get_all_surveys_in_ncei` function to see all possible
            NCEI survey names.
        echosounder_name (str): The name of the echosounder to get file names
            for.
            NOTE: The echosounder's name MUST be spelled exactly as it is in
            NCEI. Use  the `get_all_echosounders_in_ncei_cache` function to see
            all possible NCEI echosounder names.
        gcp_bq_client (bigquery.Client, optional): A GCP BigQuery client
            object. If not provided, one will be created.
            NOTE: By default, the created object will be using a connection to
            the `ggn-nmfs-aa-dev-1` project.
        return_full_paths (bool, optional): Whether or not you want a full
            path from bucket root to the subdirectory returned. Set to false
            if you only want the subdirectory names listed. Defaults to False.

    Returns:
        List[str]: A list of strings, each being a file name for a specific
            survey of a specific ship.
    """
    gcp_bq_client = (
        setup_gbq_client_objs()[0] if gcp_bq_client is None else gcp_bq_client
    )
    ship_name_normalized = normalize_ship_name(ship_name=ship_name)

    if return_full_paths:
        query = f"""SELECT s3_object_key
        FROM `ggn-nmfs-aa-dev-1.metadata.ncei_cache`
        WHERE ship_name_normalized = '{ship_name_normalized}'
        AND survey_name = '{survey_name}'
        AND echosounder_name = '{echosounder_name}'"""
        df = bq_query_to_pandas(gcp_bq_client, query)
        return df["s3_object_key"].dropna().unique().tolist()

    query = f"""SELECT file_name
    FROM `ggn-nmfs-aa-dev-1.metadata.ncei_cache`
    WHERE ship_name_normalized = '{ship_name_normalized}'
    AND survey_name = '{survey_name}'
    AND echosounder_name = '{echosounder_name}'"""
    df = bq_query_to_pandas(gcp_bq_client, query)
    return df["file_name"].dropna().unique().tolist()


def get_all_raw_file_names_from_survey_in_ncei_cache(
    ship_name: str = "",
    survey_name: str = "",
    gcp_bq_client: bigquery.Client = None,
    return_full_paths: bool = False,
):
    """Gets all of the raw file names from a specific survey of a specific
    ship in the NCEI cache in BigQuery.

    Args:
        ship_name (str): The name of the ship to get file names for.
            NOTE: The ship's name MUST be spelled exactly as it is in NCEI. Use
            the `get_all_ship_names_in_ncei` function to see all possible NCEI
            ship names.
        survey_name (str): The name of the survey to get file names for.
            NOTE: The survey's name MUST be spelled exactly as it is in NCEI.
            Use  the `get_all_surveys_in_ncei` function to see all possible
            NCEI survey names.
        gcp_bq_client (bigquery.Client, optional): A GCP BigQuery client
            object. If not provided, one will be created.
            NOTE: By default, the created object will be using a connection to
            the `ggn-nmfs-aa-dev-1` project.
        return_full_paths (bool, optional): Whether or not you want a full
            path from bucket root to the subdirectory returned. Set to false
            if you only want the subdirectory names listed. Defaults to False.

    Returns:
        List[str]: A list of strings, each being a raw file name for a specific
            survey of a specific ship.
    """
    gcp_bq_client = (
        setup_gbq_client_objs()[0] if gcp_bq_client is None else gcp_bq_client
    )
    ship_name_normalized = normalize_ship_name(ship_name=ship_name)

    if return_full_paths:
        query = f"""SELECT s3_object_key
        FROM `ggn-nmfs-aa-dev-1.metadata.ncei_cache`
        WHERE ship_name_normalized = '{ship_name_normalized}'
        AND survey_name = '{survey_name}'
        AND file_type = 'raw'"""
        df = bq_query_to_pandas(gcp_bq_client, query)
        return df["s3_object_key"].dropna().unique().tolist()

    query = f"""SELECT file_name
    FROM `ggn-nmfs-aa-dev-1.metadata.ncei_cache`
    WHERE ship_name_normalized = '{ship_name_normalized}'
    AND survey_name = '{survey_name}'
    AND file_type = 'raw'"""
    df = bq_query_to_pandas(gcp_bq_client, query)
    return df["file_name"].dropna().unique().tolist()


def get_random_raw_file_from_ncei_cache(
    gcp_bq_client: bigquery.Client = None,
) -> List[str]:
    """Gets a random raw file name from the NCEI cache in BigQuery along with
    its associated ship name, survey name, and echosounder name.

    Args:
        gcp_bq_client (bigquery.Client, optional): A GCP BigQuery client
            object. If not provided, one will be created.
            NOTE: By default, the created object will be using a connection to
            the `ggn-nmfs-aa-dev-1` project.

    Returns:
        List[str]: A list object with strings denoting each parameter required
            for creating a raw file object.
            Ex. [
                random_ship_name,
                random_survey_name,
                random_echosounder,
                random_raw_file,
            ]
    """
    random_ship_name_query = """SELECT ship_name
    FROM `ggn-nmfs-aa-dev-1.metadata.ncei_cache`
    ORDER BY RAND()
    LIMIT 1"""
    random_ship_name_df = bq_query_to_pandas(
        gcp_bq_client, random_ship_name_query
    )
    random_ship_name = random_ship_name_df["ship_name"].iloc[0]

    random_survey_name_query = f"""SELECT survey_name
    FROM `ggn-nmfs-aa-dev-1.metadata.ncei_cache`
    WHERE ship_name = '{random_ship_name}'
    ORDER BY RAND()
    LIMIT 1"""
    random_survey_name_df = bq_query_to_pandas(
        gcp_bq_client, random_survey_name_query
    )
    random_survey_name = random_survey_name_df["survey_name"].iloc[0]

    random_echosounder_query = f"""SELECT echosounder_name
    FROM `ggn-nmfs-aa-dev-1.metadata.ncei_cache`
    WHERE ship_name = '{random_ship_name}'
    AND survey_name = '{random_survey_name}'
    ORDER BY RAND()
    LIMIT 1"""
    random_echosounder_df = bq_query_to_pandas(
        gcp_bq_client, random_echosounder_query
    )
    random_echosounder = random_echosounder_df["echosounder_name"].iloc[0]

    random_raw_file_query = f"""SELECT file_name
        FROM `ggn-nmfs-aa-dev-1.metadata.ncei_cache`
        WHERE ship_name = '{random_ship_name}'
        AND survey_name = '{random_survey_name}'
        AND echosounder_name = '{random_echosounder}'
        AND file_type = 'raw'
        ORDER BY RAND()
        LIMIT 1"""
    random_raw_file_df = bq_query_to_pandas(
        gcp_bq_client, random_raw_file_query
    )
    random_raw_file = random_raw_file_df["file_name"].iloc[0]

    return [
        random_ship_name,
        random_survey_name,
        random_echosounder,
        random_raw_file,
    ]


def search_ncei_object_keys_for_string(
    search_param: str = "",
) -> List[str]:
    """Searches through the `s3_object_key` column of the NCEI cache in
    BigQuery for a specific string and returns all of the object keys that
    contain that string.

    Args:
        search_param (str): The string to search for in the `s3_object_key`
            column of the NCEI cache in BigQuery.

    Returns:
        List[str]: A list of strings, each being an `s3_object_key` that
            contains the search string.
    """
    gcp_bq_client = setup_gbq_client_objs()[0]

    query = f"""SELECT s3_object_key
    FROM `ggn-nmfs-aa-dev-1.metadata.ncei_cache`
    WHERE s3_object_key LIKE '%{search_param}%'"""
    df = bq_query_to_pandas(gcp_bq_client, query)
    return df["s3_object_key"].dropna().unique().tolist()


def get_echosounder_from_raw_file(
    file_name: str = "",
    ship_name: str = "",
    survey_name: str = "",
    gcp_bq_client: bigquery.Client = None,
) -> str:
    """Gets the echosounder name associated with a specific raw file in the
    NCEI cache in BigQuery.

    Args:
        file_name (str): The name of the raw file to get the echosounder name
            for.
            NOTE: The file name MUST be spelled exactly as it is in NCEI. Use
            the `get_all_raw_file_names_from_survey_in_ncei_cache` function to
            see all possible NCEI raw file names.
        ship_name (str): The name of the ship to get the echosounder name for.
            NOTE: The ship's name MUST be spelled exactly as it is in NCEI. Use
            the `get_all_ship_names_in_ncei` function to see all possible NCEI
            ship names.
        survey_name (str): The name of the survey to get the echosounder name
            for.
            NOTE: The survey's name MUST be spelled exactly as it is in NCEI.
            Use  the `get_all_surveys_in_ncei` function to see all possible
            NCEI survey names.
        gcp_bq_client (bigquery.Client, optional): A GCP BigQuery client
            object. If not provided, one will be created.
            NOTE: By default, the created object will be using a connection to
            the `ggn-nmfs-aa-dev-1` project.

    Returns:
        str: The echosounder name associated with the raw file, or an empty
            string if no echosounder name is found.
    """
    gcp_bq_client = (
        setup_gbq_client_objs()[0] if gcp_bq_client is None else gcp_bq_client
    )
    ship_name_normalized = normalize_ship_name(ship_name=ship_name)

    query = f"""SELECT echosounder_name
    FROM `ggn-nmfs-aa-dev-1.metadata.ncei_cache`
    WHERE ship_name_normalized = '{ship_name_normalized}'
    AND survey_name = '{survey_name}'
    AND file_name = '{file_name}'
    AND file_type = 'raw'"""
    df = bq_query_to_pandas(gcp_bq_client, query)
    if df.empty:
        return ""
    return df["echosounder_name"].iloc[0]


if __name__ == "__main__":
    gcp_bq_client, _ = setup_gbq_client_objs(project_id="ggn-nmfs-aa-dev-1")

    # Test get_all_ship_names_in_ncei_cache
    print(
        get_all_ship_names_in_ncei_cache(
            return_full_paths=True, normalize=False
        )
    )
    print(
        get_all_ship_names_in_ncei_cache(
            return_full_paths=False, normalize=True
        )
    )

    # Test get_all_surveys_in_ncei_cache
    print(
        get_all_surveys_in_ncei_cache(
            gcp_bq_client=gcp_bq_client, return_full_paths=True
        )
    )
    print(
        get_all_surveys_in_ncei_cache(
            gcp_bq_client=gcp_bq_client, return_full_paths=False
        )
    )

    # Test get_all_survey_names_from_a_ship_in_ncei_cache
    print(
        get_all_survey_names_from_a_ship_in_ncei_cache(
            ship_name="Reuben_Lasker", gcp_bq_client=gcp_bq_client
        )
    )
    print(
        get_all_survey_names_from_a_ship_in_ncei_cache(
            ship_name="Reuben_Lasker",
            gcp_bq_client=gcp_bq_client,
            return_full_paths=True,
        )
    )

    # Test get_all_echosounders_in_a_survey_in_ncei_cache
    print(
        get_all_echosounders_in_a_survey_in_ncei_cache(
            ship_name="Reuben_Lasker",
            survey_name="RL2107",
            gcp_bq_client=gcp_bq_client,
            return_full_paths=True,
        )
    )
    print(
        get_all_echosounders_in_a_survey_in_ncei_cache(
            ship_name="Reuben_Lasker",
            survey_name="RL2107",
            gcp_bq_client=gcp_bq_client,
            return_full_paths=False,
        )
    )

    # Test get_all_echosounders_in_ncei_cache
    print(
        get_all_echosounders_in_ncei_cache(
            gcp_bq_client=gcp_bq_client, return_full_paths=True
        )
    )
    print(
        get_all_echosounders_in_ncei_cache(
            gcp_bq_client=gcp_bq_client, return_full_paths=False
        )
    )

    # Test get_all_file_names_from_survey_in_ncei_cache
    print(
        get_all_file_names_from_survey_in_ncei_cache(
            ship_name="Reuben_Lasker",
            survey_name="RL2107",
            gcp_bq_client=gcp_bq_client,
            return_full_paths=True,
        )
    )
    print(
        get_all_file_names_from_survey_in_ncei_cache(
            ship_name="Reuben_Lasker",
            survey_name="RL2107",
            gcp_bq_client=gcp_bq_client,
            return_full_paths=False,
        )
    )

    # Test get_all_file_names_for_a_surveys_echosounder
    print(
        get_all_file_names_for_a_surveys_echosounder(
            ship_name="Reuben_Lasker",
            survey_name="RL2107",
            echosounder_name="EK60",
            gcp_bq_client=gcp_bq_client,
            return_full_paths=True,
        )
    )
    print(
        get_all_file_names_for_a_surveys_echosounder(
            ship_name="Reuben_Lasker",
            survey_name="RL2107",
            echosounder_name="EK60",
            gcp_bq_client=gcp_bq_client,
            return_full_paths=False,
        )
    )

    # Test get_all_raw_file_names_from_survey_in_ncei_cache
    print(
        get_all_raw_file_names_from_survey_in_ncei_cache(
            ship_name="Reuben_Lasker",
            survey_name="RL2107",
            gcp_bq_client=gcp_bq_client,
            return_full_paths=True,
        )
    )
    print(
        get_all_raw_file_names_from_survey_in_ncei_cache(
            ship_name="Reuben_Lasker",
            survey_name="RL2107",
            gcp_bq_client=gcp_bq_client,
            return_full_paths=False,
        )
    )

    # Test get_random_raw_file_from_ncei_cache
    print(get_random_raw_file_from_ncei_cache(gcp_bq_client=gcp_bq_client))

    # Test search_ncei_object_keys_for_string
    print(search_ncei_object_keys_for_string(search_param="RL2107"))

    # Test get_echosounder_from_raw_file
    print(
        get_echosounder_from_raw_file(
            file_name="XYZ.raw",
            ship_name="Reuben_Lasker",
            survey_name="RL2107",
            gcp_bq_client=gcp_bq_client,
        )
    )
