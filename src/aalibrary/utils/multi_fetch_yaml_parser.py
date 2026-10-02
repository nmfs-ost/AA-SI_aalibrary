"""This file contains the objects necessary for the parsing of a YAML file
associated with the multi-fetch algorithm developed for Active Acoustics.
This file/object will parse that yaml string/file into SQL query that can be
executed on the metadata DB."""

from pprint import pprint
from typing import List
import yaml
import sqlparse

from google.cloud import bigquery

from aalibrary.config import get_current_gcp_project_id

# For pytests-sake
if __package__ is None or __package__ == "":
    # uses current directory visibility
    from cloud_utils import (
        bq_query_to_pandas,
        setup_gbq_client_objs,
    )
    from ncei_utils import download_single_file_from_aws
    from helpers import normalize_ship_name
else:
    from aalibrary.utils.cloud_utils import (
        bq_query_to_pandas,
        setup_gbq_client_objs,
    )
    from aalibrary.utils.ncei_utils import download_single_file_from_aws
    from aalibrary.utils.helpers import (
        normalize_ship_name,
    )


def _sql_text(value) -> str:
    """A YAML scalar as SQL string content: dates and times written the way
    the column stores them, and single quotes escaped so a value cannot end
    the literal it is placed in."""
    if value is None:
        return ""
    if hasattr(value, "strftime"):
        if hasattr(value, "hour"):
            text = value.strftime("%H:%M:%S") if not hasattr(value, "year") \
                else value.strftime("%Y-%m-%d")
        else:
            text = value.strftime("%Y-%m-%d")
    elif isinstance(value, int):
        # YAML 1.1 reads 06:00:00 as a sexagesimal integer (21600).
        text = f"{value // 3600:02d}:{value % 3600 // 60:02d}:{value % 60:02d}"
    else:
        text = str(value).strip()
    return text.replace("'", "''")


class YAMLParser:
    """This class is the main class used to parse YAML objects. It's most
    important attributes are:
      - self.requests: A list containing one or more RequestParser objects.
            This is where request parsing and the SQL logic comes from.
      - self.sql_query: A string containing the SQL query that will be
            executed to fetch the results of the YAML submission. The SQL
            query, once executed, will return results as a Dataframe from the
            metadata DB, or as a list of s3 object keys that all match the
            parameters given in the requests."""

    def __init__(
        self,
        yaml_file_path: str = "",
        yaml_dict: dict = None,
        gcp_project_id: str = None,
    ):
        self.yaml_file_path = yaml_file_path
        self.yaml_dict = yaml_dict
        if gcp_project_id is None:
            self.gcp_project_id = get_current_gcp_project_id()
        else:
            self.gcp_project_id = gcp_project_id
        self.requests = []
        self.sql_query = ""
        # Load the yaml object, or read the file into a yaml object.
        self._handle_file_loading()
        self._parse_requests()
        self._generate_sql_query()

    def _handle_file_loading(self):
        if self.yaml_file_path != "":
            with open(self.yaml_file_path, "r", encoding="utf-8") as file:
                # Use safe_load for security when the source is untrusted
                self.yaml_dict = yaml.safe_load(file)

    def _print_yaml_dict(self):
        pprint(self.yaml_dict)

    def _print_requests_sql(self):
        for request in self.requests:
            print(request.sql_conditions_clause)

    def _parse_requests(self):
        for request in self.yaml_dict["requests"]:
            self.requests.append(
                RequestParser(
                    request_dict=request, gcp_project_id=self.gcp_project_id
                )
            )

    def _generate_sql_query(self):
        for idx, request in enumerate(self.requests):
            if idx == 0:
                self.sql_query += f"""(\n{request.sql_query}\n)"""
            else:
                self.sql_query += f"""\nUNION ALL\n(\n{request.sql_query}\n)"""
        # Format the SQL query for readability
        self.sql_query = sqlparse.format(
            self.sql_query, reindent=True, keyword_case="upper"
        )


class RequestParser:
    """This class handles the parsing of the requests defined in the YAML. Each
    RequestParser object refers to a request object in the YAML. The logic for
    parsing through the requests is also included in this object.
    NOTE: You can view the individual SQL conditional clause for each request
    by using print(request.sql_conditions_clause)"""

    def __init__(
        self,
        request_dict: dict = None,
        gcp_project_id: str = None,
    ):
        self.request_dict = request_dict
        if gcp_project_id is None:
            self.gcp_project_id = get_current_gcp_project_id()
        else:
            self.gcp_project_id = gcp_project_id
        self.sql_query = (
            f"""SELECT *\nFROM `{self.gcp_project_id}.metadata.ncei_cache`\n"""
        )
        self.sql_conditions_clause = """WHERE\n"""

        self._create_sql_conditions_clause()
        self._create_sql_query()

    def _create_sql_conditions_clause(self):
        self._parse_vessel_conditions()
        self._parse_survey_conditions()
        self._parse_instrument_conditions()
        self._parse_time_window_conditions()

    def _parse_vessel_conditions(self):
        if "vessel" in self.request_dict:
            ship_name_normalized = normalize_ship_name(
                self.request_dict["vessel"]
            )
            self.sql_conditions_clause += (
                f"""ship_name_normalized = '{ship_name_normalized}'\n"""
            )

    def _parse_survey_conditions(self):
        if "survey" in self.request_dict:
            self.sql_conditions_clause += """\nAND\n"""
            if isinstance(self.request_dict["survey"], str):
                self.sql_conditions_clause += (
                    f"""survey_name = '{self.request_dict["survey"]}'\n"""
                )
            elif isinstance(self.request_dict["survey"], list):
                for idx, survey_name in enumerate(self.request_dict["survey"]):
                    if idx == 0:
                        self.sql_conditions_clause += (
                            f"""survey_name = '{survey_name}'\n"""
                        )
                    else:
                        self.sql_conditions_clause += (
                            f"""OR survey_name = '{survey_name}'\n"""
                        )

    def _parse_instrument_conditions(self):
        if "instrument" in self.request_dict:
            self.sql_conditions_clause += """\nAND\n"""
            if isinstance(self.request_dict["instrument"], str):
                self.sql_conditions_clause += (
                    f"""echosounder_name = """
                    f"""'{self.request_dict["instrument"]}'\n"""
                )
            elif isinstance(self.request_dict["instrument"], list):
                for idx, echosounder_name in enumerate(
                    self.request_dict["instrument"]
                ):
                    if idx == 0:
                        self.sql_conditions_clause += (
                            f"""echosounder_name = '{echosounder_name}'\n"""
                        )
                    else:
                        self.sql_conditions_clause += (
                            f"""OR echosounder_name = '{echosounder_name}'\n"""
                        )

    def _parse_time_window_conditions(self):
        """One condition per window, OR-ed together.

        Each window is ONE continuous interval from start-date start-time to
        end-date end-time. The date and the time used to be compared
        separately (date >= start-date AND time >= start-time AND ...), which
        is only right when the window lies within a single day: a window from
        06:00 on the 3rd to 12:00 on the 5th selected 06:00-12:00 on each day
        and dropped everything overnight. The comparison is now made on the
        whole timestamp, built from the same LEFT/RIGHT pieces of
        file_datetime the old query already relied on, so it is independent
        of whether the column separates date and time with 'T' or a space.

        An end-time of "00:00:00" (or none) still means the end of the
        end-date, as before.
        """
        if "time-windows" not in self.request_dict:
            return
        windows = self.request_dict["time-windows"]
        if not isinstance(windows, list) or not windows:
            return
        stamp = "CONCAT(LEFT(file_datetime,10),' ',RIGHT(file_datetime,8))"
        clauses = []
        for time_dict in windows:
            parts = []
            start_date = _sql_text(time_dict.get("start-date"))
            if start_date:
                start_time = _sql_text(time_dict.get("start-time")) or "00:00:00"
                parts.append(f"{stamp} >= '{start_date} {start_time}'")
            end_date = _sql_text(time_dict.get("end-date"))
            if end_date:
                end_time = _sql_text(time_dict.get("end-time")) or "00:00:00"
                if end_time == "00:00:00":
                    # Handle correct end-time: no time is below 00:00:00, so
                    # a bare end date means the whole of that day.
                    end_time = "23:59:59"
                parts.append(f"{stamp} <= '{end_date} {end_time}'")
            if parts:
                clauses.append("(" + "\nAND ".join(parts) + ")")
        if clauses:
            self.sql_conditions_clause += (
                "\nAND\n(\n" + "\nOR ".join(clauses) + "\n)\n"
            )

    def _create_sql_query(self):
        self.sql_query += f"""\n{self.sql_conditions_clause}\n"""


def parse_yaml_file(yaml_file_path: str) -> str:
    """Parses a YAML file and returns a sql query string that can be executed
    on the metadata DB to return the results of the YAML submission."""
    yaml_parsed = YAMLParser(yaml_file_path=yaml_file_path)
    return yaml_parsed.sql_query


def execute_sql_query(
    sql_query: str, client: bigquery.Client = None
) -> List[str]:
    """This function executes the sql query generated from the YAML parsing.
    The results of this query will be the object keys of the files in the
    metadata DB that match the parameters given in the YAML."""
    df = bq_query_to_pandas(query=sql_query, client=client)
    return df["s3_object_key"].tolist()


def parse_yaml_and_fetch_results(
    yaml_file_path: str, client: bigquery.Client = None
) -> List[str]:
    """This function combines the two steps of parsing the YAML and executing
    the resulting SQL query to return the results of the YAML submission. If no
    client is provided, the function will create a client using the default GCP
    environment."""
    if client is None:
        client, _ = setup_gbq_client_objs()

    sql_query = parse_yaml_file(yaml_file_path=yaml_file_path)
    results = execute_sql_query(sql_query=sql_query, client=client)
    return list(set(results))


def download_results(
    results: List[str], download_directory: str = "./"
) -> None:
    """This function takes the results of the YAML submission (a list of s3
    object keys) and downloads the corresponding files from the s3 bucket.

    Args:
        results (List[str]): A list of s3 object keys to download.
        download_directory (str): The directory to download the files to.
    """
    for s3_object_key in results:
        download_single_file_from_aws(
            file_url=s3_object_key,
            download_location=download_directory,
        )


if __name__ == "__main__":
    yaml_test = YAMLParser(
        yaml_file_path=r"C:\Users\Hannah Khan\Desktop\repos\AA-SI_aalibrary\other\scripts\multi-fetch-algo-template.yaml"
    )
    # yaml_test._print_yaml_dict()
    # yaml_test._print_requests_sql()
    print(yaml_test.sql_query)
    # results = parse_yaml_and_fetch_results(
    #     yaml_file_path=r"C:\Users\Hannah Khan\Desktop\repos\AA-SI_aalibrary\other\scripts\multi-fetch-algo-template.yaml",
    #     client=bigquery.Client(project="ggn-nmfs-aa-dev-1"),
    # )
    # print(results)
    # print(len(results))
