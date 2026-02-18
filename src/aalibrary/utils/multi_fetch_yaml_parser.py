"""This file contains the objects necessary for the parsing of a YAML file
associated with the multi-fetch algorithm developed for Active Acoustics.
This file/object will parse that yaml string/file into SQL query that can be
executed on the metadata DB."""

from pprint import pprint
import yaml


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

    def __init__(self, yaml_file_path: str = "", yaml_dict: dict = None):
        self.yaml_file_path = yaml_file_path
        self.yaml_dict = yaml_dict
        self.requests = []
        # Load the yaml object, or read the file into a yaml object.
        self._handle_file_loading()
        self._parse_requests()

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
            self.requests.append(RequestParser(request_dict=request))


class RequestParser:
    """This class handles the parsing of the requests defined in the YAML. Each
    RequestParser object refers to a request object in the YAML. The logic for
    parsing through the requests is also included in this object.
    NOTE: You can view the individual SQL conditional clause for each request
    by using print(request.sql_conditions_clause)"""

    def __init__(self, request_dict: dict = None):
        self.request_dict = request_dict
        self.sql_conditions_clause = """WHERE\n"""
        self._create_sql_conditions_clause()

    def _create_sql_conditions_clause(self):
        self._parse_vessel_conditions()
        self._parse_survey_conditions()
        self._parse_instrument_conditions()
        self._parse_time_window_conditions()

    def _parse_vessel_conditions(self):
        if "vessel" in self.request_dict:
            self.sql_conditions_clause += (
                f"""ship_name = '{self.request_dict["vessel"]}'\n"""
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
        if "time-windows" in self.request_dict:
            self.sql_conditions_clause += """\nAND\n"""
            if isinstance(self.request_dict["time-windows"], list):
                for idx, time_dict in enumerate(
                    self.request_dict["time-windows"]
                ):
                    if idx == 0:
                        self.sql_conditions_clause += (
                            f"""(file_datetime >= '{time_dict["start"]}'\n"""
                        )
                        self.sql_conditions_clause += (
                            f"""file_datetime <= '{time_dict["end"]}')\n"""
                        )
                    else:
                        self.sql_conditions_clause += (
                            f"""OR (file_datetime"""
                            f""" >= '{time_dict["start"]}'\n"""
                        )
                        self.sql_conditions_clause += (
                            f"""file_datetime <= '{time_dict["end"]}')\n"""
                        )


if __name__ == "__main__":
    yaml_test = YAMLParser(
        yaml_file_path=r"C:\Users\Hannah Khan\Desktop\repos\AA-SI_aalibrary\other\scripts\multi-fetch-algo-template.yaml"
    )
    yaml_test._print_yaml_dict()
    yaml_test._print_requests_sql()
