"""This file contains validation functions for Tugboat submission JSON
files."""

# pylint: disable=missing-function-docstring,

import sys
import os
import json
import datetime


class TugboatValidator:
    """Class for validating Tugboat submission JSON files."""

    def __init__(
        self, submission_json_file_path: str = "", submission_dict: dict = None
    ):
        # self.tugboat_api = TugboatAPI()
        self.submission_json_file_path = submission_json_file_path
        self.submission_dict = submission_dict
        self._handle_submission_file_loading()
        self.validate_submission()

    def _handle_submission_file_loading(self):
        # Load the submission into a dict object if a file path is provided.
        if os.path.isfile(self.submission_json_file_path):
            try:
                with open(
                    self.submission_json_file_path, "r", encoding="utf-8"
                ) as file:
                    self.submission_dict = json.load(file)
                print("Data successfully loaded for validation.")
                print(self.submission_dict)
            except FileNotFoundError:
                print(
                    f"Error: The file '{self.submission_json_file_path}' "
                    "was not found."
                )
            except json.JSONDecodeError:
                print(
                    f"Error: Failed to decode JSON from the file: "
                    f"`{self.submission_json_file_path}`."
                )

    def validate_datetime(self, date_string):
        """
        Checks if a date_string is valid.

        Returns True if valid, False otherwise.
        """

        try:
            datetime.datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S")
            return True
        except ValueError:
            return False

    def validate_all_datetimes(self):
        """Validates all datetime fields in the submission dictionary.

        Returns:
            bool: True if all datetime fields are valid, False otherwise.
        """

        for key in self.submission_dict:
            if "time" in key.lower() or "date" in key.lower():
                date_value = self.submission_dict.get(key)
                if date_value and not self.validate_datetime(date_value):
                    print(
                        f"Invalid date format for field '{key}': {date_value}"
                    )
                    return False
        return True

    def _validate_type(self):
        """Validates the types of fields in the submission."""
        if "wcsd" not in self.submission_dict["type"].lower():
            print(
                "Error: 'type' field must contain 'wcsd' for active "
                "acoustics data."
            )

    def validate_sea_area(self):
        if self.submission_dict.get("seaArea", "") == "":
            print("Field `seaArea` cannot be empty.", file=sys.stderr)

    def validate_sources(self):
        if self.submission_dict.get("sources", []) == []:
            print("Field `sources` cannot be empty.", file=sys.stderr)

    def validate_arrival_port(self):
        if self.submission_dict.get("arrivalPort", "") == "":
            print("Field `arrivalPort` cannot be empty.", file=sys.stderr)

    def validate_departure_port(self):
        if self.submission_dict.get("departurePort", "") == "":
            print("Field `departurePort` cannot be empty.", file=sys.stderr)

    def validate_instruments(self):
        """This function validates all instruments and their attributes."""

        for instrument in self.submission_dict.get("instruments", {}):
            if instrument.get("releaseDate", "") == "":
                print("Field `releaseDate` cannot be empty.", file=sys.stderr)
            if instrument.get("status", "") == "":
                print("Field `status` cannot be empty.", file=sys.stderr)
            if instrument.get("calibrationDate", "") == "":
                print("Field `calibrationDate` cannot be empty.",
                      file=sys.stderr)

    def validate_submission(self):
        """Validates a submission."""
        self._validate_type()
        self.validate_all_datetimes()
        self.validate_sea_area()
        self.validate_sources()
        self.validate_arrival_port()
        self.validate_departure_port()
        self.validate_instruments()


if __name__ == "__main__":
    validator = TugboatValidator(submission_json_file_path=sys.argv[1])
