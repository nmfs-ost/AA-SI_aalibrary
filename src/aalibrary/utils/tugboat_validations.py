"""This file contains validation functions for Tugboat submission JSON
files."""

# pylint: disable=missing-function-docstring,

import sys
import os
import json
import datetime


class TugboatValidator:
    """Class for validating Tugboat submission JSON files for WCSD data."""

    REQUIRED_FIELDS = [
        "type",
        "cruiseId",
        "platform",
        "departureDate",
        "arrivalDate",
        "sources",
        "dataSubmitter",
        "instrumentShortName",
        "datasetType",
        "dataURI",
        "calibrationState",
    ]

    def __init__(
        self,
        submission_json_file_path: str = "",
        submission_dict: dict = None,
        print_preview: bool = False,
    ):
        """
        Args:
            submission_json_file_path (str, optional): The file path to the
                Tugboat submission JSON file.
                Defaults to "".
            submission_dict (dict, optional): A dictionary representation of
                the Tugboat submission.
                Defaults to None.
            print_preview (bool, optional): Whether to print a preview of the
                loaded submission data.
                Defaults to False.
        """
        # self.tugboat_api = TugboatAPI()
        self.submission_json_file_path = submission_json_file_path
        self.submission_dict = submission_dict
        self.error_messages = []
        self.print_preview = print_preview
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
                if self.print_preview:
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

    def validate_missing_required_fields(self):
        """Validates that all required fields are not present in the
        submission."""
        missing_fields = [
            field
            for field in self.REQUIRED_FIELDS
            if field not in self.submission_dict
        ]
        if missing_fields:
            for missing_field in missing_fields:
                self.error_messages.append(
                    f"Missing required field: {missing_field}"
                )
            return False
        return True

    def validate_null_required_fields(self):
        """Validates that all required fields are not null in the
        submission."""
        null_fields = []
        for field in self.REQUIRED_FIELDS:
            if (self.submission_dict.get(field) is None) or (
                self.submission_dict.get(field) == ""
            ):
                null_fields.append(field)
        if null_fields:
            for null_field in null_fields:
                self.error_messages.append(
                    f"Null or empty required field: {null_field}"
                )
            return False
        return True

    def _validate_date(self, date_string):
        """
        Checks if a date_string is valid.

        Returns True if valid, False otherwise.
        """

        try:
            datetime.datetime.strptime(date_string, "%Y-%m-%d")
            return True
        except ValueError:
            return False

    def validate_all_datetimes(self) -> bool:
        """Validates all datetime fields in the submission dictionary.

        Returns:
            bool: True if all datetime fields are valid, False otherwise.
        """

        datetimes_are_valid = True

        for key in self.submission_dict:
            if "date" in key.lower():
                date_value = self.submission_dict.get(key)
                if date_value and not self._validate_date(date_value):
                    self.error_messages.append(
                        f"Invalid date format for field '{key}': {date_value}"
                    )
                    datetimes_are_valid = False
        return datetimes_are_valid

    def _validate_type(self):
        """Validates the types of fields in the submission."""
        if "wcsd" not in self.submission_dict["type"].lower():
            self.error_messages.append(
                "Error: 'type' field must contain 'wcsd' for active "
                "acoustics data."
            )
            return False
        return True

    def validate_sea_area(self):
        if self.submission_dict.get("seaArea", "") == "":
            self.error_messages.append("Field `seaArea` cannot be empty.")
            return False
        return True

    def validate_instrument(self):
        """This function validates the instrument and its attributes."""

        # TODO: validate that the instrument short name is in Tugboat.

    def validate_submission(self):
        """Validates a submission."""
        self.validate_missing_required_fields()
        self.validate_null_required_fields()
        self._validate_type()
        self.validate_all_datetimes()
        self.validate_sea_area()
        self.validate_instrument()
        print(len(self.error_messages), "Error(s) found during validation:")
        for error_message in self.error_messages:
            print(" - ", error_message, file=sys.stderr)


if __name__ == "__main__":
    validator = TugboatValidator(
        submission_json_file_path="other/"
        "test_tugboat_validation_submission.json"
    )
