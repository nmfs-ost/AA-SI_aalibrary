"""This file is used to generate a csv file of deployment codes that have been
submitted to Tugboat already. This code utilizes the TugboatAPI to achieve
this.
"""

import os
from typing import List
from pathlib import Path

import pandas as pd

# For pytests-sake
if __package__ is None or __package__ == "":
    from tugboat_api import TugboatAPI
else:
    # uses current package visibility
    from aalibrary.tugboat_api import TugboatAPI


class TugboatArchivalVerifier:
    """This class is used to get the submission status (aka archival status) of
    the cruise IDs you provide. It utilizes the TugboatAPI object to achieve
    this.
    """

    def __init__(
        self,
        cruise_ids: List[str],
        save_file: bool = False,
        file_path: str = "",
        use_dev: bool = False,
        debug: bool = False,
    ):
        self.cruise_ids: List[str] = cruise_ids
        self.save_file: bool = save_file
        self.file_path: Path = file_path
        """Defaults to current directory"""
        self.use_dev: bool = use_dev
        self.debug = debug
        self.tb_api: TugboatAPI = TugboatAPI(use_dev=self.use_dev)
        self.all_jobs: pd.DataFrame = None
        self.filtered_df: pd.DataFrame = None
        self.errors = []
        self._handle_paths()
        self.get_tugboat_submission_statuses()
        self._save_file()

    def _handle_paths(self):
        """Handles file path var."""
        if self.file_path == "" or self.file_path is None:
            self.file_path = os.sep.join(
                [
                    os.getcwd(),
                    "NCEI_JSON",
                    "submission_verification",
                    "verification.csv",
                ]
            )
            self.file_path = os.path.normpath(self.file_path)
        # Handle user-entered file path.
        else:
            self.file_path = os.path.normpath(self.file_path)

        # Convert into a Path object.
        self.file_path = Path(self.file_path)

    def _get_all_jobs(self):
        """Gets all the jobs from the Tugboat API and converts them into a
        DataFrame object.
        """

        self.all_jobs = self.tb_api.get_all_jobs()
        self.all_jobs = pd.DataFrame(self.all_jobs)

    def _check_for_non_existent_deployment_codes(self):
        filtered_deployment_codes = self.filtered_df["packageId"].tolist()
        for cruise_id in self.cruise_ids:
            if cruise_id not in filtered_deployment_codes:
                self.errors.append(
                    f"`{cruise_id}` - deployment code submission"
                    " status not found."
                )
        # Check if the filtered df is empty
        if len(self.filtered_df) == 0:
            self.errors.append(
                "No matching submission statuses found. "
                "Skipping saving empty file..."
            )
            self.save_file = False
        if len(self.errors) >= 1:
            print("ERRORS FOUND:")
            for error in self.errors:
                print("\t", error)

    def get_tugboat_submission_statuses(self):
        """Gets the Tugboat Submission status and other variables using the
        PAMTugboatAPI object.
        """

        # Get all jobs.
        self._get_all_jobs()
        # Filter out jobs that dont have the same deployment code.
        self.filtered_df = self.all_jobs[
            self.all_jobs["packageId"].isin(self.cruise_ids)
        ]
        # Do a little bit of error-checking for deployment codes that don't
        # have a submission job yet.
        self._check_for_non_existent_deployment_codes()
        if self.debug:
            print(self.filtered_df)

    def _save_file(self):
        """Saves the file if requested."""
        if self.save_file:
            # Make dirs if they do not exist.
            self.file_path.parent.mkdir(parents=True, exist_ok=True)
            self.filtered_df.to_csv(self.file_path, index=False)
            print(f"CSV file saved to: {self.file_path}")


if __name__ == "__main__":
    # Example usage:
    test_cruise_ids = [
        "RL2107",
    ]

    test_statuses = TugboatArchivalVerifier(
        cruise_ids=test_cruise_ids, use_dev=False, save_file=True
    )
