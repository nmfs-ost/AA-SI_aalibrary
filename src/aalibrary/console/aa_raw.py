from aalibrary.ingestion import download_raw_file_from_ncei
import argparse
import sys
from pathlib import Path


def print_help():
    help_text = """
Usage: aa-raw [OPTIONS]

Options:
  --file_name                Name of the file to download. (Required)
  --file_type                Type of the file. (Required)
  --ship_name                Name of the ship. (Required)
  --survey_name              Name of the survey. (Required)
  --echosounder              Type of echosounder. (Required)
  --data_source              Source of the data. (Required)
  --file_download_directory  Directory to download the file. (Required)
  --config_file_path         Path to the Azure configuration file. (Required)
  --upload_to_gcp            Flag to upload the file to GCP. (Optional)
  --debug                    Enable debug mode. (Optional)

Description:
  This script downloads raw files from Azure and optionally uploads them to GCP.

Example:
  aa-raw --file_name "example.raw" --file_type "raw" --ship_name "ShipName" \\
         --survey_name "SurveyName" --echosounder "EchosounderType" \\
         --data_source "DataSource" --file_download_directory "/path/to/dir" \\
         --config_file_path "/path/to/config.ini" --upload_to_gcp True
"""
    print(help_text)


def main():

    # Display help if no arguments are provided or if --help is explicitly passed
    if len(sys.argv) == 1 or "--help" in sys.argv:
        print_help()
        sys.exit(0)

    parser = argparse.ArgumentParser(description="Download raw file from Azure.")

    parser.add_argument(
        "--file_name", required=True, help="Name of the file to download."
    )
    parser.add_argument(
        "--file_type", required=False, default="raw", help="Type of the file."
    )
    parser.add_argument("--ship_name", required=True, help="Name of the ship.")
    parser.add_argument("--survey_name", required=True, help="Name of the survey.")
    parser.add_argument("--echosounder", required=True, help="Type of echosounder.")
    parser.add_argument(
        "--data_source", required=False, default="NCEI", help="Source of the data."
    )
    parser.add_argument(
        "--file_download_directory",
        required=False,
        default=".",
        help="Directory to download the file.",
    )
    parser.add_argument(
        "--upload_to_gcp", action="store_true", help="Flag to upload the file to GCP."
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug mode.")

    args = parser.parse_args()

    download_raw_file_from_ncei(
        file_name=args.file_name,
        file_type=args.file_type,
        ship_name=args.ship_name,
        survey_name=args.survey_name,
        echosounder=args.echosounder,
        data_source=args.data_source,
        file_download_directory=args.file_download_directory,
        upload_to_gcp=args.upload_to_gcp,
        debug=args.debug,
    )

    # This is the output that may be piped elsewhere.
    downloaded_raw_file_path = Path(args.file_download_directory) / args.file_name
    print(downloaded_raw_file_path.resolve())
    # print(args.echosounder)
    sys.exit(0)


if __name__ == "__main__":
    main()
