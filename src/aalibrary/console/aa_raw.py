from aalibrary.ingestion import download_raw_file_from_ncei
import argparse
import sys
from pathlib import Path
from loguru import logger
import warnings
import pprint

warnings.filterwarnings("ignore")
def print_help():
    help_text = """
    Usage: aa-raw [OPTIONS]

    Options:
    --file_name                 Name of the file to download. (Required)
    --file_type                 Type of the file. Default: raw
    --ship_name                 Name of the ship. (Required)
    --survey_name               Name of the survey. (Required)
    --sonar_model               Type of echosounder. (Required)
    --data_source               Source of the data. Default: NCEI
    --file_download_directory   Directory to download the file. Default: current directory (.)
    --upload_to_gcp             Flag to upload the downloaded file to GCP.
    --debug                     Enable debug mode for verbose output.

    Description:
    This tool downloads a raw file from Azure based on the specified
    ship, survey, and sonar model. Optionally, the file can be uploaded
    to GCP after download. Useful for automating access to remote
    acoustic data.

    Example:
    aa-raw --file_name D20190804-T113723.raw --ship_name Henry_B._Bigelow \\
           --survey_name HB1907 --sonar_model EK60 \\
           --file_download_directory Henry_B._Bigelow_HB1907_EK60_NCEI
    """
    print(help_text)




def main():

    if len(sys.argv) == 1:
        if not sys.stdin.isatty():
            stdin_data = sys.stdin.readline().strip()
            if stdin_data:
                sys.argv.append(stdin_data)
        else:
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
    parser.add_argument("--sonar_model", required=True, help="Type of echosounder.")
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
    pretty_args = pprint.pformat(vars(args))
    logger.debug(f"Executing aa-raw configured with [OPTIONS]:\n{pretty_args}\n* ( Each aa-raw associated option_name may be overridden using --option_name value )" )
    download_raw_file_from_ncei(
        file_name=args.file_name,
        file_type=args.file_type,
        ship_name=args.ship_name,
        survey_name=args.survey_name,
        echosounder=args.sonar_model,
        data_source=args.data_source,
        file_download_directory=args.file_download_directory,
        upload_to_gcp=args.upload_to_gcp,
        debug=args.debug,
    )

    # This is the output that may be piped elsewhere.
    downloaded_raw_file_path = Path(args.file_download_directory) / args.file_name
    logger.success(f"Desired data fetched and saved to\n\t{downloaded_raw_file_path.resolve()}")
    logger.success(f"Piping saved .raw path to stdout ⟶")

    # Emit the output path to stdout for piping
    print(f"{downloaded_raw_file_path.resolve()}")
    sys.exit(0)


if __name__ == "__main__":
    main()
