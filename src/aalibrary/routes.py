"""This app routes the request to the correct function."""

from flask import request, send_from_directory

from aalibrary import ingestion, utils
from aalibrary import api_app


@api_app.route("/")
def index():
    return "Welcome to the Active Acoustics API"


@api_app.route("/hello/")
def hello_world():
    return "Hello World"


@api_app.route("/download-raw/")
def download_raw():
    file_name = request.args.get("file_name")
    file_type = request.args.get("file_type")
    ship_name = request.args.get("ship_name")
    survey_name = request.args.get("survey_name")
    echosounder = request.args.get("echosounder")
    force_download_from_ncei = request.args.get("force_download_from_ncei")
    debug = request.args.get("debug")

    ingestion.download_raw_file(file_name=file_name,
                                file_type=file_type,
                                ship_name=ship_name,
                                survey_name=survey_name,
                                echosounder=echosounder,
                                file_download_location=".",
                                is_metadata=False,
                                force_download_from_ncei=force_download_from_ncei,
                                debug=debug)


@api_app.route("/download_netcdf/")
def download_netcdf():
    file_name = request.args.get("file_name")
    file_type = request.args.get("file_type")
    ship_name = request.args.get("ship_name")
    survey_name = request.args.get("survey_name")
    echosounder = request.args.get("echosounder")
    data_source = request.args.get("data_source")
    debug = request.args.get("debug")

    gcp_stor_client, gcp_bucket_name, gcp_bucket = utils.cloud_utils.setup_gbq_storage_objs()

    ingestion.download_netcdf_file(file_name=file_name,
                                   file_type=file_type,
                                   ship_name=ship_name,
                                   survey_name=survey_name,
                                   echosounder=echosounder,
                                   data_source=data_source,
                                   file_download_location=".",
                                   gcp_bucket=gcp_bucket,
                                   is_metadata=False,
                                   debug=debug)
    
    return send_from_directory(directory=f".", filename=file_name, as_attachment=True)
