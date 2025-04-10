from aalibrary.ingestion import download_raw_file_from_azure

download_raw_file_from_azure(
    file_name="1601RL-D20160107-T074016.raw",
    file_type="raw",
    ship_name="Reuben_Lasker",
    survey_name="RL1601",
    echosounder="EK60",
    data_source="OMAO",
    file_download_directory=".",
    config_file_path="./azure_config.ini",
    is_metadata=False,
    upload_to_gcp=True,
    debug=True,
)