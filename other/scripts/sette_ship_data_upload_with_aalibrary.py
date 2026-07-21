from aalibrary.survey import LocalSurvey
from aalibrary.utils import cloud_utils

# set up storage objects
s3_client, s3_resource, s3_bucket = cloud_utils.create_s3_objs()
gcp_stor_client, gcp_bucket_name, gcp_bucket = (
        cloud_utils.setup_gcp_storage_objs()
    )

# Specify directory path
directory_path = r"C:\Users\Reka.Domokos-Boyer\Desktop\Work\DataAnalyses\AmSam2HawaiiSE2602L3\PreliminaryLooks\ExampleRawFiles"

local_survey = LocalSurvey(
    ship_name="OSCAR ELTON SETTE",
    survey_name="SE2602",
    data_source="HDD",
    echosounder="EK80",
    directory_path=directory_path,
    upload_to_gcp=False,
    gcp_bucket=gcp_bucket,
    gcp_bucket_name=gcp_bucket_name,
    debug=True,
)

# Upload commands
local_survey.print_all_files_in_directory()
print(local_survey)
local_survey._test_upload_to_gcp_speeds(megabytes=100)
# Un-comment the bottom command to upload all files.
# local_survey._upload_to_gcp()
