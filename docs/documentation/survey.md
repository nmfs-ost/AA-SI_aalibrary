# Documentation for `aalibrary.survey`

`LocalSurvey` represents a survey's files on your computer: it finds them, works out where each belongs in the GCP storage bucket, uploads them and checks the uploads. [Uploads](../usage/uploads.md#uploading-a-whole-survey-with-localsurvey) has a worked example.

In the tables below, a parameter whose **Default** is `required` must be given.

::: aalibrary.survey.LocalSurvey
    options:
      show_root_heading: false
      show_source: true
      heading_level: 2
      members:
        - upload_files
        - verify_gcp_uploads
        - print_all_files_in_directory
        - relocate
