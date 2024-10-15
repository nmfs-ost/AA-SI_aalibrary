"""For helper functions."""


import os


# def parse_and_check_file_download_location(file_download_location: str = ""):
#     """Will clean (return a file download location and the file download location directory)
#     and check if the directory even exists in the first place, if not makes it.

#     Args:
#         file_download_location (str, optional): _description_. Defaults to "".
#     """

#     # Get the last directory of the file download location.
#     file_download_location = os.path.normpath(file_download_location) # normalize the path
#     file_download_location.split(os.sep)
#     file_download_location_directory = os.sep.join([os.path.normpath(file_download_location), file_name])
