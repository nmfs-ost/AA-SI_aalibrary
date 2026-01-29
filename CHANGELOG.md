<!-- markdownlint-disable MD024 -->

# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0]

This version is the first version released after User Acceptance Testing. There are numerous updates, and features included in this version. The list noted below is not exhaustive. Thank you to everyone who participated and helped in the UAT program!

### Added

- Script to cache ncei metadata files.
- Place for video tutorials for AALibrary.
- Validation functionality to tugboat api.
- Tugboat docs to documentation website.
- File for validating inport DMPs.
- Functionality for uploading metadata for netcdf files.
- Total file size identifier tool.
- Docs for scripts, configs, best practices for uploads, more info on gcp overview, and links to external pam docs.
- More extensions enabled for mkdocs.
- Function to help create empty config file.
- New usage section to the docs website.
- Upload folder as-is to the GCP storage bucket.
- Function to download specific folder for ncei.
- Function to download survey from ncei as-is.
- Business use case test cases.
- Code to check and compare files between local and cloud.
- Search functions for ncei.
- Permissions instructions for gcp workstations added on docs website.
- Link to startup script for workstations added on docs website.
- Spellcheck for NCEI ship names.
- Function for creating a test dir automatically.
- Parameter for deleting raw files after conversion to netcdf.
- `ICES` code to the metadata upload code.
- Spell check for ship names based on the `ICES` database.
- Code for getting valid and normalized `ICES` ship names.
- Functionality to normalize ship names.
- New submodule for testing sonar files called `sonar_checker`.
- Function to delay file deletion.
- Cron job function to clean gcp storage on compute engine.
- Code to interact with Coriolix api.
- Script/job for deleting files marked for deletion.
- `CALIBRATION_FILE_PATH` to the ncei cruise metadata table.
- `NCEI_URI` and `GCP_URI` to the metadata fields.
- Queries data class for future development.
- Various other additions...

### Fixed

- Updated file datetime parsing logic; now using regex with multiple pattern recognition.
- OMAO/Azure test cases commented out for the time-being.
- More detailed `FILE ALREADY EXISTS` notifications.
- Tugboat fields that were updated.
- More clear install instructions based on user feedback.
- Invalid file download directory on linux systems.
- Moved conversion functions to their own file.
- Consistent naming convention for `file_download_directory`.
- Updated `get_subdirectories_in_s3_bucket_location` to use a faster method, instead of iterating through every single object.
- `download_netcdf_file` now raises a file not found error after the error message.
- Python version string now only reflects the current version.
- Various bug fixes, typos, import errors, and more...

### Changed

- Metadata now uses RawFile objects.
- `download_raw_file_from_azure` now uses the RawFile object.

## [0.3.0]

### Added

- Test for omao download.
- RawFile object is now used throughout the code.
- File for testing the RawFile object.
- Check for underscores in ship_name attribute.
- Added assert statements for checking the echosounder specified in the function, versus the one found in the raw file.
- Project settings instructions to `README`.
- Function to get object key for an s3 object.
- Netcdf metadata to the metadata file that gets uploaded when converting and uploading a netcdf.
- Implemented overwrite functionality when converting raw to netcdf.
- Script for linux install.
- Update(s) to `README`.
- Comment(s).

### Fixed

- Code is now linted using PyLint.
- Code is now formatted using Black formatter.
- Import errors for pytest fixed.
- `convert_raw_to_netcdf` now uploads a metadata file as well.
- Changed version of zarr so aalibrary will work with python 3.10.
- The RawFile object takes care of an empty `file_download_directory` param.
- Removed `data_source` param from `download_netcdf_file` as it is not needed.
- Logging message when uploading metadata file is now more clear.
- Issues with application default login on workstations fixed.
- Added smaller file to ncei test for faster testing.
- `convert_local_raw_to_netcdf` now has a try/except clause.
- Various other errors.

### Changed

- Made installation instructions a little bit more clear.
- Moved util functions to appropriate places within codebase.
- Major changes in download logic for raw, idx, and bot files, based on their availability.

## [0.2.3]

### Added

- Import hierarchy, so that both `utils.cloud_utils` and `aalibrary.utils.cloud_utils` work
- Python & numpy version to the metadata files
- Download directory (or path) is now created if it already doesn't exist.
- File for checking and automatically testing installations of aalibrary - code setup
- Install_requires options for dependencies
- `setup.py` file
- README changes
- Comments

### Fixed

- Remove unnecessary link in readme
- Import errors, `find_packages` errors

## [0.2.2]

### Added

- Echosounders EK60 and EK500 as valid echosounders.
- Import statements to example code in README.

### Fixed

- Syntax error in example code

## [0.2.1]

### Added

- Survey metadata gets uploaded to GCP as well.
- Each file that is uploaded will have its own metadata associated with it.
- NetCDF files are now also uploaded from local along with raw and idx files.

### Fixed

- Minor bug fixes

## [0.1.0]

### Added

- Changelog
- Versions to requirements file
- A cleaner README
- Image/logo
- Hatch versioning and dynamic project dependencies via requirements file
- Caching to user upload function
- Ability to upload files from hdd or any `data_source`
- Quick connection test
- Func to delete files from gcp (helps with testing)
- User error-checking
- Bunch of tests for end-user error-checking

### Changed

- Imports and how they are handled.
- Minor changes

### Fixed

- Dependency issues
- Needed hatchling dependency to fully implement dynamic dependencies
- A problem with incorrect gcp location
- Typing change since boto3 doesn't know how to type correctly
- Import errors
- Incorrect parsing of netcdf file name
- Issue with file type
- Changed netCDF to netcdf since im doing lower
