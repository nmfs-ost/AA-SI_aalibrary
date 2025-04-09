# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
