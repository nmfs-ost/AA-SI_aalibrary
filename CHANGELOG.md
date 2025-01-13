# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
- Typing change since boto3 doesnt know how to type correctly
- Import errors
- Incorrect parsing of netcdf file name
- Issue with file type
- Changed netCDF to netcdf since im doing lower
