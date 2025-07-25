"""Used for storing environment-specific settings such as database URIs and
such."""

RAW_DATA_FILE_TYPES = ["raw", "idx", "bot"]
CONVERTED_DATA_FILE_TYPES = ["netcdf", "nc"]
METADATA_FILE_TYPES = ["json"]
VALID_FILETYPES = ["raw", "idx", "netcdf", "nc", "json", "bot"]

VALID_ECHOSOUNDERS = [
    "EM710",
    "DAFT1-C11-201701",
    "ME70",
    "ES80",
    "EK60-EK5",
    "en615-jax-55146",
    "ar049-hat-5145",
    "GU1402L1",
    "sme140-201901",
    "EM122",
    "EM302",
    "en626-hat-55145",
    "GU1402L2",
    "M3",
    "ar040-hat-55145",
    "DAFT4-C11-201801",
    "sme80-201901",
    "EM124",
    "ar040-vac-55144",
    "DAFT2-C1-201701",
    "DAFT6-C4-201801",
    "en626-jax-55146",
    "DAFT5-C1-201801",
    "en615-vac-55144",
    "EM712",
    "sme100-201901",
    "EM2040C",
    "EM2045",
    "EM304",
    "MS70",
    "sme120-201901",
    "EK60",
    "EM3002",
    "ES60",
    "en615-hat-55145",
    "RESON7125",
    "ar040-jax-5146",
    "EK500",
    "ar049-vac-5144",
    "EM2040P",
    "EM2040",
    "en626-vac-55144",
    "EK80",
]
VALID_DATA_SOURCES = ["NCEI", "OMAO", "HDD", "TEST"]
