# Configuration

AALibrary comes with many default configuration options. For example, the default GCP project that will be used when creating GCP storage objects is the dev project `ggn-nmfs-aa-dev-1`.

You can take a look at all of the default configs within code in the function signatures, or take a peek at the [config.py]() file for variables that are used as standards within code.

## Azure Configuration

Azure configuration requires an `azure_config.ini` file that is used for storing connection strings and keys. You can create an empty file using the `create_azure_config_file()` function found in `helpers.py`.

!!! note "NOTE"
    You will also need to have a space before and after the equals sign `=` when defining a value. For example, `azure_account_url = https...`.
