# set the correct logging here
import sys
import logging
import warnings

# Import sub-packages
# flake8: noqa
if __package__ is None or __package__ == "":
    import config
else:
    from . import config

__version__ = "1.2.0"

# logging.basicConfig(
#     level=logging.DEBUG,
#     format="%(asctime)s [%(levelname)s] %(message)s",
#     handlers=[logging.StreamHandler(sys.stdout)],
# )

# # set up logging to console
# console = logging.StreamHandler()
# console.setLevel(logging.DEBUG)
# # set a format which is simpler for console use
# formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
# console.setFormatter(formatter)
# # add the handler to the root logger
# logging.getLogger("").addHandler(console)

def _disable_cloud_sdk_warning():
    """Disable the warning about missing Cloud SDK credentials."""

    warnings.filterwarnings(
        "ignore",
        message="Your application has authenticated using end user credentials",
    )

# Use the GCP production environment by default, unless whoever started this
# process already chose a project (the Workbench passes the project its user
# chose; a shell can `export AALIBRARY_GCP_PROJECT_ID=...`). Overwriting it
# here sent every tool to production whatever the caller asked. To switch
# from Python, call `aalibrary.config.use_gcp_dev()` (or use_gcp_prod()).
config.use_gcp_default()

# Disable the warning about missing Cloud SDK credentials.
_disable_cloud_sdk_warning()
