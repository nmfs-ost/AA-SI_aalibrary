# set the correct logging here
import sys
import logging
import warnings

# Import sub-packages
# flake8: noqa
from . import utils
from . import config

__version__ = "1.0.0"

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

# set up logging to console
console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
# set a format which is simpler for console use
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
console.setFormatter(formatter)
# add the handler to the root logger
logging.getLogger("").addHandler(console)

def _disable_cloud_sdk_warning():
    """Disable the warning about missing Cloud SDK credentials."""

    warnings.filterwarnings(
        "ignore",
        message="Your application has authenticated using end user credentials",
    )

# Use the GCP development environment by default. To use the production
# environment, call `aalibrary.config.use_gcp_prod()` method.
config.use_gcp_dev()

# Disable the warning about missing Cloud SDK credentials.
_disable_cloud_sdk_warning()